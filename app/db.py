"""SQLite 连接管理、建表与事务助手。

多实例（多进程）并发的关键点：
- WAL 模式允许读写并发，多个实例通过共享卷访问同一个数据库文件；
- 每个写操作使用 IMMEDIATE 事务，进入事务即获取保留锁，把锁冲突
  推迟为 ``database is locked`` 并做有界重试；
- 读操作使用 DEFERRED 事务拿到单一快照，一次请求内所有查询都基于
  同一已提交状态；
- 业务校验与写入在同一个事务内完成，失败一律 ROLLBACK，绝不留半版。
"""
from __future__ import annotations

import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager

from .config import settings

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS stations (
    station_id    TEXT PRIMARY KEY,
    version       INTEGER NOT NULL CHECK (version >= 0)
);

CREATE TABLE IF NOT EXISTS install_segments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id   TEXT NOT NULL,
    probe_id     TEXT NOT NULL,
    start_ts     TEXT NOT NULL,          -- 定宽 UTC 文本，字典序即时间序
    end_ts       TEXT,                   -- NULL 表示开放区间（至今）
    version_from INTEGER NOT NULL,
    FOREIGN KEY (station_id) REFERENCES stations(station_id),
    UNIQUE (station_id, probe_id, start_ts)
);

CREATE INDEX IF NOT EXISTS idx_segments_probe ON install_segments(probe_id);
CREATE INDEX IF NOT EXISTS idx_segments_station ON install_segments(station_id);
CREATE INDEX IF NOT EXISTS idx_segments_probe_ts
    ON install_segments(probe_id, start_ts, end_ts);

CREATE TABLE IF NOT EXISTS readings (
    observation_id TEXT PRIMARY KEY,
    station_id     TEXT NOT NULL,
    ts_utc         TEXT NOT NULL,
    temperature    REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_readings_station_ts
    ON readings(station_id, ts_utc);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        settings.db_path,
        timeout=settings.busy_timeout_ms / 1000.0,
        isolation_level=None,  # 事务完全由代码控制
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA wal_autocheckpoint = 1000")
    return conn


def init_db() -> None:
    """首次启动时建库建表（幂等）。"""
    parent = os.path.dirname(os.path.abspath(settings.db_path))
    os.makedirs(parent, exist_ok=True)
    conn = _connect()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """每请求一条连接，确保关闭。"""
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """带锁重试的 IMMEDIATE 写事务。

    业务错误（APIError）不重试，直接回滚后向上抛；仅对锁等待超时重试。
    """
    attempt = 0
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError:  # database is locked
            attempt += 1
            if attempt > settings.lock_retries:
                raise
            time.sleep(settings.lock_retry_delay)
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


@contextmanager
def read_snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """DEFERRED 只读快照：首条 SELECT 建立快照，事务内视图一致。"""
    conn.execute("BEGIN DEFERRED")
    try:
        yield conn
    finally:
        conn.execute("COMMIT")
