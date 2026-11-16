"""网关批次采集领域逻辑。

关键不变量：
- 一批的“合法行入库 + 坏行隔离 + 批次状态落库”在**同一个 IMMEDIATE 事务**
  内完成，任何一步失败整体回滚，绝无半批；
- 只有该事务成功提交后，才允许向网关发送确认，网关清退不依赖本地结果之外
  的任何状态；
- 同站同批号在 ``collection_batches`` 中只有一行，重试只做对账，不会重复
  入库、重复隔离；
- 隔离行原文、来源（站/批/行号）、原因固定保留，不覆盖任何原读数。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any

from .config import settings
from .db import read_snapshot, write_tx
from .errors import APIError
from .gateway_client import GatewayClient
from .schemas import ReadingIn, utcnow_text

# 隔离原因码（稳定可检索），消息面向人工复核
REASON_INVALID_JSON = "invalid_json"
REASON_INVALID_FIELDS = "invalid_fields"
REASON_READING_CONFLICT = "reading_conflict"

_REASON_MESSAGES = {
    REASON_INVALID_JSON: "原始报文不是合法 JSON 对象，无法解析为观测读数",
    REASON_INVALID_FIELDS: "字段缺失或非法（观测号/站号/带时区时刻/有限温度）",
    REASON_READING_CONFLICT: "同观测号已存在且内容不同，坏行隔离且原读数保持不变",
}


# ----------------------------- 行级校验 -----------------------------

def _validate_row(raw: Any) -> tuple[dict | None, str | None]:
    """按现有读数规则校验一行。

    返回 (normalized_reading, reason_code)：合法时原因码为 None；
    非法时读数为 None。网关同时给了 data 与 raw，优先用结构化 data 校验，
    raw 仅作为原文留证。
    """
    data = raw.get("data") if isinstance(raw, dict) else None
    if not isinstance(data, dict):
        return None, REASON_INVALID_JSON
    try:
        payload = ReadingIn(**data)
    except Exception:
        return None, REASON_INVALID_FIELDS
    # 再走一次数值/时刻归一化（极端类型经 pydantic 后仍可能在此失败）
    try:
        return payload.normalized(), None
    except Exception:
        return None, REASON_INVALID_FIELDS


def _raw_text(raw: Any) -> str:
    if not isinstance(raw, dict):
        return json.dumps(raw, ensure_ascii=False, default=str)
    text = raw.get("raw")
    if isinstance(text, str) and text:
        return text
    return json.dumps(raw.get("data"), ensure_ascii=False, default=str)


def _line_no(raw: Any) -> int | None:
    if isinstance(raw, dict) and isinstance(raw.get("line_no"), int):
        return raw["line_no"]
    return None


# ----------------------------- 整批落库 -----------------------------

def persist_batch(conn: sqlite3.Connection, station_id: str,
                  batch: dict[str, Any]) -> dict:
    """在调用方已开启的写事务内，整批接纳/隔离并写批次状态。

    本函数不提交事务；任何错误向上抛出由外层统一回滚。
    """
    batch_id = batch["batch_id"]
    rows = batch["rows"]
    if not isinstance(rows, list) or not rows:
        raise APIError(502, "gateway_protocol_error",
                       "网关批次为空或 rows 结构非法",
                       {"station_id": station_id, "batch_id": batch_id})
    if len(rows) > settings.max_rows_per_batch:
        raise APIError(502, "batch_too_large",
                       f"网关批次 {len(rows)} 行，超过单批上限 "
                       f"{settings.max_rows_per_batch} 行，拒绝落库",
                       {"station_id": station_id, "batch_id": batch_id,
                        "row_count": len(rows)})

    # 行号必须固定且批内唯一，否则协议被破坏，整批拒绝
    line_nos = [_line_no(r) for r in rows]
    if any(ln is None for ln in line_nos) or len(set(line_nos)) != len(rows):
        raise APIError(502, "gateway_protocol_error",
                       "网关批次行号缺失或批内重复，拒绝落库",
                       {"station_id": station_id, "batch_id": batch_id})

    now = utcnow_text()
    accepted = 0
    quarantined: list[dict] = []

    for raw in rows:
        line_no = _line_no(raw)
        raw_content = _raw_text(raw)
        reading, reason = _validate_row(raw)

        if reason is not None:
            quarantined.append({
                "line_no": line_no,
                "observation_id": None,
                "raw_content": raw_content,
                "reason_code": reason,
                "reason_message": _REASON_MESSAGES[reason],
            })
            continue

        # 合法行站号必须与采集站一致，否则按字段非法隔离
        if reading["station_id"] != station_id:
            quarantined.append({
                "line_no": line_no,
                "observation_id": reading["observation_id"],
                "raw_content": raw_content,
                "reason_code": REASON_INVALID_FIELDS,
                "reason_message": (
                    f"行内站号 {reading['station_id']} 与采集网关站号 "
                    f"{station_id} 不一致，按字段非法隔离"
                ),
            })
            continue

        existing = conn.execute(
            "SELECT observation_id, station_id, ts_utc, temperature "
            "FROM readings WHERE observation_id = ?",
            (reading["observation_id"],),
        ).fetchone()
        if existing is not None:
            existing_d = dict(existing)
            if (
                existing_d["station_id"] == reading["station_id"]
                and existing_d["ts_utc"] == reading["ts_utc"]
                and existing_d["temperature"] == reading["temperature"]
            ):
                # 同号同文：幂等跳过，不重复入库，计为已接纳
                accepted += 1
                continue
            # 同号异文：隔离坏行，原读数保持不变
            quarantined.append({
                "line_no": line_no,
                "observation_id": reading["observation_id"],
                "raw_content": raw_content,
                "reason_code": REASON_READING_CONFLICT,
                "reason_message": _REASON_MESSAGES[REASON_READING_CONFLICT],
            })
            continue

        conn.execute(
            "INSERT INTO readings (observation_id, station_id, ts_utc, temperature) "
            "VALUES (:observation_id, :station_id, :ts_utc, :temperature)",
            reading,
        )
        accepted += 1

    conn.execute(
        """
        INSERT INTO collection_batches
            (station_id, batch_id, received_rows, accepted_rows,
             quarantined_rows, state, persisted_at)
        VALUES (?, ?, ?, ?, ?, 'persisted', ?)
        """,
        (station_id, batch_id, len(rows), accepted, len(quarantined), now),
    )
    conn.executemany(
        """
        INSERT INTO quarantine_rows
            (station_id, batch_id, line_no, observation_id, raw_content,
             reason_code, reason_message, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(station_id, batch_id, q["line_no"], q["observation_id"],
          q["raw_content"], q["reason_code"], q["reason_message"], now)
         for q in quarantined],
    )

    return {
        "station_id": station_id,
        "batch_id": batch_id,
        "received_rows": len(rows),
        "accepted_rows": accepted,
        "quarantined_rows": len(quarantined),
        "state": "persisted",
        "persisted_at": now,
        "acked_at": None,
    }


# ----------------------------- 状态查询 -----------------------------

def _batch_row_to_dict(row: sqlite3.Row) -> dict:
    return {
        "station_id": row["station_id"],
        "batch_id": row["batch_id"],
        "state": row["state"],
        "received_rows": row["received_rows"],
        "accepted_rows": row["accepted_rows"],
        "quarantined_rows": row["quarantined_rows"],
        "persisted_at": row["persisted_at"],
        "acked_at": row["acked_at"],
    }


def get_collection_status(conn: sqlite3.Connection,
                          station_id: str | None) -> dict:
    clauses: list[str] = []
    params: tuple = ()
    if station_id:
        clauses.append("station_id = ?")
        params = (station_id,)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT * FROM collection_batches {where} "
        f"ORDER BY persisted_at, station_id, batch_id",
        params,
    ).fetchall()
    pending_clauses = clauses + ["state = 'persisted'"]
    pending = conn.execute(
        "SELECT * FROM collection_batches WHERE "
        + " AND ".join(pending_clauses)
        + " ORDER BY persisted_at, station_id, batch_id",
        params,
    ).fetchall()
    batches = [_batch_row_to_dict(r) for r in rows]
    return {
        "station_id": station_id,
        "total_batches": len(batches),
        "persisted_batches": sum(1 for b in batches if b["state"] == "persisted"),
        "acked_batches": sum(1 for b in batches if b["state"] == "acked"),
        "total_received": sum(b["received_rows"] for b in batches),
        "total_accepted": sum(b["accepted_rows"] for b in batches),
        "total_quarantined": sum(b["quarantined_rows"] for b in batches),
        "pending_ack": [_batch_row_to_dict(r) for r in pending],
        "batches": batches,
    }


def list_quarantine(conn: sqlite3.Connection, station_id: str | None,
                    batch_id: str | None, reason_code: str | None) -> dict:
    clauses: list[str] = []
    params: list[Any] = []
    if station_id:
        clauses.append("station_id = ?")
        params.append(station_id)
    if batch_id:
        clauses.append("batch_id = ?")
        params.append(batch_id)
    if reason_code:
        clauses.append("reason_code = ?")
        params.append(reason_code)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT station_id, batch_id, line_no, observation_id, raw_content, "
        f"reason_code, reason_message, created_at FROM quarantine_rows "
        f"{where} ORDER BY station_id, batch_id, line_no",
        params,
    ).fetchall()
    return {
        "station_id": station_id,
        "batch_id": batch_id,
        "reason_code": reason_code,
        "total": len(rows),
        "items": [dict(r) for r in rows],
    }


def get_batch_detail(conn: sqlite3.Connection, station_id: str,
                     batch_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM collection_batches WHERE station_id = ? AND batch_id = ?",
        (station_id, batch_id),
    ).fetchone()
    return None if row is None else _batch_row_to_dict(row)


# ----------------------------- 采集编排 -----------------------------

def _mark_acked(conn: sqlite3.Connection, station_id: str, batch_id: str) -> None:
    conn.execute(
        "UPDATE collection_batches SET state = 'acked', acked_at = ? "
        "WHERE station_id = ? AND batch_id = ? AND state = 'persisted'",
        (utcnow_text(), station_id, batch_id),
    )


def _persist_or_get(conn_factory, station_id: str,
                    batch: dict[str, Any]) -> tuple[bool, dict]:
    """整批落库；并发实例已抢先落库时复用其结果。

    靠主键 (station_id, batch_id) 与隔离行唯一键去重：后来者的插入触发
    IntegrityError，事务整体回滚后重读首批结果，绝不产生重复行/重复隔离。
    返回 (是否本次新落库, 批次摘要)。
    """
    batch_id = batch["batch_id"]
    for attempt in range(2):
        try:
            with conn_factory() as conn, _immediate(conn) as tx:
                existing = get_batch_detail(tx, station_id, batch_id)
                if existing is not None:
                    return False, existing
                return True, persist_batch(tx, station_id, batch)
        except sqlite3.IntegrityError:
            # 并发实例抢先提交：回滚后重试，在事务内读到首批结果
            continue
    with conn_factory() as conn, _snapshot(conn) as tx:
        existing = get_batch_detail(tx, station_id, batch_id)
    if existing is None:  # 理论不可达：冲突后首批结果必然已提交
        raise APIError(500, "collection_conflict",
                       "并发采集冲突且无法读取已落库批次",
                       {"station_id": station_id, "batch_id": batch_id})
    return False, existing


def _ack_and_mark(conn_factory, client: GatewayClient,
                  station_id: str, batch_id: str) -> dict:
    """先在数据库事务之外完成网关确认，再用一个短事务标记 acked。

    HTTP 等待期间不持有任何 SQLite 锁，其他网关采集与手工录入不受阻塞。
    网关返回 410（already_cleared）同样视为确认成功；标记失败时批次保持
    persisted，由后续对账补记，不漏标也不误报。
    """
    result = client.ack_batch(station_id, batch_id)
    if result not in ("acked", "already_cleared"):  # pragma: no cover - 客户端只返回这两类或抛错
        raise APIError(502, "gateway_ack_failed",
                       f"网关确认返回未知结果：{result}")
    with conn_factory() as conn, _immediate(conn) as tx:
        _mark_acked(tx, station_id, batch_id)
        return get_batch_detail(tx, station_id, batch_id)


def run_collection(conn_factory, station_id: str,
                   client: GatewayClient | None = None) -> dict:
    """执行一次按网关采集。

    ``conn_factory`` 为产生新连接的可调用对象（每个事务独立连接），
    所有网关 HTTP 调用都在数据库事务之外进行，等待响应时不占数据库锁。
    """
    client = client or GatewayClient()

    # 1) 对账历史“已落库未回执”批次：先快照读清单，HTTP 补确认，再短事务标记。
    #    单批确认失败不阻断其余批次，最后把第一个故障抛给调用方（状态均持久化，
    #    下次采集会再次对账，幂等不漏）。
    with conn_factory() as conn, _snapshot(conn) as tx:
        pending_rows = tx.execute(
            "SELECT station_id, batch_id FROM collection_batches "
            "WHERE station_id = ? AND state = 'persisted' ORDER BY persisted_at",
            (station_id,),
        ).fetchall()
    reconciled: list[dict] = []
    first_error: Exception | None = None
    for row in pending_rows:
        try:
            reconciled.append(
                _ack_and_mark(conn_factory, client, station_id, row["batch_id"])
            )
        except Exception as exc:  # noqa: BLE001 - 记录后继续，保证其余批次可对账
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error

    # 2) 拉取队头批次（HTTP 等待期间不持有任何本地锁）
    batch = client.pull_next_batch(station_id)
    if batch is None:
        return {"station_id": station_id, "status": "no_batch",
                "batch": None, "reconciled": reconciled}

    batch_id = batch["batch_id"]

    # 3) 整批接纳/隔离落库（单事务；本地写失败整体回滚，绝不向网关确认）
    is_new, summary = _persist_or_get(conn_factory, station_id, batch)
    if not is_new:
        # 重投递或多实例并发去重：已落库则只补回执，不重复入库/隔离
        if summary["state"] == "persisted":
            summary = _ack_and_mark(conn_factory, client, station_id, batch_id)
        return {"station_id": station_id, "status": "already_persisted",
                "batch": summary, "reconciled": reconciled}

    # 4) 本地持久化成功后才确认网关；确认失败则保留 persisted，下次对账
    summary = _ack_and_mark(conn_factory, client, station_id, batch_id)
    return {"station_id": station_id, "status": "collected",
            "batch": summary, "reconciled": reconciled}


# ----------------------------- 事务小助手 -----------------------------

@contextmanager
def _immediate(conn: sqlite3.Connection):
    with write_tx(conn) as tx:
        yield tx


@contextmanager
def _snapshot(conn: sqlite3.Connection):
    with read_snapshot(conn) as tx:
        yield tx
