"""网关批次采集的关键场景测试。

采集端通过 httpx ASGITransport 直连模拟网关应用（真实 FastAPI 协议栈、
真实 JSON 序列化），再经 API 路由触发采集，覆盖：
- 整批接纳 + 坏行隔离（不卡正常数据、不覆盖原读数）
- 重投递去重、断线重试、回执丢失、本地写失败不确认
- 重复确认不误删下一批、超 100 行拒收、跨站故障隔离
- 状态/隔离查询接口与过滤
"""
from __future__ import annotations

import socket
import sqlite3
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import collection as collection_mod
from app.config import settings
from app.db import init_db
from app.gateway_client import GatewayClient
from gateway import app as gateway_mod


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _RetryTransport(httpx.BaseTransport):
    """传输层连接重置/陈旧连接自动重试一次（与生产采集端同一容错策略）。"""

    def __init__(self) -> None:
        self._inner = httpx.HTTPTransport()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            return self._inner.handle_request(request)
        except (httpx.RemoteProtocolError, httpx.ReadError,
                httpx.WriteError, httpx.ConnectError):
            return self._inner.handle_request(request)


def _reading(oid, station="ST-A", ts="2026-01-15T08:00:00Z", temp=12.5):
    return {"observation_id": oid, "station_id": station,
            "ts_utc": ts, "temperature": temp}


def _good_row(i, station="ST-A"):
    return _reading(f"GW-O-{i}", station)


@pytest.fixture(scope="session")
def live_gateway(tmp_path_factory):
    """真实 uvicorn 线程承载模拟网关，协议与部署形态一致。"""
    data_path = str(tmp_path_factory.mktemp("gw") / "state.json")
    gateway_mod.DATA_PATH = data_path
    gateway_mod.state = gateway_mod.GatewayState()
    config = uvicorn.Config(gateway_mod.app, host="127.0.0.1",
                            port=_free_port(), log_level="warning",
                            lifespan="on", timeout_keep_alive=0)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{config.port}"
    with httpx.Client(base_url=base, timeout=10,
                      transport=_RetryTransport()) as http:
        for _ in range(100):
            try:
                if http.get("/health").status_code == 200:
                    break
            except httpx.TransportError:
                pass
            time.sleep(0.05)
        yield http
    server.should_exit = True
    thread.join(timeout=10)


@pytest.fixture()
def gw_http(live_gateway, monkeypatch):
    """每例复位网关状态，并把采集客户端指向该网关。"""
    live_gateway.post("/test/reset")
    monkeypatch.setattr(settings, "gateway_timeout_sec", 5.0, raising=False)

    def factory():
        return GatewayClient(base_url=str(live_gateway.base_url), timeout=5.0)

    monkeypatch.setattr(collection_mod, "GatewayClient", factory)
    return live_gateway


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(settings, "db_path", db_path)
    monkeypatch.setattr(settings, "max_rows_per_batch", 100, raising=False)
    init_db()
    from app.main import app
    with TestClient(app) as c:
        yield c


def _seed(gw, station="ST-A", batch_id="B1", rows=None):
    r = gw.post(f"/test/stations/{station}/backlog",
                json={"batch_id": batch_id, "rows": rows or []})
    assert r.status_code == 200, r.text
    return r.json()


def _run(client, station="ST-A"):
    return client.post("/collections/run", json={"station_id": station})


def _count(sqlite_path, table, where="", params=()):
    conn = sqlite3.connect(sqlite_path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table} {where}",
                            params).fetchone()[0]
    finally:
        conn.close()


# ----------------------------- 接纳 + 隔离 -----------------------------

def test_collect_batch_mixed_rows_quarantine_and_accept(client, gw_http):
    rows = [
        _good_row(1),                                   # 1 合法
        _reading("GW-O-2", temp=20.0),                  # 2 合法
        "not-a-json-line",                              # 3 坏 JSON
        '{"observation_id":"GW-O-4","station_id":"ST-A",'
        '"ts_utc":"2026-01-15T08:00:00Z","temperature":"hot"}',  # 4 字段非法
        '{"observation_id":"GW-O-5","station_id":"ST-A",'
        '"ts_utc":"2026-01-15T08:00:00Z","temperature":5.5}',    # 5 合法
        '{"observation_id":"GW-O-6","station_id":"ST-B",'
        '"ts_utc":"2026-01-15T08:00:00Z","temperature":6.6}',    # 6 跨站
        _good_row(8),                                   # 8 合法
    ]
    # 空行与 NaN/Infinity 在网关投放层即被拒绝（标准 JSON 无法承载）
    bad_seed = gw_http.post("/test/stations/ST-A/backlog",
                            json={"batch_id": "B1", "rows": rows + [""]})
    assert bad_seed.status_code == 422
    nan_seed = gw_http.post(
        "/test/stations/ST-A/backlog",
        content=('{"batch_id":"BN","rows":[{"observation_id":"N1",'
                 '"station_id":"ST-A","ts_utc":"2026-01-15T08:00:00Z",'
                 '"temperature":NaN}]}'),
        headers={"content-type": "application/json"},
    )
    assert nan_seed.status_code == 422
    # 正式投放 7 行
    reseed = gw_http.post("/test/stations/ST-A/backlog",
                          json={"batch_id": "B1", "rows": rows})
    assert reseed.status_code == 200, reseed.text

    r = _run(client)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "collected"
    assert body["batch"]["state"] == "acked"
    assert body["batch"]["received_rows"] == 7
    assert body["batch"]["accepted_rows"] == 4
    assert body["batch"]["quarantined_rows"] == 3
    # 网关队头已清退
    assert gw_http.get("/stations/ST-A/batches/next").status_code == 204

    # 合法行进 readings
    status = client.get("/collections/status?station_id=ST-A").json()
    assert status["total_batches"] == 1
    assert status["acked_batches"] == 1
    assert status["persisted_batches"] == 0
    assert status["total_accepted"] == 4
    assert status["total_quarantined"] == 3

    q = client.get("/collections/quarantine?station_id=ST-A").json()
    assert q["total"] == 3
    reasons = sorted(item["reason_code"] for item in q["items"])
    assert reasons == ["invalid_fields", "invalid_fields", "invalid_json"]
    # 隔离行保留原文、来源与原因
    item3 = next(i for i in q["items"] if i["line_no"] == 3)
    assert item3["raw_content"] == "not-a-json-line"
    assert item3["reason_message"]
    assert item3["station_id"] == "ST-A" and item3["batch_id"] == "B1"

    # 同号异文：先放一条原读数，再投放同号异文批次
    client.post("/readings", json=_reading("CONFLICT-1", temp=1.0))
    _seed(gw_http, batch_id="B2", rows=[
        _reading("CONFLICT-1", temp=99.9),
        _good_row(9),
    ])
    r2 = _run(client)
    assert r2.status_code == 200
    assert r2.json()["batch"]["accepted_rows"] == 1
    assert r2.json()["batch"]["quarantined_rows"] == 1
    # 原读数未被覆盖
    conn = sqlite3.connect(settings.db_path)
    try:
        val = conn.execute(
            "SELECT temperature FROM readings WHERE observation_id = ?",
            ("CONFLICT-1",)).fetchone()[0]
    finally:
        conn.close()
    assert val == 1.0
    q2 = client.get(
        "/collections/quarantine?reason_code=reading_conflict").json()
    assert q2["total"] == 1
    assert q2["items"][0]["observation_id"] == "CONFLICT-1"
    assert "CONFLICT-1" in q2["items"][0]["raw_content"]


def test_redeliver_same_batch_is_idempotent(client, gw_http):
    _seed(gw_http, batch_id="B1", rows=[_good_row(1), _good_row(2)])
    first = _run(client)
    assert first.json()["status"] == "collected"

    # 网关队列已空：再采集返回 no_batch
    second = _run(client)
    assert second.json()["status"] == "no_batch"

    # 模拟重投递：投放同批号批次（内容相同）
    _seed(gw_http, batch_id="B1", rows=[_good_row(1), _good_row(2)])
    third = _run(client)
    assert third.json()["status"] == "already_persisted"
    assert third.json()["batch"]["state"] == "acked"
    # readings 仍只有 2 条、隔离 0 条、批次仍只有 1 个
    assert _count(settings.db_path, "collection_batches") == 1
    assert _count(settings.db_path, "quarantine_rows") == 0
    assert _count(settings.db_path, "readings") == 2


def test_disconnect_retry_no_loss_no_false_complete(client, gw_http):
    _seed(gw_http, batch_id="B1", rows=[_good_row(i) for i in range(3)])
    # 断线：采集必须失败且不能误报完成
    gw_http.post("/test/faults", json={"mode": "disconnect"})
    failed = _run(client)
    assert failed.status_code == 502
    assert failed.json()["error"]["code"] == "gateway_unavailable"
    # 断线发生在拉取阶段，本地无批次、readings 无数据
    assert _count(settings.db_path, "collection_batches") == 0
    assert _count(settings.db_path, "readings") == 0
    # 解除故障后采集成功
    gw_http.post("/test/faults", json={"mode": "none"})
    ok = _run(client)
    assert ok.status_code == 200 and ok.json()["status"] == "collected"
    assert _count(settings.db_path, "readings") == 3


def test_ack_lost_then_retry_reconciles(client, gw_http):
    _seed(gw_http, batch_id="B1", rows=[_good_row(i) for i in range(2)])
    # 回执丢失：网关已清退，客户端收到 500
    gw_http.post("/test/faults", json={"mode": "ack_lost"})
    r1 = _run(client)
    assert r1.status_code == 502
    assert r1.json()["error"]["code"] == "ack_lost"
    # 本地已持久化但未回执，且不能误报完成
    st1 = client.get("/collections/status?station_id=ST-A").json()
    assert st1["persisted_batches"] == 1 and st1["acked_batches"] == 0
    assert _count(settings.db_path, "readings") == 2
    # 网关侧已清退（回执丢失是先清退后报错）
    assert gw_http.get("/stations/ST-A/batches/next").status_code == 204
    # 下一批已可投放；重试采集：先对账 B1（网关 410 -> already_cleared），再拉 B2
    gw_http.post("/test/faults", json={"mode": "none"})
    _seed(gw_http, batch_id="B2", rows=[_good_row(3)])
    r2 = _run(client)
    assert r2.status_code == 200
    assert r2.json()["status"] == "collected"
    reconciled_ids = [b["batch_id"] for b in r2.json()["reconciled"]]
    assert reconciled_ids == ["B1"]
    assert r2.json()["batch"]["batch_id"] == "B2"
    st2 = client.get("/collections/status?station_id=ST-A").json()
    assert st2["persisted_batches"] == 0 and st2["acked_batches"] == 2
    # 不漏不重
    assert _count(settings.db_path, "readings") == 3
    assert _count(settings.db_path, "quarantine_rows") == 0
    assert _count(settings.db_path, "collection_batches") == 2


def test_local_write_failure_never_acks_gateway(client, gw_http, monkeypatch):
    _seed(gw_http, batch_id="B1", rows=[_good_row(1)])

    monkeypatch.setattr(settings, "busy_timeout_ms", 100, raising=False)
    monkeypatch.setattr(settings, "lock_retries", 2, raising=False)
    monkeypatch.setattr(settings, "lock_retry_delay", 0.02, raising=False)

    # 后台连接在整个采集调用期间持有写锁，让采集端重试耗尽后失败
    blocker = sqlite3.connect(settings.db_path, isolation_level=None,
                              check_same_thread=False)
    blocker.execute("PRAGMA busy_timeout = 0")
    release = threading.Event()

    def hold_lock():
        blocker.execute("BEGIN IMMEDIATE")
        release.wait(timeout=15)
        blocker.execute("COMMIT")
        blocker.close()

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    time.sleep(0.2)  # 等写锁被持有
    try:
        r = _run(client)
        assert r.status_code >= 500
    finally:
        release.set()
        holder.join(timeout=5)

    # 写失败没有半批：无批次、无读数、无隔离
    assert _count(settings.db_path, "collection_batches") == 0
    assert _count(settings.db_path, "readings") == 0
    assert _count(settings.db_path, "quarantine_rows") == 0
    # 网关批次仍在队头（未被确认清退）
    head = gw_http.get("/stations/ST-A/batches/next")
    assert head.status_code == 200 and head.json()["batch_id"] == "B1"
    # 锁释放后重试可成功
    ok = _run(client)
    assert ok.status_code == 200 and ok.json()["status"] == "collected"


def test_duplicate_ack_does_not_delete_next_batch(client, gw_http):
    _seed(gw_http, batch_id="B1", rows=[_good_row(1)])
    _seed(gw_http, batch_id="B2", rows=[_good_row(2)])
    _run(client)  # 清退 B1，B2 成为队头
    # 直接对网关重复确认旧批号 B1：必须被拒，B2 不受影响
    dup = gw_http.post("/stations/ST-A/batches/B1/ack")
    assert dup.status_code == 410
    assert dup.json()["error"]["code"] == "batch_not_head"
    head = gw_http.get("/stations/ST-A/batches/next")
    assert head.json()["batch_id"] == "B2"
    # 再采集清退 B2
    r = _run(client)
    assert r.json()["batch"]["batch_id"] == "B2"
    assert gw_http.get("/stations/ST-A/batches/next").status_code == 204


def test_batch_over_100_rows_rejected_before_gateway_ack(client, gw_http):
    rows = [_good_row(i) for i in range(101)]
    # 网关自身也强制 100 行上限
    r = gw_http.post("/test/stations/ST-A/backlog",
                     json={"batch_id": "BIG", "rows": rows})
    assert r.status_code == 422
    # 100 行整批可正常采集
    _seed(gw_http, batch_id="B100", rows=rows[:100])
    ok = _run(client)
    assert ok.status_code == 200
    assert ok.json()["batch"]["received_rows"] == 100
    assert ok.json()["batch"]["accepted_rows"] == 100


def test_duplicate_reading_in_batch_is_idempotent_not_quarantined(client, gw_http):
    client.post("/readings", json=_good_row(1))
    _seed(gw_http, batch_id="B1", rows=[_good_row(1), _good_row(2)])
    r = _run(client)
    assert r.status_code == 200
    assert r.json()["batch"]["accepted_rows"] == 2  # 1 条幂等 + 1 条新入
    assert r.json()["batch"]["quarantined_rows"] == 0
    assert _count(settings.db_path, "readings") == 2
    assert _count(settings.db_path, "quarantine_rows") == 0


def test_status_and_quarantine_filters_and_batch_detail(client, gw_http):
    _seed(gw_http, station="ST-A", batch_id="A1", rows=[
        _good_row(1), "broken-json"])
    _seed(gw_http, station="ST-B", batch_id="B1", rows=[
        _reading("X-1", station="ST-B"), "also-broken"])
    _run(client, "ST-A")
    _run(client, "ST-B")

    all_st = client.get("/collections/status").json()
    assert all_st["total_batches"] == 2
    st_a = client.get("/collections/status?station_id=ST-A").json()
    assert st_a["station_id"] == "ST-A" and st_a["total_batches"] == 1

    q_all = client.get("/collections/quarantine").json()
    assert q_all["total"] == 2
    q_a = client.get("/collections/quarantine?station_id=ST-A").json()
    assert q_a["total"] == 1 and q_a["items"][0]["station_id"] == "ST-A"
    q_batch = client.get(
        "/collections/quarantine?station_id=ST-B&batch_id=B1").json()
    assert q_batch["total"] == 1 and q_batch["items"][0]["batch_id"] == "B1"
    q_reason = client.get(
        "/collections/quarantine?reason_code=invalid_json").json()
    assert q_reason["total"] == 2

    detail = client.get("/stations/ST-A/batches/A1")
    assert detail.status_code == 200
    assert detail.json()["batch_id"] == "A1"
    missing = client.get("/stations/ST-A/batches/NOPE")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "batch_not_found"


# ----------------------------- 多实例 / 不阻塞 -----------------------------

def test_concurrent_collection_same_batch_exactly_one_persist(client, gw_http):
    """两个采集实例并发拉取同一重投递批次：恰好落库一次、回执一次。"""
    from concurrent.futures import ThreadPoolExecutor

    _seed(gw_http, batch_id="B1", rows=[_good_row(i) for i in range(5)])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: _run(client), range(2)))
    statuses = sorted(r.json()["status"] for r in results)
    # 恰好一方完成采集；另一方要么撞上重投递去重(already_persisted)，
    # 要么在网关清退后拉空(no_batch)——两者都不会产生第二批
    assert statuses in (["collected", "no_batch"],
                        ["already_persisted", "collected"])
    assert _count(settings.db_path, "collection_batches") == 1
    assert _count(settings.db_path, "readings") == 5
    assert _count(settings.db_path, "quarantine_rows") == 0


def test_concurrent_collection_redelivery_dedup_under_race(client, gw_http):
    """重投递批次在两个实例并发处理时靠主键去重，不重复入库/隔离。"""
    from concurrent.futures import ThreadPoolExecutor

    _seed(gw_http, batch_id="DUP1", rows=[_good_row(1), _good_row(2)])
    first = _run(client)
    assert first.json()["status"] == "collected"
    # 同批号重投递
    _seed(gw_http, batch_id="DUP1", rows=[_good_row(1), _good_row(2)])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: _run(client), range(2)))
    for r in results:
        assert r.status_code == 200
        assert r.json()["status"] in ("already_persisted", "no_batch")
    assert _count(settings.db_path, "collection_batches") == 1
    assert _count(settings.db_path, "readings") == 2


def test_slow_gateway_does_not_block_other_station_or_manual_entry(
        client, gw_http):
    """ST-A 网关响应缓慢期间，ST-B 采集与手工读数录入仍能完成。"""
    from concurrent.futures import ThreadPoolExecutor

    # ST-A 投放批次，并给 ST-A 的控制/数据请求注入人工延迟
    _seed(gw_http, station="ST-A", batch_id="A1", rows=[_good_row(1, "ST-A")])
    _seed(gw_http, station="ST-B", batch_id="B1",
          rows=[_reading("B-1", station="ST-B")])
    gw_http.post("/test/faults",
                 json={"mode": "none", "station_id": "ST-A", "delay_sec": 2.0})

    with ThreadPoolExecutor(max_workers=3) as pool:
        fut_a = pool.submit(_run, client, "ST-A")
        # ST-A 等待期间，ST-B 采集必须能先完成
        fut_b = pool.submit(_run, client, "ST-B")
        # 手工录入也必须即时完成
        fut_manual = pool.submit(
            client.post, "/readings",
            json=_reading("MANUAL-1", station="ST-M", temp=3.3))

        rb = fut_b.result(timeout=10)
        rm = fut_manual.result(timeout=10)
        ra = fut_a.result(timeout=15)

    assert rb.status_code == 200 and rb.json()["status"] == "collected"
    assert rm.status_code == 201
    assert ra.status_code == 200 and ra.json()["status"] == "collected"
    # 解除延迟故障，避免污染后续用例
    gw_http.post("/test/faults", json={"mode": "none"})
    manual = client.get(
        "/collections/status").json()  # 仅确认服务整体可用
    assert manual["total_batches"] == 2


def test_restart_retry_reconciles_persisted_batch(client, gw_http):
    """采集实例在回执前重启：persisted 状态已持久化，新实例首跑自动补确认。"""
    _seed(gw_http, batch_id="R1", rows=[_good_row(i) for i in range(3)])
    # 回执丢失：本地持久化成功但网关确认失败
    gw_http.post("/test/faults", json={"mode": "ack_lost"})
    r1 = _run(client)
    assert r1.status_code == 502
    gw_http.post("/test/faults", json={"mode": "none"})

    # 模拟实例重启：新建全新客户端（无任何内存状态），数据库状态是唯一依据
    fresh_client = GatewayClient(base_url=str(gw_http.base_url), timeout=5.0)
    from app.collection import run_collection
    from app.db import get_conn
    result = run_collection(get_conn, "ST-A", fresh_client)
    # 网关侧批次已被清退，对账走 410 -> already_cleared；队列空故无新批
    assert result["status"] == "no_batch"
    assert [b["batch_id"] for b in result["reconciled"]] == ["R1"]
    st = client.get("/collections/status?station_id=ST-A").json()
    assert st["acked_batches"] == 1 and st["persisted_batches"] == 0
    # 读数不丢不重
    assert _count(settings.db_path, "readings") == 3
    assert _count(settings.db_path, "collection_batches") == 1
