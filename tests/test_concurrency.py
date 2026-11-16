"""多实例交错场景：真实 uvicorn + 多线程，SQLite 文件库 WAL 模式。"""
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

from app.config import settings


def _seg(probe, start, end=None):
    return {"probe_id": probe, "start_ts": start, "end_ts": end}


def test_concurrent_installation_exactly_one_wins(live_server):
    http = live_server
    # 两个“实例”同时以 expected_version=0 提交同一站的不同装机表
    payload_a = {"expected_version": 0,
                 "segments": [_seg("PA", "2026-01-01T00:00:00Z")]}
    payload_b = {"expected_version": 0,
                 "segments": [_seg("PB", "2026-01-01T00:00:00Z")]}

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda p: http.put("/stations/ST-C/installation", json=p),
            [payload_a, payload_b]))

    statuses = sorted(r.status_code for r in results)
    assert statuses == [200, 409]
    body = http.get("/stations/ST-C/installation").json()
    assert body["version"] == 1  # 只成功一次，无半版
    assert len(body["segments"]) == 1
    assert body["segments"][0]["probe_id"] in {"PA", "PB"}


def test_concurrent_cross_station_double_mount_one_rejected(live_server):
    http = live_server
    # 同一探头同时被挂到两座站，重叠时段必须恰好一胜一负
    def mount(station):
        return http.put(f"/stations/{station}/installation", json={
            "expected_version": 0,
            "segments": [_seg("PDUAL", "2027-01-01T00:00:00Z",
                              "2027-06-01T00:00:00Z")],
        })

    with ThreadPoolExecutor(max_workers=2) as pool:
        r1, r2 = list(pool.map(mount, ["ST-D1", "ST-D2"]))

    assert sorted((r1.status_code, r2.status_code)) == [200, 409]
    winner = http.get("/stations/ST-D1/installation").json()
    loser = http.get("/stations/ST-D2/installation").json()
    mounted = [s for s in winner["segments"] + loser["segments"]
               if s["probe_id"] == "PDUAL"]
    assert len(mounted) == 1  # 探头只挂在一座站


def test_concurrent_readings_same_id(live_server):
    http = live_server
    payload = {"observation_id": "OBS-CONC", "station_id": "ST-E",
               "ts_utc": "2026-05-01T00:00:00Z", "temperature": 5.5}
    different = {**payload, "temperature": 9.9}

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(
            lambda p: http.post("/readings", json=p),
            [payload, payload, different, payload]))

    codes = sorted(r.status_code for r in results)
    # 同值请求：1 个 201，其余 200；异文请求：409
    assert codes == sorted([201, 200, 200, 409])

    # 库中只有一条且保持原值
    conn = sqlite3.connect(settings.db_path)
    try:
        row = conn.execute(
            "SELECT temperature FROM readings WHERE observation_id = ?",
            ("OBS-CONC",)).fetchone()
    finally:
        conn.close()
    assert row == (5.5,)


def test_lock_retry_succeeds_when_other_writer_commits(live_server):
    """外部连接持有写锁时，API 写请求在锁释放后经重试成功，不向用户报错。"""
    http = live_server
    blocker = sqlite3.connect(settings.db_path, isolation_level=None,
                              check_same_thread=False)
    try:
        blocker.execute("PRAGMA busy_timeout = 0")
        blocker.execute("BEGIN IMMEDIATE")  # 抢写锁

        def release_after_delay():
            time.sleep(0.5)
            blocker.execute("COMMIT")

        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_rel = pool.submit(release_after_delay)
            fut_req = pool.submit(
                http.post, "/readings",
                json={"observation_id": "OBS-LOCK", "station_id": "ST-F",
                      "ts_utc": "2026-06-01T00:00:00Z", "temperature": 7.0})
            fut_rel.result()
            resp = fut_req.result()

        assert resp.status_code == 201
    finally:
        blocker.close()


def test_snapshot_consistency_during_write(live_server):
    """查询在单一快照内完成：写事务未提交前，读请求只看到旧的已提交状态。"""
    http = live_server
    # 先建立基线装机
    r = http.put("/stations/ST-G/installation", json={
        "expected_version": 0,
        "segments": [_seg("PSNAP", "2026-01-01T00:00:00Z",
                          "2026-02-01T00:00:00Z")],
    })
    assert r.status_code == 200

    blocker = sqlite3.connect(settings.db_path, isolation_level=None,
                              check_same_thread=False)
    try:
        blocker.execute("PRAGMA busy_timeout = 0")
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute(
            "UPDATE stations SET version = 99 WHERE station_id = ?", ("ST-G",))

        # 未提交期间，API 读到的仍是已提交版本 1（WAL 读写并发，不阻塞）
        seen = http.get("/stations/ST-G/installation").json()
        assert seen["version"] == 1
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    after = http.get("/stations/ST-G/installation").json()
    assert after["version"] == 1  # 回滚后无痕迹


def test_failed_submission_leaves_no_partial_segments(live_server):
    http = live_server
    # ST-H1 已占探头 PX；ST-H2 整版中含合法段与冲突段，必须整体拒绝
    http.put("/stations/ST-H1/installation", json={
        "expected_version": 0,
        "segments": [_seg("PX", "2028-01-01T00:00:00Z",
                          "2028-12-31T00:00:00Z")],
    })
    r = http.put("/stations/ST-H2/installation", json={
        "expected_version": 0,
        "segments": [
            _seg("POK", "2028-01-01T00:00:00Z", "2028-03-01T00:00:00Z"),
            _seg("PX", "2028-02-01T00:00:00Z", "2028-05-01T00:00:00Z"),
        ],
    })
    assert r.status_code == 409
    body = http.get("/stations/ST-H2/installation").json()
    assert body["version"] == 0 and body["segments"] == []  # 连合法段也没留下
