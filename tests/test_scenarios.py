"""关键业务场景自测（TestClient，每例独立临时库）。"""


def _reading(oid, station, ts, temp):
    return {"observation_id": oid, "station_id": station,
            "ts_utc": ts, "temperature": temp}


def _seg(probe, start, end=None):
    return {"probe_id": probe, "start_ts": start, "end_ts": end}


# ----------------------- 读数：补传 / 异文 -----------------------

def test_reading_accept_then_idempotent_retransmit(client):
    r1 = client.post("/readings", json=_reading("OBS-1", "ST-A",
                                                "2026-01-01T00:00:00Z", 21.5))
    assert r1.status_code == 201 and r1.json()["status"] == "accepted"

    # 同号同值补传：不重复入库
    r2 = client.post("/readings", json=_reading("OBS-1", "ST-A",
                                                "2026-01-01T00:00:00Z", 21.5))
    assert r2.status_code == 200 and r2.json()["status"] == "duplicate"

    # 不同时区写法表示同一 UTC 时刻，也算同值
    r3 = client.post("/readings", json=_reading(
        "OBS-1", "ST-A", "2026-01-01T08:00:00+08:00", 21.5))
    assert r3.status_code == 200 and r3.json()["status"] == "duplicate"


def test_reading_same_id_different_payload_rejected(client):
    client.post("/readings", json=_reading("OBS-2", "ST-A",
                                           "2026-01-01T00:00:00Z", 21.5))
    r = client.post("/readings", json=_reading("OBS-2", "ST-A",
                                               "2026-01-01T00:00:00Z", 99.9))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "reading_conflict"

    # 原读数未被覆盖
    affected = client.get("/probes/P1/affected",
                          params={"start": "2026-01-01T00:00:00Z",
                                  "end": "2026-01-02T00:00:00Z"}).json()
    # 尚无装机，读数应在未归属清单而不是被冒充
    assert affected["unattributed_readings"] == []  # 该站从未装过 P1，不纳入探头视角


# ----------------------- 装机：版本 / 重叠 / 双挂 -----------------------

def test_installation_first_submission_and_version_bump(client):
    r = client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [_seg("P1", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")],
    })
    assert r.status_code == 200 and r.json()["version"] == 1

    # 再用旧版本提交必须冲突
    r2 = client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [_seg("P1", "2026-01-01T00:00:00Z")],
    })
    assert r2.status_code == 409 and r2.json()["error"]["code"] == "version_conflict"

    r3 = client.put("/stations/ST-A/installation", json={
        "expected_version": 1,
        "segments": [_seg("P1", "2026-01-01T00:00:00Z")],
    })
    assert r3.status_code == 200 and r3.json()["version"] == 2
    assert r3.json()["segments"][0]["end_ts"] is None  # 开放区间


def test_station_internal_overlap_whole_version_rejected(client):
    r = client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [
            _seg("P1", "2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z"),
            _seg("P2", "2026-02-01T00:00:00Z", "2026-04-01T00:00:00Z"),
        ],
    })
    assert r.status_code == 409 and r.json()["error"]["code"] == "station_overlap"
    # 原表（空表/版本 0）保留
    got = client.get("/stations/ST-A/installation").json()
    assert got["version"] == 0 and got["segments"] == []


def test_half_open_intervals_may_touch(client):
    # 半开：前一段 end == 后一段 start 不算重叠
    r = client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [
            _seg("P1", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"),
            _seg("P2", "2026-02-01T00:00:00Z", "2026-03-01T00:00:00Z"),
        ],
    })
    assert r.status_code == 200 and r.json()["version"] == 1


def test_probe_cannot_mount_on_two_stations_concurrently(client):
    client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [_seg("P1", "2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z")],
    })
    r = client.put("/stations/ST-B/installation", json={
        "expected_version": 0,
        "segments": [_seg("P1", "2026-02-01T00:00:00Z", "2026-04-01T00:00:00Z")],
    })
    assert r.status_code == 409 and r.json()["error"]["code"] == "probe_double_mount"

    # ST-B 原表保持空；ST-A 原表不动
    assert client.get("/stations/ST-B/installation").json()["segments"] == []
    assert len(client.get("/stations/ST-A/installation").json()["segments"]) == 1

    # 不重叠的跨站换装（拆下后再装）应允许
    ok = client.put("/stations/ST-B/installation", json={
        "expected_version": 0,
        "segments": [_seg("P1", "2026-03-01T00:00:00Z", "2026-05-01T00:00:00Z")],
    })
    assert ok.status_code == 200


def test_invalid_interval_rejected(client):
    r = client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [_seg("P1", "2026-03-01T00:00:00Z", "2026-01-01T00:00:00Z")],
    })
    assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_interval"


# ----------------------- 追溯：归属 / 未覆盖 / 迟到 -----------------------

def _install_two_probes_swap(client):
    # ST-A：P1 [01-01, 02-01)，换装 P2 [02-01, 开放)
    client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [
            _seg("P1", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"),
            _seg("P2", "2026-02-01T00:00:00Z"),
        ],
    })


def test_affected_attribution_and_uncovered_readings(client):
    _install_two_probes_swap(client)
    # P1 装机期内的读数
    client.post("/readings", json=_reading("O-1", "ST-A",
                                           "2026-01-15T12:00:00Z", 10.0))
    # 换装后（P2 期）的读数：按 P1 追溯时不得冒充归属
    client.post("/readings", json=_reading("O-2", "ST-A",
                                           "2026-02-15T12:00:00Z", 11.0))
    # 边界：恰好 02-01T00:00:00 属于 P2（半开）
    client.post("/readings", json=_reading("O-3", "ST-A",
                                           "2026-02-01T00:00:00Z", 12.0))

    res = client.get("/probes/P1/affected", params={
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-03-01T00:00:00Z",
    }).json()

    attributed = [a["reading"]["observation_id"] for a in res["attributed_readings"]]
    unattributed = [u["reading"]["observation_id"]
                    for u in res["unattributed_readings"]]
    assert attributed == ["O-1"]
    assert sorted(unattributed) == ["O-2", "O-3"]
    assert len(res["install_segments"]) == 1
    assert res["install_segments"][0]["probe_id"] == "P1"
    # 归属读数必须带对应装机记录
    assert res["attributed_readings"][0]["install"]["start_ts"].startswith("2026-01-01")


def test_late_reading_immediately_reflected(client):
    _install_two_probes_swap(client)
    # 先查询：空
    before = client.get("/probes/P1/affected", params={
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-02-01T00:00:00Z",
    }).json()
    assert before["attributed_readings"] == []

    # 迟到读数后立即出现
    client.post("/readings", json=_reading("LATE-1", "ST-A",
                                           "2026-01-20T08:00:00Z", 9.5))
    after = client.get("/probes/P1/affected", params={
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-02-01T00:00:00Z",
    }).json()
    assert [a["reading"]["observation_id"]
            for a in after["attributed_readings"]] == ["LATE-1"]


def test_retroactive_reinstall_changes_listing(client):
    _install_two_probes_swap(client)
    client.post("/readings", json=_reading("R-1", "ST-A",
                                           "2026-02-15T12:00:00Z", 11.0))
    # 初始：R-1 对 P1 未归属
    res1 = client.get("/probes/P1/affected", params={
        "start": "2026-01-01T00:00:00Z", "end": "2026-03-01T00:00:00Z"}).json()
    assert [u["reading"]["observation_id"]
            for u in res1["unattributed_readings"]] == ["R-1"]

    # 追溯换装：把 P1 的装机延长覆盖 2 月（整版替换，版本 1 -> 2）
    r = client.put("/stations/ST-A/installation", json={
        "expected_version": 1,
        "segments": [
            _seg("P1", "2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z"),
            _seg("P2", "2026-03-01T00:00:00Z"),
        ],
    })
    assert r.status_code == 200
    res2 = client.get("/probes/P1/affected", params={
        "start": "2026-01-01T00:00:00Z", "end": "2026-03-01T00:00:00Z"}).json()
    assert [a["reading"]["observation_id"]
            for a in res2["attributed_readings"]] == ["R-1"]
    assert res2["unattributed_readings"] == []


def test_open_ended_window_and_probe_never_at_station(client):
    _install_two_probes_swap(client)
    client.post("/readings", json=_reading("O-1", "ST-A",
                                           "2026-01-15T12:00:00Z", 10.0))
    # 另一座从未装过 P1 的站
    client.put("/stations/ST-X/installation", json={
        "expected_version": 0,
        "segments": [_seg("P9", "2026-01-01T00:00:00Z")],
    })
    client.post("/readings", json=_reading("O-X", "ST-X",
                                           "2026-01-15T12:00:00Z", 10.0))

    res = client.get("/probes/P1/affected",
                     params={"start": "2026-01-01T00:00:00Z"}).json()
    ids = [a["reading"]["observation_id"] for a in res["attributed_readings"]]
    assert ids == ["O-1"]  # ST-X 的读数不会混入
    assert res["window_end"] is None


def test_uncovered_reading_at_station_probe_left_before_window(client):
    """探头在窗口开始前就已离开该站：窗口内该站读数显式列入未归属。"""
    client.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [_seg("P1", "2026-01-01T00:00:00Z",
                          "2026-02-01T00:00:00Z")],
    })
    client.post("/readings", json=_reading("LATE-2", "ST-A",
                                           "2026-02-15T12:00:00Z", 8.8))
    res = client.get("/probes/P1/affected", params={
        "start": "2026-02-01T00:00:00Z",
        "end": "2026-03-01T00:00:00Z",
    }).json()
    assert res["install_segments"] == []  # 窗口内无相交装机段
    assert [u["reading"]["observation_id"]
            for u in res["unattributed_readings"]] == ["LATE-2"]
    assert res["attributed_readings"] == []


def test_input_validation(client):
    # 缺字段
    r = client.post("/readings", json={"observation_id": "X"})
    assert r.status_code == 422
    # NaN / Infinity 拒收（非标准 JSON 字面量，用原始报文送达）
    headers = {"content-type": "application/json"}
    r2 = client.post(
        "/readings",
        content='{"observation_id":"O-NAN","station_id":"ST-A",'
                '"ts_utc":"2026-01-01T00:00:00Z","temperature":NaN}',
        headers=headers,
    )
    assert r2.status_code == 422
    r2b = client.post(
        "/readings",
        content='{"observation_id":"O-INF","station_id":"ST-A",'
                '"ts_utc":"2026-01-01T00:00:00Z","temperature":Infinity}',
        headers=headers,
    )
    assert r2b.status_code == 422
    # 无时区时刻拒收
    r3 = client.post("/readings", json={
        "observation_id": "O-NAIVE", "station_id": "ST-A",
        "ts_utc": "2026-01-01T00:00:00", "temperature": 1.0})
    assert r3.status_code == 422
    # 查询参数缺时区同样拒收
    r3b = client.get("/probes/P1/affected",
                     params={"start": "2026-01-01T00:00:00"})
    assert r3b.status_code == 422
    # 查询区间倒置
    r4 = client.get("/probes/P1/affected", params={
        "start": "2026-03-01T00:00:00Z",
        "end": "2026-01-01T00:00:00Z"})
    assert r4.status_code == 400
