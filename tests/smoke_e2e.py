"""端到端冒烟：真实 HTTP 调用，覆盖故障探头追溯与网关批次采集的完整链路。

用法（先启动服务）：
    BASE_URL=http://127.0.0.1:8080 python tests/smoke_e2e.py

网关模拟服务默认与 API 同 Compose 网络，本地直连时可用 GATEWAY_URL 覆盖，
例如：GATEWAY_URL=http://127.0.0.1:8100
"""
import os

import httpcore
import httpx

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8080")
GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8100")


class _RetryTransport(httpx.BaseTransport):
    """传输层连接重置/陈旧连接自动重试一次（与生产采集端同一容错策略）。"""

    def __init__(self) -> None:
        self._inner = httpx.HTTPTransport()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            return self._inner.handle_request(request)
        except (httpx.RemoteProtocolError, httpx.ReadError,
                httpx.WriteError, httpcore.ReadError):
            return self._inner.handle_request(request)


def check(label, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {label} {detail}")
    if not cond:
        raise SystemExit(f"冒烟失败: {label} {detail}")


with httpx.Client(base_url=BASE, timeout=10) as c:
    check("health", c.get("/health").json() == {"status": "ok"})
    openapi = c.get("/openapi.json")
    check("openapi", openapi.status_code == 200 and "paths" in openapi.json())
    docs = c.get("/docs")
    check("swagger-ui", docs.status_code == 200 and "swagger" in docs.text.lower())

    # 1) 装机：ST-A 先 P1 后 P2；ST-B 在更早时段装过 P1（不重叠的跨站换装）
    r = c.put("/stations/ST-A/installation", json={
        "expected_version": 0,
        "segments": [
            {"probe_id": "P1", "start_ts": "2026-01-01T00:00:00Z",
             "end_ts": "2026-02-01T00:00:00Z"},
            {"probe_id": "P2", "start_ts": "2026-02-01T00:00:00Z",
             "end_ts": None},
        ],
    })
    check("ST-A v1", r.status_code == 200 and r.json()["version"] == 1)

    r = c.put("/stations/ST-B/installation", json={
        "expected_version": 0,
        "segments": [
            {"probe_id": "P1", "start_ts": "2025-11-01T00:00:00Z",
             "end_ts": "2025-12-01T00:00:00Z"},
        ],
    })
    check("ST-B v1", r.status_code == 200)

    # 2) 读数：P1 期、P2 期、ST-B 上的 P1 期、异文、补传
    c.post("/readings", json={"observation_id": "O1", "station_id": "ST-A",
                              "ts_utc": "2026-01-20T10:00:00Z", "temperature": 21.1})
    c.post("/readings", json={"observation_id": "O2", "station_id": "ST-A",
                              "ts_utc": "2026-02-10T10:00:00Z", "temperature": 22.2})
    c.post("/readings", json={"observation_id": "O3", "station_id": "ST-B",
                              "ts_utc": "2025-11-10T10:00:00Z", "temperature": 19.9})
    dup = c.post("/readings", json={"observation_id": "O1", "station_id": "ST-A",
                                    "ts_utc": "2026-01-20T10:00:00Z", "temperature": 21.1})
    check("补传幂等", dup.json()["status"] == "duplicate" and dup.status_code == 200)
    clash = c.post("/readings", json={"observation_id": "O1", "station_id": "ST-A",
                                      "ts_utc": "2026-01-20T10:00:00Z", "temperature": 99.0})
    check("异文拒收", clash.status_code == 409)

    # 3) 按故障探头 P1 追溯
    res = c.get("/probes/P1/affected", params={
        "start": "2025-11-01T00:00:00Z",
        "end": "2026-03-01T00:00:00Z",
    }).json()
    attr = sorted(a["reading"]["observation_id"] for a in res["attributed_readings"])
    unattr = sorted(u["reading"]["observation_id"]
                    for u in res["unattributed_readings"])
    check("P1 归属读数", attr == ["O1", "O3"], str(attr))
    check("未覆盖不冒充", unattr == ["O2"], str(unattr))
    check("装机依据条数", len(res["install_segments"]) == 2)
    check("归属携带装机记录", all(a["install"]["probe_id"] == "P1"
                                  for a in res["attributed_readings"]))

    # 4) 追溯换装：P1 延长至 3 月，O2 立即转为归属
    r = c.put("/stations/ST-A/installation", json={
        "expected_version": 1,
        "segments": [
            {"probe_id": "P1", "start_ts": "2026-01-01T00:00:00Z",
             "end_ts": "2026-03-01T00:00:00Z"},
            {"probe_id": "P2", "start_ts": "2026-03-01T00:00:00Z",
             "end_ts": None},
        ],
    })
    check("追溯换装成功", r.status_code == 200 and r.json()["version"] == 2)
    res2 = c.get("/probes/P1/affected", params={
        "start": "2025-11-01T00:00:00Z",
        "end": "2026-03-01T00:00:00Z",
    }).json()
    attr2 = sorted(a["reading"]["observation_id"] for a in res2["attributed_readings"])
    check("换装后清单立即变化", attr2 == ["O1", "O2", "O3"], str(attr2))

    # 5) 冲突整版拒绝：同站重叠 + 双挂
    ov = c.put("/stations/ST-A/installation", json={
        "expected_version": 2,
        "segments": [
            {"probe_id": "P9", "start_ts": "2026-01-01T00:00:00Z",
             "end_ts": "2026-02-01T00:00:00Z"},
            {"probe_id": "P8", "start_ts": "2026-01-15T00:00:00Z",
             "end_ts": None},
        ],
    })
    check("同站重叠整版拒绝", ov.status_code == 409
          and ov.json()["error"]["code"] == "station_overlap")
    dm = c.put("/stations/ST-C/installation", json={
        "expected_version": 0,
        "segments": [
            {"probe_id": "P2", "start_ts": "2026-03-15T00:00:00Z",
             "end_ts": "2026-04-01T00:00:00Z"},
        ],
    })
    check("跨站双挂拒绝", dm.status_code == 409
          and dm.json()["error"]["code"] == "probe_double_mount")
    ver = c.get("/stations/ST-A/installation").json()["version"]
    check("拒绝后原表保留", ver == 2)
    check("ST-C 保持空表", c.get("/stations/ST-C/installation").json()["version"] == 0)

    # 6) 版本乐观锁
    stale = c.put("/stations/ST-A/installation", json={
        "expected_version": 1,
        "segments": [{"probe_id": "P2", "start_ts": "2026-03-01T00:00:00Z"}],
    })
    check("陈旧版本拒绝", stale.status_code == 409
          and stale.json()["error"]["code"] == "version_conflict")

    # 7) 网关批次采集：投放混合批次（合法行 + 坏 JSON + 字段非法 + 跨站 + 同号异文）
    with httpx.Client(base_url=GATEWAY, timeout=10,
                      transport=_RetryTransport()) as gw:
        check("网关健康", gw.get("/health").json()["service"] == "gateway")
        gw.post("/test/reset")
        # 先手工放一条原读数，用于验证同号异文隔离不覆盖
        c.post("/readings", json={"observation_id": "GW-CONF", "station_id": "ST-GW",
                                  "ts_utc": "2026-04-01T00:00:00Z", "temperature": 1.0})
        seed = gw.post("/test/stations/ST-GW/backlog", json={
            "batch_id": "GW-B1",
            "rows": [
                {"observation_id": "GW-1", "station_id": "ST-GW",
                 "ts_utc": "2026-04-01T00:00:00Z", "temperature": 10.1},
                {"observation_id": "GW-2", "station_id": "ST-GW",
                 "ts_utc": "2026-04-01T01:00:00Z", "temperature": 10.2},
                "this-is-not-json",
                '{"observation_id":"GW-3","station_id":"ST-GW",'
                '"ts_utc":"bad-time","temperature":10.3}',
                '{"observation_id":"GW-4","station_id":"ST-OTHER",'
                '"ts_utc":"2026-04-01T02:00:00Z","temperature":10.4}',
                {"observation_id": "GW-CONF", "station_id": "ST-GW",
                 "ts_utc": "2026-04-01T00:00:00Z", "temperature": 99.9},
            ],
        })
        check("网关投放成功", seed.status_code == 200, seed.text)

        run = c.post("/collections/run", json={"station_id": "ST-GW"})
        check("采集成功", run.status_code == 200 and run.json()["status"] == "collected")
        b = run.json()["batch"]
        check("批内行数固定", b["received_rows"] == 6)
        check("接纳 2 行", b["accepted_rows"] == 2)
        check("隔离 4 行", b["quarantined_rows"] == 4)
        check("采集后已回执", b["state"] == "acked")
        check("网关已清退",
              gw.get("/stations/ST-GW/batches/next").status_code == 204)

        # 重复确认旧批号不能误删下一批
        gw.post("/test/stations/ST-GW/backlog", json={
            "batch_id": "GW-B2",
            "rows": [{"observation_id": "GW-5", "station_id": "ST-GW",
                      "ts_utc": "2026-04-02T00:00:00Z", "temperature": 11.0}],
        })
        dup_ack = gw.post("/stations/ST-GW/batches/GW-B1/ack")
        check("重复确认被拒且不误删", dup_ack.status_code == 410
              and dup_ack.json()["error"]["code"] == "batch_not_head")

        # 回执丢失场景：网关先清退后报错，重试自动对账
        gw.post("/test/faults", json={"mode": "ack_lost"})
        lost = c.post("/collections/run", json={"station_id": "ST-GW"})
        check("回执丢失不误报完成", lost.status_code == 502
              and lost.json()["error"]["code"] == "ack_lost")
        gw.post("/test/faults", json={"mode": "none"})
        retry = c.post("/collections/run", json={"station_id": "ST-GW"})
        check("重试对账成功", retry.status_code == 200
              and [x["batch_id"] for x in retry.json()["reconciled"]] == ["GW-B2"])

    # 隔离明细可追溯：原文、来源、原因齐全
    q = c.get("/collections/quarantine",
              params={"station_id": "ST-GW"}).json()
    check("隔离明细 4 条", q["total"] == 4)
    raw_bad = next(i for i in q["items"] if i["line_no"] == 3)
    check("隔离保留原文", raw_bad["raw_content"] == "this-is-not-json"
          and raw_bad["reason_code"] == "invalid_json")
    conflict = next(i for i in q["items"]
                    if i["reason_code"] == "reading_conflict")
    check("同号异文隔离", conflict["observation_id"] == "GW-CONF")
    # 原读数未被覆盖
    keep = c.post("/readings", json={
        "observation_id": "GW-CONF", "station_id": "ST-GW",
        "ts_utc": "2026-04-01T00:00:00Z", "temperature": 1.0})
    check("原读数保持不变(幂等)", keep.json()["status"] == "duplicate")

    st = c.get("/collections/status", params={"station_id": "ST-GW"}).json()
    check("无未回执积压", st["persisted_batches"] == 0
          and st["acked_batches"] == 2)

print("\n全部冒烟检查通过")
