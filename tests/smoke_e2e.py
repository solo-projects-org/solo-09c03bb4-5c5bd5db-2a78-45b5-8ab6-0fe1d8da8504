"""端到端冒烟：真实 HTTP 调用，覆盖故障探头追溯的完整业务链路。

用法（先启动服务）：
    BASE_URL=http://127.0.0.1:8080 python tests/smoke_e2e.py
"""
import os

import httpx

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8080")


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

print("\n全部冒烟检查通过")
