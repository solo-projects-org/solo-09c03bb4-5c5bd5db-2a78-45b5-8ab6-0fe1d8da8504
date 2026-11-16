"""领域逻辑：读数入库、装机时段整版替换、按探头追溯受影响读数。

所有写操作均在单个 IMMEDIATE 事务内完成“校验 + 落库”，冲突整版拒绝；
查询在单一 DEFERRED 快照内完成，保证读数与装机依据来自同一已提交状态。
"""
from __future__ import annotations

import sqlite3

from .errors import APIError
from .schemas import TS_FAR_FUTURE, InstallationSubmission, ReadingIn


def _segments_overlap(a_start: str, a_end: str | None,
                      b_start: str, b_end: str | None) -> bool:
    """半开区间 [start, end) 是否相交。NULL end 视为开放区间。"""
    a_e = a_end if a_end is not None else TS_FAR_FUTURE
    b_e = b_end if b_end is not None else TS_FAR_FUTURE
    return a_start < b_e and b_start < a_e


# ----------------------------- 读数 -----------------------------

def submit_reading(conn: sqlite3.Connection, payload: ReadingIn) -> tuple[str, dict]:
    """同号同值补传幂等；同号异文拒收。返回 (status, reading)。"""
    data = payload.normalized()
    # 调用方已开启 IMMEDIATE 事务，本函数只做校验与写入，错误由外层统一回滚
    row = conn.execute(
        "SELECT observation_id, station_id, ts_utc, temperature "
        "FROM readings WHERE observation_id = ?",
        (data["observation_id"],),
    ).fetchone()
    if row is not None:
        existing = dict(row)
        if (
            existing["station_id"] == data["station_id"]
            and existing["ts_utc"] == data["ts_utc"]
            and existing["temperature"] == data["temperature"]
        ):
            return "duplicate", existing
        raise APIError(
            409,
            "reading_conflict",
            f"观测号 {data['observation_id']} 已存在且内容不同，异文补传拒收",
            {"existing": existing, "received": data},
        )
    conn.execute(
        "INSERT INTO readings (observation_id, station_id, ts_utc, temperature) "
        "VALUES (:observation_id, :station_id, :ts_utc, :temperature)",
        data,
    )
    return "accepted", data


# ----------------------------- 装机 -----------------------------

def replace_installation(conn: sqlite3.Connection, station_id: str,
                         submission: InstallationSubmission) -> dict:
    """整版替换某站装机表，冲突时抛 APIError 且不改动任何数据。"""
    segments = sorted(
        (s.normalized() for s in submission.segments),
        key=lambda s: (s["start_ts"], s["probe_id"]),
    )

    # 1) 每段自身必须是合法半开区间
    for seg in segments:
        if seg["end_ts"] is not None and not seg["start_ts"] < seg["end_ts"]:
            raise APIError(
                400, "invalid_interval",
                f"探头 {seg['probe_id']} 的时段起止非法：start 必须严格小于 end",
                seg,
            )

    # 2) 同站装机不得重叠（含两段同时开放的情况）
    for i in range(len(segments)):
        for j in range(i + 1, len(segments)):
            a, b = segments[i], segments[j]
            if _segments_overlap(a["start_ts"], a["end_ts"],
                                 b["start_ts"], b["end_ts"]):
                raise APIError(
                    409, "station_overlap",
                    f"测站 {station_id} 内装机时段重叠，整版拒绝",
                    {"segment_a": a, "segment_b": b},
                )

    # 3) 乐观版本检查 + 4) 跨站探头冲突检查 + 5) 整版替换，全部同事务
    row = conn.execute(
        "SELECT version FROM stations WHERE station_id = ?", (station_id,)
    ).fetchone()
    current_version = row["version"] if row is not None else 0
    if current_version != submission.expected_version:
        raise APIError(
            409, "version_conflict",
            f"版本冲突：期望 {submission.expected_version}，当前 {current_version}",
            {"expected": submission.expected_version, "current": current_version},
        )

    for seg in segments:
        end = seg["end_ts"] if seg["end_ts"] is not None else TS_FAR_FUTURE
        # 只检查“其他测站”；本站旧段即将被整版删除，同站重叠已在上面拦截
        conflicts = conn.execute(
            """
            SELECT station_id, probe_id, start_ts, end_ts
              FROM install_segments
             WHERE probe_id = ?
               AND station_id <> ?
               AND start_ts < ?
               AND COALESCE(end_ts, ?) > ?
            """,
            (seg["probe_id"], station_id, end, TS_FAR_FUTURE, seg["start_ts"]),
        ).fetchall()
        if conflicts:
            raise APIError(
                409, "probe_double_mount",
                f"探头 {seg['probe_id']} 在重叠时段已挂载于其他测站，整版拒绝",
                {"rejected_segment": seg,
                 "conflicts": [dict(c) for c in conflicts]},
            )

    new_version = current_version + 1
    if row is None:
        conn.execute(
            "INSERT INTO stations (station_id, version) VALUES (?, 1)",
            (station_id,),
        )
    else:
        conn.execute(
            "UPDATE stations SET version = ? WHERE station_id = ?",
            (new_version, station_id),
        )
    conn.execute(
        "DELETE FROM install_segments WHERE station_id = ?", (station_id,)
    )
    conn.executemany(
        """
        INSERT INTO install_segments
            (station_id, probe_id, start_ts, end_ts, version_from)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(station_id, s["probe_id"], s["start_ts"], s["end_ts"], new_version)
         for s in segments],
    )
    return {"station_id": station_id, "version": new_version, "segments": segments}


def get_installation(conn: sqlite3.Connection, station_id: str) -> dict:
    row = conn.execute(
        "SELECT version FROM stations WHERE station_id = ?", (station_id,)
    ).fetchone()
    version = row["version"] if row is not None else 0
    seg_rows = conn.execute(
        "SELECT probe_id, start_ts, end_ts FROM install_segments "
        "WHERE station_id = ? ORDER BY start_ts, probe_id",
        (station_id,),
    ).fetchall()
    return {
        "station_id": station_id,
        "version": version,
        "segments": [dict(r) for r in seg_rows],
    }


# ----------------------------- 受影响查询 -----------------------------

def find_affected(conn: sqlite3.Connection, probe_id: str,
                  window_start: str, window_end: str | None) -> dict:
    """在同一快照内返回探头在窗口内的装机段、归属读数与未覆盖读数。"""
    win_end = window_end if window_end is not None else TS_FAR_FUTURE

    # 与查询窗口半开相交的装机段（作为归属依据返回）
    seg_rows = conn.execute(
        """
        SELECT station_id, probe_id, start_ts, end_ts
          FROM install_segments
         WHERE probe_id = ?
           AND start_ts < ?
           AND COALESCE(end_ts, ?) > ?
         ORDER BY start_ts
        """,
        (probe_id, win_end, TS_FAR_FUTURE, window_start),
    ).fetchall()
    install_segments = [dict(r) for r in seg_rows]

    # 候选站：该探头“曾经”挂载过的所有站（不局限于窗口相交段），
    # 这样窗口内这些站上未被装机段覆盖的读数也会显式进入未归属清单，
    # 而不是被静默忽略，杜绝冒充归属。
    station_rows = conn.execute(
        "SELECT DISTINCT station_id FROM install_segments WHERE probe_id = ?",
        (probe_id,),
    ).fetchall()
    stations = [r["station_id"] for r in station_rows]

    attributed: list[dict] = []
    unattributed: list[dict] = []

    if stations:
        placeholders = ",".join("?" for _ in stations)
        reading_rows = conn.execute(
            f"""
            SELECT observation_id, station_id, ts_utc, temperature
              FROM readings
             WHERE station_id IN ({placeholders})
               AND ts_utc >= ?
               AND ts_utc < ?
             ORDER BY ts_utc, observation_id
            """,
            (*stations, window_start, win_end),
        ).fetchall()

        for r in reading_rows:
            reading = dict(r)
            match = conn.execute(
                """
                SELECT probe_id, start_ts, end_ts
                  FROM install_segments
                 WHERE probe_id = ?
                   AND station_id = ?
                   AND start_ts <= ?
                   AND COALESCE(end_ts, ?) > ?
                """,
                (probe_id, reading["station_id"], reading["ts_utc"],
                 TS_FAR_FUTURE, reading["ts_utc"]),
            ).fetchone()
            if match is not None:
                attributed.append({"reading": reading, "install": dict(match)})
            else:
                unattributed.append({
                    "reading": reading,
                    "reason": "该时刻探头未在此测站装机，读数不得冒充归属",
                })

    return {
        "probe_id": probe_id,
        "window_start": window_start,
        "window_end": window_end,
        "install_segments": install_segments,
        "attributed_readings": attributed,
        "unattributed_readings": unattributed,
    }
