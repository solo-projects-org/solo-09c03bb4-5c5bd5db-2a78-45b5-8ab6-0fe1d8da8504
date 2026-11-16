"""读数与装机管理 HTTP 路由。"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Response
from pydantic import ValidationError

from ..db import get_conn, read_snapshot, write_tx
from ..errors import APIError
from ..schemas import (
    AffectedQuery,
    AffectedResponse,
    InstallationOut,
    InstallationSubmission,
    ReadingAck,
    ReadingIn,
    normalize_ts,
)
from ..service import find_affected, get_installation, replace_installation, submit_reading

router = APIRouter()


def affected_query(start: str, end: str | None = None) -> AffectedQuery:
    """把查询参数解析收敛为 RequestValidationError 风格的 422。"""
    try:
        return AffectedQuery(start=start, end=end)
    except ValidationError as exc:
        from fastapi.exceptions import RequestValidationError
        raise RequestValidationError(exc.errors()) from exc


@router.post("/readings", response_model=ReadingAck, tags=["读数"],
             summary="提交一条原始观测读数")
def post_reading(payload: ReadingIn, response: Response) -> ReadingAck:
    """同观测号同内容的补传幂等返回 duplicate；同号异文以 409 拒收。"""
    with get_conn() as conn, write_tx(conn):
        status, reading = submit_reading(conn, payload)
    response.status_code = 200 if status == "duplicate" else 201
    return ReadingAck(status=status, reading=reading)


@router.put("/stations/{station_id}/installation",
            response_model=InstallationOut, tags=["装机时段"],
            summary="整版提交某测站的完整装机时段表（乐观并发）")
def put_installation(station_id: str, submission: InstallationSubmission) -> InstallationOut:
    """携带 expected_version；重叠/双挂/版本冲突一律整版拒绝，原表保留。"""
    with get_conn() as conn, write_tx(conn):
        result = replace_installation(conn, station_id, submission)
    return InstallationOut(**result)


@router.get("/stations/{station_id}/installation",
            response_model=InstallationOut, tags=["装机时段"],
            summary="查询某测站当前装机时段表与版本号")
def read_installation(station_id: str) -> InstallationOut:
    with get_conn() as conn, read_snapshot(conn):
        result = get_installation(conn, station_id)
    return InstallationOut(**result)


@router.get("/probes/{probe_id}/affected",
            response_model=AffectedResponse, tags=["受影响追溯"],
            summary="按探头与 UTC 区间查询受影响读数及装机依据")
def get_affected(
    probe_id: str,
    query: Annotated[AffectedQuery, Depends(affected_query)],
) -> AffectedResponse:
    """读数与装机记录取自同一快照；未覆盖时段的读数进入 unattributed 清单。"""
    start_s = normalize_ts(query.start)
    end_s = None if query.end is None else normalize_ts(query.end)
    if end_s is not None and not start_s < end_s:
        raise APIError(400, "invalid_interval",
                       "查询区间非法：start 必须严格小于 end")
    with get_conn() as conn, read_snapshot(conn):
        result = find_affected(conn, probe_id, start_s, end_s)
    return AffectedResponse(**result)
