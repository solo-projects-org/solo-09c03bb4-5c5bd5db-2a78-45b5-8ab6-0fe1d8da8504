"""网关批次采集 HTTP 路由。"""
from __future__ import annotations

import sqlite3

from fastapi import APIRouter

from ..collection import (
    get_batch_detail,
    get_collection_status,
    list_quarantine,
    run_collection,
)
from ..db import get_conn, read_snapshot
from ..errors import APIError
from ..gateway_client import GatewayError
from ..schemas import (
    BatchSummaryOut,
    CollectionRunIn,
    CollectionRunOut,
    CollectionStatusOut,
    QuarantineListOut,
)

router = APIRouter()


@router.post("/collections/run", response_model=CollectionRunOut,
             tags=["网关采集"], summary="按网关采集一批积压温度观测")
def post_collection_run(payload: CollectionRunIn) -> CollectionRunOut:
    """一次调用处理一个批次：先对账未回执批次，再拉取、整批落库、确认网关。

    网关不可达/超时返回 502/504，本地写锁竞争失败返回 503，
    本地已落库批次保持 ``persisted``，下一次调用自动补确认，
    不漏行、不重复隔离、不误报完成。
    """
    try:
        result = run_collection(get_conn, payload.station_id)
    except GatewayError as exc:
        status = 504 if exc.code == "gateway_timeout" else 502
        raise APIError(status, exc.code, exc.message,
                       {"status_code": exc.status_code}
                       if exc.status_code is not None else None) from exc
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower():
            raise
        raise APIError(503, "database_busy",
                       "本地数据库写锁竞争超时，本次采集未完成，"
                       "数据未半批写入，请重试；网关批次不会被提前确认",
                       {"retryable": True}) from exc
    return CollectionRunOut(**result)


@router.get("/collections/status", response_model=CollectionStatusOut,
            tags=["网关采集"], summary="查询采集批次状态与未回执积压")
def get_collections_status(station_id: str | None = None) -> CollectionStatusOut:
    with get_conn() as conn, read_snapshot(conn):
        result = get_collection_status(conn, station_id)
    return CollectionStatusOut(**result)


@router.get("/collections/quarantine", response_model=QuarantineListOut,
            tags=["网关采集"], summary="查询隔离明细（可按站/批/原因过滤）")
def get_quarantine(
    station_id: str | None = None,
    batch_id: str | None = None,
    reason_code: str | None = None,
) -> QuarantineListOut:
    with get_conn() as conn, read_snapshot(conn):
        result = list_quarantine(conn, station_id, batch_id, reason_code)
    return QuarantineListOut(**result)


@router.get("/stations/{station_id}/batches/{batch_id}",
            response_model=BatchSummaryOut, tags=["网关采集"],
            summary="查询单个采集批次的接纳/隔离/回执状态")
def get_batch(station_id: str, batch_id: str) -> BatchSummaryOut:
    with get_conn() as conn, read_snapshot(conn):
        detail = get_batch_detail(conn, station_id, batch_id)
    if detail is None:
        raise APIError(404, "batch_not_found",
                       f"测站 {station_id} 不存在批次 {batch_id}",
                       {"station_id": station_id, "batch_id": batch_id})
    return BatchSummaryOut(**detail)
