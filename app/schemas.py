"""Pydantic 输入/输出模型。

时间统一规范为定宽 UTC 文本（YYYY-MM-DDTHH:MM:SS.ffffff+00:00，32 字符），
字典序与时间序一致，因此可直接用字符串比较完成半开区间判断。
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

TS_FMT = "%Y-%m-%dT%H:%M:%S.%f%z"
# 开放区间上界的哨兵值：大于任何合法定宽时间文本
TS_FAR_FUTURE = "9999-12-31T23:59:59.999999+00:00"


def utcnow_text() -> str:
    """当前 UTC 时刻的定宽文本，与读数时刻的存储格式完全一致。"""
    return datetime.now(timezone.utc).strftime(TS_FMT)


def normalize_ts(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("时刻必须带时区偏移（建议直接使用 UTC，例如 2026-01-01T00:00:00Z）")
    return value.astimezone(timezone.utc).strftime(TS_FMT)


def _check_tz(v: datetime) -> datetime:
    if v.tzinfo is None or v.utcoffset() is None:
        raise ValueError("时刻必须带时区偏移（建议直接使用 UTC，例如 2026-01-01T00:00:00Z）")
    return v


class ReadingIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observation_id: str = Field(min_length=1, max_length=128)
    station_id: str = Field(min_length=1, max_length=64)
    ts_utc: datetime
    temperature: float

    @field_validator("ts_utc")
    @classmethod
    def _tz(cls, v: datetime) -> datetime:
        return _check_tz(v)

    @field_validator("temperature")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("温度必须是有限数值")
        return v

    def normalized(self) -> dict:
        return {
            "observation_id": self.observation_id,
            "station_id": self.station_id,
            "ts_utc": normalize_ts(self.ts_utc),
            "temperature": self.temperature,
        }


class SegmentIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    probe_id: str = Field(min_length=1, max_length=64)
    start_ts: datetime
    end_ts: datetime | None = None  # None = 开放区间，探头至今在站

    @field_validator("start_ts", "end_ts")
    @classmethod
    def _tz(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _check_tz(v)

    def normalized(self) -> dict:
        return {
            "probe_id": self.probe_id,
            "start_ts": normalize_ts(self.start_ts),
            "end_ts": None if self.end_ts is None else normalize_ts(self.end_ts),
        }


class InstallationSubmission(BaseModel):
    """某测站的完整装机时段表（整版替换语义）。"""

    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=0)
    segments: list[SegmentIn]


class ReadingOut(BaseModel):
    observation_id: str
    station_id: str
    ts_utc: str
    temperature: float


class ReadingAck(BaseModel):
    status: str  # accepted | duplicate
    reading: ReadingOut


class SegmentOut(BaseModel):
    probe_id: str
    start_ts: str
    end_ts: str | None


class InstallationOut(BaseModel):
    station_id: str
    version: int
    segments: list[SegmentOut]


class AttributedReadingOut(BaseModel):
    reading: ReadingOut
    install: SegmentOut


class UnattributedReadingOut(BaseModel):
    reading: ReadingOut
    reason: str


class AffectedQuery(BaseModel):
    """GET /probes/{probe_id}/affected 的查询参数。"""

    model_config = ConfigDict(extra="forbid")

    start: datetime
    end: datetime | None = None

    @field_validator("start", "end")
    @classmethod
    def _tz(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _check_tz(v)


class AffectedResponse(BaseModel):
    probe_id: str
    window_start: str
    window_end: str | None
    install_segments: list[SegmentOut]
    attributed_readings: list[AttributedReadingOut]
    unattributed_readings: list[UnattributedReadingOut]


# ============================= 网关采集 =============================

class CollectionRunIn(BaseModel):
    """触发一次按网关采集（一次调用至多处理一个批次）。"""

    model_config = ConfigDict(extra="forbid")

    station_id: str = Field(min_length=1, max_length=64)


class BatchSummaryOut(BaseModel):
    station_id: str
    batch_id: str
    state: str  # persisted | acked
    received_rows: int
    accepted_rows: int
    quarantined_rows: int
    persisted_at: str
    acked_at: str | None = None


class CollectionRunOut(BaseModel):
    station_id: str
    # no_batch：网关积压已空；collected：本次新落库一批；
    # already_persisted：批次此前已落库（重试/重复采集），本次仅做回执对账
    status: str
    batch: BatchSummaryOut | None = None
    # 本次采集前对历史“已落库未回执”批次的对账结果
    reconciled: list[BatchSummaryOut] = []


class CollectionStatusOut(BaseModel):
    station_id: str | None = None
    total_batches: int
    persisted_batches: int   # 已落库、等待网关回执
    acked_batches: int       # 网关已清退
    total_received: int
    total_accepted: int
    total_quarantined: int
    pending_ack: list[BatchSummaryOut]
    batches: list[BatchSummaryOut]


class QuarantineRowOut(BaseModel):
    station_id: str
    batch_id: str
    line_no: int
    observation_id: str | None
    raw_content: str
    reason_code: str
    reason_message: str
    created_at: str


class QuarantineListOut(BaseModel):
    station_id: str | None = None
    batch_id: str | None = None
    reason_code: str | None = None
    total: int
    items: list[QuarantineRowOut]
