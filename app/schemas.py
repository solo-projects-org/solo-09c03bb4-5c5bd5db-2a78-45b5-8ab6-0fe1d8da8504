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
