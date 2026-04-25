#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""时间解析/时区格式化（通用工具）。"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def parse_iso_utc(ts: str | None) -> datetime | None:
    """解析 ISO 时间戳并归一到 UTC；失败返回 None。"""
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_local(dt: datetime | None, tz: str = "Asia/Shanghai", fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """把 datetime 格式化成指定时区字符串。dt=None 返回 '—'。"""
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        z = ZoneInfo(tz)
    except Exception:
        z = ZoneInfo("Asia/Shanghai")
    return dt.astimezone(z).strftime(fmt)


def fmt_from_iso(ts: str | None, tz: str = "Asia/Shanghai", fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """ISO -> datetime(UTC) -> 本地时区字符串。解析失败返回原字符串或 '—'。"""
    if not ts:
        return "—"
    s = str(ts).strip()
    if not s:
        return "—"
    dt = parse_iso_utc(s)
    if dt is None:
        return s
    return fmt_local(dt, tz=tz, fmt=fmt)

