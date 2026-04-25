#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ai_overview.json 读写与合并（通用工具）。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AI_OVERVIEW_FILENAME = "ai_overview.json"


def load_ai_overview(session_dir: Path) -> dict[str, Any]:
    p = session_dir / AI_OVERVIEW_FILENAME
    return json.loads(p.read_text(encoding="utf-8"))


def write_ai_overview(session_dir: Path, overview: dict[str, Any]) -> None:
    p = session_dir / AI_OVERVIEW_FILENAME
    p.write_text(json.dumps(overview, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def merge_ai_overview(primary: dict[str, Any], secondary: dict[str, Any], *, note: str | None = None) -> dict[str, Any]:
    """
    合并两份 ai_overview（按 pair 合并 frames）。
    - primary: 作为基础（通常是先跑出来的周期，如 4h）
    - secondary: 后合并进来（通常是后跑的周期，如 1h）

    规则：
    - 顶层字段以 primary 为主，但会更新 generated_at_utc
    - assets 按 pair 合并 frames；若同一 interval 键冲突，则 secondary 覆盖 primary
    """
    now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    out: dict[str, Any] = dict(primary)
    out["generated_at_utc"] = now_utc
    out["note"] = note or "merged ai_overview: primary + secondary intervals"

    def idx_assets(ov: dict[str, Any]) -> dict[str, dict[str, Any]]:
        m: dict[str, dict[str, Any]] = {}
        for a in ov.get("assets") or []:
            if not isinstance(a, dict):
                continue
            pair = str(a.get("pair") or "").strip()
            if pair:
                m[pair] = a
        return m

    p_map = idx_assets(primary)
    s_map = idx_assets(secondary)
    all_pairs = sorted(set(p_map.keys()) | set(s_map.keys()))
    merged_assets: list[dict[str, Any]] = []

    for pair in all_pairs:
        a_p = p_map.get(pair) or {}
        a_s = s_map.get(pair) or {}
        frames: dict[str, Any] = {}
        fr_p = a_p.get("frames") if isinstance(a_p.get("frames"), dict) else {}
        fr_s = a_s.get("frames") if isinstance(a_s.get("frames"), dict) else {}
        if isinstance(fr_p, dict):
            frames.update(fr_p)
        if isinstance(fr_s, dict):
            frames.update(fr_s)  # secondary wins on key conflicts
        asset_name = str(a_s.get("asset") or a_p.get("asset") or pair)
        merged_assets.append({"pair": pair, "asset": asset_name, "frames": frames})

    out["assets"] = merged_assets
    return out

