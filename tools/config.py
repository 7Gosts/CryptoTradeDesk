#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""YAML 配置读取与安全取值（通用工具）。"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def load_yaml(path: Path) -> dict[str, Any]:
    """读取 YAML 文件；失败返回 {}。"""
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    try:
        if not path.is_file():
            return {}
        obj = yaml.safe_load(path.read_text(encoding="utf-8"))  # type: ignore
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def cfg_get(d: dict[str, Any], key_path: str, default: Any = None) -> Any:
    """按 a.b.c 路径取值。"""
    cur: Any = d
    for k in key_path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def cfg_str(d: dict[str, Any], key_path: str, default: str = "") -> str:
    v = cfg_get(d, key_path, None)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default

