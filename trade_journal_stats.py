#!/usr/bin/env python3
"""
交易台账统计脚本（周/月）。

示例:
  python3 trade_journal_stats.py
  python3 trade_journal_stats.py --journal output/trade_journal.jsonl --json
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

JOURNAL_DISPLAY_TZ_ENV = "CRYPTO_TRADEDESK_DISPLAY_TZ"


from tools.time_utils import parse_iso_utc


def load_journal(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def safe_pct(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round(num / den * 100.0, 2)


def period_stats(entries: list[dict[str, Any]], *, now_utc: datetime, days: int) -> dict[str, Any]:
    start = now_utc - timedelta(days=days)
    scoped: list[dict[str, Any]] = []
    for e in entries:
        created = parse_iso_utc(str(e.get("created_at_utc") or ""))
        if created is None:
            continue
        if start <= created <= now_utc:
            scoped.append(e)

    total = len(scoped)
    hit = 0
    tp = 0
    sl = 0
    wins: list[float] = []
    losses: list[float] = []
    for e in scoped:
        status = str(e.get("status") or "")
        if status in {"filled", "closed"} or e.get("filled_at_utc"):
            hit += 1
        ex = str(e.get("exit_status") or "")
        if ex == "tp":
            tp += 1
        elif ex == "sl":
            sl += 1
        rp = e.get("realized_pnl_pct")
        if isinstance(rp, (int, float)):
            rv = float(rp)
            if rv > 0:
                wins.append(rv)
            elif rv < 0:
                losses.append(rv)

    closed_ts = tp + sl
    avg_win = (sum(wins) / len(wins)) if wins else None
    avg_loss_abs = (abs(sum(losses) / len(losses))) if losses else None
    avg_rr = None
    if avg_win is not None and avg_loss_abs and avg_loss_abs > 1e-12:
        avg_rr = round(avg_win / avg_loss_abs, 3)
    return {
        "days": days,
        "candidate_total": total,
        "hit_count": hit,
        "hit_rate_pct": safe_pct(hit, total),
        "tp_count": tp,
        "sl_count": sl,
        "tp_rate_pct": safe_pct(tp, closed_ts),
        "sl_rate_pct": safe_pct(sl, closed_ts),
        "avg_rr": avg_rr,
    }


def period_stats_by_pair(entries: list[dict[str, Any]], *, now_utc: datetime, days: int) -> list[dict[str, Any]]:
    start = now_utc - timedelta(days=days)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for e in entries:
        created = parse_iso_utc(str(e.get("created_at_utc") or ""))
        if created is None or not (start <= created <= now_utc):
            continue
        pair = str(e.get("pair") or "UNKNOWN")
        grouped.setdefault(pair, []).append(e)

    out: list[dict[str, Any]] = []
    for pair, items in grouped.items():
        total = len(items)
        hit = 0
        tp = 0
        sl = 0
        wins: list[float] = []
        losses: list[float] = []
        for e in items:
            status = str(e.get("status") or "")
            if status in {"filled", "closed"} or e.get("filled_at_utc"):
                hit += 1
            ex = str(e.get("exit_status") or "")
            if ex == "tp":
                tp += 1
            elif ex == "sl":
                sl += 1
            rp = e.get("realized_pnl_pct")
            if isinstance(rp, (int, float)):
                rv = float(rp)
                if rv > 0:
                    wins.append(rv)
                elif rv < 0:
                    losses.append(rv)
        closed_ts = tp + sl
        avg_win = (sum(wins) / len(wins)) if wins else None
        avg_loss_abs = (abs(sum(losses) / len(losses))) if losses else None
        avg_rr = None
        if avg_win is not None and avg_loss_abs and avg_loss_abs > 1e-12:
            avg_rr = round(avg_win / avg_loss_abs, 3)
        out.append(
            {
                "pair": pair,
                "candidate_total": total,
                "hit_rate_pct": safe_pct(hit, total),
                "tp_rate_pct": safe_pct(tp, closed_ts),
                "sl_rate_pct": safe_pct(sl, closed_ts),
                "avg_rr": avg_rr,
            }
        )
    out.sort(key=lambda x: (-int(x["candidate_total"]), str(x["pair"])))
    return out


def fmt_pct(v: float | None) -> str:
    return f"{v:.2f}%" if isinstance(v, (int, float)) else "—"


def fmt_num(v: float | None) -> str:
    return f"{v:.3f}" if isinstance(v, (int, float)) else "—"


def _display_zone() -> tuple[ZoneInfo, str]:
    raw = (os.environ.get(JOURNAL_DISPLAY_TZ_ENV) or "").strip() or "Asia/Shanghai"
    try:
        return ZoneInfo(raw), raw
    except Exception:
        return ZoneInfo("Asia/Shanghai"), "Asia/Shanghai"


def _display_time_label() -> str:
    _, name = _display_zone()
    if name == "Asia/Shanghai":
        return "北京时间"
    return f"显示时间（{name}）"


def fmt_local_second(dt: datetime) -> str:
    """默认北京时间（Asia/Shanghai），秒级 `YYYY-MM-DD HH:MM:SS`（供 Markdown 标题人类可读）。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    tz, _ = _display_zone()
    return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def render_markdown(now_utc: datetime, week: dict[str, Any], month: dict[str, Any], by_pair_30d: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append(f"# 交易台账统计（{_display_time_label()} {fmt_local_second(now_utc)}）\n\n")
    lines.append("| 统计窗口 | 候选单 | 命中率 | 止盈率 | 止损率 | 平均盈亏比 |\n")
    lines.append("|---|---:|---:|---:|---:|---:|\n")
    lines.append(
        f"| 近7天 | {week['candidate_total']} | {fmt_pct(week['hit_rate_pct'])} | {fmt_pct(week['tp_rate_pct'])} | {fmt_pct(week['sl_rate_pct'])} | {fmt_num(week['avg_rr'])} |\n"
    )
    lines.append(
        f"| 近30天 | {month['candidate_total']} | {fmt_pct(month['hit_rate_pct'])} | {fmt_pct(month['tp_rate_pct'])} | {fmt_pct(month['sl_rate_pct'])} | {fmt_num(month['avg_rr'])} |\n"
    )
    if by_pair_30d:
        lines.append("\n## 分组统计（按币种，近30天）\n\n")
        lines.append("| 币种 | 候选单 | 命中率 | 止盈率 | 止损率 | 平均盈亏比 |\n")
        lines.append("|---|---:|---:|---:|---:|---:|\n")
        for row in by_pair_30d:
            lines.append(
                f"| {row['pair']} | {row['candidate_total']} | {fmt_pct(row['hit_rate_pct'])} | "
                f"{fmt_pct(row['tp_rate_pct'])} | {fmt_pct(row['sl_rate_pct'])} | {fmt_num(row['avg_rr'])} |\n"
            )
    return "".join(lines)


def main() -> int:
    p = argparse.ArgumentParser(description="交易台账周/月统计")
    p.add_argument(
        "--journal",
        default="output/trade_journal.jsonl",
        help="台账文件路径",
    )
    p.add_argument("--json", action="store_true", help="输出 JSON")
    args = p.parse_args()

    journal_path = Path(args.journal).resolve()
    entries = load_journal(journal_path)
    now_utc = datetime.now(timezone.utc)
    week = period_stats(entries, now_utc=now_utc, days=7)
    month = period_stats(entries, now_utc=now_utc, days=30)
    by_pair_30d = period_stats_by_pair(entries, now_utc=now_utc, days=30)
    payload = {
        "journal": str(journal_path),
        "generated_at_utc": now_utc.isoformat(),
        "week_7d": week,
        "month_30d": month,
        "by_pair_30d": by_pair_30d,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(now_utc, week, month, by_pair_30d))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

