#!/usr/bin/env python3
"""
Gate.io 现货 K 线：拉取、蜡烛图 PNG、完整 Markdown 报告、AI 极简简报与 AI 总览 JSON。（免责见仓库内 `DISCLAIMER.md`）
`--market-brief` 或指定 `--pair` 时默认：**full_report + ai_brief + ai_overview + 台账**（**不生成 PNG**，供 AI 分析）；需要 K 线图时加 **`--with-charts`**（或 `--chart-only` 仅图）。默认按日期目录自动切换：首次建目录跑 1d+4h，已存在目录跑 4h；`--single-timeframe` 可手动指定周期。
**同日输出**：`<out-dir>/<UTC日期>/` 内 **`full_report.md`、`ai_brief.md`** 各仅一份，多次运行向下**追加**并以「追加记录」标注 UTC/北京时间（默认 `Asia/Shanghai`，可用环境变量 `CRYPTO_TRADEDESK_DISPLAY_TZ` 覆盖）；**`ai_overview.json`** 为末次运行覆盖写入。PNG 为 **`{交易对slug}_{周期}.png`** 按日覆盖。旧版 `*_YYYYMMDD_HHMMSS.md/json` 会在写入后自动删除。

交易对列表默认见 `config/market_config.json`（可用 `--config` 覆盖路径）。

依赖: pip install -r requirements.txt

示例:
  python3 gateio_kline_chart.py --market-brief
  python3 gateio_kline_chart.py --market-brief --with-charts
  python3 gateio_kline_chart.py --pair ETH_USDT
  python3 gateio_kline_chart.py --pair ETH_USDT --chart-only
  python3 gateio_kline_chart.py --pair ETH_USDT --report-only
  python3 gateio_kline_chart.py --pair ETH_USDT --single-timeframe --interval 4h --limit 120
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from kline_analysis import (
    MA_LONG,
    MA_MID,
    MA_SHORT,
    MIN_JOURNAL_RR,
    compute_ohlc_stats,
    format_ai_brief_md,
    format_cross_market_analysis,
    format_dual_asset_strategy,
    format_strategy_card,
    inject_mtf_pivot_resonance,
)

from tools.time_utils import fmt_from_iso, fmt_local, parse_iso_utc, safe_tz

SCRIPT_DIR = Path(__file__).resolve().parent

GATEIO_CANDLES = "https://api.gateio.ws/api/v4/spot/candlesticks"

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 内置默认交易对（无配置文件时使用）
GATEIO_DEFAULT_BRIEF = ("BTC_USDT", "SOL_USDT", "ETH_USDT", "XAUT_USDT")

ASSET_TITLES: dict[str, str] = {
    "BTC_USDT": "BTC（比特币）",
    "SOL_USDT": "SOL",
    "ETH_USDT": "ETH（以太坊）",
    "XAUT_USDT": "XAUT（黄金代币）",
    "PAXG_USDT": "PAXG（黄金代币）",
}


def load_market_config(config_path: Path | None) -> tuple[list[str], dict[str, str]]:
    """读取 market_config.json：default_pairs、asset_titles（合并到内置标题表）。"""
    titles: dict[str, str] = dict(ASSET_TITLES)
    pairs: list[str] = list(GATEIO_DEFAULT_BRIEF)
    path = config_path if config_path is not None else SCRIPT_DIR / "config" / "market_config.json"
    if not path.is_file():
        return pairs, titles
    try:
        with path.open(encoding="utf-8") as f:
            cfg = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[警告] 无法解析配置文件 {path}: {e}，使用内置默认。", file=sys.stderr)
        return pairs, titles
    if isinstance(cfg.get("default_pairs"), list) and cfg["default_pairs"]:
        pairs = [str(x).upper().replace("-", "_") for x in cfg["default_pairs"]]
    ext = cfg.get("asset_titles")
    if isinstance(ext, dict):
        for k, v in ext.items():
            titles[str(k).upper().replace("-", "_")] = str(v)
    return pairs, titles


def _http_get_json(url: str, timeout: float = 30.0) -> Any:
    req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def fetch_gateio_candles(currency_pair: str, interval: str, limit: int) -> list[dict[str, Any]]:
    lim = min(max(limit, 1), 1000)
    q = urlencode(
        {
            "currency_pair": currency_pair.upper().replace("-", "_"),
            "interval": interval,
            "limit": str(lim),
        }
    )
    url = f"{GATEIO_CANDLES}?{q}"
    data = _http_get_json(url)
    if not isinstance(data, list):
        raise ValueError(f"Gate.io 返回异常: {data!r}")

    rows: list[dict[str, Any]] = []
    for c in data:
        ts_sec = int(c[0])
        rows.append(
            {
                "time": datetime.fromtimestamp(ts_sec, tz=timezone.utc).isoformat(),
                "open": float(c[5]),
                "high": float(c[3]),
                "low": float(c[4]),
                "close": float(c[2]),
                "volume": float(c[6]),
                "quote_volume": float(c[1]),
                "closed": c[7] if len(c) > 7 else None,
            }
        )
    rows.sort(key=lambda r: r["time"])
    return rows


def _lazy_plot_imports():
    import matplotlib

    matplotlib.use("Agg")
    import pandas as pd
    import mplfinance as mpf

    return pd, mpf


def rows_to_ohlcv_df(rows: list[dict], pd: Any) -> Any:
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("ts")
    df.set_index("ts", inplace=True)
    return df[["open", "high", "low", "close", "volume"]].astype(float)


def _mav_periods_for_len(n: int) -> tuple[int, ...] | None:
    """与 kline_analysis 口径一致：收盘 SMA8 / SMA21 / SMA55；根数不足则只画已有样本可算的周期。"""
    periods: list[int] = []
    if n >= MA_SHORT:
        periods.append(MA_SHORT)
    if n >= MA_MID:
        periods.append(MA_MID)
    if n >= MA_LONG:
        periods.append(MA_LONG)
    return tuple(periods) if periods else None


def plot_kline(df: Any, title: str, save_path: str, mpf: Any) -> str:
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
    n = len(df)
    mav = _mav_periods_for_len(n)
    plot_kw: dict[str, Any] = {
        "type": "candle",
        "volume": True,
        "style": "charles",
        "title": title,
        "ylabel": "Price (USDT)",
        "ylabel_lower": "Volume",
        "figratio": (16, 8),
        "figscale": 1.2,
        "savefig": save_path,
    }
    if mav:
        plot_kw["mav"] = mav
    mpf.plot(df, **plot_kw)
    return save_path


def _title_for(pair: str, interval: str) -> str:
    return f"{pair.replace('_', '/')} {interval} (Gate.io)"


def _slug(pair: str) -> str:
    return pair.replace("_", "").lower()


def _journal_path(out_base: Path) -> Path:
    return out_base / "trade_journal.jsonl"


def _calc_rr(idea: dict[str, Any]) -> float | None:
    """
    计算盈亏比 RR（reward/risk），用于台账写入过滤。
    - entry: entry_price（或 entry_zone 中点）
    - stop: stop_loss
    - tp1: take_profit_levels[0]
    """
    try:
        entry_zone = idea.get("entry_zone") or []
        entry_price = idea.get("entry_price")
        if isinstance(entry_price, (int, float)):
            entry = float(entry_price)
        elif isinstance(entry_zone, list) and len(entry_zone) == 2:
            entry = (float(entry_zone[0]) + float(entry_zone[1])) / 2.0
        else:
            return None
        stop = float(idea.get("stop_loss") or 0.0)
        tps = idea.get("take_profit_levels") or []
        if not (isinstance(tps, list) and tps):
            return None
        tp1 = float(tps[0])
    except (TypeError, ValueError):
        return None

    risk = abs(entry - stop)
    reward = abs(tp1 - entry)
    if risk <= 1e-12 or reward <= 1e-12:
        return None
    return reward / risk


# 按 UTC 日历日聚合：同日仅保留各一份主报告，多次运行追加 Markdown；JSON 仅保留末次快照。
DAILY_FULL_REPORT_NAME = "full_report.md"
DAILY_AI_BRIEF_NAME = "ai_brief.md"
DAILY_AI_OVERVIEW_NAME = "ai_overview.json"
_LEGACY_REPORT_TS = re.compile(
    r"^(full_report|ai_brief|ai_overview)_(\d{8})_(\d{6})\.(md|json)$"
)


def _rollup_append_header(
    now_utc: datetime,
    mode_label: str,
    interval_label: str,
    pairs_desc: str,
) -> str:
    utc_line = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    local_line = _fmt_journal_local_ts(now_utc)
    tz_label = _journal_display_time_label()
    return (
        "\n\n---\n\n"
        f"## 追加记录 · UTC **{utc_line}** · {tz_label} **{local_line}**\n\n"
        f"- **运行**：{mode_label}  \n"
        f"- **周期**：{interval_label}  \n"
        f"- **标的**：{pairs_desc}  \n\n"
    )


def _first_daily_md_prefix(session_date: str, now_utc: datetime) -> str:
    tz_label = _journal_display_time_label()
    return (
        f"<!-- 日报聚合 · UTC 日历日 {session_date} · 首次写入 UTC {_to_iso_utc(now_utc)} · "
        f"{tz_label} {_fmt_journal_local_ts(now_utc)} · 同日后续运行见下方「追加记录」-->\n\n"
    )


def _write_or_append_daily_md(
    path: Path,
    *,
    new_body: str,
    session_date: str,
    now_utc: datetime,
    mode_label: str,
    interval_label: str,
    pairs_desc: str,
) -> None:
    new_body = new_body.rstrip() + "\n"
    sep = _rollup_append_header(now_utc, mode_label, interval_label, pairs_desc)
    if path.is_file() and path.stat().st_size > 0:
        existing = path.read_text(encoding="utf-8")
        # 需求：同日聚合文件“新增块”写在文档顶端（便于直接看到最新快照），而不是末尾追加。
        #
        # 保留文件头部的“首次写入”注释（HTML comment），其余内容整体下移。
        prefix_end = 0
        if existing.lstrip().startswith("<!--"):
            # 约定 _first_daily_md_prefix() 写入后紧跟一个空行（\n\n）
            i = existing.find("\n\n")
            prefix_end = (i + 2) if i >= 0 else 0
        prefix = existing[:prefix_end]
        rest = existing[prefix_end:].lstrip("\n")
        path.write_text(prefix + sep + new_body + "\n" + rest, encoding="utf-8")
    else:
        prefix = _first_daily_md_prefix(session_date, now_utc)
        path.write_text(prefix + sep + new_body, encoding="utf-8")


def _prune_legacy_timestamped_reports(session_dir: Path) -> None:
    """删除旧版 full_report_/ai_brief_/ai_overview_ 带时间戳后缀的文件，避免同日堆积。"""
    try:
        names = list(session_dir.iterdir())
    except OSError:
        return
    for p in names:
        if p.is_file() and _LEGACY_REPORT_TS.match(p.name):
            try:
                p.unlink()
            except OSError:
                pass


def _parse_iso_utc(ts: str | None) -> datetime | None:
    return parse_iso_utc(ts)


def _to_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


JOURNAL_DISPLAY_TZ_ENV = "CRYPTO_TRADEDESK_DISPLAY_TZ"
RENDER_CHARTS_ENV = "CRYPTO_TRADEDESK_RENDER_CHARTS"


def _env_flag_true(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _journal_display_zone() -> tuple[ZoneInfo, str]:
    """人类可读时间默认 Asia/Shanghai；可用环境变量覆盖（IANA 名称）。"""
    raw = (os.environ.get(JOURNAL_DISPLAY_TZ_ENV) or "").strip() or "Asia/Shanghai"
    tz, name = safe_tz(raw)
    # 兼容：返回类型标注为 ZoneInfo，但在 Windows 缺 tzdata 时会退化为 fixed offset tzinfo
    return tz, name  # type: ignore[return-value]


def _journal_display_time_label() -> str:
    _, name = _journal_display_zone()
    if name == "Asia/Shanghai":
        return "北京时间"
    return f"显示时间（{name}）"


def _fmt_journal_local_ts(dt: datetime | None) -> str:
    """台账人类可读：默认北京时间（Asia/Shanghai），秒级 `YYYY-MM-DD HH:MM:SS`。"""
    if dt is None:
        return "—"
    tz, _ = _journal_display_zone()
    # ZoneInfo.key 是 IANA 名称（如 Asia/Shanghai）；保底用 Asia/Shanghai
    name = getattr(tz, "key", "Asia/Shanghai")
    return fmt_local(dt, tz=str(name))


def _fmt_journal_local_ts_from_iso(ts: str | None) -> str:
    if not ts or not str(ts).strip():
        return "—"
    _, name = _journal_display_zone()
    return fmt_from_iso(str(ts), tz=name)


def _interval_minutes(interval: str) -> int:
    key = interval.lower()
    if key.endswith("m"):
        try:
            return int(key[:-1])
        except ValueError:
            return 60
    if key.endswith("h"):
        try:
            return int(key[:-1]) * 60
        except ValueError:
            return 240
    if key.endswith("d"):
        try:
            return int(key[:-1]) * 1440
        except ValueError:
            return 1440
    return 60


def _norm_interval_key(interval: str) -> str:
    return interval.lower().strip()


# 战术单 entry_zone = anchor*(1±half)：过窄易被噪声扫损；按周期抬 floor / cap
_TACTICAL_ENTRY_HALF_WIDTH: dict[str, tuple[float, float]] = {
    "15m": (0.0020, 0.012),
    "30m": (0.0022, 0.013),
    "1h": (0.0028, 0.015),
    "4h": (0.0040, 0.022),
    "1d": (0.0060, 0.030),
}
_DEFAULT_TACTICAL_HALF = (0.0030, 0.016)

# 止损距「入场区间中点」至少比例（结构 pivot 过密时不再取紧邻第二档）
_STOP_MIN_DIST_PCT: dict[str, float] = {
    "15m": 0.0045,
    "30m": 0.0055,
    "1h": 0.0070,
    "4h": 0.0120,
    "1d": 0.0180,
}
_DEFAULT_STOP_MIN_DIST_PCT = 0.008

# TP 至少距入场中点的最小有利比例；档与档之间再拉开一点避免贴在一起
_TP_MIN_DIST_PCT: dict[str, float] = {
    "15m": 0.0035,
    "30m": 0.0040,
    "1h": 0.0055,
    "4h": 0.0090,
    "1d": 0.0120,
}
_DEFAULT_TP_MIN_DIST_PCT = 0.006

# 波段单：原 sma8/sma21±0.15% 过窄；按周期保证「最小半宽」与均线间距取大
_SWING_ZONE_MIN_HALF_PCT: dict[str, float] = {
    "15m": 0.0028,
    "30m": 0.0032,
    "1h": 0.0045,
    "4h": 0.0065,
    "1d": 0.0100,
}
_DEFAULT_SWING_HALF_PCT = 0.0045


def _tactical_entry_half_width(interval: str, anchor: float, last: float) -> float:
    """战术单入场半宽（相对 anchor），与锚点偏离取大，避免总宽度只有千分之几。"""
    key = _norm_interval_key(interval)
    lo, hi = _TACTICAL_ENTRY_HALF_WIDTH.get(key, _DEFAULT_TACTICAL_HALF)
    drift = abs(anchor - last) / max(abs(last), 1e-12)
    return max(lo, min(hi, max(drift * 0.45, lo)))


def _min_stop_distance_pct(interval: str) -> float:
    return _STOP_MIN_DIST_PCT.get(_norm_interval_key(interval), _DEFAULT_STOP_MIN_DIST_PCT)


def _min_tp_distance_pct(interval: str) -> float:
    return _TP_MIN_DIST_PCT.get(_norm_interval_key(interval), _DEFAULT_TP_MIN_DIST_PCT)


def _swing_zone_min_half_pct(interval: str) -> float:
    return _SWING_ZONE_MIN_HALF_PCT.get(_norm_interval_key(interval), _DEFAULT_SWING_HALF_PCT)


def _pick_stop_long(
    below: list[float], entry_mid: float, interval: str, fallback: float
) -> float:
    """below: 价格从高到低。选第一个距 entry_mid 足够远的支撑作止损。"""
    min_pct = _min_stop_distance_pct(interval)
    for p in below:
        if (entry_mid - p) / max(entry_mid, 1e-12) >= min_pct - 1e-12:
            return float(p)
    return float(fallback)


def _pick_stop_short(
    above: list[float], entry_mid: float, interval: str, fallback: float
) -> float:
    """above: 价格从低到高。选第一个距 entry_mid 足够远的压力作止损。"""
    min_pct = _min_stop_distance_pct(interval)
    for p in above:
        if (p - entry_mid) / max(entry_mid, 1e-12) >= min_pct - 1e-12:
            return float(p)
    return float(fallback)


def _pick_tp_long(
    above: list[float], entry_mid: float, interval: str, fallbacks: list[float]
) -> list[float]:
    min_tp = _min_tp_distance_pct(interval)
    min_gap = min_tp * 0.55
    out: list[float] = []
    for p in above:
        if p <= entry_mid:
            continue
        if (p - entry_mid) / max(entry_mid, 1e-12) < min_tp - 1e-12:
            continue
        if not out or (p - out[-1]) / max(out[-1], 1e-12) >= min_gap - 1e-12:
            out.append(float(p))
        if len(out) >= 3:
            break
    if not out:
        return [float(x) for x in fallbacks[:3]]
    if len(out) == 1 and len(fallbacks) >= 1:
        out.append(max(out[0] * (1.0 + min_tp), fallbacks[0]))
    return out[:3]


def _pick_tp_short(
    below: list[float], entry_mid: float, interval: str, fallbacks: list[float]
) -> list[float]:
    min_tp = _min_tp_distance_pct(interval)
    min_gap = min_tp * 0.55
    out: list[float] = []
    for p in below:
        if p >= entry_mid:
            continue
        if (entry_mid - p) / max(entry_mid, 1e-12) < min_tp - 1e-12:
            continue
        if not out or (out[-1] - p) / max(out[-1], 1e-12) >= min_gap - 1e-12:
            out.append(float(p))
        if len(out) >= 3:
            break
    if not out:
        return [float(x) for x in fallbacks[:3]]
    if len(out) == 1 and len(fallbacks) >= 1:
        out.append(min(out[0] * (1.0 - min_tp), fallbacks[0]))
    return out[:3]


def _sma_from_closes(closes: list[float], n: int) -> float | None:
    if n <= 0 or len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def _load_journal(path: Path) -> list[dict[str, Any]]:
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
            if "plan_type" not in obj:
                obj["plan_type"] = "tactical"
            out.append(obj)
    return out


def _save_journal(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(json.dumps(x, ensure_ascii=False) for x in entries)
    path.write_text((body + "\n") if body else "", encoding="utf-8")


def _classify_order_kind_cn(last: float, entry_lo: float, entry_hi: float) -> str:
    """现价已在计划入场带内 → 实时单；否则为挂单等待。"""
    lo = float(min(entry_lo, entry_hi))
    hi = float(max(entry_lo, entry_hi))
    if lo <= last <= hi:
        return "实时单"
    return "挂单"


def _collect_levels(stats: dict[str, Any], last: float) -> tuple[list[float], list[float]]:
    above: list[float] = []
    below: list[float] = []
    for x in (stats.get("sma21"), stats.get("sma55"), stats.get("hh5"), stats.get("hh_prev"), stats.get("ll5"), stats.get("ll_prev")):
        if isinstance(x, (int, float)):
            v = float(x)
            if v > last:
                above.append(v)
            elif v < last:
                below.append(v)
    fib = stats.get("fib_levels") or {}
    if isinstance(fib, dict):
        for v in fib.values():
            if not isinstance(v, (int, float)):
                continue
            fv = float(v)
            if fv > last:
                above.append(fv)
            elif fv < last:
                below.append(fv)
    for e in stats.get("structure_pivot_highs") or []:
        if isinstance(e, dict) and isinstance(e.get("price"), (int, float)):
            v = float(e["price"])
            if v > last:
                above.append(v)
    for e in stats.get("structure_pivot_lows") or []:
        if isinstance(e, dict) and isinstance(e.get("price"), (int, float)):
            v = float(e["price"])
            if v < last:
                below.append(v)
    return sorted(set(above)), sorted(set(below), reverse=True)


def _estimate_stop_prob(stats: dict[str, Any]) -> float:
    sscore = stats.get("signal_score") or {}
    total = float(sscore.get("total") or 0.0)
    wf = stats.get("walk_forward") or {}
    wf_score = float(wf.get("score") or 0.0)
    regime = stats.get("market_regime") or {}
    regime_id = str(regime.get("id") or "")
    direction = str(sscore.get("direction") or "neutral")
    base = 0.5
    base -= (total - 50.0) / 10.0 * 0.05
    base -= (wf_score - 50.0) / 10.0 * 0.03
    if regime_id in {"high_vol_chop", "transition"}:
        base += 0.08
    if (regime_id == "trend_up" and direction == "bearish") or (regime_id == "trend_down" and direction == "bullish"):
        base += 0.10
    return max(0.15, min(0.80, round(base, 3)))


def _build_trade_idea(
    *,
    pair: str,
    asset: str,
    interval: str,
    stats: dict[str, Any],
    now_utc: datetime,
) -> dict[str, Any] | None:
    filt = stats.get("signal_filter") or {}
    decision = str(filt.get("decision") or "")
    sscore = stats.get("signal_score") or {}
    total = int(sscore.get("total") or 0)
    direction = str(sscore.get("direction") or "neutral")
    if direction == "neutral":
        return None
    # 仅记录高质量候选单
    if not (decision == "executable" or (decision == "observe" and total >= 70)):
        return None

    last = float(stats["last"])
    above, below = _collect_levels(stats, last)
    if direction == "bullish":
        anchor = below[0] if below else last
        side = "long"
        z = _tactical_entry_half_width(interval, anchor, last)
        entry_low = anchor * (1.0 - z)
        entry_high = anchor * (1.0 + z)
        entry_mid = (entry_low + entry_high) / 2.0
        stop_fb = entry_mid * (1.0 - _min_stop_distance_pct(interval))
        stop = _pick_stop_long(below, entry_mid, interval, stop_fb)
        tp_src = [x for x in above if x > entry_mid] or above
        tp = _pick_tp_long(tp_src, entry_mid, interval, [anchor * 1.006, anchor * 1.012])
    else:
        anchor = above[0] if above else last
        side = "short"
        z = _tactical_entry_half_width(interval, anchor, last)
        entry_low = anchor * (1.0 - z)
        entry_high = anchor * (1.0 + z)
        entry_mid = (entry_low + entry_high) / 2.0
        stop_fb = entry_mid * (1.0 + _min_stop_distance_pct(interval))
        stop = _pick_stop_short(above, entry_mid, interval, stop_fb)
        tp_src = [x for x in below if x < entry_mid] or below
        tp = _pick_tp_short(tp_src, entry_mid, interval, [anchor * 0.994, anchor * 0.988])
    interval_min = _interval_minutes(interval)
    valid_until = now_utc + timedelta(minutes=interval_min * 8)
    risk_pct = 1.0 if total >= 80 else (0.7 if total >= 70 else 0.5)
    regime = stats.get("market_regime") or {}
    reason = (
        f"{interval} {str(regime.get('label') or '—')}，"
        f"强度 {total}/100，过滤结论 {str(filt.get('decision_cn') or '—')}。"
    )
    el = round(float(min(entry_low, entry_high)), 6)
    eh = round(float(max(entry_low, entry_high)), 6)
    entry_price = round((el + eh) / 2.0, 6)
    order_kind_cn = _classify_order_kind_cn(last, el, eh)
    return {
        "idea_id": uuid4().hex[:12],
        "created_at_utc": _to_iso_utc(now_utc),
        "pair": pair,
        "asset": asset,
        "interval": interval,
        "plan_type": "tactical",
        "direction": side,
        "entry_type": "limit",
        "entry_zone": [el, eh],
        "entry_price": entry_price,
        "order_kind_cn": order_kind_cn,
        "signal_last": round(float(last), 6),
        "position_risk_pct": risk_pct,
        "stop_loss": round(float(stop), 6),
        "take_profit_levels": [round(float(x), 6) for x in tp],
        "strategy_reason": reason,
        "risk_stop_prob": _estimate_stop_prob(stats),
        "valid_until_utc": _to_iso_utc(valid_until),
        "status": "pending",
        "exit_status": "none",
        "signal_score_total": total,
        "market_regime": regime.get("label"),
        "walk_forward_score": int((stats.get("walk_forward") or {}).get("score") or 0),
        "signal_filter_thresholds": (filt.get("thresholds") or {}),
        "review_notes": "",
        "updated_at_utc": _to_iso_utc(now_utc),
    }


def _build_trade_idea_swing(
    *,
    pair: str,
    asset: str,
    interval: str,
    stats: dict[str, Any],
    rows: list[dict[str, Any]],
    now_utc: datetime,
) -> dict[str, Any] | None:
    closes = [float(r.get("close")) for r in rows if isinstance(r.get("close"), (int, float))]
    if len(closes) < 55:
        return None
    sma8 = _sma_from_closes(closes, 8)
    sma21 = _sma_from_closes(closes, 21)
    sma55 = _sma_from_closes(closes, 55)
    if sma8 is None or sma21 is None or sma55 is None:
        return None
    last = float(closes[-1])

    if last > sma21 > sma55:
        side = "long"
    elif last < sma21 < sma55:
        side = "short"
    else:
        return None

    filt = stats.get("signal_filter") or {}
    decision = str(filt.get("decision") or "")
    total = int((stats.get("signal_score") or {}).get("total") or 0)
    # swing 只保留更干净信号
    if decision == "avoid" or total < 60:
        return None

    above, below = _collect_levels(stats, last)
    min_half_pct = _swing_zone_min_half_pct(interval)
    min_half = last * min_half_pct
    half = max(abs(sma8 - sma21) / 2.0, min_half, abs(sma21 - sma55) * 0.28)
    center = sma21
    if side == "long":
        zone_low = center - half
        zone_high = center + half
        entry_mid = (zone_low + zone_high) / 2.0
        stop_fb = min(entry_mid * (1.0 - _min_stop_distance_pct(interval)), sma55 * 0.995)
        stop = _pick_stop_long(below, entry_mid, interval, stop_fb)
        tp_src = [x for x in above if x > entry_mid] or above
        tp = _pick_tp_long(tp_src, entry_mid, interval, [last * 1.015, last * 1.03])
    else:
        zone_low = center - half
        zone_high = center + half
        entry_mid = (zone_low + zone_high) / 2.0
        stop_fb = max(entry_mid * (1.0 + _min_stop_distance_pct(interval)), sma55 * 1.005)
        stop = _pick_stop_short(above, entry_mid, interval, stop_fb)
        tp_src = [x for x in below if x < entry_mid] or below
        tp = _pick_tp_short(tp_src, entry_mid, interval, [last * 0.985, last * 0.97])

    interval_min = _interval_minutes(interval)
    valid_until = now_utc + timedelta(minutes=interval_min * 16)
    regime = stats.get("market_regime") or {}
    reason = (
        f"{interval} 波段过滤(8/21/55)：last={last:.4f}, sma8={sma8:.4f}, sma21={sma21:.4f}, sma55={sma55:.4f}；"
        f"Regime {str(regime.get('label') or '—')}，强度 {total}/100。"
    )
    zl = round(float(min(zone_low, zone_high)), 6)
    zh = round(float(max(zone_low, zone_high)), 6)
    entry_price = round((zl + zh) / 2.0, 6)
    order_kind_cn = _classify_order_kind_cn(last, zl, zh)
    return {
        "idea_id": uuid4().hex[:12],
        "created_at_utc": _to_iso_utc(now_utc),
        "pair": pair,
        "asset": asset,
        "interval": interval,
        "plan_type": "swing",
        "direction": side,
        "entry_type": "limit",
        "entry_zone": [zl, zh],
        "entry_price": entry_price,
        "order_kind_cn": order_kind_cn,
        "signal_last": round(float(last), 6),
        "position_risk_pct": 0.7 if total >= 75 else 0.5,
        "stop_loss": round(float(stop), 6),
        "take_profit_levels": [round(float(x), 6) for x in tp],
        "strategy_reason": reason,
        "risk_stop_prob": _estimate_stop_prob(stats),
        "valid_until_utc": _to_iso_utc(valid_until),
        "status": "pending",
        "exit_status": "none",
        "signal_score_total": total,
        "market_regime": regime.get("label"),
        "walk_forward_score": int((stats.get("walk_forward") or {}).get("score") or 0),
        "signal_filter_thresholds": (filt.get("thresholds") or {}),
        "review_notes": "",
        "updated_at_utc": _to_iso_utc(now_utc),
    }


def _has_active_idea(
    entries: list[dict[str, Any]],
    *,
    pair: str,
    interval: str,
    direction: str,
    plan_type: str,
) -> bool:
    for e in entries:
        if e.get("pair") != pair or e.get("interval") != interval:
            continue
        if e.get("direction") != direction:
            continue
        if str(e.get("plan_type") or "tactical") != plan_type:
            continue
        if str(e.get("status")) in {"pending", "filled"}:
            return True
    return False


def _update_idea_with_rows(idea: dict[str, Any], rows: list[dict[str, Any]], now_utc: datetime) -> bool:
    changed = False
    created_at = _parse_iso_utc(str(idea.get("created_at_utc") or ""))
    if created_at is None:
        created_at = now_utc
    valid_until = _parse_iso_utc(str(idea.get("valid_until_utc") or "")) or now_utc
    direction = str(idea.get("direction") or "long")
    status = str(idea.get("status") or "pending")
    entry_zone = idea.get("entry_zone") or []
    if not (isinstance(entry_zone, list) and len(entry_zone) == 2):
        return changed
    entry_low = float(min(entry_zone))
    entry_high = float(max(entry_zone))
    entry_px = idea.get("entry_price")
    fill_mid = (
        float(entry_px)
        if isinstance(entry_px, (int, float))
        else (entry_low + entry_high) / 2.0
    )
    stop_loss = float(idea.get("stop_loss") or 0.0)
    tps = [float(x) for x in (idea.get("take_profit_levels") or []) if isinstance(x, (int, float))]

    parsed_rows: list[tuple[datetime, float, float, float]] = []
    for r in rows:
        t = _parse_iso_utc(str(r.get("time") or ""))
        if t is None:
            continue
        parsed_rows.append((t, float(r.get("low") or 0.0), float(r.get("high") or 0.0), float(r.get("close") or 0.0)))
    parsed_rows.sort(key=lambda x: x[0])
    if not parsed_rows:
        return changed

    if status == "pending":
        for t, low, high, _close in parsed_rows:
            if t < created_at:
                continue
            if low <= entry_high and high >= entry_low:
                idea["status"] = "filled"
                idea["filled_at_utc"] = _to_iso_utc(t)
                idea["fill_price"] = round(fill_mid, 6)
                idea["exit_status"] = "none"
                changed = True
                status = "filled"
                break
        if status == "pending" and now_utc > valid_until:
            idea["status"] = "expired"
            idea["exit_status"] = "none"
            changed = True
            return changed

    if str(idea.get("status")) != "filled":
        if changed:
            idea["updated_at_utc"] = _to_iso_utc(now_utc)
        return changed

    filled_at = _parse_iso_utc(str(idea.get("filled_at_utc") or "")) or created_at
    fill_price = float(idea.get("fill_price") or fill_mid)
    last_close = parsed_rows[-1][3]
    for t, low, high, _close in parsed_rows:
        if t <= filled_at:
            continue
        sl_hit = (low <= stop_loss) if direction == "long" else (high >= stop_loss)
        tp_hit = False
        if tps:
            tp1 = tps[0]
            tp_hit = (high >= tp1) if direction == "long" else (low <= tp1)
        if sl_hit and tp_hit:
            # 同根同时命中时取保守：先止损
            idea["status"] = "closed"
            idea["exit_status"] = "sl"
            idea["closed_at_utc"] = _to_iso_utc(t)
            pnl_pct = (stop_loss - fill_price) / max(fill_price, 1e-12) * 100.0
            if direction == "short":
                pnl_pct = -pnl_pct
            idea["closed_price"] = round(stop_loss, 6)
            idea["realized_pnl_pct"] = round(pnl_pct, 3)
            changed = True
            break
        if sl_hit:
            idea["status"] = "closed"
            idea["exit_status"] = "sl"
            idea["closed_at_utc"] = _to_iso_utc(t)
            pnl_pct = (stop_loss - fill_price) / max(fill_price, 1e-12) * 100.0
            if direction == "short":
                pnl_pct = -pnl_pct
            idea["closed_price"] = round(stop_loss, 6)
            idea["realized_pnl_pct"] = round(pnl_pct, 3)
            changed = True
            break
        if tp_hit:
            idea["status"] = "closed"
            idea["exit_status"] = "tp"
            idea["closed_at_utc"] = _to_iso_utc(t)
            tp_px = tps[0]
            pnl_pct = (tp_px - fill_price) / max(fill_price, 1e-12) * 100.0
            if direction == "short":
                pnl_pct = -pnl_pct
            idea["closed_price"] = round(tp_px, 6)
            idea["realized_pnl_pct"] = round(pnl_pct, 3)
            changed = True
            break

    if str(idea.get("status")) == "filled":
        pnl = (last_close - fill_price) / max(fill_price, 1e-12) * 100.0
        if direction == "short":
            pnl = -pnl
        idea["unrealized_pnl_pct"] = round(pnl, 3)
        idea["exit_status"] = "float_profit" if pnl >= 0 else "float_loss"
        changed = True

    if changed:
        idea["updated_at_utc"] = _to_iso_utc(now_utc)
    return changed


def _safe_pct(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round(num / den * 100.0, 2)


def _collect_period_stats(entries: list[dict[str, Any]], *, now_utc: datetime, days: int) -> dict[str, Any]:
    start = now_utc - timedelta(days=days)
    scoped: list[dict[str, Any]] = []
    for e in entries:
        created = _parse_iso_utc(str(e.get("created_at_utc") or ""))
        if created is None:
            continue
        if start <= created <= now_utc:
            scoped.append(e)

    total = len(scoped)
    hit = 0
    tp = 0
    sl = 0
    realized_pos: list[float] = []
    realized_neg: list[float] = []
    for e in scoped:
        status = str(e.get("status") or "")
        if status in {"filled", "closed"} or e.get("filled_at_utc"):
            hit += 1
        exit_status = str(e.get("exit_status") or "")
        if exit_status == "tp":
            tp += 1
        elif exit_status == "sl":
            sl += 1
        rp = e.get("realized_pnl_pct")
        if isinstance(rp, (int, float)):
            rv = float(rp)
            if rv > 0:
                realized_pos.append(rv)
            elif rv < 0:
                realized_neg.append(rv)

    closed_ts = tp + sl
    avg_win = (sum(realized_pos) / len(realized_pos)) if realized_pos else None
    avg_loss_abs = (abs(sum(realized_neg) / len(realized_neg))) if realized_neg else None
    rr = None
    if avg_win is not None and avg_loss_abs and avg_loss_abs > 1e-12:
        rr = round(avg_win / avg_loss_abs, 3)
    return {
        "days": days,
        "candidate_total": total,
        "hit_count": hit,
        "hit_rate_pct": _safe_pct(hit, total),
        "tp_count": tp,
        "sl_count": sl,
        "tp_rate_pct": _safe_pct(tp, closed_ts),
        "sl_rate_pct": _safe_pct(sl, closed_ts),
        "avg_rr": rr,
    }


def _collect_period_stats_by_pair(
    entries: list[dict[str, Any]],
    *,
    now_utc: datetime,
    days: int,
) -> list[dict[str, Any]]:
    start = now_utc - timedelta(days=days)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for e in entries:
        created = _parse_iso_utc(str(e.get("created_at_utc") or ""))
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
        rr = None
        if avg_win is not None and avg_loss_abs and avg_loss_abs > 1e-12:
            rr = round(avg_win / avg_loss_abs, 3)
        out.append(
            {
                "pair": pair,
                "candidate_total": total,
                "hit_rate_pct": _safe_pct(hit, total),
                "tp_rate_pct": _safe_pct(tp, closed_ts),
                "sl_rate_pct": _safe_pct(sl, closed_ts),
                "avg_rr": rr,
            }
        )
    out.sort(key=lambda x: (-int(x["candidate_total"]), str(x["pair"])))
    return out


def _fmt_pct(v: float | None) -> str:
    return f"{v:.2f}%" if isinstance(v, (int, float)) else "—"


def _fmt_num(v: float | None) -> str:
    return f"{v:.3f}" if isinstance(v, (int, float)) else "—"


def _build_journal_summary_md(entries: list[dict[str, Any]], now_utc: datetime) -> str:
    if not entries:
        return ""
    open_pending = sum(1 for e in entries if str(e.get("status") or "") == "pending")
    open_filled = sum(1 for e in entries if str(e.get("status") or "") == "filled")
    week = _collect_period_stats(entries, now_utc=now_utc, days=7)
    month = _collect_period_stats(entries, now_utc=now_utc, days=30)
    by_pair_30d = _collect_period_stats_by_pair(entries, now_utc=now_utc, days=30)

    lines: list[str] = []
    lines.append("## 〇、台账回顾摘要（自动生成）\n\n")
    lines.append(
        f"- 更新时间（{_journal_display_time_label()}）：**{_fmt_journal_local_ts(now_utc)}**；当前未完成：pending **{open_pending}** 条，filled **{open_filled}** 条。\n"
    )
    lines.append(
        f"- 近 7 天：候选 **{week['candidate_total']}**，命中率 **{_fmt_pct(week['hit_rate_pct'])}**，"
        f"止盈率 **{_fmt_pct(week['tp_rate_pct'])}**，止损率 **{_fmt_pct(week['sl_rate_pct'])}**，平均盈亏比 **{_fmt_num(week['avg_rr'])}**。\n"
    )
    lines.append(
        f"- 近 30 天：候选 **{month['candidate_total']}**，命中率 **{_fmt_pct(month['hit_rate_pct'])}**，"
        f"止盈率 **{_fmt_pct(month['tp_rate_pct'])}**，止损率 **{_fmt_pct(month['sl_rate_pct'])}**，平均盈亏比 **{_fmt_num(month['avg_rr'])}**。\n"
    )
    if by_pair_30d:
        lines.append("\n### 台账分组（按币种，近30天）\n\n")
        lines.append("| 币种 | 候选单 | 命中率 | 止盈率 | 止损率 | 平均盈亏比 |\n")
        lines.append("|---|---:|---:|---:|---:|---:|\n")
        for row in by_pair_30d:
            lines.append(
                f"| {row['pair']} | {row['candidate_total']} | {_fmt_pct(row['hit_rate_pct'])} | "
                f"{_fmt_pct(row['tp_rate_pct'])} | {_fmt_pct(row['sl_rate_pct'])} | {_fmt_num(row['avg_rr'])} |\n"
            )
    lines.append("\n---\n\n")
    return "".join(lines)


def _write_journal_stats_files(out_base: Path, entries: list[dict[str, Any]], now_utc: datetime) -> Path:
    week = _collect_period_stats(entries, now_utc=now_utc, days=7)
    month = _collect_period_stats(entries, now_utc=now_utc, days=30)
    by_pair_30d = _collect_period_stats_by_pair(entries, now_utc=now_utc, days=30)
    payload = {
        "generated_at_utc": _to_iso_utc(now_utc),
        "week_7d": week,
        "month_30d": month,
        "by_pair_30d": by_pair_30d,
    }
    md_lines = [
        f"# 交易台账统计（{_journal_display_time_label()} {_fmt_journal_local_ts(now_utc)}）\n\n",
        "| 统计窗口 | 候选单 | 命中率 | 止盈率 | 止损率 | 平均盈亏比 |\n",
        "|---|---:|---:|---:|---:|---:|\n",
        f"| 近7天 | {week['candidate_total']} | {_fmt_pct(week['hit_rate_pct'])} | {_fmt_pct(week['tp_rate_pct'])} | {_fmt_pct(week['sl_rate_pct'])} | {_fmt_num(week['avg_rr'])} |\n",
        f"| 近30天 | {month['candidate_total']} | {_fmt_pct(month['hit_rate_pct'])} | {_fmt_pct(month['tp_rate_pct'])} | {_fmt_pct(month['sl_rate_pct'])} | {_fmt_num(month['avg_rr'])} |\n",
    ]
    if by_pair_30d:
        md_lines.extend(
            [
                "\n## 分组统计（按币种，近30天）\n\n",
                "| 币种 | 候选单 | 命中率 | 止盈率 | 止损率 | 平均盈亏比 |\n",
                "|---|---:|---:|---:|---:|---:|\n",
            ]
        )
        for row in by_pair_30d:
            md_lines.append(
                f"| {row['pair']} | {row['candidate_total']} | {_fmt_pct(row['hit_rate_pct'])} | "
                f"{_fmt_pct(row['tp_rate_pct'])} | {_fmt_pct(row['sl_rate_pct'])} | {_fmt_num(row['avg_rr'])} |\n"
            )
    out_base.mkdir(parents=True, exist_ok=True)
    md_path = out_base / "trade_journal_stats_latest.md"
    md_path.write_text("".join(md_lines), encoding="utf-8")
    # 历史版本会写 JSON；当前按需求仅保留 Markdown 统计快照。
    legacy_json_path = out_base / "trade_journal_stats_latest.json"
    if legacy_json_path.exists():
        try:
            legacy_json_path.unlink()
        except OSError:
            pass
    return md_path


def _fmt_num_cell(v: Any, ndigits: int = 3) -> str:
    if isinstance(v, (int, float)):
        return f"{float(v):.{ndigits}f}"
    return "—"


def _fmt_journal_price(v: Any) -> str:
    """人类可读台账表中价位列（入场 / 止损 / TP1 等）：统一保留小数点后一位。"""
    if isinstance(v, (int, float)):
        return f"{float(v):.1f}"
    return "—"


def _journal_fill_or_entry_price(e: dict[str, Any]) -> float | None:
    """已成交/已了结用 fill_price；否则用 entry_price 或 entry_zone 中点。"""
    st = str(e.get("status") or "")
    if st in {"filled", "closed"} and isinstance(e.get("fill_price"), (int, float)):
        return float(e["fill_price"])
    ep = e.get("entry_price")
    if isinstance(ep, (int, float)):
        return float(ep)
    z = e.get("entry_zone")
    if isinstance(z, list) and len(z) == 2:
        return (float(z[0]) + float(z[1])) / 2.0
    return None


def _fmt_journal_entry_point(e: dict[str, Any]) -> str:
    p = _journal_fill_or_entry_price(e)
    if p is None:
        return "—"
    return _fmt_journal_price(p)


def _fmt_journal_order_kind_cn(e: dict[str, Any]) -> str:
    k = e.get("order_kind_cn")
    if isinstance(k, str) and k.strip():
        return k.strip()
    sl = e.get("signal_last")
    z = e.get("entry_zone")
    if isinstance(sl, (int, float)) and isinstance(z, list) and len(z) == 2:
        return _classify_order_kind_cn(float(sl), float(z[0]), float(z[1]))
    if isinstance(z, list) and len(z) == 2:
        return "挂单（旧记录）"
    return "—"


def _journal_tp1(e: dict[str, Any]) -> float | None:
    tps = e.get("take_profit_levels") or []
    if isinstance(tps, list) and tps:
        v = tps[0]
        if isinstance(v, (int, float)):
            return float(v)
    return None


def _journal_rr(e: dict[str, Any]) -> float | None:
    """尽量从台账 rr 字段读取；否则用 entry/SL/TP1 计算（按方向区分）。"""
    rr = e.get("rr")
    if isinstance(rr, (int, float)) and rr > 0:
        return float(rr)
    entry = _journal_fill_or_entry_price(e)
    sl = e.get("stop_loss")
    tp1 = _journal_tp1(e)
    if not isinstance(entry, (int, float)):
        return None
    if not isinstance(sl, (int, float)):
        return None
    if not isinstance(tp1, (int, float)):
        return None
    entry_f = float(entry)
    sl_f = float(sl)
    tp1_f = float(tp1)
    d = str(e.get("direction") or "").lower()
    if d == "long":
        risk = entry_f - sl_f
        reward = tp1_f - entry_f
    elif d == "short":
        risk = sl_f - entry_f
        reward = entry_f - tp1_f
    else:
        return None
    if risk <= 0:
        return None
    return reward / risk


def _infer_signal_filter_decision_cn(e: dict[str, Any]) -> str:
    """用台账里已有字段推断：可执行/观察/回避；老记录可能无法推断。"""
    total = e.get("signal_score_total")
    wf = e.get("walk_forward_score")
    th = e.get("signal_filter_thresholds") or {}
    min_total = th.get("min_total_score")
    min_wf = th.get("min_walk_forward_score")
    if not isinstance(total, (int, float)) or not isinstance(wf, (int, float)):
        return "—"
    if not isinstance(min_total, (int, float)) or not isinstance(min_wf, (int, float)):
        return "—"
    if float(total) >= float(min_total) and float(wf) >= float(min_wf):
        return "可执行"
    # 阈值不达标时：用区分“观察/回避”的信息不足（台账不一定存 decision），统一标观察更保守
    return "观察"


def _infer_action_hint_cn(e: dict[str, Any]) -> str:
    """面向人类扫读：值得做/再观察/保守观望（仅供讨论）。"""
    decision = _infer_signal_filter_decision_cn(e)
    rr = _journal_rr(e)
    score = e.get("signal_score_total")
    if decision == "可执行":
        if isinstance(rr, (int, float)) and rr >= 1.5 and isinstance(score, (int, float)) and score >= 70:
            return "值得做"
        if isinstance(rr, (int, float)) and rr >= 1.2:
            return "再观察"
        return "保守观望"
    if decision == "观察":
        return "再观察"
    return "保守观望"


def _write_journal_human_views(out_base: Path, entries: list[dict[str, Any]], now_utc: datetime) -> Path:
    out_base.mkdir(parents=True, exist_ok=True)
    md_path = out_base / "trade_journal_readable.md"

    for e in entries:
        if "plan_type" not in e:
            e["plan_type"] = "tactical"

    def _row(e: dict[str, Any]) -> str:
        tp1 = _journal_tp1(e)
        rr = _journal_rr(e)
        score = e.get("signal_score_total")
        action_cn = _infer_action_hint_cn(e)
        return (
            f"| {e.get('idea_id','—')} | {e.get('pair','—')} | {e.get('interval','—')} | {e.get('plan_type','—')} | {e.get('direction','—')} | "
            f"{_fmt_journal_entry_point(e)} | {_fmt_journal_order_kind_cn(e)} | {_fmt_journal_price(e.get('stop_loss'))} | {_fmt_journal_price(tp1)} | "
            f"{_fmt_num_cell(rr, 3)} | {_fmt_num_cell(score, 0)} | {action_cn} | "
            f"{e.get('status','—')} | {e.get('exit_status','—')} | {_fmt_num_cell(e.get('realized_pnl_pct'), 3)} | "
            f"{_fmt_num_cell(e.get('unrealized_pnl_pct'), 3)} | {_fmt_journal_local_ts_from_iso(str(e.get('created_at_utc') or ''))} |\n"
        )

    lines: list[str] = []
    lines.append(f"# 交易台账（人类可读）\n\n")
    lines.append(f"- 更新时间（{_journal_display_time_label()}）：`{_fmt_journal_local_ts(now_utc)}`\n")
    lines.append(
        "- **入场点位**：计划价为 **`entry_zone` 中点**（字段 `entry_price`）；已成交/已了结行显示 **`fill_price`**。\n"
        "- **开单类型**：生成时若收盘价 `signal_last` 落在 `entry_zone` 内为 **实时单**（偏市价思路），否则为 **挂单**；旧 JSONL 无字段时显示 **挂单（旧记录）**。\n\n"
    )

    def _append_group(plan_type: str, title: str) -> None:
        scoped = [e for e in entries if str(e.get("plan_type") or "tactical") == plan_type]
        pending = [e for e in scoped if str(e.get("status") or "") in {"pending", "filled"}]
        closed = [e for e in scoped if str(e.get("status") or "") in {"closed", "expired", "cancelled"}]
        pending.sort(key=lambda x: str(x.get("created_at_utc") or ""), reverse=True)
        closed.sort(key=lambda x: str(x.get("updated_at_utc") or x.get("created_at_utc") or ""), reverse=True)
        lines.append(
            f"## {title}（{plan_type}）\n\n"
            f"- 总记录：**{len(scoped)}**，未完成：**{len(pending)}**，已结束：**{len(closed)}**\n\n"
        )
        lines.append(
            "| ID | 币种 | 周期 | 计划类型 | 方向 | 入场点位 | 开单类型 | 止损 | TP1 | 盈亏比 | 信号强度 | 建议 | 状态 | 出局状态 | 已实现P/L% | 浮动P/L% | 创建时间 |\n"
        )
        lines.append("|---|---|---|---|---|---|---|---:|---:|---:|---:|---|---|---|---:|---:|---|\n")
        shown = pending[:80] + closed[:40]
        if shown:
            for e in shown:
                lines.append(_row(e))
        else:
            lines.append("| — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — | — |\n")
        lines.append("\n")

    _append_group("tactical", "短线计划")
    _append_group("swing", "中级别计划（8/21/55）")

    md_path.write_text("".join(lines), encoding="utf-8")
    return md_path


def run_one(
    pair: str,
    interval: str,
    limit: int,
    *,
    out_dir: str,
    report: bool,
    render_chart: bool,
    asset_titles: dict[str, str],
    pd: Any,
    mpf: Any,
) -> tuple[str | None, str | None, tuple[str, str, dict[str, Any]] | None, list[tuple[str, dict[str, Any]]] | None]:
    pair_u = pair.upper().replace("-", "_")
    rows = fetch_gateio_candles(pair_u, interval, limit)
    if not rows:
        return None, f"【{pair_u}】无数据", None, None

    out_p = Path(out_dir)
    png: str | None = None
    if render_chart:
        png = str(out_p / f"{_slug(pair_u)}_{interval}.png")
        df = rows_to_ohlcv_df(rows, pd)
        plot_kline(df, _title_for(pair_u, interval), png, mpf)

    asset = asset_titles.get(pair_u, pair_u)
    if not report:
        return png, None, None, None

    stats = compute_ohlc_stats(rows, interval=interval)
    if stats:
        text = format_strategy_card(
            interval,
            pair_u,
            asset,
            stats,
        )
        meta = (pair_u, asset, stats)
        return png, text, meta, [(interval, stats)]
    return png, f"【{asset}】K 线过少，无法生成策略卡片。", None, None


def run_pair_dual(
    pair: str,
    frame_specs: tuple[tuple[str, int], ...],
    *,
    out_dir: str,
    report: bool,
    render_chart: bool,
    asset_titles: dict[str, str],
    pd: Any,
    mpf: Any,
) -> tuple[list[str], str | None, tuple[str, str, dict[str, Any]] | None, list[tuple[str, dict[str, Any]]] | None]:
    pair_u = pair.upper().replace("-", "_")
    asset = asset_titles.get(pair_u, pair_u)
    png_paths: list[str] = []
    collected: list[tuple[str, dict[str, Any]]] = []
    failed: list[str] = []
    cross_meta: tuple[str, str, dict[str, Any]] | None = None
    out_p = Path(out_dir)

    for interval, limit in frame_specs:
        rows = fetch_gateio_candles(pair_u, interval, limit)
        if not rows:
            failed.append(interval)
            continue
        if render_chart:
            png = str(out_p / f"{_slug(pair_u)}_{interval}.png")
            df = rows_to_ohlcv_df(rows, pd)
            plot_kline(df, _title_for(pair_u, interval), png, mpf)
            png_paths.append(png)
        if not report:
            continue
        stats = compute_ohlc_stats(rows, interval=interval)
        if stats:
            collected.append((interval, stats))
            if interval == "4h":
                cross_meta = (pair_u, asset, stats)
        else:
            failed.append(f"{interval}（样本不足）")

    if not report:
        return png_paths, None, cross_meta, None

    by_iv = {iv: st for iv, st in collected}
    ordered = [(iv, by_iv[iv]) for iv, _lim in frame_specs if iv in by_iv]
    inject_mtf_pivot_resonance(ordered)
    if not ordered:
        msg = f"【{asset}】无法拉取 K 线或样本不足，未生成策略卡片。"
        if failed:
            msg += f"（失败周期：{', '.join(failed)}）"
        return png_paths, msg, None, None

    text = format_dual_asset_strategy(
        pair_u,
        asset,
        ordered,
    )
    if failed:
        text += f"\n> 部分周期未生成图表或统计：{', '.join(failed)}\n"

    return png_paths, text, cross_meta, ordered


def _build_ai_overview_json(
    items: list[tuple[str, str, list[tuple[str, dict[str, Any]]]]],
    *,
    generated_iso: str,
    cross_interval: str,
) -> dict[str, Any]:
    """生成供 AI 快速消费的结构化总览。"""
    assets: list[dict[str, Any]] = []
    for pair_sym, asset, frames in items:
        frame_map: dict[str, Any] = {}
        for iv, st in frames:
            frame_map[iv] = {
                "last": st["last"],
                "sma8": st.get("sma8"),
                "sma21": st.get("sma21"),
                "sma55": st.get("sma55"),
                "p8_pct": st.get("p8"),
                "p21_pct": st.get("p21"),
                "p55_pct": st.get("p55"),
                "ret8_pct": st.get("ret8"),
                "ret21_pct": st.get("ret21"),
                "hh5": st.get("hh5"),
                "hh_prev": st.get("hh_prev"),
                "ll5": st.get("ll5"),
                "ll_prev": st.get("ll_prev"),
                "swing_anchor_high": {
                    "price": st.get("swing_anchor_high"),
                    "time": st.get("swing_anchor_high_time"),
                },
                "swing_anchor_low": {
                    "price": st.get("swing_anchor_low"),
                    "time": st.get("swing_anchor_low_time"),
                },
                "fib_anchor_window": st.get("fib_anchor_window"),
                "fib_levels": st.get("fib_levels"),
                "price_vs_fib_zone": st.get("price_vs_fib_zone"),
                "structure_pivot_highs": st.get("structure_pivot_highs"),
                "structure_pivot_lows": st.get("structure_pivot_lows"),
                "structure_pivot_lookback_bars": st.get("structure_pivot_lookback_bars"),
                "method_123": st.get("method_123"),
                "signal_score": st.get("signal_score"),
                "market_regime": st.get("market_regime"),
                "walk_forward": st.get("walk_forward"),
                "signal_filter": st.get("signal_filter"),
            }
        assets.append({"pair": pair_sym, "asset": asset, "frames": frame_map})
    return {
        "generated_at_utc": generated_iso,
        "cross_market_interval": cross_interval,
        "notes": (
            "Machine-readable overview for AI post-processing. "
            "method_123: code-normalized Ross-style reversal 1-2-3 on fractal pivots "
            "(ruleset_id, bullish/bearish or null, break_level, state_vs_structure); "
            "signal_score: 0-100 deterministic score (trend40/structure30/momentum20/resonance10); "
            "market_regime: lightweight regime tag; walk_forward: segmented stability proxy; "
            "signal_filter: executable/observe/avoid decision by thresholds. "
            "interpretation and execution wording belong in chat, not in JSON."
        ),
        "assets": assets,
    }


def main_chart(args: argparse.Namespace) -> int:
    cfg_path = Path(args.config).resolve() if args.config else None
    brief_pairs, titles_merged = load_market_config(cfg_path)

    if args.market_brief:
        pairs = brief_pairs
    else:
        if not args.pair:
            print("请指定 --pair ETH_USDT 或使用 --market-brief", file=sys.stderr)
            return 2
        pairs = [args.pair]
    if args.chart_only and args.report_only:
        print("--chart-only 与 --report-only 不能同时使用", file=sys.stderr)
        return 2
    if args.chart_only and args.with_charts:
        print("--chart-only 与 --with-charts 不能同时使用", file=sys.stderr)
        return 2
    if args.report_only:
        do_report = True
        render_chart = False
    elif args.chart_only:
        do_report = False
        render_chart = True
    elif args.with_charts or _env_flag_true(RENDER_CHARTS_ENV):
        do_report = True
        render_chart = True
    else:
        do_report = True
        render_chart = False

    pd: Any
    mpf: Any
    if render_chart:
        pd, mpf = _lazy_plot_imports()
    else:
        pd, mpf = None, None

    session_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_base = Path(args.out_dir).resolve()
    session_dir = out_base / session_date
    session_exists_before = session_dir.exists()
    session_dir.mkdir(parents=True, exist_ok=True)
    out_dir = str(session_dir)

    all_text: list[str] = []
    cross_items: list[tuple[str, str, dict[str, Any]]] = []
    brief_items: list[tuple[str, str, list[tuple[str, dict[str, Any]]]]] = []
    auto_frames: tuple[tuple[str, int], ...] = (
        (("4h", 120),) if session_exists_before else (("1d", 120), ("4h", 120))
    )
    use_auto_single = not args.single_timeframe
    journal_updates = 0
    journal_created = 0
    journal_entries: list[dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)

    for pair in pairs:
        pair_u = pair.upper().replace("-", "_")
        try:
            if use_auto_single:
                pngs, txt, meta, ordered = run_pair_dual(
                    pair,
                    auto_frames,
                    out_dir=out_dir,
                    report=do_report,
                    render_chart=render_chart,
                    asset_titles=titles_merged,
                    pd=pd,
                    mpf=mpf,
                )
                for p in pngs:
                    print(f"[图] {p}", file=sys.stderr)
                if txt:
                    all_text.append(txt)
                if meta is not None:
                    cross_items.append((meta[0], meta[1], meta[2]))
                if ordered:
                    asset = titles_merged.get(pair_u, pair_u)
                    brief_items.append((pair_u, asset, ordered))
            else:
                png, txt, meta, frames_meta = run_one(
                    pair,
                    args.interval,
                    args.limit,
                    out_dir=out_dir,
                    report=do_report,
                    render_chart=render_chart,
                    asset_titles=titles_merged,
                    pd=pd,
                    mpf=mpf,
                )
                if png:
                    print(f"[图] {png}", file=sys.stderr)
                if txt:
                    all_text.append(txt)
                if meta is not None:
                    cross_items.append((meta[0], meta[1], meta[2]))
                if frames_meta:
                    asset = titles_merged.get(pair_u, pair_u)
                    brief_items.append((pair_u, asset, frames_meta))
        except Exception as e:
            print(f"[错] {pair}: {e}", file=sys.stderr)
            continue

    if do_report and brief_items:
        journal_file = _journal_path(out_base)
        journal_entries = _load_journal(journal_file)
        iv_limit = {iv: lim for iv, lim in auto_frames} if use_auto_single else {args.interval: args.limit}
        for pair_sym, asset, frames in brief_items:
            for iv, st in frames:
                lim = int(iv_limit.get(iv, 120))
                rows = fetch_gateio_candles(pair_sym, iv, lim)
                for idea in journal_entries:
                    if idea.get("pair") != pair_sym or idea.get("interval") != iv:
                        continue
                    if _update_idea_with_rows(idea, rows, now_utc):
                        journal_updates += 1
                tactical_idea = _build_trade_idea(
                    pair=pair_sym,
                    asset=asset,
                    interval=iv,
                    stats=st,
                    now_utc=now_utc,
                )
                swing_idea = _build_trade_idea_swing(
                    pair=pair_sym,
                    asset=asset,
                    interval=iv,
                    stats=st,
                    rows=rows,
                    now_utc=now_utc,
                )
                for idea in (tactical_idea, swing_idea):
                    if not idea:
                        continue
                    if _has_active_idea(
                        journal_entries,
                        pair=pair_sym,
                        interval=iv,
                        direction=str(idea.get("direction") or "long"),
                        plan_type=str(idea.get("plan_type") or "tactical"),
                    ):
                        continue
                    rr = _calc_rr(idea)
                    if rr is None or rr < float(MIN_JOURNAL_RR):
                        continue
                    idea["rr"] = round(float(rr), 4)
                    journal_entries.append(idea)
                    journal_created += 1
        _save_journal(journal_file, journal_entries)
        stats_md = _write_journal_stats_files(out_base, journal_entries, now_utc)
        readable_md = _write_journal_human_views(out_base, journal_entries, now_utc)
        print(
            f"[台账] {journal_file}（更新 {journal_updates} 条，新增 {journal_created} 条）",
            file=sys.stderr,
        )
        print(f"[台账统计] {stats_md}", file=sys.stderr)
        print(f"[台账视图] {readable_md}", file=sys.stderr)

    if render_chart and do_report:
        artifact_mode = "PNG+报告"
    elif render_chart:
        artifact_mode = "仅PNG"
    else:
        artifact_mode = "仅报告"
    iv_run = "+".join(iv for iv, _ in auto_frames) if use_auto_single else args.interval
    png_note = (
        "PNG 为 `<slug>_<interval>.png` 按 UTC 日期覆盖"
        if render_chart
        else "未生成 PNG（需要图请加 `--with-charts`，或设 `CRYPTO_TRADEDESK_RENDER_CHARTS=1`）"
    )
    print(
        f"[数据] 产物已写入 {session_dir}（{artifact_mode}；报告 {DAILY_FULL_REPORT_NAME}/{DAILY_AI_BRIEF_NAME}/"
        f"{DAILY_AI_OVERVIEW_NAME}；{png_note}；本次周期 {iv_run}）",
        file=sys.stderr,
    )

    cross_text = ""
    if do_report and len(cross_items) >= 2:
        cross_iv = "4h"
        cross_text = format_cross_market_analysis(
            cross_items,
            cross_iv,
            multi_timeframe_context=False,
        )

    full_body = "\n".join(all_text) + cross_text
    if do_report and full_body.strip() and journal_entries:
        full_body = _build_journal_summary_md(journal_entries, now_utc) + full_body

    if do_report and full_body.strip():
        gen_iso = datetime.now(timezone.utc).isoformat()
        mode_label = (
            "market-brief（多品种）"
            if use_auto_single
            else f"single-timeframe · {pairs[0] if len(pairs) == 1 else ','.join(pairs)}"
        )
        interval_label = (
            "+".join(iv for iv, _ in auto_frames) if use_auto_single else str(args.interval)
        )
        pairs_desc = ", ".join(pairs)

        fr_path = session_dir / DAILY_FULL_REPORT_NAME
        _write_or_append_daily_md(
            fr_path,
            new_body=full_body,
            session_date=session_date,
            now_utc=now_utc,
            mode_label=mode_label,
            interval_label=interval_label,
            pairs_desc=pairs_desc,
        )
        print(f"[报告] {fr_path}", file=sys.stderr)
        if brief_items:
            ai_body = format_ai_brief_md(
                brief_items,
                cross_section=cross_text,
                generated_iso=gen_iso,
            )
            ai_path = session_dir / DAILY_AI_BRIEF_NAME
            _write_or_append_daily_md(
                ai_path,
                new_body=ai_body,
                session_date=session_date,
                now_utc=now_utc,
                mode_label=mode_label,
                interval_label=interval_label,
                pairs_desc=pairs_desc,
            )
            print(f"[AI简报] {ai_path}", file=sys.stderr)
            overview = _build_ai_overview_json(
                brief_items,
                generated_iso=gen_iso,
                cross_interval="4h",
            )
            overview_path = session_dir / DAILY_AI_OVERVIEW_NAME
            overview_path.write_text(
                json.dumps(overview, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[AI总览] {overview_path}", file=sys.stderr)
        _prune_legacy_timestamped_reports(session_dir)

    print(f"[会话目录] {session_dir}", file=sys.stderr)
    return 0


def main() -> int:
    default_out = str(SCRIPT_DIR / "output")
    p = argparse.ArgumentParser(description="Gate.io K 线：报告+AI总览（默认不出图；按日期目录自动周期）")
    p.add_argument(
        "--config",
        default=None,
        help=f"市场配置 JSON 路径，默认 {SCRIPT_DIR / 'config' / 'market_config.json'}",
    )
    p.add_argument(
        "--pair",
        default=None,
        help="交易对；出图模式必填（除非 --market-brief）",
    )
    p.add_argument(
        "--market-brief",
        action="store_true",
        help="按配置文件 default_pairs 运行并生成 full_report、ai_brief、ai_overview（默认不生成 PNG）",
    )
    p.add_argument(
        "--single-timeframe",
        action="store_true",
        help="仅单周期（--interval / --limit）；默认按日期目录自动：新建目录=1d+4h，已存在目录=4h",
    )
    p.add_argument("--interval", default="4h", help="单周期模式下的 K 线周期")
    p.add_argument("--limit", type=int, default=120, help="单周期模式下的条数")
    p.add_argument(
        "--out-dir",
        default=default_out,
        help="输出根目录；实际写入 <out-dir>/<UTC日期>/",
    )
    p.add_argument(
        "--with-charts",
        action="store_true",
        help="同时生成 PNG（与报告一起写入；默认不出图）",
    )
    p.add_argument(
        "--chart-only",
        action="store_true",
        help="仅 PNG，不写 full_report / ai_brief / ai_overview，stdout 不输出长文",
    )
    p.add_argument(
        "--report-only",
        action="store_true",
        help="仅报告（full_report / ai_brief / ai_overview），不生成 PNG（与默认行为相同）",
    )
    args = p.parse_args()

    if args.market_brief:
        pass
    elif not args.pair:
        print("请指定 --pair 或 --market-brief", file=sys.stderr)
        return 2

    return main_chart(args)


if __name__ == "__main__":
    raise SystemExit(main())
