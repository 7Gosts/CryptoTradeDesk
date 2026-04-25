"""
K 线技术分析文案（基于 OHLC，非投资建议）。
供 gateio_kline_chart.py 共用。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_DEFAULT_CFG_PATH = _SCRIPT_DIR / "config" / "analysis_defaults.example.yaml"


def _load_analysis_config() -> dict[str, Any]:
    """
    加载分析参数配置（YAML）。
    - 默认路径：config/analysis_defaults.example.yaml
    - 可用环境变量覆盖：CRYPTOTRADEDESK_ANALYSIS_CONFIG=/abs/path/to/yaml
    - 读取失败则返回空 dict（使用代码默认值）
    """
    cfg_path = os.getenv("CRYPTOTRADEDESK_ANALYSIS_CONFIG", "").strip()
    path = Path(cfg_path).expanduser().resolve() if cfg_path else _DEFAULT_CFG_PATH
    from tools.config import load_yaml

    return load_yaml(path)


_CFG = _load_analysis_config()


def _cfg_num(key: str, default: float) -> float:
    v = _CFG.get(key, default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _cfg_int(key: str, default: int) -> int:
    v = _CFG.get(key, default)
    try:
        return int(v)
    except Exception:
        return int(default)


def _cfg_str(key: str, default: str) -> str:
    v = _CFG.get(key, default)
    return str(v) if v is not None and str(v).strip() else default


# 台账写入约束：最小盈亏比（RR = reward / risk）
# - RR 计算口径：以 entry_price（entry_zone 中点）为入场，TP 用第 1 档 take_profit_levels[0]，
#   risk 用 |entry - stop_loss|，reward 用 |tp1 - entry|。
# - 仅用于“是否写入 trade_journal.jsonl”的质量门槛；不影响报告文本本身。
MIN_JOURNAL_RR = _cfg_num("min_journal_rr", 1.2)

# 收盘均线周期、摆动窗口与 Fib 取样上限（须与 gateio_kline_chart 图中 mav 一致）
MA_SHORT = _cfg_int("ma_short", 8)
MA_MID = _cfg_int("ma_mid", 21)
MA_LONG = _cfg_int("ma_long", 55)
SWING_LEN = MA_SHORT  # 近 N 根高/低，与 MA_SHORT 对齐
FIB_WINDOW_MAX = MA_LONG  # Fib 锚点窗口上限

# 摆动拐点（分形）：左右各 K 根内为极值 ≈ 常见「局部高低」争夺参考（无成交量分布，仅 OHLC 近似）
PIVOT_LEFT = _cfg_int("pivot_left", 2)
PIVOT_RIGHT = _cfg_int("pivot_right", 2)
# 分形扫描与「触及」统计共用更长窗口（相对原 120 根，覆盖更大级别结构）
PIVOT_LONG_LOOKBACK_BARS = _cfg_int("pivot_long_lookback_bars", 400)
PIVOT_MERGE_REL = _cfg_num("pivot_merge_rel", 0.001)  # 合并相对距离小于约 0.1% 的价位（视为同一博弈带）
PIVOT_TOUCH_REL = _cfg_num("pivot_touch_rel", 0.001)  # 单根 K 的 high/low 距价位 ≤ 此相对带宽则计一次触及
PIVOT_MTF_MATCH_REL = _cfg_num("pivot_mtf_match_rel", 0.004)  # 4h 与 1d 同侧结构价位相对容差，视为多周期共振

# Ross 式反转 1-2-3：与分形共用左右臂；回看与结构拐点窗口一致便于对齐
METHOD_123_LOOKBACK_BARS = PIVOT_LONG_LOOKBACK_BARS
METHOD_123_RULESET_ID = _cfg_str("method_123_ruleset_id", "ross_123_reversal_fractal_v1")


def _sma(values: list[float], n: int) -> float | None:
    if len(values) < n or n <= 0:
        return None
    return sum(values[-n:]) / n


def _pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def _fmt_px(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    return f"{x:.2f}"


def _fib_levels(anchor_low: float, anchor_high: float) -> dict[str, float]:
    """基于 anchor low/high 计算常用 Fib 水平（0~1）。"""
    span = anchor_high - anchor_low
    return {
        "0.0": anchor_low,
        "0.236": anchor_low + span * 0.236,
        "0.382": anchor_low + span * 0.382,
        "0.5": anchor_low + span * 0.5,
        "0.618": anchor_low + span * 0.618,
        "0.786": anchor_low + span * 0.786,
        "1.0": anchor_high,
    }


def _price_vs_fib_zone(last: float, fib: dict[str, float]) -> str:
    """返回现价所处 Fib 区间标签（如 0.5~0.618）。"""
    order = ["0.0", "0.236", "0.382", "0.5", "0.618", "0.786", "1.0"]
    vals = [fib[k] for k in order]
    if last < vals[0]:
        return "below_0.0"
    if last > vals[-1]:
        return "above_1.0"
    for i in range(len(vals) - 1):
        if vals[i] <= last <= vals[i + 1]:
            return f"{order[i]}~{order[i + 1]}"
    return "unknown"


def _merge_nearby_levels_sorted(ascending: list[float], rel_merge: float) -> list[float]:
    """已升序、去重后的价位，合并相对间距 < rel_merge 的相邻项为均值。"""
    if not ascending:
        return []
    out: list[float] = [ascending[0]]
    for x in ascending[1:]:
        base = out[-1]
        if base == 0:
            out.append(x)
            continue
        if abs(x - base) / abs(base) <= rel_merge:
            out[-1] = (base + x) / 2.0
        else:
            out.append(x)
    return out


def _fractal_pivot_high_prices(
    highs: list[float],
    *,
    left: int,
    right: int,
    min_bar_index: int,
) -> list[float]:
    """局部高点：中心 K 线最高价等于窗口 [i-left, i+right] 内最大值（经典分形压力候选）。"""
    n = len(highs)
    if n < left + right + 1:
        return []
    prices: list[float] = []
    for i in range(left, n - right):
        if i < min_bar_index:
            continue
        window = highs[i - left : i + right + 1]
        hi = highs[i]
        if hi >= max(window) - 1e-12 and abs(hi - max(window)) < 1e-9:
            prices.append(hi)
    return prices


def _fractal_pivot_low_prices(
    lows: list[float],
    *,
    left: int,
    right: int,
    min_bar_index: int,
) -> list[float]:
    n = len(lows)
    if n < left + right + 1:
        return []
    prices: list[float] = []
    for i in range(left, n - right):
        if i < min_bar_index:
            continue
        window = lows[i - left : i + right + 1]
        lo = lows[i]
        m = min(window)
        if lo <= m + 1e-12 and abs(lo - m) < 1e-9:
            prices.append(lo)
    return prices


def _fractal_pivot_high_points(
    highs: list[float],
    *,
    left: int,
    right: int,
    min_bar_index: int,
) -> list[tuple[int, float]]:
    """分形高点，返回 (bar_index, price)。"""
    n = len(highs)
    out: list[tuple[int, float]] = []
    if n < left + right + 1:
        return out
    for i in range(left, n - right):
        if i < min_bar_index:
            continue
        window = highs[i - left : i + right + 1]
        hi = highs[i]
        if hi >= max(window) - 1e-12 and abs(hi - max(window)) < 1e-9:
            out.append((i, hi))
    return out


def _fractal_pivot_low_points(
    lows: list[float],
    *,
    left: int,
    right: int,
    min_bar_index: int,
) -> list[tuple[int, float]]:
    n = len(lows)
    out: list[tuple[int, float]] = []
    if n < left + right + 1:
        return out
    for i in range(left, n - right):
        if i < min_bar_index:
            continue
        window = lows[i - left : i + right + 1]
        lo = lows[i]
        m = min(window)
        if lo <= m + 1e-12 and abs(lo - m) < 1e-9:
            out.append((i, lo))
    return out


def pivot_entry_price(entry: Any) -> float:
    """结构拐点条目：支持 `{"price": float, ...}` 或裸 float（兼容旧数据）。"""
    if isinstance(entry, dict):
        return float(entry["price"])
    return float(entry)


def _method_123_point_dict(idx: int, price: float, times: list[str]) -> dict[str, Any]:
    t = times[idx] if 0 <= idx < len(times) else ""
    return {"bar_index": idx, "price": float(price), "time": t}


def _find_bullish_123_fractal(
    pivot_lows: list[tuple[int, float]],
    pivot_highs: list[tuple[int, float]],
) -> tuple[int, float, int, float, int, float] | None:
    """低点1 → 高点2 → 低点3（点3>点1），点2 高于点1与点3；取最近一组（点3 索引最大）。"""
    pl = sorted(pivot_lows, key=lambda x: x[0])
    ph = sorted(pivot_highs, key=lambda x: x[0])
    for i3, p3 in reversed(pl):
        for i2, p2 in ph:
            if i2 >= i3:
                continue
            for i1, p1 in pl:
                if i1 >= i2:
                    continue
                if p3 > p1 and p2 > p1 and p2 > p3:
                    return (i1, p1, i2, p2, i3, p3)
    return None


def _find_bearish_123_fractal(
    pivot_highs: list[tuple[int, float]],
    pivot_lows: list[tuple[int, float]],
) -> tuple[int, float, int, float, int, float] | None:
    """高点1 → 低点2 → 高点3（点3<点1），点2 低于点1与点3。"""
    ph = sorted(pivot_highs, key=lambda x: x[0])
    pl = sorted(pivot_lows, key=lambda x: x[0])
    for i3, p3 in reversed(ph):
        for i2, p2 in pl:
            if i2 >= i3:
                continue
            for i1, p1 in ph:
                if i1 >= i2:
                    continue
                if p3 < p1 and p2 < p1 and p2 < p3:
                    return (i1, p1, i2, p2, i3, p3)
    return None


def _bullish_123_state(last: float, break_level: float, p3: float) -> str:
    if last > break_level:
        return "above_break"
    if last > p3:
        return "between_p3_and_break"
    return "below_or_at_p3"


def _bearish_123_state(last: float, break_level: float, p3: float) -> str:
    if last < break_level:
        return "below_break"
    if last < p3:
        return "between_break_and_p3"
    return "above_or_at_p3"


def _compute_method_123_facts(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    times: list[str],
    n: int,
) -> dict[str, Any]:
    """
    Ross 反转 1-2-3 的代码骨架：分形拐点 + 几何约束；供报告与 ai_overview，非下单信号。
    """
    lr, rr = PIVOT_LEFT, PIVOT_RIGHT
    lookback = min(METHOD_123_LOOKBACK_BARS, n)
    min_i = max(0, n - lookback)
    ph_pts = _fractal_pivot_high_points(highs, left=lr, right=rr, min_bar_index=min_i)
    pl_pts = _fractal_pivot_low_points(lows, left=lr, right=rr, min_bar_index=min_i)
    last = float(closes[-1]) if closes else 0.0
    out: dict[str, Any] = {
        "ruleset_id": METHOD_123_RULESET_ID,
        "lookback_bars": lookback,
        "bullish": None,
        "bearish": None,
    }
    bull = _find_bullish_123_fractal(pl_pts, ph_pts)
    if bull is not None:
        i1, p1, i2, p2, i3, p3 = bull
        out["bullish"] = {
            "point1": _method_123_point_dict(i1, p1, times),
            "point2": _method_123_point_dict(i2, p2, times),
            "point3": _method_123_point_dict(i3, p3, times),
            "break_level": float(p2),
            "state_vs_structure": _bullish_123_state(last, p2, p3),
        }
    bear = _find_bearish_123_fractal(ph_pts, pl_pts)
    if bear is not None:
        i1, p1, i2, p2, i3, p3 = bear
        out["bearish"] = {
            "point1": _method_123_point_dict(i1, p1, times),
            "point2": _method_123_point_dict(i2, p2, times),
            "point3": _method_123_point_dict(i3, p3, times),
            "break_level": float(p2),
            "state_vs_structure": _bearish_123_state(last, p2, p3),
        }
    return out


_M123_BULL_STATE_CN: dict[str, str] = {
    "above_break": "收盘高于点2参考高（经典「突破点2」形态条件，**非**交易指令）",
    "between_p3_and_break": "收盘介于点3低与点2高之间",
    "below_or_at_p3": "收盘未高于点3低",
}
_M123_BEAR_STATE_CN: dict[str, str] = {
    "below_break": "收盘低于点2参考低（经典「跌破点2」形态条件，**非**交易指令）",
    "between_break_and_p3": "收盘介于点2低与点3高之间",
    "above_or_at_p3": "收盘未低于点3高",
}


def format_method_123_md_lines(stats: dict[str, Any], *, interval_cn: str) -> list[str]:
    """供 full_report：规则法事实短句列表。"""
    m = stats.get("method_123")
    if not m:
        return []
    lines: list[str] = []
    rs = str(m.get("ruleset_id") or METHOD_123_RULESET_ID)
    lb = int(m.get("lookback_bars") or 0)
    bull, bear = m.get("bullish"), m.get("bearish")
    lines.append(
        f"- **{interval_cn}**｜规则集 `{rs}`｜回看约 **{lb}** 根："
        "**1-2-3 以下为代码归一事实**；是否交易由对话结合关注带/Fib 展开，**非**本段指令。\n"
    )
    if not bull and not bear:
        lines.append("- 未检出符合条件的多/空反转 1-2-3；对话中**禁止**自拟点1/点2/点3 价位。\n")
        return lines
    if bull:
        p1, p2, p3d = bull["point1"], bull["point2"], bull["point3"]
        st = str(bull.get("state_vs_structure") or "")
        st_cn = _M123_BULL_STATE_CN.get(st, st)
        t1 = str(p1.get("time") or "—")
        lines.append(
            f"- **多头 1-2-3**：点1 低 **{_fmt_px(float(p1['price']))}**（时标 {t1}）"
            f"、点2 高 **{_fmt_px(float(p2['price']))}**、点3 低 **{_fmt_px(float(p3d['price']))}**；"
            f"突破参考（点2 高）**{_fmt_px(float(bull['break_level']))}**；"
            f"现价相对结构：**{st_cn}**。\n"
        )
    if bear:
        p1, p2, p3d = bear["point1"], bear["point2"], bear["point3"]
        st = str(bear.get("state_vs_structure") or "")
        st_cn = _M123_BEAR_STATE_CN.get(st, st)
        t1 = str(p1.get("time") or "—")
        lines.append(
            f"- **空头 1-2-3**：点1 高 **{_fmt_px(float(p1['price']))}**（时标 {t1}）"
            f"、点2 低 **{_fmt_px(float(p2['price']))}**、点3 高 **{_fmt_px(float(p3d['price']))}**；"
            f"跌破参考（点2 低）**{_fmt_px(float(bear['break_level']))}**；"
            f"现价相对结构：**{st_cn}**。\n"
        )
    return lines


def _count_touches_near_high(
    highs: list[float],
    start: int,
    end_excl: int,
    level: float,
    rel: float,
) -> int:
    """窗口内 high 落入 [level±rel*level] 的 K 线根数（压力带测试次数近似）。"""
    if level <= 0 or start >= end_excl:
        return 0
    c = 0
    for i in range(max(0, start), end_excl):
        if abs(highs[i] - level) / level <= rel:
            c += 1
    return c


def _count_touches_near_low(
    lows: list[float],
    start: int,
    end_excl: int,
    level: float,
    rel: float,
) -> int:
    if level <= 0 or start >= end_excl:
        return 0
    c = 0
    for i in range(max(0, start), end_excl):
        if abs(lows[i] - level) / level <= rel:
            c += 1
    return c


def _structure_pivot_levels(
    highs: list[float],
    lows: list[float],
    n: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """
    返回合并后的结构压力/支撑带条目（含触及次数）、以及实际使用的回看根数。
    触及：同窗口内 high（压力）/ low（支撑）落入价位相对带宽的次数。
    """
    lr = PIVOT_LEFT
    rr = PIVOT_RIGHT
    lookback = min(PIVOT_LONG_LOOKBACK_BARS, n)
    min_i = max(0, n - lookback)
    raw_h = _fractal_pivot_high_prices(highs, left=lr, right=rr, min_bar_index=min_i)
    raw_l = _fractal_pivot_low_prices(lows, left=lr, right=rr, min_bar_index=min_i)
    merged_h = _merge_nearby_levels_sorted(sorted(set(raw_h)), PIVOT_MERGE_REL)
    merged_l = _merge_nearby_levels_sorted(sorted(set(raw_l)), PIVOT_MERGE_REL)

    out_h: list[dict[str, Any]] = []
    for px in merged_h:
        t = _count_touches_near_high(highs, min_i, n, px, PIVOT_TOUCH_REL)
        out_h.append({"price": px, "touches": t})
    out_l: list[dict[str, Any]] = []
    for px in merged_l:
        t = _count_touches_near_low(lows, min_i, n, px, PIVOT_TOUCH_REL)
        out_l.append({"price": px, "touches": t})
    return out_h, out_l, lookback


def inject_mtf_pivot_resonance(frames: list[tuple[str, dict[str, Any]]]) -> None:
    """
    若 frames 中同时含 1d 与 4h，为 4h 的 structure_pivot_* 每条目就地写入
    `mtf_resonance: bool`（与日线同侧拐点价位接近）。
    """
    frame_map = {iv: st for iv, st in frames}
    d1 = frame_map.get("1d")
    h4 = frame_map.get("4h")
    if not d1 or not h4:
        return
    rel = PIVOT_MTF_MATCH_REL

    def _mark(side: str) -> None:
        lower = h4.get(side) or []
        higher = d1.get(side) or []
        hi_ps = [pivot_entry_price(x) for x in higher]
        new_list: list[dict[str, Any]] = []
        for e in lower:
            base = dict(e) if isinstance(e, dict) else {"price": float(e), "touches": 0}
            p = float(base["price"])
            base["mtf_resonance"] = any(
                abs(p - hp) / max(abs(p), abs(hp), 1e-12) <= rel for hp in hi_ps
            )
            new_list.append(base)
        h4[side] = new_list

    _mark("structure_pivot_highs")
    _mark("structure_pivot_lows")


def _sort_pivot_entries_above(
    last: float, entries: list[Any], *, limit: int = 6
) -> list[dict[str, Any]]:
    above = [e for e in entries if pivot_entry_price(e) > last]
    above.sort(key=lambda e: (-int((e.get("touches") if isinstance(e, dict) else 0) or 0), pivot_entry_price(e)))
    norm: list[dict[str, Any]] = []
    for e in above[:limit]:
        norm.append(dict(e) if isinstance(e, dict) else {"price": float(e), "touches": 0})
    return norm


def _sort_pivot_entries_below(
    last: float, entries: list[Any], *, limit: int = 6
) -> list[dict[str, Any]]:
    below = [e for e in entries if pivot_entry_price(e) < last]
    below.sort(
        key=lambda e: (
            -int((e.get("touches") if isinstance(e, dict) else 0) or 0),
            -pivot_entry_price(e),
        )
    )
    norm: list[dict[str, Any]] = []
    for e in below[:limit]:
        norm.append(dict(e) if isinstance(e, dict) else {"price": float(e), "touches": 0})
    return norm


def _format_pivot_entry_short(e: dict[str, Any]) -> str:
    p = _fmt_px(float(e["price"]))
    t = int(e.get("touches") or 0)
    s = f"**{p}**（触及约 **{t}** 次"
    if e.get("mtf_resonance"):
        s += "，与**日线**结构共振"
    return s + "）"


def compute_ohlc_stats(rows: list[dict[str, Any]], *, interval: str | None = None) -> dict[str, Any] | None:
    """从 K 线行列表提取均线、波段、涨跌节奏，供策略卡片与跨品种分析。"""
    closes: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    times: list[str] = []
    for r in rows:
        c = r.get("close")
        if c is None:
            continue
        closes.append(float(c))
        highs.append(float(r.get("high") or c))
        lows.append(float(r.get("low") or c))
        times.append(str(r.get("time") or ""))
    n = len(closes)
    if n < 8:
        return None
    last = closes[-1]
    sma8 = _sma(closes, MA_SHORT)
    sma21 = _sma(closes, MA_MID)
    sma55 = _sma(closes, MA_LONG) if n >= MA_LONG else None
    sl = SWING_LEN
    hh5 = max(highs[-sl:])
    hh_prev = max(highs[-2 * sl : -sl]) if n >= 2 * sl else hh5
    ll5 = min(lows[-sl:])
    ll_prev = min(lows[-2 * sl : -sl]) if n >= 2 * sl else ll5
    ret8 = _pct(last, closes[-(sl + 1)]) if n >= sl + 1 else None
    ret21 = _pct(last, closes[-(MA_MID + 1)]) if n >= MA_MID + 1 else None

    # 用最近窗口构建统一锚点，避免 AI 自行猜测 Fib 起止点。
    anchor_window = min(FIB_WINDOW_MAX, n)
    start = n - anchor_window
    win_highs = highs[start:]
    win_lows = lows[start:]
    win_times = times[start:]
    hi_idx = max(range(anchor_window), key=lambda i: win_highs[i])
    lo_idx = min(range(anchor_window), key=lambda i: win_lows[i])
    anchor_high = win_highs[hi_idx]
    anchor_low = win_lows[lo_idx]
    anchor_high_time = win_times[hi_idx]
    anchor_low_time = win_times[lo_idx]
    fib = _fib_levels(anchor_low, anchor_high) if anchor_high > anchor_low else {}
    price_zone = _price_vs_fib_zone(last, fib) if fib else "unknown"

    struct_hi, struct_lo, pivot_lb = _structure_pivot_levels(highs, lows, n)
    method_123 = _compute_method_123_facts(highs, lows, closes, times, n)

    stats: dict[str, Any] = {
        "last": last,
        "sma8": sma8,
        "sma21": sma21,
        "sma55": sma55,
        "p8": _pct(last, sma8) if sma8 else 0.0,
        "p21": _pct(last, sma21) if sma21 else 0.0,
        "p55": _pct(last, sma55) if sma55 else None,
        "ret8": ret8,
        "ret21": ret21,
        "hh5": hh5,
        "hh_prev": hh_prev,
        "ll5": ll5,
        "ll_prev": ll_prev,
        "n": n,
        "swing_anchor_high": anchor_high,
        "swing_anchor_high_time": anchor_high_time,
        "swing_anchor_low": anchor_low,
        "swing_anchor_low_time": anchor_low_time,
        "fib_anchor_window": anchor_window,
        "fib_levels": fib,
        "price_vs_fib_zone": price_zone,
        # 分形拐点合并带 + 触及次数；与 hh5/ll5 短期极值互补
        "structure_pivot_highs": struct_hi,
        "structure_pivot_lows": struct_lo,
        "structure_pivot_lookback_bars": pivot_lb,
        # Ross 反转 1-2-3：代码骨架；解读与执行由对话层完成
        "method_123": method_123,
    }
    stats["interval"] = interval
    stats["signal_score"] = signal_strength_score(stats)
    stats["market_regime"] = detect_market_regime(stats)
    stats["walk_forward"] = evaluate_walk_forward_stability(closes)
    stats["signal_filter"] = build_signal_filter_decision(stats, interval=interval)
    return stats


def _interval_heading_cn(interval: str) -> str:
    return {"1d": "日线（1d）", "4h": "4 小时（4h）"}.get(interval, interval)


def _structure_tag(stats: dict[str, Any]) -> str:
    """一句话结构标签，供 AI 简报。"""
    last = stats["last"]
    sma21 = stats.get("sma21")
    sma55 = stats.get("sma55")
    hh5, hh_p = stats["hh5"], stats["hh_prev"]
    ll5, ll_p = stats["ll5"], stats["ll_prev"]
    hh_down = hh5 < hh_p * 0.998
    ll_down = ll5 < ll_p * 0.998
    bits: list[str] = []
    if sma55 and sma21:
        if last > sma55 and last < sma21:
            bits.append("夹缝震荡(SMA55上/SMA21下)")
        elif last < sma55 and last < sma21:
            bits.append("双均线下偏弱")
        elif last > sma55 and last > sma21:
            bits.append("双均线上偏强")
        else:
            bits.append("均线混合")
    if hh_down and ll_down:
        bits.append("高低点同下移")
    elif ll_down:
        bits.append("低点下移")
    return "，".join(bits) if bits else "数据不足"


def composite_bias_label(stats: dict[str, Any]) -> str:
    """
    规则化「综合倾向」：偏多 / 偏空 / 震荡偏多 / 震荡偏空 / 震荡中性 / 中性。
    仅基于价与 SMA21/55、摆动，非预测。
    """
    last = stats["last"]
    sma21 = stats.get("sma21")
    sma55 = stats.get("sma55")
    p21 = float(stats.get("p21") or 0.0)
    p55 = stats.get("p55")

    hh5, hh_p = stats["hh5"], stats["hh_prev"]
    ll5, ll_p = stats["ll5"], stats["ll_prev"]
    hh_down = hh5 < hh_p * 0.998
    ll_down = ll5 < ll_p * 0.998

    if not sma21:
        return "中性"

    near21 = abs(p21) < 0.12
    if sma55 and p55 is not None:
        near55 = abs(float(p55)) < 0.12
    else:
        near55 = True
    if near21 and near55:
        return "中性"

    if sma55:
        above21 = last > sma21
        above55 = last > sma55
        if above21 and above55:
            return "偏多"
        if not above21 and not above55:
            return "偏空"
        if above55 and not above21:
            if hh_down and ll_down:
                return "震荡偏空"
            return "震荡中性"
        if not above55 and above21:
            return "震荡偏多"
        return "震荡中性"

    return "偏多" if last > sma21 else "偏空"


def is_signal_weak(stats: dict[str, Any]) -> bool:
    """
    判断是否属于「方向信号不强」：
    - 综合倾向为中性/震荡中性；或
    - 价贴近 SMA21 且短中节奏均弱，且高低点未形成明显同向推进。
    """
    bias = composite_bias_label(stats)
    if bias in {"中性", "震荡中性"}:
        return True

    p21 = abs(float(stats.get("p21") or 0.0))
    ret8 = abs(float(stats.get("ret8") or 0.0))
    ret21 = abs(float(stats.get("ret21") or 0.0))
    hh5, hh_p = stats["hh5"], stats["hh_prev"]
    ll5, ll_p = stats["ll5"], stats["ll_prev"]
    hh_down = hh5 < hh_p * 0.998
    hh_up = hh5 > hh_p * 1.002
    ll_down = ll5 < ll_p * 0.998
    ll_up = ll5 > ll_p * 1.002
    no_clear_swing = not ((hh_up and ll_up) or (hh_down and ll_down))

    return p21 < 0.45 and ret8 < 1.0 and ret21 < 1.5 and no_clear_swing


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _trend_direction_from_bias(bias: str) -> str:
    if bias in {"偏多", "震荡偏多"}:
        return "bullish"
    if bias in {"偏空", "震荡偏空"}:
        return "bearish"
    return "neutral"


def _score_trend(stats: dict[str, Any]) -> int:
    """趋势分（0-40）：同侧站位 + 离均线距离（绝对值）。"""
    last = float(stats["last"])
    sma21 = stats.get("sma21")
    sma55 = stats.get("sma55")
    p21 = abs(float(stats.get("p21") or 0.0))
    p55 = abs(float(stats.get("p55") or 0.0))
    if not sma21:
        return 0
    if sma55:
        same_side = (last > sma21 and last > sma55) or (last < sma21 and last < sma55)
        near21 = p21 < 0.12
        near55 = p55 < 0.12
        if near21 and near55:
            return 4
        base = 22 if same_side else 10
        bonus = min(18.0, p21 * 4.0 + p55 * 2.0)
        return int(round(_clamp(base + bonus, 0, 40)))
    base = 18
    bonus = min(22.0, p21 * 6.0)
    return int(round(_clamp(base + bonus, 0, 40)))


def _score_structure(stats: dict[str, Any]) -> int:
    """结构分（0-30）：摆动推进 + 分形触及密度 + 1-2-3 状态。"""
    hh5, hh_p = float(stats["hh5"]), float(stats["hh_prev"])
    ll5, ll_p = float(stats["ll5"]), float(stats["ll_prev"])
    hh_up = hh5 > hh_p * 1.002
    hh_down = hh5 < hh_p * 0.998
    ll_up = ll5 > ll_p * 1.002
    ll_down = ll5 < ll_p * 0.998
    swing_score = 10 if ((hh_up and ll_up) or (hh_down and ll_down)) else 4

    highs = stats.get("structure_pivot_highs") or []
    lows = stats.get("structure_pivot_lows") or []
    max_touch = 0
    for e in highs + lows:
        if isinstance(e, dict):
            max_touch = max(max_touch, int(e.get("touches") or 0))
    pivot_score = int(round(_clamp(max_touch / 2.0, 0, 10)))

    m = stats.get("method_123") or {}
    states: list[str] = []
    bull = m.get("bullish")
    bear = m.get("bearish")
    if isinstance(bull, dict):
        states.append(str(bull.get("state_vs_structure") or ""))
    if isinstance(bear, dict):
        states.append(str(bear.get("state_vs_structure") or ""))
    if any(s in {"above_break", "below_break"} for s in states):
        m123_score = 10
    elif any(s in {"between_p3_and_break", "between_break_and_p3"} for s in states):
        m123_score = 7
    elif states:
        m123_score = 4
    else:
        m123_score = 0

    return int(round(_clamp(swing_score + pivot_score + m123_score, 0, 30)))


def _score_momentum(stats: dict[str, Any]) -> int:
    """动量分（0-20）：短中收益幅度 + 方向一致性。"""
    ret8 = float(stats.get("ret8") or 0.0)
    ret21 = float(stats.get("ret21") or 0.0)
    amp = min(12.0, abs(ret8) * 3.0 + abs(ret21) * 1.5)

    same_sign = (ret8 > 0 and ret21 > 0) or (ret8 < 0 and ret21 < 0)
    if same_sign and abs(ret8) > 0.6 and abs(ret21) > 1.0:
        sync = 8.0
    elif same_sign:
        sync = 5.0
    else:
        sync = 2.0
    return int(round(_clamp(amp + sync, 0, 20)))


def _score_resonance(stats: dict[str, Any]) -> int:
    """共振分（0-10）：多条件近价聚集 + Fib 所处区间。"""
    last = float(stats["last"])
    if last <= 0:
        return 0
    levels: list[float] = []
    for x in (stats.get("sma21"), stats.get("sma55")):
        if isinstance(x, (int, float)) and x > 0:
            levels.append(float(x))
    for x in _fib_level_values(stats.get("fib_levels")):
        if x > 0:
            levels.append(float(x))
    for e in (stats.get("structure_pivot_highs") or []) + (stats.get("structure_pivot_lows") or []):
        try:
            p = pivot_entry_price(e)
            if p > 0:
                levels.append(p)
        except Exception:
            continue

    band_rel = 0.0035
    near_cnt = 0
    for lv in levels:
        if abs(lv - last) / last <= band_rel:
            near_cnt += 1
    confluence = min(6, near_cnt)

    fib_zone = str(stats.get("price_vs_fib_zone") or "")
    if fib_zone in {"0.0~0.236", "0.786~1.0", "below_0.0", "above_1.0"}:
        fib_score = 3
    elif fib_zone in {"0.236~0.382", "0.618~0.786"}:
        fib_score = 2
    elif fib_zone in {"0.382~0.5", "0.5~0.618"}:
        fib_score = 1
    else:
        fib_score = 0

    return int(round(_clamp(confluence + fib_score, 0, 10)))


def signal_strength_score(stats: dict[str, Any]) -> dict[str, Any]:
    """
    0-100 信号强度分（方向无关的“确定性/清晰度”分数）：
    趋势 40 + 结构 30 + 动量 20 + 共振 10。
    """
    trend = _score_trend(stats)
    structure = _score_structure(stats)
    momentum = _score_momentum(stats)
    resonance = _score_resonance(stats)
    total = int(round(_clamp(trend + structure + momentum + resonance, 0, 100)))
    if total >= 75:
        grade = "A"
    elif total >= 60:
        grade = "B"
    elif total >= 45:
        grade = "C"
    else:
        grade = "D"
    bias = composite_bias_label(stats)
    return {
        "version": "v1",
        "total": total,
        "grade": grade,
        "direction": _trend_direction_from_bias(bias),
        "components": {
            "trend_40": trend,
            "structure_30": structure,
            "momentum_20": momentum,
            "resonance_10": resonance,
        },
    }


def detect_market_regime(stats: dict[str, Any]) -> dict[str, Any]:
    """
    市场状态（Regime）轻量判定，供信号过滤使用。
    """
    last = float(stats["last"])
    sma21 = stats.get("sma21")
    sma55 = stats.get("sma55")
    p21 = float(stats.get("p21") or 0.0)
    p55 = float(stats.get("p55") or 0.0)
    ret21 = float(stats.get("ret21") or 0.0)
    hh5 = float(stats.get("hh5") or last)
    ll5 = float(stats.get("ll5") or last)
    range_pct = abs(hh5 - ll5) / max(abs(last), 1e-12) * 100.0

    above21 = bool(sma21 and last > float(sma21))
    above55 = bool(sma55 and last > float(sma55))
    below21 = bool(sma21 and last < float(sma21))
    below55 = bool(sma55 and last < float(sma55))
    trend_strength = abs(p21) + abs(p55)

    regime_id = "transition"
    regime_cn = "过渡震荡"
    conf = 52

    if trend_strength < 0.28 and range_pct < 1.2:
        regime_id = "range"
        regime_cn = "窄幅震荡"
        conf = 74
    elif range_pct >= 2.0 and trend_strength < 0.9:
        regime_id = "high_vol_chop"
        regime_cn = "高波动震荡"
        conf = 68
    elif above21 and above55 and ret21 > 0:
        regime_id = "trend_up"
        regime_cn = "趋势上行"
        conf = int(round(_clamp(62 + min(26.0, trend_strength * 6.0), 55, 92)))
    elif below21 and below55 and ret21 < 0:
        regime_id = "trend_down"
        regime_cn = "趋势下行"
        conf = int(round(_clamp(62 + min(26.0, trend_strength * 6.0), 55, 92)))

    return {
        "id": regime_id,
        "label": regime_cn,
        "confidence": int(_clamp(conf, 0, 100)),
        "range_pct_swing": round(range_pct, 3),
    }


def evaluate_walk_forward_stability(closes: list[float]) -> dict[str, Any]:
    """
    轻量 Walk-forward 稳定性：按时间分段，比较“段末均线方向”与“下一段收益方向”一致率。
    """
    n = len(closes)
    if n < 40:
        return {
            "enabled": False,
            "segments": 0,
            "alignment_ratio": None,
            "score": 45,
            "note": "样本不足（<40）",
        }

    seg = max(10, n // 5)
    starts = list(range(0, n - seg + 1, seg))
    fold_signals: list[int] = []
    fold_rets: list[float] = []
    for s in starts:
        e = min(n, s + seg)
        part = closes[s:e]
        if len(part) < 8:
            continue
        sma21 = _sma(part, MA_MID)
        if sma21 is None:
            continue
        end_px = float(part[-1])
        sig = 1 if end_px > sma21 else -1
        ret = _pct(end_px, float(part[0]))
        fold_signals.append(sig)
        fold_rets.append(ret)

    if len(fold_signals) < 3:
        return {
            "enabled": False,
            "segments": len(fold_signals),
            "alignment_ratio": None,
            "score": 48,
            "note": "分段不足",
        }

    hits = 0
    valid = 0
    for i in range(len(fold_signals) - 1):
        nxt_ret = fold_rets[i + 1]
        if abs(nxt_ret) < 0.05:
            continue
        valid += 1
        if fold_signals[i] * nxt_ret > 0:
            hits += 1
    if valid == 0:
        ratio = 0.5
    else:
        ratio = hits / valid

    abs_rets = [abs(x) for x in fold_rets]
    avg_abs = sum(abs_rets) / max(len(abs_rets), 1)
    strength = min(1.0, avg_abs / 1.2)
    score = int(round(_clamp(ratio * 75.0 + strength * 25.0, 0, 100)))
    return {
        "enabled": True,
        "segments": len(fold_signals),
        "alignment_ratio": round(ratio, 3),
        "score": score,
        "note": "ok",
    }


def build_signal_filter_decision(stats: dict[str, Any], *, interval: str | None = None) -> dict[str, Any]:
    """
    信号过滤结论：可执行 / 观察 / 回避。
    """
    sscore = stats.get("signal_score") or {}
    total = int(sscore.get("total") or 0)
    weak = is_signal_weak(stats)
    regime = stats.get("market_regime") or {}
    regime_id = str(regime.get("id") or "transition")
    wf = stats.get("walk_forward") or {}
    wf_score = int(wf.get("score") or 0)
    direction = str(sscore.get("direction") or "neutral")

    interval_key = (interval or "").lower()
    # 基础阈值按周期分开：短周期更严格，长周期更宽。
    interval_thresholds: dict[str, tuple[int, int]] = {
        "15m": (64, 60),
        "1h": (62, 58),
        "4h": (58, 55),
        "1d": (54, 52),
    }
    base_total, base_wf = interval_thresholds.get(interval_key, (60, 55))

    regime_bump = {
        "trend_up": 58,
        "trend_down": 58,
        "range": 64,
        "high_vol_chop": 70,
        "transition": 66,
    }
    # regime 只做“附加门槛”，不回退基础周期门槛。
    min_total = max(base_total, int(regime_bump.get(regime_id, base_total)))
    min_wf = base_wf + (3 if regime_id in {"high_vol_chop", "transition"} else 0)

    reasons: list[str] = []
    if total < min_total:
        reasons.append(f"强度分 {total} 低于阈值 {min_total}")
    if wf_score < min_wf:
        reasons.append(f"walk-forward 稳定性 {wf_score} 低于阈值 {min_wf}")
    if weak:
        reasons.append("方向信号弱（弱信号规则触发）")

    if regime_id == "trend_up" and direction == "bearish":
        reasons.append("方向与上行 regime 不一致")
    if regime_id == "trend_down" and direction == "bullish":
        reasons.append("方向与下行 regime 不一致")

    if not reasons:
        decision = "executable"
        decision_cn = "可执行"
    elif total >= max(45, min_total - 10) and wf_score >= 45:
        decision = "observe"
        decision_cn = "观察"
    else:
        decision = "avoid"
        decision_cn = "回避"

    return {
        "decision": decision,
        "decision_cn": decision_cn,
        "interval": interval_key or "default",
        "thresholds": {
            "min_total_score": min_total,
            "min_walk_forward_score": min_wf,
        },
        "reasons": reasons[:4],
    }


def _fib_level_values(fib: dict[str, Any] | None) -> list[float]:
    if not fib:
        return []
    out: list[float] = []
    for v in fib.values():
        if isinstance(v, (int, float)):
            out.append(float(v))
    return out


def _resistance_above_last(last: float, stats: dict[str, Any]) -> list[float]:
    """
    现价上方的压力阶梯候选，与「关注带」口径一致，并并入本周期已算的 Fib 水平（去重排序）。
    不含「自编锚点」：Fib 仅来自 stats['fib_levels']。
    """
    cands: list[float] = []
    for x in (stats.get("sma55"), stats.get("sma21"), stats["hh5"], stats["hh_prev"]):
        if x is not None and float(x) > last:
            cands.append(float(x))
    for px in stats.get("structure_pivot_highs") or []:
        v = pivot_entry_price(px)
        if v > last:
            cands.append(v)
    cands.extend(fx for fx in _fib_level_values(stats.get("fib_levels")) if fx > last)
    return sorted(set(cands))


def _support_below_last(last: float, stats: dict[str, Any]) -> list[float]:
    """现价下方的支撑阶梯候选（均线摆动 + Fib）。"""
    cands: list[float] = []
    for x in (stats.get("sma55"), stats["ll5"], stats["ll_prev"]):
        if x is not None and float(x) < last:
            cands.append(float(x))
    for px in stats.get("structure_pivot_lows") or []:
        v = pivot_entry_price(px)
        if v < last:
            cands.append(v)
    cands.extend(fx for fx in _fib_level_values(stats.get("fib_levels")) if fx < last)
    return sorted(set(cands), reverse=True)


def _format_price_ladder_md_lines(stats: dict[str, Any], quote: str) -> list[str]:
    """与「关注带」一致的价上/价下阶梯（事实层，非执行单）。"""
    last = stats["last"]
    above = _resistance_above_last(last, stats)
    below = _support_below_last(last, stats)
    fibz = stats.get("price_vs_fib_zone")
    up_txt = "、".join(_fmt_px(x) for x in above) if above else "—（均线/摆动高/Fib 纳入后暂无高于现价的条目）"
    dn_txt = "、".join(_fmt_px(x) for x in below) if below else "—（同上，下方）"
    lines = [
        f"- **现价上方压力（由近到远）**：{up_txt} {quote}\n",
        f"- **现价下方支撑（由近到远）**：{dn_txt} {quote}\n",
    ]
    if fibz and str(fibz) != "unknown":
        lines.append(
            f"- **Fib 位置标签**（锚点与时间见同目录 **ai_overview**）：**{fibz}**\n"
        )
    return lines


def format_trade_scenarios_md(
    stats: dict[str, Any],
    interval: str,
    sec: str,
    quote: str = "USDT",
) -> str:
    """
    情景推演：价位阶梯（结构事实）+ 偏多/偏空路径的条件预演。
    不写入场、止损、止盈执行价，避免误导；执行层由对话 AI 结合 ai_overview 与关注带给出。
    """
    sma21 = stats.get("sma21")
    hh5 = stats["hh5"]
    ll5 = stats["ll5"]

    lines: list[str] = []
    lines.append(f"\n{sec} 情景推演（技术参考，概率非保证）\n")
    bias = composite_bias_label(stats)
    sscore = stats.get("signal_score") or {}
    comps = sscore.get("components") or {}
    regime = stats.get("market_regime") or {}
    wf = stats.get("walk_forward") or {}
    filt = stats.get("signal_filter") or {}
    if sscore:
        lines.append(
            "- **信号强度**："
            f"**{int(sscore.get('total', 0))}/100（{sscore.get('grade', '—')}）**"
            f"｜趋势 {int(comps.get('trend_40', 0))}/40，"
            f"结构 {int(comps.get('structure_30', 0))}/30，"
            f"动量 {int(comps.get('momentum_20', 0))}/20，"
            f"共振 {int(comps.get('resonance_10', 0))}/10。\n"
        )
    if regime:
        lines.append(
            f"- **市场状态（Regime）**：**{regime.get('label', '—')}**（置信度约 {int(regime.get('confidence', 0))}/100）。\n"
        )
    if wf:
        wf_ratio = wf.get("alignment_ratio")
        wf_ratio_txt = f"{float(wf_ratio) * 100:.1f}%" if isinstance(wf_ratio, (int, float)) else "—"
        lines.append(
            f"- **Walk-forward 稳定性**：**{int(wf.get('score', 0))}/100**（分段 {int(wf.get('segments', 0))}，方向一致率 {wf_ratio_txt}）。\n"
        )
    if filt:
        lines.append(
            f"- **信号过滤结论**：**{filt.get('decision_cn', '观察')}**"
            f"（阈值：强度≥{int((filt.get('thresholds') or {}).get('min_total_score', 0))}，"
            f"WF≥{int((filt.get('thresholds') or {}).get('min_walk_forward_score', 0))}）。\n"
        )
        rs = filt.get("reasons") or []
        if rs:
            lines.append(f"- **过滤原因**：{'；'.join(str(x) for x in rs[:3])}。\n")
    lines.append(
        f"- **综合倾向**：**{bias}**（规则归纳，非投资建议）\n"
    )
    if is_signal_weak(stats):
        lines.append(
            f"- **结论**：当前 **{interval}** 方向信号不强，暂按「**无明显方向**」处理；"
            "优先等待关键位突破/跌破并收盘确认，再决定是否切换到趋势预演。\n"
        )
        lines.append(
            f"- **观察位**：上沿关注 **{_fmt_px(hh5)}** / SMA21（约 {_fmt_px(sma21) if sma21 else '—'}），"
            f"下沿关注 **{_fmt_px(ll5)}**。\n"
        )
        lines.append(
            "\n*以上价位由历史 K 线自动推算，**非交易指令**；实盘须自定杠杆、仓位与滑点。*\n"
        )
        return "".join(lines)

    sub = "####" if sec == "###" else "#####"
    lines.append(f"{sub} 价位阶梯（结构事实，非执行单）\n")
    lines.extend(_format_price_ladder_md_lines(stats, quote))
    lines.append(
        "- **说明**：具体**入场、止损、止盈数值**不在此生成；须在对话解读中结合本节阶梯、上文「关注带」与同目录 **ai_overview**（`fib_levels`、锚点）给出。\n"
    )

    lines.append(f"\n{sub} 偏多路径（条件预演）\n")
    lines.append(
        f"- **触发**：{interval} **收盘站上 SMA21（约 {_fmt_px(sma21) if sma21 else '—'} {quote}）** 且量能配合；"
        "或下探下方支撑带后出现明确止跌结构再收回。\n"
    )
    lines.append(
        f"- **结构失效（参考）**：有效跌破近期摆动低（约 **{_fmt_px(ll5)} {quote}**）"
        f"且反抽不过 SMA21（约 {_fmt_px(sma21) if sma21 else '—'}）。\n"
    )

    lines.append(f"\n{sub} 偏空路径（条件预演）\n")
    lines.append(
        f"- **触发**：{interval} **收盘跌破近期摆动低（约 {_fmt_px(ll5)} {quote}）**"
        f"且反抽不过 SMA21（约 {_fmt_px(sma21) if sma21 else '—'}）；"
        "或反抽至压力带后承压回落。\n"
    )
    lines.append(
        f"- **结构失效（参考）**：**收盘重新站稳 SMA21**（约 {_fmt_px(sma21) if sma21 else '—'} {quote}）"
        "且量能配合，偏空路径需降级或作废。\n"
    )

    lines.append(
        "\n*以上为 K 线规则归纳，**非交易指令**；实盘须自定杠杆、仓位与滑点。*\n"
    )
    return "".join(lines)


def _append_focus_band_section(
    lines: list[str],
    *,
    sec: str,
    interval: str | None,
    stats: dict[str, Any],
    quote: str,
    pivot_style: str = "single",
    mtf_resonance: bool = False,
) -> None:
    """追加关注带与结构拐点（供单周期/多周期复用）。"""
    last = float(stats["last"])
    sma21 = stats.get("sma21")
    sma55 = stats.get("sma55")
    hh5 = float(stats["hh5"])
    hh_prev = float(stats["hh_prev"])
    ll5 = float(stats["ll5"])

    lines.append(f"\n{sec} 关注带\n")
    if interval:
        lines.append(f"- **执行口径**：{_interval_heading_cn(interval)}。\n")
    res21 = _fmt_px(sma21) if sma21 else "—"
    lines.append(
        f"- **压力**：SMA21 一带（约 **{res21}**）、前高 **{_fmt_px(hh5)}**、"
        f"更远平台 **{_fmt_px(hh_prev)}**。\n"
    )
    if sma55 and float(sma55) < last:
        lines.append(
            f"- **支撑**：SMA55（约 **{_fmt_px(sma55)}**）、近期低 **{_fmt_px(ll5)}**；"
            "跌破并收在下方则结构转弱。\n"
        )
    else:
        lines.append(
            f"- **支撑**：近期低 **{_fmt_px(ll5)}**。"
            "（SMA55 当前在现价上方，更多作为上方约束）\n"
        )

    sph = stats.get("structure_pivot_highs") or []
    spl = stats.get("structure_pivot_lows") or []
    plb = int(stats.get("structure_pivot_lookback_bars") or PIVOT_LONG_LOOKBACK_BARS)
    piv_above = _sort_pivot_entries_above(last, sph, limit=6)
    piv_below = _sort_pivot_entries_below(last, spl, limit=6)
    if not (piv_above or piv_below):
        return

    up_p = "、".join(_format_pivot_entry_short(x) for x in piv_above) if piv_above else "—"
    dn_p = "、".join(_format_pivot_entry_short(x) for x in piv_below) if piv_below else "—"
    if pivot_style == "dual":
        mtf_note = "含 **4h↔1d** 同价共振标注（容差约 0.4%）。" if mtf_resonance else ""
        lines.append(
            f"- **结构拐点（分形，执行周期近 **{plb}** 根；优先触及多者）**："
            f"现价上方 **{up_p}**；下方 **{dn_p}**{mtf_note}"
            "（OHLC 近似，非成交量分布）。\n"
        )
    else:
        lines.append(
            f"- **结构拐点（分形，近 **{plb}** 根内局部高/低；邻近价已合并；"
            f"排序优先触及次数多者）**：现价上方 **{up_p}**；下方 **{dn_p}**"
            "（仅 OHLC 近似，**非**成交量分布/VPOC）。\n"
        )


def _append_discipline_framework_section(
    lines: list[str],
    *,
    sec: str,
    stats: dict[str, Any],
) -> None:
    """追加策略框架（纪律向）（供单周期/多周期复用）。"""
    sma21 = stats.get("sma21")
    ll5 = float(stats["ll5"])
    hh5 = float(stats["hh5"])
    hi_raw = float(sma21) if sma21 else hh5
    lo_px, hi_px = min(ll5, hi_raw), max(ll5, hi_raw)
    lines.append(f"\n{sec} 策略框架（纪律向）\n")
    lines.append(
        "- **偏趋势**：放量站稳 SMA21 再考虑顺势偏多；"
        f"有效跌破近期摆动低（约 **{_fmt_px(ll5)}**）且反抽不过，偏空延续。\n"
    )
    lines.append(
        f"- **偏区间**：**{_fmt_px(lo_px)}～{_fmt_px(hi_px)}** 内高抛低吸需窄止损，"
        "均线粘合区避免重仓追涨杀跌。\n"
    )


def format_strategy_card(
    interval: str,
    pair_symbol: str,
    asset_title: str,
    stats: dict[str, Any],
    quote: str = "USDT",
    *,
    nested: bool = False,
    interval_heading: str | None = None,
    show_disclaimer: bool = True,
    include_scenarios: bool = True,
) -> str:
    """
    单币种：数据摘要 / 结构理解 / 关注带 / 策略框架（纪律向）。
    pair_symbol 如 SOL_USDT。
    nested=True 时用于嵌入双周期报告：以 ### 周期标题 + #### 各小节，且不写顶层 ## 标题。
    """
    sec = "####" if nested else "###"
    last = stats["last"]
    sma8, sma21, sma55 = stats["sma8"], stats["sma21"], stats["sma55"]
    p8, p21 = stats["p8"], stats["p21"]
    p55 = stats["p55"]
    ret8, ret21 = stats["ret8"], stats["ret21"]
    hh5, hh_p = stats["hh5"], stats["hh_prev"]
    ll5, ll_p = stats["ll5"], stats["ll_prev"]

    def ad(p: float) -> str:
        if p > 0.05:
            return "高于"
        if p < -0.05:
            return "低于"
        return "接近"

    lines: list[str] = []
    if nested:
        lines.append(f"### {interval_heading or _interval_heading_cn(interval)}\n\n")
    else:
        lines.append(f"## 一、{asset_title}（{pair_symbol}）\n")
    lines.append(f"{sec} 数据摘要\n")
    s8 = _fmt_px(sma8) if sma8 else "—"
    s21 = _fmt_px(sma21) if sma21 else "—"
    s55 = _fmt_px(sma55) if sma55 else "—"
    sscore = stats.get("signal_score") or {}
    comps = sscore.get("components") or {}
    p55_clause = (
        f"（{ad(p55)}约 **{p55:+.2f}%**）。" if p55 is not None else "。"
    )
    lines.append(
        f"最新收盘约 **{_fmt_px(last)} {quote}**；SMA8 ≈ {s8}（{ad(p8)}约 **{p8:+.2f}%**）；"
        f"SMA21 ≈ {s21}（{ad(p21)}约 **{p21:+.2f}%**）；"
        f"SMA55 ≈ {s55}{p55_clause}\n"
    )
    r8t = f"近 {MA_SHORT} 根涨跌约 **{ret8:+.2f}%**" if ret8 is not None else f"近 {MA_SHORT} 根涨跌（样本不足）"
    r21t = f"近 {MA_MID} 根约 **{ret21:+.2f}%**" if ret21 is not None else f"近 {MA_MID} 根（样本不足）"
    lines.append(f"{r8t}（相对更短周期），{r21t}。\n")
    if sscore:
        lines.append(
            f"**信号强度**：**{int(sscore.get('total', 0))}/100（{sscore.get('grade', '—')}）**，"
            f"分项：趋势 {int(comps.get('trend_40', 0))}/40、结构 {int(comps.get('structure_30', 0))}/30、"
            f"动量 {int(comps.get('momentum_20', 0))}/20、共振 {int(comps.get('resonance_10', 0))}/10。\n"
        )
    regime = stats.get("market_regime") or {}
    wf = stats.get("walk_forward") or {}
    filt = stats.get("signal_filter") or {}
    if regime:
        lines.append(
            f"**Regime**：**{regime.get('label', '—')}**（置信度 {int(regime.get('confidence', 0))}/100）。"
        )
        if wf:
            lines.append(
                f" **WF**：{int(wf.get('score', 0))}/100。"
            )
        if filt:
            lines.append(
                f" **过滤结论**：**{filt.get('decision_cn', '观察')}**。\n"
            )
        else:
            lines.append("\n")
    hh_down = hh5 < hh_p * 0.998
    ll_down = ll5 < ll_p * 0.998
    lines.append(
        f"**摆动**：近 {MA_SHORT} 根高点 {_fmt_px(hh5)}、前一段 {_fmt_px(hh_p)}（"
        + ("高点明显下移" if hh_down else "高点未显著下移")
        + f"）；近 {MA_SHORT} 根低点 "
        f"{_fmt_px(ll5)}、前一段 {_fmt_px(ll_p)}（"
        + ("低点亦下移" if ll_down else "低点未显著下移")
        + "）。\n"
    )

    lines.append(f"\n{sec} 结构理解\n")
    chunks: list[str] = []
    if sma8 and sma21:
        if last > sma8 and last < sma21:
            chunks.append("现价在 **SMA8 上方、SMA21 下方**：短线有反弹动能，**中期仍受 SMA21 压制**。")
        elif last <= sma8 and last < sma21:
            chunks.append("现价在 **短期与中期均线下方**，反弹偏弱。")
        elif last > sma21:
            chunks.append("现价**站在 SMA21 上方**，中期压制减轻。")
        else:
            chunks.append("现价与短中期均线**纠缠**，偏震荡。")
    if sma55 and sma21:
        if last > sma55 and last < sma21:
            chunks.append(
                "同时处于 **SMA55 上方、SMA21 下方** 的夹缝，属常见「大级别未破位、中期偏弱」的震荡修复。"
            )
        elif last < sma55 and last < sma21:
            chunks.append("价在 **SMA55 与 SMA21 之下**，结构偏空，反弹宜谨慎。")
        elif last > sma55 and last > sma21:
            chunks.append("价在 **双均线上方**，趋势类策略占优概率更高。")
    if hh_down and ll_down:
        chunks.append("**高低点同步下移**，偏「反弹偏弱、震荡下行或寻底」而非强势多头。")
    elif not hh_down and ll_down:
        chunks.append("低点下移、高点未抬高，**下沿风险**仍需警惕。")
    lines.append("".join(chunks) + "\n")

    m123_lines = format_method_123_md_lines(stats, interval_cn=_interval_heading_cn(interval))
    if m123_lines:
        lines.append(f"\n{sec} 规则化交易法（代码骨架｜1-2-3）\n")
        lines.extend(m123_lines)

    _append_focus_band_section(
        lines,
        sec=sec,
        interval=None,
        stats=stats,
        quote=quote,
        pivot_style="single",
    )
    _append_discipline_framework_section(
        lines,
        sec=sec,
        stats=stats,
    )
    if include_scenarios:
        lines.append(
            format_trade_scenarios_md(
                stats,
                interval,
                sec,
                quote,
            )
        )
    if show_disclaimer:
        lines.append(
            "\n*（以上为技术指标归纳，不构成投资建议。）*\n"
        )
    return "".join(lines)


def format_dual_asset_strategy(
    pair_symbol: str,
    asset_title: str,
    frames: list[tuple[str, dict[str, Any]]],
    quote: str = "USDT",
) -> str:
    """
    同一交易对多周期：按 frames 顺序（建议 1d 再 4h）拼接嵌套策略卡，末尾统一免责。
    frames: [(interval, stats), ...]，stats 为 compute_ohlc_stats 结果。
    """
    if not frames:
        return f"## 一、{asset_title}（{pair_symbol}）\n\n（K 线过少，无法生成策略卡片。）\n"

    stats_map = {iv: st for iv, st in frames}
    d1 = stats_map.get("1d")
    h4 = stats_map.get("4h")
    exec_iv, exec_stats = ("4h", h4) if h4 is not None else frames[-1]

    def summary(iv: str, st: dict[str, Any]) -> str:
        sma21 = _fmt_px(st["sma21"]) if st.get("sma21") else "—"
        sma55 = _fmt_px(st["sma55"]) if st.get("sma55") else "—"
        ret8 = f"{st['ret8']:+.2f}%" if st.get("ret8") is not None else "样本不足"
        ret21 = f"{st['ret21']:+.2f}%" if st.get("ret21") is not None else "样本不足"
        sscore = st.get("signal_score") or {}
        regime = st.get("market_regime") or {}
        filt = st.get("signal_filter") or {}
        score_txt = ""
        if sscore:
            score_txt = f"；信号强度 **{int(sscore.get('total', 0))}/100（{sscore.get('grade', '—')}）**"
        regime_txt = f"；Regime **{regime.get('label', '—')}**" if regime else ""
        filt_txt = f"；过滤 **{filt.get('decision_cn', '观察')}**" if filt else ""
        return (
            f"- **{_interval_heading_cn(iv)}**：收盘 **{_fmt_px(st['last'])} {quote}**；"
            f"SMA21 **{sma21}**（{st['p21']:+.2f}%），SMA55 **{sma55}**；"
            f"近 {MA_SHORT} 根 **{ret8}**、近 {MA_MID} 根 **{ret21}**；"
            f"高点 {_fmt_px(st['hh5'])} / 前段 {_fmt_px(st['hh_prev'])}，"
            f"低点 {_fmt_px(st['ll5'])} / 前段 {_fmt_px(st['ll_prev'])}{score_txt}{regime_txt}{filt_txt}。"
        )

    lines: list[str] = []
    lines.append(f"## 一、{asset_title}（{pair_symbol}）\n\n")
    lines.append("### 数据摘要\n")
    if d1:
        lines.append(summary("1d", d1) + "\n")
    if h4:
        lines.append(summary("4h", h4) + "\n")
    if not d1 and not h4:
        iv, st = frames[-1]
        lines.append(summary(iv, st) + "\n")

    lines.append("\n### 结构理解\n")
    if d1:
        lines.append(f"- **日线**：{_structure_tag(d1)}。\n")
    if h4:
        lines.append(f"- **4 小时**：{_structure_tag(h4)}。\n")
    if d1 and h4:
        lines.append("- **综合**：日线定主趋势，4h 定执行节奏；若冲突，优先按日线框架处理仓位与方向。\n")

    lines.append("\n### 规则化交易法（代码骨架｜1-2-3）\n")
    if d1:
        lines.extend(format_method_123_md_lines(d1, interval_cn="日线（1d）"))
    if h4:
        lines.extend(format_method_123_md_lines(h4, interval_cn="4 小时（4h）"))
    if not d1 and not h4:
        iv, st = frames[-1]
        lines.extend(format_method_123_md_lines(st, interval_cn=_interval_heading_cn(iv)))

    _append_focus_band_section(
        lines,
        sec="###",
        interval=exec_iv,
        stats=exec_stats,
        quote=quote,
        pivot_style="dual",
        mtf_resonance=bool(d1),
    )
    _append_discipline_framework_section(
        lines,
        sec="###",
        stats=exec_stats,
    )
    lines.append(
        format_trade_scenarios_md(
            exec_stats,
            exec_iv,
            "###",
            quote,
        )
    )
    lines.append("\n*（以上为技术指标归纳，不构成投资建议。）*\n")
    return "".join(lines)


def format_cross_market_analysis(
    items: list[tuple[str, str, dict[str, Any]]],
    interval: str,
    *,
    multi_timeframe_context: bool = False,
) -> str:
    """
    items: [(pair_symbol, asset_title, stats), ...]
    多币种后的「关联分析市场趋势」规则化段落。
    """
    if len(items) < 2:
        return ""

    valid = [(p, t, s) for p, t, s in items if s is not None]
    if len(valid) < 2:
        return ""

    lines: list[str] = []
    lines.append(f"\n---\n\n## 二、关联分析市场趋势（{interval}，规则归纳）\n\n")
    if multi_timeframe_context:
        lines.append(
            f"> **说明**：本节跨品种排序与结构标签均基于 **{interval}**；同币种的 **1d 日线** 结论请对照上文该币「日线（1d）」小节。\n\n"
        )

    ranked = sorted(valid, key=lambda x: x[2].get("p21", -999), reverse=True)
    order_parts = [f"{t}（较 SMA21 **{s['p21']:+.2f}%**）" for _, t, s in ranked]
    lines.append("**相对强弱（现价相对 SMA21 偏离，高者相对强，仅供参考）**：" + " > ".join(order_parts) + "。\n\n")

    risk_names = [t for _, t, s in valid if s.get("sma55") and s["last"] < s["sma55"] and s["last"] < s["sma21"]]
    if risk_names:
        lines.append(
            f"**同步偏弱**：{('、'.join(risk_names))} 同时落在 SMA55 与 SMA21 下方，"
            "若 BTC/ETH 亦同向，整体更偏**风险偏好收缩**；仅个别品种弱则需防结构性分化。\n\n"
        )

    squeeze = [t for _, t, s in valid if s.get("sma55") and s.get("sma21") and s["last"] > s["sma55"] and s["last"] < s["sma21"]]
    if len(squeeze) >= 2:
        lines.append(
            f"**夹缝震荡**：{('、'.join(squeeze))} 等多为「SMA55 上、SMA21 下」，"
            "市场大类资产可能处于**方向选择前**；突破 SMA21 或跌破下沿支撑带前，宜控制仓位与频率。\n\n"
        )

    xaut = next(((p, t, s) for p, t, s in valid if "XAUT" in p or "PAXG" in p), None)
    btc = next(((p, t, s) for p, t, s in valid if p == "BTC_USDT"), None)
    if xaut and btc:
        if xaut[2]["p21"] > btc[2]["p21"] + 0.3:
            lines.append(
                "**黄金代币 vs BTC**：黄金代币相对 SMA21 的偏离优于 BTC 时，或反映**避险/保值**定价略强；"
                "若相反则偏风险偏好。\n\n"
            )
        elif xaut[2]["p21"] < btc[2]["p21"] - 0.3:
            lines.append(
                "**黄金代币弱于 BTC**：风险资产相对更强时，注意黄金代币**跟跌**可能；仍须结合外盘现货黄金。\n\n"
            )

    lines.append(
        "**执行提示**：跨品种结论仅为 K 线统计归纳；实盘需叠加宏观、链上与个人风控。"
        " **不构成投资建议。**\n"
    )
    return "".join(lines)


def format_ai_brief_md(
    items: list[tuple[str, str, list[tuple[str, dict[str, Any]]]]],
    *,
    cross_section: str = "",
    generated_iso: str,
) -> str:
    """
    供模型快速消费的极简 Markdown：结构快照（价、均线偏离、摆动、价上/价下阶梯、Fib 标签）。
    不含脚本生成的入场/止损/止盈执行价；策略价位由对话解读结合 ai_overview 与 full_report 给出。
    items: [(pair_symbol, asset_title, [(interval, stats), ...]), ...]
    """
    lines: list[str] = []
    lines.append("# AI 极简简报\n\n")
    lines.append(f"生成时间（UTC）：{generated_iso}\n\n")
    lines.append(
        "**阅读说明**：本文件为**结构快照**（收盘、SMA21 偏离、结构标签、价上/价下关键位、Fib 标签）。"
        "**不含**脚本生成的入场/止损/止盈执行价；须在对话中结合 **ai_overview**（Fib 锚点与档位）与 **full_report** 关注带自行给出策略。"
        " **须结合** **PNG**；合规 **`DISCLAIMER.md`**。\n\n"
    )
    for pair_sym, asset, frames in items:
        lines.append(f"## {pair_sym}｜{asset}\n")
        if not frames:
            lines.append("- （无有效统计）\n\n")
            continue
        for iv, st in frames:
            last = st["last"]
            ab = _resistance_above_last(last, st)
            be = _support_below_last(last, st)
            up = "、".join(_fmt_px(x) for x in ab[:8]) if ab else "—"
            dn = "、".join(_fmt_px(x) for x in be[:8]) if be else "—"
            fib_note = ""
            pz = st.get("price_vs_fib_zone")
            if pz and str(pz) != "unknown":
                fib_note = f"｜Fib 标签 **{pz}**"
            sscore = st.get("signal_score") or {}
            score_note = ""
            if sscore:
                score_note = f"｜信号强度 **{int(sscore.get('total', 0))}/100（{sscore.get('grade', '—')}）**"
            regime = st.get("market_regime") or {}
            regime_note = f"｜Regime **{regime.get('label', '—')}**" if regime else ""
            filt = st.get("signal_filter") or {}
            filt_note = f"｜过滤 **{filt.get('decision_cn', '观察')}**" if filt else ""
            lines.append(
                f"- **{_interval_heading_cn(iv)}**：收盘 **{_fmt_px(last)}**｜较 SMA21 **{st['p21']:+.2f}%**｜"
                f"结构：**{_structure_tag(st)}**{fib_note}{score_note}{regime_note}{filt_note}\n"
            )
            lines.append(f"  - **价上压力（近→远）**：{up}\n")
            lines.append(f"  - **价下支撑（近→远）**：{dn}\n")
        lines.append("\n")
    if cross_section.strip():
        lines.append("## 跨品种（4h 规则摘要）\n\n")
        lines.append(cross_section.strip() + "\n\n")
    lines.append(
        "---\n*执行层策略（入场/止损/止盈）须在对话中给出；完整结构、关注带与路径预演见 full_report；Fib 锚点见 ai_overview；合规见 `DISCLAIMER.md`。*\n"
    )
    return "".join(lines)
