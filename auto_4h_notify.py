#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每隔一段时间跑一次简报（默认 1 小时），同时生成 4h 与 1h 两个周期的结构快照；
若出现“强信号”，就把可执行策略推送到飞书私聊（open_id）。

判定“强信号”（默认，可通过参数调整）：
  - 4h 或 1h 的 signal_filter.decision_cn == "可执行"
  - 且 signal_score.total >= --min-score（默认 70）
  - 且 walk_forward.score >= --min-wf（默认 55）

推送内容（当前实现）：
  - 仅推送“本轮新增/更新”里筛选出的单条最佳策略单摘要
  - test-send 模式下若本轮无差量，会从全量台账回退挑一条用于链路验证
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from tools.ai_overview import load_ai_overview, merge_ai_overview, write_ai_overview
from tools.config import cfg_get, cfg_str, load_yaml
from tools.feishu_sender import get_tenant_access_token, load_credential, send_text
from tools.time_utils import fmt_from_iso


@dataclass(frozen=True)
class SignalHit:
    pair: str
    interval: str
    score: int | None
    wf: int | None
    decision_cn: str | None


@dataclass(frozen=True)
class JournalDelta:
    """台账差量（用于判定“本轮新增/更新”）。"""

    added_ids: set[str]
    updated_ids: set[str]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_local(dt: datetime) -> str:
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _log(msg: str) -> None:
    print(f"[auto_4h_notify] {msg}", flush=True)


def _ceil_to_next_4h_boundary(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    base = dt.replace(minute=0, second=0, microsecond=0)
    hour = base.hour
    next_hour = ((hour // 4) + 1) * 4
    if next_hour >= 24:
        base = base.replace(hour=0) + timedelta(days=1)
        return base
    return base.replace(hour=next_hour)


def _run_market_brief(out_dir: Path, *, interval: str | None = None, limit: int = 120) -> None:
    """
    运行 gateio_kline_chart.py。
    - interval is None: 使用脚本的“按日期目录自动多周期”逻辑（通常含 4h；新建目录还会含 1d）
    - interval provided: 强制单周期（用于补跑 1h）
    """
    cmd = [sys.executable, str((Path(__file__).resolve().parent / "gateio_kline_chart.py").resolve()), "--market-brief"]
    if interval:
        cmd += ["--single-timeframe", "--interval", str(interval), "--limit", str(int(limit))]
    cmd += ["--out-dir", str(out_dir)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        stderr = (p.stderr or "").strip()
        raise RuntimeError(f"market-brief 失败（code={p.returncode}）: {stderr}")


def _today_session_dir(out_dir: Path, now_utc: datetime) -> Path:
    return out_dir / now_utc.strftime("%Y-%m-%d")


def _load_ai_overview(session_dir: Path) -> dict[str, Any]:
    return load_ai_overview(session_dir)


def _write_ai_overview(session_dir: Path, overview: dict[str, Any]) -> None:
    write_ai_overview(session_dir, overview)


def _merge_ai_overview(primary: dict[str, Any], secondary: dict[str, Any]) -> dict[str, Any]:
    return merge_ai_overview(primary, secondary)


def _load_yaml_config(path: Path) -> dict[str, Any]:
    return load_yaml(path)


def _cfg_get(d: dict[str, Any], key_path: str, default: Any = None) -> Any:
    return cfg_get(d, key_path, default)


def _pick_first_nonempty(*vals: str) -> str:
    for v in vals:
        if (v or "").strip():
            return (v or "").strip()
    return ""

def _cfg_str(d: dict[str, Any], *key_paths: str, default: str = "") -> str:
    for kp in key_paths:
        s = cfg_str(d, kp, default="").strip()
        if s:
            return s
    return default


def _pick_strong_hits(
    overview: dict[str, Any], *, min_score: int, min_wf: int, interval: str
) -> list[SignalHit]:
    hits: list[SignalHit] = []
    for a in overview.get("assets") or []:
        pair = str(a.get("pair") or "")
        frames = a.get("frames") or {}
        fr = frames.get(interval) or {}
        if not isinstance(fr, dict):
            continue
        sf = fr.get("signal_filter") or {}
        decision_cn = (sf.get("decision_cn") or sf.get("decision") or "").strip()
        if decision_cn != "可执行":
            continue
        score = (fr.get("signal_score") or {}).get("total")
        wf = (fr.get("walk_forward") or {}).get("score")
        try:
            score_i = int(score) if score is not None else None
        except Exception:
            score_i = None
        try:
            wf_i = int(wf) if wf is not None else None
        except Exception:
            wf_i = None
        if score_i is None or wf_i is None:
            continue
        if score_i < min_score or wf_i < min_wf:
            continue
        hits.append(SignalHit(pair=pair, interval=interval, score=score_i, wf=wf_i, decision_cn=decision_cn))
    return hits


def _read_trade_journal(out_dir: Path) -> str:
    p = out_dir / "trade_journal_readable.md"
    if not p.is_file():
        return "（未找到 trade_journal_readable.md；请先运行 market-brief 生成台账视图。）"
    return p.read_text(encoding="utf-8", errors="replace")

def _journal_file(out_dir: Path) -> Path:
    return out_dir / "trade_journal.jsonl"


def _parse_journal_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _idx_journal(entries: list[dict[str, Any]]) -> dict[str, str]:
    """idea_id -> updated_at_utc（字符串）"""
    out: dict[str, str] = {}
    for e in entries:
        idea_id = str(e.get("idea_id") or "").strip()
        if not idea_id:
            continue
        out[idea_id] = str(e.get("updated_at_utc") or e.get("created_at_utc") or "")
    return out


def _diff_journal(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> JournalDelta:
    b = _idx_journal(before)
    a = _idx_journal(after)
    added = set(a.keys()) - set(b.keys())
    updated = {k for k in a.keys() & b.keys() if (a.get(k) or "") != (b.get(k) or "")}
    return JournalDelta(added_ids=added, updated_ids=updated)


def _entry_mid(e: dict[str, Any]) -> float | None:
    if isinstance(e.get("entry_price"), (int, float)):
        return float(e["entry_price"])
    z = e.get("entry_zone")
    if isinstance(z, list) and len(z) == 2 and all(isinstance(x, (int, float)) for x in z):
        return (float(z[0]) + float(z[1])) / 2.0
    return None


def _fmt_num(x: Any) -> str:
    if isinstance(x, (int, float)):
        # 价格：大于 1000 走千分位
        if abs(float(x)) >= 1000:
            return f"{float(x):,.2f}"
        return f"{float(x):.4f}".rstrip("0").rstrip(".")
    return str(x)


def _fmt_ts_beijing(ts: Any) -> str:
    return fmt_from_iso(str(ts) if ts is not None else None, tz="Asia/Shanghai")


def _pick_tp1(e: dict[str, Any]) -> float | None:
    tps = e.get("take_profit_levels")
    if isinstance(tps, list) and tps and isinstance(tps[0], (int, float)):
        return float(tps[0])
    return None


def _calc_rr(e: dict[str, Any]) -> float | None:
    entry = _entry_mid(e)
    stop = e.get("stop_loss")
    tp1 = _pick_tp1(e)
    if entry is None or not isinstance(stop, (int, float)) or tp1 is None:
        return None
    entry = float(entry)
    stop = float(stop)
    tp1 = float(tp1)
    direction = str(e.get("direction") or "").lower()
    if direction == "long":
        risk = entry - stop
        reward = tp1 - entry
    elif direction == "short":
        risk = stop - entry
        reward = entry - tp1
    else:
        return None
    if risk <= 0:
        return None
    return reward / risk


def _pick_best_order(orders: list[dict[str, Any]]) -> dict[str, Any] | None:
    """从本轮新增/更新的订单里挑 1 条最值得推送的。"""

    def key(e: dict[str, Any]) -> tuple[int, int, float]:
        # 优先 tactical > swing；优先 pending；优先 rr 高
        plan = 1 if str(e.get("plan_type") or "") == "tactical" else 0
        pending = 1 if str(e.get("status") or "") == "pending" else 0
        rr = e.get("rr")
        rr_v: float | None = float(rr) if isinstance(rr, (int, float)) else _calc_rr(e)
        rr_v = rr_v if rr_v is not None else -1.0
        return (plan, pending, rr_v)

    if not orders:
        return None
    return sorted(orders, key=key, reverse=True)[0]


def _format_order_message(e: dict[str, Any]) -> str:
    pair = str(e.get("pair") or "—")
    asset = str(e.get("asset") or pair)
    direction = str(e.get("direction") or "—")
    interval = str(e.get("interval") or "—")
    plan_type = str(e.get("plan_type") or "—")
    entry_mid = _entry_mid(e)
    entry_zone = e.get("entry_zone")
    stop = e.get("stop_loss")
    tps = e.get("take_profit_levels") if isinstance(e.get("take_profit_levels"), list) else []
    score = e.get("signal_score_total")
    wf = e.get("walk_forward_score")
    thresholds = e.get("signal_filter_thresholds") or {}
    decision_cn = "可执行"
    # 兼容老记录没有 order_kind_cn
    order_kind = str(e.get("order_kind_cn") or ("挂单" if e.get("entry_type") == "limit" else "实时单"))
    valid_until_bj = _fmt_ts_beijing(e.get("valid_until_utc"))
    rr = e.get("rr")
    rr_v: float | None = float(rr) if isinstance(rr, (int, float)) else _calc_rr(e)
    rr_s = f"{rr_v:.3f}" if rr_v is not None else "—"

    lines = [
        f"币种：{asset}（{pair}）",
        f"周期：{interval}｜计划类型：{plan_type}",
        f"方向：{direction}",
        f"入场点位：{_fmt_num(entry_mid) if entry_mid is not None else '—'}（{order_kind}）",
        f"入场区间：{entry_zone if isinstance(entry_zone, list) else '—'}",
        f"止损：{_fmt_num(stop) if isinstance(stop, (int, float)) else '—'}",
        f"止盈：{tps if tps else '—'}",
        f"盈亏比RR（TP1）：{rr_s}",
        f"信号强度：{score if score is not None else '—'}｜WF：{wf if wf is not None else '—'}｜过滤：{decision_cn}",
        f"阈值：{thresholds if isinstance(thresholds, dict) else '—'}",
        f"有效期至（北京时间）：{valid_until_bj}",
    ]
    reason = str(e.get("strategy_reason") or "").strip()
    if reason:
        lines.append(f"理由：{reason}")
    return "\n".join(lines)


def _build_message(overview: dict[str, Any], hits: list[SignalHit], journal_md: str) -> str:
    gen = str(overview.get("generated_at_utc") or "—")
    header = [
        f"[CryptoTradeDesk] 触发强信号（4h/1h）",
        f"- generated_at_utc: {gen}",
        "- hits: " + ", ".join(f"{h.pair}/{h.interval}(score={h.score},wf={h.wf})" for h in hits),
        "",
        "=== trade_journal_readable.md ===",
        "",
    ]
    body = "\n".join(header) + journal_md.strip() + "\n"
    # 飞书 text 长度存在上限；保守截断
    max_len = 9000
    if len(body) > max_len:
        tail_note = "\n\n（内容过长，已截断；请到服务器查看完整 trade_journal_readable.md）\n"
        body = body[: max_len - len(tail_note)] + tail_note
    return body


def _send(open_id: str, msg: str, *, app_id: str, app_secret: str) -> dict[str, Any]:
    cred = load_credential(app_id, app_secret)
    token = get_tenant_access_token(cred)
    return send_text(open_id, msg, token)


def _ok_resp(resp: dict[str, Any]) -> bool:
    code = resp.get("code")
    return code in (0, "0", None)


def main() -> int:
    ap = argparse.ArgumentParser(description="Auto run 4h market-brief and Feishu notify.")
    ap.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "output"))
    ap.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parent / "config" / "analysis_defaults.yaml"),
        help="默认配置 YAML（用于读取飞书凭据/open_id/阈值）",
    )
    ap.add_argument("--min-score", type=int, default=0, help="强信号最小 score（0=用 YAML 默认）")
    ap.add_argument("--min-wf", type=int, default=0, help="强信号最小 walk-forward（0=用 YAML 默认）")
    ap.add_argument("--interval-hours", type=int, default=0, help="运行间隔小时数（0=用 YAML 默认）")
    ap.add_argument("--align-4h", action="store_true", help="对齐到 UTC 4h 边界再跑（默认可由 YAML 指定）")
    ap.add_argument("--test-send", action="store_true", help="测试发送：无论是否命中强信号都发一条")
    ap.add_argument("--once", action="store_true", help="只运行一次（不常驻）")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = _load_yaml_config(Path(args.config).resolve())
    # Feishu creds: always from YAML (no env/cli overrides)
    open_id = _cfg_str(cfg, "FEISHU_OPEN_ID", "feishu.open_id")
    app_id = _cfg_str(cfg, "FEISHU_APP_ID", "feishu.app_id")
    app_secret = _cfg_str(cfg, "FEISHU_APP_SECRET", "feishu.app_secret")
    if not open_id:
        raise RuntimeError("缺少飞书 open_id：请在 YAML 中填写 FEISHU_OPEN_ID 或 feishu.open_id")
    if not app_id or not app_secret:
        raise RuntimeError("缺少飞书凭据：请在 YAML 中填写 FEISHU_APP_ID/FEISHU_APP_SECRET 或 feishu.app_id/feishu.app_secret")

    min_score = int(args.min_score) if int(args.min_score) > 0 else int(_cfg_get(cfg, "auto_notify.min_score", 70))
    min_wf = int(args.min_wf) if int(args.min_wf) > 0 else int(_cfg_get(cfg, "auto_notify.min_wf", 55))
    interval_hours = (
        int(args.interval_hours) if int(args.interval_hours) > 0 else int(_cfg_get(cfg, "auto_notify.interval_hours", 1))
    )
    align_4h_default = bool(_cfg_get(cfg, "auto_notify.align_4h", False))
    align_4h = bool(args.align_4h) or align_4h_default

    _log(
        "启动："
        f"min_score={min_score}, min_wf={min_wf}, interval_hours={interval_hours}, "
        f"align_4h={align_4h}, once={args.once}, test_send={args.test_send}"
    )

    while True:
        now = _utc_now()
        _log(f"开始新一轮：now_utc={_fmt_utc(now)} | now_local={_fmt_local(now)}")
        if align_4h and not args.once:
            nxt = _ceil_to_next_4h_boundary(now)
            sleep_s = max(0.0, (nxt - now).total_seconds())
            if sleep_s > 1:
                _log(
                    f"对齐 4h 边界：next_utc={_fmt_utc(nxt)} | next_local={_fmt_local(nxt)} | "
                    f"sleep={int(sleep_s)}s"
                )
                time.sleep(sleep_s)

        now = _utc_now()
        # 运行前记录台账快照，用于判定“本轮新增/更新”
        journal_before = _parse_journal_jsonl(_journal_file(out_dir))
        # 先跑“自动多周期”（通常含 4h；新建目录还会含 1d），再补跑 1h。
        # 因为 ai_overview.json 每次运行会覆盖写入，所以需要把两次的 overview 合并后再写回。
        _run_market_brief(out_dir, interval=None)
        journal_after = _parse_journal_jsonl(_journal_file(out_dir))
        delta = _diff_journal(journal_before, journal_after)
        session_dir = _today_session_dir(out_dir, now)
        overview_4h = _load_ai_overview(session_dir)

        _run_market_brief(out_dir, interval="1h", limit=120)
        overview_1h = _load_ai_overview(session_dir)
        overview = _merge_ai_overview(overview_4h, overview_1h)
        _write_ai_overview(session_dir, overview)

        hits: list[SignalHit] = []
        hits.extend(_pick_strong_hits(overview, min_score=min_score, min_wf=min_wf, interval="4h"))
        hits.extend(_pick_strong_hits(overview, min_score=min_score, min_wf=min_wf, interval="1h"))
        hit_details = (
            ", ".join(f"{h.pair}/{h.interval}(score={h.score},wf={h.wf})" for h in hits) if hits else "none"
        )
        _log(
            f"本轮结果：hits={len(hits)}, journal_added={len(delta.added_ids)}, "
            f"journal_updated={len(delta.updated_ids)}, hit_list={hit_details}"
        )
        if hits or args.test_send:
            hit_pairs = {h.pair for h in hits} if hits else set()
            # 本轮新增/更新的策略单
            changed_ids = delta.added_ids | delta.updated_ids
            changed_orders = [e for e in journal_after if str(e.get("idea_id") or "") in changed_ids]
            # 仅推送命中强信号币种（test-send 则不过滤）
            if hit_pairs:
                changed_orders = [e for e in changed_orders if str(e.get("pair") or "") in hit_pairs]

            best = _pick_best_order(changed_orders)
            if best is None:
                # 没有差量策略单就不发；test-send 也只发“单条策略单摘要”（不再回退发送整个文件）
                if args.test_send:
                    # test-send：从全量台账里挑一条 pending（若无则挑最新），用于验证发送链路与格式
                    fallback_pool = journal_after
                    best = _pick_best_order(fallback_pool)
                    msg = _format_order_message(best) if best is not None else "（test-send：未找到可推送的策略单）"
                else:
                    msg = ""
            else:
                msg = _format_order_message(best)

            if not msg.strip():
                _log("本轮无可推送策略单：不发送飞书消息。")
                if args.once:
                    _log("单次模式结束。")
                    return 0
                sleep_secs = max(60, int(interval_hours * 3600))
                next_run = _utc_now() + timedelta(seconds=sleep_secs)
                _log(
                    f"执行完成，等待下次运行：sleep={sleep_secs}s | "
                    f"next_utc={_fmt_utc(next_run)} | next_local={_fmt_local(next_run)}"
                )
                time.sleep(sleep_secs)
                continue

            resp = _send(
                open_id,
                msg,
                app_id=app_id,
                app_secret=app_secret,
            )
            if not _ok_resp(resp):
                raise RuntimeError(f"飞书发送失败: {resp}")
            best_pair = str(best.get("pair") or "—") if best is not None else "—"
            best_interval = str(best.get("interval") or "—") if best is not None else "—"
            _log(f"飞书发送成功：pair={best_pair}, interval={best_interval}, hits={len(hits)}")
        else:
            _log("本轮未命中强信号且未开启 test-send：不发送飞书消息。")

        if args.once:
            _log("单次模式结束。")
            return 0

        sleep_secs = max(60, int(interval_hours * 3600))
        next_run = _utc_now() + timedelta(seconds=sleep_secs)
        _log(
            f"执行完成，等待下次运行：sleep={sleep_secs}s | "
            f"next_utc={_fmt_utc(next_run)} | next_local={_fmt_local(next_run)}"
        )
        time.sleep(sleep_secs)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)

