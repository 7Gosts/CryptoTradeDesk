# CryptoTradeDesk 项目整体分析（2026-04）

## 1. 项目定位

`CryptoTradeDesk` 当前是一个“**结构化行情分析与决策辅助**”项目，不是自动交易执行系统。  
核心目标是：稳定产出可读的技术面结构事实，供人工判断与风控决策使用。

---

## 2. 当前架构概览

- **入口脚本**：`gateio_kline_chart.py`
  - 负责拉取 K 线、计算结构统计、组织报告产物；**默认不生成 PNG**（`--with-charts` 才写 `<slug>_<interval>.png`）。
  - 统一输出到 `output/<UTC日期>/`：**`full_report.md` / `ai_brief.md`** 同日各一份、多次运行**追加**并带「追加记录」头；**`ai_overview.json`** 为末次覆盖。
- **分析引擎**：`kline_analysis.py`
  - 负责特征提取、结构标注、文案组装。
  - 包含均线、Fib、分形结构、1-2-3、强度评分、Regime、WF、信号过滤。
- **规则文档**：
  - `AI_行情对话提示.md`：主规则与输出口径（唯一准绳）。
  - `AGENT_说明.md`：速查入口与简化指令。

---

## 3. 数据与产物链路

1. 读取市场配置（`config/market_config.json`）得到交易对。
2. 拉取 K 线；**可选**生成 PNG（`--with-charts` / `--chart-only`）。
3. 计算结构统计（`compute_ohlc_stats`）：
   - 均线与偏离、摆动高低点
   - Fib 锚点与区间
   - 分形结构位与触及次数
   - Ross 1-2-3 结构事实
   - 信号强度 `signal_score`（0-100）
   - 市场状态 `market_regime`
   - 轻量 WF 稳定性 `walk_forward`
   - 决策过滤 `signal_filter`（可执行/观察/回避）
4. 输出报告：
   - `full_report.md`（人读细节，同日聚合追加）
   - `ai_brief.md`（模型快读，同日聚合追加）
   - `ai_overview.json`（结构化字段，**仅末次运行**）

---

## 4. 已形成的优势

- **解释性强**：所有关键结论都能回溯到明确规则字段。
- **执行口径清楚**：脚本产“事实层”，对话层产“执行层”。
- **跨窗口可复用**：提示词体系已基本统一。
- **层2能力已接入**：Regime + WF + 过滤结果，能显著减少低质量信号。

---

## 5. 当前主要风险/限制

- **规则驱动非学习驱动**：预测上限受限于人工规则设计。
- **WF 为轻量代理**：当前稳定性评估是简化版，不等同完整 walk-forward 回测框架。
- **执行仍人工主导**：这是设计选择（优点是可控，缺点是需要纪律执行）。

---

## 6. 本次冗余清理记录

针对多轮需求改动后的“过程性冗余/矛盾”，本次已处理：

- **清理关注带语义矛盾**：当 `SMA34` 位于现价上方时，不再在报告中机械写为“支撑”。
  - 现改为：仅在 `SMA34 < last` 时作为支撑输出；
  - 否则明确说明其更偏上方约束（避免“上方价位被写成支撑”的语义冲突）。
- **主文档口径补齐**：提示词中已同步声明 `market_regime`、`walk_forward`、`signal_filter` 字段，避免规则与代码脱节。

---

## 7. 后续建议（按优先级）

1. **完整 WF 评估模块**（训练窗/验证窗滚动）  
2. **信号过滤回测报表**（过滤前后胜率、盈亏比、回撤对比）  
3. **输出归档策略**（长期运行时控制 output 文件增长）

> 已完成：`signal_filter` 阈值按周期分开（15m/1h/4h/1d），并叠加 Regime 门槛。

---

## 8. 结论

项目当前已从“规则输出”升级到“规则 + 质量控制输出”，方向正确。  
若目标是进一步提高策略胜率上限，下一阶段应把“轻量 WF”升级为“完整滚动验证 + 过滤效果量化复盘”。

---

## 9. 交易台账（代码化）

为避免“只靠对话记忆”的漂移，项目已加入**代码化台账**：

- 文件：`CryptoTradeDesk/output/trade_journal.jsonl`
- 写入时机：每次运行 `--market-brief` 或单币报告模式且存在有效统计时
- 记录门槛：仅高质量候选（`signal_filter=executable`，或高分 `observe`）
- 自动更新：
  - 成交状态：`pending -> filled/expired`
  - 出局状态：`tp/sl/float_profit/float_loss`
  - 并回写 `updated_at_utc`
- 自动统计：
- 每次运行后自动刷新 `output/trade_journal_stats_latest.md`
  - 统计口径含近 7 天/30 天：候选单命中率、止盈率、止损率、平均盈亏比
  - 新增近 30 天**按币种分组**统计（如 BTC/ETH/SOL 各自命中率与盈亏比）
- 人类可读视图：
  - 每次运行后自动刷新 `output/trade_journal_readable.md`（分“未完成/已结束”两段表格）

核心字段包括：
`pair`、`interval`、`direction`、`entry_zone`（成交判定仍用上下沿）、**`entry_price`**（计划入场点，为 `entry_zone` 中点）、**`order_kind_cn`**（**实时单** / **挂单**，由生成时 `signal_last` 是否落在 `entry_zone` 内判定）、**`signal_last`**（生成时收盘价快照）、`stop_loss`、`take_profit_levels`、`fill_price`、`position_risk_pct`、`strategy_reason`、`risk_stop_prob`、`status`、`exit_status`、`signal_score_total`、`market_regime`、`walk_forward_score`、`signal_filter_thresholds`。

**人类可读表 `trade_journal_readable.md`**：列 **入场点位**（pending 用 `entry_price`，已了结用 `fill_price`）、**开单类型**（实时单 / 挂单），文首有简短口径说明。**入场点位、止损、TP1** 在表中统一格式为 **小数点后一位**。表中时间默认按 **北京时间（`Asia/Shanghai`）** 显示；可用环境变量 **`CRYPTO_TRADEDESK_DISPLAY_TZ`** 指定其他 IANA 时区名。

**价位宽度（易扫损问题）**：台账里的 tactical / swing 价位由 `gateio_kline_chart.py` 自动生成，已按周期使用**最小入场半宽**、**止损距入场中点的最小比例**、**TP 档间距**，避免旧版那种「总宽度仅千分之几、止损贴下一根 pivot」的过窄结构；若你要 **数小时～数日** 的中级单，仍建议在对话解读中按 **更高周期 Fib / `full_report` 关注带** 再放宽止损外沿与分批目标（见 `AI_行情对话提示.md` 对应条款）。

如需手动查看统计，可运行：

```bash
python3 CryptoTradeDesk/trade_journal_stats.py
```

---

## 10. 术语解释（执行与过滤相关）

- **过滤（signal_filter）**：脚本对信号质量做的门槛判断，综合 `signal_score`、`walk_forward`、Regime 等条件，输出最终执行级别。
- **回避（avoid）**：过滤结果中最保守档位。表示当前质量不达标，建议不交易，仅记录观察。
- **观察（observe）**：过滤结果中间档位。允许跟踪关键位与收盘确认，但不建议重仓或提前抢跑。
- **walk-forward**：轻量滚动稳定性指标，用分段方式看信号在不同片段的一致性；分数越高，结构稳定性通常越好（不是完整回测收益）。
- **TP（Take Profit）**：止盈价位。价格到达后按计划分批减仓或平仓，锁定收益。
- **SL（Stop Loss）**：止损价位。价格触发后执行离场，限制单笔亏损扩张。
- **break_level**：1-2-3 结构中的关键突破/跌破参考位（通常对应点2），用于判断结构是否被确认。
- **between_p3_and_break**：当前价格位于「点3」与「break_level」之间，表示结构在临界区，尚未完成有效突破确认。
