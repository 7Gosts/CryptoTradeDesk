# Agent 说明（速查版）

本文件已与 `AI_行情对话提示.md` 合并维护。  
**主规则、输出结构、合规口径全部以 `AI_行情对话提示.md` 为唯一准绳。**

---

## 快速命令

```bash
# 多币（按 config/market_config.json；默认不出图）
python3 CryptoTradeDesk/gateio_kline_chart.py --market-brief --out-dir CryptoTradeDesk/output

# 多币 + 生成 K 线 PNG
python3 CryptoTradeDesk/gateio_kline_chart.py --market-brief --with-charts --out-dir CryptoTradeDesk/output

# 单币（按日期目录自动：首次创建目录跑 1d+4h，之后跑 4h；默认不出图）
python3 CryptoTradeDesk/gateio_kline_chart.py --pair ETH_USDT --out-dir CryptoTradeDesk/output

# 单币 + 出图
python3 CryptoTradeDesk/gateio_kline_chart.py --pair ETH_USDT --with-charts --out-dir CryptoTradeDesk/output

# 仅出图（不写 full_report / ai_brief / ai_overview）
python3 CryptoTradeDesk/gateio_kline_chart.py --pair ETH_USDT --chart-only --out-dir CryptoTradeDesk/output

# 仅出报告（与默认等价：不生成 PNG）
python3 CryptoTradeDesk/gateio_kline_chart.py --pair ETH_USDT --report-only --out-dir CryptoTradeDesk/output
```

---

## 对话提示词建议（自然口令）

下面话术给 **Cursor Agent** 用即可；Agent 会按 `AI_行情对话提示.md` 跑脚本并读产物。**默认只出报告、不出图**；只有当你**明确要图**时，才在话里带上「出图 / 生成图」类表述（或下文括号里的等价句），Agent 应改用 `--with-charts`。

### 1）只要行情分析报告（不要 PNG）

任选其一或自行改写，核心是 **不要** 出现「图 / PNG / K 线图」等出图词：

- 「按 `AI_行情对话提示.md` 跑今日行情：**只要报告**，**不要生成图片**。读完 `ai_brief` → `ai_overview` → `full_report` 后按规范输出，并追加写回 `ai_brief` 附录。」
- 「看下今天 / 现在行情，**文字分析即可**，**不用出 K 线图**。」
- 「例行 market-brief，**仅结构化报告**，**不要 `--with-charts`**。」

### 2）要看图 + 报告（PNG + 分析）

话里必须**明确要图**，例如包含：**「生成 k 线图」「产生图片」「出图」「要 PNG」「带图」** 等，例如：

- 「按 `AI_行情对话提示.md` 跑今日行情，**请生成 K 线图**（`--market-brief --with-charts`），然后结合 **PNG + ai_brief + ai_overview + full_report** 做解读，并追加写回 `ai_brief` 附录。」
- 「看下行情，**要出图**，报告和图一起对照说。」

### 3）只要图、不要长报告（少见）

- 「**只要 K 线 PNG**，不写 full_report：用 `--chart-only` 指定交易对。」（需说明币种，如 ETH_USDT。）

---

## Agent 对话入口（复制）

```text
请按 CryptoTradeDesk/AI_行情对话提示.md 执行：
1) 运行 market-brief（默认不出图；用户明确要图时加 `--with-charts`）
2) 严格按固定顺序读取 output/<日期>/：先时间间隔，再 **ai_brief.md** → **ai_overview.json** → **full_report.md** →（若有）PNG（`<slug>_<interval>.png`）→ DISCLAIMER；默认 `--market-brief` **不出图**，用户明确要图时改跑 `--with-charts`
3) 用中文输出：先逐币后跨币；每币先给粗体摘要，再给触发/入场/失效/止盈止损/风险；贴近关键位但需等待确认时，明确“观察位非建议入场”；文末须给「建议下次复核时间」（结合策略与 `AI_行情对话提示.md` 第 1.2 节）
4) 行情类完整回复后，把本次解读正文**追加**到当日 **ai_brief.md** 末尾（仅追加，不覆盖脚本块）
5) 所有细则以 `AI_行情对话提示.md` 为准，若有冲突以该文件为唯一准绳
```
