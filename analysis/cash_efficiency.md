# Capital Efficiency — The 80%-Cash Pattern

Top-ranked competitors visibly hold ~80% of their starting capital in cash at any given snapshot. This is a deliberate strategy, not capital under-utilization. Note from observation, written up here so we don't forget the reasoning.

---

## 1. What "80% cash" actually means

- Starting cash = $100k per exchange × 10 exchanges = $1M total.
- "80% in cash" = ~$800k unallocated at any snapshot.
- Aggregate position value = ~$200k → ~2,000 shares of $100-priced stock spread across 10 exchanges and 25 instruments.
- Average position per (exchange, instrument) = 2000 / (10 × 25) ≈ 8 shares.

Actual peak on any one line is probably higher (not every line is held simultaneously), but the *average* deployment is small.

---

## 2. Why this dominates

Two reasons. Both fall out of the rules.

### 2.1 Zero fees → turnover is free

With no maker or taker fees, doing 100 round-trips of $0.01 edge on 10 shares = $10 profit, same as 1 round-trip of $1.00 edge on 10 shares. But:

- The 100-trip strategy can re-deploy the same capital 100×.
- The 1-trip strategy ties capital up while the position rides.
- In a 10-min segment, a 100-trip strategy on a single line can re-cycle ~$10k of capital to capture $1000+ of cumulative edge. The 1-trip strategy captures $100.

So in fee-free environments, **turnover beats size**.

### 2.2 Settlement risk + segment reset

Open positions at segment boundary are marked-to-market by an undisclosed methodology (participant guide §13). Anything you hold across the boundary is exposed to mark-out risk you can't model.

Holding small positions, briefly, removes that exposure almost entirely. The bot is functionally always in cash, occasionally taking a 1–2 second detour.

---

## 3. namikv2's posture vs the scalper posture

| Knob | namikv2 | Scalper-style (fabijan_v5) | Effect |
|---|---|---|---|
| `SOFT_POS_MAX` | 1800 | 250 | Per-line peak capital: $180k → $25k. Main lever. |
| `ARB_MAX_K` | 25 | 5 | ETF arb fire size: 150 sh → 30 sh. |
| `XV_MAX_QTY` | 100 | 30 | XV arb fire size. |
| `MM_QTY` | 4 | 3 | Resting MM order size. |
| `MM_REFRESH_S` | 1.5 | 0.5 | Quotes update 3× faster. |
| `BASE_XV_EDGE` / `BASE_ARB_EDGE` | 4 / 4 | 2 / 2 | Floor halved — more fires. |
| `ADAPT_QUANTILE` | 0.30 | 0.20 | Adaptive threshold floats lower. |
| `TAPER_START_FRAC` / `TAPER_END_FRAC` | 0.5 / 0.95 | 0.3 / 0.7 | Sizing decay starts earlier. |
| MM universe | 7 instruments | all 25 | Wider noise-trader capture. |
| Inventory-aware MM skew | ❌ | ✅ ±1¢ when \|pos\| > 30% of cap | Active force toward zero. |

The change is *not* "trade less." It's "trade many small instead of few large."

---

## 4. Why fabijan_v4 went the wrong direction

`fabijan_v4` made the bot *more selective* (cluster-first scan, constrained-seat restrictions). That's the opposite of what the data points to. The competitive edge here is high turnover at near-zero capital per line; selectivity reduces fire count without reducing capital lock-up per fire. Net negative.

If we revisit location-awareness later, it should be additive on top of fabijan_v5 — e.g., per-segment re-detect to keep the cluster-tie-break correct — without the constrained-seat restrictions that block global edges.

---

## 5. Risks of the fabijan_v5 changes

- **More fires per second** could brush the 500 msg/s rate limit. Worst-case venue (ZSE, full coverage) at 25 MM instruments × refresh per 0.5s ≈ 100 msg/s on MM alone. Plus arbs and inventory queries. Should fit within budget but worth watching the heartbeat for `Message rate limit exceeded` text frames.
- **Lower thresholds** may fire on edges that don't actually clear the implicit ~2¢ MM spread cost. Adaptive learning (5s window) should pull thresholds back up if the edges aren't real, but verify in a testing round before evaluation.
- **Inventory-aware MM skew** could cause "tug-of-war" with microprice skew when both signal in the same direction (e.g., we're long AND microprice predicts up). Could overshoot. Watch for cyclical inventory swings on the heartbeat.

---

## 6. How to validate

Run fabijan_v5 in a testing round and compare against namikv2:

1. Total realized PnL — should be higher.
2. Cash levels per exchange over time — should sit near $100k most of the time, dipping briefly during arb fires.
3. Per-strategy fill counts — should be roughly 3–5× higher than namikv2.
4. Per-instrument peak position over the segment — should rarely exceed 200 shares per (exchange, instrument).

If any of those fail, the next dial to turn is `SOFT_POS_MAX` — bump to 500 if positions are bouncing off the cap and limiting fire frequency, or down to 100 if cash isn't staying near $100k.
