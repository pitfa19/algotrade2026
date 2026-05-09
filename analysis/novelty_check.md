# What's Already Covered vs What's New

Comparing the location/ETF-arb plans in `location_strategy.md` and `etf_arb_map.md` against the current best-performing bot, **`namikv2.py`**, plus the rest of the repo and `mm_analysis.md`.

---

## 1. Baseline = namikv2

`namikv2` is more sophisticated than my earlier survey suggested. Re-read in full, here's what it actually does:

| Feature | Implementation in namikv2 |
|---|---|
| **Auto cluster detection** | `auto_detect_cluster()` — probes TCP-connect RTT to every venue at startup, picks fastest, classifies into NA / EU / ASIA / IN / ZSE clusters. |
| **Cluster-aware leg selection** | `_best_buy_venue` / `_best_sell_venue` tie-break on same-cluster preference for ETF basket arb and sub-ETF arb counterparties. |
| **Cross-cluster drift penalty** | `CROSS_CLUSTER_DRIFT_CENTS = 3` added to the XV edge threshold per out-of-cluster leg. Scales with how far the trade reaches. |
| **Adaptive thresholds** | `SpreadTracker` — rolling 5-second time-windowed per-(strategy, ticker) spread observations. Threshold = 30th percentile of recent samples, floored by `BASE_*_EDGE`. Adapts both up *and* down within a segment. |
| **Inventory-tapered sizing** | Quadratic decay between 50% and 95% of position cap. New orders shrink as we approach limits. |
| **Depth walking** | Cross-venue arb walks 20¢ past best on both sides if the avg fill price still beats threshold. |
| **Microprice in MM** | Microprice vs mid drives a ±1¢ skew on both bid and ask quotes. |
| **Settlement-aware unwind** | Last 60s = graduated unwinding; last 8s = aggressive flatten. |
| **Per-strategy P&L stats** | Heartbeat logs realized cents broken down by strategy tag. |

**This is a strong baseline.** Three of the four "new" things I claimed in my prior version of this doc are partially or fully addressed:

- ✅ **Latency-probe location detection** — `auto_detect_cluster()` does this.
- ✅ **RTT-aware cross-venue thresholds** — `CROSS_CLUSTER_DRIFT_CENTS` does this (uniform 3¢ per cross-cluster leg).
- ✅ **Cluster-aware counterparty selection** — done via tie-break in `_best_*_venue`.

Adaptive per-instrument thresholds (which I didn't even propose) are an additional layer namikv2 has and my plans didn't cover. That's a real point in namikv2's favor.

---

## 2. What namikv2 still does NOT do

Despite all of the above, four concrete gaps remain. Each is worth quantifying.

### 2.1 Detection runs **once at startup**, never re-runs at segment boundary

`main()` → `auto_detect_cluster()` → `amain(active, my_cluster)` → `Hub.__init__` stores `my_cluster` as a constant. After segment 1 ends, `Connection.run()` reconnects, `on_welcome()` resets cash/positions/orders, but `my_cluster` is **never re-evaluated**.

Consequence: the bot rotates physical location every 10 minutes, but its cluster preference is frozen at the location it had when the operator started it. After two rotations, the cluster value is wrong on average 2/3 of the time — i.e., for 20 of the 30 minutes of a round, the cluster preferences and drift penalties are aimed at the wrong home venue.

This is **the single biggest gap** vs my plan, and the cheapest to fix:

```python
# In Connection.run(), after the welcome but before strategize starts:
async def run(self):
    while not self.hub.stop:
        async with ws_connect(...) as ws:
            ...
            self.hub.on_welcome(self.exchange, welcome)
            # NEW: trigger a hub-level re-detect once per segment.
            # First connection to come up after end_of_round wins the race
            # via an asyncio.Lock + "already detected this segment" flag.
            await self.hub.maybe_redetect_cluster()
```

Plus a `Hub.maybe_redetect_cluster()` method that runs `auto_detect_cluster()` at most once per segment (use a per-segment epoch counter so concurrent reconnects don't all probe).

Cost: ~30 lines. Benefit: cluster preference and drift penalty correctly aimed for all 3 segments instead of just 1.

### 2.2 Cross-venue arb uses **global best bid/ask**, not best **cluster-local** bid/ask

In `cross_venue_arbs()`:

```python
for v in venues:
    if bk.best_bid > best_b[1]:   best_b = (v, ...)
    if bk.best_ask < best_a[1]:   best_a = (v, ...)
```

This picks the single global best on each side. The cluster drift penalty only widens the *threshold* — it doesn't change which counterparty is picked. So if the best ask is on HKEX and we're at NYSE, the bot will reach for the HKEX ask (paying 180ms RTT) even when TMX has an ask only 1–2¢ worse and is 11ms away.

What's missing: a **cluster-first scan** that prefers any local-cluster pair meeting threshold over a higher-edge cross-cluster pair. The "local triangle" rationale from `etf_arb_map.md` §6 — for CARD/SIMP at NYSE: NYSE↔NASDAQ↔TMX (1–11ms); at ZSE: ZSE↔LSE↔Euronext (6–24ms); at HKEX: HKEX↔SSE↔JPX (18–37ms) — operationalises this.

Implementation: when scanning for the best XV pair on a ticker, first look for the best within `my_cluster`; only fall back to global if no in-cluster pair clears threshold. The 3¢ drift penalty is meant to approximate this but it doesn't, because both legs of a cross-cluster pair often have wider spreads that easily absorb 3–6¢.

### 2.3 No partial-basket ETF arb on HKEX/NYSE/Euronext

`etf_basket_arbs()` requires `_best_sell_venue` and `_best_buy_venue` to return non-None for *every* basket constituent globally. So in principle ETFA arb works from any venue — but the legs spread across venues with no awareness of the resulting RTT.

What it doesn't do: **deliberately trade ETFA on HKEX hedged with the 3 of 6 constituents that are also on HKEX (OIT, FSR, XFR), accepting basis risk on NGUP/KTST/JZRO.** This would be a strictly local trade (0ms hedge) with bounded basis risk. Currently the bot either does the global trade (ETFA on ZSE hedged with all 6 constituents on ZSE — only fires when *we are at ZSE* effectively, otherwise the legs scatter across venues 100+ms apart) or doesn't trade ETFA at all.

The catch: partial-basket arb requires per-pair basis-vol calibration to pick a sensible threshold. We'd need a `BASIS_VOL_CENTS[etf, missing_set]` table calibrated from `market_data/`. Not a small project.

### 2.4 HKEX segment doesn't get a different strategy posture

`my_cluster == "ASIA"` (when at HKEX) only changes the drift penalty by 3¢ per cross-cluster leg. Strategy modules themselves run identically. In practice this means at HKEX:

- ETF basket arb still tries to fire, with its legs scattered to ZSE for the missing constituents → 150ms one-way to ZSE means ~one full broadcast tick of staleness before the leg lands. The threshold widens by 3¢ × 3 missing legs = 9¢, which doesn't compensate for ~150ms of constituent-mid drift.
- Cross-venue arb still fires NYSE↔NASDAQ trades from HKEX — at 180ms RTT, our orders land 1.8 ticks late. Global best-pair picking makes it worse (see §2.2).

What's missing: a **strategy-mix table keyed on `my_cluster`** that disables cross-cluster ETF arb and far cross-venue arb when at HKEX, and instead allocates the rate-limit budget to:
- Local single-instrument MM on the 11 HKEX-listed names,
- HKEX-ETFB3 self-arb (only fully-replicable basket on HKEX),
- ASIA-cluster-only XV pairs (HKEX↔SSE, HKEX↔JPX, SSE↔JPX) where RTT is 18–37ms.

This is a strategic decision, not a parameter tweak. namikv2's continuous drift penalty doesn't make it.

---

## 3. Honest assessment

| Topic | namikv2 has it | My plan adds | Worth implementing |
|---|---|---|---|
| Auto cluster detection at startup | ✅ | — | already done |
| Cluster-aware tie-break on counterparty selection | ✅ | — | already done |
| Cross-cluster drift penalty in thresholds | ✅ (uniform 3¢) | — | already done |
| Adaptive per-instrument thresholds | ✅ | — | already done (and not in my plans) |
| Inventory tapering, depth walking, microprice MM | ✅ | — | already done |
| **Per-segment re-detection of cluster** | ❌ frozen at startup | yes | **YES — top priority, ~30 lines** |
| **Cluster-first cross-venue counterparty selection** | ❌ global-best | yes | **YES — modest refactor of `cross_venue_arbs`** |
| **HKEX-specific posture (disable far arb)** | ❌ | yes | YES — strategy-mix table keyed on `my_cluster` |
| Partial-basket ETF arb (HKEX-ETFA hedged 3/6) | ❌ | yes | maybe — needs basis calibration first |
| Local-cluster CARD/SIMP triangle | partial (drift penalty) | explicit | merges into §2.2 |

---

## 4. Concrete next steps, ranked

1. **Per-segment cluster re-detect** (§2.1). Change one method, add one helper, gain correct cluster awareness for all 3 segments instead of 1. Highest ROI per LOC.

2. **Cluster-first XV counterparty selection** (§2.2). Refactor `cross_venue_arbs` to prefer any in-cluster pair meeting threshold over a global pair. Same logic that already exists for ETF basket counterparty tie-break, just applied to XV. ~50 lines.

3. **Strategy-mix-by-cluster table** (§2.4). At Hub init (and on re-detect), set per-strategy `enabled[my_cluster]` flags from a config dict. ETF basket arb → disabled if `my_cluster == "ASIA"` and the basket spans cluster boundaries; XV arb → restricted to in-cluster pairs from `my_cluster == "ASIA"`; passive MM expanded to all 11 HKEX-listed names from "ASIA".

4. **Partial-basket arb** (§2.3) — only after the above three. Requires basis-vol study.

The first three combined are maybe 200 lines on top of namikv2 and address all the location-aware gaps that namikv2's uniform 3¢ drift penalty doesn't.
