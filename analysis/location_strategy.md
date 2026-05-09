# Location-Aware Strategy Plan

How the bot detects which of the 3 rotation locations (NYSE / ZSE / HKEX) it is in, and what posture to take in each.

---

## 1. Background

- Each round = 3 segments × 10 min. Bot is co-located at NYSE, ZSE, or HKEX, one segment each. Order of rotation differs by group.
- At each segment boundary: cash, positions, orders reset. WS connections are closed by `end_of_round`. The bot must reconnect and re-detect its location.
- Latency to every other exchange depends only on which of the 3 home locations is active.

This means the bot's trading universe (which instruments are reachable in time to act on signals) is reshaped every 10 minutes. Hard-coding behavior won't work — strategy parameters must be re-keyed at every segment start by the detected location.

---

## 2. Location detection

### 2.1 Method

Single-probe detection is sufficient. The latency matrix is unique per row, so RTT to *any one* of the 3 home venues identifies location:

| Probe to | NYSE-home | ZSE-home | HKEX-home |
|---|---|---|---|
| NYSE | **0–1 ms** | 96 ms | 180 ms |
| ZSE | 96 ms | **0 ms** | 150 ms |
| HKEX | 180 ms | 150 ms | **0 ms** |

The gap between any two rows (≥ ~50ms) is wide enough that even noisy single-probe RTT measurement will not misclassify.

### 2.2 Implementation sketch

1. At segment start, open WS to NYSE *and* HKEX in parallel (redundancy in case one is slow to come up).
2. Send `get_inventory` (or any small request expecting a response) and timestamp send/receive.
3. Use median of 3 round-trips to filter jitter — total ~30 ms wall-clock.
4. Classify:
   - RTT(NYSE) < 30 ms → at NYSE
   - RTT(HKEX) < 30 ms → at HKEX
   - else → at ZSE (default — also confirmable: RTT(NYSE) ≈ 96ms)
5. Cache `LOCATION` in module state for the segment. Use it to key strategy config.

### 2.3 Cheaper alternative

WS connect handshake itself contains a round trip (TCP + WS upgrade). The first frame received after connect can be timestamp-diffed against the connect call. No need to send our own probe — we get the RTT measurement for free as part of standard connection bring-up.

### 2.4 Sanity check

After classification, verify by sampling RTT on one or two more venues against the predicted row. If observed RTTs disagree with the matrix by >25%, log a warning — could indicate the matrix changed for this round, or that we're misclassified.

---

## 3. Per-location strategy posture

### 3.1 At ZSE (Europe segment) — primary edge

- ZSE is the only exchange listing every stock and every ETF.
- All 5 ETFs are *fully replicable locally at 0 ms latency* — every constituent is on the same venue.
- LSE (24 ms) and Euronext (22 ms) are within one broadcast tick (100 ms) round-trip — viable for cross-venue.

**Tactics**:
- Run aggressive ETF self-arbitrage on all 5 ETFs locally. Quote both sides of each ETF tight to fair value, and hedge into constituents on fills.
- Cross-venue ETF arbitrage on ETFA (ZSE↔Euronext) and ETFB (ZSE↔LSE) where the partner exchange has a partial basket.
- Constituent-only relative-value within sectors (e.g. Sector A correlated drift) — ZSE has all 6 of each sector.
- Treat US/Asia exchanges as observation only: too slow to trade against from here.

**Risk budget**: highest. This segment should produce the bulk of the round's PnL.

### 3.2 At NYSE (Americas segment) — narrow but very fast

- Local self-arb is restricted: only ETFA3 (NYSE) and ETFB3 (NASDAQ, 1 ms) are fully replicable.
- TMX adds another ETFA3 listing at 11 ms → 3-venue ETFA3 mispricing is the cleanest cross-venue play of the round.
- Europe (Euronext, LSE) at 80–84 ms — borderline. One broadcast tick is 100 ms, so cross-venue here is 1.5–2 ticks behind. Useable for slow drifts, not for reactive arb.
- HKEX, JPX, SSE are 150+ ms → ignore entirely except for stale-data MM defense (see §4.3).

**Tactics**:
- ETFA3 triangulation across NYSE/NASDAQ/TMX. Tight quotes on all three, hedge the cheapest leg.
- ETFB3 self-arb on NASDAQ.
- CARD/SIMP cross-venue micro-arb between NYSE/NASDAQ (1ms) and TMX (11ms) — these are listed on all 10 venues, so American local pairs are the fastest available trio.

**Risk budget**: medium. Fewer instruments arbing locally but the latency edge is real.

### 3.3 At HKEX (Asia segment) — most constrained

- Local self-arb: ETFB3 only.
- SSE (19ms) and JPX (37ms) are near — but **SSE lists zero ETFs**, and JPX lists only ETFSH (which HKEX does not list, so no pair-trade).
- HKEX itself lists ETFA, ETFB, ETFB3 — but ETFA's basket on HKEX has only 3 of 6 (OIT, FSR, XFR). ETFB on HKEX has 4 of 6 (KOTD, INA, JNAF, DLKV). Imperfect hedges.
- Cross-venue to anywhere with full coverage (ZSE) is 150 ms — order arrives 1.5 broadcast ticks late.

**Tactics**:
- ETFB3 self-arb on HKEX (clean 3-leg).
- Single-instrument liquidity provision on HKEX local instruments (CARD, SIMP, OIT, FSR, JNAF, DLKV, INA, KOTD, MDKA, XFR).
- Partial-basket ETFA / ETFB market making on HKEX, hedging only the constituents that are co-listed. Accept the residual basis risk vs the missing constituents.
- Don't attempt cross-venue arb to Europe or Americas from here — 150–180 ms RTT means the signal is gone by the time we land.

**Risk budget**: lowest. Goal: don't lose money, capture small-but-clean local edges.

---

## 4. Cross-cutting considerations

### 4.1 Segment handover

- `end_of_round` is a hard reset. Open orders on remote venues do not carry over.
- Reconnect loop with backoff (1s, 2s, 4s, capped at 5s) for every venue we trade on. New exchange may take a few seconds to come up.
- On reconnect, *re-detect location first* — the location may have changed (segment 2 → segment 3 rotation).

### 4.2 Capital is per-exchange

- $100k starting cash per exchange, $50k cash floor per exchange.
- Cross-venue arbs need both legs sized to fit within both venues' cash floors.
- ETF self-arbs are net-flat in cash within one venue, so capital is rarely the binding constraint there.

### 4.3 Stale-data defense

- When at NYSE, our quotes on remote venues (if any) are based on data that arrived 80–180 ms ago. Either:
  - Don't quote remotely — only react to our local broadcasts.
  - Quote very wide on remote venues so the staleness premium is paid by counterparties, not us.
- Same logic at HKEX and ZSE for their respective far venues.

### 4.4 Rate limit per venue

- 500 msg/s/exchange, hard close on overrun. Local venue eats most of the budget.
- Build a token-bucket rate limiter (450 msg/s ceiling per venue, leaving 10% headroom) shared across all bot threads/coroutines per venue.

---

## 5. Open questions / things to test

- Does the latency matrix hold in practice or are there geographic asymmetries (e.g. is RTT(ZSE→NYSE) actually equal to RTT(NYSE→ZSE) — the matrix says yes, worth confirming)?
- What is the actual broadcast delivery jitter — is the 100 ms cadence tight or does it drift?
- How aggressive does the Market Maker's inventory skew get? It quotes around its modeled fair value — if its model lags real moves, that itself is exploitable.
- Settlement methodology: "time- or volume-weighted measure over a window near the close." We can't optimize for it directly, but we know to flatten before the last ~30s of each segment to avoid mark-out surprises.
