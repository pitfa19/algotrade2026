# ETF Arbitrage Map

Complete enumeration of ETF mispricing trades, derived from cross-referencing the stock listings table and the ETF listings table in the participant guide.

---

## 1. ETF basket reference

| ETF | Constituents | n |
|---|---|---|
| ETFA | NGUP, OIT, KTST, FSR, JZRO, XFR | 6 |
| ETFB | KOTD, INA, HT, JNAF, DLKV, DDJH | 6 |
| ETFA3 | NGUP, KTST, XFR | 3 |
| ETFB3 | KOTD, INA, DLKV | 3 |
| ETFSH | GOLD, XAG | 2 |

Fair value = simple equal-weighted mean of constituent prices.

---

## 2. ETF listings recap

| ETF | NYSE | NASDAQ | LSE | EUR | JPX | SSE | HKEX | NSE | TMX | ZSE |
|---|---|---|---|---|---|---|---|---|---|---|
| ETFA | ✔ |  |  | ✔ |  |  | ✔ |  |  | ✔ |
| ETFB |  | ✔ | ✔ |  |  |  | ✔ |  |  | ✔ |
| ETFA3 | ✔ |  |  |  |  |  |  |  | ✔ | ✔ |
| ETFB3 |  | ✔ |  |  |  |  | ✔ |  |  | ✔ |
| ETFSH |  |  |  | ✔ | ✔ |  |  |  |  | ✔ |

---

## 3. Self-contained arbitrage (ETF + 100% of basket on same venue)

These are the cleanest opportunities — 0 ms latency between the ETF leg and the hedge legs.

| Venue | ETF | Basket complete? | Notes |
|---|---|---|---|
| **ZSE** | ETFA | ✔ all 6 | Full sector A basket on Zagreb |
| **ZSE** | ETFB | ✔ all 6 | Full sector B basket on Zagreb |
| **ZSE** | ETFA3 | ✔ all 3 | |
| **ZSE** | ETFB3 | ✔ all 3 | |
| **ZSE** | ETFSH | ✔ all 2 | GOLD + XAG |
| NYSE | ETFA3 | ✔ all 3 | NGUP, KTST, XFR all on NYSE |
| NASDAQ | ETFB3 | ✔ all 3 | KOTD, INA, DLKV all on NASDAQ |
| TMX | ETFA3 | ✔ all 3 | NGUP, KTST, XFR all on TMX |
| HKEX | ETFB3 | ✔ all 3 | KOTD, INA, DLKV all on HKEX |
| Euronext | ETFSH | ✔ all 2 | GOLD + XAG |
| JPX | ETFSH | ✔ all 2 | GOLD + XAG |

**Headline**: ZSE is the only exchange with full self-contained coverage of all 5 ETFs. Combined with the fact that one of the 3 rotation locations is co-located at ZSE, this is the structural edge of the competition.

---

## 4. Partial-basket arbitrage (ETF on a venue, some constituents missing)

When you can hedge only some of the basket locally and accept residual basis risk on the rest. The "missing" constituents carry a tracking-error exposure.

### ETFA (basket: NGUP, OIT, KTST, FSR, JZRO, XFR)

| Venue | Listed constituents | Missing | Coverage |
|---|---|---|---|
| ZSE | all 6 | — | 100% |
| NYSE | NGUP, KTST, JZRO, XFR | OIT, FSR | 4/6 |
| Euronext | NGUP, OIT, JZRO | KTST, FSR, XFR | 3/6 |
| HKEX | OIT, FSR, XFR | NGUP, KTST, JZRO | 3/6 |

### ETFB (basket: KOTD, INA, HT, JNAF, DLKV, DDJH)

| Venue | Listed constituents | Missing | Coverage |
|---|---|---|---|
| ZSE | all 6 | — | 100% |
| NASDAQ | KOTD, INA, HT, DLKV | JNAF, DDJH | 4/6 |
| LSE | KOTD, HT, DLKV, DDJH | INA, JNAF | 4/6 |
| HKEX | KOTD, INA, JNAF, DLKV | HT, DDJH | 4/6 |

ETFA and ETFB partial baskets are workable on multiple venues, especially HKEX (where ETFB has 4/6 — only HT and DDJH are missing).

---

## 5. Cross-venue ETF mispricing pairs

Same ETF listed on multiple venues → its price can drift between them. Track all pair RTTs from each home location.

### ETFA (NYSE, Euronext, HKEX, ZSE)

| Pair | RTT from NYSE | RTT from ZSE | RTT from HKEX |
|---|---|---|---|
| NYSE↔Euronext | 84 | — | — |
| NYSE↔HKEX | 180 | — | — |
| NYSE↔ZSE | 96 | — | — |
| Euronext↔ZSE | — | 22 | — |
| HKEX↔ZSE | — | 150 | — |
| Euronext↔HKEX | — | — | 130 |

### ETFB (NASDAQ, LSE, HKEX, ZSE)

| Pair | RTT from NYSE | RTT from ZSE | RTT from HKEX |
|---|---|---|---|
| NASDAQ↔LSE | 80 | — | — |
| NASDAQ↔HKEX | 180 | — | — |
| NASDAQ↔ZSE | 96 | — | — |
| LSE↔ZSE | — | 24 | — |
| HKEX↔ZSE | — | 150 | — |
| LSE↔HKEX | — | — | 135 |

### ETFA3 (NYSE, TMX, ZSE)

| Pair | RTT from NYSE | RTT from ZSE | RTT from HKEX |
|---|---|---|---|
| NYSE↔TMX | 11 | — | — |
| NYSE↔ZSE | 96 | — | — |
| TMX↔ZSE | — | 98 | — |

### ETFB3 (NASDAQ, HKEX, ZSE)

| Pair | RTT from NYSE | RTT from ZSE | RTT from HKEX |
|---|---|---|---|
| NASDAQ↔HKEX | 180 | — | — |
| NASDAQ↔ZSE | 96 | — | — |
| HKEX↔ZSE | — | 150 | — |

### ETFSH (Euronext, JPX, ZSE)

| Pair | RTT from NYSE | RTT from ZSE | RTT from HKEX |
|---|---|---|---|
| Euronext↔JPX | 145 | — | — |
| Euronext↔ZSE | — | 22 | — |
| JPX↔ZSE | — | 140 | — |

---

## 6. Recommended trade list per location

Filter the pairs/self-arbs above by the rule "RTT < 50ms is fast enough to react inside one broadcast tick (100ms)".

### When at NYSE
- Self-arb: NYSE-ETFA3 (local), NASDAQ-ETFB3 (1ms), TMX-ETFA3 (11ms).
- Cross-venue: NYSE↔TMX ETFA3 (11ms RTT — best of round).
- Cross-venue: NYSE↔NASDAQ on CARD/SIMP (1ms — all 10 venues list these two stocks).
- Skip: anything to ZSE/HKEX/JPX/SSE/NSE.

### When at ZSE
- Self-arb on every ETF locally: ETFA, ETFB, ETFA3, ETFB3, ETFSH (all on ZSE, all baskets complete).
- Cross-venue: ZSE↔Euronext on ETFA (22ms) and ETFSH (22ms).
- Cross-venue: ZSE↔LSE on ETFB (24ms).
- Constituent-level cross-venue: anything on LSE/Euronext at <30ms, including all the partial-basket stocks.
- Skip: ZSE↔Asia (140–150ms), ZSE↔Americas (96–98ms — borderline; only worth it if the spread is wide).

### When at HKEX
- Self-arb: HKEX-ETFB3 (local).
- Partial-basket: HKEX-ETFB hedged with KOTD, INA, JNAF, DLKV (4/6) — accept HT/DDJH basis.
- Partial-basket: HKEX-ETFA hedged with OIT, FSR, XFR (3/6) — accept higher basis (3 missing).
- Cross-venue to JPX (37ms) only useful for ETFSH — but HKEX doesn't list ETFSH, so no pair.
- Skip: anything to Americas (180ms) or Europe (130–135ms).

---

## 7. Special-instrument plays

CARD and SIMP are listed on **all 10 exchanges**. They have no ETF dependency, but they provide the only pure cross-venue arbitrage on a single instrument.

Best CARD/SIMP triangle per location:
- At NYSE: NYSE↔NASDAQ↔TMX (1ms / 11ms / 11ms).
- At ZSE: ZSE↔Euronext↔LSE (22ms / 24ms / 6ms).
- At HKEX: HKEX↔SSE↔JPX (19ms / 37ms / 18ms).

Worth running a CARD/SIMP cross-venue scanner on the local-cluster triangle in every segment. 18–37ms RTT in HKEX's local triangle is also one of the few fast trades available from there.

GOLD and XAG (safe-haven) are listed on more limited venues but still appear on enough to allow cross-venue. From ZSE you can pair GOLD on Euronext (22ms), JPX (140ms — slow), TMX (98ms), NASDAQ (96ms). The Euronext pair is the only fast one.

---

## 8. Implementation notes

- For each (location, ETF, venue) tuple, precompute: the constituent set on that venue, the missing constituents (and which other venues list them and at what RTT), and the latency budget.
- A scanner that runs every broadcast tick (100 ms) and emits "(ETF deviation in cents, hedgeable, RTT to hedge legs)" rows is sufficient signal for the trader.
- Threshold for trading: ETF mispricing > spread + a small buffer (covers expected mid-move during the RTT). Tighten as we calibrate during testing rounds.
