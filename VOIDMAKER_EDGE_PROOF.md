# Voidmaker Edge Proof

This note uses only the local docs and `market_data/` replay. I did not use the
existing bot implementations.

## Proven Edge: Passive Legal-Extreme Landmines

Place a passive bid at `1` cent and a passive ask at `999999` cents, with small
quantity, on every live instrument.

### Why This Is Unconditionally Nonnegative If Filled

The API accepts integer-cent limit prices with `0 < price < 1_000_000`. Matching
executes at the resting order's price. The market maker keeps the book from
being empty.

For a passive bid fill at price `p = 1`, the bot buys `q` shares. If it unwinds
at any later valid bid/settlement price `s >= 1`, realized PnL is:

```text
q * (s - p) >= 0
```

For a passive ask fill at price `p = 999999`, the bot sells `q` shares. If it
unwinds at any later valid ask/settlement price `s <= 999999`, realized PnL is:

```text
q * (p - s) >= 0
```

So the two legal extremes are free options: fills are not guaranteed, but an
extreme fill cannot be worse than flat when unwound inside the legal price
domain. In normal books near `$100`, the payoff is very large.

### Replay Evidence

The replay contains actual sweeps into off-market resting orders:

- `SSE-ZABA` traded at `999999` for `40` total shares.
- `ZSE-ZABA` traded at `999999` for `20` total shares.
- `Euronext-SIMP` traded at `1` for `210` total shares.
- Several other instruments traded at `1..5` or `20k..35k`.

`python3 tools/backtest_voidmaker.py --data-dir market_data --top 40` scores
the visible extreme fills at `$770,379.10` gross using a conservative `$100`
cover mark.

## Empirical Edge: Close-Cluster Cross-Venue Lag

This one is not unconditional, because fills and convergence are latency
dependent. It is still the scale signal that best explains a `$5M` winner.

The replay repeatedly shows the same ticker bid on one nearby venue far above
the ask on another nearby venue. With a conservative `25` share clip, `25` cent
minimum spread, and `150ms` per pair cooldown:

```text
NYSE location cluster (NYSE,NASDAQ,TMX):     $1,659,086.95 gross
ZSE location cluster (ZSE,Euronext,LSE):     $3,594,572.40 gross
HKEX location cluster (HKEX,SSE,JPX,NSE):    $1,620,294.58 gross
```

The strongest repeated source is `CARD`, followed by local shared listings such
as `DDJH`, `MDKA`, `JZRO`, `KOTD`, `HT`, and `ZABA`.

This is almost certainly what the leader is doing: harvesting close-exchange
stale quotes aggressively, especially CARD, while legal-extreme landmines catch
the oversized sweeps made by other bots.

## Disproof: Self-Print Settlement Is Not Unconditional From This Data

The replay has fingerprints of price-print manipulation (`1`, `999999`, and
very stale passive orders), but it does not include team IDs, and the settlement
formula is explicitly undisclosed. Therefore there is no unconditional proof
from this data that self-trading is allowed or that a self-print changes final
settlement enough to guarantee profit.

Voidmaker does not rely on that assumption. It uses the proven passive
landmines plus close-cluster IOC arbitrage.
