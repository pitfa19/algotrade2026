# AlgoTrade Bot Admin Runbook

This file teaches an operator how to run, monitor, compare, and score the bots
in this repo during a live AlgoTrade segment.

Use this from the team VM on the venue network. Do not run multiple trading bots
that touch the same exchange/account unless you intentionally want them to share
cash, positions, order limits, and message budget.

## 1. Live Setup

Connect to the VM:

```bash
ssh root@vm.algotrade.hr
```

Prepare Python:

```bash
cd ~/algotrade2026
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run every long-lived process inside `tmux`:

```bash
tmux new -s bot
# run bot command here
# detach: Ctrl-b d
# reattach: tmux attach -t bot
```

Before live trading:

```bash
python3 -B analyzerbot.py --data-dir market_data --top 10
python3 -B edge_trader_bot.py
```

The first command analyzes recorded data. The second command dry-runs the live
edge trader and prints opportunities without sending orders.

## 2. Safety Rules

Default to dry-run unless the bot explicitly says otherwise. For bots with a
`LIVE_TRADING` flag, live orders require:

```bash
LIVE_TRADING=1
```

Do not exceed message budget. The server hard limit is 500 messages/sec per
exchange. Prefer bot settings around 250-400 msgs/sec.

Avoid running these together:

- `fabijan_v1.py` and `fabijan_v3.py`: both consume ZSE basket-arb risk.
- `fabijan_v2.py` and `fabijan_v3.py`: both consume cross-venue ETF risk.
- `edge_trader_bot.py` and `codex_bot.py`: both can trade broad multi-venue arb.
- `edge_trader_bot.py` and `novel_edge_bot.py`: both fire ETF dislocations,
  cross-venue pairs, and single-leg fair-value takes.
- `codex_bot.py` and `codex_bot_v2.py`: same family, same edges, will fight.
- `namikv1.py` and `namikv2.py`: same family, same edges, will fight.
- `prism.py` and any other active trading bot: `prism.py` is broad and aggressive.
- `prism.py` and `cascade.py`: identical strategy surface, will fight each other.
- `cascade.py` and any other active trading bot: same edge surface as prism, broader fills.
- `parallax.py` and any other active trading bot: `parallax.py` is a broad
  consensus-FV bot and should run alone.
- `parallax.py` and `prism.py`: maximum overlap; both fire ETF/basket/sub-ETF/xv arbs.
- `parallax.py` and `apex_bot.py`: both fire ETF basket arb and cross-venue arb.
- `parallax.py` and `god_bot.py`: same edge surface (ETF/basket, cross-venue, safe-haven).
- `prism_v2.py` and any other active trading bot: it combines prism/cascade
  execution with parallax-style FV/stat/sector/MM edges. Run it alone.
- `prism_v2.py` and `parallax.py`: nearly identical broad alpha surface, with
  different ETF depth execution. They will duplicate orders.
- `god_bot.py` and any other active trading bot: `god_bot.py` spans ETF basket
  arb, cross-venue lead/lag, CARD/SIMP anomaly detection, safe-haven rotation,
  and end-of-segment flattening. Run it alone unless you are deliberately
  partitioning exchanges and risk budgets.
- `apex_bot.py` and any of `fabijan_v1/v2/v3.py`, `edge_trader_bot.py`, `codex_bot.py`,
  `codex_bot_v2.py`, `namikv1.py`, `namikv2.py`, `alpha_bot.py`, `novel_edge_bot.py`,
  `prism.py`, `cascade.py`, `parallax.py`, `prism_v2.py`, or `god_bot.py`:
  `apex_bot.py` covers ETF basket arb, cross-venue arb, MM-skew, and EOS
  unwind — running it next to another arb bot duplicates orders against the
  same edges and shares cash/positions.

Safe combinations:

- `history_bot.py` + one trading bot, if connection limits allow.
- `dashboard.py` + one trading bot, if connection limits allow.
- `analyzerbot.py` anytime, because it is offline only.

## 3. Quick Recommendation

For first live deployment, use:

```bash
tmux new -s fabijan1
LOG_LEVEL=INFO python3 fabijan_v1.py
```

For analyzer-driven route trading, use:

```bash
tmux new -s edge
LIVE_TRADING=1 EXCHANGES=HKEX,NASDAQ,ZSE,NYSE,SSE,JPX,NSE EDGE_QTY=5 MAX_MSGS_PER_SEC=250 python3 edge_trader_bot.py
```

For a tested dry-run-gated broad IOC bot, use:

```bash
tmux new -s novel
LIVE_TRADING=1 EXCHANGES=ZSE,NYSE,NASDAQ,HKEX,TMX MAX_MSGS_PER_SEC=250 SINGLE_QTY=5 PAIR_QTY=5 BASKET_UNITS=2 python3 novel_edge_bot.py
```

For the new dry-run-gated multi-strategy bot, use:

```bash
tmux new -s god
LIVE_TRADING=1 GOD_LOCATION=ZSE GOD_VENUES=ZSE,NYSE,NASDAQ,EURONEXT,LSE,HKEX,TMX GOD_MAX_MSGS_PER_SEC=120 GOD_MAX_ORDER_QTY=8 GOD_MAX_SYMBOL_ABS_POS=80 GOD_ARB_UNIT_SIZE=1 GOD_ORDERS_PER_EVAL=16 GOD_ENABLE_PASSIVE_MICRO=0 python3 god_bot.py
```

For the classic Prism-family all-market bot, use:

```bash
tmux new -s prism
LOGLEVEL=INFO python3 prism.py
```

For the same risk profile but with consensus-FV triangulation, vol-adaptive
edges, statistical FV snipes, sector residual hedged via the ETF, and an
inventory + flow-skewed market maker, use:

```bash
tmux new -s parallax
LOGLEVEL=INFO python3 parallax.py --venues ZSE,NYSE,TMX
```

For maximum current ambition after a subset smoke test, run `prism_v2.py`
alone on all 10 venues:

```bash
tmux new -s prismv2
LOGLEVEL=INFO python3 -u prism_v2.py --venues NYSE,NASDAQ,SSE,JPX,EURONEXT,LSE,HKEX,NSE,TMX,ZSE
```

## 4. Bot Scores

Scores are operator scores from 1-10, not guaranteed PnL.

| Bot | Trades? | Main Edge | Safety | Expected Edge | Complexity | Readiness | Overall | Admin Use |
|---|---:|---|---:|---:|---:|---:|---:|---|
| `fabijan_v1.py` | Yes | ZSE ETF basket arb | 8 | 7 | 3 | 8 | 8 | Best first live bot |
| `fabijan_v2.py` | Yes | Low-latency same-ETF cross-venue arb | 8 | 6 | 4 | 8 | 7 | Good second bot, do not pair with v1 on same risk unless careful |
| `fabijan_v3.py` | Yes | v1 + v2 combined | 7 | 8 | 5 | 8 | 8 | Best Fabijan variant if stable |
| `edge_trader_bot.py` | Yes | Analyzer-discovered ETF/routes/CARD | 7 | 8 | 5 | 7 | 8 | Best data-driven bot |
| `novel_edge_bot.py` | Yes | ZSE basket arb + cross-venue pairs + single-leg FV dislocations | 7 | 8 | 6 | 8 | 8 | Clean dry-run-gated broad IOC bot; good controlled competitor to edge/god |
| `codex_bot.py` | Yes | Broad microprice, latency, sector, ETF lead-lag | 5 | 8 | 8 | 6 | 7 | Experimental broad bot |
| `codex_bot_v2.py` | Yes | Newer broad Codex variant | 5 | 8 | 8 | 6 | 7 | Experimental, compare to `codex_bot.py` |
| `namikv1.py` | Yes | Full multi-strategy with hedge/adaptive features | 5 | 8 | 9 | 6 | 7 | Advanced experimental |
| `namikv2.py` | Yes | namikv1 + ETF-implied stock fair value, more strategies | 5 | 8 | 9 | 5 | 7 | Newer Namik variant — compare dry-run to v1 |
| `alpha_bot.py` | Yes | MM-skew, non-50 size, CARD/SIMP, adaptive thresholds | 5 | 8 | 9 | 6 | 7 | Advanced experimental |
| `prism.py` | Yes | Multi-venue ETF/basket/sub-ETF/stock arb + passive MM | 4 | 9 | 10 | 6 | 7 | Highest ambition, highest blast radius |
| `cascade.py` | Yes | prism + depth-walked ETF basket arb, plan ranking, faster reconnect | 4 | 9 | 10 | 6 | 8 | Surgical superset of prism — verified 3× ETF arb size on stacked-edge books |
| `parallax.py` | Yes | prism edges + consensus FV, vol-adaptive thresholds, stat FV snipe, sector residual ETF-hedged, SH coherence, inv+flow-skewed MM | 4 | 10 | 10 | 5 | 8 | Broad consensus-FV bot; high expected edge and high blast radius |
| `prism_v2.py` | Yes | parallax-style broad alpha + true depth-walked ETF basket sizing | 4 | 10 | 10 | 6 | 8 | Current most ambitious Prism-family bot; run alone after subset smoke test |
| `apex_bot.py` | Yes | Rule-based ETF basket arb + ZSE oracle cross-venue + MM-skew + EOS unwind | 7 | 8 | 6 | 7 | 8 | New flagship, deterministic ETF edge, untested live |
| `god_bot.py` | Yes | Dry-run-gated ETF/basket, lead-lag, anomaly, safe-haven, flattening | 8 | 8 | 7 | 7 | 8 | New safest broad bot; first live with conservative caps |
| `history_bot.py` | No | Data capture | 10 | N/A | 2 | 9 | 9 | Always useful in tests |
| `analyzerbot.py` | No | Offline analysis and scoring | 10 | N/A | 2 | 9 | 9 | Run after captures |
| `dashboard.py` | No | Monitoring UI | 8 | N/A | 4 | 7 | 7 | Useful if connection budget permits |
| `demo_bot.cpp` | Demo | C++ reference framework | 6 | 2 | 6 | 5 | 4 | Reference only |
| `bot.cpp` | Patched C++ trader | NYSE fair-value cross-venue IOC | 4 | 4 | 7 | 3 | 4 | Experimental only, CMake build checked |

## 5. Individual Bot Runbook

### `history_bot.py`

Purpose: record order books, trades, candles, and cancel events to CSV files.
It does not trade.

Live/test run:

```bash
tmux new -s history
EXCHANGES=NYSE,NASDAQ,HKEX,ZSE OUTPUT_DIR=./market_data python3 history_bot.py
```

Flags:

| Env | Default | Meaning |
|---|---|---|
| `EXCHANGES` | all 10 | Comma-separated exchange names |
| `OUTPUT_DIR` | `./market_data` | Directory for CSV output |

Output files:

```text
market_data/<EXCHANGE>_orderbooks.csv
market_data/<EXCHANGE>_trades.csv
market_data/<EXCHANGE>_candles.csv
market_data/<EXCHANGE>_events.csv
```

Admin score: 9/10 utility. Run during testing rounds whenever possible.

### `analyzerbot.py`

Purpose: offline analysis of `history_bot.py` CSVs. It does not connect and
does not trade.

Text report:

```bash
python3 -B analyzerbot.py --data-dir market_data --top 20
```

JSON report:

```bash
python3 -B analyzerbot.py --data-dir market_data --top 50 --json
```

Write JSON:

```bash
python3 -B analyzerbot.py --data-dir market_data --write-json analysis/report.json
```

Flags:

| Flag | Default | Meaning |
|---|---:|---|
| `--data-dir` | `market_data` | CSV directory |
| `--top` | `20` | Rows per report section |
| `--sync-tolerance-ms` | `75` | Time tolerance for ZSE ETF fair value matching |
| `--bucket-ms` | `100` | Cross-venue bucket size |
| `--json` | false | Print machine-readable JSON |
| `--write-json` | none | Save full JSON report |

Admin score: 9/10 utility. Use before selecting thresholds.

### `edge_trader_bot.py`

Purpose: live trader based on edges found by `analyzerbot.py`:

- ZSE ETF fair-value dislocations.
- Recurring cross-venue routes such as `INA HKEX -> NASDAQ`.
- Large CARD cross-venue divergence.
- End-of-segment IOC flattening.

Dry run:

```bash
python3 edge_trader_bot.py
```

Conservative live run:

```bash
tmux new -s edge
LIVE_TRADING=1 \
EXCHANGES=HKEX,NASDAQ,ZSE,NYSE,SSE,JPX,NSE \
EDGE_QTY=5 \
MAX_GROUPS_PER_TICK=2 \
MAX_MSGS_PER_SEC=250 \
ETF_THRESHOLD_CENTS=60 \
CARD_THRESHOLD_CENTS=180 \
python3 edge_trader_bot.py
```

Aggressive live run:

```bash
LIVE_TRADING=1 \
EXCHANGES=HKEX,NASDAQ,ZSE,NYSE,SSE,JPX,NSE,TMX,Euronext,LSE \
EDGE_QTY=8 \
MAX_GROUPS_PER_TICK=3 \
MAX_MSGS_PER_SEC=350 \
ETF_THRESHOLD_CENTS=45 \
CARD_THRESHOLD_CENTS=140 \
python3 edge_trader_bot.py
```

Flags:

| Env | Default | Meaning |
|---|---:|---|
| `LIVE_TRADING` | false | `1` sends real orders |
| `EXCHANGES` | all 10 | Connected exchanges |
| `EDGE_QTY` | `8` | Shares per leg |
| `MAX_GROUPS_PER_TICK` | `3` | Max opportunity groups per market-data tick |
| `MAX_MSGS_PER_SEC` | `350` | Per-exchange local send cap |
| `ETF_THRESHOLD_CENTS` | `45` | ETF fair-value edge threshold |
| `CARD_THRESHOLD_CENTS` | `140` | CARD cross-venue edge threshold |
| `MAX_LONG` | `120` | Soft long cap per instrument/exchange |
| `MAX_SHORT` | `-80` | Soft short cap per instrument/exchange |

Admin score: 8/10. Best data-driven live trader.

### `novel_edge_bot.py`

Purpose: self-contained broad IOC bot with a cleaner risk gate than the older
experimental broad bots. It trades three edge classes:

- ZSE ETF basket arbitrage with integer hedge ratios.
- Same-ticker cross-venue IOC pairs.
- Single-leg fair-value dislocations, with small lead-lag adjustment.

Dry run:

```bash
python3 novel_edge_bot.py
```

Conservative live run:

```bash
tmux new -s novel
LIVE_TRADING=1 \
EXCHANGES=ZSE,NYSE,NASDAQ,HKEX,TMX \
MAX_MSGS_PER_SEC=250 \
SINGLE_EDGE_CENTS=45 \
PAIR_EDGE_CENTS=85 \
BASKET_EDGE_CENTS=28 \
SINGLE_QTY=5 \
PAIR_QTY=5 \
BASKET_UNITS=2 \
MAX_GROUPS_PER_TICK=2 \
python3 novel_edge_bot.py
```

Aggressive live run:

```bash
tmux new -s novel-aggr
LIVE_TRADING=1 \
EXCHANGES=NYSE,NASDAQ,SSE,JPX,Euronext,LSE,HKEX,NSE,TMX,ZSE \
MAX_MSGS_PER_SEC=350 \
SINGLE_EDGE_CENTS=35 \
PAIR_EDGE_CENTS=70 \
BASKET_EDGE_CENTS=22 \
SINGLE_QTY=8 \
PAIR_QTY=6 \
BASKET_UNITS=3 \
MAX_GROUPS_PER_TICK=3 \
python3 novel_edge_bot.py
```

Flags:

| Env | Default | Meaning |
|---|---:|---|
| `LIVE_TRADING` | false | `1` sends real orders |
| `EXCHANGES` | all 10 | Comma-separated exchange subset |
| `MAX_MSGS_PER_SEC` | `350` | Per-exchange local send cap |
| `MAX_BOOK_AGE_SECONDS` | `1.25` | Freshness window for fair value inputs |
| `SINGLE_EDGE_CENTS` | `35` | Single-leg FV dislocation threshold |
| `PAIR_EDGE_CENTS` | `70` | Cross-venue pair threshold |
| `BASKET_EDGE_CENTS` | `22` | ZSE ETF basket threshold |
| `SINGLE_QTY` | `8` | Single-leg order size |
| `PAIR_QTY` | `6` | Cross-venue pair size |
| `BASKET_UNITS` | `3` | Basket multiples per ETF arb |
| `MAX_GROUPS_PER_TICK` | `3` | Max opportunity groups per tick |
| `MAX_LONG` | `350` | Soft long cap per instrument |
| `MAX_SHORT` | `-120` | Soft short cap per instrument |
| `MIN_CASH_CENTS` | `-2500000` | Soft per-exchange cash floor |
| `NO_NEW_RISK_AFTER_MS` | `575000` | Stop opening near segment end |
| `GROUP_COOLDOWN_SECONDS` | `0.35` | Duplicate opportunity cooldown |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

Operator notes:

- This is a good middle path between `edge_trader_bot.py` and `god_bot.py`:
  dry-run-gated, unit-tested, but still broad enough to find cross-venue and
  FV dislocation edges.
- It does not currently implement a dedicated end-of-segment flatten strategy;
  it blocks new risk near segment end. Watch residual inventory.
- Do not run with `edge_trader_bot.py`, `codex_bot*.py`, `namikv*.py`,
  `alpha_bot.py`, `apex_bot.py`, `prism.py`, `cascade.py`, `parallax.py`,
  `prism_v2.py`, or `god_bot.py`.

Admin score: 8/10. Good controlled broad bot for live comparison.

### `fabijan_v1.py`

Purpose: ZSE-only ETF basket arbitrage. Trades ETF versus all constituent
legs on ZSE. This is simple because ZSE lists everything.

Live run:

```bash
tmux new -s fabijan1
LOG_LEVEL=INFO python3 fabijan_v1.py
```

If DNS fails:

```bash
ZSE_HOST=10.0.210.2 LOG_LEVEL=INFO python3 fabijan_v1.py
```

Flags/constants:

| Name | Default | Meaning |
|---|---:|---|
| `ZSE_HOST` | `zse.algotrade.hr` | ZSE host override |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `LOCAL_RATE_LIMIT` | `400` | Local send cap |
| `ARB_SIZE` | `20` | Shares per leg |
| `ARB_THRESHOLD` | `25` | Per-share edge threshold |
| `ETF_POS_CAP` | `80` | Soft ETF position cap |
| `FLATTEN_REMAINING_MS` | `30000` | Stop opening and flatten in last 30s |

Admin score: 8/10. Recommended first live bot.

### `fabijan_v2.py`

Purpose: cross-venue same-ETF arbitrage on selected low-latency pairs:

```text
ETFA   Euronext <-> ZSE
ETFB   LSE      <-> ZSE
ETFA3  NYSE     <-> TMX
ETFSH  Euronext <-> ZSE
```

Live run:

```bash
tmux new -s fabijan2
LOG_LEVEL=INFO python3 fabijan_v2.py
```

Flags/constants:

| Name | Default | Meaning |
|---|---:|---|
| `LOG_LEVEL` | `INFO` | Python logging level |
| `LOCAL_RATE_LIMIT` | `400` | Per-exchange local send cap |
| `ARB_SIZE` | `20` | Shares per leg |
| `ARB_THRESHOLD` | `30` | Same-ETF cross-venue edge threshold |
| `POS_CAP` | `60` | Soft position cap per ETF/exchange |
| `FLATTEN_REMAINING_MS` | `30000` | Flatten window |

Admin score: 7/10. Good when low-latency ETF locks are visible.

### `fabijan_v3.py`

Purpose: combined version of `fabijan_v1.py` and `fabijan_v2.py`. Shares state
so the two strategies do not fight each other.

Live run:

```bash
tmux new -s fabijan3
LOG_LEVEL=INFO python3 fabijan_v3.py
```

Debug:

```bash
LOG_LEVEL=DEBUG python3 fabijan_v3.py
```

Flags/constants:

| Name | Default | Meaning |
|---|---:|---|
| `LOG_LEVEL` | `INFO` | Python logging level |
| `LOCAL_RATE_LIMIT` | `400` | Per-exchange cap |
| `ARB_SIZE` | `20` | Shares per leg |
| `ARB_THRESHOLD` | `25` | Basket-arb threshold |
| `ARB_THRESHOLD_CV` | `30` | Cross-venue ETF threshold |
| `ETF_POS_CAP` | `80` | ZSE ETF cap |
| `CV_POS_CAP` | `60` | Cross-venue cap |
| `FLATTEN_REMAINING_MS` | `30000` | Flatten window |

Admin score: 8/10. Best Fabijan bot if stable in dry/test.

### `codex_bot.py`

Purpose: broad multi-exchange strategy bot. It trades active fair-value
arbitrage, latency lead-lag, sector residuals, ETF basket lead-lag, optional
passive quoting, and end-of-segment unwind.

Dry run:

```bash
python3 codex_bot.py
```

Conservative live run:

```bash
tmux new -s codex
LIVE_TRADING=1 \
EXCHANGES=ZSE,NASDAQ,HKEX,NYSE \
HOME_LOCATION=ZSE \
ORDER_QTY=5 \
MIN_EDGE_CENTS=20 \
MAX_ORDERS_PER_TICK=2 \
MAX_MSGS_PER_SEC=250 \
LATENCY_ARB=1 \
PASSIVE_ENABLED=0 \
BOT_OUTPUT_DIR=bot_logs \
python3 codex_bot.py
```

Flags:

| Env | Default | Meaning |
|---|---:|---|
| `LIVE_TRADING` | false | `1` sends real orders |
| `EXCHANGES` | all 10 | Exchanges to connect |
| `HOME_LOCATION` | `ZSE` | Latency profile hint |
| `MIN_EDGE_CENTS` | `12` | Base minimum edge |
| `ORDER_QTY` | `10` | Base shares per order |
| `MAX_ORDERS_PER_TICK` | `6` | Max orders per market tick |
| `MAX_MSGS_PER_SEC` | `350` | Local per-exchange cap |
| `LATENCY_ARB` | true | Enable/disable latency arb |
| `PASSIVE_ENABLED` | false | Enable passive quoting |
| `BOT_OUTPUT_DIR` | none | JSONL logs |

Admin score: 7/10. High potential, but monitor closely.

### `codex_bot_v2.py`

Purpose: newer broad Codex variant. Similar to `codex_bot.py`, with additional
features in the same family.

Run the same way as `codex_bot.py`:

```bash
LIVE_TRADING=1 EXCHANGES=ZSE,NASDAQ,HKEX ORDER_QTY=5 MAX_MSGS_PER_SEC=250 python3 codex_bot_v2.py
```

Flags are the same family as `codex_bot.py`:

```text
LIVE_TRADING, EXCHANGES, HOME_LOCATION, MIN_EDGE_CENTS, ORDER_QTY,
MAX_ORDERS_PER_TICK, MAX_MSGS_PER_SEC, LATENCY_ARB, PASSIVE_ENABLED,
BOT_OUTPUT_DIR
```

Admin score: 7/10. Compare dry-run output against `codex_bot.py`.

### `namikv1.py`

Purpose: full multi-strategy bot with hedging, adaptive thresholds, inventory
skew, sector arb, ETF basket arb, latency arb, and unwind controls.

Conservative live run:

```bash
tmux new -s namik
LIVE_TRADING=1 \
EXCHANGES=ZSE,NASDAQ,HKEX,NYSE \
HOME_LOCATION=ZSE \
ORDER_QTY=5 \
MIN_EDGE_CENTS=20 \
MAX_ORDERS_PER_TICK=2 \
MAX_MSGS_PER_SEC=250 \
LATENCY_ARB=1 \
SECTOR_ARB=1 \
ETF_BASKET_ARB=1 \
HEDGE=1 \
ADAPTIVE_THRESHOLD=1 \
INVENTORY_SKEW=1 \
UNWIND=1 \
PASSIVE_ENABLED=0 \
python3 namikv1.py
```

Flags:

| Env | Default | Meaning |
|---|---:|---|
| `LIVE_TRADING` | false | Send orders |
| `EXCHANGES` | all 10 | Exchanges |
| `HOME_LOCATION` | `ZSE` | Latency profile |
| `MIN_EDGE_CENTS` | `12` | Base edge |
| `ORDER_QTY` | `10` | Base order size |
| `MAX_ORDERS_PER_TICK` | `6` | Tick throttle |
| `MAX_MSGS_PER_SEC` | `350` | Message throttle |
| `LATENCY_ARB` | true | Lead-lag strategy |
| `SECTOR_ARB` | true | Sector residual strategy |
| `ETF_BASKET_ARB` | true | ETF basket strategy |
| `HEDGE` | true | Hedge legs |
| `ADAPTIVE_THRESHOLD` | true | Volatility-scaled thresholds |
| `INVENTORY_SKEW` | true | Position-aware thresholds |
| `UNWIND` | true | End-of-segment unwind |
| `PASSIVE_ENABLED` | false | Passive quoting |
| `BOT_OUTPUT_DIR` | none | JSONL logs |

Admin score: 7/10. Powerful but complex.

### `namikv2.py`

Purpose: newer Namik variant. Same async/websocket core as `namikv1.py`,
adds ETF-implied stock fair value (back out a stock's price from ETF and
co-constituents), more strategies, and the same env-driven feature gates.

Conservative live run:

```bash
tmux new -s namik2
LIVE_TRADING=1 \
EXCHANGES=ZSE,NASDAQ,HKEX,NYSE \
HOME_LOCATION=ZSE \
ORDER_QTY=5 \
MIN_EDGE_CENTS=20 \
MAX_ORDERS_PER_TICK=2 \
MAX_MSGS_PER_SEC=250 \
LATENCY_ARB=1 \
SECTOR_ARB=1 \
ETF_BASKET_ARB=1 \
HEDGE=1 \
ADAPTIVE_THRESHOLD=1 \
INVENTORY_SKEW=1 \
UNWIND=1 \
PASSIVE_ENABLED=0 \
python3 namikv2.py
```

Flags are the same family as `namikv1.py`:

```text
LIVE_TRADING, EXCHANGES, HOME_LOCATION, MIN_EDGE_CENTS, ORDER_QTY,
MAX_ORDERS_PER_TICK, MAX_MSGS_PER_SEC, LATENCY_ARB, SECTOR_ARB,
ETF_BASKET_ARB, HEDGE, ADAPTIVE_THRESHOLD, INVENTORY_SKEW, UNWIND,
PASSIVE_ENABLED, BOT_OUTPUT_DIR
```

Do not run with `namikv1.py` — they share the same edges and will fight.

Admin score: 7/10. Compare dry-run output against `namikv1.py` before live.

### `alpha_bot.py`

Purpose: experimental full-market bot using MM-skew inference, non-50 top-size
signals, host/IP switching, multi-venue ETF fair value, aggressor-flow bias,
CARD/SIMP stat-arb, and adaptive thresholds.

Dry run:

```bash
python3 alpha_bot.py
```

Live run:

```bash
tmux new -s alpha
LIVE_TRADING=1 ALPHA_LOG_LEVEL=INFO python3 alpha_bot.py
```

Use DNS hostnames instead of IPs:

```bash
LIVE_TRADING=1 ALPHA_USE_HOSTNAMES=1 python3 alpha_bot.py
```

Flags:

| Env | Default | Meaning |
|---|---:|---|
| `LIVE_TRADING` | false | Send orders |
| `ALPHA_USE_HOSTNAMES` | false | Use `*.algotrade.hr` instead of IPs |
| `ALPHA_LOG_LEVEL` | `INFO` | Logging verbosity |

Important constants in file:

```text
RATE_LIMIT_SOFT=380
QUOTE_EXPIRY_MS=250
TAKE_EXPIRY_MS=500
MIN_TICK_EDGE=6
EDGE_VOL_MULTIPLIER=1.6
```

Admin score: 7/10. High creativity, high monitoring burden.

### `prism.py`

Purpose: ambitious all-market strategy:

- Multi-venue ETF versus basket arbitrage.
- Sub-ETF identities like `ETFA` vs `ETFA3 + complement`.
- Cross-venue same-stock arbitrage.
- Passive making one tick inside the market maker.
- Settlement-aware unwind.

Dry/live behavior: this bot appears to trade directly; there is no
`LIVE_TRADING` gate in the file. Treat it as live by default.

Subset test run:

```bash
tmux new -s prism-test
LOGLEVEL=INFO python3 prism.py --venues ZSE,NYSE,TMX
```

Full run:

```bash
tmux new -s prism
LOGLEVEL=INFO python3 prism.py
```

Flags:

| Flag/Env | Default | Meaning |
|---|---:|---|
| `--venues` | all 10 | Comma-separated venue subset |
| `LOGLEVEL` | `INFO` | Logging verbosity |

Important constants in file:

```text
RATE_PER_S=400
SOFT_POS_MAX=1800
SOFT_POS_MIN=-180
ARB_EDGE=4
SUB_ETF_EDGE=6
XV_EDGE=3
ARB_MAX_K=25
XV_MAX_QTY=40
MM_QTY=4
EOS_UNWIND_MS=60000
EOS_FLATTEN_MS=8000
```

Admin score: 7/10 overall, but 4/10 safety. Use only after observing it on a
small venue subset.

### `cascade.py`

Purpose: surgical descendant of `prism.py`. Same strategies, same thresholds,
same defensive bits — three execution-quality changes only:

1. **Depth-walked ETF basket arb.** The ETF leg walks every ask/bid level
   that still beats `ARB_EDGE` on its marginal price, instead of stopping
   at top-of-book qty. The IOC limit is set to the worst accepted level so
   server price-time priority gives price improvement on shallower levels.
   Same edge floor, same risk-per-share, ~3× the size on the same
   opportunity when the MM is mispriced through multiple levels.
2. **Plan ranking.** When multiple arb plans land on one tick, the
   highest-edge plan fires first so it consumes position/cash headroom
   before the smaller ones do.
3. **Faster reconnect.** `MAX_BACKOFF_S` lowered from `4.0` to `1.5`. With
   3 segment boundaries per round, this returns 5–10 s of trade time.

Verified empirically on a stacked-edge synthetic book: prism takes `k=8`
baskets at top-of-book, cascade takes `k=25`. On a thin book with only
top-of-book having edge, cascade and prism produce identical plans.

Subset test run:

```bash
tmux new -s cascade-test
LOGLEVEL=INFO python3 cascade.py --venues ZSE,NYSE,TMX
```

Full run:

```bash
tmux new -s cascade
LOGLEVEL=INFO python3 cascade.py
```

Flags:

| Flag/Env | Default | Meaning |
|---|---:|---|
| `--venues` | all 10 | Comma-separated venue subset |
| `LOGLEVEL` | `INFO` | Logging verbosity |

Important constants in file (identical to prism except `MAX_BACKOFF_S`):

```text
RATE_PER_S=400
SOFT_POS_MAX=1800
SOFT_POS_MIN=-180
ARB_EDGE=4
SUB_ETF_EDGE=6
XV_EDGE=3
ARB_MAX_K=25
XV_MAX_QTY=40
MM_QTY=4
EOS_UNWIND_MS=60000
EOS_FLATTEN_MS=8000
MAX_BACKOFF_S=1.5     # prism: 4.0
```

Do not run alongside `prism.py` — identical strategy surface, will fight
for the same fills against the same shared cash and inventory.

Admin score: 8/10 overall, 4/10 safety. If prism was already winning,
cascade is the strict superset.

### `parallax.py`

Purpose: broad consensus-FV strategy bot. Treats every quote as a
latency-delayed observation of one fair value and triangulates a consensus
FV per ticker. Trades stale quotes wherever they appear.

Strategies (priority order):

- ETF versus basket arbitrage, multi-venue routed.
- Sub-ETF identity arbitrage (`6·ETFA = 3·ETFA3 + complement`, same for B).
- Cross-venue same-stock arbitrage.
- Statistical fair-value snipe (single-leg) when a venue's quote is far off
  consensus FV in σ-multiples; capped per-ticker gross-inventory exposure.
- Sector residual mean-reversion, hedged with the matching sector ETF.
- Safe-haven coherence guard — fades ETFSH when synth-market and SH index
  drift in the same direction.
- Inventory- and flow-skewed two-sided market making on a small set.
- Settlement-aware unwind — last 60 s ramp, last 8 s IOC sweep.

Defensive layer:

- Local 400 msg/s/exchange token bucket.
- Atomic plan validation (every leg's headroom checked before any leg ships).
- Periodic `get_inventory` reconcile against truth.
- Volatility-adaptive edge threshold per ticker.
- Aggressor-flow window per ticker, used as gate on stat-arb and MM skew.
- Plans ranked by edge and capped per tick.

There is no `LIVE_TRADING` gate in the file. Treat as live by default.

Subset test run:

```bash
tmux new -s parallax-test
LOGLEVEL=INFO python3 parallax.py --venues ZSE,NYSE,TMX
```

Full run:

```bash
tmux new -s parallax
LOGLEVEL=INFO python3 parallax.py
```

Flags:

| Flag/Env | Default | Meaning |
|---|---:|---|
| `--venues` | all 10 | Comma-separated venue subset |
| `LOGLEVEL` | `INFO` | Logging verbosity |

Important constants in file (tuned to be unambiguously more aggressive than
`prism.py` in every regime — calm, choppy, and high-vol):

```text
RATE_PER_S=400
SOFT_POS_MAX=1800
SOFT_POS_MIN=-180
MIN_ARB_EDGE=2                  # prism ARB_EDGE=4, XV_EDGE=3
SUB_ETF_EDGE=4                  # prism: 6
EDGE_VOL_K=0.6                  # additive: edge = MIN + k·σ_tick
STAT_EDGE_SIGMA=1.8             # statistical FV-snipe threshold (σ-multiples)
STAT_MAX_QTY=35
STAT_MAX_INVENTORY=250
SECTOR_Z_THRESHOLD=1.5
SECTOR_MAX_QTY=25
SH_GUARD_THRESHOLD=25
SH_GUARD_MAX_QTY=12
ARB_MAX_K=45                    # prism: 25
XV_MAX_QTY=60                   # prism: 40
MM_QTY_BASE=10                  # prism MM_QTY: 4
MM_QTY_MAX=24
MM_REFRESH_S=0.8                # prism: 1.5
MM_INSTRUMENTS=CARD,SIMP,ETFA,ETFB,ETFA3,ETFB3,GOLD,XAG,ETFSH
MAX_PLANS_PER_TICK=30           # prism: uncapped
EOS_UNWIND_MS=60000
EOS_FLATTEN_MS=8000
```

Empirical edge thresholds: in calm markets `edge_for() ≈ 2.1c`; after a
vol spike it rises to roughly `3.8c`, still under prism's flat `4c`.

Tuning notes:

- If the bot trips the rate-limit close, lower `MAX_PLANS_PER_TICK`, raise
  `MM_REFRESH_S`, and consider trimming `MM_INSTRUMENTS`.
- If positions hit soft caps too often, raise `MIN_ARB_EDGE` to 3 and
  drop `STAT_MAX_INVENTORY` back toward 100.
- Set `EDGE_VOL_K=0` to make all edges flat at the floor (most aggressive).
- If the stat-arb loses money — meaning the consensus FV is wrong — raise
  `STAT_EDGE_SIGMA` to 2.5 or disable by setting `STAT_MAX_INVENTORY=0`.

Admin score: 8/10 overall, 4/10 safety. Highest expected edge in the repo
on paper, but the broadest blast radius. Run on a small venue subset before
expanding.

### `prism_v2.py`

Purpose: current most ambitious Prism-family bot. It keeps the deterministic
multi-venue arb core from `prism.py`, adds `cascade.py`-style true top-5
depth-walked ETF basket execution, and layers in `parallax.py`-style
consensus fair value, statistical snipes, sector residual hedges, safe-haven
coherence, and inventory/flow-skewed passive market making.

Strategies (priority order):

- Depth-walked ETF versus basket arbitrage, multi-venue routed.
- Sub-ETF identity arbitrage (`6·ETFA = 3·ETFA3 + complement`, same for B).
- Cross-venue same-stock arbitrage.
- Statistical FV snipes from the consensus tape.
- Sector residual mean-reversion hedged with the matching ETF.
- Safe-haven coherence fade around `ETFSH`.
- Inventory- and flow-skewed two-sided market making.
- Settlement-aware unwind — last 60 s ramp, last 8 s IOC sweep.

Defensive layer:

- Local 400 msg/s/exchange token bucket.
- Atomic plan validation before dispatch.
- Optimistic position/cash tracking with periodic `get_inventory` reconcile.
- Volatility-adaptive edge floors.
- Aggressor-flow gating and MM skew.
- Duplicate-plan fire gap and max plans per tick.

There is no `LIVE_TRADING` gate in the file. Treat as live by default.

Subset smoke test:

```bash
tmux new -s prismv2-test
LOGLEVEL=INFO python3 -u prism_v2.py --venues ZSE,NYSE,TMX
```

Full-potential run, all 10 venues:

```bash
tmux new -s prismv2
LOGLEVEL=INFO python3 -u prism_v2.py --venues NYSE,NASDAQ,SSE,JPX,EURONEXT,LSE,HKEX,NSE,TMX,ZSE
```

Flags:

| Flag/Env | Default | Meaning |
|---|---:|---|
| `--venues` | all 10 | Comma-separated venue subset |
| `LOGLEVEL` | `INFO` | Logging verbosity |

Important constants in file:

```text
RATE_PER_S=400
SOFT_POS_MAX=1800
SOFT_POS_MIN=-180
MIN_ARB_EDGE=2
SUB_ETF_EDGE=4
EDGE_VOL_K=0.6
STAT_EDGE_SIGMA=1.8
STAT_MAX_QTY=35
STAT_MAX_INVENTORY=250
SECTOR_Z_THRESHOLD=1.5
SECTOR_MAX_QTY=25
SH_GUARD_THRESHOLD=25
SH_GUARD_MAX_QTY=12
ARB_MAX_K=45
XV_MAX_QTY=60
MM_QTY_BASE=10
MM_QTY_MAX=24
MM_REFRESH_S=0.8
MAX_PLANS_PER_TICK=30
PLAN_FIRE_GAP_S=0.04
EOS_UNWIND_MS=60000
EOS_FLATTEN_MS=8000
```

Pre-flight:

```bash
python3 -m py_compile prism_v2.py
python3 -m unittest tests.test_prism_v2 -v
```

Operator notes:

- Run alone. It overlaps with almost every profitable broad edge in the repo.
- Use `LOGLEVEL=INFO`; `DEBUG` can slow the bot and flood the terminal.
- If the server closes connections with `Message rate limit exceeded`, lower
  `RATE_PER_S` toward `300-350` or reduce `MAX_PLANS_PER_TICK`.
- If inventory sits near soft caps, raise `MIN_ARB_EDGE` to 3, reduce
  `STAT_MAX_INVENTORY`, or test on fewer venues first.
- Compared with `parallax.py`, this is the stronger Prism-family candidate
  when stacked-depth ETF dislocations appear. Compared with `god_bot.py`, it
  is much more aggressive and has no live/dry gate.

Admin score: 8/10 overall, 4/10 safety. Highest current ambition; smoke-test
on a subset before the all-venue run.

### `apex_bot.py`

Purpose: single-file multi-strategy bot built around the deterministic ETF
rule (FV = mean of constituents). Strategies, in priority order:

1. Same-exchange ETF basket arb (model-free, IOC).
2. ZSE-anchored cross-venue ETF arb (ZSE is the only venue listing all 25
   instruments, so its basket math is the universal oracle).
3. ZSE-anchored cross-venue stock arb, latency-budgeted.
4. MM inventory-skew passive harvesting around basket fair (only on the
   co-located venue or ZSE).
5. Convergence holding clock — force-exit arb lots that don't unwind in 9 s.
6. End-of-segment unwind in the last 30 s (cancel resting orders, IOC-close
   exposure; settlement is a weighted close, not edge).

Operational features:

- Auto-detects the co-located exchange via `/health` RTT every 30 s. No config
  edits needed when the team rotates between segments.
- Token-bucket rate limiter at 380 msg/s (76 % of the 500 cap) — burst-tolerant.
- Distinguishes `end_of_round` (resets state, jittered staggered reconnect)
  from a mid-segment WS drop (preserves state, re-syncs via `get_inventory`).
- Self-fill detection counts only the passive side of trade events to avoid
  double-counting fills already attributed by `add_order_response.immediate_*`.
- Reserved-cash and reserved-position estimates from local live orders cover
  the 5-second window between authoritative inventory syncs.

Dry run (no orders sent):

```bash
APEX_DRY_RUN=1 APEX_LOG=INFO python3 apex_bot.py
```

Live run, all 10 exchanges:

```bash
tmux new -s apex
python3 apex_bot.py
```

Live run, subset of exchanges:

```bash
APEX_EXCHANGES=zse,nyse,hkex,euronext,tmx python3 apex_bot.py
```

Selectively disable strategies (e.g. for triage):

```bash
APEX_DISABLE=mm_skew,cross_venue_stock_arb python3 apex_bot.py
```

Verbose debug:

```bash
APEX_LOG=DEBUG python3 apex_bot.py
```

Flags (env vars):

| Env | Default | Meaning |
|---|---|---|
| `APEX_DRY_RUN` | `0` | `1` logs every order/cancel instead of sending |
| `APEX_LOG` | `INFO` | `DEBUG`, `INFO`, `WARNING` |
| `APEX_EXCHANGES` | all 10 | Comma-separated subset (e.g. `zse,nyse`) |
| `APEX_DISABLE` | none | Comma-separated strategy names to skip |

Strategy names for `APEX_DISABLE`:

```text
etf_basket_arb, zse_anchored_etf_arb, cross_venue_stock_arb,
mm_skew, convergence_clock, eos_unwind
```

Important constants in file:

```text
SAFE_MSGS_PER_SEC=380       # of 500 hard cap
ARB_MIN_EDGE_CENTS=4        # same-exchange ETF arb threshold
ARB_CROSS_VENUE_BASE_EDGE=12  # cross-venue ETF/stock arb threshold
SKEW_MIN_CENTS=4            # MM-skew passive trigger
ARB_CLIP_QTY=8              # shares per IOC arb fill
SKEW_CLIP_QTY=4             # shares per passive MM quote
MAX_POS_LONG=80             # soft long cap per instrument
MAX_POS_SHORT=-60           # soft short cap (vs −200 wall)
CONVERGENCE_HOLD_MS=9000    # forced unwind clock
EOS_UNWIND_LEAD_MS=30000    # start unwinding 30s before close
```

Operator notes:

- The bot is **always live** — there is no `LIVE_TRADING` gate. Use
  `APEX_DRY_RUN=1` for observe-only.
- Skip co-locating with `prism.py`, `cascade.py`, `parallax.py`, `prism_v2.py`,
  `fabijan_v*.py`, `edge_trader_bot.py`, `novel_edge_bot.py`, `codex_bot*.py`,
  `namikv*.py`, `alpha_bot.py`, or `god_bot.py`. They contend for the same
  edges and the team account is shared by source IP.
- Pairs cleanly with `history_bot.py` and `dashboard.py` (read-only).
- Logs at INFO are quiet by design — only connect/disconnect/co-location
  events. Use `APEX_LOG=DEBUG` to see strategy-level decisions.

Admin score: 8/10. Strongest theoretical edge in the repo (rule-based ETF
mean reversion), but never run live. Watch the first segment closely.

### `god_bot.py`

Purpose: standalone competition bot with a shared risk gate and independent
strategy modules:

- ETF fair-value dislocations versus executable equal-weight baskets.
- Cross-venue price leadership and stale-quote taking using the current
  `GOD_LOCATION` latency profile.
- Microprice/order-book imbalance signals, disabled by default for safety.
- Safe-haven lag/fade behavior for `GOLD`, `XAG`, and `ETFSH`.
- CARD/SIMP median/MAD anomaly detection without relying on hidden rules.
- End-of-segment inventory flattening, with market-order panic flattening only
  in the final window.

Dry run, no orders sent:

```bash
python3 god_bot.py
```

Print resolved config without connecting:

```bash
python3 god_bot.py --print-config --no-connect
```

Conservative first live run:

```bash
tmux new -s god
LIVE_TRADING=1 \
GOD_LOCATION=ZSE \
GOD_VENUES=ZSE,NYSE,NASDAQ,EURONEXT,LSE,HKEX,TMX \
GOD_MAX_MSGS_PER_SEC=120 \
GOD_MAX_ORDER_QTY=8 \
GOD_MAX_SYMBOL_ABS_POS=80 \
GOD_ARB_UNIT_SIZE=1 \
GOD_ORDERS_PER_EVAL=16 \
GOD_ENABLE_PASSIVE_MICRO=0 \
python3 god_bot.py
```

Aggressive live run:

```bash
tmux new -s god-aggr
LIVE_TRADING=1 \
GOD_LOCATION=HKEX \
GOD_VENUES=NYSE,NASDAQ,SSE,JPX,EURONEXT,LSE,HKEX,NSE,TMX,ZSE \
GOD_MAX_MSGS_PER_SEC=400 \
GOD_MAX_ORDER_QTY=80 \
GOD_MAX_SYMBOL_ABS_POS=450 \
GOD_MIN_CASH_CENTS=-4500000 \
GOD_ARB_UNIT_SIZE=8 \
GOD_CROSS_UNIT_SIZE=20 \
GOD_ANOMALY_UNIT_SIZE=20 \
GOD_SAFE_UNIT_SIZE=12 \
GOD_ORDERS_PER_EVAL=80 \
GOD_ENABLE_PASSIVE_MICRO=1 \
python3 god_bot.py
```

Flags:

| Env/Flag | Default | Meaning |
|---|---:|---|
| `LIVE_TRADING` / `--live` | false | Required before real orders are sent |
| `--dry-run` | false | Force observe-only mode even if `LIVE_TRADING=1` |
| `GOD_LOCATION` / `--location` | `ZSE` | Current latency profile: `NYSE`, `ZSE`, or `HKEX` |
| `GOD_VENUES` / `--venues` | `ZSE,NYSE,NASDAQ,EURONEXT,LSE,HKEX,TMX` | Connected exchanges |
| `GOD_HOST_OVERRIDES` | none | Comma-separated `EXCHANGE=host` overrides |
| `GOD_MAX_MSGS_PER_SEC` | `120` | Per-exchange local send cap; server hard limit is 500 |
| `GOD_ORDERS_PER_EVAL` | `16` | Total submitted order legs per strategy evaluation |
| `GOD_MAX_ORDER_QTY` | `8` | Max shares per single order |
| `GOD_MAX_SYMBOL_ABS_POS` | `80` | Soft absolute position cap per instrument/exchange |
| `GOD_MIN_CASH_CENTS` | `-2000000` | Soft per-exchange cash floor |
| `GOD_ARB_UNIT_SIZE` | `1` | Basket-arb unit size |
| `GOD_CROSS_UNIT_SIZE` | `3` | Cross-venue lead/lag order size |
| `GOD_ANOMALY_UNIT_SIZE` | `3` | CARD/SIMP anomaly order size |
| `GOD_SAFE_UNIT_SIZE` | `2` | Safe-haven rotation order size |
| `GOD_ENABLE_PASSIVE_MICRO` | false | Enable short-expiry passive microprice orders |
| `GOD_FLATTEN_WINDOW_MS` | `75000` | Stop opening and start reducing inventory |
| `GOD_PANIC_FLATTEN_MS` | `20000` | Use market orders for urgent flattening |
| `LOG_LEVEL` / `GOD_LOG_LEVEL` | `INFO` | Structured JSON logging verbosity |

Operator notes:

- `LIVE_TRADING=1` is required for live orders. Dry-run logs every would-be
  order as `dry_order`.
- Update `GOD_LOCATION` at each 10-minute segment rotation. Wrong location
  makes the lead/lag routing overconfident.
- Keep `GOD_ENABLE_PASSIVE_MICRO=0` for first live runs. Passive quoting adds
  fill uncertainty and pending-order pressure.
- Run alone for scoring attempts. It already consumes the broad ETF,
  cross-venue, CARD/SIMP, safe-haven, and flattening risk budgets.
- INFO logs are JSON lines with `opportunity`, `order_submit`, `order_ack`,
  `order_reject`, `inventory_snapshot`, reconnect, and segment-reset events.

Admin score: 8/10. Best new broad bot for a controlled first live attempt
because it is dry-run-gated and conservative by default.

### `dashboard.py`

Purpose: local monitoring dashboard, not a trading bot.

Run:

```bash
tmux new -s dash
python3 dashboard.py
```

Open:

```text
http://<vm-ip>:8080
```

Flags:

| Env | Default | Meaning |
|---|---:|---|
| `HOST` | `0.0.0.0` | Bind host |
| `PORT` | `8080` | Web port |
| `INV_POLL_SEC` | `1.0` | Inventory poll interval |
| `HEALTH_POLL_SEC` | `2.0` | Health poll interval |
| `LOG_LEVEL` | `INFO` | Logging level |

Admin score: 7/10. Useful but consumes exchange connections.

### `demo_bot.cpp`

Purpose: C++ reference framework. It connects to exchanges, parses market
data, demonstrates strategy callbacks, and is useful for writing a future C++
bot. Treat it as infrastructure sample code, not as a competition strategy.

Build:

```bash
apt install -y libboost-all-dev nlohmann-json3-dev cmake g++
cmake -S . -B build
cmake --build build
```

Run:

```bash
EXCHANGES=NYSE,NASDAQ,LSE ./build/demo_bot
```

Notes:

| Item | Assessment |
|---|---|
| Trades live? | Reference strategy only |
| Built by CMake? | Yes, target `demo_bot` |
| Primary value | Fast C++ scaffold for a maintained strategy |
| Live use | Not recommended as a scoring bot |

Admin score: 4/10. Keep as a C++ template.

### `bot.cpp`

Purpose: modified C++ bot that tries to trade cross-venue price differences
against NYSE mid-price. It has been patched for the obvious API and symbol
mapping bugs, but it is still not a first-choice live bot.

What it tries to do:

1. Connect one thread per exchange.
2. Keep a shared order book snapshot.
3. Normalize instruments to base symbols, such as `CARD`, so `NYSE-CARD` can
   be compared with `NASDAQ-CARD`.
4. Use NYSE mid-price as the reference fair value.
5. If current venue ask is at least 5 cents below NYSE mid, IOC-buy with side
   `bid`.
6. If current venue bid is at least 5 cents above NYSE mid, IOC-sell with side
   `ask`.
7. Cap each update to two submitted orders.

Debug status:

| Item | Status |
|---|---|
| API order sides | Fixed to use `bid` and `ask` |
| Cross-venue instrument matching | Fixed to compare base symbols |
| Threshold | Fixed from `0.05` cents to `5` cents |
| IOC orders | Fixed by sending `order_type: ioc` |
| Build target | Fixed by adding CMake target `bot_cpp` |
| CMake build | Verified locally with target `bot_cpp` |
| Inventory/cash guard | Still missing |
| Global message throttle | Still weak; only per-update order cap exists |
| Fill detail tracking | Still incomplete |
| Multi-segment operation | Still exits on first `end_of_round` |

Do not run broad live as-is. If testing it, use one or two venues first.

If an admin needs to compile it for inspection:

```bash
cmake -S . -B build
cmake --build build --target bot_cpp
```

Direct compile alternative, if local include and link paths are already set:

```bash
g++ -std=c++20 -O2 -o bot_cpp bot.cpp -lpthread
```

If the local Boost install requires explicit system linking:

```bash
g++ -std=c++20 -O2 -o bot_cpp bot.cpp -lpthread -lboost_system
```

Dry inspection run, on a small venue set only:

```bash
EXCHANGES=NYSE,NASDAQ ./bot_cpp
```

Required fixes before live:

1. Repeat the CMake build on the team VM before the round.
2. Add inventory, cash, and per-symbol position limits.
3. Add a true global message-rate limiter.
4. Track order IDs, pending orders, fills, and per-exchange reject counts.
5. Reconnect or restart cleanly between round segments.
6. Paper-run on `EXCHANGES=NYSE,NASDAQ` before adding more venues.

Admin score: 4/10. More usable after debugging, but still lower priority than
the tested Python bots.

## 6. Live Decision Matrix

Use this during a round:

| Situation | Run |
|---|---|
| First live attempt, want stability | `fabijan_v1.py` |
| ETF locks across Euronext/ZSE/LSE/TMX are visible | `fabijan_v2.py` or `fabijan_v3.py` |
| Analyzer shows repeated routes like `INA HKEX -> NASDAQ` | `edge_trader_bot.py` |
| You want a dry-run-gated broad IOC bot | `novel_edge_bot.py` conservative command |
| You want broad alpha and can monitor closely | `codex_bot.py`, `codex_bot_v2.py`, `namikv1.py`, `namikv2.py`, or `alpha_bot.py` |
| You want maximum ambition and accept risk | `prism.py --venues ...` |
| You like prism but want bigger fills on the same edges | `cascade.py --venues ...` |
| You want broader-than-prism alpha with consensus FV, stat snipes, and inventory-skewed MM | `parallax.py --venues ...` |
| You want the current most ambitious Prism-family bot | `prism_v2.py --venues ...` after subset smoke test |
| You want the deterministic ETF edge with auto co-location | `apex_bot.py` (dry-run first) |
| You want the new broad bot with dry-run gate and strict risk checks | `god_bot.py` conservative command |
| You are in a testing round | `history_bot.py` + `analyzerbot.py` |
| Official dashboard is poor | `dashboard.py` |
| You need a C++ starting point | `demo_bot.cpp`; use `bot.cpp` only after a small-venue build check |

## 7. Admin Scoring Method

After a testing segment, score each bot with:

```text
Score = 0.35 * PnLScore
      + 0.20 * StabilityScore
      + 0.20 * RiskScore
      + 0.15 * OpportunityScore
      + 0.10 * OperatorScore
```

Definitions:

- `PnLScore`: positive realized PnL, not mark-to-hope.
- `StabilityScore`: stayed connected, survived segment end, no rate-limit close.
- `RiskScore`: low inventory into settlement, no cash-floor/position-limit stress.
- `OpportunityScore`: trades matched observed analyzer edges.
- `OperatorScore`: logs are understandable and bot is easy to stop/restart.

Manual score sheet:

| Bot | PnL | Stability | Risk | Opportunity | Operator | Weighted Score | Notes |
|---|---:|---:|---:|---:|---:|---:|---|
| `fabijan_v1.py` |  |  |  |  |  |  |  |
| `fabijan_v2.py` |  |  |  |  |  |  |  |
| `fabijan_v3.py` |  |  |  |  |  |  |  |
| `edge_trader_bot.py` |  |  |  |  |  |  |  |
| `novel_edge_bot.py` |  |  |  |  |  |  |  |
| `codex_bot.py` |  |  |  |  |  |  |  |
| `codex_bot_v2.py` |  |  |  |  |  |  |  |
| `namikv1.py` |  |  |  |  |  |  |  |
| `namikv2.py` |  |  |  |  |  |  |  |
| `alpha_bot.py` |  |  |  |  |  |  |  |
| `prism.py` |  |  |  |  |  |  |  |
| `cascade.py` |  |  |  |  |  |  |  |
| `parallax.py` |  |  |  |  |  |  |  |
| `prism_v2.py` |  |  |  |  |  |  |  |
| `apex_bot.py` |  |  |  |  |  |  |  |
| `god_bot.py` |  |  |  |  |  |  |  |
| `history_bot.py` |  |  |  |  |  |  |  |
| `analyzerbot.py` |  |  |  |  |  |  |  |
| `dashboard.py` |  |  |  |  |  |  |  |
| `demo_bot.cpp` |  |  |  |  |  |  |  |
| `bot.cpp` |  |  |  |  |  |  |  |

## 8. Shutdown

Graceful stop:

```text
Ctrl-c
```

If detached in tmux:

```bash
tmux attach -t bot
Ctrl-c
```

List sessions:

```bash
tmux ls
```

Kill a stuck session:

```bash
tmux kill-session -t bot
```

## 9. Final Operator Advice

The safest profitable path is:

```text
history_bot.py -> analyzerbot.py -> fabijan_v1.py, edge_trader_bot.py, or god_bot.py
```

Use `novel_edge_bot.py` when you want a dry-run-gated broad IOC bot that is
easier to reason about than the biggest experimental systems. Use `prism_v2.py`
only when you are deliberately making a high-risk, all-venue scoring attempt.

Use broad bots only after they show clean dry-run or subset output and stable
behavior in a testing segment. If two bots disagree on the same instrument,
stop one. The account is shared by source IP, so every bot shares the same
inventory, cash, order count, and rate-limit blast radius.
