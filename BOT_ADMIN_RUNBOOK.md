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
pip install websockets aiohttp
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
- `prism.py` and any other active trading bot: `prism.py` is broad and aggressive.

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

For maximum alpha but highest operational risk, use:

```bash
tmux new -s prism
LOGLEVEL=INFO python3 prism.py
```

## 4. Bot Scores

Scores are operator scores from 1-10, not guaranteed PnL.

| Bot | Trades? | Main Edge | Safety | Expected Edge | Complexity | Readiness | Overall | Admin Use |
|---|---:|---|---:|---:|---:|---:|---:|---|
| `fabijan_v1.py` | Yes | ZSE ETF basket arb | 8 | 7 | 3 | 8 | 8 | Best first live bot |
| `fabijan_v2.py` | Yes | Low-latency same-ETF cross-venue arb | 8 | 6 | 4 | 8 | 7 | Good second bot, do not pair with v1 on same risk unless careful |
| `fabijan_v3.py` | Yes | v1 + v2 combined | 7 | 8 | 5 | 8 | 8 | Best Fabijan variant if stable |
| `edge_trader_bot.py` | Yes | Analyzer-discovered ETF/routes/CARD | 7 | 8 | 5 | 7 | 8 | Best data-driven bot |
| `codex_bot.py` | Yes | Broad microprice, latency, sector, ETF lead-lag | 5 | 8 | 8 | 6 | 7 | Experimental broad bot |
| `codex_bot_v2.py` | Yes | Newer broad Codex variant | 5 | 8 | 8 | 6 | 7 | Experimental, compare to `codex_bot.py` |
| `namikv1.py` | Yes | Full multi-strategy with hedge/adaptive features | 5 | 8 | 9 | 6 | 7 | Advanced experimental |
| `alpha_bot.py` | Yes | MM-skew, non-50 size, CARD/SIMP, adaptive thresholds | 5 | 8 | 9 | 6 | 7 | Advanced experimental |
| `prism.py` | Yes | Multi-venue ETF/basket/sub-ETF/stock arb + passive MM | 4 | 9 | 10 | 6 | 7 | Highest ambition, highest blast radius |
| `history_bot.py` | No | Data capture | 10 | N/A | 2 | 9 | 9 | Always useful in tests |
| `analyzerbot.py` | No | Offline analysis and scoring | 10 | N/A | 2 | 9 | 9 | Run after captures |
| `dashboard.py` | No | Monitoring UI | 8 | N/A | 4 | 7 | 7 | Useful if connection budget permits |
| `demo_bot.cpp` | Demo | C++ reference framework | 6 | 2 | 6 | 5 | 4 | Reference only |
| `bot.cpp` | Broken/unsafe C++ trader | NYSE-mid comparison attempt | 2 | 1 | 7 | 1 | 1 | Do not run live until fixed |
| `bot.py` | No | C++ text in `.py` file | 1 | 0 | 1 | 0 | 0 | Do not run with Python |

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
against NYSE mid-price. It is not live-ready.

What it tries to do:

1. Connect one thread per exchange.
2. Keep a shared order book snapshot.
3. For each instrument seen on the current exchange, find the same instrument
   on NYSE.
4. If NYSE mid-price is higher than the current exchange mid-price, buy on the
   current exchange.
5. If NYSE mid-price is lower, sell on the current exchange.

Critical problems:

| Problem | Impact |
|---|---|
| Uses `"buy"` and `"sell"` as order sides | The exchange API expects `"bid"` and `"ask"`, so orders are likely rejected |
| Compares exact instrument IDs across venues | `NYSE-CARD` and `NASDAQ-CARD` do not match as strings, so cross-venue logic misses most intended pairs |
| Threshold is `0.05` cents | Prices are integer cents, so this is effectively zero and would overtrade if the symbol bug is fixed |
| No rate limiter | Could exceed message budget after the strategy is corrected |
| No inventory or cash guard | Can accumulate position without local risk control |
| No pending-order cap management | Resting orders can pile up until expiry or exchange rejection |
| No IOC flag | Orders are regular expiring limits, not clean arbitrage takers |
| Ends whole process on first `end_of_round` | Not useful for multi-segment operation without wrapper/restart logic |
| Fill callback lacks instrument/side/price | Makes strategy-level PnL and risk response weak |
| Not built by existing CMake | `cmake --build build` only produces `demo_bot` |

Do not run live as-is.

If an admin needs to compile it for inspection:

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

1. Normalize symbols to base symbols such as `CARD`, then map them back to
   exchange-specific instrument IDs before sending orders.
2. Replace order sides with `bid` and `ask`.
3. Use cents-based thresholds that cover spread, fees/slippage assumptions, and
   stale-book risk.
4. Add a local message-rate limiter.
5. Add inventory, cash, and per-symbol position limits.
6. Use IOC orders for arbitrage unless intentionally making passive quotes.
7. Track order IDs, pending orders, fills, and per-exchange reject counts.
8. Reconnect or restart cleanly between round segments.

Admin score: 1/10. Interesting C++ experiment, but lower priority than fixing
and testing the Python bots.

### `bot.py`

This file currently contains C++ text despite the `.py` extension. Do not run:

```bash
python3 bot.py
```

Admin score: 0/10.

## 6. Live Decision Matrix

Use this during a round:

| Situation | Run |
|---|---|
| First live attempt, want stability | `fabijan_v1.py` |
| ETF locks across Euronext/ZSE/LSE/TMX are visible | `fabijan_v2.py` or `fabijan_v3.py` |
| Analyzer shows repeated routes like `INA HKEX -> NASDAQ` | `edge_trader_bot.py` |
| You want broad alpha and can monitor closely | `codex_bot.py`, `namikv1.py`, or `alpha_bot.py` |
| You want maximum ambition and accept risk | `prism.py --venues ...` |
| You are in a testing round | `history_bot.py` + `analyzerbot.py` |
| Official dashboard is poor | `dashboard.py` |
| You need a C++ starting point | `demo_bot.cpp`; avoid `bot.cpp` live |

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
| `codex_bot.py` |  |  |  |  |  |  |  |
| `alpha_bot.py` |  |  |  |  |  |  |  |
| `prism.py` |  |  |  |  |  |  |  |

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
history_bot.py -> analyzerbot.py -> fabijan_v1.py or edge_trader_bot.py
```

Use broad bots only after they show clean dry-run output and stable behavior in
a testing segment. If two bots disagree on the same instrument, stop one. The
account is shared by source IP, so every bot shares the same inventory, cash,
order count, and rate-limit blast radius.
