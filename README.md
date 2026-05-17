# algotrade2026

Trading bots and research tooling for the **AlgoTrade 2026** hackathon
(May 2026, Zagreb) — a simulated multi-exchange algo-trading competition
run by X.FER / FER Zagreb.

10 simulated exchanges, 25 instruments, 30-minute rounds split into three
10-minute segments with hard resets at every boundary. Every team starts
with $100k of cash on every exchange, capital does not pool, and the
server kicks any connection that exceeds 500 messages per second per
exchange. The full rule set lives in [`.firecrawl/`](.firecrawl/) (mirror
of the venue intranet docs).

## Layout

```
.
├── bots/                 active strategies — one bot per file
│   ├── voidmaker.py        passive landmines + close-cluster cross-venue lag
│   ├── edge_max.py         aggressive ETF / cross-venue IOC engine
│   ├── parallax_smart.py   adaptive market maker
│   ├── prism_v2.py         volatility-tracking quoter
│   ├── kuracpalac.py       live-safe execution bot
│   ├── simple_mm_bot.py    minimal MM reference
│   ├── simp_card_cross_median_bot.py   SIMP/CARD cross-venue median edge
│   ├── certificate_bot.py  paper-trading certificate runner
│   ├── history_bot.py      reference scaffold from the upstream docs
│   ├── archive/            earlier iterations kept for diffability
│   └── cpp/                C++ port — bot.cpp, demo_bot.cpp, CMakeLists.txt
├── dashboard/            local multi-exchange dashboard (NAV, PnL, books)
├── scripts/              backtest.py, flatten.py — one-shot operational tools
├── tools/                replay scorers and backtest harnesses for bots/
├── tests/                unittest suite (also runs under pytest)
├── analysis/             one-off market microstructure studies + plots
├── edge_research/        scripts behind docs/EDGE_*_PROOF.md
├── docs/                 EDGE proofs, admin runbook
├── .firecrawl/           local mirror of the venue intranet docs
├── CLAUDE.md             agent / contributor guide for this repo
├── conftest.py           pytest path setup (adds bots/ to sys.path)
└── requirements.txt      websockets, aiohttp
```

## Competition constraints

These shape every design decision in this repo. Anything not respecting
them gets disconnected by the server within a minute.

- **Prices are integer cents.** `10050` = $100.50. No floats in messages.
- **Rate limit: 500 msgs/sec per team per exchange.** Exceeding it closes
  the connection, no throttle. Each bot rate-limits client-side.
- **Auth is by source IP**, capped at 10 concurrent WS connections per
  exchange. Dashboard + bot must share that budget.
- **Segment boundaries are hard resets.** Every 10 minutes the exchange
  process restarts: cash, positions, orders gone. Bots must reconnect
  with backoff every segment.
- **Capital is per-exchange, not pooled.** $100k start per exchange,
  cash floor −$50k, position floor −200 / ceiling +2,000 per instrument.
- **Market data is broadcast every 100 ms** with incremental `events`
  and `candles`. The bot keeps history; the wire does not.
- **Top-5 aggregated depth.** No team IDs on the wire, no leaderboard
  API. PnL is reconstructed locally by the dashboard.

## Running a bot

The team VM ships Python 3.13 under PEP 668, so install into a venv:

```sh
ssh root@vm.algotrade.hr        # password: algotrade
cd ~/algotrade2026 && git pull

apt install -y python3.13-venv  # one-time
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

tmux new -s bot
python bots/voidmaker.py        # or any other bot in bots/
# Ctrl-b d to detach; `tmux attach -t bot` to reattach
```

`LOG_LEVEL=DEBUG` enables verbose order logs. Most bots accept
exchange-host overrides via environment variables — see the constants
block at the top of each file.

## Local dashboard

The official `dashboard.algotrade.hr` UI is unreliable during rounds.
`dashboard/dashboard.py` is a self-hosted replacement that connects one
WebSocket to each of the 10 exchanges, polls inventory and pending
orders once a second, and serves a small web UI.

```sh
python dashboard/dashboard.py            # http://localhost:8080
PORT=9000 python dashboard/dashboard.py  # override port
```

It shows total NAV / PnL across all exchanges, per-exchange status,
open positions, pending orders, segment time remaining, a live order
book viewer, and a filterable trade/cancel feed.

Caveat: each WS counts against the per-exchange connection cap (10),
so leave headroom if you also run a bot.

## Backtests

The `tools/` directory holds replay harnesses for the more involved
bots. They read recorded CSV snapshots from `market_data/`
(gitignored — populate it with `scripts/backtest.py` first or copy
recordings from a previous round).

```sh
python tools/backtest_voidmaker.py --data-dir market_data
python tools/backtest_edge_max.py  --data-dir market_data
python tools/replay_simp_card_edge.py --data-dir market_data
python tools/simple_mm_backtest.py --data-dir market_data
```

Each harness is intentionally conservative — it fills only against the
visible top-of-book depth so reported edge is a strict lower bound on
the real number.

## Tests

```sh
python -m unittest discover -s tests       # stdlib
pytest                                     # if available
```

Both runners pick up `tests/__init__.py` / `conftest.py`, which add
`bots/` to `sys.path` so test modules can import bots by short name.

## Documentation

- [`CLAUDE.md`](CLAUDE.md) — agent / contributor guide; competition
  constraints, runtime environment, repo conventions.
- [`docs/BOT_ADMIN_RUNBOOK.md`](docs/BOT_ADMIN_RUNBOOK.md) — VM
  operations during a round.
- [`docs/EDGE_MAX_PROOF.md`](docs/EDGE_MAX_PROOF.md) /
  [`docs/VOIDMAKER_EDGE_PROOF.md`](docs/VOIDMAKER_EDGE_PROOF.md) —
  written justification for the edges behind the two main bots,
  with backtest evidence.
- [`.firecrawl/`](.firecrawl/) — local mirror of the venue intranet
  docs. The upstream is intranet-only and only reachable on the
  competition network.
