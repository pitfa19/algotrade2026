# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## What this repo is

Trading bots and research tooling for the **AlgoTrade 2026 hackathon** (May 2026, Zagreb) — a simulated multi-exchange algo-trading competition run by X.FER / FER Zagreb. Active strategies live in `bots/` (one file per bot); older iterations are kept under `bots/archive/` for diffability. Tests run with stdlib `unittest` or `pytest`. See `README.md` for the full layout.

## Documentation

The official docs live on the venue intranet at `http://docs.algotrade.hr` (resolves to `10.1.10.20` only on the venue network — not reachable from outside, and not reachable from cloud tools like Firecrawl/WebFetch). A full local cache is in `.firecrawl/`:

- `algotrade-index.md` — landing page
- `algotrade-participant-guide.md` — rules, instruments, exchanges, limits, scoring
- `algotrade-api.md` — full WebSocket API reference (every message type, every field, every error)
- `algotrade-bots.md` — reference bot architecture (Python `Strategy` framework, C++ port, history bot)
- `algotrade-network-ssh.md` — VM access, file transfer, tmux

**Read these before writing any non-trivial code.** They are the source of truth for protocol details. To refresh from the live site (only works on venue network): `curl -sL http://docs.algotrade.hr/<path>/ | ...` — note Firecrawl/WebFetch will fail with DNS errors because the host is intranet-only.

## Competition constraints any code must respect

These are the non-obvious rules that shape architecture decisions:

- **Prices are integer cents.** `10050` = $100.50. Min tick = 1 cent. Non-integer cents are rejected. Never use floats for prices in messages.
- **All numeric fields are 64-bit signed integers**, including timestamps (ms).
- **Rate limit: 500 msgs/sec per team per exchange.** Exceeding it doesn't throttle — the server text-frames `"Message rate limit exceeded"` and **closes the connection**. Build local rate limiting before ever hitting the wire.
- **Auth is by source IP**, no token. All connections from the team's network are one account. Up to 10 concurrent WS connections per team per exchange.
- **Segment boundaries are hard resets.** Each round = 3 × 10-min segments. At each boundary the exchange process restarts: cash, positions, orders all reset; existing connections are closed after `end_of_round`. The bot **must** reconnect with backoff for every segment.
- **Capital is per-exchange, not pooled.** $100k start per exchange (`10_000_000` cents), cash floor −$50k per exchange. Position floor −200, ceiling +2,000 per instrument per exchange.
- **Market data arrives every 100 ms** as broadcasts (no subscribe needed). `events` and `candles` are **incremental** — only what's new since the last tick. Persist them yourself if you need history.
- **Order book depth is top-5, aggregated, anonymous.** You cannot see whose orders sit at which level. Trade events have no team ID either.
- **Geographic rotation:** within a round the team rotates NYSE → ZSE → HKEX (one segment each). Latency to every other exchange shifts each segment — strategies that assume fixed latency will break.

## Instruments at a glance

20 stocks (Sector A, Sector B, Independent, Safe-Haven {GOLD, XAG}) + 5 ETFs (`ETFA`, `ETFB`, `ETFA3`, `ETFB3`, `ETFSH`). All start at $100. ETF fair value = simple equal-weighted mean of constituents. CARD/SIMP listed on all 10 exchanges; ZSE lists everything else; other stocks are on a subset of venues (full table in the participant guide). Only ZSE has full coverage — useful for cross-venue strategies that need a common reference.

## Runtime environment

- **Team VM:** `ssh root@vm.algotrade.hr` (password `algotrade`), 4 vCPU, 6 GB. Linux. Bot runs here in production.
- **Exchanges:** `ws://<exchange>.algotrade.hr:9001/trade` — the 10 hostnames are `nyse`, `nasdaq`, `sse`, `jpx`, `euronext`, `lse`, `hkex`, `nse`, `tmx`, `zse`. Health check: `GET /health` on the same port returns `{status, time, round_length}`.
- **Dashboard:** `http://dashboard.algotrade.hr` (10.0.112.3) for manual book inspection / debugging. Not required for the bot. Only serves HTTP while a round is live — outside rounds DNS still resolves and a route exists, but every common TCP port (80/443/8080/9001/3000/5000) is closed/filtered. Don't treat unreachability as a config bug.
- **Exchange WS endpoints behave the same:** `ws://...:9001/trade` and the `/health` endpoint also only respond during active rounds. Probing them between rounds will time out.
- **Venue network only.** None of the above resolves outside the event venue. This workstation is on the venue subnet (`10.1.112.0/24`); `docs.algotrade.hr` (10.1.10.20) is reachable from here at all times — the docs are the only venue host that stays up between rounds.

## Conventions for this repo

- Active strategies live in `bots/`; older iterations are archived under `bots/archive/`. The C++ port and CMake build sit in `bots/cpp/`.
- Tests use stdlib `unittest` (or `pytest`). `tests/__init__.py` and `conftest.py` add `bots/` and `bots/archive/` to `sys.path` so test modules can `import voidmaker` etc. by short name.
- Replay / backtest harnesses are in `tools/`. They read CSVs from `market_data/` (gitignored).
- Long-running processes on the VM should run inside `tmux` so they survive SSH drops.
