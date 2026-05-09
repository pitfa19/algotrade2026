# algotrade2026

ZSE ETF basket-arbitrage bot for AlgoTrade 2026.

## Strategy (v1)

Single WS connection to ZSE — the only venue listing every stock and every
ETF, so it's the only venue where the basket leg can be fully executed. For
each of the 5 ETFs, compare ETF touch vs the equal-weighted constituent
basket; when the edge clears `ARB_THRESHOLD` cents, fire IOC orders on every
leg simultaneously. Conservative caps (`ETF_POS_CAP = 20`, `ARB_SIZE = 5`).
In the last 30 s of a segment we stop opening and flatten residuals at market.

## Run on the team VM

The VM ships with PEP-668-managed Python 3.13, so install into a venv:

```sh
ssh root@vm.algotrade.hr                      # password: algotrade
cd ~/algotrade2026 && git pull origin fabijan

apt install -y python3.13-venv                # one-time
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

tmux new -s bot
python bot.py
# detach with Ctrl-b d; reattach with `tmux attach -t bot`
```

`LOG_LEVEL=DEBUG` for verbose order logs. `ZSE_HOST` overrides the host (e.g.
to the IP `10.0.210.2`) if DNS misbehaves.

## Self-dashboard

The official `dashboard.algotrade.hr` UI is unreliable. `dashboard.py` is a
local replacement that connects one WebSocket to each of the 10 exchanges,
polls `get_inventory` / `get_pending_orders` once a second, and serves a
small web UI.

```sh
pip install -r requirements.txt
python dashboard.py            # http://localhost:8080
PORT=9000 python dashboard.py  # override port
```

What it shows:

- Total NAV / PnL across all 10 exchanges (NAV = cash + Σ position × mid)
- Per-exchange status, cash, NAV, PnL, open positions, pending orders,
  segment time remaining
- Live order-book viewer (any instrument on any exchange)
- Filterable feed of recent trade / cancel events

Caveats: there is **no public leaderboard API** at AlgoTrade — order books
are aggregated/anonymous and trade events carry no team ID. The "score"
shown is your own NAV minus initial capital, which is exactly what the
official scoring formula is computed from. Each WS counts against the
per-exchange connection cap (10), so leave headroom if you also run a bot.
