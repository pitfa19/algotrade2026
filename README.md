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

```sh
ssh root@vm.algotrade.hr     # password: algotrade
git pull origin fabijan
pip install -r requirements.txt

tmux new -s bot
python bot.py
# detach with Ctrl-b d
```

`LOG_LEVEL=DEBUG` for verbose order logs. `ZSE_HOST` overrides the host (e.g.
to the IP `10.0.210.2`) if DNS misbehaves.
