# Home

Algorithmic trading competition  ·  May 2026, Zagreb

### Participant Guide

Rules, exchanges, instruments, scoring.

### WebSocket API

Orders, market data, responses, error codes.

### Reference Bots

Starter bots in Python and C++.

### Network & SSH

VM access, file transfer, tmux.

* * *

Build a bot, connect to the exchange network, trade 25 instruments across 10 venues. Four 30-minute rounds, 75 teams. Profit wins.

* * *

## Getting started¶

SSH into your VM and run the demo bot:
    
    
    ssh root@vm.algotrade.hr   # password: algotrade
    
    python bots/python/demo_bot.py
    

Your bot connects to `ws://<exchange>.algotrade.hr:9001/trade`. Market data arrives every 100 ms. See the Participant Guide for the full exchange list and rules.

Run your bot in `tmux` so it survives disconnects:
    
    
    tmux new -s bot
    python my_bot.py
    # Ctrl-b d to detach
    

* * *

Made by X.FER with \<3
