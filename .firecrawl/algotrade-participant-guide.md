# AlgoTrade 2026 — Participant Guide¶

* * *

## 1) Overview¶

AlgoTrade 2026 is a simulated multi-exchange algorithmic trading competition. Your team will build and deploy a trading bot that trades 25 instruments (20 stocks + 5 ETFs) across 10 geographically distributed exchanges with realistic intercontinental latencies. One year of market volatility is compressed into a 30-minute session, creating a high density of trading opportunities within a short window.

Your goal is simple: **maximize profit.**

The strategy space is broad and there is no single "correct" approach. Cross-venue dynamics, the relationship between ETFs and their constituents, and reactions to market events are all areas worth investigating — figuring out where the edges are is part of the challenge.

* * *

## 2) Rounds & Schedule¶

### Evaluation Rounds¶

There are **4 evaluation rounds** , each lasting **30 minutes**. Each evaluation round is split into **3 segments of 10 minutes**.

The 75 competing teams are divided into **3 groups of 25**. Within an evaluation round, each group rotates through the 3 designated locations (one segment per location), so 25 teams share your latency profile during any given segment.

At the start of each segment:

  * All positions, cash, and inventory **reset to initial state**.
  * Your team is assigned to a different geographic location (see Exchanges section).



Your round score is the **sum of profits across all 3 segments**.

### Testing Rounds¶

Between evaluation rounds, there are **testing rounds** where you can iterate on your bot. Testing rounds are **not scored** — use them to experiment and improve.

### Presentations¶

Teams will present their strategies and results. Details on presentation format and schedule will be announced separately.

* * *

## 3) The Task¶

Your team must:

  1. **Build a trading bot** — an automated program that connects to exchanges via WebSocket and trades algorithmically.
  2. **Deploy it on your VM** — each team receives a virtual machine (4 vCPU, 6 GB RAM) where your bot runs.
  3. **Trade for profit** — across 3 segments per round, maximize your total PnL.



You can use **any programming language**. The exchange API is WebSocket-based with JSON messages — if your language can open a WebSocket and parse JSON, it works.

Your bot runs on your VM and connects to exchanges over the network. You are free to connect to as many or as few exchanges as you want, and to trade any subset of instruments.

* * *

## 4) Stocks¶

There are **20 stocks** organized into groups:

Group | Tickers  
---|---  
**Sector A** | NGUP, OIT, KTST, FSR, JZRO, XFR  
**Sector B** | KOTD, INA, HT, JNAF, DLKV, DDJH  
**Independent** | MDKA, KRAS, ZITO, ZABA, SIMP, CARD  
**Safe-Haven** | GOLD, XAG  
  
Key properties:

  * All stocks start at **$100.00**.
  * **Sector A** and **Sector B** stocks are correlated within their sector — when one moves, others in the same sector tend to follow.
  * **Independent** stocks have no sector correlation — they move with the broad market but not with each other.
  * **GOLD** and **XAG** are **safe-haven** assets — they tend to move **inversely** to the overall market. When most stocks fall, these tend to rise.
  * **CARD** and **SIMP** are special instruments listed on all 10 exchanges, their logic we leave for you to figure out.
  * Stocks are **not listed on every exchange** — each stock is available on a specific subset of venues. The listing table is below.



### Stock Listings by Exchange¶

Ticker | NYSE | NASDAQ | LSE | Euronext | JPX | SSE | HKEX | NSE | TMX | ZSE  
---|---|---|---|---|---|---|---|---|---|---  
CARD | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔  
SIMP | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔  
NGUP | ✔ | ✔ |  | ✔ |  |  |  |  | ✔ | ✔  
OIT |  |  | ✔ | ✔ |  |  | ✔ | ✔ |  | ✔  
KTST | ✔ |  |  |  | ✔ |  |  |  | ✔ | ✔  
FSR |  | ✔ | ✔ |  |  | ✔ | ✔ |  |  | ✔  
JZRO | ✔ |  | ✔ | ✔ |  |  |  |  | ✔ | ✔  
XFR | ✔ |  |  |  |  |  | ✔ |  | ✔ | ✔  
KOTD |  | ✔ | ✔ | ✔ |  |  | ✔ |  |  | ✔  
INA | ✔ | ✔ |  | ✔ |  |  | ✔ |  |  | ✔  
HT |  | ✔ | ✔ |  | ✔ | ✔ |  |  | ✔ | ✔  
JNAF | ✔ |  |  | ✔ | ✔ |  | ✔ |  |  | ✔  
DLKV |  | ✔ | ✔ |  |  |  | ✔ | ✔ |  | ✔  
DDJH | ✔ |  | ✔ | ✔ |  |  |  |  | ✔ | ✔  
MDKA | ✔ |  | ✔ |  |  |  | ✔ |  | ✔ | ✔  
KRAS | ✔ |  |  | ✔ |  | ✔ |  |  | ✔ | ✔  
ZITO |  | ✔ | ✔ | ✔ |  |  |  | ✔ |  | ✔  
ZABA | ✔ |  | ✔ |  |  | ✔ |  | ✔ | ✔ | ✔  
GOLD |  | ✔ |  | ✔ | ✔ |  |  |  | ✔ | ✔  
XAG |  |  | ✔ | ✔ | ✔ |  |  |  |  | ✔  
  
Note that **ZSE (Zagreb)** lists every stock — it is the only exchange with full coverage.

* * *

## 5) ETFs¶

There are **5 ETFs** — synthetic instruments whose value is derived from a basket of underlying stocks. ETFs trade exactly like stocks (same API, same order types), but their fair value tracks an equal-weighted average of their constituents.

ETF | Basket | Description  
---|---|---  
**ETFA** | NGUP, OIT, KTST, FSR, JZRO, XFR | Full Sector A  
**ETFB** | KOTD, INA, HT, JNAF, DLKV, DDJH | Full Sector B  
**ETFA3** | NGUP, KTST, XFR | Sector A subset (3 stocks)  
**ETFB3** | KOTD, INA, DLKV | Sector B subset (3 stocks)  
**ETFSH** | GOLD, XAG | Safe-Haven (moves inversely to market)  
  
### ETF Fair Value¶

An ETF's fair value is the **simple equal-weighted average** of its constituent stock prices:

\\[FV_{ETF} = \frac{1}{n} \sum_{i=1}^{n} P_i\\]

For example, if ETFA3 tracks {NGUP, KTST, XFR} and their prices are $102, $98, and $100, then ETFA3's fair value is $100.

### ETF Listings¶

ETF | NYSE | NASDAQ | LSE | Euronext | JPX | SSE | HKEX | NSE | TMX | ZSE  
---|---|---|---|---|---|---|---|---|---|---  
ETFA | ✔ |  |  | ✔ |  |  | ✔ |  |  | ✔  
ETFB |  | ✔ | ✔ |  |  |  | ✔ |  |  | ✔  
ETFA3 | ✔ |  |  |  |  |  |  |  | ✔ | ✔  
ETFB3 |  | ✔ |  |  |  |  | ✔ |  |  | ✔  
ETFSH |  |  |  | ✔ | ✔ |  |  |  |  | ✔  
  
* * *

## 6) Market Making & Liquidity¶

Every instrument on every exchange has a **Market Maker** — a built-in bot that continuously provides buy and sell quotes. The MM ensures you can always trade, even when no other teams are active.

Key MM characteristics:

  * Quotes **5 price levels** on each side (bid and ask), with **50 shares per level**.
  * Has **unlimited supply** — the order book is never empty.
  * Quotes a **tight spread** around its modeled fair value. The spread varies by asset.
  * Skews quotes based on accumulated inventory.



There are also **Noise Traders** that send random market orders, providing a source of fills for resting limit orders.

**Fees:** There are **zero trading fees** — no maker fees, no taker fees.

* * *

## 7) Exchanges & Latencies¶

### The 10 Exchanges¶

Each exchange listens on **port 9001** and is reachable on the venue network at both a hostname and an IP.

Exchange | Location | Hostname | IP  
---|---|---|---  
NYSE | New York | `nyse.algotrade.hr` | `10.0.201.2`  
NASDAQ | New York | `nasdaq.algotrade.hr` | `10.0.202.2`  
SSE | Shanghai | `sse.algotrade.hr` | `10.0.203.2`  
JPX | Tokyo | `jpx.algotrade.hr` | `10.0.204.2`  
Euronext | Paris | `euronext.algotrade.hr` | `10.0.205.2`  
LSE | London | `lse.algotrade.hr` | `10.0.206.2`  
HKEX | Hong Kong | `hkex.algotrade.hr` | `10.0.207.2`  
NSE | Mumbai | `nse.algotrade.hr` | `10.0.208.2`  
TMX | Toronto | `tmx.algotrade.hr` | `10.0.209.2`  
ZSE | Zagreb | `zse.algotrade.hr` | `10.0.210.2`  
  
### Location Rotation¶

Each round, your team rotates through **3 geographic locations** : **NYSE** (Americas), **ZSE** (Europe), and **HKEX** (Asia). One segment at each location, 10 minutes per segment. The order of rotation varies between groups.

Your "location" determines your network latency to each exchange. When you are co-located at NYSE, connections to NYSE and NASDAQ are nearly instant, but connections to HKEX are slow. When you rotate to HKEX, the opposite is true.

This rotation ensures no team has a permanent latency advantage — you must build a strategy that adapts to different latency profiles.

### Inter-Exchange Latency Matrix (round-trip, ms)¶

| NYSE | NASDAQ | SSE | JPX | Euronext | LSE | HKEX | NSE | TMX | ZSE  
---|---|---|---|---|---|---|---|---|---|---  
**NYSE** | 0 | 1 | 165 | 152 | 84 | 80 | 180 | 174 | 11 | 96  
**NASDAQ** | 1 | 0 | 165 | 152 | 84 | 80 | 180 | 174 | 11 | 96  
**SSE** | 165 | 165 | 0 | 18 | 160 | 156 | 19 | 54 | 159 | 145  
**JPX** | 152 | 152 | 18 | 0 | 145 | 141 | 37 | 53 | 145 | 140  
**Euronext** | 84 | 84 | 160 | 145 | 0 | 6 | 130 | 130 | 86 | 22  
**LSE** | 80 | 80 | 156 | 141 | 6 | 0 | 135 | 134 | 82 | 24  
**HKEX** | 180 | 180 | 19 | 37 | 130 | 135 | 0 | 53 | 174 | 150  
**NSE** | 174 | 174 | 54 | 53 | 130 | 134 | 53 | 0 | 174 | 95  
**TMX** | 11 | 11 | 159 | 145 | 86 | 82 | 174 | 174 | 0 | 98  
**ZSE** | 96 | 96 | 145 | 140 | 22 | 24 | 150 | 95 | 98 | 0  
  
These latencies affect both your order execution _and_ how fast price information propagates between exchanges.

* * *

## 8) Demo Bot & History Bot¶

Reference bots are provided in the `bots/` directory as starting points:

  * **`bots/python/demo_bot.py`** — minimal Python trading bot framework. Subclass `Strategy` and put your logic in `on_market_data()`.
  * **`bots/cpp/demo_bot.cpp`** — same structure, ported to C++20.
  * **`bots/python/history_bot.py`** — connects to exchanges and records all market data to CSV for offline analysis.



See **`bots/README.md`** for setup, dependencies, build instructions, environment variables, and the full callback list.

* * *

## 9) Trading UI¶

A web-based **trading dashboard** is provided for monitoring and manual interaction. Open it from any device on the venue network at:

http://dashboard.algotrade.hr

It is **not required** for competition — your bot operates independently — but it is useful for development and debugging.

### Features¶

  * **Order Book** — real-time display of bid/ask depth for each instrument
  * **Candlestick Charts** — price history with OHLCV candles
  * **Order Form** — manually place and cancel orders
  * **Inventory** — view your positions and cash balance across exchanges
  * **Pending Orders** — track your active orders
  * **Events Feed** — live stream of trades and cancellations
  * **Exchange Selector** — switch between exchanges



* * *

## 10) Network Access & Authentication¶

There are two ways to get on the venue network:

  * **Wired (recommended).** Each team's table has a switch with Ethernet ports — plug in and you are on. We strongly recommend the wired connection for stability.
  * **WiFi.** Each team is given a per-team WiFi login at the start of the event.



The exchange network is not reachable from outside the venue.

Once connected, the following names resolve:

  * `vm.algotrade.hr` — your team's VM (SSH)
  * `dashboard.algotrade.hr` — trading UI
  * `<exchange>.algotrade.hr` — the 10 exchanges (full list in §7)



Authentication is **automatic** — there is no token, secret, or login step on the WebSocket connection itself. Any connection you open while on the venue network is recognised as your team's:
    
    
    ws://<exchange_host>:9001/trade
    

This applies equally to your VM and to your own laptops. All connections from your team are treated as a single account for inventory, rate limits, and order ownership.

For SSH and file transfer, see **`Algotrade-Network-SSH-Guide.md`**.

* * *

## 11) API — Connecting to an Exchange¶

Full API reference: see **`WEBSOCKET_API.md`** in the docs root.

### Quick Start¶

Connect via WebSocket:
    
    
    ws://nyse.algotrade.hr:9001/trade        # by hostname
    ws://10.0.201.2:9001/trade               # equivalent, by IP
    

Each exchange runs on its own host on port **9001**. The full hostname/IP table is in §7 (and mirrored in `bots/README.md`).

You may open **multiple concurrent connections to the same exchange** if it helps your architecture (for example, a separate connection dedicated to recording market data while your trading bot uses another). All connections from your team are treated as a single account for inventory, rate limits, and order ownership purposes.

### Message Format¶

All messages are JSON. Every message has a `type` field. Include a `user_request_id` in your requests to correlate responses.

### Key Operations¶

**Place an order:**
    
    
    {
      "type": "add_order",
      "user_request_id": "order-1",
      "instrument_id": "NYSE-CARD",
      "price": 10050,
      "expiry": 1740001000000,
      "side": "bid",
      "quantity": 10
    }
    

  * **Prices are in cents** (10050 = \\(100.50). The minimum tick size is **1 cent** (\\)0.01) — non-integer cent values are rejected.
  * **`side`** is `"bid"` (buy) or `"ask"` (sell).
  * **`expiry`** is a Unix epoch timestamp in **milliseconds**. The order is automatically cancelled at that time if still resting on the book.



**Cancel an order:**
    
    
    {
      "type": "cancel_order",
      "user_request_id": "cancel-1",
      "order_id": 42,
      "instrument_id": "NYSE-CARD"
    }
    

**Check your inventory:**
    
    
    {
      "type": "get_inventory",
      "user_request_id": "inv-1"
    }
    

### Market Data¶

You automatically receive **market data broadcasts every 100 ms** after connecting. Each broadcast includes:

  * **Order book depth** — top price levels on each side for every instrument
  * **Candles** — completed 1-second OHLCV candles
  * **Events** — recent trades and cancellations (from all participants)



Instrument IDs follow the format `<EXCHANGE>-<TICKER>` (e.g., `NYSE-CARD`, `HKEX-GOLD`).

The displayed order book **aggregates all resting orders from every participant** (your team, other teams, and the built-in market maker) — you cannot tell from the book alone whose orders sit at which level. Trade events are also **anonymous** : the broadcast carries price, quantity, and order IDs, but no team or bot identifier.

The `end_of_round` message is sent at the end of every **segment** (every 10 minutes). After it fires the exchange shuts down — your connection will close. A fresh exchange (with reset positions and cash) starts for the next segment, and you must reconnect.

### Minimal Python Example¶
    
    
    import asyncio
    import json
    import time
    from websockets.asyncio.client import connect as ws_connect
    
    SERVER_URL = "ws://nyse.algotrade.hr:9001/trade"  # NYSE — see §7 for the full table
    
    async def main():
        async with ws_connect(SERVER_URL) as ws:
            welcome = json.loads(await ws.recv())
            print(f"Connected: {welcome['message']}")
    
            # Place a buy order for CARD at $100.00
            await ws.send(json.dumps({
                "type": "add_order",
                "user_request_id": "order-1",
                "instrument_id": "NYSE-CARD",
                "price": 10000,
                "expiry": int(time.time() * 1000) + 60000,
                "side": "bid",
                "quantity": 10
            }))
    
            async for message in ws:
                data = json.loads(message)
                if data["type"] == "add_order_response":
                    print(f"Order result: {data}")
                elif data["type"] == "market_data_update":
                    # Process market data here
                    pass
                elif data["type"] == "end_of_round":
                    print("Round ended!")
                    break
    
    asyncio.run(main())
    

* * *

## 12) VM Access & SSH¶

Each team receives a dedicated virtual machine:

  * **Specs:** 4 vCPU, 6 GB RAM
  * **OS:** Linux
  * **Hostname:** `vm.algotrade.hr`
  * **User:** `root`
  * **Initial password:** `algotrade`



### Connecting via SSH¶
    
    
    ssh root@vm.algotrade.hr        # password: algotrade
    

You have full root access to install packages and configure your environment.

### Deploying Your Bot¶

  1. SSH into your VM (`ssh root@vm.algotrade.hr`).
  2. Transfer your bot code (`scp`, `rsync`, `git clone`, `VS Code Remote-SSH` or `Filezilla`).
  3. Install dependencies (`pip install`, `npm install`, etc.).
  4. Run your bot — preferably inside `tmux` or `screen` so it survives disconnects.



For SSH/SCP/SFTP usage on every common OS, key setup, port forwarding, and troubleshooting, see **`Algotrade-Network-SSH-Guide.md`**.

* * *

## 13) Limits, Orders & Scoring¶

### Short Selling¶

Short selling is **allowed**. You can sell an instrument you don't own to profit from a price decrease. Your net position per instrument per exchange may not fall below **−200 shares**.

### Position & Risk Limits¶

Limit | Value  
---|---  
Starting capital | $100,000 per exchange ($1,000,000 total across 10 exchanges)  
Starting price | ~$100.00 per stock/ETF  
Short position floor | Net position no lower than −200 shares per instrument per exchange  
Cash floor | Cash balance per exchange may not fall below **−$50,000**  
Max pending orders | 6,000 per team per exchange  
Message rate limit | 500 messages per team per exchange per second  
  
**Capital is isolated per exchange** — cash on NYSE cannot be used to cover positions on HKEX.

### Order Types¶

  * **Limit** — rests on the book at your specified price until filled, cancelled, or expired.
  * **Market** — executes immediately against the best available prices in the book; any unfilled remainder is cancelled.
  * **IOC (Immediate-Or-Cancel)** — like a limit order but the unfilled remainder is cancelled instead of resting.



### Matching Engine¶

Orders are matched using **price-time priority (FIFO)** :

  1. Best price first.
  2. At the same price, earliest order first.
  3. Trades execute at the resting (passive) order's price.



### Scoring¶

Your score for a round is based on your **total profit** across the 3 segments, scaled logarithmically against the round's top performer:

\\[\text{score} = \text{score}_{\max} \cdot \max\\!\left(0,\; \frac{\ln(1 + \max(P,\, 0))}{\ln(1 + P_{\max})}\right)\\]

  * \\(P\\) = your total profit across the 3 segments of the round
  * \\(P_{\max}\\) = highest total profit among all teams in the round
  * \\(\text{score}_{\max}\\) = the points awarded to the top performer
  * Zero or negative profit yields a score of 0



The log scaling compresses the top of the distribution, so doubling the top team's profit does not double your score.

### Settlement¶

## At the end of each segment, all open positions are marked to market and settled. The exact methodology is **not disclosed in advance** , it is some time- or volume-weighted measure over a window near the close.¶

## 14) Q&A / Discord¶

### Getting Help¶

  * **Discord** — join the competition Discord server for announcements, Q&A, and real-time support.
  * **On-site support** — organizers will be available during the event to answer technical questions.



### Common Questions¶

**Q: Can I connect to multiple exchanges at once?** A: Yes — that is the expected setup. You can hold connections to all 10 exchanges simultaneously, and you can also open more than one connection to the same exchange (for example, a dedicated connection for recording market data alongside your trading bot). All connections from your team are treated as a single account.

**Q: Do I need to trade on all exchanges?** A: No. You can focus on a subset of exchanges or instruments.

**Q: What happens if my bot crashes mid-segment?** A: Within the segment, your existing orders remain on the book until they expire or are cancelled, and your positions and cash are preserved server-side — restart and reconnect to resume. Note that segment boundaries reset everything regardless, so a crash near the end of a segment is mostly recovered by the reset itself.

**Q: Can I trade manually using the UI?** A: The UI is provided for monitoring and debugging. Manual trading is technically possible but not practical at competition speed.

**Q: What happens to my connections at a segment boundary?** A: After `end_of_round` fires, the exchange shuts down and your WebSocket connections close. A fresh exchange (with reset positions and cash) starts for the next segment — your bot must reconnect. Build a reconnect loop with a short backoff; the new server may take a few seconds to come up.

* * *

_Good luck, and may the best algorithm win._
