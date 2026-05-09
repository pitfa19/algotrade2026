# AlgoTrade 2026 — Reference Bots

Starter bots to help you get up and running quickly.

All bots are pre-configured for the **production exchange network**. Each exchange has a fixed IP, a venue-DNS hostname, and listens on port 9001. Authentication is by team IP — no team secret is sent in the URL.

Exchange | Hostname | IP  
---|---|---  
NYSE | `nyse.algotrade.hr` | `10.0.201.2`  
NASDAQ | `nasdaq.algotrade.hr` | `10.0.202.2`  
SSE | `sse.algotrade.hr` | `10.0.203.2`  
JPX | `jpx.algotrade.hr` | `10.0.204.2`  
Euronext | `euronext.algotrade.hr` | `10.0.205.2`  
LSE | `lse.algotrade.hr` | `10.0.206.2`  
HKEX | `hkex.algotrade.hr` | `10.0.207.2`  
NSE | `nse.algotrade.hr` | `10.0.208.2`  
TMX | `tmx.algotrade.hr` | `10.0.209.2`  
ZSE | `zse.algotrade.hr` | `10.0.210.2`  
  
The demo bots use the IPs by default. Either form works.

By default each bot connects to all 10 exchanges. Set `EXCHANGES` to a comma-separated list of names to narrow the set.

## Python Demo Bot

**File:** `python/demo_bot.py`

A trading bot framework with a clean strategy interface. The bot handles all WebSocket connections, message parsing, and market state management. You implement your strategy by subclassing `Strategy`.
    pip install websockets
    python python/demo_bot.py
    
    # Or restrict to a few exchanges:
    EXCHANGES="NYSE,NASDAQ,LSE" python python/demo_bot.py
    
### Structure

Class | Purpose  
---|---  
`Strategy` | Abstract base — override `on_market_data()` with your logic  
`SimpleStrategy` | Example that prints top-of-book (does not trade)  
`MarketState` | Aggregated order books, candles, trades across all exchanges  
`ExchangeConnection` | One WebSocket connection with order/cancel/inventory helpers  
`Bot` | Orchestrator — connects to exchanges, dispatches events to your strategy  
  
### Implement Your Strategy
    class MyStrategy(Strategy):
        async def on_market_data(self, bot: Bot, exchange: str, state: MarketState) -> None:
            book = state.get_book(exchange, f"{exchange}-CARD")
            if book and book.best_bid and book.best_ask and book.spread > 200:
                # Your logic here — bot is passed directly into every callback
                await bot.place_order(exchange, f"{exchange}-CARD", "bid",
                                      book.best_bid + 1, 10)
    
    # In main():
    strategy = MyStrategy()
    bot = Bot(strategy)
    asyncio.run(bot.run())
    
* * *

## C++ Demo Bot

**File:** `cpp/demo_bot.cpp`

Same architecture as the Python version, ported to C++20.

### Dependencies

  * [Boost.Beast](<https://www.boost.org/doc/libs/release/libs/beast/>) — WebSocket client (part of Boost)
  * [nlohmann/json](<https://github.com/nlohmann/json>) — JSON parsing

    # Ubuntu/Debian
    sudo apt install libboost-all-dev nlohmann-json3-dev
    
    # Arch Linux
    sudo pacman -S boost nlohmann-json
    
### Build & Run
    # Option A: Direct compile
    g++ -std=c++20 -O2 -o demo_bot cpp/demo_bot.cpp -lpthread
    
    # Option B: CMake
    cd cpp && mkdir build && cd build
    cmake .. && make
    
    # Run — defaults to all 10 production exchanges
    ./demo_bot
    
    # Or restrict to a few exchanges:
    EXCHANGES="NYSE,NASDAQ,LSE" ./demo_bot
    
### Implement Your Strategy
    class MyStrategy : public Strategy {
    public:
        void on_market_data(Bot& bot, const std::string& exchange,
                            const MarketState& state) override {
            auto book = state.get_book(exchange, exchange + "-CARD");
            if (book && book->best_bid() && book->best_ask()
                && *book->spread() > 200) {
                // Your logic here — bot is passed directly into every callback
                bot.place_order(exchange, exchange + "-CARD", "bid",
                                *book->best_bid() + 1, 10);
            }
        }
    };
    
* * *

## Python History Bot

**File:** `python/history_bot.py`

Connects to exchanges and records all market data to CSV files for offline analysis.
    pip install websockets
    python python/history_bot.py
    
    # Or restrict to a few exchanges and choose an output dir:
    EXCHANGES="NYSE,NASDAQ" OUTPUT_DIR="./market_data" python python/history_bot.py
    
### Output Files (per exchange)

File | Contents | Columns  
---|---|---  
`<EXCH>_orderbooks.csv` | Top-of-book snapshots every tick | time, instrument, bid1-3 price/qty, ask1-3 price/qty  
`<EXCH>_trades.csv` | All trade events | time, instrument, price, quantity, passive/active order IDs  
`<EXCH>_candles.csv` | Completed OHLCV candles | index, instrument, open, high, low, close, volume  
`<EXCH>_events.csv` | Cancel events | time, instrument, order_id, expired  
  
### Example: Loading Data for Analysis
    import pandas as pd
    
    # Load order book history
    ob = pd.read_csv("market_data/NYSE_orderbooks.csv")
    
    # Get CARD mid prices over time
    card = ob[ob["instrument"] == "NYSE-CARD"].copy()
    card["mid"] = (card["bid1_price"] + card["ask1_price"]) / 2
    card.plot(x="time", y="mid", title="NYSE-CARD Mid Price")
    
    # Load trades
    trades = pd.read_csv("market_data/NYSE_trades.csv")
    print(f"Total trades: {len(trades)}")
    
* * *

## Environment Variables

All bots share these environment variables:

Variable | Description | Default  
---|---|---  
`EXCHANGES` | Comma-separated list of exchange names. Known names: `NYSE`, `NASDAQ`, `SSE`, `JPX`, `Euronext`, `LSE`, `HKEX`, `NSE`, `TMX`, `ZSE`. | all 10 exchanges  
`OUTPUT_DIR` | History bot output directory | `./market_data`  
  
> The production network authenticates by team IP, so no `TEAM_SECRET` is required. Per-exchange hosts and the port (9001) are hardcoded — edit the `EXCHANGE_HOSTS` constant at the top of each bot if your network differs.

