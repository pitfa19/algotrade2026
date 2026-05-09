# AlgoTrade 2026 — WebSocket API Reference¶

Complete documentation for the exchange WebSocket API. All trading and market data flows through a single WebSocket connection.

* * *

## Table of Contents¶

  * Connection
  * Endpoint
  * Authentication
  * Connection Limits
  * Rate Limits
  * WebSocket Configuration
  * Message Protocol
  * General Format
  * Message Flow
  * Server → Client Messages
  * Welcome Message
  * Market Data Update (Broadcast)
  * End of Round
  * Client → Server Requests
  * Add Order
  * Cancel Order
  * Get Inventory
  * Get Pending Orders
  * Get Market Data
  * Response Types
  * Add Order Response
  * Cancel Order Response
  * Get Inventory Response
  * Get Pending Orders Response
  * Error Response
  * Data Types Reference
  * Scalar Types
  * Order Object
  * Orderbook Depth
  * Candle (OHLCV)
  * Trade Event
  * Cancel Event
  * Market Data Details
  * Broadcast Interval
  * Orderbook Depth
  * Candle Aggregation
  * Events
  * Trading Rules
  * Order Matching
  * Order Expiry
  * Position Limits
  * Round Lifecycle
  * HTTP Endpoints
  * Health Check
  * Client Examples
  * Python
  * JavaScript
  * Error Reference



* * *

## Connection¶

### Endpoint¶
    
    
    ws://<host>:9001/trade
    

Parameter | Default | Description  
---|---|---  
`host` | — | Exchange hostname or IP (see participant guide §7)  
port | `9001` | Fixed  
  
### Authentication¶

On the venue network, authentication is **automatic** — the server identifies your team by the source IP of the connection. There is no token to send and no login message. Open the WebSocket and you are in.

Outcome | HTTP Status | Description  
---|---|---  
Success | `101 Switching Protocols` | Connection upgraded, welcome message sent  
Unrecognised source IP | `401 Unauthorized` | Client IP does not map to any registered team  
Open rate exceeded | `429 Too Many Requests` | Too many connection attempts per second  
Connection cap reached | `429 Too Many Requests` | Team already has maximum concurrent connections  
  
### Connection Limits¶

Limit | Value | Description  
---|---|---  
Max connections per team | **10** | Concurrent WebSocket connections per team  
Connection opens per team/second | **10** | Maximum new connections a team can open per second  
  
### Rate Limits¶

Limit | Value | Window | Description  
---|---|---|---  
Messages per team per second | **500** | 1000 ms | Maximum messages a team can send per second  
  
Exceeding the message rate limit causes the server to send `"Message rate limit exceeded"` as a text frame and **immediately close** the WebSocket connection.

### WebSocket Configuration¶

Setting | Value | Description  
---|---|---  
Compression | Disabled | No per-message compression  
Max payload length | 16 MB | Maximum size of a single WebSocket frame  
Idle timeout | 60 seconds | Connection closed after 60 s of inactivity  
Max backpressure | 16 MB | Maximum buffered outgoing data before force-close  
Close on backpressure limit | Yes | Connection forcibly closed if backpressure limit is exceeded  
Reset idle timeout on send | Yes | Server-sent messages reset the idle timer  
Automatic pings | Yes | Server sends WebSocket pings to keep connections alive  
  
* * *

## Message Protocol¶

### General Format¶

All messages are JSON objects sent as WebSocket **text** frames. Every message has a `type` field identifying the message kind.
    
    
    { "type": "<message_type>", ... }
    

### Message Flow¶
    
    
    Client                                            Server
      │                                                  │
      │──── WS upgrade (auto-auth by source IP) ────────>│
      │<────────────── Welcome message ──────────────────│
      │                                                  │
      │<──────── Market Data Broadcast (every 100ms) ────│  (automatic, pub/sub)
      │<──────── Market Data Broadcast ──────────────────│
      │                                                  │
      │──── add_order request ──────────────────────────>│
      │<──── add_order_response ─────────────────────────│
      │                                                  │
      │──── cancel_order request ───────────────────────>│
      │<──── cancel_order_response ──────────────────────│
      │                                                  │
      │──── get_inventory request ──────────────────────>│
      │<──── get_inventory_response ─────────────────────│
      │                                                  │
      │──── get_pending_orders request ─────────────────>│
      │<──── get_pending_orders_response ────────────────│
      │                                                  │
      │──── get_market_data request ────────────────────>│
      │<──── market_data_update (with user_request_id) ──│
      │                                                  │
      │       ... (round continues) ...                  │
      │                                                  │
      │<──── end_of_round ───────────────────────────────│
    

On connection open the client is automatically subscribed to the `market_data` pub/sub topic. Market data broadcasts arrive every **100 ms** without any explicit subscription request.

* * *

## Server → Client Messages¶

### Welcome Message¶

Sent immediately after a successful WebSocket upgrade.
    
    
    {
      "type": "welcome",
      "message": "Connected to OrderBook API"
    }
    

Field | Type | Description  
---|---|---  
`type` | string | Always `"welcome"`  
`message` | string | Human-readable connection greeting  
  
### Market Data Update (Broadcast)¶

Pushed to **all connected clients** every **100 ms** (configurable via `PLAYER_UPDATE_MS`). Contains the full current market snapshot.
    
    
    {
      "type": "market_data_update",
      "time": 52300,
      "candles": {
        "tradeable": {
          "NYSE-CARD": [
            {
              "open": 10000,
              "close": 10020,
              "high": 10030,
              "low": 9990,
              "mid": null,
              "volume": 150,
              "index": 1740000
            }
          ],
          "NYSE-SIMP": []
        }
      },
      "orderbook_depths": {
        "NYSE-CARD": {
          "bids": {
            "10000": 25,
            "9990":  50,
            "9980":  100,
            "9970":  120,
            "9960":  150
          },
          "asks": {
            "10020": 30,
            "10030": 45,
            "10040": 80,
            "10050": 90,
            "10060": 110
          }
        }
      },
      "events": [
        {
          "event_type": "trade",
          "data": {
            "instrumentID": "NYSE-CARD",
            "passiveOrderID": 42,
            "activeOrderID": 99,
            "quantity": 10,
            "price": 10010,
            "time": 52250
          }
        },
        {
          "event_type": "cancel",
          "data": {
            "orderID": 38,
            "instrumentID": "NYSE-CARD",
            "time": 52200,
            "expired": false
          }
        }
      ]
    }
    

Field | Type | Description  
---|---|---  
`type` | string | Always `"market_data_update"`  
`user_request_id` | string | null | Present only when this is a response to `get_market_data`  
`time` | integer | Current server time in ms (relative to round start)  
`candles` | object `{ tradeable: { [instrument]: Candle[] } }` | New completed candles since last broadcast (may be empty)  
`orderbook_depths` | object `{ [instrument]: OrderbookDepth }` | Top-of-book depth for every tradeable instrument  
`events` | array of `TradeEvent \| CancelEvent` | Events (trades, cancellations) that occurred since last broadcast  
  
> **Note:** The `candles` field contains only **newly completed** candles — not the full history. The current in-progress candle is never included. The `events` array contains only events since the previous broadcast, not a full event log.

### End of Round¶

Broadcast to all clients when the trading round ends and final settlement begins. In the competition this fires at the end of every 10-minute segment.
    
    
    {
      "type": "end_of_round"
    }
    

After this message: \- No new orders can be placed (requests return an error). \- All open orders are automatically cancelled. \- Final settlement is performed (positions are marked to market). \- The exchange shuts down shortly after; your connection will close. A fresh exchange (with reset state) starts for the next segment — reconnect when it is up.

* * *

## Client → Server Requests¶

Every request must include a `type` field. Most requests should also include a `user_request_id` (a client-chosen string) so you can correlate responses.

### Add Order¶

Place a new order on an instrument. Three order types are supported: `limit` (default), `ioc`, and `market`.
    
    
    {
      "type": "add_order",
      "user_request_id": "my-req-1",
      "instrument_id": "NYSE-CARD",
      "price": 10050,
      "expiry": 1740001000000,
      "side": "bid",
      "quantity": 10,
      "order_type": "limit"
    }
    

Field | Type | Required | Description  
---|---|---|---  
`type` | string | Yes | Must be `"add_order"`  
`user_request_id` | string | Yes | Client-generated correlation ID  
`instrument_id` | string | Yes | Instrument to trade (e.g. `"NYSE-CARD"`)  
`side` | string | Yes | `"bid"` (buy) or `"ask"` (sell)  
`quantity` | integer | Yes | Number of units (must be > 0)  
`order_type` | string | No (default `"limit"`) | One of `"limit"`, `"ioc"`, `"market"`  
`price` | integer | Yes for `limit`/`ioc` | Limit price in **cents** (must satisfy `0 < price < 1,000,000`). Ignored for `market`.  
`expiry` | integer | Yes for `limit`/`ioc` | Expiry time in **Unix milliseconds** (must be in the future). Ignored for `market`.  
  
**Order types:**

Type | Behavior  
---|---  
`limit` | Standard resting order. Matches what it can immediately at `price` or better; remainder rests on the book until expiry or cancel.  
`ioc` | Immediate-or-Cancel. Matches what it can immediately at `price` or better; any unfilled remainder is cancelled (does not rest).  
`market` | Aggressive cross. `price` and `expiry` are not required — the server crosses the book and any unfilled remainder is cancelled. Fails if the opposite side is empty.  
  
**Validation rules:** \- `quantity > 0` \- `side` must be `"bid"` or `"ask"` \- `order_type` (if provided) must be `"limit"`, `"ioc"`, or `"market"` \- For `limit` / `ioc`: `0 < price < 1,000,000`, and `expiry` must be strictly greater than current server time \- For `market`: the opposite side of the book must be non-empty \- The `instrument_id` must refer to a valid tradeable instrument \- The round must not have ended \- The team must not exceed 6,000 pending orders \- Sufficient cash balance (for bids) or instrument position (for asks) required

**Matching behavior:** When a new order is added, the engine immediately attempts to match it against resting orders on the opposite side (price-time priority). Any resulting trades are executed atomically. For `limit` orders the unfilled remainder rests on the book; for `ioc` and `market` it is cancelled.

### Cancel Order¶

Cancel an existing live order.
    
    
    {
      "type": "cancel_order",
      "user_request_id": "my-req-2",
      "order_id": 42,
      "instrument_id": "NYSE-CARD"
    }
    

Field | Type | Required | Description  
---|---|---|---  
`type` | string | Yes | Must be `"cancel_order"`  
`user_request_id` | string | Yes | Client-generated correlation ID  
`order_id` | integer | Yes | The order ID to cancel (must be > 0)  
`instrument_id` | string | Yes | Instrument the order belongs to  
  
**Validation rules:** \- `order_id > 0` \- The order must exist and be live \- The order must belong to the requesting team \- If the round has ended, returns an error (all orders are auto-cancelled at round end)

### Get Inventory¶

Request the team's current inventory (positions and cash balance).
    
    
    {
      "type": "get_inventory",
      "user_request_id": "my-req-3"
    }
    

Field | Type | Required | Description  
---|---|---|---  
`type` | string | Yes | Must be `"get_inventory"`  
`user_request_id` | string | Yes | Client-generated correlation ID  
  
### Get Pending Orders¶

Request all live (pending) orders for the team across all instruments.
    
    
    {
      "type": "get_pending_orders",
      "user_request_id": "my-req-4"
    }
    

Field | Type | Required | Description  
---|---|---|---  
`type` | string | Yes | Must be `"get_pending_orders"`  
`user_request_id` | string | Yes | Client-generated correlation ID  
  
If the round has ended, returns an empty data set (all orders have been cancelled).

### Get Market Data¶

Request the latest cached market data snapshot (identical format to the broadcast).
    
    
    {
      "type": "get_market_data",
      "user_request_id": "my-req-5"
    }
    

Field | Type | Required | Description  
---|---|---|---  
`type` | string | Yes | Must be `"get_market_data"`  
`user_request_id` | string | Yes | Client-generated correlation ID  
  
Returns a `market_data_update` message with `user_request_id` populated from the request. This is useful for getting an immediate snapshot without waiting for the next broadcast.

* * *

## Response Types¶

### Add Order Response¶
    
    
    {
      "type": "add_order_response",
      "user_request_id": "my-req-1",
      "success": true,
      "data": {
        "order_id": 42,
        "message": null,
        "immediate_inventory_change": null,
        "immediate_balance_change": null
      }
    }
    

Field | Type | Description  
---|---|---  
`type` | string | Always `"add_order_response"`  
`user_request_id` | string | Echoed from the request  
`success` | boolean | `true` if the order was successfully placed  
`data.order_id` | integer | null | Assigned order ID (present on success)  
`data.message` | string | null | Error/info message (present on failure)  
`data.immediate_inventory_change` | integer | null | Immediate position change from fills at placement (null if no fill)  
`data.immediate_balance_change` | integer | null | Immediate cash change from fills at placement (null if no fill)  
  
**On failure** , `success` is `false` and `data.message` contains the reason. `data.order_id` will be absent or null.

**On a fill at placement** (typical for IOC/market orders, or aggressive limit orders that cross), `immediate_inventory_change` and `immediate_balance_change` are populated. `immediate_inventory_change` is positive on a buy and negative on a sell; `immediate_balance_change` has the opposite sign. They are both `null` when the order rests without crossing.

### Cancel Order Response¶
    
    
    {
      "type": "cancel_order_response",
      "user_request_id": "my-req-2",
      "success": true,
      "message": null
    }
    

Field | Type | Description  
---|---|---  
`type` | string | Always `"cancel_order_response"`  
`user_request_id` | string | Echoed from the request  
`success` | boolean | `true` if the order was cancelled  
`message` | string | null | Error message (present on failure)  
  
### Get Inventory Response¶
    
    
    {
      "type": "get_inventory_response",
      "user_request_id": "my-req-3",
      "data": {
        "$": [0, 10000000],
        "NYSE-CARD": [500, 1500],
        "NYSE-SIMP": [0, -200]
      }
    }
    

Field | Type | Description  
---|---|---  
`type` | string | Always `"get_inventory_response"`  
`user_request_id` | string | Echoed from the request  
`data` | object | Map of `instrument_id` → `[reserved, total]` (pair of integers)  
  
**Inventory fields:**

Key | Description  
---|---  
`"$"` | Cash balance. `reserved` = cash locked in pending bid orders. `total` = total cash including reserved  
Other | Instrument positions. `reserved` = units locked in pending ask orders. `total` = net position  
  
  * **Initial cash balance** is **10,000,000 cents** ($100,000) per team per exchange.
  * Positive `total` = long position; negative `total` = short position.



### Get Pending Orders Response¶
    
    
    {
      "type": "get_pending_orders_response",
      "user_request_id": "my-req-4",
      "data": {
        "NYSE-CARD": [
          [
            {
              "orderID": 42,
              "teamID": 100,
              "price": 10000,
              "time": 50000,
              "expiry": 1740001000000,
              "side": "BID",
              "unfilled_quantity": 10,
              "total_quantity": 10,
              "live": true
            }
          ],
          [
            {
              "orderID": 43,
              "teamID": 100,
              "price": 10100,
              "time": 50100,
              "expiry": 1740001000000,
              "side": "ASK",
              "unfilled_quantity": 5,
              "total_quantity": 5,
              "live": true
            }
          ]
        ]
      }
    }
    

Field | Type | Description  
---|---|---  
`type` | string | Always `"get_pending_orders_response"`  
`user_request_id` | string | Echoed from the request  
`data` | object | Map of `instrument_id` → `[bid_orders[], ask_orders[]]` (pair of order arrays)  
  
Each order in the arrays is an Order Object.

### Error Response¶

Returned when a request fails parsing or an unknown command is sent.
    
    
    {
      "type": "error",
      "user_request_id": "",
      "message": "Unknown command type: foo"
    }
    

Field | Type | Description  
---|---|---  
`type` | string | Always `"error"`  
`user_request_id` | string | Echoed from request (may be empty)  
`message` | string | Human-readable error description  
  
* * *

## Data Types Reference¶

### Scalar Types¶

All numeric values are **64-bit signed integers** (transmitted as JSON numbers).

Type Name | JSON Type | Description  
---|---|---  
`TeamID_t` | integer | Unique team identifier  
`OrderID_t` | integer | Unique order identifier (auto-incrementing per orderbook)  
`Price_t` | integer | Price in **cents** (e.g. 10000 = $100.00)  
`Quantity_t` | integer | Number of units (shares)  
`Time_t` | integer | Time in milliseconds (server-relative or Unix epoch)  
`InstrumentID_t` | string | Instrument identifier (e.g. `"NYSE-CARD"`)  
  
### Order Object¶

Returned in `get_pending_orders_response`.

Field | Type | Description  
---|---|---  
`orderID` | integer | Unique order identifier  
`teamID` | integer | Team that placed the order  
`price` | integer | Limit price in cents  
`time` | integer | Server time when order was placed (ms)  
`expiry` | integer | Expiry time (Unix ms or server time depending on context)  
`side` | string | `"BID"` or `"ASK"`  
`unfilled_quantity` | integer | Remaining quantity not yet matched  
`total_quantity` | integer | Original order quantity  
`live` | boolean | `true` if the order is still active on the book  
  
### Orderbook Depth¶

Top 5 price levels on each side, per instrument.
    
    
    {
      "bids": {
        "10000": 25,
        "9990":  50,
        "9980":  100,
        "9970":  120,
        "9960":  150
      },
      "asks": {
        "10020": 30,
        "10030": 45,
        "10040": 80,
        "10050": 90,
        "10060": 110
      }
    }
    

Field | Type | Description  
---|---|---  
`bids` | object `{ [price]: quantity }` | Aggregated quantities at the top 5 bid price levels  
`asks` | object `{ [price]: quantity }` | Aggregated quantities at the top 5 ask price levels  
  
  * Prices are integer strings (JSON object keys).
  * Quantities are integers.
  * **Depth is limited to 5 levels** on each side.
  * If fewer than 5 levels exist, only the available levels are returned.
  * An empty side is represented as `{}`.



### Candle (OHLCV)¶

Candle data for tradeable instruments. Each candle represents **1 second** of real time (mapped from 1 "in-game hour").
    
    
    {
      "open": 10000,
      "close": 10020,
      "high": 10030,
      "low": 9990,
      "mid": null,
      "volume": 150,
      "index": 1740000
    }
    

Field | Type | Description  
---|---|---  
`open` | integer | null | First trade price in the candle period  
`close` | integer | null | Last trade price in the candle period  
`high` | integer | null | Highest trade price in the candle period  
`low` | integer | null | Lowest trade price in the candle period  
`mid` | integer | null | Always `null` — field is reserved and never populated. Ignore.  
`volume` | integer | null | Total quantity traded in the candle period  
`index` | integer | Candle index (absolute, based on start time)  
  
  * Only **completed** candles are broadcast. The in-progress candle is never sent.
  * A candle with no trades will have all OHLC fields as `null` and `volume` as `0` or `null`.
  * Candles are sent incrementally — only newly completed candles since the last broadcast are included.



### Trade Event¶

Occurs when two orders match.
    
    
    {
      "event_type": "trade",
      "data": {
        "instrumentID": "NYSE-CARD",
        "passiveOrderID": 42,
        "activeOrderID": 99,
        "quantity": 10,
        "price": 10010,
        "time": 52250
      }
    }
    

Field | Type | Description  
---|---|---  
`event_type` | string | Always `"trade"`  
`data.instrumentID` | string | Instrument where the trade occurred  
`data.passiveOrderID` | integer | Order ID of the resting (maker) order  
`data.activeOrderID` | integer | Order ID of the incoming (taker) order  
`data.quantity` | integer | Number of units traded  
`data.price` | integer | Execution price in cents  
`data.time` | integer | Server time of the trade (ms)  
  
### Cancel Event¶

Occurs when an order is cancelled (manually or by expiry).
    
    
    {
      "event_type": "cancel",
      "data": {
        "orderID": 38,
        "instrumentID": "NYSE-CARD",
        "time": 52200,
        "expired": false
      }
    }
    

Field | Type | Description  
---|---|---  
`event_type` | string | Always `"cancel"`  
`data.orderID` | integer | The cancelled order's ID  
`data.instrumentID` | string | Instrument the order was on  
`data.time` | integer | Server time of cancellation (ms)  
`data.expired` | boolean | `true` if the order was cancelled due to expiry  
  
* * *

## Market Data Details¶

### Broadcast Interval¶

Market data is broadcast to all subscribed clients every **100 ms** (`PLAYER_UPDATE_MS`).

The broadcast is skipped if the round has not started yet (server time < 0) or if the round has ended.

### Orderbook Depth Details¶

  * **Depth:** Top **5 price levels** on each side (bid and ask).
  * **Aggregation:** Quantities at the same price level are summed.
  * **Coverage:** Published for **every tradeable instrument** on every broadcast, even if the book is empty.
  * Prices are serialized as **string keys** in the JSON object (due to JSON object key constraints), but represent integer values in cents.



### Candle Aggregation¶

  * **Period:** 1 candle = 1000 ms of server time (1 "in-game hour").
  * **Delivery:** Only newly **completed** candles are included in each broadcast. The current open candle is never sent.
  * **Fields:** Standard OHLCV. All price fields are `null` if no trades occurred in the period.
  * **Index:** The `index` field is an absolute sequential identifier for the candle period.
  * **Instrument filtering:** Candles are only included for instruments that have new completed candles. An instrument with no new candles is omitted from the `candles.tradeable` map.



### Events¶

Events are **incremental** — each broadcast contains only events that occurred since the previous broadcast. Events are **not** re-sent.

Two event types exist:

Event Type | Trigger  
---|---  
`trade` | Two orders matched and a trade was executed  
`cancel` | An order was manually cancelled, expired, or system-cancelled  
  
Events from **all teams** are broadcast to **all clients**. This means you can observe trades and cancellations from other participants.

* * *

## Trading Rules¶

### Order Matching¶

The exchange uses a **price-time priority** matching engine:

  1. When a new order is placed, it is checked against resting orders on the opposite side.
  2. **Bids** match against **asks** with the lowest price first. **Asks** match against **bids** with the highest price first.
  3. At the same price level, earlier orders (by placement time) are matched first.
  4. Trades execute at the **passive** (resting) order's price.
  5. Partial fills are supported — an order can match multiple resting orders.
  6. Unmatched residual quantity rests on the book.



### Order Expiry¶

  * Orders automatically expire when the server time reaches or exceeds the order's `expiry` timestamp.
  * Expired orders are removed from the book and generate a `cancel` event.
  * Expiry checking occurs on every `add_order`, `cancel_order`, and periodic update cycle.



### Position Limits¶

Limit | Value | Description  
---|---|---  
Max pending orders/team | **6,000** | Maximum simultaneous live orders per team  
Initial cash balance | **10,000,000** | Starting cash per team per exchange (cents = $100,000)  
Max long position | **2,000** | Maximum long position per instrument (units)  
Max short position | **−200** | Floor on net position per instrument (units)  
Max negative cash | **−5,000,000** | Floor on cash balance per exchange (cents = −$50,000)  
  
### Round Lifecycle¶

The server runs **one round per process**. In the competition each round corresponds to a single 10-minute segment — the exchange is restarted between segments and connections do not persist across the boundary.

  1. **Pre-round** (`time < 0`): Server is up but the round has not started. Market data broadcasts are skipped. Connections are accepted.
  2. **Active round** (`0 ≤ time ≤ round_length`): Trading is open. Market data broadcasts every 100 ms.
  3. **Round end** (`time > round_length`):
  4. `end_of_round` message is broadcast.
  5. All remaining live orders are automatically cancelled.
  6. No new orders can be placed.
  7. Final settlement is performed and balances are saved.
  8. The exchange shuts down shortly after — your existing connection will close.



For the next segment, a fresh server starts with reset positions and cash. You must reconnect.

* * *

## HTTP Endpoints¶

These are standard HTTP endpoints (not WebSocket), available on the same port.

### Health Check¶
    
    
    GET /health
    

No authentication required.

**Response:**
    
    
    {
      "status": "healthy",
      "time": 52300,
      "round_length": 600000
    }
    

Field | Type | Description  
---|---|---  
`status` | string | Always `"healthy"`  
`time` | integer | Current server time in ms (relative to start)  
`round_length` | integer | Total round duration in ms  
  
* * *

## Client Examples¶

### Python¶
    
    
    import asyncio
    import json
    import time
    from websockets.asyncio.client import connect as ws_connect
    
    SERVER_URL = "ws://nyse.algotrade.hr:9001/trade"
    
    async def main():
        async with ws_connect(SERVER_URL) as ws:
            # Read welcome message
            welcome = json.loads(await ws.recv())
            print(f"Connected: {welcome['message']}")
    
            # Place an order
            order = {
                "type": "add_order",
                "user_request_id": "order-1",
                "instrument_id": "NYSE-CARD",
                "price": 10000,
                "expiry": int(time.time() * 1000) + 60000,  # 60s from now
                "side": "bid",
                "quantity": 10
            }
            await ws.send(json.dumps(order))
    
            # Listen for messages
            async for message in ws:
                data = json.loads(message)
                msg_type = data.get("type", "")
    
                if msg_type == "add_order_response":
                    if data["success"]:
                        print(f"Order placed: ID={data['data']['order_id']}")
                    else:
                        print(f"Order failed: {data['data'].get('message')}")
    
                elif msg_type == "market_data_update":
                    # Process market data
                    orderbooks = data.get("orderbook_depths", {})
                    for instrument, ob in orderbooks.items():
                        bids = ob.get("bids", {})
                        asks = ob.get("asks", {})
                        if bids and asks:
                            best_bid = max(int(p) for p in bids.keys())
                            best_ask = min(int(p) for p in asks.keys())
                            print(f"{instrument}: bid={best_bid} ask={best_ask}")
    
                elif msg_type == "end_of_round":
                    print("Round ended!")
                    break
    
    asyncio.run(main())
    

### JavaScript¶
    
    
    const WebSocket = require("ws");
    
    const ws = new WebSocket("ws://nyse.algotrade.hr:9001/trade");
    
    ws.on("open", () => {
      console.log("Connected");
    
      // Place an order
      ws.send(
        JSON.stringify({
          type: "add_order",
          user_request_id: "order-1",
          instrument_id: "NYSE-CARD",
          price: 10000,
          expiry: Date.now() + 60000,
          side: "bid",
          quantity: 10,
        })
      );
    });
    
    ws.on("message", (raw) => {
      const data = JSON.parse(raw);
    
      switch (data.type) {
        case "welcome":
          console.log(`Welcome: ${data.message}`);
          break;
    
        case "add_order_response":
          if (data.success) {
            console.log(`Order placed: ${data.data.order_id}`);
          } else {
            console.log(`Order failed: ${data.data.message}`);
          }
          break;
    
        case "market_data_update":
          // Process orderbook, candles, events
          const depths = data.orderbook_depths;
          for (const [instrument, ob] of Object.entries(depths)) {
            const bidPrices = Object.keys(ob.bids || {}).map(Number);
            const askPrices = Object.keys(ob.asks || {}).map(Number);
            if (bidPrices.length && askPrices.length) {
              console.log(
                `${instrument}: ${Math.max(...bidPrices)} / ${Math.min(...askPrices)}`
              );
            }
          }
          break;
    
        case "cancel_order_response":
          console.log(`Cancel: success=${data.success}`);
          break;
    
        case "end_of_round":
          console.log("Round ended");
          ws.close();
          break;
      }
    });
    

* * *

## Error Reference¶

Error Message | Trigger  
---|---  
`"Failed to parse message: Invalid JSON or missing 'type' field"` | Malformed JSON or missing `type` field  
`"Unknown command type: <type>"` | Unrecognized `type` value  
`"Round has ended, no new orders can be placed"` | `add_order` after round end  
`"Round has ended, all orders have been automatically cancelled"` | `cancel_order` after round end  
`"Price must be positive"` | `price <= 0` in add_order  
`"Quantity must be positive"` | `quantity <= 0` in add_order  
`"Side must be 'bid' or 'ask'"` | Invalid `side` value  
`"Expiry must be in the future"` | `expiry <= current_server_time`  
`"Instrument not found"` | Invalid `instrument_id`  
`"Order ID must be positive"` | `order_id <= 0` in cancel_order  
`"Order ID not found"` | Order doesn't exist in the book  
`"Order does not belong to the team"` | Attempting to cancel another team's order  
`"Order not found"` | Order is not live (already cancelled or filled)  
`"Message rate limit exceeded"` | Exceeded 500 messages/second — **connection is closed**  
  
* * *

_Generated from source code analysis. Last updated: 2026-05-08._
