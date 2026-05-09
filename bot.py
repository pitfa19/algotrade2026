"""
AlgoTrade 2026 trading bot.

Strategy:
  1. Passive market-making one tick inside the built-in MM, inventory-skewed,
     across every instrument on every exchange that lists it.
  2. ETF / basket arbitrage on ZSE (the only venue that lists every ETF and
     every constituent), fired as IOC orders when the ETF mid deviates from
     the equal-weighted basket fair beyond a threshold.

All numerics are integer cents. Local rate limiter stays at 80% of the
500 msg/s/exchange budget. Reconnect loop survives the per-segment exchange
restart (every 10 minutes).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import websockets
try:
    from websockets.asyncio.client import connect as ws_connect  # websockets >= 12
except ImportError:  # pragma: no cover
    from websockets.client import connect as ws_connect          # websockets <= 11


log = logging.getLogger("bot")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

EXCHANGE_HOSTS = {
    "NYSE":     "nyse.algotrade.hr",
    "NASDAQ":   "nasdaq.algotrade.hr",
    "SSE":      "sse.algotrade.hr",
    "JPX":      "jpx.algotrade.hr",
    "Euronext": "euronext.algotrade.hr",
    "LSE":      "lse.algotrade.hr",
    "HKEX":     "hkex.algotrade.hr",
    "NSE":      "nse.algotrade.hr",
    "TMX":      "tmx.algotrade.hr",
    "ZSE":      "zse.algotrade.hr",
}
PORT = 9001

# Stock listings: ticker -> set of exchanges (from participant guide §4).
STOCK_LISTINGS: dict[str, set[str]] = {
    "CARD": {"NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"},
    "SIMP": {"NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"},
    "NGUP": {"NYSE","NASDAQ","Euronext","TMX","ZSE"},
    "OIT":  {"LSE","Euronext","HKEX","NSE","ZSE"},
    "KTST": {"NYSE","JPX","TMX","ZSE"},
    "FSR":  {"NASDAQ","LSE","SSE","HKEX","ZSE"},
    "JZRO": {"NYSE","LSE","Euronext","TMX","ZSE"},
    "XFR":  {"NYSE","HKEX","TMX","ZSE"},
    "KOTD": {"NASDAQ","LSE","Euronext","HKEX","ZSE"},
    "INA":  {"NYSE","NASDAQ","Euronext","HKEX","ZSE"},
    "HT":   {"NASDAQ","LSE","JPX","SSE","TMX","ZSE"},
    "JNAF": {"NYSE","Euronext","JPX","HKEX","ZSE"},
    "DLKV": {"NASDAQ","LSE","HKEX","NSE","ZSE"},
    "DDJH": {"NYSE","LSE","Euronext","TMX","ZSE"},
    "MDKA": {"NYSE","LSE","HKEX","TMX","ZSE"},
    "KRAS": {"NYSE","Euronext","SSE","TMX","ZSE"},
    "ZITO": {"NASDAQ","LSE","Euronext","NSE","ZSE"},
    "ZABA": {"NYSE","LSE","SSE","NSE","TMX","ZSE"},
    "GOLD": {"NASDAQ","Euronext","JPX","TMX","ZSE"},
    "XAG":  {"LSE","Euronext","JPX","ZSE"},
}

# ETF listings: ticker -> set of exchanges (from participant guide §5).
ETF_LISTINGS: dict[str, set[str]] = {
    "ETFA":  {"NYSE","Euronext","HKEX","ZSE"},
    "ETFB":  {"NASDAQ","LSE","HKEX","ZSE"},
    "ETFA3": {"NYSE","TMX","ZSE"},
    "ETFB3": {"NASDAQ","HKEX","ZSE"},
    "ETFSH": {"Euronext","JPX","ZSE"},
}

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}

# Per-exchange list of (ticker, is_etf) we trade.
def instruments_on(exchange: str) -> list[tuple[str, bool]]:
    out: list[tuple[str, bool]] = []
    for t, exs in STOCK_LISTINGS.items():
        if exchange in exs:
            out.append((t, False))
    for t, exs in ETF_LISTINGS.items():
        if exchange in exs:
            out.append((t, True))
    return out

# --- Limits (server enforced) ---
SERVER_RATE_LIMIT = 500           # msg/s/exchange — exceed = connection closed
LOCAL_RATE_LIMIT = 400            # 80% safety margin
SERVER_POS_FLOOR = -200
SERVER_POS_CEIL = 2000
SERVER_CASH_FLOOR = -5_000_000    # cents

# --- Strategy params ---
MAX_POS_PER_INST = 50             # local cap, well inside server floor/ceiling
QUOTE_SIZE = 5                    # shares per quote side
MIN_HALF_SPREAD = 5               # cents — never quote tighter than this
DEFAULT_HALF_SPREAD = 15          # cents — used when book is thin
INVENTORY_SKEW_CENTS_PER_SHARE = 1   # mid shifts this many cents per share of position
QUOTE_EXPIRY_MS = 5_000           # rest 5s before auto-cancel
REQUOTE_THRESHOLD_CENTS = 2       # only cancel/replace when desired quote moves > this
ETF_ARB_THRESHOLD = 30            # cents of edge before crossing
ETF_ARB_SIZE = 5                  # shares per IOC fire
ETF_MAX_POS = 30                  # tighter cap on ETFs (directional risk)
INVENTORY_RESYNC_INTERVAL_S = 5.0
RECONNECT_BACKOFF_S = 2.0


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Async token bucket. Refills at `rate` tokens/sec, capacity = rate."""

    def __init__(self, rate: int):
        self.rate = rate
        self.tokens = float(rate)
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(wait)


# ---------------------------------------------------------------------------
# Per-exchange state
# ---------------------------------------------------------------------------

@dataclass
class Book:
    bids: dict[int, int] = field(default_factory=dict)  # price -> qty
    asks: dict[int, int] = field(default_factory=dict)

    @property
    def best_bid(self) -> Optional[int]:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> Optional[int]:
        b, a = self.best_bid, self.best_ask
        return (a + b) // 2 if b is not None and a is not None else None


@dataclass
class MyQuote:
    """Tracks one resting order I placed."""
    order_id: int
    side: str       # "bid" or "ask"
    price: int
    quantity: int   # original quantity


@dataclass
class ExchangeState:
    """Everything I know about one exchange."""
    name: str
    books: dict[str, Book] = field(default_factory=lambda: defaultdict(Book))
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    cash: int = 10_000_000  # initial: $100k in cents
    # Per-instrument: my live orders by side -> MyQuote (one quote per side max)
    my_quotes: dict[tuple[str, str], MyQuote] = field(default_factory=dict)
    # order_id -> (instrument, side, qty) so events can update positions
    my_orders: dict[int, tuple[str, str, int]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Exchange connection
# ---------------------------------------------------------------------------

class ExchangeClient:
    """One WS connection. Reconnect loop runs in `run()`."""

    def __init__(self, name: str):
        self.name = name
        self.url = f"ws://{EXCHANGE_HOSTS[name]}:{PORT}/trade"
        self.state = ExchangeState(name=name)
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.limiter = RateLimiter(LOCAL_RATE_LIMIT)
        self.req_id = 0
        # Map user_request_id -> pending order intent so we can stitch the
        # add_order_response back to (instrument, side, qty).
        self.pending: dict[str, tuple[str, str, int, int]] = {}  # rid -> (instr,side,price,qty)
        self.last_resync = 0.0

    def next_rid(self, prefix: str) -> str:
        self.req_id += 1
        return f"{prefix}-{self.req_id}"

    async def send(self, msg: dict) -> None:
        await self.limiter.acquire()
        if self.ws is None:
            return
        await self.ws.send(json.dumps(msg))

    async def place_limit(self, instrument: str, side: str, price: int, qty: int,
                          ioc: bool = False) -> None:
        rid = self.next_rid("ioc" if ioc else "ord")
        self.pending[rid] = (instrument, side, price, qty)
        await self.send({
            "type": "add_order",
            "user_request_id": rid,
            "instrument_id": instrument,
            "price": price,
            "expiry": int(time.time() * 1000) + QUOTE_EXPIRY_MS,
            "side": side,
            "quantity": qty,
            "order_type": "ioc" if ioc else "limit",
        })

    async def cancel(self, instrument: str, order_id: int) -> None:
        await self.send({
            "type": "cancel_order",
            "user_request_id": self.next_rid("can"),
            "order_id": order_id,
            "instrument_id": instrument,
        })

    async def get_inventory(self) -> None:
        await self.send({
            "type": "get_inventory",
            "user_request_id": self.next_rid("inv"),
        })

    # -- message handlers ---------------------------------------------------

    def _apply_book(self, instrument: str, ob: dict) -> None:
        b = self.state.books[instrument]
        b.bids = {int(p): q for p, q in (ob.get("bids") or {}).items()}
        b.asks = {int(p): q for p, q in (ob.get("asks") or {}).items()}

    def _apply_event(self, ev: dict) -> None:
        if ev.get("event_type") != "trade":
            # cancel events: drop our side tracking if it was ours
            data = ev.get("data") or {}
            oid = data.get("orderID")
            if oid in self.state.my_orders:
                instr, side, _qty = self.state.my_orders.pop(oid)
                self.state.my_quotes.pop((instr, side), None)
            return
        data = ev["data"]
        oid_p = data["passiveOrderID"]
        oid_a = data["activeOrderID"]
        instr = data["instrumentID"]
        price = int(data["price"])
        qty = int(data["quantity"])
        # If the passive order is mine, my side is the resting side.
        # If active is mine, my side is the resting side's opposite.
        for oid, is_passive in ((oid_p, True), (oid_a, False)):
            if oid not in self.state.my_orders:
                continue
            my_instr, my_side, my_qty = self.state.my_orders[oid]
            if my_instr != instr:
                continue
            sign = +1 if my_side == "bid" else -1
            self.state.positions[instr] += sign * qty
            self.state.cash -= sign * price * qty
            new_qty = my_qty - qty
            if new_qty <= 0:
                self.state.my_orders.pop(oid, None)
                # remove matching quote if present
                q = self.state.my_quotes.get((instr, my_side))
                if q and q.order_id == oid:
                    self.state.my_quotes.pop((instr, my_side), None)
            else:
                self.state.my_orders[oid] = (my_instr, my_side, new_qty)

    def _apply_inventory(self, data: dict) -> None:
        for k, v in data.items():
            if not isinstance(v, list) or len(v) != 2:
                continue
            _reserved, total = v
            if k == "$":
                self.state.cash = int(total)
            else:
                self.state.positions[k] = int(total)

    def _apply_add_response(self, msg: dict) -> None:
        rid = msg.get("user_request_id", "")
        intent = self.pending.pop(rid, None)
        if intent is None:
            return
        instr, side, price, qty = intent
        if not msg.get("success"):
            log.debug("[%s] add_order failed (%s %s %d@%d): %s",
                      self.name, side, instr, qty, price,
                      (msg.get("data") or {}).get("message"))
            return
        data = msg.get("data") or {}
        oid = data.get("order_id")
        inv_change = data.get("immediate_inventory_change")
        bal_change = data.get("immediate_balance_change")
        # Update local cash/position from immediate fills.
        if inv_change is not None:
            self.state.positions[instr] += int(inv_change)
        if bal_change is not None:
            self.state.cash += int(bal_change)
        immediate_filled = abs(int(inv_change)) if inv_change is not None else 0
        remainder = qty - immediate_filled
        if oid is not None and remainder > 0 and rid.startswith("ord"):
            # Limit order with resting remainder.
            self.state.my_orders[oid] = (instr, side, remainder)
            self.state.my_quotes[(instr, side)] = MyQuote(oid, side, price, remainder)

    # -- main loop ----------------------------------------------------------

    async def _read_loop(self, bot: "Bot") -> None:
        assert self.ws is not None
        async for raw in self.ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                if isinstance(raw, str) and "rate limit" in raw.lower():
                    log.error("[%s] RATE LIMIT BREACH — server closed connection", self.name)
                continue
            t = msg.get("type")
            if t == "market_data_update":
                for instr, ob in (msg.get("orderbook_depths") or {}).items():
                    self._apply_book(instr, ob)
                for ev in msg.get("events") or []:
                    self._apply_event(ev)
                # Drive strategy on every market update for this exchange.
                await bot.on_tick(self)
            elif t == "add_order_response":
                self._apply_add_response(msg)
            elif t == "cancel_order_response":
                pass  # success/fail tracked indirectly via cancel events
            elif t == "get_inventory_response":
                self._apply_inventory(msg.get("data") or {})
            elif t == "end_of_round":
                log.info("[%s] end_of_round received — segment over", self.name)
                return
            elif t == "welcome":
                log.info("[%s] welcome: %s", self.name, msg.get("message"))
            elif t == "error":
                log.warning("[%s] error: %s", self.name, msg.get("message"))

    async def _resync_loop(self) -> None:
        while self.ws is not None:
            await asyncio.sleep(INVENTORY_RESYNC_INTERVAL_S)
            try:
                await self.get_inventory()
            except Exception:  # noqa: BLE001
                return

    async def run(self, bot: "Bot") -> None:
        while True:
            try:
                async with ws_connect(self.url, max_size=16 * 1024 * 1024) as ws:
                    self.ws = ws
                    # Reset per-segment state — exchange just (re)started.
                    self.state = ExchangeState(name=self.name)
                    self.pending.clear()
                    resync = asyncio.create_task(self._resync_loop())
                    try:
                        await self.get_inventory()
                        await self._read_loop(bot)
                    finally:
                        resync.cancel()
            except (websockets.ConnectionClosed, OSError) as e:
                log.info("[%s] disconnected (%s); reconnecting in %.1fs",
                         self.name, type(e).__name__, RECONNECT_BACKOFF_S)
            except Exception as e:  # noqa: BLE001
                log.exception("[%s] unexpected error: %s — reconnecting", self.name, e)
            finally:
                self.ws = None
            await asyncio.sleep(RECONNECT_BACKOFF_S)


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

class Bot:
    def __init__(self, exchanges: list[str]):
        self.clients: dict[str, ExchangeClient] = {x: ExchangeClient(x) for x in exchanges}

    def _instr(self, exchange: str, ticker: str) -> str:
        return f"{exchange}-{ticker}"

    async def on_tick(self, client: ExchangeClient) -> None:
        await self._market_make(client)
        if client.name == "ZSE":
            await self._etf_arb_zse(client)

    # -- market making ------------------------------------------------------

    async def _market_make(self, client: ExchangeClient) -> None:
        st = client.state
        for ticker, _is_etf in instruments_on(client.name):
            instr = self._instr(client.name, ticker)
            book = st.books.get(instr)
            if book is None:
                continue
            bb, ba = book.best_bid, book.best_ask
            if bb is None or ba is None or ba <= bb:
                continue

            mid = (bb + ba) // 2
            mm_half = (ba - bb) // 2
            half = max(MIN_HALF_SPREAD, min(mm_half - 1, DEFAULT_HALF_SPREAD))
            if half < MIN_HALF_SPREAD:
                continue  # spread too tight to step inside profitably

            pos = st.positions.get(instr, 0)
            skew = pos * INVENTORY_SKEW_CENTS_PER_SHARE
            fair = mid - skew

            cap = ETF_MAX_POS if ticker in ETF_LISTINGS else MAX_POS_PER_INST
            want_bid = (pos + QUOTE_SIZE) <= cap
            want_ask = (pos - QUOTE_SIZE) >= -cap

            if want_bid:
                target_bid = max(bb + 1, fair - half)
                target_bid = min(target_bid, ba - 1)
                await self._maybe_quote(client, instr, "bid", target_bid)
            else:
                await self._cancel_side(client, instr, "bid")

            if want_ask:
                target_ask = min(ba - 1, fair + half)
                target_ask = max(target_ask, bb + 1)
                await self._maybe_quote(client, instr, "ask", target_ask)
            else:
                await self._cancel_side(client, instr, "ask")

    async def _maybe_quote(self, client: ExchangeClient, instr: str,
                           side: str, target_price: int) -> None:
        if target_price <= 0:
            return
        existing = client.state.my_quotes.get((instr, side))
        if existing is not None and abs(existing.price - target_price) <= REQUOTE_THRESHOLD_CENTS:
            return
        if existing is not None:
            await client.cancel(instr, existing.order_id)
            client.state.my_quotes.pop((instr, side), None)
            client.state.my_orders.pop(existing.order_id, None)
        await client.place_limit(instr, side, target_price, QUOTE_SIZE, ioc=False)

    async def _cancel_side(self, client: ExchangeClient, instr: str, side: str) -> None:
        existing = client.state.my_quotes.get((instr, side))
        if existing is None:
            return
        await client.cancel(instr, existing.order_id)
        client.state.my_quotes.pop((instr, side), None)
        client.state.my_orders.pop(existing.order_id, None)

    # -- ETF basket arb on ZSE ----------------------------------------------

    async def _etf_arb_zse(self, client: ExchangeClient) -> None:
        st = client.state
        for etf, basket in ETF_BASKETS.items():
            etf_instr = self._instr("ZSE", etf)
            etf_book = st.books.get(etf_instr)
            if etf_book is None:
                continue
            ebb, eba = etf_book.best_bid, etf_book.best_ask
            if ebb is None or eba is None:
                continue

            # Basket fair = mean of constituent mids on ZSE.
            mids: list[int] = []
            for t in basket:
                bk = st.books.get(self._instr("ZSE", t))
                if bk is None or bk.mid is None:
                    break
                mids.append(bk.mid)
            if len(mids) != len(basket):
                continue
            fair = sum(mids) // len(mids)

            pos = st.positions.get(etf_instr, 0)
            # ETF too cheap to buy: ask < fair - threshold and we have room to go long.
            if eba <= fair - ETF_ARB_THRESHOLD and pos + ETF_ARB_SIZE <= ETF_MAX_POS:
                await client.place_limit(etf_instr, "bid", eba, ETF_ARB_SIZE, ioc=True)
            # ETF too rich to sell: bid > fair + threshold and we have room to go short.
            elif ebb >= fair + ETF_ARB_THRESHOLD and pos - ETF_ARB_SIZE >= -ETF_MAX_POS:
                await client.place_limit(etf_instr, "ask", ebb, ETF_ARB_SIZE, ioc=True)

    # -- entry --------------------------------------------------------------

    async def run(self) -> None:
        await asyncio.gather(*(c.run(self) for c in self.clients.values()))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raw = os.environ.get("EXCHANGES", ",".join(EXCHANGE_HOSTS.keys()))
    exchanges = [x.strip() for x in raw.split(",") if x.strip()]
    unknown = [x for x in exchanges if x not in EXCHANGE_HOSTS]
    if unknown:
        raise SystemExit(f"Unknown exchange(s): {unknown}")
    log.info("starting bot on exchanges: %s", exchanges)
    asyncio.run(Bot(exchanges).run())


if __name__ == "__main__":
    main()
