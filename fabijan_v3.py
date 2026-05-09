"""
AlgoTrade 2026 — combined ZSE basket arb + cross-venue ETF arb (v3).

Runs both strategies on the same set of WebSocket connections, sharing
position/cash state per exchange so they don't fight each other:

  Strategy A — ZSE basket arb (was v1).
    For each of the 5 ETFs on ZSE: when (Σ constituent_bids)/n − etf_ask
    or etf_bid − (Σ constituent_asks)/n exceeds ARB_THRESHOLD cents, IOC
    the ETF leg and every constituent leg simultaneously.

  Strategy B — cross-venue ETF arb (was v2).
    For each of 4 same-ETF cross-listings whose inter-exchange RTT < 25 ms,
    IOC-buy at the cheaper venue and IOC-sell at the richer one when the
    edge clears ARB_THRESHOLD_CV cents.

Both strategies log at INFO when they fire so silence-vs-broken is
distinguishable. Conservative caps; last-30 s of each venue's segment we
stop opening and flatten residual positions at market.

All numerics are integer cents. Each connection has its own rate limiter,
in-flight tracker, and reconnect-with-backoff loop.
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
    "TMX":      "tmx.algotrade.hr",
    "LSE":      "lse.algotrade.hr",
    "Euronext": "euronext.algotrade.hr",
    "ZSE":      "zse.algotrade.hr",
}
PORT = 9001

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}

# Cross-venue arb pairs (etf, exchangeA, exchangeB), inter-exchange RTT < 25ms.
CV_PAIRS: list[tuple[str, str, str]] = [
    ("ETFA",  "Euronext", "ZSE"),
    ("ETFB",  "LSE",      "ZSE"),
    ("ETFA3", "NYSE",     "TMX"),
    ("ETFSH", "Euronext", "ZSE"),
]

# Server limits (for reference).
SERVER_RATE_LIMIT = 500
SERVER_POS_FLOOR = -200
SERVER_POS_CEIL = 2000
SERVER_CASH_FLOOR = -5_000_000

# Local strategy params.
LOCAL_RATE_LIMIT = 400           # per exchange
ARB_SIZE = 20                    # shares per leg per fire (fits MM 50-deep top)
ARB_THRESHOLD = 25               # basket-arb edge in cents/share
ARB_THRESHOLD_CV = 30             # cross-venue edge in cents/share
ETF_POS_CAP = 80                 # basket-arb cap per ETF on ZSE
CV_POS_CAP = 60                  # cross-venue cap per (ETF, exchange)
EXPIRY_MS = 5_000
INVENTORY_RESYNC_INTERVAL_S = 1.0
RECONNECT_BACKOFF_S = 2.0
FLATTEN_REMAINING_MS = 30_000
HEARTBEAT_TICKS = 100            # ~10 s at 100 ms broadcasts


# ---------------------------------------------------------------------------
# Rate limiter (token bucket) — one per exchange
# ---------------------------------------------------------------------------

class RateLimiter:
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
# Books and state
# ---------------------------------------------------------------------------

@dataclass
class Book:
    bids: dict[int, int] = field(default_factory=dict)
    asks: dict[int, int] = field(default_factory=dict)

    @property
    def best_bid(self) -> Optional[int]:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return min(self.asks) if self.asks else None


@dataclass
class State:
    books: dict[str, Book] = field(default_factory=lambda: defaultdict(Book))
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    cash: int = 10_000_000
    server_time_ms: int = 0
    round_length_ms: int = 600_000
    tick_count: int = 0


# ---------------------------------------------------------------------------
# Per-exchange connection
# ---------------------------------------------------------------------------

class ExchangeConn:
    def __init__(self, name: str, host: str):
        self.name = name
        self.url = f"ws://{host}:{PORT}/trade"
        self.state = State()
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.limiter = RateLimiter(LOCAL_RATE_LIMIT)
        self.req_id = 0
        self.pending: dict[str, tuple[str, str, int]] = {}

    def _rid(self, prefix: str) -> str:
        self.req_id += 1
        return f"{prefix}-{self.req_id}"

    def instr(self, ticker: str) -> str:
        return f"{self.name}-{ticker}"

    async def _send(self, msg: dict) -> None:
        await self.limiter.acquire()
        if self.ws is None:
            return
        await self.ws.send(json.dumps(msg))

    async def ioc(self, instr: str, side: str, price: int, qty: int) -> None:
        rid = self._rid("ioc")
        self.pending[rid] = (instr, side, qty)
        await self._send({
            "type": "add_order",
            "user_request_id": rid,
            "instrument_id": instr,
            "price": price,
            "expiry": int(time.time() * 1000) + EXPIRY_MS,
            "side": side,
            "quantity": qty,
            "order_type": "ioc",
        })

    async def market(self, instr: str, side: str, qty: int) -> None:
        rid = self._rid("mkt")
        self.pending[rid] = (instr, side, qty)
        await self._send({
            "type": "add_order",
            "user_request_id": rid,
            "instrument_id": instr,
            "side": side,
            "quantity": qty,
            "order_type": "market",
        })

    async def get_inventory(self) -> None:
        await self._send({"type": "get_inventory", "user_request_id": self._rid("inv")})

    # -- handlers ---------------------------------------------------------

    def _apply_book(self, instr: str, ob: dict) -> None:
        b = self.state.books[instr]
        b.bids = {int(p): q for p, q in (ob.get("bids") or {}).items()}
        b.asks = {int(p): q for p, q in (ob.get("asks") or {}).items()}

    def _apply_inventory(self, data: dict) -> None:
        for k, v in data.items():
            if not isinstance(v, list) or len(v) != 2:
                continue
            _reserved, total = v
            if k == "$":
                self.state.cash = int(total)
            else:
                self.state.positions[k] = int(total)

    def _apply_add_resp(self, msg: dict) -> None:
        rid = msg.get("user_request_id", "")
        intent = self.pending.pop(rid, None)
        if intent is None:
            return
        instr, side, _qty = intent
        if not msg.get("success"):
            data = msg.get("data") or {}
            log.debug("[%s] add_order failed (%s %s): %s",
                      self.name, side, instr, data.get("message"))
            return
        data = msg.get("data") or {}
        inv_change = data.get("immediate_inventory_change")
        bal_change = data.get("immediate_balance_change")
        if inv_change is not None:
            self.state.positions[instr] += int(inv_change)
            log.info("[%s] FILL %s %s: %+d shares (pos=%d)",
                     self.name, side, instr, int(inv_change),
                     self.state.positions[instr])
        if bal_change is not None:
            self.state.cash += int(bal_change)

    def in_flight(self, instr: str) -> int:
        delta = 0
        for i, side, q in self.pending.values():
            if i == instr:
                delta += q if side == "bid" else -q
        return delta

    def effective_pos(self, instr: str) -> int:
        return self.state.positions.get(instr, 0) + self.in_flight(instr)

    def segment_remaining_ms(self) -> int:
        return max(0, self.state.round_length_ms - self.state.server_time_ms)

    # -- main loop --------------------------------------------------------

    async def _read_loop(self, on_tick) -> None:
        assert self.ws is not None
        async for raw in self.ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                if isinstance(raw, str) and "rate limit" in raw.lower():
                    log.error("[%s] RATE LIMIT BREACH — connection closed", self.name)
                continue
            t = msg.get("type")
            if t == "market_data_update":
                self.state.server_time_ms = int(msg.get("time", 0))
                self.state.tick_count += 1
                for instr, ob in (msg.get("orderbook_depths") or {}).items():
                    self._apply_book(instr, ob)
                await on_tick(self)
            elif t == "add_order_response":
                self._apply_add_resp(msg)
            elif t == "get_inventory_response":
                self._apply_inventory(msg.get("data") or {})
            elif t == "end_of_round":
                log.info("[%s] end_of_round — segment over", self.name)
                return
            elif t == "welcome":
                log.info("[%s] welcome: %s", self.name, msg.get("message"))
            elif t == "error":
                log.warning("[%s] server error: %s", self.name, msg.get("message"))

    async def _resync_loop(self) -> None:
        while self.ws is not None:
            await asyncio.sleep(INVENTORY_RESYNC_INTERVAL_S)
            try:
                await self.get_inventory()
            except Exception:  # noqa: BLE001
                return

    async def run(self, on_tick) -> None:
        while True:
            try:
                async with ws_connect(self.url, max_size=16 * 1024 * 1024) as ws:
                    self.ws = ws
                    self.state = State()
                    self.pending.clear()
                    resync = asyncio.create_task(self._resync_loop())
                    try:
                        await self.get_inventory()
                        await self._read_loop(on_tick)
                    finally:
                        resync.cancel()
            except (websockets.ConnectionClosed, OSError) as e:
                log.info("[%s] disconnected (%s); reconnecting in %.1fs",
                         self.name, type(e).__name__, RECONNECT_BACKOFF_S)
            except Exception as e:  # noqa: BLE001
                log.exception("[%s] unexpected error: %s", self.name, e)
            finally:
                self.ws = None
            await asyncio.sleep(RECONNECT_BACKOFF_S)


# ---------------------------------------------------------------------------
# Bot — runs both strategies
# ---------------------------------------------------------------------------

class Bot:
    def __init__(self) -> None:
        self.conns: dict[str, ExchangeConn] = {
            name: ExchangeConn(name, host) for name, host in EXCHANGE_HOSTS.items()
        }
        self.cv_pairs_by_exchange: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        for etf, a, b in CV_PAIRS:
            self.cv_pairs_by_exchange[a].append((etf, a, b))
            self.cv_pairs_by_exchange[b].append((etf, a, b))

    # -- ZSE basket arb (Strategy A) -------------------------------------

    def _basket_bid_total(self, basket: list[str]) -> Optional[int]:
        zse = self.conns["ZSE"]
        total = 0
        for t in basket:
            book = zse.state.books.get(zse.instr(t))
            if book is None or book.best_bid is None:
                return None
            total += book.best_bid
        return total

    def _basket_ask_total(self, basket: list[str]) -> Optional[int]:
        zse = self.conns["ZSE"]
        total = 0
        for t in basket:
            book = zse.state.books.get(zse.instr(t))
            if book is None or book.best_ask is None:
                return None
            total += book.best_ask
        return total

    async def _open_basket_arb(self, etf: str, basket: list[str], buy_etf: bool,
                               edge_per_share: int) -> None:
        zse = self.conns["ZSE"]
        etf_instr = zse.instr(etf)
        etf_book = zse.state.books[etf_instr]
        if buy_etf:
            etf_price = etf_book.best_ask
            etf_side, leg_side = "bid", "ask"
        else:
            etf_price = etf_book.best_bid
            etf_side, leg_side = "ask", "bid"
        if etf_price is None:
            return
        log.info("BASKET %s: %s ETF @%d, %d legs @touch, edge=%dc/share",
                 etf, "BUY" if buy_etf else "SELL", etf_price, len(basket), edge_per_share)
        await zse.ioc(etf_instr, etf_side, etf_price, ARB_SIZE)
        for t in basket:
            book = zse.state.books[zse.instr(t)]
            px = book.best_bid if leg_side == "ask" else book.best_ask
            if px is None:
                continue
            await zse.ioc(zse.instr(t), leg_side, px, ARB_SIZE)

    async def _basket_arb_zse(self, zse: ExchangeConn) -> None:
        for etf, basket in ETF_BASKETS.items():
            etf_instr = zse.instr(etf)
            etf_book = zse.state.books.get(etf_instr)
            if etf_book is None:
                continue
            ebb, eba = etf_book.best_bid, etf_book.best_ask
            if ebb is None or eba is None:
                continue

            n = len(basket)
            pos = zse.effective_pos(etf_instr)

            sell_basket_total = self._basket_bid_total(basket)
            if sell_basket_total is not None \
               and sell_basket_total - eba * n >= ARB_THRESHOLD * n \
               and pos + ARB_SIZE <= ETF_POS_CAP:
                edge = (sell_basket_total - eba * n) // n
                await self._open_basket_arb(etf, basket, buy_etf=True, edge_per_share=edge)
                continue

            buy_basket_total = self._basket_ask_total(basket)
            if buy_basket_total is not None \
               and ebb * n - buy_basket_total >= ARB_THRESHOLD * n \
               and pos - ARB_SIZE >= -ETF_POS_CAP:
                edge = (ebb * n - buy_basket_total) // n
                await self._open_basket_arb(etf, basket, buy_etf=False, edge_per_share=edge)

    # -- Cross-venue arb (Strategy B) ------------------------------------

    async def _try_cv_arb(self, etf: str, ex_buy: str, ex_sell: str) -> bool:
        cb = self.conns[ex_buy]
        cs = self.conns[ex_sell]
        instr_b = cb.instr(etf)
        instr_s = cs.instr(etf)
        book_b = cb.state.books.get(instr_b)
        book_s = cs.state.books.get(instr_s)
        if book_b is None or book_s is None:
            return False
        ask = book_b.best_ask
        bid = book_s.best_bid
        if ask is None or bid is None:
            return False
        if bid - ask < ARB_THRESHOLD_CV:
            return False
        if cb.effective_pos(instr_b) + ARB_SIZE > CV_POS_CAP:
            return False
        if cs.effective_pos(instr_s) - ARB_SIZE < -CV_POS_CAP:
            return False
        log.info("CV-ARB %s: BUY %s @%d / SELL %s @%d, edge=%dc/share",
                 etf, ex_buy, ask, ex_sell, bid, bid - ask)
        await cb.ioc(instr_b, "bid", ask, ARB_SIZE)
        await cs.ioc(instr_s, "ask", bid, ARB_SIZE)
        return True

    async def _cross_venue_arb(self, conn: ExchangeConn) -> None:
        for etf, a, b in self.cv_pairs_by_exchange[conn.name]:
            if await self._try_cv_arb(etf, ex_buy=a, ex_sell=b):
                continue
            await self._try_cv_arb(etf, ex_buy=b, ex_sell=a)

    # -- Flatten + heartbeat ---------------------------------------------

    async def _flatten(self, conn: ExchangeConn) -> None:
        for instr in list(conn.state.positions.keys()):
            qty = conn.effective_pos(instr)
            if qty == 0:
                continue
            side = "ask" if qty > 0 else "bid"
            await conn.market(instr, side, abs(qty))

    def _log_heartbeat(self, conn: ExchangeConn) -> None:
        """Periodic snapshot showing each ETF's best edge — helps see whether the
        threshold is the binding constraint vs a bug."""
        if conn.name != "ZSE":
            return  # one heartbeat is enough; ZSE has the most signal
        edges = []
        for etf, basket in ETF_BASKETS.items():
            etf_instr = conn.instr(etf)
            book = conn.state.books.get(etf_instr)
            if book is None or book.best_bid is None or book.best_ask is None:
                continue
            n = len(basket)
            sb = self._basket_bid_total(basket)
            ba = self._basket_ask_total(basket)
            if sb is None or ba is None:
                continue
            buy_edge = (sb - book.best_ask * n) // n
            sell_edge = (book.best_bid * n - ba) // n
            edges.append(f"{etf}={max(buy_edge, sell_edge)}c")
        log.info("[heartbeat ZSE] tick=%d remaining=%ds best basket-edges (need %dc): %s",
                 conn.state.tick_count, conn.segment_remaining_ms() // 1000,
                 ARB_THRESHOLD, " ".join(edges))

    # -- tick dispatch ---------------------------------------------------

    async def on_tick(self, conn: ExchangeConn) -> None:
        if conn.state.tick_count % HEARTBEAT_TICKS == 0:
            self._log_heartbeat(conn)

        if conn.segment_remaining_ms() <= FLATTEN_REMAINING_MS:
            await self._flatten(conn)
            return

        # Cross-venue arb runs on every tick from any venue.
        await self._cross_venue_arb(conn)
        # Basket arb runs only when ZSE ticks (it only reads ZSE books anyway).
        if conn.name == "ZSE":
            await self._basket_arb_zse(conn)

    async def run(self) -> None:
        await asyncio.gather(*(c.run(self.on_tick) for c in self.conns.values()))


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log.info("starting fabijan_v3 — basket arb on ZSE + cross-venue arb on %s",
             ", ".join(EXCHANGE_HOSTS.keys()))
    log.info("CV pairs: %s", CV_PAIRS)
    asyncio.run(Bot().run())


if __name__ == "__main__":
    main()
