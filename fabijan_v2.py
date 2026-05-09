"""
AlgoTrade 2026 — cross-venue ETF arbitrage bot (v2).

Connects to 5 exchanges and arbitrages 4 same-ETF cross-listings whose
inter-exchange RTT stays under ~25 ms (so the dislocation window is wider
than our order one-way latency in the worst rotation segment):

    ETFA   Euronext ↔ ZSE      (22 ms inter-exchange RTT)
    ETFB   LSE      ↔ ZSE      (24 ms)
    ETFA3  NYSE     ↔ TMX      (11 ms)
    ETFSH  Euronext ↔ ZSE      (22 ms)

When venue A's ask + threshold ≤ venue B's bid we IOC-buy at A and IOC-sell
at B in parallel; symmetric the other way. No basket leg — the trade is
delta-neutral by construction.

Each connection has its own rate limiter, position tracking, in-flight
tracker, and reconnect-with-backoff loop. v2 deliberately does not run the
ZSE basket arb — pick v1 or v2, not both, since they share the ZSE rate
budget and the ZSE position floors.

All numerics are integer cents. Local rate limit stays at 80% of the
500 msg/s/exchange budget.
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

# (ETF, ExchangeA, ExchangeB) — order doesn't matter; we check both directions.
ARB_PAIRS: list[tuple[str, str, str]] = [
    ("ETFA",  "Euronext", "ZSE"),
    ("ETFB",  "LSE",      "ZSE"),
    ("ETFA3", "NYSE",     "TMX"),
    ("ETFSH", "Euronext", "ZSE"),
]

# Server-enforced limits.
SERVER_RATE_LIMIT = 500
SERVER_POS_FLOOR = -200
SERVER_POS_CEIL = 2000
SERVER_CASH_FLOOR = -5_000_000

# Local strategy params.
LOCAL_RATE_LIMIT = 400           # 80% of server limit, applied per exchange
ARB_SIZE = 20                    # shares per leg per fire (fits MM 50-deep top)
ARB_THRESHOLD = 30               # cents of per-share edge before crossing
POS_CAP = 60                     # max |position| per (ETF, exchange).
                                 # Each ETF appears on 2 venues here, so worst
                                 # signed exposure on a venue = POS_CAP, well
                                 # inside the −200 / +2000 server floor/ceiling.
EXPIRY_MS = 5_000
INVENTORY_RESYNC_INTERVAL_S = 1.0
RECONNECT_BACKOFF_S = 2.0
FLATTEN_REMAINING_MS = 30_000


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
# State
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
        # rid -> (instrument, side, qty)
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

    # -- handlers ----------------------------------------------------------

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

    # -- main loop ---------------------------------------------------------

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
                    self.state = State()  # fresh per segment
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
# Bot — cross-venue arb orchestrator
# ---------------------------------------------------------------------------

class Bot:
    def __init__(self) -> None:
        self.conns: dict[str, ExchangeConn] = {
            name: ExchangeConn(name, host) for name, host in EXCHANGE_HOSTS.items()
        }
        # Index pairs by exchange so we only check relevant ones on each tick.
        self.pairs_by_exchange: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        for etf, a, b in ARB_PAIRS:
            self.pairs_by_exchange[a].append((etf, a, b))
            self.pairs_by_exchange[b].append((etf, a, b))

    async def _try_arb(self, etf: str, ex_buy: str, ex_sell: str) -> bool:
        """If buying at ex_buy and selling at ex_sell shows >= threshold edge,
        fire both legs IOC. Returns True if fired."""
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
        if bid - ask < ARB_THRESHOLD:
            return False
        # Position-cap check: long on buy side, short on sell side.
        if cb.effective_pos(instr_b) + ARB_SIZE > POS_CAP:
            return False
        if cs.effective_pos(instr_s) - ARB_SIZE < -POS_CAP:
            return False
        log.info("ARB %s: BUY %s@%d / SELL %s@%d (edge=%d)",
                 etf, ex_buy, ask, ex_sell, bid, bid - ask)
        await cb.ioc(instr_b, "bid", ask, ARB_SIZE)
        await cs.ioc(instr_s, "ask", bid, ARB_SIZE)
        return True

    async def _flatten(self, conn: ExchangeConn) -> None:
        """Market-flatten any non-zero positions on this exchange."""
        for instr in list(conn.state.positions.keys()):
            qty = conn.effective_pos(instr)
            if qty == 0:
                continue
            side = "ask" if qty > 0 else "bid"
            await conn.market(instr, side, abs(qty))

    async def on_tick(self, conn: ExchangeConn) -> None:
        if conn.segment_remaining_ms() <= FLATTEN_REMAINING_MS:
            await self._flatten(conn)
            return
        # Check every pair this exchange participates in, both directions.
        for etf, a, b in self.pairs_by_exchange[conn.name]:
            if await self._try_arb(etf, ex_buy=a, ex_sell=b):
                continue
            await self._try_arb(etf, ex_buy=b, ex_sell=a)

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
    log.info("starting cross-venue ETF arb bot on: %s",
             ", ".join(EXCHANGE_HOSTS.keys()))
    log.info("pairs: %s", ARB_PAIRS)
    asyncio.run(Bot().run())


if __name__ == "__main__":
    main()
