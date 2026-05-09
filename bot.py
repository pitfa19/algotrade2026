"""
AlgoTrade 2026 — ZSE ETF basket arbitrage bot (v2).

Strategy: a single WebSocket connection to ZSE (the only venue listing every
stock and every ETF). For each of the 5 ETFs, compare ETF touch against the
basket's *executable* total — the sum of constituent bids when we'd be selling
the basket, asks when we'd be buying. When the per-share edge clears
ARB_THRESHOLD cents, IOC-fire the ETF leg and every constituent leg at the
touch in parallel. Conservative caps (ETF_POS_CAP, ARB_SIZE). In the last
30 s of a segment we stop opening and flatten residual inventory at market.

All numerics are integer cents. Local rate limiter stays at 80% of the
500 msg/s budget. Reconnect loop survives the per-segment exchange restart.
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

ZSE_HOST = os.environ.get("ZSE_HOST", "zse.algotrade.hr")
PORT = 9001
URL = f"ws://{ZSE_HOST}:{PORT}/trade"

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}

# Server-enforced limits (for reference; we stay well inside).
SERVER_RATE_LIMIT = 500          # msg/s — exceed = connection closed
SERVER_POS_FLOOR = -200          # per instrument
SERVER_POS_CEIL = 2000           # per instrument
SERVER_CASH_FLOOR = -5_000_000   # cents

# Local strategy params (conservative).
LOCAL_RATE_LIMIT = 400           # 80% of server limit
ARB_SIZE = 5                     # shares per IOC fire (per leg)
ARB_THRESHOLD = 30               # cents of edge before crossing
ETF_POS_CAP = 20                 # max |ETF position| we'll open
EXPIRY_MS = 5_000                # IOC expiry window (effectively immediate)
INVENTORY_RESYNC_INTERVAL_S = 1.0
RECONNECT_BACKOFF_S = 2.0
FLATTEN_REMAINING_MS = 30_000    # in last 30s: stop opening, flatten existing


# ---------------------------------------------------------------------------
# Rate limiter (token bucket)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Refills at `rate` tokens/sec, capacity = rate."""

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

    @property
    def mid(self) -> Optional[int]:
        b, a = self.best_bid, self.best_ask
        return (a + b) // 2 if b is not None and a is not None else None


@dataclass
class State:
    books: dict[str, Book] = field(default_factory=lambda: defaultdict(Book))
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    cash: int = 10_000_000
    server_time_ms: int = 0
    round_length_ms: int = 600_000  # 10 min default; refreshed from /health


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class Bot:
    def __init__(self) -> None:
        self.state = State()
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.limiter = RateLimiter(LOCAL_RATE_LIMIT)
        self.req_id = 0
        # rid -> (instrument, side, qty) so we can apply immediate fills.
        self.pending: dict[str, tuple[str, str, int]] = {}

    def _rid(self, prefix: str) -> str:
        self.req_id += 1
        return f"{prefix}-{self.req_id}"

    @staticmethod
    def _instr(ticker: str) -> str:
        return f"ZSE-{ticker}"

    async def _send(self, msg: dict) -> None:
        await self.limiter.acquire()
        if self.ws is None:
            return
        await self.ws.send(json.dumps(msg))

    async def _ioc(self, instr: str, side: str, price: int, qty: int) -> None:
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

    async def _market(self, instr: str, side: str, qty: int) -> None:
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

    async def _get_inventory(self) -> None:
        await self._send({"type": "get_inventory", "user_request_id": self._rid("inv")})

    # -- message handling --------------------------------------------------

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
            log.debug("add_order failed (%s %s): %s", side, instr, data.get("message"))
            return
        data = msg.get("data") or {}
        inv_change = data.get("immediate_inventory_change")
        bal_change = data.get("immediate_balance_change")
        if inv_change is not None:
            self.state.positions[instr] += int(inv_change)
        if bal_change is not None:
            self.state.cash += int(bal_change)

    # -- strategy ----------------------------------------------------------

    def _segment_remaining_ms(self) -> int:
        return max(0, self.state.round_length_ms - self.state.server_time_ms)

    def _in_flight(self, instr: str) -> int:
        """Net signed position delta from orders we sent but haven't seen ack'd."""
        delta = 0
        for i, side, q in self.pending.values():
            if i == instr:
                delta += q if side == "bid" else -q
        return delta

    def _effective_pos(self, instr: str) -> int:
        return self.state.positions.get(instr, 0) + self._in_flight(instr)

    def _basket_bid_total(self, basket: list[str]) -> Optional[int]:
        """Sum of constituent best_bids — revenue if we sold the whole basket."""
        total = 0
        for t in basket:
            book = self.state.books.get(self._instr(t))
            if book is None or book.best_bid is None:
                return None
            total += book.best_bid
        return total

    def _basket_ask_total(self, basket: list[str]) -> Optional[int]:
        """Sum of constituent best_asks — cost if we bought the whole basket."""
        total = 0
        for t in basket:
            book = self.state.books.get(self._instr(t))
            if book is None or book.best_ask is None:
                return None
            total += book.best_ask
        return total

    async def _open_arb(self, etf: str, basket: list[str], buy_etf: bool) -> None:
        """
        buy_etf=True  → IOC-buy ETF + IOC-sell each constituent at touch.
        buy_etf=False → IOC-sell ETF + IOC-buy  each constituent at touch.
        Constituent legs hit the touch on the opposite side of their book.
        """
        etf_instr = self._instr(etf)
        etf_book = self.state.books[etf_instr]
        if buy_etf:
            etf_price = etf_book.best_ask
            etf_side = "bid"
            leg_side = "ask"
        else:
            etf_price = etf_book.best_bid
            etf_side = "ask"
            leg_side = "bid"
        if etf_price is None:
            return
        # Send all legs in parallel — token bucket smooths them.
        await self._ioc(etf_instr, etf_side, etf_price, ARB_SIZE)
        for t in basket:
            book = self.state.books[self._instr(t)]
            px = book.best_bid if leg_side == "ask" else book.best_ask
            if px is None:
                continue
            await self._ioc(self._instr(t), leg_side, px, ARB_SIZE)

    async def _flatten(self) -> None:
        """Market-flatten any non-zero ETF or constituent positions.

        Uses effective position (live + in-flight) so we don't double-fire while
        a prior flatten order is still on the wire.
        """
        for instr in list(self.state.positions.keys()):
            qty = self._effective_pos(instr)
            if qty == 0:
                continue
            side = "ask" if qty > 0 else "bid"
            await self._market(instr, side, abs(qty))

    async def _on_tick(self) -> None:
        remaining = self._segment_remaining_ms()
        if remaining <= FLATTEN_REMAINING_MS:
            await self._flatten()
            return

        for etf, basket in ETF_BASKETS.items():
            etf_instr = self._instr(etf)
            etf_book = self.state.books.get(etf_instr)
            if etf_book is None:
                continue
            ebb, eba = etf_book.best_bid, etf_book.best_ask
            if ebb is None or eba is None:
                continue

            n = len(basket)
            pos = self._effective_pos(etf_instr)

            # Buy 1 ETF + sell 1 of each constituent. Net cents per ETF share:
            #   edge = (Σ constituent_bids) / n  -  eba
            # Use multiplied-by-n form to avoid integer division rounding.
            sell_basket_total = self._basket_bid_total(basket)
            if sell_basket_total is not None \
               and sell_basket_total - eba * n >= ARB_THRESHOLD * n \
               and pos + ARB_SIZE <= ETF_POS_CAP:
                await self._open_arb(etf, basket, buy_etf=True)
                continue

            # Sell 1 ETF + buy 1 of each constituent. Edge:
            #   edge = ebb  -  (Σ constituent_asks) / n
            buy_basket_total = self._basket_ask_total(basket)
            if buy_basket_total is not None \
               and ebb * n - buy_basket_total >= ARB_THRESHOLD * n \
               and pos - ARB_SIZE >= -ETF_POS_CAP:
                await self._open_arb(etf, basket, buy_etf=False)

    # -- main loop ---------------------------------------------------------

    async def _read_loop(self) -> None:
        assert self.ws is not None
        async for raw in self.ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                if isinstance(raw, str) and "rate limit" in raw.lower():
                    log.error("RATE LIMIT BREACH — server closed connection")
                continue
            t = msg.get("type")
            if t == "market_data_update":
                self.state.server_time_ms = int(msg.get("time", 0))
                for instr, ob in (msg.get("orderbook_depths") or {}).items():
                    self._apply_book(instr, ob)
                await self._on_tick()
            elif t == "add_order_response":
                self._apply_add_resp(msg)
            elif t == "get_inventory_response":
                self._apply_inventory(msg.get("data") or {})
            elif t == "end_of_round":
                log.info("end_of_round received — segment over")
                return
            elif t == "welcome":
                log.info("welcome: %s", msg.get("message"))
            elif t == "error":
                log.warning("server error: %s", msg.get("message"))

    async def _resync_loop(self) -> None:
        while self.ws is not None:
            await asyncio.sleep(INVENTORY_RESYNC_INTERVAL_S)
            try:
                await self._get_inventory()
            except Exception:  # noqa: BLE001
                return

    async def run(self) -> None:
        while True:
            try:
                async with ws_connect(URL, max_size=16 * 1024 * 1024) as ws:
                    self.ws = ws
                    self.state = State()       # fresh per segment
                    self.pending.clear()
                    resync = asyncio.create_task(self._resync_loop())
                    try:
                        await self._get_inventory()
                        await self._read_loop()
                    finally:
                        resync.cancel()
            except (websockets.ConnectionClosed, OSError) as e:
                log.info("disconnected (%s); reconnecting in %.1fs",
                         type(e).__name__, RECONNECT_BACKOFF_S)
            except Exception as e:  # noqa: BLE001
                log.exception("unexpected error: %s — reconnecting", e)
            finally:
                self.ws = None
            await asyncio.sleep(RECONNECT_BACKOFF_S)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log.info("starting ZSE basket-arb bot (URL=%s)", URL)
    asyncio.run(Bot().run())


if __name__ == "__main__":
    main()
