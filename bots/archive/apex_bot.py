#!/usr/bin/env python3
"""
APEX — AlgoTrade 2026 trading bot.

Single-file async bot. The only deterministic edge in this competition is the
ETF rule: an ETF's fair value is the simple equal-weighted average of its
constituents. Everything else (MM models, noise traders, sector correlations)
is a heuristic. APEX is built around the deterministic edge first and uses
heuristics only as opportunistic add-ons.

Strategies (in priority order):
  1. Same-exchange ETF basket arbitrage (model-free, single-leg, IOC).
  2. ZSE-anchored cross-venue ETF arbitrage (when local constituents missing).
  3. Inventory-skew passive limit orders around synthetic fair value.
  4. Cross-venue stock arbitrage for tickers listed on multiple exchanges.
  5. End-of-segment delta neutralization (close convergence trades, avoid mark variance).

Architectural novelties:
  - Self-locating latency probe via /health RTT (no external config needed).
  - Per-exchange token-bucket rate limiter at 76% of the 500 msg/s cap.
  - Asymmetric inventory-aware thresholds (auto mean-reversion of own book).
  - Self-fill detection via own-order-ID set.
  - Periodic inventory reconciliation against authoritative get_inventory.
  - Jittered staggered reconnect at segment boundaries (under the open-rate limit).

Run:
  pip install -r requirements.txt
  python apex_bot.py                     # all 10 exchanges
  APEX_EXCHANGES=zse,nyse python apex_bot.py  # subset
  APEX_DRY_RUN=1 python apex_bot.py      # don't actually place/cancel orders
  APEX_LOG=DEBUG python apex_bot.py      # verbose
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import time
from collections import defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    import aiohttp
except ImportError:
    aiohttp = None  # only needed for /health probes

import websockets
from websockets.asyncio.client import connect as ws_connect

# ════════════════════════════════════════════════════════════════════════════
# Constants — competition layout
# ════════════════════════════════════════════════════════════════════════════

PORT = 9001

EXCHANGES = ("NYSE", "NASDAQ", "SSE", "JPX", "EURONEXT", "LSE", "HKEX", "NSE", "TMX", "ZSE")

# Stock listings — from participant guide §4.
STOCK_LISTINGS: dict[str, frozenset[str]] = {
    "CARD": frozenset(EXCHANGES),
    "SIMP": frozenset(EXCHANGES),
    "NGUP": frozenset(("NYSE", "NASDAQ", "EURONEXT", "TMX", "ZSE")),
    "OIT":  frozenset(("LSE", "EURONEXT", "HKEX", "NSE", "ZSE")),
    "KTST": frozenset(("NYSE", "JPX", "TMX", "ZSE")),
    "FSR":  frozenset(("NASDAQ", "LSE", "SSE", "HKEX", "ZSE")),
    "JZRO": frozenset(("NYSE", "LSE", "EURONEXT", "TMX", "ZSE")),
    "XFR":  frozenset(("NYSE", "HKEX", "TMX", "ZSE")),
    "KOTD": frozenset(("NASDAQ", "LSE", "EURONEXT", "HKEX", "ZSE")),
    "INA":  frozenset(("NYSE", "NASDAQ", "EURONEXT", "HKEX", "ZSE")),
    "HT":   frozenset(("NASDAQ", "LSE", "JPX", "SSE", "TMX", "ZSE")),
    "JNAF": frozenset(("NYSE", "EURONEXT", "JPX", "HKEX", "ZSE")),
    "DLKV": frozenset(("NASDAQ", "LSE", "HKEX", "NSE", "ZSE")),
    "DDJH": frozenset(("NYSE", "LSE", "EURONEXT", "TMX", "ZSE")),
    "MDKA": frozenset(("NYSE", "LSE", "HKEX", "TMX", "ZSE")),
    "KRAS": frozenset(("NYSE", "EURONEXT", "SSE", "TMX", "ZSE")),
    "ZITO": frozenset(("NASDAQ", "LSE", "EURONEXT", "NSE", "ZSE")),
    "ZABA": frozenset(("NYSE", "LSE", "SSE", "NSE", "TMX", "ZSE")),
    "GOLD": frozenset(("NASDAQ", "EURONEXT", "JPX", "TMX", "ZSE")),
    "XAG":  frozenset(("LSE", "EURONEXT", "JPX", "ZSE")),
}

ETF_BASKETS: dict[str, tuple[str, ...]] = {
    "ETFA":  ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"),
    "ETFB":  ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"),
    "ETFA3": ("NGUP", "KTST", "XFR"),
    "ETFB3": ("KOTD", "INA", "DLKV"),
    "ETFSH": ("GOLD", "XAG"),
}

ETF_LISTINGS: dict[str, frozenset[str]] = {
    "ETFA":  frozenset(("NYSE", "EURONEXT", "HKEX", "ZSE")),
    "ETFB":  frozenset(("NASDAQ", "LSE", "HKEX", "ZSE")),
    "ETFA3": frozenset(("NYSE", "TMX", "ZSE")),
    "ETFB3": frozenset(("NASDAQ", "HKEX", "ZSE")),
    "ETFSH": frozenset(("EURONEXT", "JPX", "ZSE")),
}

# All instruments tradeable on each exchange.
EXCHANGE_INSTRUMENTS: dict[str, frozenset[str]] = {}
for _ex in EXCHANGES:
    _syms: set[str] = set()
    for _t, _vens in STOCK_LISTINGS.items():
        if _ex in _vens:
            _syms.add(_t)
    for _etf, _vens in ETF_LISTINGS.items():
        if _ex in _vens:
            _syms.add(_etf)
    EXCHANGE_INSTRUMENTS[_ex] = frozenset(_syms)

# Latency matrix (round-trip ms) — diagonal is 0; needed only as a fallback when
# we cannot probe /health. Order matches EXCHANGES.
_LATENCY_RTT_MS: dict[tuple[str, str], int] = {}
_LATENCY_TABLE = [
    #          NYSE NASDAQ SSE  JPX  EURO LSE  HKEX NSE  TMX  ZSE
    ("NYSE",     [0,   1, 165, 152,  84,  80, 180, 174,  11,  96]),
    ("NASDAQ",   [1,   0, 165, 152,  84,  80, 180, 174,  11,  96]),
    ("SSE",      [165, 165, 0,  18, 160, 156,  19,  54, 159, 145]),
    ("JPX",      [152, 152, 18,  0, 145, 141,  37,  53, 145, 140]),
    ("EURONEXT", [84,  84, 160, 145,  0,   6, 130, 130,  86,  22]),
    ("LSE",      [80,  80, 156, 141,  6,   0, 135, 134,  82,  24]),
    ("HKEX",     [180, 180, 19,  37, 130, 135,  0,  53, 174, 150]),
    ("NSE",      [174, 174, 54,  53, 130, 134, 53,   0, 174,  95]),
    ("TMX",      [11,  11, 159, 145,  86,  82, 174, 174,  0,  98]),
    ("ZSE",      [96,  96, 145, 140,  22,  24, 150,  95,  98,   0]),
]
for _src, _row in _LATENCY_TABLE:
    for _dst, _ms in zip(EXCHANGES, _row):
        _LATENCY_RTT_MS[(_src, _dst)] = _ms

# ════════════════════════════════════════════════════════════════════════════
# Limits & knobs
# ════════════════════════════════════════════════════════════════════════════

STARTING_CASH = 10_000_000      # 10M cents = $100k
CASH_FLOOR = -5_000_000         # -$50k
POS_CEIL = 2_000
POS_FLOOR = -200
MAX_PENDING_ORDERS = 6_000
MAX_MSGS_PER_SEC = 500
SAFE_MSGS_PER_SEC = 380         # 76% of cap — robust to bursts
MAX_CONNECTIONS = 10
BROADCAST_MS = 100

# Strategy thresholds
ARB_MIN_EDGE_CENTS = 4
ARB_CROSS_VENUE_BASE_EDGE = 12
SKEW_MIN_CENTS = 4
SKEW_MAX_QUEUE_AGE_MS = 1500     # don't act on stale books
ORDER_TTL_MS_DEFAULT = 4_000
ORDER_TTL_MS_MM = 6_000
ORDER_TTL_MS_UNWIND = 3_000

# Per-instrument exposure caps. Far below the ±200/+2000 walls — we want to
# keep enough room to cycle the same instrument many times in a segment, and
# to absorb a stuck position without breaching limits.
MAX_POS_LONG = 80
MAX_POS_SHORT = -60     # short side has tighter wall (−200) so be more conservative
ARB_CLIP_QTY = 8
SKEW_CLIP_QTY = 4
UNWIND_CLIP_QTY = 25

# Convergence-trade holding clock: if an arb position hasn't unwound itself
# via natural reversion within this window, force a market exit.
CONVERGENCE_HOLD_MS = 9_000
ARB_FORCE_EXIT_DEPTH = 3        # how many price levels to sweep when forced

# End-of-segment behavior
EOS_UNWIND_LEAD_MS = 30_000
EOS_HARD_STOP_MS = 4_000
EOS_INVENTORY_RECON_MS = 5_000

# Latency probing
LATENCY_PROBE_INTERVAL_S = 30.0
LATENCY_PROBE_TIMEOUT_S = 1.5
COLOCATED_RTT_MAX_MS = 5        # < 5ms means we're at that exchange

# Reconnect policy
INITIAL_BACKOFF_S = 0.4
MAX_BACKOFF_S = 8.0
SEGMENT_BOUNDARY_STAGGER_S = 0.15  # spread reconnects to dodge open-rate limit
INVENTORY_RECONCILE_INTERVAL_S = 5.0

# ════════════════════════════════════════════════════════════════════════════
# Logging
# ════════════════════════════════════════════════════════════════════════════

LOG_LEVEL = os.environ.get("APEX_LOG", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("apex")

DRY_RUN = os.environ.get("APEX_DRY_RUN", "0") not in ("0", "", "false", "False")

# ════════════════════════════════════════════════════════════════════════════
# Utilities
# ════════════════════════════════════════════════════════════════════════════

def now_unix_ms() -> int:
    return int(time.time() * 1000)


def ws_url(exchange: str) -> str:
    return f"ws://{exchange.lower()}.algotrade.hr:{PORT}/trade"


def health_url(exchange: str) -> str:
    return f"http://{exchange.lower()}.algotrade.hr:{PORT}/health"


def instrument_id(exchange: str, ticker: str) -> str:
    return f"{exchange}-{ticker}"


# ════════════════════════════════════════════════════════════════════════════
# TokenBucket — async rate limiter
# ════════════════════════════════════════════════════════════════════════════

class TokenBucket:
    """Async token bucket. Smooth-fills tokens; never bursts above `rate`.

    The exchange enforces 500 msgs/s in a 1000ms sliding window and *closes*
    the connection on overrun. We size capacity equal to rate (no burst above
    steady-state) and keep the safe rate at ~76% to leave margin for clock
    skew and any tiny scheduler jitter.
    """

    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None) -> None:
        self.rate = float(rate_per_sec)
        self.capacity = float(capacity if capacity is not None else rate_per_sec)
        self.tokens = self.capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def take(self, n: float = 1.0) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self._last) * self.rate)
                self._last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                deficit = n - self.tokens
                await asyncio.sleep(max(0.001, deficit / self.rate))


# ════════════════════════════════════════════════════════════════════════════
# Order book snapshot
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Book:
    bids: dict[int, int] = field(default_factory=dict)
    asks: dict[int, int] = field(default_factory=dict)
    last_update_ms: int = 0

    def update_from_depth(self, depth: dict, server_t_ms: int) -> None:
        new_bids: dict[int, int] = {}
        for p, q in (depth.get("bids") or {}).items():
            try:
                pi, qi = int(p), int(q)
            except (TypeError, ValueError):
                continue
            if qi > 0:
                new_bids[pi] = qi
        new_asks: dict[int, int] = {}
        for p, q in (depth.get("asks") or {}).items():
            try:
                pi, qi = int(p), int(q)
            except (TypeError, ValueError):
                continue
            if qi > 0:
                new_asks[pi] = qi
        self.bids = new_bids
        self.asks = new_asks
        self.last_update_ms = server_t_ms

    @property
    def best_bid(self) -> Optional[int]:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return min(self.asks) if self.asks else None

    @property
    def best_bid_qty(self) -> int:
        bb = self.best_bid
        return self.bids.get(bb, 0) if bb is not None else 0

    @property
    def best_ask_qty(self) -> int:
        ba = self.best_ask
        return self.asks.get(ba, 0) if ba is not None else 0

    @property
    def mid(self) -> Optional[int]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return (bb + ba) // 2

    @property
    def spread(self) -> Optional[int]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return ba - bb

    def is_fresh(self, server_t_ms: int, max_age_ms: int = SKEW_MAX_QUEUE_AGE_MS) -> bool:
        return (server_t_ms - self.last_update_ms) <= max_age_ms

    def is_two_sided(self) -> bool:
        return bool(self.bids) and bool(self.asks)


# ════════════════════════════════════════════════════════════════════════════
# Live order tracking
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class LiveOrder:
    order_id: int
    ticker: str
    side: str               # "bid" | "ask"
    price: int
    quantity: int           # remaining unfilled
    placed_mono: float      # monotonic seconds when placed
    expiry_unix_ms: int
    purpose: str            # "arb" | "mm" | "unwind" | "cross"
    user_request_id: str

    @property
    def signed_qty(self) -> int:
        return self.quantity if self.side == "bid" else -self.quantity


@dataclass
class ArbLot:
    """An open convergence trade — used to time-out and force-close."""
    ticker: str
    side: str               # "bid" or "ask" — direction of original entry
    qty: int
    entry_price: int
    placed_mono: float


# ════════════════════════════════════════════════════════════════════════════
# Per-exchange state
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class ExchangeState:
    exchange: str
    books: dict[str, Book] = field(default_factory=dict)

    cash_total: int = STARTING_CASH
    cash_reserved: int = 0
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    positions_reserved: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    live_orders: dict[int, LiveOrder] = field(default_factory=dict)
    pending_by_urid: dict[str, dict[str, Any]] = field(default_factory=dict)
    own_order_ids: set[int] = field(default_factory=set)

    server_time_ms: int = 0
    round_length_ms: Optional[int] = None
    last_md_ms: int = 0
    welcome_received: bool = False
    end_of_round: bool = False

    open_lots: dict[str, deque[ArbLot]] = field(
        default_factory=lambda: defaultdict(deque)
    )

    last_inventory_sync_mono: float = 0.0
    last_health_rtt_ms: Optional[float] = None

    def reset_for_new_segment(self) -> None:
        self.cash_total = STARTING_CASH
        self.cash_reserved = 0
        self.positions = defaultdict(int)
        self.positions_reserved = defaultdict(int)
        self.live_orders.clear()
        self.pending_by_urid.clear()
        self.own_order_ids.clear()
        self.server_time_ms = 0
        self.round_length_ms = None
        self.last_md_ms = 0
        self.end_of_round = False
        self.open_lots = defaultdict(deque)
        self.last_inventory_sync_mono = 0.0

    def book(self, ticker: str) -> Book:
        b = self.books.get(ticker)
        if b is None:
            b = Book()
            self.books[ticker] = b
        return b

    def time_remaining_ms(self) -> Optional[int]:
        if self.round_length_ms is None or self.server_time_ms <= 0:
            return None
        return max(0, self.round_length_ms - self.server_time_ms)

    def position(self, ticker: str) -> int:
        return self.positions.get(ticker, 0)


# ════════════════════════════════════════════════════════════════════════════
# Exchange WebSocket connection
# ════════════════════════════════════════════════════════════════════════════

class ExchangeConnection:
    """One WebSocket connection per exchange. Owns the send queue, the rate
    limiter, the recv dispatch, and a reconnect loop that survives the
    `end_of_round` boundary."""

    def __init__(self, exchange: str, state: ExchangeState, on_md: Any) -> None:
        self.exchange = exchange
        self.state = state
        self.on_md = on_md
        self.ws: Optional[Any] = None
        self.bucket = TokenBucket(SAFE_MSGS_PER_SEC, capacity=SAFE_MSGS_PER_SEC)
        self.outbox: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
        self._urid_counter = 0
        self._closing = False
        self._stop = asyncio.Event()
        self._send_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None
        self.log = logging.getLogger(f"apex.{exchange.lower()}")

    # — public surface —

    def make_urid(self, tag: str = "") -> str:
        self._urid_counter += 1
        return f"{self.exchange[:3]}-{tag}-{self._urid_counter}" if tag else f"{self.exchange[:3]}-{self._urid_counter}"

    async def stop(self) -> None:
        self._closing = True
        self._stop.set()
        if self.ws is not None:
            with suppress(Exception):
                await self.ws.close()

    async def run(self) -> None:
        backoff = INITIAL_BACKOFF_S
        while not self._closing:
            try:
                await self._one_session()
                backoff = INITIAL_BACKOFF_S
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.warning("session error: %s", e)
                backoff = min(MAX_BACKOFF_S, backoff * 2)
            finally:
                self.ws = None

            if self._closing:
                break

            # Distinguish segment boundary (reset state) from mid-segment drop
            # (server preserves positions/cash; we'll re-sync via get_inventory).
            if self.state.end_of_round:
                self.state.reset_for_new_segment()
                # Stagger across exchanges so 10 simultaneous reconnects don't
                # blow the open-rate-per-second limit.
                jitter = random.uniform(0.0, SEGMENT_BOUNDARY_STAGGER_S * 5)
                await asyncio.sleep(backoff + jitter)
            else:
                # Mid-segment: short backoff, keep local state for sync.
                await asyncio.sleep(min(backoff, 1.0))

    async def _one_session(self) -> None:
        self.log.info("connecting %s", ws_url(self.exchange))
        # Drop any stale queued messages from a previous (now-dead) session.
        while not self.outbox.empty():
            with suppress(asyncio.QueueEmpty):
                self.outbox.get_nowait()
        async with ws_connect(
            ws_url(self.exchange),
            ping_interval=20,
            ping_timeout=20,
            max_size=16 * 1024 * 1024,
            close_timeout=2,
        ) as ws:
            self.ws = ws
            self.state.welcome_received = False
            self._send_task = asyncio.create_task(self._send_loop())
            self._recv_task = asyncio.create_task(self._recv_loop())
            try:
                # Either task completing ends the session — cancel the survivor.
                done, pending = await asyncio.wait(
                    [self._send_task, self._recv_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                    with suppress(Exception, asyncio.CancelledError):
                        await t
                # Surface session-ending exceptions from completed tasks.
                for t in done:
                    exc = t.exception()
                    if exc is not None and not isinstance(exc, asyncio.CancelledError):
                        raise exc
            except asyncio.CancelledError:
                for t in (self._send_task, self._recv_task):
                    if t and not t.done():
                        t.cancel()
                raise

    async def _send_loop(self) -> None:
        try:
            while True:
                msg = await self.outbox.get()
                await self.bucket.take(1)
                if self.ws is None:
                    return
                try:
                    await self.ws.send(msg)
                except Exception as e:
                    self.log.warning("send failed: %s", e)
                    return
        except asyncio.CancelledError:
            raise

    async def _recv_loop(self) -> None:
        if self.ws is None:
            return
        try:
            async for raw in self.ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    self.log.warning("non-json frame: %r", raw[:200] if isinstance(raw, str) else raw)
                    continue
                t = msg.get("type")
                if t == "market_data_update":
                    await self._handle_md(msg)
                elif t == "welcome":
                    self.state.welcome_received = True
                    self.log.info("welcome: %s", msg.get("message"))
                elif t == "add_order_response":
                    self._handle_add_resp(msg)
                elif t == "cancel_order_response":
                    self._handle_cancel_resp(msg)
                elif t == "get_inventory_response":
                    self._handle_inventory_resp(msg)
                elif t == "get_pending_orders_response":
                    self._handle_pending_resp(msg)
                elif t == "end_of_round":
                    self.log.info("end_of_round")
                    self.state.end_of_round = True
                    return
                elif t == "error":
                    self.log.warning("error: %s (urid=%s)", msg.get("message"), msg.get("user_request_id"))
                else:
                    self.log.debug("other: %s", msg)
        except websockets.ConnectionClosed as e:
            self.log.info("ws closed: %s", e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.warning("recv error: %s", e)

    # — message senders —

    async def _enqueue(self, payload: dict) -> bool:
        if DRY_RUN:
            self.log.debug("DRY %s", payload)
            return True
        try:
            self.outbox.put_nowait(json.dumps(payload, separators=(",", ":")))
            return True
        except asyncio.QueueFull:
            self.log.warning("outbox full, dropping %s", payload.get("type"))
            return False

    async def add_order(
        self,
        ticker: str,
        side: str,
        price: Optional[int],
        quantity: int,
        order_type: str = "limit",
        ttl_ms: int = ORDER_TTL_MS_DEFAULT,
        purpose: str = "arb",
    ) -> Optional[str]:
        if quantity <= 0:
            return None
        urid = self.make_urid(purpose)
        payload: dict[str, Any] = {
            "type": "add_order",
            "user_request_id": urid,
            "instrument_id": instrument_id(self.exchange, ticker),
            "side": side,
            "quantity": int(quantity),
            "order_type": order_type,
        }
        if order_type in ("limit", "ioc"):
            if price is None:
                return None
            payload["price"] = int(price)
            payload["expiry"] = now_unix_ms() + max(ttl_ms, 250)
        # Stash the request body so the response can build a LiveOrder.
        self.state.pending_by_urid[urid] = {**payload, "purpose": purpose, "placed_mono": time.monotonic()}
        ok = await self._enqueue(payload)
        return urid if ok else None

    async def cancel_order(self, order_id: int, ticker: str) -> bool:
        urid = self.make_urid("cancel")
        payload = {
            "type": "cancel_order",
            "user_request_id": urid,
            "order_id": int(order_id),
            "instrument_id": instrument_id(self.exchange, ticker),
        }
        return await self._enqueue(payload)

    async def get_inventory(self) -> bool:
        urid = self.make_urid("inv")
        return await self._enqueue({"type": "get_inventory", "user_request_id": urid})

    async def get_pending(self) -> bool:
        urid = self.make_urid("pend")
        return await self._enqueue({"type": "get_pending_orders", "user_request_id": urid})

    # — message handlers —

    async def _handle_md(self, msg: dict) -> None:
        st = self.state
        st.last_md_ms = time.monotonic_ns() // 1_000_000
        st.server_time_ms = int(msg.get("time", 0))
        depths = msg.get("orderbook_depths") or {}
        for full_id, depth in depths.items():
            ticker = full_id.split("-", 1)[1] if "-" in full_id else full_id
            st.book(ticker).update_from_depth(depth, st.server_time_ms)
        # Apply trade events to local position when our orders were involved.
        for ev in (msg.get("events") or []):
            etype = ev.get("event_type")
            data = ev.get("data") or {}
            if etype == "trade":
                self._apply_trade_event(data)
            elif etype == "cancel":
                oid = int(data.get("orderID", 0))
                if oid in st.live_orders:
                    self._remove_live_order(oid)
        # Hand off to engine for strategy evaluation.
        await self.on_md(self.exchange)

    def _apply_trade_event(self, data: dict) -> None:
        st = self.state
        passive_id = int(data.get("passiveOrderID", 0))
        qty = int(data.get("quantity", 0))
        price = int(data.get("price", 0))
        full = data.get("instrumentID", "")
        ticker = full.split("-", 1)[1] if "-" in full else full
        if qty <= 0:
            return
        # Only count fills where we were the PASSIVE side. Active-side fills
        # are already counted by add_order_response.immediate_inventory_change;
        # double-applying would corrupt our local position.
        if passive_id not in st.own_order_ids:
            return
        order = st.live_orders.get(passive_id)
        if order is None:
            return
        signed = qty if order.side == "bid" else -qty
        st.positions[ticker] += signed
        st.cash_total -= signed * price
        order.quantity -= qty
        if order.quantity <= 0:
            self._remove_live_order(passive_id)
        # Track open lot for forced-unwind clock.
        st.open_lots[ticker].append(
            ArbLot(ticker=ticker, side=order.side, qty=qty, entry_price=price, placed_mono=time.monotonic())
        )

    def _remove_live_order(self, oid: int) -> None:
        order = self.state.live_orders.pop(oid, None)
        if order is None:
            return
        self.state.own_order_ids.discard(oid)

    def _handle_add_resp(self, msg: dict) -> None:
        st = self.state
        urid = msg.get("user_request_id", "")
        success = bool(msg.get("success"))
        data = msg.get("data") or {}
        req = st.pending_by_urid.pop(urid, None)
        if not success:
            self.log.debug("add failed urid=%s msg=%s", urid, data.get("message"))
            return
        if req is None:
            return
        oid = data.get("order_id")
        ticker = req["instrument_id"].split("-", 1)[1]
        purpose = req.get("purpose", "?")
        # Apply any immediate fills to inventory.
        inv_change = data.get("immediate_inventory_change")
        bal_change = data.get("immediate_balance_change")
        if inv_change is not None:
            st.positions[ticker] += int(inv_change)
        if bal_change is not None:
            st.cash_total += int(bal_change)
            if int(inv_change or 0) != 0:
                st.open_lots[ticker].append(
                    ArbLot(
                        ticker=ticker,
                        side=req["side"],
                        qty=abs(int(inv_change)),
                        entry_price=int(req.get("price", 0)),
                        placed_mono=time.monotonic(),
                    )
                )
        # If anything rests, register it as a live order.
        otype = req.get("order_type", "limit")
        if oid is not None and otype == "limit":
            ordered_qty = int(req["quantity"])
            filled = abs(int(inv_change or 0))
            remaining = ordered_qty - filled
            if remaining > 0:
                lo = LiveOrder(
                    order_id=int(oid),
                    ticker=ticker,
                    side=req["side"],
                    price=int(req.get("price", 0)),
                    quantity=remaining,
                    placed_mono=req.get("placed_mono", time.monotonic()),
                    expiry_unix_ms=int(req.get("expiry", now_unix_ms() + ORDER_TTL_MS_DEFAULT)),
                    purpose=purpose,
                    user_request_id=urid,
                )
                st.live_orders[lo.order_id] = lo
                st.own_order_ids.add(lo.order_id)

    def _handle_cancel_resp(self, msg: dict) -> None:
        # Public cancel events also clean up; we let those drive removal.
        if not msg.get("success"):
            self.log.debug("cancel failed: %s", msg.get("message"))

    def _handle_inventory_resp(self, msg: dict) -> None:
        st = self.state
        data = msg.get("data") or {}
        # Reconcile positions and cash against authoritative state.
        for k, pair in data.items():
            try:
                reserved = int(pair[0])
                total = int(pair[1])
            except (TypeError, ValueError, IndexError):
                continue
            if k == "$":
                if total != st.cash_total:
                    self.log.debug("cash drift %s -> %s", st.cash_total, total)
                st.cash_total = total
                st.cash_reserved = reserved
            else:
                ticker = k.split("-", 1)[1] if "-" in k else k
                if total != st.positions.get(ticker, 0):
                    self.log.debug("pos drift %s: %s -> %s", ticker, st.positions.get(ticker, 0), total)
                st.positions[ticker] = total
                st.positions_reserved[ticker] = reserved
        st.last_inventory_sync_mono = time.monotonic()

    def _handle_pending_resp(self, msg: dict) -> None:
        st = self.state
        data = msg.get("data") or {}
        seen_oids: set[int] = set()
        for full_id, sides in data.items():
            ticker = full_id.split("-", 1)[1] if "-" in full_id else full_id
            for orderlist in sides:
                for o in orderlist or []:
                    oid = int(o.get("orderID", 0))
                    if oid <= 0:
                        continue
                    seen_oids.add(oid)
                    if oid in st.live_orders:
                        st.live_orders[oid].quantity = int(o.get("unfilled_quantity", 0))
                    else:
                        # Unknown — register so we can manage it.
                        side = o.get("side", "BID").lower()
                        st.live_orders[oid] = LiveOrder(
                            order_id=oid,
                            ticker=ticker,
                            side="bid" if side == "bid" else "ask",
                            price=int(o.get("price", 0)),
                            quantity=int(o.get("unfilled_quantity", 0)),
                            placed_mono=time.monotonic(),
                            expiry_unix_ms=int(o.get("expiry", now_unix_ms() + 1000)),
                            purpose="recovered",
                            user_request_id="",
                        )
                        st.own_order_ids.add(oid)
        # Drop any local orders not in the authoritative list.
        stale = [oid for oid in st.live_orders if oid not in seen_oids]
        for oid in stale:
            self._remove_live_order(oid)


# ════════════════════════════════════════════════════════════════════════════
# Cross-exchange market data view
# ════════════════════════════════════════════════════════════════════════════

class MarketView:
    """Provides cross-venue computations (basket fair value, ZSE oracle prices)."""

    def __init__(self, states: dict[str, ExchangeState]) -> None:
        self.states = states

    def book(self, exchange: str, ticker: str) -> Optional[Book]:
        st = self.states.get(exchange)
        if st is None:
            return None
        b = st.books.get(ticker)
        return b if (b is not None and b.is_two_sided()) else None

    def mid(self, exchange: str, ticker: str) -> Optional[int]:
        b = self.book(exchange, ticker)
        return b.mid if b is not None else None

    def best_bid(self, exchange: str, ticker: str) -> Optional[tuple[int, int]]:
        b = self.book(exchange, ticker)
        if b is None or b.best_bid is None:
            return None
        return b.best_bid, b.best_bid_qty

    def best_ask(self, exchange: str, ticker: str) -> Optional[tuple[int, int]]:
        b = self.book(exchange, ticker)
        if b is None or b.best_ask is None:
            return None
        return b.best_ask, b.best_ask_qty

    def basket_fair_local(self, exchange: str, etf: str) -> Optional[int]:
        """Equal-weighted basket fair value using the SAME exchange's quotes."""
        basket = ETF_BASKETS.get(etf)
        if not basket:
            return None
        mids: list[int] = []
        for t in basket:
            m = self.mid(exchange, t)
            if m is None:
                return None
            mids.append(m)
        return sum(mids) // len(mids)

    def basket_fair_zse(self, etf: str) -> Optional[int]:
        return self.basket_fair_local("ZSE", etf)

    def best_basket_fair(self, exchange: str, etf: str) -> Optional[tuple[int, str]]:
        """Prefer the local basket; fall back to ZSE oracle.

        Returns (price_in_cents, source) where source is "local" or "zse".
        """
        v = self.basket_fair_local(exchange, etf)
        if v is not None:
            return v, "local"
        v = self.basket_fair_zse(etf)
        return (v, "zse") if v is not None else None


# ════════════════════════════════════════════════════════════════════════════
# Strategy framework
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class StrategyContext:
    engine: "TradingEngine"
    market: MarketView
    states: dict[str, ExchangeState]
    home_exchange: Optional[str]      # our co-located exchange (lowest /health RTT)
    server_time_ms_by_exchange: dict[str, int]


class Strategy:
    name: str = "base"

    async def evaluate(self, ctx: StrategyContext, exchange: str) -> None:
        raise NotImplementedError


# ─── 1. Same-exchange ETF basket arbitrage (deterministic edge) ───────────

class ETFBasketArbStrategy(Strategy):
    """If the local ETF best ask is below the local basket fair, buy ETF (IOC).
    If the local ETF best bid is above the local basket fair, sell ETF (IOC).

    This is *single-leg*: we don't hedge with the basket. The mean-reversion
    is enforced by the contest rule; over many trades the law of large
    numbers carries the day. We bound risk via (a) tiny clip qty, (b) hard
    inventory caps, (c) forced unwind after CONVERGENCE_HOLD_MS.
    """
    name = "etf_basket_arb"

    async def evaluate(self, ctx: StrategyContext, exchange: str) -> None:
        st = ctx.states.get(exchange)
        if st is None or st.end_of_round or not st.welcome_received:
            return
        conn = ctx.engine.connections.get(exchange)
        if conn is None:
            return
        for etf, basket in ETF_BASKETS.items():
            if etf not in EXCHANGE_INSTRUMENTS.get(exchange, frozenset()):
                continue
            local_fair = ctx.market.basket_fair_local(exchange, etf)
            if local_fair is None:
                continue
            etf_book = ctx.market.book(exchange, etf)
            if etf_book is None or not etf_book.is_two_sided():
                continue
            # Inventory-aware threshold.
            pos = st.position(etf)
            extra_buy_threshold = max(0, pos // 8)   # tighter buy threshold the longer we are
            extra_sell_threshold = max(0, (-pos) // 8)
            buy_threshold = ARB_MIN_EDGE_CENTS + extra_buy_threshold
            sell_threshold = ARB_MIN_EDGE_CENTS + extra_sell_threshold

            # Buy side: ETF cheap?
            ba = etf_book.best_ask
            ba_qty = etf_book.best_ask_qty
            if ba is not None and (local_fair - ba) >= buy_threshold:
                if pos + ARB_CLIP_QTY <= MAX_POS_LONG:
                    qty = min(ARB_CLIP_QTY, ba_qty, _safe_buy_qty(st, etf, ba))
                    if qty > 0 and not _has_pending(st, etf, "bid"):
                        await conn.add_order(
                            etf, "bid", ba, qty, order_type="ioc",
                            ttl_ms=400, purpose="arb",
                        )
            # Sell side: ETF rich?
            bb = etf_book.best_bid
            bb_qty = etf_book.best_bid_qty
            if bb is not None and (bb - local_fair) >= sell_threshold:
                if pos - ARB_CLIP_QTY >= MAX_POS_SHORT:
                    qty = min(ARB_CLIP_QTY, bb_qty, _safe_sell_qty(st, etf))
                    if qty > 0 and not _has_pending(st, etf, "ask"):
                        await conn.add_order(
                            etf, "ask", bb, qty, order_type="ioc",
                            ttl_ms=400, purpose="arb",
                        )


# ─── 2. ZSE-anchored ETF arbitrage (cross-venue) ──────────────────────────

class ZSEAnchoredETFArbStrategy(Strategy):
    """Use the ZSE basket fair as the oracle for every venue. Trade only when
    the per-share edge clears the latency-cost threshold."""
    name = "zse_anchored_etf_arb"

    async def evaluate(self, ctx: StrategyContext, exchange: str) -> None:
        st = ctx.states.get(exchange)
        if st is None or st.end_of_round or not st.welcome_received:
            return
        if exchange == "ZSE":
            return  # ZSE is handled by ETFBasketArbStrategy
        conn = ctx.engine.connections.get(exchange)
        if conn is None:
            return
        rtt = _LATENCY_RTT_MS.get((ctx.home_exchange or exchange, exchange), 100)
        edge_required = ARB_CROSS_VENUE_BASE_EDGE + max(0, (rtt - 30) // 5)
        for etf, basket in ETF_BASKETS.items():
            if etf not in EXCHANGE_INSTRUMENTS.get(exchange, frozenset()):
                continue
            # If we have a local basket, the same-exchange strategy already covers it.
            if ctx.market.basket_fair_local(exchange, etf) is not None:
                continue
            zfair = ctx.market.basket_fair_zse(etf)
            if zfair is None:
                continue
            etf_book = ctx.market.book(exchange, etf)
            if etf_book is None or not etf_book.is_two_sided():
                continue
            pos = st.position(etf)
            ba = etf_book.best_ask
            ba_qty = etf_book.best_ask_qty
            if ba is not None and (zfair - ba) >= edge_required:
                if pos + ARB_CLIP_QTY <= MAX_POS_LONG:
                    qty = min(ARB_CLIP_QTY, ba_qty, _safe_buy_qty(st, etf, ba))
                    if qty > 0 and not _has_pending(st, etf, "bid"):
                        await conn.add_order(
                            etf, "bid", ba, qty, order_type="ioc",
                            ttl_ms=400, purpose="cross",
                        )
            bb = etf_book.best_bid
            bb_qty = etf_book.best_bid_qty
            if bb is not None and (bb - zfair) >= edge_required:
                if pos - ARB_CLIP_QTY >= MAX_POS_SHORT:
                    qty = min(ARB_CLIP_QTY, bb_qty, _safe_sell_qty(st, etf))
                    if qty > 0 and not _has_pending(st, etf, "ask"):
                        await conn.add_order(
                            etf, "ask", bb, qty, order_type="ioc",
                            ttl_ms=400, purpose="cross",
                        )


# ─── 3. Inventory-skew passive market making around true fair ────────────

class MMSkewStrategy(Strategy):
    """Detect the MM's inventory skew via spread asymmetry around basket fair,
    then place a passive limit on the opposite side to harvest noise-trader
    fills.

    Only active for instruments with a computable synthetic fair (ETFs +
    constituents whose basket implies a fair). Skips CARD/SIMP — their
    "logic is for you to figure out" — too risky without a fair anchor.
    """
    name = "mm_skew"

    async def evaluate(self, ctx: StrategyContext, exchange: str) -> None:
        st = ctx.states.get(exchange)
        if st is None or st.end_of_round or not st.welcome_received:
            return
        # Only run aggressively when co-located OR on ZSE (always-on full coverage).
        if ctx.home_exchange not in (exchange, "ZSE") and exchange != "ZSE":
            return
        conn = ctx.engine.connections.get(exchange)
        if conn is None:
            return
        time_remaining = st.time_remaining_ms()
        if time_remaining is not None and time_remaining < EOS_UNWIND_LEAD_MS:
            return  # close-out phase: no new resting orders

        # First, ETFs.
        for etf in ETF_BASKETS:
            if etf not in EXCHANGE_INSTRUMENTS.get(exchange, frozenset()):
                continue
            fair = ctx.market.basket_fair_local(exchange, etf) or ctx.market.basket_fair_zse(etf)
            if fair is None:
                continue
            await self._maybe_quote(ctx, conn, st, etf, fair)

        # Then constituents — but only if we can imply a fair from peers.
        # We approximate per-constituent fair as the local mid; skew detection
        # is then vs the local mid. This is weaker than the ETF case.

    async def _maybe_quote(
        self,
        ctx: StrategyContext,
        conn: ExchangeConnection,
        st: ExchangeState,
        ticker: str,
        fair: int,
    ) -> None:
        b = ctx.market.book(conn.exchange, ticker)
        if b is None or not b.is_two_sided():
            return
        if not b.is_fresh(st.server_time_ms):
            return
        bb, ba = b.best_bid, b.best_ask
        if bb is None or ba is None:
            return
        mm_mid = (bb + ba) // 2
        skew = mm_mid - fair  # positive: book leans high (MM short, ask is overpriced)
        spread = ba - bb
        if spread <= 1 or abs(skew) < SKEW_MIN_CENTS:
            return
        pos = st.position(ticker)

        # Only quote one side at a time, on the side opposite the MM's lean
        # (i.e. the side closer to the true fair).
        if skew > 0 and pos - SKEW_CLIP_QTY >= MAX_POS_SHORT:
            # MM ask is overpriced. Sell at fair + 1 cent (or just inside MM ask).
            target = max(fair + 1, bb + 1)
            target = min(target, ba - 1)
            if target > bb and not _has_pending_at(st, ticker, "ask", target):
                await conn.add_order(
                    ticker, "ask", target, SKEW_CLIP_QTY,
                    order_type="limit", ttl_ms=ORDER_TTL_MS_MM, purpose="mm",
                )
        elif skew < 0 and pos + SKEW_CLIP_QTY <= MAX_POS_LONG:
            # MM bid is underpriced. Buy at fair − 1 cent (or just inside MM bid).
            target = min(fair - 1, ba - 1)
            target = max(target, bb + 1)
            if target < ba and not _has_pending_at(st, ticker, "bid", target):
                qty = min(SKEW_CLIP_QTY, _safe_buy_qty(st, ticker, target))
                if qty > 0:
                    await conn.add_order(
                        ticker, "bid", target, qty,
                        order_type="limit", ttl_ms=ORDER_TTL_MS_MM, purpose="mm",
                    )


# ─── 4. Cross-venue stock arbitrage (latency-budgeted) ────────────────────

class CrossVenueStockArbStrategy(Strategy):
    """For tickers listed on multiple exchanges, execute single-leg arb when
    one venue's price clears the latency-cost edge vs the ZSE oracle (or
    against the lower-latency venue). One direction at a time per ticker."""
    name = "cross_venue_stock_arb"

    async def evaluate(self, ctx: StrategyContext, exchange: str) -> None:
        st = ctx.states.get(exchange)
        if st is None or st.end_of_round or not st.welcome_received:
            return
        if exchange == "ZSE":
            return  # ZSE is the oracle; no incoming arb here
        conn = ctx.engine.connections.get(exchange)
        if conn is None:
            return
        rtt = _LATENCY_RTT_MS.get((ctx.home_exchange or exchange, exchange), 100)
        # Tighter venues require less edge.
        edge_required = ARB_CROSS_VENUE_BASE_EDGE + max(0, (rtt - 30) // 4)

        for ticker in EXCHANGE_INSTRUMENTS.get(exchange, frozenset()):
            if ticker in ETF_BASKETS:
                continue
            zse_mid = ctx.market.mid("ZSE", ticker)
            if zse_mid is None:
                continue
            book = ctx.market.book(exchange, ticker)
            if book is None or not book.is_two_sided():
                continue
            pos = st.position(ticker)
            ba = book.best_ask
            ba_qty = book.best_ask_qty
            if ba is not None and (zse_mid - ba) >= edge_required:
                if pos + ARB_CLIP_QTY <= MAX_POS_LONG:
                    qty = min(ARB_CLIP_QTY, ba_qty, _safe_buy_qty(st, ticker, ba))
                    if qty > 0 and not _has_pending(st, ticker, "bid"):
                        await conn.add_order(
                            ticker, "bid", ba, qty, order_type="ioc",
                            ttl_ms=400, purpose="cross",
                        )
            bb = book.best_bid
            bb_qty = book.best_bid_qty
            if bb is not None and (bb - zse_mid) >= edge_required:
                if pos - ARB_CLIP_QTY >= MAX_POS_SHORT:
                    qty = min(ARB_CLIP_QTY, bb_qty, _safe_sell_qty(st, ticker))
                    if qty > 0 and not _has_pending(st, ticker, "ask"):
                        await conn.add_order(
                            ticker, "ask", bb, qty, order_type="ioc",
                            ttl_ms=400, purpose="cross",
                        )


# ─── 5. End-of-segment delta neutralization ───────────────────────────────

class EndOfSegmentUnwindStrategy(Strategy):
    """In the final EOS_UNWIND_LEAD_MS of a segment, cancel resting orders
    and close exposure with IOC orders against the top of book.

    Settlement is mark-to-market with an undisclosed window — variance, not
    edge. Better to lock convergence profits than to gamble on the close.
    """
    name = "eos_unwind"

    async def evaluate(self, ctx: StrategyContext, exchange: str) -> None:
        st = ctx.states.get(exchange)
        if st is None or st.end_of_round or not st.welcome_received:
            return
        rem = st.time_remaining_ms()
        if rem is None or rem > EOS_UNWIND_LEAD_MS:
            return
        conn = ctx.engine.connections.get(exchange)
        if conn is None:
            return

        # Cancel everything still resting.
        for oid, order in list(st.live_orders.items()):
            await conn.cancel_order(oid, order.ticker)

        if rem <= EOS_HARD_STOP_MS:
            return  # too late — let the server settle.

        # Walk net positions and close them with IOC against current top-of-book.
        for ticker, qty in list(st.positions.items()):
            if qty == 0 or ticker == "$":
                continue
            book = ctx.market.book(exchange, ticker)
            if book is None or not book.is_two_sided():
                continue
            if qty > 0:
                # Long: sell down. Hit the bid with IOC.
                bb = book.best_bid
                bb_qty = book.best_bid_qty
                clip = min(qty, UNWIND_CLIP_QTY, bb_qty)
                if clip > 0 and bb is not None:
                    await conn.add_order(
                        ticker, "ask", bb, clip, order_type="ioc",
                        ttl_ms=400, purpose="unwind",
                    )
            else:
                # Short: buy back. Lift the ask with IOC.
                ba = book.best_ask
                ba_qty = book.best_ask_qty
                clip = min(-qty, UNWIND_CLIP_QTY, ba_qty)
                if clip > 0 and ba is not None:
                    safe = _safe_buy_qty(st, ticker, ba)
                    clip = min(clip, safe)
                    if clip > 0:
                        await conn.add_order(
                            ticker, "bid", ba, clip, order_type="ioc",
                            ttl_ms=400, purpose="unwind",
                        )


# ─── 6. Convergence holding clock — force exit stale arb lots ─────────────

class ConvergenceClockStrategy(Strategy):
    """If an arbitrage entry hasn't unwound itself naturally within the
    holding window, force-exit through the book (sweep up to a few levels)."""
    name = "convergence_clock"

    async def evaluate(self, ctx: StrategyContext, exchange: str) -> None:
        st = ctx.states.get(exchange)
        if st is None or st.end_of_round or not st.welcome_received:
            return
        rem = st.time_remaining_ms()
        if rem is not None and rem < EOS_UNWIND_LEAD_MS:
            return  # leave to EOS unwind
        conn = ctx.engine.connections.get(exchange)
        if conn is None:
            return
        now = time.monotonic()
        for ticker, lots in list(st.open_lots.items()):
            while lots and (now - lots[0].placed_mono) > (CONVERGENCE_HOLD_MS / 1000.0):
                lot = lots.popleft()
                book = ctx.market.book(exchange, ticker)
                if book is None or not book.is_two_sided():
                    continue
                if lot.side == "bid":
                    # We bought; close by selling.
                    bb = book.best_bid
                    if bb is not None and st.position(ticker) > 0:
                        clip = min(lot.qty, st.position(ticker), book.best_bid_qty)
                        if clip > 0:
                            await conn.add_order(
                                ticker, "ask", bb, clip, order_type="ioc",
                                ttl_ms=400, purpose="unwind",
                            )
                else:
                    ba = book.best_ask
                    if ba is not None and st.position(ticker) < 0:
                        clip = min(lot.qty, -st.position(ticker), book.best_ask_qty)
                        if clip > 0:
                            safe = _safe_buy_qty(st, ticker, ba)
                            clip = min(clip, safe)
                            if clip > 0:
                                await conn.add_order(
                                    ticker, "bid", ba, clip, order_type="ioc",
                                    ttl_ms=400, purpose="unwind",
                                )


# ════════════════════════════════════════════════════════════════════════════
# Helpers used by strategies
# ════════════════════════════════════════════════════════════════════════════

def _has_pending(st: ExchangeState, ticker: str, side: str) -> bool:
    """Is there already a live order on this side for this ticker?"""
    for o in st.live_orders.values():
        if o.ticker == ticker and o.side == side:
            return True
    # Also check pending_by_urid (request in flight, no response yet).
    for req in st.pending_by_urid.values():
        if req.get("instrument_id", "").endswith("-" + ticker) and req.get("side") == side:
            return True
    return False


def _has_pending_at(st: ExchangeState, ticker: str, side: str, price: int) -> bool:
    for o in st.live_orders.values():
        if o.ticker == ticker and o.side == side and o.price == price:
            return True
    for req in st.pending_by_urid.values():
        if (
            req.get("instrument_id", "").endswith("-" + ticker)
            and req.get("side") == side
            and int(req.get("price", -1)) == price
        ):
            return True
    return False


def _local_reserved_cash(st: ExchangeState) -> int:
    """Estimate cash locked by our own resting bids (server may not yet have
    reflected them in cash_reserved)."""
    total = 0
    for o in st.live_orders.values():
        if o.side == "bid":
            total += o.price * o.quantity
    return total


def _local_reserved_pos(st: ExchangeState, ticker: str) -> int:
    """Estimate units locked by our own resting asks for a given ticker."""
    total = 0
    for o in st.live_orders.values():
        if o.side == "ask" and o.ticker == ticker:
            total += o.quantity
    return total


def _safe_buy_qty(st: ExchangeState, ticker: str, price: int) -> int:
    """How many can we buy without breaching cash floor or pos ceiling?"""
    pos_room = MAX_POS_LONG - st.position(ticker)
    if pos_room <= 0:
        return 0
    reserved = max(st.cash_reserved, _local_reserved_cash(st))
    cash_room = max(0, st.cash_total - CASH_FLOOR - reserved)
    if price <= 0:
        return pos_room
    by_cash = cash_room // max(price, 1)
    return max(0, min(pos_room, by_cash))


def _safe_sell_qty(st: ExchangeState, ticker: str) -> int:
    """How many can we sell without breaching short floor (counting our own
    resting asks as already-committed inventory)?"""
    reserved_pos = max(st.positions_reserved.get(ticker, 0), _local_reserved_pos(st, ticker))
    pos_room = st.position(ticker) - reserved_pos - MAX_POS_SHORT
    return max(0, pos_room)


# ════════════════════════════════════════════════════════════════════════════
# Latency probe — auto-detect co-located exchange via /health RTT
# ════════════════════════════════════════════════════════════════════════════

async def probe_latency(exchanges: list[str]) -> dict[str, float]:
    """Returns ms RTT per exchange. Missing = probe failed. Uses aiohttp if
    available; falls back to asyncio TCP open if not."""
    results: dict[str, float] = {}
    if aiohttp is not None:
        timeout = aiohttp.ClientTimeout(total=LATENCY_PROBE_TIMEOUT_S)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def probe(ex: str) -> None:
                start = time.monotonic()
                try:
                    async with session.get(health_url(ex)) as r:
                        await r.read()
                    results[ex] = (time.monotonic() - start) * 1000.0
                except Exception:
                    pass
            await asyncio.gather(*(probe(ex) for ex in exchanges))
    else:
        async def probe(ex: str) -> None:
            start = time.monotonic()
            try:
                fut = asyncio.open_connection(f"{ex.lower()}.algotrade.hr", PORT)
                reader, writer = await asyncio.wait_for(fut, LATENCY_PROBE_TIMEOUT_S)
                results[ex] = (time.monotonic() - start) * 1000.0
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()
            except Exception:
                pass
        await asyncio.gather(*(probe(ex) for ex in exchanges))
    return results


# ════════════════════════════════════════════════════════════════════════════
# TradingEngine — orchestrator
# ════════════════════════════════════════════════════════════════════════════

class TradingEngine:
    def __init__(self, exchanges: list[str]) -> None:
        self.exchanges = [e.upper() for e in exchanges]
        self.states: dict[str, ExchangeState] = {
            ex: ExchangeState(exchange=ex) for ex in self.exchanges
        }
        self.connections: dict[str, ExchangeConnection] = {}
        self.market = MarketView(self.states)
        self.home_exchange: Optional[str] = None
        self._md_locks: dict[str, asyncio.Lock] = {ex: asyncio.Lock() for ex in self.exchanges}
        self._stop = asyncio.Event()

        disabled = {
            s.strip().lower()
            for s in os.environ.get("APEX_DISABLE", "").split(",")
            if s.strip()
        }
        all_strategies: list[Strategy] = [
            ETFBasketArbStrategy(),
            ZSEAnchoredETFArbStrategy(),
            CrossVenueStockArbStrategy(),
            MMSkewStrategy(),
            ConvergenceClockStrategy(),
            EndOfSegmentUnwindStrategy(),
        ]
        self.strategies = [s for s in all_strategies if s.name not in disabled]

    async def run(self) -> None:
        log.info(
            "starting APEX on %s (DRY_RUN=%s, strategies=%s)",
            self.exchanges, DRY_RUN, [s.name for s in self.strategies],
        )

        # Connections
        for i, ex in enumerate(self.exchanges):
            conn = ExchangeConnection(ex, self.states[ex], self._on_md)
            self.connections[ex] = conn

        tasks: list[asyncio.Task] = []
        for i, ex in enumerate(self.exchanges):
            # Stagger initial connection opens to dodge the open-rate limit.
            await asyncio.sleep(SEGMENT_BOUNDARY_STAGGER_S * (i % MAX_CONNECTIONS))
            tasks.append(asyncio.create_task(self.connections[ex].run(), name=f"conn-{ex}"))

        tasks.append(asyncio.create_task(self._latency_probe_loop(), name="latency-probe"))
        tasks.append(asyncio.create_task(self._inventory_recon_loop(), name="inventory-recon"))
        tasks.append(asyncio.create_task(self._stale_order_sweeper(), name="stale-sweeper"))
        tasks.append(asyncio.create_task(self._round_length_loop(), name="round-length"))

        try:
            await self._stop.wait()
        finally:
            for c in self.connections.values():
                await c.stop()
            for t in tasks:
                t.cancel()
            with suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        self._stop.set()

    # — strategy fan-out —

    async def _on_md(self, exchange: str) -> None:
        # Serialize per-exchange evaluation so two MD ticks don't race.
        lock = self._md_locks[exchange]
        if lock.locked():
            return
        async with lock:
            ctx = StrategyContext(
                engine=self,
                market=self.market,
                states=self.states,
                home_exchange=self.home_exchange,
                server_time_ms_by_exchange={
                    ex: st.server_time_ms for ex, st in self.states.items()
                },
            )
            for strat in self.strategies:
                try:
                    await strat.evaluate(ctx, exchange)
                except Exception as e:
                    log.exception("strategy %s on %s: %s", strat.name, exchange, e)

    # — periodic loops —

    async def _latency_probe_loop(self) -> None:
        # Use the *full* set of exchanges for probing if we can — gives us a
        # better co-location read even if we only trade a subset.
        probe_set = list(EXCHANGES)
        # First probe immediately, then every interval.
        first = True
        while not self._stop.is_set():
            try:
                rtts = await probe_latency(probe_set)
                if rtts:
                    home = min(rtts, key=lambda k: rtts[k])
                    if rtts[home] <= COLOCATED_RTT_MAX_MS:
                        if home != self.home_exchange:
                            log.info("co-located at %s (RTT=%.1fms)", home, rtts[home])
                        self.home_exchange = home
                    else:
                        # Fall back to lowest-RTT venue we can see, even if not strictly co-located.
                        if home != self.home_exchange:
                            log.info("nearest exchange: %s (RTT=%.1fms)", home, rtts[home])
                        self.home_exchange = home
                    for ex, rtt in rtts.items():
                        if ex in self.states:
                            self.states[ex].last_health_rtt_ms = rtt
            except Exception as e:
                log.debug("probe error: %s", e)
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       timeout=LATENCY_PROBE_INTERVAL_S if not first else 5.0)
                return
            except asyncio.TimeoutError:
                first = False

    async def _inventory_recon_loop(self) -> None:
        while not self._stop.is_set():
            for ex, conn in self.connections.items():
                st = self.states[ex]
                if st.welcome_received and not st.end_of_round:
                    await conn.get_inventory()
                    await conn.get_pending()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=INVENTORY_RECONCILE_INTERVAL_S)
                return
            except asyncio.TimeoutError:
                pass

    async def _round_length_loop(self) -> None:
        """Hit /health on each exchange to populate round_length_ms once we
        connect, so EOS unwind has a clock."""
        while not self._stop.is_set():
            todo = [ex for ex, st in self.states.items() if st.round_length_ms is None]
            if todo and aiohttp is not None:
                timeout = aiohttp.ClientTimeout(total=2.0)
                try:
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async def grab(ex: str) -> None:
                            try:
                                async with session.get(health_url(ex)) as r:
                                    body = await r.json(content_type=None)
                                    rl = int(body.get("round_length", 0))
                                    if rl > 0:
                                        self.states[ex].round_length_ms = rl
                                        log.info("%s round_length=%dms", ex, rl)
                            except Exception:
                                pass
                        await asyncio.gather(*(grab(ex) for ex in todo))
                except Exception as e:
                    log.debug("round-length probe error: %s", e)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=10.0)
                return
            except asyncio.TimeoutError:
                pass

    async def _stale_order_sweeper(self) -> None:
        """Sweep zombie pending_by_urid entries that never got a response."""
        while not self._stop.is_set():
            cutoff = time.monotonic() - 8.0
            for st in self.states.values():
                drop = [u for u, req in st.pending_by_urid.items()
                        if req.get("placed_mono", time.monotonic()) < cutoff]
                for u in drop:
                    st.pending_by_urid.pop(u, None)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=2.0)
                return
            except asyncio.TimeoutError:
                pass


# ════════════════════════════════════════════════════════════════════════════
# Entrypoint
# ════════════════════════════════════════════════════════════════════════════

def parse_exchanges_env() -> list[str]:
    raw = os.environ.get("APEX_EXCHANGES", "").strip()
    if not raw:
        return list(EXCHANGES)
    out: list[str] = []
    for tok in raw.replace(";", ",").split(","):
        t = tok.strip().upper()
        if t in EXCHANGES and t not in out:
            out.append(t)
    return out or list(EXCHANGES)


async def amain() -> int:
    exchanges = parse_exchanges_env()
    if len(exchanges) > MAX_CONNECTIONS:
        log.warning("limiting to %d exchanges (rule: max %d connections)",
                    MAX_CONNECTIONS, MAX_CONNECTIONS)
        exchanges = exchanges[:MAX_CONNECTIONS]
    engine = TradingEngine(exchanges)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(engine.shutdown()))

    try:
        await engine.run()
    except asyncio.CancelledError:
        pass
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(amain()))
    except KeyboardInterrupt:
        raise SystemExit(130)
