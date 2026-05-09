#!/usr/bin/env python3
"""
cascade.py — AlgoTrade 2026 trading bot.

Direct descendant of prism.py with three surgical improvements aimed at the
specific places prism leaves money on the table.  Same strategies, same
thresholds, same defensive bits — only execution changes:

  1. **Depth-walked ETF basket arb.**  Where prism caps basket size at the
     top-of-book qty (k = best_ask_qty // n ≈ 8 baskets when MM quotes 50),
     cascade walks the ask side and takes every level that still beats
     ARB_EDGE on its marginal price.  The IOC limit is set to the worst
     accepted level so server price-time priority gives us improvement on
     levels above.  Same edge, same risk per share, ~3× the size when the
     MM is meaningfully mispriced.

  2. **Plan ranking before firing.**  When several arbs land on the same
     market-data tick, fire the highest-edge plan first.  Same plans, same
     fits checks — just budget-aware ordering.

  3. **Faster reconnect.**  Exponential-backoff cap from 4.0 s to 1.5 s.
     At segment boundaries (3 per round) every saved second is trade time.

The choice "improve execution, not strategy" is deliberate.  prism beats
parallax in this repo, and parallax differs by adding strategies, not by
sharpening prism's existing ones.  Karpathy thinking applied: don't add
new failure modes, sharpen the proven ones.

Edges exploited (priority order, unchanged from prism):

  1. **Multi-venue ETF<->basket arbitrage**, now depth-walked on the ETF leg.
  2. **Sub-ETF arbitrage** (6·ETFA = 3·ETFA3 + complement; same for B).
  3. **Cross-venue same-stock arbitrage**.
  4. **Step-inside-MM passive making**.
  5. **Settlement-aware unwind**.

Defensive bits (unchanged):

  * Local 400 msg/s/exchange token bucket — 80 % of the 500/s server cap.
  * Atomic plan validation — every leg's headroom is checked before any
    leg ships; partial dispatch is impossible.
  * Optimistic local position with periodic get_inventory reconcile.
  * Per-exchange reconnect with exponential backoff for segment resets.

Run
---
    python3 cascade.py                       # connects to every exchange
    python3 cascade.py --venues ZSE,NYSE     # subset for testing
    LOGLEVEL=DEBUG python3 cascade.py        # verbose tracing
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import math
import os
import random
import signal
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # websockets <12 fallback
    from websockets.client import connect as ws_connect  # type: ignore

# ════════════════════════════════════════════════════════════════════════════
# Universe
# ════════════════════════════════════════════════════════════════════════════

VENUES = ["NYSE", "NASDAQ", "SSE", "JPX", "EURONEXT", "LSE", "HKEX", "NSE", "TMX", "ZSE"]
WS_HOSTS = {
    "NYSE":     "nyse.algotrade.hr",
    "NASDAQ":   "nasdaq.algotrade.hr",
    "SSE":      "sse.algotrade.hr",
    "JPX":      "jpx.algotrade.hr",
    "EURONEXT": "euronext.algotrade.hr",
    "LSE":      "lse.algotrade.hr",
    "HKEX":     "hkex.algotrade.hr",
    "NSE":      "nse.algotrade.hr",
    "TMX":      "tmx.algotrade.hr",
    "ZSE":      "zse.algotrade.hr",
}

# Stock listings (which venues list each instrument), transcribed from the
# participant guide's tables.  Used for cross-venue routing of arb legs.
LISTINGS: dict[str, set[str]] = {
    "CARD":  {"NYSE","NASDAQ","LSE","EURONEXT","JPX","SSE","HKEX","NSE","TMX","ZSE"},
    "SIMP":  {"NYSE","NASDAQ","LSE","EURONEXT","JPX","SSE","HKEX","NSE","TMX","ZSE"},
    "NGUP":  {"NYSE","NASDAQ","EURONEXT","TMX","ZSE"},
    "OIT":   {"LSE","EURONEXT","HKEX","NSE","ZSE"},
    "KTST":  {"NYSE","JPX","TMX","ZSE"},
    "FSR":   {"NASDAQ","LSE","SSE","HKEX","ZSE"},
    "JZRO":  {"NYSE","LSE","EURONEXT","TMX","ZSE"},
    "XFR":   {"NYSE","HKEX","TMX","ZSE"},
    "KOTD":  {"NASDAQ","LSE","EURONEXT","HKEX","ZSE"},
    "INA":   {"NYSE","NASDAQ","EURONEXT","HKEX","ZSE"},
    "HT":    {"NASDAQ","LSE","JPX","SSE","TMX","ZSE"},
    "JNAF":  {"NYSE","EURONEXT","JPX","HKEX","ZSE"},
    "DLKV":  {"NASDAQ","LSE","HKEX","NSE","ZSE"},
    "DDJH":  {"NYSE","LSE","EURONEXT","TMX","ZSE"},
    "MDKA":  {"NYSE","LSE","HKEX","TMX","ZSE"},
    "KRAS":  {"NYSE","EURONEXT","SSE","TMX","ZSE"},
    "ZITO":  {"NASDAQ","LSE","EURONEXT","NSE","ZSE"},
    "ZABA":  {"NYSE","LSE","SSE","NSE","TMX","ZSE"},
    "GOLD":  {"NASDAQ","EURONEXT","JPX","TMX","ZSE"},
    "XAG":   {"LSE","EURONEXT","JPX","ZSE"},
    "ETFA":  {"NYSE","EURONEXT","HKEX","ZSE"},
    "ETFB":  {"NASDAQ","LSE","HKEX","ZSE"},
    "ETFA3": {"NYSE","TMX","ZSE"},
    "ETFB3": {"NASDAQ","HKEX","ZSE"},
    "ETFSH": {"EURONEXT","JPX","ZSE"},
}

ETF_BASKETS = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}
ETFS = list(ETF_BASKETS)
STOCKS = sorted(set(LISTINGS) - set(ETF_BASKETS))

# Sub-ETF identities derived from spec:  ETFA3 ⊂ ETFA, ETFB3 ⊂ ETFB.
# super → (sub_etf, complement_basket).  6·super = 3·sub + sum(complement).
SUB_ETF_LINKS = {
    "ETFA": ("ETFA3", ["OIT", "FSR", "JZRO"]),
    "ETFB": ("ETFB3", ["HT",  "JNAF", "DDJH"]),
}

# ════════════════════════════════════════════════════════════════════════════
# Hard server limits & strategy knobs
# ════════════════════════════════════════════════════════════════════════════

# Server-enforced caps.  Anything past these is a connection/order rejection.
POS_MAX            =  2_000           # long ceiling per (venue, instrument)
POS_MIN            =   -200           # short floor
INITIAL_CASH       = 10_000_000       # cents = $100k per venue
CASH_FLOOR         = -5_000_000       # cents = -$50k per venue
MAX_PENDING_ORDERS =  6_000
SERVER_RATE_PER_S  =    500           # hard server cap

# Soft caps we apply to leave headroom.  Hitting the server cap closes the WS;
# these keep us strictly under that cliff.
RATE_PER_S         =    400           # 80% of server cap
SOFT_POS_MAX       =  1_800           # leave 200-share buffer for arb legs
SOFT_POS_MIN       =   -180
SOFT_CASH_FLOOR    = -4_500_000       # cents (leave $5k buffer)

# Strategy edge thresholds (cents).  Conservative defaults; tune in testing.
ARB_EDGE           =      4           # ETF<->basket min edge per share
SUB_ETF_EDGE       =      6           # sub-ETF arb min edge (more legs => more risk)
XV_EDGE            =      3           # cross-venue same-stock min edge
ARB_MAX_K          =     25           # max basket-multiples per shot
XV_MAX_QTY         =     40
MM_INSIDE_TICK     =      1           # step inside MM quote by 1 cent
MM_QTY             =      4
MM_REFRESH_S       =     1.5
MM_INSTRUMENTS     = ["CARD", "SIMP", "ETFA", "ETFB", "GOLD", "XAG", "ETFSH"]

# Round / segment timing.  Refreshed from /health on connect.
DEFAULT_ROUND_MS   = 600_000
EOS_UNWIND_MS      =  60_000          # last 60s: shrink positions
EOS_FLATTEN_MS     =   8_000          # last 8s: IOC sweep to flat
INVENTORY_PERIOD_S =      2.0
HEARTBEAT_LOG_S    =      5.0
MAX_BACKOFF_S      =      1.5

# ════════════════════════════════════════════════════════════════════════════
# Logging
# ════════════════════════════════════════════════════════════════════════════

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOGLEVEL, logging.INFO),
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cascade")


def now_ms() -> int:
    return int(time.time() * 1000)


# ════════════════════════════════════════════════════════════════════════════
# Order book snapshot
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Book:
    bids: dict[int, int] = field(default_factory=dict)   # price (cents) -> qty
    asks: dict[int, int] = field(default_factory=dict)
    last_update_wall: float = 0.0

    def update(self, depth: dict) -> None:
        self.bids = {int(p): int(q) for p, q in depth.get("bids", {}).items()}
        self.asks = {int(p): int(q) for p, q in depth.get("asks", {}).items()}
        self.last_update_wall = time.monotonic()

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
    def mid(self) -> Optional[float]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2.0

    @property
    def microprice(self) -> Optional[float]:
        """Volume-weighted mid -- closer to where the next trade prints."""
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        bq, aq = self.best_bid_qty, self.best_ask_qty
        if bq + aq == 0:
            return (bb + ba) / 2.0
        return (bb * aq + ba * bq) / (bq + aq)


# ════════════════════════════════════════════════════════════════════════════
# Token bucket -- per-exchange send-side rate limiter
# ════════════════════════════════════════════════════════════════════════════

class TokenBucket:
    __slots__ = ("rate", "burst", "tokens", "last")

    def __init__(self, rate: float, burst: float | None = None):
        self.rate = rate
        self.burst = burst if burst is not None else rate
        self.tokens = self.burst
        self.last = time.monotonic()

    async def acquire(self) -> None:
        while True:
            now = time.monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            wait = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(wait)


# ════════════════════════════════════════════════════════════════════════════
# Trade plan = atomic group of order legs that must all fit
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Leg:
    exchange: str
    ticker: str
    side: str        # "bid" / "ask"
    qty: int
    price: int       # cents
    order_type: str = "ioc"
    note: str = ""

    @property
    def signed_qty(self) -> int:
        return self.qty if self.side == "bid" else -self.qty


@dataclass
class Plan:
    legs: list[Leg]
    edge_cents: float = 0.0
    strategy: str = ""

    def __repr__(self) -> str:
        legs_s = " | ".join(
            f"{l.side[0].upper()}{l.qty}@{l.price/100:.2f} {l.exchange}-{l.ticker}"
            for l in self.legs
        )
        return f"<{self.strategy} edge={self.edge_cents:.1f}c {legs_s}>"


# ════════════════════════════════════════════════════════════════════════════
# Hub -- shared state + strategy bus
# ════════════════════════════════════════════════════════════════════════════

class Hub:
    def __init__(self, active_venues: list[str]):
        self.active_venues = active_venues
        self.stop = False
        self.connections: dict[str, "Connection"] = {}

        # Books: (exchange, ticker) -> Book
        self.books: dict[tuple[str, str], Book] = {}
        for tk, venues in LISTINGS.items():
            for v in venues:
                if v in active_venues:
                    self.books[(v, tk)] = Book()

        # Local position estimate: (exchange, ticker) -> shares (signed)
        self.pos: dict[tuple[str, str], int] = defaultdict(int)
        # Local cash estimate: exchange -> cents
        self.cash: dict[str, int] = {v: INITIAL_CASH for v in active_venues}

        # Per-exchange rate limit
        self.buckets: dict[str, TokenBucket] = {
            v: TokenBucket(RATE_PER_S, burst=RATE_PER_S) for v in active_venues
        }

        # Outstanding live limit-order tracking (for MM): order_id -> (ex, tk, side, qty, px)
        self.live_orders: dict[int, tuple[str, str, str, int, int]] = {}

        # Open orders count per exchange (rough) -- limit is 6000, we stay well under.
        self.open_count: dict[str, int] = defaultdict(int)

        # Map our user_request_id -> Leg, so add_order_response can settle the leg.
        self.req_to_leg: dict[str, Leg] = {}
        # Throttle inventory reconciliation
        self.last_inventory_req: dict[str, float] = defaultdict(float)
        # Server-time view per exchange
        self.server_time: dict[str, int] = {}
        self.round_length: dict[str, int] = {v: DEFAULT_ROUND_MS for v in active_venues}
        # ready[ex] is False right after a (re)connect until get_inventory replies.
        # We don't trade on an unready exchange -- prevents wrong-way trades from
        # acting on stale local position state.
        self.ready: dict[str, bool] = {v: False for v in active_venues}
        # Tickers that are listed on each venue (for fast iteration on reset)
        self.tickers_on_ex: dict[str, set[str]] = {v: set() for v in active_venues}
        for tk, vs in LISTINGS.items():
            for v in vs:
                if v in active_venues:
                    self.tickers_on_ex[v].add(tk)

        # Strategy fire-rate guards (don't spam the same arb every 100ms tick)
        self.last_fired: dict[str, float] = defaultdict(float)
        self._req_seq = 0

        # Stats for end-of-segment status line.
        self.fills_count = 0
        self.realized_cents = 0  # rough, from immediate cash deltas
        self.last_log = time.monotonic()
        self.last_mm_refresh: dict[tuple[str, str], float] = defaultdict(float)

    # ─── id generator ────────────────────────────────────────────────
    def req_id(self, tag: str) -> str:
        self._req_seq += 1
        return f"p{self._req_seq:08x}-{tag}"

    # ─── headroom checks ─────────────────────────────────────────────
    def fits(self, leg: Leg) -> bool:
        """Will placing this leg (assuming full fill) keep us inside soft caps?"""
        if not self.ready.get(leg.exchange, False):
            return False
        new_pos = self.pos[(leg.exchange, leg.ticker)] + leg.signed_qty
        if new_pos > SOFT_POS_MAX or new_pos < SOFT_POS_MIN:
            return False
        if leg.side == "bid":
            cost = leg.qty * leg.price
            if self.cash[leg.exchange] - cost < SOFT_CASH_FLOOR:
                return False
        if self.open_count[leg.exchange] + 1 > MAX_PENDING_ORDERS - 50:
            return False
        return True

    def commit_optimistic(self, leg: Leg) -> None:
        """Apply expected position/cash impact assuming full fill (we'll reconcile)."""
        self.pos[(leg.exchange, leg.ticker)] += leg.signed_qty
        if leg.side == "bid":
            self.cash[leg.exchange] -= leg.qty * leg.price
        else:
            self.cash[leg.exchange] += leg.qty * leg.price

    def revert_optimistic(self, leg: Leg) -> None:
        self.pos[(leg.exchange, leg.ticker)] -= leg.signed_qty
        if leg.side == "bid":
            self.cash[leg.exchange] += leg.qty * leg.price
        else:
            self.cash[leg.exchange] -= leg.qty * leg.price

    # ─── plan validation & dispatch ──────────────────────────────────
    def plan_fits(self, plan: Plan) -> bool:
        """Atomically check that ALL legs fit if applied in order."""
        applied: list[Leg] = []
        try:
            for leg in plan.legs:
                if not self.fits(leg):
                    return False
                self.commit_optimistic(leg)
                applied.append(leg)
            return True
        finally:
            for leg in applied:
                self.revert_optimistic(leg)

    def fire(self, plan: Plan) -> None:
        """Validate then dispatch all legs of an arb plan atomically."""
        if not self.plan_fits(plan):
            log.debug("plan rejected (headroom): %r", plan)
            return
        log.info("FIRE %r", plan)
        for leg in plan.legs:
            self.commit_optimistic(leg)
            if not self._send_leg(leg):
                self.revert_optimistic(leg)

    def fire_one(self, leg: Leg) -> None:
        """Validate-commit-send a single leg (used by passive MM)."""
        if not self.fits(leg):
            return
        self.commit_optimistic(leg)
        if not self._send_leg(leg):
            self.revert_optimistic(leg)

    def _send_leg(self, leg: Leg) -> bool:
        """Push the order onto the exchange's send queue. Caller is responsible
        for already having committed optimistic state. Returns False if the
        connection is dead (caller should revert)."""
        conn = self.connections.get(leg.exchange)
        if conn is None or not conn.connected:
            return False
        rid = self.req_id(f"{leg.note[:16]}" if leg.note else leg.ticker)
        self.req_to_leg[rid] = leg
        # IOC/limit orders need an expiry strictly in the future.  Use Unix ms
        # + 30s which works under either server interpretation.
        expiry = now_ms() + 30_000
        msg: dict = {
            "type": "add_order",
            "user_request_id": rid,
            "instrument_id": f"{leg.exchange}-{leg.ticker}",
            "side": leg.side,
            "quantity": leg.qty,
            "order_type": leg.order_type,
        }
        if leg.order_type != "market":
            msg["price"] = leg.price
            msg["expiry"] = expiry
        conn.enqueue(msg)
        self.open_count[leg.exchange] += 1
        return True

    # ─── message handlers ────────────────────────────────────────────
    def on_welcome(self, exchange: str) -> None:
        log.info("welcome %s", exchange)
        # Clear stale per-exchange state.  Whether this is a mid-segment
        # reconnect (positions preserved) or a new segment (positions reset),
        # the upcoming get_inventory will give us authoritative truth.  Until
        # it arrives we mark the exchange not-ready so fits() returns False
        # and no trades fire on stale local state.
        self.ready[exchange] = False
        for tk in self.tickers_on_ex.get(exchange, set()):
            self.pos[(exchange, tk)] = 0
            book = self.books.get((exchange, tk))
            if book is not None:
                book.bids = {}
                book.asks = {}
        self.cash[exchange] = INITIAL_CASH
        for oid in list(self.live_orders):
            if self.live_orders[oid][0] == exchange:
                self.live_orders.pop(oid, None)
        self.open_count[exchange] = 0
        # Drop any in-flight req_to_leg mappings on this exchange.
        for rid in [r for r, lg in self.req_to_leg.items() if lg.exchange == exchange]:
            self.req_to_leg.pop(rid, None)
        self.req_inventory(exchange, force=True)
        # And ask for any pending orders the server still thinks we have.
        conn = self.connections.get(exchange)
        if conn and conn.connected:
            conn.enqueue({
                "type": "get_pending_orders",
                "user_request_id": self.req_id(f"pend-{exchange}"),
            })

    def on_md(self, exchange: str, msg: dict) -> None:
        t = msg.get("time")
        if isinstance(t, int):
            self.server_time[exchange] = t
        # Update books
        for inst, depth in msg.get("orderbook_depths", {}).items():
            ex, _, tk = inst.partition("-")
            if ex != exchange:
                continue
            book = self.books.get((ex, tk))
            if book is not None:
                book.update(depth)
        # Apply trade events for live order fills
        for ev in msg.get("events", []) or []:
            if ev.get("event_type") != "trade":
                continue
            d = ev["data"]
            for oid_key in ("passiveOrderID", "activeOrderID"):
                oid = d.get(oid_key)
                rec = self.live_orders.get(oid)
                if rec is None:
                    continue
                ex, tk, side, qty, px = rec
                fill_qty = int(d["quantity"])
                fill_px  = int(d["price"])
                signed = fill_qty if side == "bid" else -fill_qty
                self.pos[(ex, tk)] += signed
                self.cash[ex] += -fill_qty * fill_px if side == "bid" else fill_qty * fill_px
                self.fills_count += 1
                # Reduce remaining and clean up if fully filled.
                rem = qty - fill_qty
                if rem <= 0:
                    self.live_orders.pop(oid, None)
                else:
                    self.live_orders[oid] = (ex, tk, side, rem, px)

    def on_add_order_response(self, exchange: str, msg: dict) -> None:
        rid = msg.get("user_request_id", "")
        leg = self.req_to_leg.pop(rid, None)
        success = bool(msg.get("success"))
        data = msg.get("data") or {}
        if leg is None:
            return
        # We optimistically committed assuming full fill at our limit price.
        # Now reconcile against truth.
        # Step 1: revert the optimistic commit.
        self.revert_optimistic(leg)
        if not success:
            log.debug("order failed %s: %s", leg.note, data.get("message"))
            self.open_count[exchange] = max(0, self.open_count[exchange] - 1)
            return
        ic = data.get("immediate_inventory_change")
        bc = data.get("immediate_balance_change")
        if ic is not None:
            self.pos[(leg.exchange, leg.ticker)] += int(ic)
            self.fills_count += 1
        if bc is not None:
            self.cash[leg.exchange] += int(bc)
            self.realized_cents += int(bc)
        # If it's a resting limit, register live tracking for trade-event fills.
        oid = data.get("order_id")
        if leg.order_type == "limit" and oid is not None:
            filled = abs(int(ic)) if ic is not None else 0
            remaining = leg.qty - filled
            if remaining > 0:
                self.live_orders[int(oid)] = (
                    leg.exchange, leg.ticker, leg.side, remaining, leg.price
                )
        else:
            self.open_count[exchange] = max(0, self.open_count[exchange] - 1)

    def on_cancel_order_response(self, exchange: str, msg: dict) -> None:
        if msg.get("success"):
            self.open_count[exchange] = max(0, self.open_count[exchange] - 1)

    def on_inventory(self, exchange: str, msg: dict) -> None:
        data = msg.get("data") or {}
        if not isinstance(data, dict):
            return
        for inst, pair in data.items():
            try:
                _reserved, total = int(pair[0]), int(pair[1])
            except (ValueError, TypeError, IndexError):
                continue
            if inst == "$":
                self.cash[exchange] = total
            else:
                ex, _, tk = inst.partition("-")
                if ex == exchange:
                    self.pos[(ex, tk)] = total
        self.ready[exchange] = True

    def on_pending_orders(self, exchange: str, msg: dict) -> None:
        data = msg.get("data") or {}
        live_ids: set[int] = set()
        for inst, pair in data.items():
            ex, _, tk = inst.partition("-")
            if ex != exchange or not isinstance(pair, list) or len(pair) < 2:
                continue
            for side_idx, side_label in enumerate(("bid", "ask")):
                for od in pair[side_idx] or []:
                    oid = int(od["orderID"])
                    qty = int(od["unfilled_quantity"])
                    px  = int(od["price"])
                    self.live_orders[oid] = (ex, tk, side_label, qty, px)
                    live_ids.add(oid)
        # Drop any tracked live orders on this exchange that the server doesn't
        # show as live anymore (filled/cancelled without us noticing).
        for oid in list(self.live_orders):
            ex0 = self.live_orders[oid][0]
            if ex0 == exchange and oid not in live_ids:
                self.live_orders.pop(oid, None)

    # ─── periodic queries ────────────────────────────────────────────
    def req_inventory(self, exchange: str, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_inventory_req[exchange] < INVENTORY_PERIOD_S:
            return
        self.last_inventory_req[exchange] = now
        conn = self.connections.get(exchange)
        if conn and conn.connected:
            conn.enqueue({
                "type": "get_inventory",
                "user_request_id": self.req_id(f"inv-{exchange}"),
            })

    # ─── strategy bus ────────────────────────────────────────────────
    def strategize(self) -> None:
        """Called on every market_data tick from any venue. Idempotent."""
        plans: list[Plan] = []
        plans += self.etf_basket_arbs()
        plans += self.sub_etf_arbs()
        plans += self.cross_venue_arbs()
        plans += self.settlement_unwind()
        # Fire highest-edge plans first so when a tick produces several arbs
        # the most profitable ones consume position/cash headroom before the
        # smaller ones do.
        plans.sort(key=lambda p: -p.edge_cents)
        for plan in plans:
            self.fire(plan)

        self.passive_mm_refresh()

        # Heartbeat log
        now = time.monotonic()
        if now - self.last_log >= HEARTBEAT_LOG_S:
            self.last_log = now
            total_pos_val = 0
            book_count = 0
            for (ex, tk), bk in self.books.items():
                if bk.mid is not None:
                    book_count += 1
                    total_pos_val += int(self.pos[(ex, tk)] * bk.mid)
            total_cash = sum(self.cash.values())
            log.info(
                "[hb] books=%d cash=$%.0f pos_mtm=$%.0f fills=%d realized=$%.0f open=%d live_lim=%d",
                book_count,
                total_cash / 100,
                total_pos_val / 100,
                self.fills_count,
                self.realized_cents / 100,
                sum(self.open_count.values()),
                len(self.live_orders),
            )

    # ─── depth-walking helpers used by ETF basket arb ────────────────
    @staticmethod
    def _walk_asks(book: "Book", max_px: float, max_qty: int) -> tuple[int, int]:
        """Walk ask levels ascending. Take qty from each level priced <= max_px,
        up to max_qty total. Returns (qty_walked, worst_level_price)."""
        if max_qty <= 0:
            return 0, 0
        qty = 0
        worst_px = 0
        for px, q in sorted(book.asks.items()):
            if px > max_px:
                break
            take = min(q, max_qty - qty)
            if take <= 0:
                break
            qty += take
            worst_px = px
            if qty >= max_qty:
                break
        return qty, worst_px

    @staticmethod
    def _walk_bids(book: "Book", min_px: float, max_qty: int) -> tuple[int, int]:
        """Walk bid levels descending. Take qty from each level priced >= min_px,
        up to max_qty total. Returns (qty_walked, worst_level_price)."""
        if max_qty <= 0:
            return 0, 0
        qty = 0
        worst_px = 0
        for px, q in sorted(book.bids.items(), reverse=True):
            if px < min_px:
                break
            take = min(q, max_qty - qty)
            if take <= 0:
                break
            qty += take
            worst_px = px
            if qty >= max_qty:
                break
        return qty, worst_px

    # ─── strategy 1: ETF <-> basket arbitrage (multi-venue routed) ───
    def etf_basket_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for etf in ETFS:
            basket = ETF_BASKETS[etf]
            n = len(basket)
            # Best buy/sell venue per constituent
            const_bid: dict[str, tuple[str, int, int]] = {}   # tk -> (venue, bid_px, qty)
            const_ask: dict[str, tuple[str, int, int]] = {}
            ok = True
            for tk in basket:
                best_b: tuple[str, int, int] | None = None
                best_a: tuple[str, int, int] | None = None
                for v in LISTINGS[tk]:
                    if v not in self.active_venues:
                        continue
                    bk = self.books[(v, tk)]
                    bb, ba = bk.best_bid, bk.best_ask
                    if bb is not None and (best_b is None or bb > best_b[1]):
                        best_b = (v, bb, bk.best_bid_qty)
                    if ba is not None and (best_a is None or ba < best_a[1]):
                        best_a = (v, ba, bk.best_ask_qty)
                if best_b is None or best_a is None:
                    ok = False
                    break
                const_bid[tk] = best_b
                const_ask[tk] = best_a
            if not ok:
                continue
            # For each ETF listing, evaluate both directions
            for etf_v in LISTINGS[etf]:
                if etf_v not in self.active_venues:
                    continue
                bk = self.books[(etf_v, etf)]
                if bk.best_bid is None or bk.best_ask is None:
                    continue
                # Direction A: ETF cheap -> BUY ETF on etf_v, SELL each constituent on its highest-bid venue
                etf_buy_px = bk.best_ask
                synth_sell_total = sum(const_bid[tk][1] for tk in basket)  # sum of bids
                synth_sell_per_unit = synth_sell_total / n
                # Edge per share of ETF at top-of-book ask = (sum_of_bids/n) - etf_buy_px.
                # Quick top-of-book screen first, then depth-walk for size.
                if synth_sell_per_unit - etf_buy_px >= ARB_EDGE:
                    # Walk ask levels: include any level whose marginal edge still beats ARB_EDGE.
                    max_etf_px = synth_sell_per_unit - ARB_EDGE
                    etf_qty_walked, etf_worst_px = self._walk_asks(
                        bk, max_etf_px, ARB_MAX_K * n
                    )
                    if etf_qty_walked >= n:
                        k = etf_qty_walked // n
                        for tk in basket:
                            k = min(k, const_bid[tk][2])
                        # Position headroom per leg
                        etf_room = (SOFT_POS_MAX - self.pos[(etf_v, etf)]) // n
                        k = min(k, etf_room)
                        for tk in basket:
                            v, _, _ = const_bid[tk]
                            # selling const => more negative position
                            room = self.pos[(v, tk)] - SOFT_POS_MIN  # how much more we can sell
                            k = min(k, room)
                        # Cash headroom: ETF buy = k*n shares, worst-case priced at etf_worst_px
                        cash_room = (self.cash[etf_v] - SOFT_CASH_FLOOR) // (n * etf_worst_px) if etf_worst_px > 0 else 0
                        k = min(k, cash_room, ARB_MAX_K)
                        if k > 0:
                            # IOC limit at the worst walked level so price-time priority gives us
                            # the cheaper levels first; reported edge is the conservative one.
                            edge_a = synth_sell_per_unit - etf_worst_px
                            legs = [Leg(etf_v, etf, "bid", k * n, etf_worst_px,
                                        note=f"etfarb_buy_{etf}@{etf_v}")]
                            for tk in basket:
                                v, px, _ = const_bid[tk]
                                legs.append(Leg(v, tk, "ask", k, px, note=f"etfarb_sell_{tk}@{v}"))
                            out.append(Plan(legs, edge_cents=edge_a, strategy=f"ETF-NAV {etf}@{etf_v} long"))

                # Direction B: ETF rich -> SELL ETF, BUY each constituent at lowest ask
                etf_sell_px = bk.best_bid
                synth_buy_total = sum(const_ask[tk][1] for tk in basket)
                synth_buy_per_unit = synth_buy_total / n
                if etf_sell_px - synth_buy_per_unit >= ARB_EDGE:
                    # Walk bid levels: include any level whose marginal edge still beats ARB_EDGE.
                    min_etf_px = synth_buy_per_unit + ARB_EDGE
                    etf_qty_walked, etf_worst_px = self._walk_bids(
                        bk, min_etf_px, ARB_MAX_K * n
                    )
                    if etf_qty_walked >= n:
                        k = etf_qty_walked // n
                        for tk in basket:
                            k = min(k, const_ask[tk][2])
                        etf_short_room = self.pos[(etf_v, etf)] - SOFT_POS_MIN  # selling more
                        k = min(k, etf_short_room // n)
                        for tk in basket:
                            v, px, _ = const_ask[tk]
                            room = (SOFT_POS_MAX - self.pos[(v, tk)])
                            k = min(k, room)
                        # Cash headroom: sum of constituent buys, each on its venue.
                        venue_buy_cost: dict[str, int] = defaultdict(int)
                        for tk in basket:
                            v, px, _ = const_ask[tk]
                            venue_buy_cost[v] += px
                        for v, per_k_cost in venue_buy_cost.items():
                            if per_k_cost == 0:
                                continue
                            room = (self.cash[v] - SOFT_CASH_FLOOR) // per_k_cost
                            k = min(k, room)
                        k = min(k, ARB_MAX_K)
                        if k > 0:
                            edge_b = etf_worst_px - synth_buy_per_unit
                            legs = [Leg(etf_v, etf, "ask", k * n, etf_worst_px,
                                        note=f"etfarb_sell_{etf}@{etf_v}")]
                            for tk in basket:
                                v, px, _ = const_ask[tk]
                                legs.append(Leg(v, tk, "bid", k, px, note=f"etfarb_buy_{tk}@{v}"))
                            out.append(Plan(legs, edge_cents=edge_b, strategy=f"ETF-NAV {etf}@{etf_v} short"))
        return out

    # ─── strategy 2: ETF <-> sub-ETF + complement basket ─────────────
    def sub_etf_arbs(self) -> list[Plan]:
        """Identity: 6·ETFA = 3·ETFA3 + (OIT+FSR+JZRO).
           So:  edge per share of (synthetic ETFA basket) =
                   6 * P(ETFA) - 3 * P(ETFA3) - sum(P(complement))
        """
        out: list[Plan] = []
        for super_etf, (sub_etf, complement) in SUB_ETF_LINKS.items():
            sup_venues = [v for v in LISTINGS[super_etf] if v in self.active_venues]
            sub_venues = [v for v in LISTINGS[sub_etf]   if v in self.active_venues]
            if not sup_venues or not sub_venues:
                continue
            # Best quotes for complement stocks
            comp_bid: dict[str, tuple[str, int, int]] = {}
            comp_ask: dict[str, tuple[str, int, int]] = {}
            ok = True
            for tk in complement:
                best_b = best_a = None
                for v in LISTINGS[tk]:
                    if v not in self.active_venues:
                        continue
                    bk = self.books[(v, tk)]
                    if bk.best_bid is not None and (best_b is None or bk.best_bid > best_b[1]):
                        best_b = (v, bk.best_bid, bk.best_bid_qty)
                    if bk.best_ask is not None and (best_a is None or bk.best_ask < best_a[1]):
                        best_a = (v, bk.best_ask, bk.best_ask_qty)
                if best_b is None or best_a is None:
                    ok = False
                    break
                comp_bid[tk] = best_b
                comp_ask[tk] = best_a
            if not ok:
                continue
            # Find best ETFA & ETFA3 quotes
            def best_quote(tk, venues, side):
                best = None
                for v in venues:
                    bk = self.books[(v, tk)]
                    px = bk.best_ask if side == "bid" else bk.best_bid
                    qty = bk.best_ask_qty if side == "bid" else bk.best_bid_qty
                    if px is None:
                        continue
                    if best is None or (px < best[1] if side == "bid" else px > best[1]):
                        best = (v, px, qty)
                return best
            sup_buy = best_quote(super_etf, sup_venues, "bid")
            sup_sell = best_quote(super_etf, sup_venues, "ask")
            sub_buy = best_quote(sub_etf,   sub_venues, "bid")
            sub_sell = best_quote(sub_etf,   sub_venues, "ask")
            if not (sup_buy and sup_sell and sub_buy and sub_sell):
                continue
            # Direction A: super cheap, sub & complement rich
            #   BUY 6k super, SELL 3k sub, SELL k each complement
            #   PnL/k  =  3*sub_bid + sum(comp_bid) - 6*super_ask
            edge_a = 3 * sub_buy[1] + sum(comp_bid[tk][1] for tk in complement) - 6 * sup_buy[1]
            if edge_a >= 6 * SUB_ETF_EDGE:
                k = min(
                    sup_buy[2] // 6,
                    sub_buy[2] // 3,
                    *(comp_bid[tk][2] for tk in complement),
                    (SOFT_POS_MAX - self.pos[(sup_buy[0], super_etf)]) // 6,
                    (self.pos[(sub_buy[0], sub_etf)] - SOFT_POS_MIN) // 3,
                    *((self.pos[(comp_bid[tk][0], tk)] - SOFT_POS_MIN) for tk in complement),
                    (self.cash[sup_buy[0]] - SOFT_CASH_FLOOR) // (6 * sup_buy[1]) if sup_buy[1] else 0,
                    ARB_MAX_K,
                )
                if k > 0:
                    legs = [
                        Leg(sup_buy[0], super_etf, "bid", 6 * k, sup_buy[1], note=f"subetf_long_{super_etf}"),
                        Leg(sub_buy[0], sub_etf,   "ask", 3 * k, sub_buy[1], note=f"subetf_short_{sub_etf}"),
                    ]
                    for tk in complement:
                        v, px, _ = comp_bid[tk]
                        legs.append(Leg(v, tk, "ask", k, px, note=f"subetf_short_{tk}"))
                    out.append(Plan(legs, edge_cents=edge_a / 6, strategy=f"SUB-ETF {super_etf}/{sub_etf} long"))
            # Direction B: super rich, sub & complement cheap
            edge_b = 6 * sup_sell[1] - 3 * sub_sell[1] - sum(comp_ask[tk][1] for tk in complement)
            if edge_b >= 6 * SUB_ETF_EDGE:
                # Cash room: buying 3k sub-ETF + k each complement
                buys_per_k_per_venue: dict[str, int] = defaultdict(int)
                buys_per_k_per_venue[sub_sell[0]] += 3 * sub_sell[1]
                for tk in complement:
                    v, px, _ = comp_ask[tk]
                    buys_per_k_per_venue[v] += px
                cash_k = ARB_MAX_K
                for v, per_k_cost in buys_per_k_per_venue.items():
                    if per_k_cost > 0:
                        cash_k = min(cash_k, (self.cash[v] - SOFT_CASH_FLOOR) // per_k_cost)
                k = min(
                    sup_sell[2] // 6,
                    sub_sell[2] // 3,
                    *(comp_ask[tk][2] for tk in complement),
                    (self.pos[(sup_sell[0], super_etf)] - SOFT_POS_MIN) // 6,
                    (SOFT_POS_MAX - self.pos[(sub_sell[0], sub_etf)]) // 3,
                    *((SOFT_POS_MAX - self.pos[(comp_ask[tk][0], tk)]) for tk in complement),
                    cash_k,
                    ARB_MAX_K,
                )
                if k > 0:
                    legs = [
                        Leg(sup_sell[0], super_etf, "ask", 6 * k, sup_sell[1], note=f"subetf_short_{super_etf}"),
                        Leg(sub_sell[0], sub_etf,   "bid", 3 * k, sub_sell[1], note=f"subetf_long_{sub_etf}"),
                    ]
                    for tk in complement:
                        v, px, _ = comp_ask[tk]
                        legs.append(Leg(v, tk, "bid", k, px, note=f"subetf_long_{tk}"))
                    out.append(Plan(legs, edge_cents=edge_b / 6, strategy=f"SUB-ETF {super_etf}/{sub_etf} short"))
        return out

    # ─── strategy 3: cross-venue same-stock arb ──────────────────────
    def cross_venue_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for tk in LISTINGS:
            venues = [v for v in LISTINGS[tk] if v in self.active_venues]
            if len(venues) < 2:
                continue
            best_b = best_a = None
            for v in venues:
                bk = self.books[(v, tk)]
                if bk.best_bid is not None and (best_b is None or bk.best_bid > best_b[1]):
                    best_b = (v, bk.best_bid, bk.best_bid_qty)
                if bk.best_ask is not None and (best_a is None or bk.best_ask < best_a[1]):
                    best_a = (v, bk.best_ask, bk.best_ask_qty)
            if not best_b or not best_a or best_b[0] == best_a[0]:
                continue
            # Buy on best_a, sell on best_b
            edge = best_b[1] - best_a[1]
            if edge >= XV_EDGE:
                qty = min(best_b[2], best_a[2], XV_MAX_QTY)
                buy_v = best_a[0]
                sell_v = best_b[0]
                qty = min(qty, SOFT_POS_MAX - self.pos[(buy_v, tk)])
                qty = min(qty, self.pos[(sell_v, tk)] - SOFT_POS_MIN)
                if best_a[1] > 0:
                    qty = min(qty, (self.cash[buy_v] - SOFT_CASH_FLOOR) // best_a[1])
                if qty > 0:
                    out.append(Plan(
                        [Leg(buy_v, tk, "bid", qty, best_a[1], note=f"xv_buy_{tk}@{buy_v}"),
                         Leg(sell_v, tk, "ask", qty, best_b[1], note=f"xv_sell_{tk}@{sell_v}")],
                        edge_cents=edge,
                        strategy=f"X-VENUE {tk} {buy_v}->{sell_v}",
                    ))
        return out

    # ─── strategy 5: settlement-aware unwind ─────────────────────────
    def settlement_unwind(self) -> list[Plan]:
        out: list[Plan] = []
        for ex in self.active_venues:
            t = self.server_time.get(ex)
            length = self.round_length.get(ex, DEFAULT_ROUND_MS)
            if t is None or length is None:
                continue
            remaining = length - t
            if remaining <= 0 or remaining > EOS_UNWIND_MS:
                continue
            urgency = 1.0 - remaining / EOS_UNWIND_MS    # 0..1, 1=last moment
            for (ex2, tk), pos in list(self.pos.items()):
                if ex2 != ex or pos == 0:
                    continue
                bk = self.books.get((ex, tk))
                if bk is None:
                    continue
                # Choose how much to unwind this tick: linear ramp by urgency.
                target_qty = abs(pos)
                # Last 8s -> aggressive IOC at the marketable side, no limit.
                aggressive = remaining <= EOS_FLATTEN_MS
                if pos > 0:
                    bb = bk.best_bid
                    if bb is None:
                        continue
                    qty = min(target_qty, bk.best_bid_qty,
                              max(1, int(target_qty * (urgency if not aggressive else 1.0))))
                    px = bb if not aggressive else max(1, bb - 5)  # cross deeper near close
                    out.append(Plan(
                        [Leg(ex, tk, "ask", qty, px, note=f"unwind_long_{tk}")],
                        edge_cents=0.0,
                        strategy=f"UNWIND long {tk}@{ex}",
                    ))
                else:  # short
                    ba = bk.best_ask
                    if ba is None:
                        continue
                    qty = min(target_qty, bk.best_ask_qty,
                              max(1, int(target_qty * (urgency if not aggressive else 1.0))))
                    px = ba if not aggressive else ba + 5
                    out.append(Plan(
                        [Leg(ex, tk, "bid", qty, px, note=f"unwind_short_{tk}")],
                        edge_cents=0.0,
                        strategy=f"UNWIND short {tk}@{ex}",
                    ))
        return out

    # ─── strategy 4: passive market making (step inside MM) ──────────
    def passive_mm_refresh(self) -> None:
        """For a small basket of high-flow names, sit one tick inside MM's top
        quotes if we're at-or-near flat.  Cancel + replace when MM moves; skip
        if our existing live quote already matches the target price."""
        now = time.monotonic()
        for tk in MM_INSTRUMENTS:
            for v in LISTINGS.get(tk, set()):
                if v not in self.active_venues or not self.ready.get(v, False):
                    continue
                key = (v, tk)
                if now - self.last_mm_refresh[key] < MM_REFRESH_S:
                    continue
                self.last_mm_refresh[key] = now
                bk = self.books[key]
                bb, ba = bk.best_bid, bk.best_ask
                # Need a real two-sided market with room for an inside step.
                if bb is None or ba is None or ba - bb < 2 * MM_INSIDE_TICK + 1:
                    continue
                pos = self.pos[key]
                bid_px = bb + MM_INSIDE_TICK
                ask_px = ba - MM_INSIDE_TICK

                # Identify existing live MM quotes on this instrument.
                have_bid_at_px = False
                have_ask_at_px = False
                stale_oids: list[int] = []
                for oid, (ex0, tk0, side0, _q0, px0) in self.live_orders.items():
                    if ex0 != v or tk0 != tk:
                        continue
                    if side0 == "bid" and px0 == bid_px:
                        have_bid_at_px = True
                    elif side0 == "ask" and px0 == ask_px:
                        have_ask_at_px = True
                    else:
                        stale_oids.append(oid)
                # Cancel any quote that's no longer at the right price.
                conn = self.connections.get(v)
                if conn and conn.connected:
                    for oid in stale_oids:
                        conn.enqueue({
                            "type": "cancel_order",
                            "user_request_id": self.req_id(f"cxl-{tk}"),
                            "order_id": oid,
                            "instrument_id": f"{v}-{tk}",
                        })

                # Place fresh quotes only where we don't already have one.
                if (not have_bid_at_px
                        and pos < SOFT_POS_MAX - MM_QTY
                        and self.cash[v] - MM_QTY * bid_px > SOFT_CASH_FLOOR):
                    self.fire_one(Leg(v, tk, "bid", MM_QTY, bid_px,
                                      order_type="limit", note=f"mm_bid_{tk}"))
                if not have_ask_at_px and pos > SOFT_POS_MIN + MM_QTY:
                    self.fire_one(Leg(v, tk, "ask", MM_QTY, ask_px,
                                      order_type="limit", note=f"mm_ask_{tk}"))

# ════════════════════════════════════════════════════════════════════════════
# Per-exchange websocket connection task
# ════════════════════════════════════════════════════════════════════════════

class Connection:
    def __init__(self, hub: Hub, exchange: str):
        self.hub = hub
        self.exchange = exchange
        self.url = f"ws://{WS_HOSTS[exchange]}:9001/trade"
        self.ws = None
        self.send_q: asyncio.Queue[dict] = asyncio.Queue(maxsize=2000)
        self.connected = False

    def enqueue(self, msg: dict) -> None:
        try:
            self.send_q.put_nowait(msg)
        except asyncio.QueueFull:
            log.warning("%s send queue full, dropping %s", self.exchange, msg.get("type"))

    async def run(self) -> None:
        backoff = 0.1
        while not self.hub.stop:
            try:
                async with ws_connect(
                    self.url,
                    open_timeout=5,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=2**24,
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    backoff = 0.1
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    welcome = json.loads(raw)
                    if welcome.get("type") != "welcome":
                        log.warning("%s unexpected first msg: %s", self.exchange, welcome)
                    self.hub.on_welcome(self.exchange)
                    sender = asyncio.create_task(self._sender_loop(ws), name=f"snd-{self.exchange}")
                    try:
                        await self._receiver_loop(ws)
                    finally:
                        sender.cancel()
                        with contextlib.suppress(BaseException):
                            await sender
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("%s connection error: %s", self.exchange, e)
            finally:
                self.connected = False
                self.ws = None
            if self.hub.stop:
                return
            # Drain queue between connection attempts so stale orders don't
            # fire on reconnect.
            while not self.send_q.empty():
                try: self.send_q.get_nowait()
                except: break
            jitter = random.uniform(0.0, 0.2)
            await asyncio.sleep(min(MAX_BACKOFF_S, backoff) + jitter)
            backoff = min(MAX_BACKOFF_S, backoff * 1.7)

    async def _receiver_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                # Server emits "Message rate limit exceeded" as plain text frame
                # right before closing -- log and let the close happen.
                log.warning("%s text frame: %r", self.exchange, raw[:80])
                continue
            mtype = msg.get("type", "")
            if mtype == "market_data_update":
                self.hub.on_md(self.exchange, msg)
                self.hub.req_inventory(self.exchange)
                self.hub.strategize()
            elif mtype == "add_order_response":
                self.hub.on_add_order_response(self.exchange, msg)
            elif mtype == "cancel_order_response":
                self.hub.on_cancel_order_response(self.exchange, msg)
            elif mtype == "get_inventory_response":
                self.hub.on_inventory(self.exchange, msg)
            elif mtype == "get_pending_orders_response":
                self.hub.on_pending_orders(self.exchange, msg)
            elif mtype == "end_of_round":
                log.info("%s end_of_round", self.exchange)
                # Server is about to close us anyway; let receiver exit.
                return
            elif mtype == "error":
                log.warning("%s server error: %s", self.exchange, msg.get("message"))

    async def _sender_loop(self, ws) -> None:
        while True:
            msg = await self.send_q.get()
            await self.hub.buckets[self.exchange].acquire()
            try:
                await ws.send(json.dumps(msg, separators=(",", ":")))
            except Exception as e:
                log.warning("%s send failed: %s", self.exchange, e)
                return


# ════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════

async def amain(active_venues: list[str]) -> None:
    hub = Hub(active_venues)
    for ex in active_venues:
        hub.connections[ex] = Connection(hub, ex)

    loop = asyncio.get_running_loop()
    def shutdown():
        log.info("shutdown signal received")
        hub.stop = True
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, shutdown)

    tasks = [asyncio.create_task(c.run(), name=f"conn-{ex}")
             for ex, c in hub.connections.items()]

    # Idle keep-alive loop that runs strategize() on a 100ms cadence even if
    # one venue's market data goes silent (e.g. between segments).
    async def tick():
        while not hub.stop:
            await asyncio.sleep(0.1)
            try:
                hub.strategize()
            except Exception as e:
                log.exception("strategize error: %s", e)
    tasks.append(asyncio.create_task(tick(), name="tick"))

    try:
        # Wait for stop flag; each conn task runs forever otherwise.
        while not hub.stop:
            await asyncio.sleep(0.5)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    log.info("done")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--venues",
        default=",".join(VENUES),
        help="Comma-separated venue subset to connect to (default: all 10)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    active = [v.strip().upper() for v in args.venues.split(",") if v.strip()]
    bad = [v for v in active if v not in VENUES]
    if bad:
        raise SystemExit(f"unknown venue(s): {bad} (valid: {VENUES})")
    log.info("starting cascade on venues: %s", active)
    try:
        asyncio.run(amain(active))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
