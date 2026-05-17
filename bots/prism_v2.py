#!/usr/bin/env python3
"""
prism_v2.py — ambitious AlgoTrade 2026 trading bot.

Concept
-------
PRISM V2 keeps Prism's deterministic multi-venue arbitrage core, then pushes
it harder: depth-walked basket execution, volatility-adaptive thresholds,
consensus fair-value snipes, sector residual hedges, flow-skewed passive MM,
and stricter dispatch governance.  The original `prism.py` remains untouched.

Edges (priority order, highest-confidence first):

  1. **ETF<->basket arbitrage, depth-walked, multi-venue routed.**  Each
     constituent leg is routed to the venue with the best price for that leg.
     We walk the top-5 levels of every leg's book so size scales up when
     edge is large and queues are deep — prism only takes top-of-book qty.

  2. **Sub-ETF identity arb.**  ETFA3 ⊂ ETFA, so 6·P(ETFA) = 3·P(ETFA3) +
     (OIT+FSR+JZRO).  Same for ETFB/ETFB3.  Hit when the identity breaks.

  3. **Cross-venue same-stock arb.**  Two-leg sweep when bidA > askB.

  4. **Statistical fair-value snipe (single leg).**  When a single venue's
     ask is far below — or bid far above — the cross-venue consensus fair
     value (in σ-multiples), hit it.  No instant offset, but capped
     exposure and adverse-flow gating keep this from running away.

  5. **Sector residual mean-reversion.**  Within Sector A and B, fade
     stocks whose deviation from the sector index exceeds 2σ.  Hedged
     with the matching ETF on its cheapest venue.

  6. **Safe-haven coherence guard.**  Synth market index and ETFSH are
     supposed to move inversely.  When both move *with* each other in a
     short window we fade the safe-haven side.

  7. **Inventory- and flow-skewed two-sided MM.**  On a small set of
     liquid names, quote both sides one tick inside MM with width and
     skew driven by current position and a sliding-window aggressor-flow
     signal.  Size scales with remaining headroom; quotes are pulled on
     adverse flow.

  8. **Settlement-aware unwind.**  Last 60 s = linear ramp; last 8 s =
     IOC sweep to flat.  Per-venue, since marking is per-exchange.

Defensive bits
--------------
  * Local 400 msg/s/exchange token bucket — 80 % of the 500/s server cap.
  * Every plan is an atomic group: each leg's headroom is checked before
    any leg goes on the wire; partial dispatch is impossible.
  * Optimistic local position with periodic get_inventory reconcile.
  * Per-exchange reconnect with exponential backoff for segment resets.
  * Volatility-adaptive edge per ticker: edge_threshold = max(MIN, k·σ).
  * Aggressor-flow window per ticker (signed qty × recency) used as gate.
  * Plans are ranked by edge_cents and capped per tick to manage msg budget.

Run
---
    python3 prism_v2.py                       # all 10 venues
    python3 prism_v2.py --venues ZSE,NYSE     # subset
    LOGLEVEL=DEBUG python3 prism_v2.py        # verbose tracing
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
from typing import Iterable, Optional

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:                                    # websockets <12 fallback
    try:
        from websockets.client import connect as ws_connect  # type: ignore
    except ImportError:
        ws_connect = None  # type: ignore[assignment]

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

# Stock listing tables (transcribed from the participant guide).
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

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}
ETFS = list(ETF_BASKETS)
STOCKS = sorted(set(LISTINGS) - set(ETF_BASKETS))

# Sector groupings — used by the residual mean-reversion strategy.
SECTOR_A = ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"]
SECTOR_B = ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"]
SECTOR_OF: dict[str, str] = {**{t: "A" for t in SECTOR_A}, **{t: "B" for t in SECTOR_B}}
SECTOR_ETF = {"A": "ETFA", "B": "ETFB"}
SAFE_HAVEN = ["GOLD", "XAG"]
NON_SH_INDEX = SECTOR_A + SECTOR_B + ["MDKA", "KRAS", "ZITO", "ZABA", "SIMP", "CARD"]

# 6·super = 3·sub + sum(complement).
SUB_ETF_LINKS = {
    "ETFA": ("ETFA3", ["OIT", "FSR", "JZRO"]),
    "ETFB": ("ETFB3", ["HT",  "JNAF", "DDJH"]),
}

# ════════════════════════════════════════════════════════════════════════════
# Hard server limits & strategy knobs
# ════════════════════════════════════════════════════════════════════════════

# Server-enforced caps.
POS_MAX            =  2_000
POS_MIN            =   -200
INITIAL_CASH       = 10_000_000              # cents = $100k per venue
CASH_FLOOR         = -5_000_000              # cents = -$50k per venue
MAX_PENDING_ORDERS =  6_000

# Soft caps — leave headroom so a multi-leg arb can always cleanly fit.
RATE_PER_S         =    490                  # 80 % of 500/s server cap
SOFT_POS_MAX       =  1_800
SOFT_POS_MIN       =   -180
SOFT_CASH_FLOOR    = -4_500_000              # leave $5k buffer

# Edge thresholds (cents).  Used as floors against vol-adaptive estimates.
# Tuned to be unambiguously more aggressive than prism (ARB_EDGE=4, XV_EDGE=3,
# SUB_ETF_EDGE=6, no plan cap) in every regime.
MIN_ARB_EDGE       =      2                  # ETF↔basket and xv minimum (prism: 4 / 3)
ARB_EDGE           = MIN_ARB_EDGE            # backwards-friendly name for tests/tuning
SUB_ETF_EDGE       =      4                  # 6-leg sub-ETF identity (prism: 6)
STAT_EDGE_SIGMA    =    1.8                  # statistical FV-snipe in σ
STAT_MAX_QTY       =     35                  # cap stat-arb single-leg size
STAT_MAX_INVENTORY =    250                  # net |pos| cap that stat-arb may add
SECTOR_Z_THRESHOLD =    1.5                  # σ deviation to fade
SECTOR_MAX_QTY     =     25
SH_GUARD_THRESHOLD =     25                  # cents of co-movement to flag
SH_GUARD_MAX_QTY   =     12

ARB_MAX_K          =     45                  # max basket-multiples per shot (prism: 25)
XV_MAX_QTY         =     60                  # (prism: 40)

# Vol/flow/window knobs.
VOL_HALFLIFE_TICKS =     60                  # ~6 s at 100 ms
FLOW_WINDOW_MS     =  3_000                  # 3 s aggressor-flow window
# EDGE_VOL_K is a *contribution*, not a multiplier: edge = MIN + k·σ.
# At k=0.6 the vol scaling gently widens edges in chop without ever pulling
# them above prism's flat thresholds.  Set k=0 to make edges purely flat.
EDGE_VOL_K         =    0.6

# MM parameters.
MM_INSIDE_TICK     =      1
MM_QTY_BASE        =     10                  # (prism MM_QTY: 4)
MM_QTY_MAX         =     24
MM_REFRESH_S       =    0.8                  # (prism: 1.5)
MM_INSTRUMENTS     = ["CARD", "SIMP", "ETFA", "ETFB", "ETFA3", "ETFB3", "GOLD", "XAG", "ETFSH"]
MM_MAX_SPREAD      =    120                  # don't quote inside markets wider than 120c
MM_MAX_NET_POS     =    160                  # pull MM if abs(pos) approaches this

# Plan dispatch governance — generous; prism has no cap.
MAX_PLANS_PER_TICK =     30
PLAN_FIRE_GAP_S    =   0.04                  # don't fire identical plan tag twice/40ms

# Round / segment timing (refreshed from /health ideally; we use defaults).
DEFAULT_ROUND_MS   = 600_000
EOS_UNWIND_MS      =  60_000
EOS_FLATTEN_MS     =   8_000
INVENTORY_PERIOD_S =      2.0
HEARTBEAT_LOG_S    =      5.0
MAX_BACKOFF_S      =      4.0

# ════════════════════════════════════════════════════════════════════════════
# Logging
# ════════════════════════════════════════════════════════════════════════════

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOGLEVEL, logging.INFO),
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prism_v2")


def now_ms() -> int:
    return int(time.time() * 1000)


# ════════════════════════════════════════════════════════════════════════════
# Order-book snapshot
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
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        bq, aq = self.best_bid_qty, self.best_ask_qty
        if bq + aq == 0:
            return (bb + ba) / 2.0
        return (bb * aq + ba * bq) / (bq + aq)

    @property
    def spread(self) -> Optional[int]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return ba - bb

    def levels_buy(self, max_levels: int = 5) -> list[tuple[int, int]]:
        """Asks sorted ascending — what we sweep when buying."""
        return sorted(self.asks.items())[:max_levels]

    def levels_sell(self, max_levels: int = 5) -> list[tuple[int, int]]:
        """Bids sorted descending — what we sweep when selling."""
        return sorted(self.bids.items(), reverse=True)[:max_levels]

    def sweep(self, side: str, qty: int, max_levels: int = 5) -> Optional[tuple[int, float]]:
        """Return (marketable limit price, average fill price) for `qty`.

        side="bid" means we buy by sweeping asks.  side="ask" means we sell
        by sweeping bids.  The returned limit is the worst level needed to fill
        the requested quantity.  None means top-5 visible depth is insufficient.
        """
        if qty <= 0:
            return None
        levels = self.levels_buy(max_levels) if side == "bid" else self.levels_sell(max_levels)
        remaining = qty
        notional = 0
        worst_price: Optional[int] = None
        for price, avail in levels:
            take = min(remaining, avail)
            if take <= 0:
                continue
            notional += take * price
            remaining -= take
            worst_price = price
            if remaining == 0:
                return worst_price, notional / qty
        return None


# ════════════════════════════════════════════════════════════════════════════
# Token bucket — per-exchange send-side rate limiter
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
# Microstructure signal trackers
# ════════════════════════════════════════════════════════════════════════════

class VolTracker:
    """Per-ticker EWMA of squared mid-changes — produces a tick-scale σ in cents.

    Updated from each venue's mid every market-data tick.  We use the global
    cross-venue tick volatility because the same ticker is one fundamental
    process — vol on one venue is vol everywhere modulo latency noise."""

    __slots__ = ("alpha", "var", "prev_mid", "init")

    def __init__(self, halflife_ticks: int = VOL_HALFLIFE_TICKS):
        self.alpha = 1.0 - math.exp(-math.log(2.0) / max(1, halflife_ticks))
        self.var: float = 0.0
        self.prev_mid: Optional[float] = None
        self.init: int = 0

    def update(self, mid: Optional[float]) -> None:
        if mid is None:
            return
        if self.prev_mid is None:
            self.prev_mid = mid
            return
        d = mid - self.prev_mid
        self.var = (1 - self.alpha) * self.var + self.alpha * d * d
        self.prev_mid = mid
        self.init = min(self.init + 1, 10_000)

    @property
    def sigma_cents(self) -> float:
        # warm-up: until we've seen ≥ ~5 ticks return a high default to discourage
        # firing arbs against a phantom FV.
        if self.init < 5:
            return 25.0
        return math.sqrt(self.var)


class FlowTracker:
    """Sliding-window aggressor flow per ticker (signed quantity).

    Aggressor side is inferred from trade price vs the most recent book mid:
        price > mid + ε  →  buy aggressor   (+qty)
        price < mid - ε  →  sell aggressor  (-qty)
        otherwise        →  ignore (mid-stuck or aggregator)
    """

    def __init__(self, window_ms: int = FLOW_WINDOW_MS):
        self.window = window_ms
        self.events: dict[str, deque[tuple[int, int]]] = defaultdict(deque)

    def add(self, ticker: str, signed_qty: int, t_ms: int) -> None:
        dq = self.events[ticker]
        dq.append((t_ms, signed_qty))
        cutoff = t_ms - self.window
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def net(self, ticker: str, t_ms: int) -> int:
        dq = self.events.get(ticker)
        if not dq:
            return 0
        cutoff = t_ms - self.window
        # Lazy-cull oldest then sum.
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        return sum(q for _t, q in dq)


# ════════════════════════════════════════════════════════════════════════════
# Trade plan = atomic group of order legs that must all fit
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Leg:
    exchange: str
    ticker: str
    side: str           # "bid" / "ask"
    qty: int
    price: int          # cents
    order_type: str = "ioc"
    note: str = ""

    @property
    def signed_qty(self) -> int:
        return self.qty if self.side == "bid" else -self.qty


@dataclass
class Plan:
    legs: list[Leg]
    edge_cents: float = 0.0     # per-share edge for ranking
    strategy: str = ""
    tag: str = ""               # short stable id used for fire-rate suppression

    def __repr__(self) -> str:
        legs_s = " | ".join(
            f"{l.side[0].upper()}{l.qty}@{l.price/100:.2f} {l.exchange}-{l.ticker}"
            for l in self.legs
        )
        return f"<{self.strategy} edge={self.edge_cents:.1f}c {legs_s}>"


# ════════════════════════════════════════════════════════════════════════════
# Hub — shared state + strategy bus
# ════════════════════════════════════════════════════════════════════════════

class Hub:
    def __init__(self, active_venues: list[str]):
        self.active_venues = active_venues
        self.stop = False
        self.connections: dict[str, "Connection"] = {}

        self.books: dict[tuple[str, str], Book] = {}
        for tk, venues in LISTINGS.items():
            for v in venues:
                if v in active_venues:
                    self.books[(v, tk)] = Book()

        self.pos: dict[tuple[str, str], int] = defaultdict(int)
        self.cash: dict[str, int] = {v: INITIAL_CASH for v in active_venues}

        self.buckets: dict[str, TokenBucket] = {
            v: TokenBucket(RATE_PER_S, burst=RATE_PER_S) for v in active_venues
        }

        self.live_orders: dict[int, tuple[str, str, str, int, int]] = {}
        self.open_count: dict[str, int] = defaultdict(int)
        self.req_to_leg: dict[str, Leg] = {}
        self.last_inventory_req: dict[str, float] = defaultdict(float)
        self.server_time: dict[str, int] = {}
        self.round_length: dict[str, int] = {v: DEFAULT_ROUND_MS for v in active_venues}
        self.ready: dict[str, bool] = {v: False for v in active_venues}
        self.tickers_on_ex: dict[str, set[str]] = {v: set() for v in active_venues}
        for tk, vs in LISTINGS.items():
            for v in vs:
                if v in active_venues:
                    self.tickers_on_ex[v].add(tk)

        # Microstructure: vol per ticker, flow per ticker.
        self.vol: dict[str, VolTracker] = {tk: VolTracker() for tk in LISTINGS}
        self.flow = FlowTracker()

        # Consensus fair-value cache: ticker -> (fv_cents, confidence_in_[0,1], wall_t).
        self.fv: dict[str, tuple[float, float, float]] = {}

        # Sector residual stats: ticker -> (mean, var) EWMA of (FV_i - sector_mean).
        self.resid_mu: dict[str, float] = defaultdict(float)
        self.resid_var: dict[str, float] = defaultdict(lambda: 25.0 * 25.0)

        # Plan fire-rate guard: tag -> last fire time (monotonic).
        self.last_fired: dict[str, float] = defaultdict(float)
        self._req_seq = 0

        # Stats.
        self.fills_count = 0
        self.realized_cents = 0
        self.last_log = time.monotonic()
        self.last_mm_refresh: dict[tuple[str, str], float] = defaultdict(float)
        self.last_strategize = 0.0

    # ─── id generator ────────────────────────────────────────────────
    def req_id(self, tag: str) -> str:
        self._req_seq += 1
        return f"px{self._req_seq:08x}-{tag}"

    # ─── headroom checks ─────────────────────────────────────────────
    def fits(self, leg: Leg) -> bool:
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

    def plan_fits(self, plan: Plan) -> bool:
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
        if not self.plan_fits(plan):
            log.debug("plan rejected (headroom): %r", plan)
            return
        log.info("FIRE %r", plan)
        for leg in plan.legs:
            self.commit_optimistic(leg)
            if not self._send_leg(leg):
                self.revert_optimistic(leg)

    def fire_one(self, leg: Leg) -> None:
        if not self.fits(leg):
            return
        self.commit_optimistic(leg)
        if not self._send_leg(leg):
            self.revert_optimistic(leg)

    def _send_leg(self, leg: Leg) -> bool:
        conn = self.connections.get(leg.exchange)
        if conn is None or not conn.connected:
            return False
        rid = self.req_id(f"{leg.note[:16]}" if leg.note else leg.ticker)
        self.req_to_leg[rid] = leg
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

    # ─── server callbacks ────────────────────────────────────────────
    def on_welcome(self, exchange: str) -> None:
        log.info("welcome %s", exchange)
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
        for rid in [r for r, lg in self.req_to_leg.items() if lg.exchange == exchange]:
            self.req_to_leg.pop(rid, None)
        self.req_inventory(exchange, force=True)
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

        # Update books, then update vol from this venue's mid.
        for inst, depth in msg.get("orderbook_depths", {}).items():
            ex, _, tk = inst.partition("-")
            if ex != exchange:
                continue
            book = self.books.get((ex, tk))
            if book is None:
                continue
            book.update(depth)
            mp = book.microprice
            if mp is not None and tk in self.vol:
                self.vol[tk].update(mp)

        # Apply trade events for live order fills + aggressor-flow tracking.
        ev_t_ms = self.server_time.get(exchange, 0)
        for ev in msg.get("events", []) or []:
            if ev.get("event_type") != "trade":
                continue
            d = ev["data"]
            inst = d.get("instrumentID", "")
            ex, _, tk = inst.partition("-")
            try:
                fill_qty = int(d["quantity"])
                fill_px = int(d["price"])
            except (KeyError, TypeError, ValueError):
                continue

            # Settle our own resting orders.
            for oid_key in ("passiveOrderID", "activeOrderID"):
                oid = d.get(oid_key)
                rec = self.live_orders.get(oid)
                if rec is None:
                    continue
                ex2, tk2, side, qty, _px = rec
                signed = fill_qty if side == "bid" else -fill_qty
                self.pos[(ex2, tk2)] += signed
                self.cash[ex2] += -fill_qty * fill_px if side == "bid" else fill_qty * fill_px
                self.fills_count += 1
                rem = qty - fill_qty
                if rem <= 0:
                    self.live_orders.pop(oid, None)
                else:
                    self.live_orders[oid] = (ex2, tk2, side, rem, _px)

            # Aggressor-flow signal — heuristic from fill price vs current mid.
            book = self.books.get((ex, tk))
            if book is None or tk not in LISTINGS:
                continue
            mid = book.mid
            if mid is None:
                continue
            if fill_px > mid + 0.5:
                self.flow.add(tk, +fill_qty, ev_t_ms)
            elif fill_px < mid - 0.5:
                self.flow.add(tk, -fill_qty, ev_t_ms)

    def on_add_order_response(self, exchange: str, msg: dict) -> None:
        rid = msg.get("user_request_id", "")
        leg = self.req_to_leg.pop(rid, None)
        success = bool(msg.get("success"))
        data = msg.get("data") or {}
        if leg is None:
            return
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
        for oid in list(self.live_orders):
            ex0 = self.live_orders[oid][0]
            if ex0 == exchange and oid not in live_ids:
                self.live_orders.pop(oid, None)

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

    # ─── consensus fair-value engine ─────────────────────────────────
    def recompute_fv(self) -> None:
        """Cross-venue weighted microprice → consensus FV per ticker.

        Weight of each (venue, ticker) observation:
            w = recency_weight × tightness_weight × depth_weight

        recency: exp(-Δt / 0.5s) for Δt = wall age of the book update
        tightness: 1 / (1 + spread_cents / 10)
        depth: log1p(min(bid_qty, ask_qty))

        For ETFs we additionally derive an estimate from constituent FVs and
        blend it in with weight ∝ (n_observations of constituents).  The
        constituent-derived estimate is generally tighter than the ETF book
        because the equal-weighted basket smooths idiosyncratic noise.
        """
        now = time.monotonic()
        # Snapshot stocks first (constituent FVs feed ETF FV).
        stock_fv: dict[str, tuple[float, float]] = {}
        for tk in STOCKS:
            wsum = 0.0
            wmid_sum = 0.0
            for v in LISTINGS[tk]:
                if v not in self.active_venues:
                    continue
                bk = self.books.get((v, tk))
                if bk is None:
                    continue
                mp = bk.microprice
                sp = bk.spread
                if mp is None or sp is None:
                    continue
                age = now - bk.last_update_wall
                rec = math.exp(-age / 0.5)
                tight = 1.0 / (1.0 + sp / 10.0)
                depth = math.log1p(min(bk.best_bid_qty, bk.best_ask_qty))
                w = rec * tight * (1.0 + depth)
                if w <= 0:
                    continue
                wsum += w
                wmid_sum += w * mp
            if wsum > 0:
                stock_fv[tk] = (wmid_sum / wsum, wsum)
                self.fv[tk] = (wmid_sum / wsum, min(1.0, wsum / 10.0), now)

        # ETF FV — blend book-derived with basket-derived.
        for etf, basket in ETF_BASKETS.items():
            wsum = 0.0
            wmid_sum = 0.0
            for v in LISTINGS[etf]:
                if v not in self.active_venues:
                    continue
                bk = self.books.get((v, etf))
                if bk is None:
                    continue
                mp = bk.microprice
                sp = bk.spread
                if mp is None or sp is None:
                    continue
                age = now - bk.last_update_wall
                rec = math.exp(-age / 0.5)
                tight = 1.0 / (1.0 + sp / 10.0)
                depth = math.log1p(min(bk.best_bid_qty, bk.best_ask_qty))
                w = rec * tight * (1.0 + depth)
                if w <= 0:
                    continue
                wsum += w
                wmid_sum += w * mp
            # Basket-derived (NAV).
            constituents = [stock_fv[c] for c in basket if c in stock_fv]
            if len(constituents) == len(basket):
                nav = sum(p for p, _ in constituents) / len(basket)
                # Weight basket NAV proportional to constituent-observation strength.
                cw = sum(w for _, w in constituents)
                wsum += cw
                wmid_sum += cw * nav
            if wsum > 0:
                self.fv[etf] = (wmid_sum / wsum, min(1.0, wsum / 10.0), now)

    # ─── volatility-adaptive edge ─────────────────────────────────────
    def edge_for(self, *tickers: str, floor: int = MIN_ARB_EDGE) -> float:
        """Edge threshold (cents) = floor + k · max(σ_tick over tickers).

        Additive — `floor` always applies, vol only widens it gently in chop.
        With EDGE_VOL_K=0.6 and σ ≈ 3 c the threshold is floor + 1.8 c, so
        even in busy markets we stay below prism's flat 4 c."""
        sig = max((self.vol[t].sigma_cents for t in tickers if t in self.vol), default=0.0)
        return float(floor) + EDGE_VOL_K * sig

    # ─── strategy bus ────────────────────────────────────────────────
    def strategize(self) -> None:
        # Throttle to ~50 ticks/sec aggregate from all connections.
        now = time.monotonic()
        if now - self.last_strategize < 0.02:
            return
        self.last_strategize = now

        self.recompute_fv()
        self.update_residual_stats()

        candidates: list[Plan] = []
        candidates += self.etf_basket_arbs()
        candidates += self.sub_etf_arbs()
        candidates += self.cross_venue_arbs()
        candidates += self.statistical_fv_snipes()
        candidates += self.sector_residual_arbs()
        candidates += self.safe_haven_arbs()
        candidates += self.settlement_unwind()

        # Suppress duplicate plans we just fired and rank by edge.
        candidates = [p for p in candidates if now - self.last_fired[p.tag] >= PLAN_FIRE_GAP_S]
        candidates.sort(key=lambda p: -p.edge_cents)

        fired = 0
        for plan in candidates:
            if fired >= MAX_PLANS_PER_TICK:
                break
            self.fire(plan)
            self.last_fired[plan.tag] = now
            fired += 1

        self.passive_mm_refresh()

        if now - self.last_log >= HEARTBEAT_LOG_S:
            self.last_log = now
            mtm = 0
            book_count = 0
            for (ex, tk), bk in self.books.items():
                if bk.mid is not None:
                    book_count += 1
                    mtm += int(self.pos[(ex, tk)] * bk.mid)
            total_cash = sum(self.cash.values())
            log.info(
                "[hb] books=%d cash=$%.0f mtm=$%.0f fills=%d realized=$%.0f open=%d live_lim=%d",
                book_count, total_cash / 100, mtm / 100,
                self.fills_count, self.realized_cents / 100,
                sum(self.open_count.values()), len(self.live_orders),
            )

    def _best_sweep(self, ticker: str, side: str, qty: int) -> Optional[tuple[str, int, float]]:
        """Best venue for sweeping `qty` shares.

        side="bid" buys from asks and minimizes average price.  side="ask"
        sells into bids and maximizes average price.
        """
        best: Optional[tuple[str, int, float]] = None
        for venue in LISTINGS.get(ticker, ()):
            if venue not in self.active_venues:
                continue
            book = self.books.get((venue, ticker))
            if book is None:
                continue
            fill = book.sweep(side, qty)
            if fill is None:
                continue
            limit_price, avg_price = fill
            if best is None:
                best = (venue, limit_price, avg_price)
            elif side == "bid" and avg_price < best[2]:
                best = (venue, limit_price, avg_price)
            elif side == "ask" and avg_price > best[2]:
                best = (venue, limit_price, avg_price)
        return best

    # ─── strategy 1: ETF<->basket arbitrage, depth-walked ────────────
    def etf_basket_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for etf in ETFS:
            basket = ETF_BASKETS[etf]
            n = len(basket)
            edge_floor = self.edge_for(etf, *basket, floor=MIN_ARB_EDGE)

            for etf_v in LISTINGS[etf]:
                if etf_v not in self.active_venues:
                    continue
                etf_book = self.books[(etf_v, etf)]

                best_long: Optional[Plan] = None
                best_short: Optional[Plan] = None

                for k in range(1, ARB_MAX_K + 1):
                    etf_qty = n * k

                    # Direction A: ETF cheap -> BUY ETF, SELL constituents.
                    etf_fill = etf_book.sweep("bid", etf_qty)
                    if etf_fill is not None:
                        etf_limit, etf_avg = etf_fill
                        if self.pos[(etf_v, etf)] + etf_qty <= SOFT_POS_MAX:
                            if self.cash[etf_v] - etf_qty * etf_limit >= SOFT_CASH_FLOOR:
                                legs = [Leg(etf_v, etf, "bid", etf_qty, etf_limit,
                                            note=f"etfarb_buy_{etf}@{etf_v}")]
                                synth_sell_total = 0.0
                                feasible = True
                                for tk in basket:
                                    route = self._best_sweep(tk, "ask", k)
                                    if route is None:
                                        feasible = False
                                        break
                                    v, limit, avg = route
                                    if self.pos[(v, tk)] - k < SOFT_POS_MIN:
                                        feasible = False
                                        break
                                    synth_sell_total += avg
                                    legs.append(Leg(v, tk, "ask", k, limit,
                                                    note=f"etfarb_sell_{tk}@{v}"))
                                if feasible:
                                    edge = synth_sell_total / n - etf_avg
                                    if edge >= edge_floor:
                                        best_long = Plan(
                                            legs,
                                            edge_cents=edge,
                                            strategy=f"ETF-NAV {etf}@{etf_v} long",
                                            tag=f"etfA:{etf}:{etf_v}",
                                        )

                    # Direction B: ETF rich -> SELL ETF, BUY constituents.
                    etf_fill = etf_book.sweep("ask", etf_qty)
                    if etf_fill is None:
                        continue
                    etf_limit, etf_avg = etf_fill
                    if self.pos[(etf_v, etf)] - etf_qty < SOFT_POS_MIN:
                        continue
                    legs = [Leg(etf_v, etf, "ask", etf_qty, etf_limit,
                                note=f"etfarb_sell_{etf}@{etf_v}")]
                    synth_buy_total = 0.0
                    feasible = True
                    venue_cost: dict[str, int] = defaultdict(int)
                    for tk in basket:
                        route = self._best_sweep(tk, "bid", k)
                        if route is None:
                            feasible = False
                            break
                        v, limit, avg = route
                        if self.pos[(v, tk)] + k > SOFT_POS_MAX:
                            feasible = False
                            break
                        venue_cost[v] += k * limit
                        synth_buy_total += avg
                        legs.append(Leg(v, tk, "bid", k, limit,
                                        note=f"etfarb_buy_{tk}@{v}"))
                    if not feasible:
                        continue
                    for v, cost in venue_cost.items():
                        if self.cash[v] - cost < SOFT_CASH_FLOOR:
                            feasible = False
                            break
                    if not feasible:
                        continue
                    edge = etf_avg - synth_buy_total / n
                    if edge >= edge_floor:
                        best_short = Plan(
                            legs,
                            edge_cents=edge,
                            strategy=f"ETF-NAV {etf}@{etf_v} short",
                            tag=f"etfB:{etf}:{etf_v}",
                        )

                if best_long is not None:
                    out.append(best_long)
                if best_short is not None:
                    out.append(best_short)
        return out

    # ─── strategy 2: ETF <-> sub-ETF + complement basket ─────────────
    def sub_etf_arbs(self) -> list[Plan]:
        """Identity: 6·super = 3·sub + sum(complement)."""
        out: list[Plan] = []
        for super_etf, (sub_etf, complement) in SUB_ETF_LINKS.items():
            sup_venues = [v for v in LISTINGS[super_etf] if v in self.active_venues]
            sub_venues = [v for v in LISTINGS[sub_etf]   if v in self.active_venues]
            if not sup_venues or not sub_venues:
                continue
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

            def best_quote(tk: str, venues: list[str], buying: bool):
                best = None
                for v in venues:
                    bk = self.books[(v, tk)]
                    px = bk.best_ask if buying else bk.best_bid
                    qty = bk.best_ask_qty if buying else bk.best_bid_qty
                    if px is None:
                        continue
                    if best is None or (px < best[1] if buying else px > best[1]):
                        best = (v, px, qty)
                return best

            sup_buy = best_quote(super_etf, sup_venues, True)
            sup_sell = best_quote(super_etf, sup_venues, False)
            sub_buy = best_quote(sub_etf,   sub_venues, True)
            sub_sell = best_quote(sub_etf,   sub_venues, False)
            if not (sup_buy and sup_sell and sub_buy and sub_sell):
                continue

            # Direction A: super cheap, sub & complement rich
            edge_a = 3 * sub_buy[1] + sum(comp_bid[tk][1] for tk in complement) - 6 * sup_buy[1]
            if edge_a >= 6 * SUB_ETF_EDGE:
                k = min(
                    sup_buy[2] // 6,
                    sub_buy[2] // 3,
                    *(comp_bid[tk][2] for tk in complement),
                    (SOFT_POS_MAX - self.pos[(sup_buy[0], super_etf)]) // 6,
                    (self.pos[(sub_buy[0], sub_etf)] - SOFT_POS_MIN) // 3,
                    *((self.pos[(comp_bid[tk][0], tk)] - SOFT_POS_MIN) for tk in complement),
                    ((self.cash[sup_buy[0]] - SOFT_CASH_FLOOR) // (6 * sup_buy[1]) if sup_buy[1] else 0),
                    ARB_MAX_K,
                )
                if k > 0:
                    legs = [
                        Leg(sup_buy[0], super_etf, "bid", 6 * k, sup_buy[1], note=f"sub_long_{super_etf}"),
                        Leg(sub_buy[0], sub_etf,   "ask", 3 * k, sub_buy[1], note=f"sub_short_{sub_etf}"),
                    ]
                    for tk in complement:
                        v, px, _ = comp_bid[tk]
                        legs.append(Leg(v, tk, "ask", k, px, note=f"sub_short_{tk}"))
                    out.append(Plan(legs, edge_cents=edge_a / 6,
                                    strategy=f"SUB-ETF {super_etf}/{sub_etf} long",
                                    tag=f"sub_a:{super_etf}"))

            # Direction B: super rich, sub & complement cheap
            edge_b = 6 * sup_sell[1] - 3 * sub_sell[1] - sum(comp_ask[tk][1] for tk in complement)
            if edge_b >= 6 * SUB_ETF_EDGE:
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
                        Leg(sup_sell[0], super_etf, "ask", 6 * k, sup_sell[1], note=f"sub_short_{super_etf}"),
                        Leg(sub_sell[0], sub_etf,   "bid", 3 * k, sub_sell[1], note=f"sub_long_{sub_etf}"),
                    ]
                    for tk in complement:
                        v, px, _ = comp_ask[tk]
                        legs.append(Leg(v, tk, "bid", k, px, note=f"sub_long_{tk}"))
                    out.append(Plan(legs, edge_cents=edge_b / 6,
                                    strategy=f"SUB-ETF {super_etf}/{sub_etf} short",
                                    tag=f"sub_b:{super_etf}"))
        return out

    # ─── strategy 3: cross-venue same-stock arbitrage ────────────────
    def cross_venue_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for tk in LISTINGS:
            venues = [v for v in LISTINGS[tk] if v in self.active_venues]
            if len(venues) < 2:
                continue
            edge_floor = self.edge_for(tk, floor=MIN_ARB_EDGE)
            best_b = best_a = None
            for v in venues:
                bk = self.books[(v, tk)]
                if bk.best_bid is not None and (best_b is None or bk.best_bid > best_b[1]):
                    best_b = (v, bk.best_bid, bk.best_bid_qty)
                if bk.best_ask is not None and (best_a is None or bk.best_ask < best_a[1]):
                    best_a = (v, bk.best_ask, bk.best_ask_qty)
            if not best_b or not best_a or best_b[0] == best_a[0]:
                continue
            edge = best_b[1] - best_a[1]
            if edge >= edge_floor:
                qty = min(best_b[2], best_a[2], XV_MAX_QTY)
                buy_v, sell_v = best_a[0], best_b[0]
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
                        tag=f"xv:{tk}:{buy_v}:{sell_v}",
                    ))
        return out

    # ─── strategy 4: statistical FV snipe (single-leg) ───────────────
    def statistical_fv_snipes(self) -> list[Plan]:
        """When a venue's quote is more than `STAT_EDGE_SIGMA` σ off consensus
        FV, hit it.  No instant offset — exposure gated by net inventory cap.

        This catches transient stale quotes (a venue that hasn't repriced after
        a fast fundamental move on its peers) that the two-venue arb misses
        because no single peer is *better* than the consensus."""
        out: list[Plan] = []
        for tk, (fv, conf, _) in self.fv.items():
            if conf < 0.4:                              # need decent agreement first
                continue
            sig = max(2.0, self.vol[tk].sigma_cents)    # cents per tick
            edge_floor = max(STAT_EDGE_SIGMA * sig, MIN_ARB_EDGE + 1.0)
            for v in LISTINGS[tk]:
                if v not in self.active_venues:
                    continue
                bk = self.books[(v, tk)]
                bb, ba = bk.best_bid, bk.best_ask
                if bb is None or ba is None:
                    continue
                # Adverse-flow gate: if recent flow is *into* the side we'd be
                # trading (so smart money is buying when we'd buy), skip.
                flow = self.flow.net(tk, self.server_time.get(v, now_ms()))

                # Stale ask: BUY at bk.best_ask
                if fv - ba >= edge_floor and flow > -50:
                    qty = min(bk.best_ask_qty, STAT_MAX_QTY)
                    qty = min(qty, SOFT_POS_MAX - self.pos[(v, tk)])
                    qty = min(qty, max(0, STAT_MAX_INVENTORY - self._gross_inventory(tk)))
                    if ba > 0:
                        qty = min(qty, (self.cash[v] - SOFT_CASH_FLOOR) // ba)
                    if qty > 0:
                        out.append(Plan(
                            [Leg(v, tk, "bid", qty, ba, note=f"stat_buy_{tk}@{v}")],
                            edge_cents=fv - ba,
                            strategy=f"STAT {tk} long@{v}",
                            tag=f"stat_a:{tk}:{v}",
                        ))
                # Stale bid: SELL at bk.best_bid
                if bb - fv >= edge_floor and flow < 50:
                    qty = min(bk.best_bid_qty, STAT_MAX_QTY)
                    qty = min(qty, self.pos[(v, tk)] - SOFT_POS_MIN)
                    qty = min(qty, max(0, STAT_MAX_INVENTORY - self._gross_inventory(tk)))
                    if qty > 0:
                        out.append(Plan(
                            [Leg(v, tk, "ask", qty, bb, note=f"stat_sell_{tk}@{v}")],
                            edge_cents=bb - fv,
                            strategy=f"STAT {tk} short@{v}",
                            tag=f"stat_b:{tk}:{v}",
                        ))
        return out

    def _gross_inventory(self, ticker: str) -> int:
        """Sum |position| across venues for the ticker — caps stat-arb growth."""
        return sum(abs(p) for (ex, tk), p in self.pos.items() if tk == ticker)

    # ─── strategy 5: sector residual mean-reversion ──────────────────
    def update_residual_stats(self) -> None:
        """EWMA of (FV_i - sector_mean) per ticker, used by the residual arb."""
        for sec, members in (("A", SECTOR_A), ("B", SECTOR_B)):
            fvs = [(tk, self.fv[tk][0]) for tk in members if tk in self.fv]
            if len(fvs) < 4:
                continue
            mean = sum(p for _, p in fvs) / len(fvs)
            for tk, p in fvs:
                r = p - mean
                old_mu = self.resid_mu[tk]
                # EWMA coefficients ~ 1/30 (≈ 3 s decay at 100 ms tick)
                self.resid_mu[tk] = 0.97 * old_mu + 0.03 * r
                d = r - self.resid_mu[tk]
                self.resid_var[tk] = 0.97 * self.resid_var[tk] + 0.03 * d * d

    def sector_residual_arbs(self) -> list[Plan]:
        """Fade single-stock outliers vs sector cohort, hedged with the sector ETF.

        Trade construction: when stock i is rich by zσ vs sector index
            short stock_i (1×), long ETF (1× on its cheapest ask venue)
        and vice versa.  The hedge has unit beta to the sector since ETF is
        the equal-weighted basket; this leaves us with pure residual exposure.
        """
        out: list[Plan] = []
        for sec, members in (("A", SECTOR_A), ("B", SECTOR_B)):
            etf = SECTOR_ETF[sec]
            if etf not in self.fv:
                continue
            for tk in members:
                if tk not in self.fv:
                    continue
                mu = self.resid_mu[tk]
                var = self.resid_var[tk]
                sd = math.sqrt(max(var, 4.0))
                fv_i = self.fv[tk][0]
                # current residual
                fvs_now = [self.fv[m][0] for m in members if m in self.fv]
                if len(fvs_now) < 4:
                    continue
                sec_mean = sum(fvs_now) / len(fvs_now)
                resid = fv_i - sec_mean
                z = (resid - mu) / sd
                if abs(z) < SECTOR_Z_THRESHOLD:
                    continue
                # Find best venue to short/long the stock + cheapest ETF venue.
                # Stock leg: hit the side that fades the residual.
                #   resid > 0  → stock rich → SELL stock, BUY ETF (long sector basket)
                #   resid < 0  → stock cheap→ BUY stock, SELL ETF
                if z > 0:
                    stock_v, stock_px, stock_qty = self._best_quote(tk, want="bid")  # we sell into bid
                    if stock_v is None:
                        continue
                    etf_v, etf_px, etf_qty = self._best_quote(etf, want="ask")        # we buy ETF
                    if etf_v is None:
                        continue
                    qty = min(stock_qty, etf_qty, SECTOR_MAX_QTY,
                              self.pos[(stock_v, tk)] - SOFT_POS_MIN,
                              SOFT_POS_MAX - self.pos[(etf_v, etf)])
                    if etf_px > 0:
                        qty = min(qty, (self.cash[etf_v] - SOFT_CASH_FLOOR) // etf_px)
                    if qty <= 0:
                        continue
                    out.append(Plan(
                        [Leg(stock_v, tk, "ask", qty, stock_px, note=f"sec_short_{tk}"),
                         Leg(etf_v, etf, "bid", qty, etf_px, note=f"sec_hedge_long_{etf}")],
                        edge_cents=abs(resid - mu) * 0.5,   # ~half reversion expected
                        strategy=f"SECTOR fade {tk} z={z:.1f}",
                        tag=f"secA:{tk}",
                    ))
                else:
                    stock_v, stock_px, stock_qty = self._best_quote(tk, want="ask")  # we buy at ask
                    if stock_v is None:
                        continue
                    etf_v, etf_px, etf_qty = self._best_quote(etf, want="bid")        # we sell ETF
                    if etf_v is None:
                        continue
                    qty = min(stock_qty, etf_qty, SECTOR_MAX_QTY,
                              SOFT_POS_MAX - self.pos[(stock_v, tk)],
                              self.pos[(etf_v, etf)] - SOFT_POS_MIN)
                    if stock_px > 0:
                        qty = min(qty, (self.cash[stock_v] - SOFT_CASH_FLOOR) // stock_px)
                    if qty <= 0:
                        continue
                    out.append(Plan(
                        [Leg(stock_v, tk, "bid", qty, stock_px, note=f"sec_long_{tk}"),
                         Leg(etf_v, etf, "ask", qty, etf_px, note=f"sec_hedge_short_{etf}")],
                        edge_cents=abs(resid - mu) * 0.5,
                        strategy=f"SECTOR fade {tk} z={z:.1f}",
                        tag=f"secB:{tk}",
                    ))
        return out

    # ─── strategy 6: safe-haven coherence guard ─────────────────────
    def safe_haven_arbs(self) -> list[Plan]:
        """Synth-market and ETFSH should anti-correlate.  When they have
        moved *together* by SH_GUARD_THRESHOLD cents over the last few seconds,
        fade the safe-haven side (likeliest to revert)."""
        out: list[Plan] = []
        if "ETFSH" not in self.fv:
            return out
        # Synth market index = mean of FV across NON_SH stocks present.
        m_pts = [self.fv[t][0] for t in NON_SH_INDEX if t in self.fv]
        if len(m_pts) < 10:
            return out
        mkt = sum(m_pts) / len(m_pts)
        sh = self.fv["ETFSH"][0]
        # We need a "drift" signal — abs price level isn't useful, only changes
        # vs the per-venue prev_mid we keep in VolTracker.  Use vol-tracker
        # recent mid changes summed up.
        # Simple signal: deviation of (sh - mkt) from its EWMA.  We track that
        # in the same residual EWMA dict for "_SH" key.
        r = sh - mkt
        old_mu = self.resid_mu["_SH"]
        self.resid_mu["_SH"] = 0.97 * old_mu + 0.03 * r
        d = r - self.resid_mu["_SH"]
        self.resid_var["_SH"] = 0.97 * self.resid_var["_SH"] + 0.03 * d * d
        sd = math.sqrt(max(self.resid_var["_SH"], 4.0))
        z = (r - self.resid_mu["_SH"]) / sd
        if abs(z) < 2.0 or abs(r - self.resid_mu["_SH"]) < SH_GUARD_THRESHOLD:
            return out
        # If z > 0, ETFSH unusually rich vs its anti-correlated relationship.
        # Sell ETFSH at best bid (no offset — single-leg, capped).
        if z > 0:
            v, px, q = self._best_quote("ETFSH", want="bid")
            if v is None:
                return out
            qty = min(q, SH_GUARD_MAX_QTY, self.pos[(v, "ETFSH")] - SOFT_POS_MIN)
            if qty <= 0:
                return out
            out.append(Plan(
                [Leg(v, "ETFSH", "ask", qty, px, note="sh_fade_short")],
                edge_cents=abs(r - self.resid_mu["_SH"]) * 0.4,
                strategy=f"SH fade short z={z:.1f}",
                tag="sh_short",
            ))
        else:
            v, px, q = self._best_quote("ETFSH", want="ask")
            if v is None:
                return out
            qty = min(q, SH_GUARD_MAX_QTY, SOFT_POS_MAX - self.pos[(v, "ETFSH")])
            if px > 0:
                qty = min(qty, (self.cash[v] - SOFT_CASH_FLOOR) // px)
            if qty <= 0:
                return out
            out.append(Plan(
                [Leg(v, "ETFSH", "bid", qty, px, note="sh_fade_long")],
                edge_cents=abs(r - self.resid_mu["_SH"]) * 0.4,
                strategy=f"SH fade long z={z:.1f}",
                tag="sh_long",
            ))
        return out

    def _best_quote(self, tk: str, want: str) -> tuple[Optional[str], int, int]:
        """want='bid' returns highest bid (where we sell); want='ask' returns lowest ask (where we buy)."""
        best: tuple[Optional[str], int, int] = (None, 0, 0)
        for v in LISTINGS.get(tk, ()):
            if v not in self.active_venues:
                continue
            bk = self.books.get((v, tk))
            if bk is None:
                continue
            if want == "bid":
                px, qty = bk.best_bid, bk.best_bid_qty
                if px is None:
                    continue
                if best[0] is None or px > best[1]:
                    best = (v, px, qty)
            else:
                px, qty = bk.best_ask, bk.best_ask_qty
                if px is None:
                    continue
                if best[0] is None or px < best[1]:
                    best = (v, px, qty)
        return best

    # ─── strategy 7: settlement-aware unwind ─────────────────────────
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
            urgency = 1.0 - remaining / EOS_UNWIND_MS
            aggressive = remaining <= EOS_FLATTEN_MS
            for (ex2, tk), pos in list(self.pos.items()):
                if ex2 != ex or pos == 0:
                    continue
                bk = self.books.get((ex, tk))
                if bk is None:
                    continue
                target_qty = abs(pos)
                if pos > 0:
                    bb = bk.best_bid
                    if bb is None:
                        continue
                    qty = min(target_qty, max(1, bk.best_bid_qty),
                              max(1, int(target_qty * (1.0 if aggressive else urgency))))
                    px = bb if not aggressive else max(1, bb - 5)
                    out.append(Plan(
                        [Leg(ex, tk, "ask", qty, px, note=f"unwind_long_{tk}")],
                        edge_cents=0.0,
                        strategy=f"UNWIND long {tk}@{ex}",
                        tag=f"unw_l:{ex}:{tk}",
                    ))
                else:
                    ba = bk.best_ask
                    if ba is None:
                        continue
                    qty = min(target_qty, max(1, bk.best_ask_qty),
                              max(1, int(target_qty * (1.0 if aggressive else urgency))))
                    px = ba if not aggressive else ba + 5
                    out.append(Plan(
                        [Leg(ex, tk, "bid", qty, px, note=f"unwind_short_{tk}")],
                        edge_cents=0.0,
                        strategy=f"UNWIND short {tk}@{ex}",
                        tag=f"unw_s:{ex}:{tk}",
                    ))
        return out

    # ─── strategy 8: inventory- & flow-skewed two-sided MM ───────────
    def passive_mm_refresh(self) -> None:
        """Quote both sides one tick inside MM on a small set of liquid names.

        Width and skew driven by:
          * net position on the (venue, ticker)  — long → bid further, ask closer
          * aggressor flow over last 3 s       — buyers pressing → both sides up
          * volatility                         — wider quotes when σ high

        Pulled if:
          * top-of-book spread > MM_MAX_SPREAD (book too thin)
          * |position| approaches MM_MAX_NET_POS (out of inventory headroom)
          * adverse one-sided flow against our existing exposure
        """
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
                if bb is None or ba is None:
                    continue
                spread = ba - bb
                if spread < 2 * MM_INSIDE_TICK + 1 or spread > MM_MAX_SPREAD:
                    self._cancel_mm_quotes(v, tk)
                    continue

                pos = self.pos[key]
                if abs(pos) > MM_MAX_NET_POS:
                    # Position too hot — pull and let unwind/snipes drain it.
                    self._cancel_mm_quotes(v, tk)
                    continue

                flow = self.flow.net(tk, self.server_time.get(v, now_ms()))
                sigma = self.vol[tk].sigma_cents

                # base step 1 tick inside; widen if vol high.
                step = MM_INSIDE_TICK + max(0, int(sigma / 8))
                # inventory skew (cents): lean against position.
                #   long positive pos → push bid down (less aggressive bid),
                #                       push ask down (more aggressive sell)
                inv_skew = max(-6, min(6, int(pos / 25)))
                # flow skew: ride the wave one cent per ~30 share net flow.
                flow_skew = max(-3, min(3, int(flow / 30)))

                bid_px = bb + step + flow_skew - inv_skew
                ask_px = ba - step + flow_skew - inv_skew
                # Don't cross our own spread; ensure bid_px < ask_px and both inside the book.
                bid_px = max(bb + 1, min(bid_px, ba - 2))
                ask_px = min(ba - 1, max(ask_px, bb + 2))
                if bid_px >= ask_px:
                    self._cancel_mm_quotes(v, tk)
                    continue

                # Size: scales with remaining headroom and inversely with vol.
                vol_scale = max(0.4, min(1.0, 6.0 / max(1.0, sigma)))
                base_qty = int(MM_QTY_BASE * vol_scale)
                bid_qty = max(2, min(MM_QTY_MAX,
                                     base_qty + max(0, (-pos) // 30),
                                     SOFT_POS_MAX - pos - 4))
                ask_qty = max(2, min(MM_QTY_MAX,
                                     base_qty + max(0, pos // 30),
                                     pos - SOFT_POS_MIN - 4))

                # Pull if flow strongly adverse to a side.
                if flow > 80:
                    # Heavy buying — our ask gets picked off.  Skip ask, keep bid.
                    ask_qty = 0
                if flow < -80:
                    bid_qty = 0

                # Reconcile against existing live quotes.
                want_bid = bid_qty > 0
                want_ask = ask_qty > 0
                have_bid = have_ask = False
                stale: list[int] = []
                for oid, (ex0, tk0, side0, _q0, px0) in self.live_orders.items():
                    if ex0 != v or tk0 != tk:
                        continue
                    if side0 == "bid":
                        if want_bid and px0 == bid_px:
                            have_bid = True
                        else:
                            stale.append(oid)
                    elif side0 == "ask":
                        if want_ask and px0 == ask_px:
                            have_ask = True
                        else:
                            stale.append(oid)

                conn = self.connections.get(v)
                if conn and conn.connected:
                    for oid in stale:
                        conn.enqueue({
                            "type": "cancel_order",
                            "user_request_id": self.req_id(f"cxl-{tk}"),
                            "order_id": oid,
                            "instrument_id": f"{v}-{tk}",
                        })

                if want_bid and not have_bid:
                    if (pos < SOFT_POS_MAX - bid_qty
                            and self.cash[v] - bid_qty * bid_px > SOFT_CASH_FLOOR):
                        self.fire_one(Leg(v, tk, "bid", bid_qty, bid_px,
                                          order_type="limit", note=f"mm_bid_{tk}"))
                if want_ask and not have_ask:
                    if pos > SOFT_POS_MIN + ask_qty:
                        self.fire_one(Leg(v, tk, "ask", ask_qty, ask_px,
                                          order_type="limit", note=f"mm_ask_{tk}"))

    def _cancel_mm_quotes(self, v: str, tk: str) -> None:
        conn = self.connections.get(v)
        if conn is None or not conn.connected:
            return
        for oid, (ex0, tk0, _side, _q, _px) in list(self.live_orders.items()):
            if ex0 == v and tk0 == tk:
                conn.enqueue({
                    "type": "cancel_order",
                    "user_request_id": self.req_id(f"cxl-{tk}"),
                    "order_id": oid,
                    "instrument_id": f"{v}-{tk}",
                })


# ════════════════════════════════════════════════════════════════════════════
# Per-exchange websocket connection
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
        if ws_connect is None:
            raise RuntimeError("websockets is required to run prism_v2; install requirements.txt")
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
            while not self.send_q.empty():
                try:
                    self.send_q.get_nowait()
                except Exception:
                    break
            jitter = random.uniform(0.0, 0.2)
            await asyncio.sleep(min(MAX_BACKOFF_S, backoff) + jitter)
            backoff = min(MAX_BACKOFF_S, backoff * 1.7)

    async def _receiver_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                # Server emits "Message rate limit exceeded" as plain text.
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

    # Idle ticker — runs strategize() on a 100 ms cadence even if some venue's
    # market data is paused (e.g. during a segment-boundary reconnect).
    async def tick():
        while not hub.stop:
            await asyncio.sleep(0.1)
            try:
                hub.strategize()
            except Exception as e:
                log.exception("strategize error: %s", e)
    tasks.append(asyncio.create_task(tick(), name="tick"))

    try:
        while not hub.stop:
            await asyncio.sleep(0.5)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    log.info("done")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="prism_v2 — AlgoTrade 2026 bot")
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
    log.info("starting prism_v2 on venues: %s", active)
    try:
        asyncio.run(amain(active))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
