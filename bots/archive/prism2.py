#!/usr/bin/env python3
"""
prism2.py — AlgoTrade 2026 trading bot, v2.

Built on prism v1 (multi-venue ETF routing + sub-ETF identity + atomic plans),
with five data-driven additions calibrated against the 9-venue training set:

  1. **Per-instrument adaptive thresholds.** Each ticker tracks a rolling window
     of observed cross-venue spreads. Threshold floats to a fixed quantile of
     that distribution — wide for CARD (~600¢ p25 spread), tight for ZITO (~60¢).

  2. **Depth walking on book sweeps.** v1 sized cross-venue arb at the lesser
     of best_bid_qty / best_ask_qty (level 1 only). With observed spreads of
     50-300¢, levels 2-3 are still well above any reasonable edge floor, so
     we walk them for the additional fills.

  3. **Microprice in fair value.** v1's Book class had .microprice but it was
     never called. Asymmetric depth (50 bid / 10 ask) is a real signal of
     where the next print lands; using mid throws it away.

  4. **Inventory-tapered sizing.** Hard caps (SOFT_POS_MAX) only protect against
     hitting the wall. We add a graduated taper: as |position| approaches the
     cap, new orders shrink quadratically and the threshold widens.

  5. **Per-strategy P&L instrumentation.** Each fill is attributed to its source
     strategy. Heartbeat logs PnL by strategy so you can see which actually
     made money and disable / tune the rest after a dry segment.

Latency-aware execution (this revision)
----------------------------------------
The earlier `CROSS_CLUSTER_DRIFT_CENTS = 3` adder was a flat constant; it
neither matched true RTT nor adapted when market conditions changed. Replaced
with three coordinated mechanisms:

  * **Measured per-venue RTT.** Every add_order's send time is paired with its
    add_order_response receipt; an EWMA per venue gives us the actual round-trip
    in milliseconds, not a cluster-table estimate.
  * **Per-(venue, ticker) IOC miss-rate.** When the server accepts an IOC but
    immediate_inventory_change == 0, the level we targeted was gone before our
    request landed. We track the empirical fill probability with a Beta prior
    and divide the edge floor by it: if we win 30% of races on HKEX-CARD, we
    need 3.3× the nominal edge before firing.
  * **Latency-priced venue selection.** _best_buy_venue / _best_sell_venue
    now score by `effective_price = ask + drift_per_ms × measured_rtt_ms`
    (and the symmetric form for sells). The drift estimate comes from a
    rolling window of mid changes per ticker, so a 200ms-far venue 3¢ better
    in nominal price loses to a 30ms-near venue when expected price drift
    over the RTT eats the gap.

Defensive layer:
  * **RTT-based qty haircut.** Smaller IOCs to slow venues — less wasted
    capital on phantom levels, free to re-fire next tick if depth persists.
  * **Stale-book guard.** If a venue's last_update_wall is older than
    STALE_BOOK_S, we skip it from venue selection entirely — don't act on
    data we know is rotten.
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
except ImportError:
    from websockets.client import connect as ws_connect  # type: ignore

# ════════════════════════════════════════════════════════════════════════════
# Universe (unchanged from v1)
# ════════════════════════════════════════════════════════════════════════════

VENUES = ["NYSE", "NASDAQ", "SSE", "JPX", "Euronext", "LSE", "HKEX", "NSE", "TMX", "ZSE"]
WS_HOSTS = {
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

# Latency clusters — used as cold-start RTT default before we have real
# measurements, and for telemetry grouping.
CLUSTERS = {
    "NA":   {"NYSE", "NASDAQ", "TMX"},
    "EU":   {"LSE", "Euronext"},
    "ASIA": {"JPX", "HKEX", "SSE"},
    "IN":   {"NSE"},
    "ZSE":  {"ZSE"},
}
CLUSTER_OF = {v: c for c, vs in CLUSTERS.items() for v in vs}

LISTINGS: dict[str, set[str]] = {
    "CARD":  {"NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"},
    "SIMP":  {"NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"},
    "NGUP":  {"NYSE","NASDAQ","Euronext","TMX","ZSE"},
    "OIT":   {"LSE","Euronext","HKEX","NSE","ZSE"},
    "KTST":  {"NYSE","JPX","TMX","ZSE"},
    "FSR":   {"NASDAQ","LSE","SSE","HKEX","ZSE"},
    "JZRO":  {"NYSE","LSE","Euronext","TMX","ZSE"},
    "XFR":   {"NYSE","HKEX","TMX","ZSE"},
    "KOTD":  {"NASDAQ","LSE","Euronext","HKEX","ZSE"},
    "INA":   {"NYSE","NASDAQ","Euronext","HKEX","ZSE"},
    "HT":    {"NASDAQ","LSE","JPX","SSE","TMX","ZSE"},
    "JNAF":  {"NYSE","Euronext","JPX","HKEX","ZSE"},
    "DLKV":  {"NASDAQ","LSE","HKEX","NSE","ZSE"},
    "DDJH":  {"NYSE","LSE","Euronext","TMX","ZSE"},
    "MDKA":  {"NYSE","LSE","HKEX","TMX","ZSE"},
    "KRAS":  {"NYSE","Euronext","SSE","TMX","ZSE"},
    "ZITO":  {"NASDAQ","LSE","Euronext","NSE","ZSE"},
    "ZABA":  {"NYSE","LSE","SSE","NSE","TMX","ZSE"},
    "GOLD":  {"NASDAQ","Euronext","JPX","TMX","ZSE"},
    "XAG":   {"LSE","Euronext","JPX","ZSE"},
    "ETFA":  {"NYSE","Euronext","HKEX","ZSE"},
    "ETFB":  {"NASDAQ","LSE","HKEX","ZSE"},
    "ETFA3": {"NYSE","TMX","ZSE"},
    "ETFB3": {"NASDAQ","HKEX","ZSE"},
    "ETFSH": {"Euronext","JPX","ZSE"},
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
SUB_ETF_LINKS = {
    "ETFA": ("ETFA3", ["OIT", "FSR", "JZRO"]),
    "ETFB": ("ETFB3", ["HT",  "JNAF", "DDJH"]),
}

# ════════════════════════════════════════════════════════════════════════════
# Limits & knobs
# ════════════════════════════════════════════════════════════════════════════

POS_MAX            =  2_000
POS_MIN            =   -200
INITIAL_CASH       = 10_000_000
CASH_FLOOR         = -5_000_000
MAX_PENDING_ORDERS =  6_000
SERVER_RATE_PER_S  =    500

RATE_PER_S         =    400
SOFT_POS_MAX       =  1_800
SOFT_POS_MIN       =   -180
SOFT_CASH_FLOOR    = -4_500_000

# Base thresholds — these are FLOORS. The adaptive layer only ever makes
# the actual threshold larger, never smaller.
BASE_XV_EDGE       =      4
BASE_ARB_EDGE      =      4
BASE_SUB_ETF_EDGE  =      6

# Adaptive threshold params.
ADAPT_WINDOW_SECONDS =     5.0
ADAPT_QUANTILE       =     0.30
ADAPT_MIN_SAMPLES    =    20

# Inventory taper.
TAPER_START_FRAC   =      0.5
TAPER_END_FRAC     =      0.95

# Depth walking — how many extra cents past best to consider in cross-venue.
DEPTH_WALK_CENTS   =     20

ARB_MAX_K          =     25
XV_MAX_QTY         =    100
MM_INSIDE_TICK     =      1
MM_QTY             =      4
MM_REFRESH_S       =      1.5
MM_INSTRUMENTS     = ["CARD", "SIMP", "ETFA", "ETFB", "GOLD", "XAG", "ETFSH"]

DEFAULT_ROUND_MS   = 600_000
EOS_UNWIND_MS      =  60_000
EOS_FLATTEN_MS     =   8_000
INVENTORY_PERIOD_S =      2.0
HEARTBEAT_LOG_S    =      5.0
MAX_BACKOFF_S      =      4.0

# ─── Latency-aware execution ────────────────────────────────────────────────
# RTT EWMA smoothing — higher = more reactive, lower = more stable.
RTT_EWMA_ALPHA          = 0.25
# How aggressively we discount a venue's price by expected drift over its RTT.
# 1.0 = full expected drift; >1 is more conservative, <1 more aggressive.
LATENCY_PENALTY_LAMBDA  = 1.0
# Window over which we estimate per-ticker drift (cents/ms abs change in mid).
DRIFT_WINDOW_S          = 5.0
# Fallback drift used before we have enough mid-history samples (~5¢/100ms).
DRIFT_DEFAULT_C_PER_MS  = 0.05
# Beta-prior smoothing for IOC fill probability per (venue, ticker).
# Acts as N pseudo-attempts at probability P, blended with empirical samples.
FILL_PROB_PRIOR_N       = 8
FILL_PROB_PRIOR_P       = 0.6
# Floor on fill probability used as a divisor — prevents pathological 1/0.05
# blow-ups when a single early miss hasn't yet been outweighed by the prior.
FILL_PROB_FLOOR         = 0.10
# Cold-start RTT defaults before we have measurements (one cluster vs another).
RTT_DEFAULT_INTRA_MS    = 30.0
RTT_DEFAULT_CROSS_MS    = 180.0
# Smooth knee for RTT-based qty haircut: factor = KNEE / (KNEE + rtt_ms),
# floored at FLOOR. So 0ms → 1.0, 100ms → 0.5, 300ms → 0.25 → floored.
RTT_QTY_HAIRCUT_KNEE_MS = 100.0
RTT_QTY_HAIRCUT_FLOOR   = 0.30
# Skip arbs touching books older than this — likely missed a tick or worse.
STALE_BOOK_S            = 0.30

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOGLEVEL, logging.INFO),
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prism2")


def now_ms() -> int:
    return int(time.time() * 1000)


# ════════════════════════════════════════════════════════════════════════════
# Order book (extended with depth walking and microprice usage)
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Book:
    bids: dict[int, int] = field(default_factory=dict)
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
        return (bb + ba) / 2.0 if bb is not None and ba is not None else None

    @property
    def microprice(self) -> Optional[float]:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        bq, aq = self.best_bid_qty, self.best_ask_qty
        if bq + aq == 0:
            return (bb + ba) / 2.0
        return (bb * aq + ba * bq) / (bq + aq)

    def buy_depth_to(self, price_limit: int) -> tuple[int, Optional[float]]:
        """If we send a buy IOC at price_limit, how much can we get and at what avg fill?"""
        total_q, total_cost = 0, 0
        for p in sorted(self.asks):
            if p > price_limit:
                break
            q = self.asks[p]
            total_q += q
            total_cost += p * q
        if total_q == 0:
            return 0, None
        return total_q, total_cost / total_q

    def sell_depth_to(self, price_limit: int) -> tuple[int, Optional[float]]:
        """If we send a sell IOC at price_limit, how much will fill and at what avg price?"""
        total_q, total_revenue = 0, 0
        for p in sorted(self.bids, reverse=True):
            if p < price_limit:
                break
            q = self.bids[p]
            total_q += q
            total_revenue += p * q
        if total_q == 0:
            return 0, None
        return total_q, total_revenue / total_q


# ════════════════════════════════════════════════════════════════════════════
# Adaptive threshold tracker — per-instrument, per-strategy
# ════════════════════════════════════════════════════════════════════════════

class SpreadTracker:
    def __init__(self):
        self._windows: dict[tuple[str, str], deque[tuple[float, float]]] = {}

    def _evict_old(self, q: deque, now: float) -> None:
        cutoff = now - ADAPT_WINDOW_SECONDS
        while q and q[0][0] < cutoff:
            q.popleft()

    def record(self, strategy: str, ticker: str, spread: float) -> None:
        key = (strategy, ticker)
        q = self._windows.get(key)
        if q is None:
            q = deque()
            self._windows[key] = q
        now = time.monotonic()
        q.append((now, max(0.0, float(spread))))
        self._evict_old(q, now)

    def threshold(self, strategy: str, ticker: str, base_floor: int) -> int:
        q = self._windows.get((strategy, ticker))
        if q is None:
            return base_floor
        now = time.monotonic()
        self._evict_old(q, now)
        if len(q) < ADAPT_MIN_SAMPLES:
            return base_floor
        sorted_q = sorted(s for _, s in q)
        idx = int(ADAPT_QUANTILE * len(sorted_q))
        return max(base_floor, int(sorted_q[idx]))

    def stats(self) -> dict[str, int]:
        return {f"{s}-{t}": len(q) for (s, t), q in self._windows.items()}


# ════════════════════════════════════════════════════════════════════════════
# Per-strategy P&L stats
# ════════════════════════════════════════════════════════════════════════════

class StatsTracker:
    def __init__(self):
        self.realized_by_strategy: dict[str, int] = defaultdict(int)
        self.fills_by_strategy: dict[str, int] = defaultdict(int)
        self.attempts_by_strategy: dict[str, int] = defaultdict(int)

    def record_attempt(self, strategy: str) -> None:
        self.attempts_by_strategy[strategy] += 1

    def record_fill(self, strategy: str, cash_delta_cents: int) -> None:
        self.realized_by_strategy[strategy] += cash_delta_cents
        self.fills_by_strategy[strategy] += 1

    def summary(self) -> str:
        if not self.realized_by_strategy:
            return ""
        lines = []
        for s in sorted(self.realized_by_strategy, key=lambda k: -self.realized_by_strategy[k]):
            lines.append(f"{s}: ${self.realized_by_strategy[s]/100:+.0f} "
                         f"({self.fills_by_strategy[s]} fills, "
                         f"{self.attempts_by_strategy.get(s, 0)} attempts)")
        return " | ".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# Token bucket
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
            await asyncio.sleep((1.0 - self.tokens) / self.rate)


# ════════════════════════════════════════════════════════════════════════════
# Plan / Leg
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Leg:
    exchange: str
    ticker: str
    side: str
    qty: int
    price: int                   # IOC limit price
    order_type: str = "ioc"
    note: str = ""
    strategy: str = ""
    expected_avg_price: Optional[int] = None

    @property
    def cost_price(self) -> int:
        return self.expected_avg_price if self.expected_avg_price is not None else self.price

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
# Hub
# ════════════════════════════════════════════════════════════════════════════

class Hub:
    def __init__(self, active_venues: list[str], my_cluster: str = "NA"):
        self.active_venues = active_venues
        self.my_cluster = my_cluster
        self.stop = False
        self.connections: dict[str, "Connection"] = {}

        self.books: dict[tuple[str, str], Book] = {}
        for tk, vs in LISTINGS.items():
            for v in vs:
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

        self.last_fired: dict[str, float] = defaultdict(float)
        self._req_seq = 0

        self.fills_count = 0
        self.realized_cents = 0
        self.last_log = time.monotonic()
        self.last_mm_refresh: dict[tuple[str, str], float] = defaultdict(float)
        self.fills_by_venue: dict[str, int] = defaultdict(int)
        self.realized_by_venue: dict[str, int] = defaultdict(int)

        self.spread_tracker = SpreadTracker()
        self.stats = StatsTracker()

        # ─── Latency-aware execution state ─────────────────────────────────
        # EWMA round-trip (in seconds) per venue, populated by on_add_order_response.
        self.rtt_ewma: dict[str, float] = {}
        # rid → monotonic send time. Cleared when the response arrives.
        self.req_send_time: dict[str, float] = {}
        # IOC attempts and "miss" counts per (venue, ticker). A miss = server
        # accepted the order but no contra fills happened — the level was gone.
        self.fill_attempts: dict[tuple[str, str], int] = defaultdict(int)
        self.fill_misses:   dict[tuple[str, str], int] = defaultdict(int)
        # Rolling per-ticker mid history for drift estimation.
        self.mid_history: dict[str, deque[tuple[float, float]]] = defaultdict(deque)

    def req_id(self, tag: str) -> str:
        self._req_seq += 1
        return f"p{self._req_seq:08x}-{tag}"

    # ─── Latency / fill-rate / drift helpers ─────────────────────────
    def measured_rtt_ms(self, venue: str) -> float:
        """EWMA round-trip in ms; falls back to cluster default before measurement."""
        s = self.rtt_ewma.get(venue)
        if s is not None:
            return s * 1000.0
        if CLUSTER_OF.get(venue) == self.my_cluster:
            return RTT_DEFAULT_INTRA_MS
        return RTT_DEFAULT_CROSS_MS

    def drift_per_ms(self, ticker: str) -> float:
        """Mean abs Δmid per ms over the recent window. Falls back to a
        plausible constant before we have enough samples."""
        hist = self.mid_history.get(ticker)
        if hist is None or len(hist) < 2:
            return DRIFT_DEFAULT_C_PER_MS
        items = list(hist)
        total_drift = 0.0
        total_dt_ms = 0.0
        prev_t, prev_mid = items[0]
        for t, mid in items[1:]:
            dt_ms = (t - prev_t) * 1000.0
            if dt_ms > 0:
                total_drift += abs(mid - prev_mid)
                total_dt_ms += dt_ms
            prev_t, prev_mid = t, mid
        if total_dt_ms <= 0:
            return DRIFT_DEFAULT_C_PER_MS
        return total_drift / total_dt_ms

    def latency_penalty_cents(self, venue: str, ticker: str) -> float:
        """Expected price drift in cents during the time it takes our IOC to
        reach this venue.  Adds onto asks, subtracts from bids."""
        return (LATENCY_PENALTY_LAMBDA
                * self.measured_rtt_ms(venue)
                * self.drift_per_ms(ticker))

    def fill_prob(self, venue: str, ticker: str) -> float:
        """Smoothed empirical IOC fill probability for (venue, ticker).
        Beta posterior with prior (FILL_PROB_PRIOR_N, FILL_PROB_PRIOR_P)."""
        key = (venue, ticker)
        n = self.fill_attempts.get(key, 0)
        misses = self.fill_misses.get(key, 0)
        fills = n - misses
        return ((fills + FILL_PROB_PRIOR_N * FILL_PROB_PRIOR_P)
                / (n + FILL_PROB_PRIOR_N))

    def rtt_qty_haircut(self, venue: str) -> float:
        """Multiplier on best_qty for IOC sizing — shrinks as RTT grows."""
        rtt_ms = self.measured_rtt_ms(venue)
        if rtt_ms <= 0:
            return 1.0
        factor = RTT_QTY_HAIRCUT_KNEE_MS / (RTT_QTY_HAIRCUT_KNEE_MS + rtt_ms)
        return max(RTT_QTY_HAIRCUT_FLOOR, factor)

    def book_is_fresh(self, key: tuple[str, str]) -> bool:
        bk = self.books.get(key)
        if bk is None or bk.last_update_wall == 0.0:
            return False
        return (time.monotonic() - bk.last_update_wall) <= STALE_BOOK_S

    # ─── inventory taper ─────────────────────────────────────────────
    def inventory_taper(self, exchange: str, ticker: str, side: str) -> float:
        pos = self.pos[(exchange, ticker)]
        if side == "bid":
            cap = SOFT_POS_MAX
            frac_used = pos / cap if cap > 0 else 0
        else:
            cap = SOFT_POS_MIN
            frac_used = pos / cap if cap < 0 else 0
        frac_used = max(0.0, min(1.0, frac_used))
        if frac_used <= TAPER_START_FRAC:
            return 1.0
        if frac_used >= TAPER_END_FRAC:
            return 0.05
        t = (frac_used - TAPER_START_FRAC) / (TAPER_END_FRAC - TAPER_START_FRAC)
        return max(0.05, (1.0 - t) ** 2)

    # ─── headroom checks ─────────────────────────────────────────────
    def fits(self, leg: Leg) -> bool:
        if not self.ready.get(leg.exchange, False):
            return False
        new_pos = self.pos[(leg.exchange, leg.ticker)] + leg.signed_qty
        if new_pos > SOFT_POS_MAX or new_pos < SOFT_POS_MIN:
            return False
        if leg.side == "bid":
            cost = leg.qty * leg.cost_price
            if self.cash[leg.exchange] - cost < SOFT_CASH_FLOOR:
                return False
        if self.open_count[leg.exchange] + 1 > MAX_PENDING_ORDERS - 50:
            return False
        return True

    def commit_optimistic(self, leg: Leg) -> None:
        self.pos[(leg.exchange, leg.ticker)] += leg.signed_qty
        if leg.side == "bid":
            self.cash[leg.exchange] -= leg.qty * leg.cost_price
        else:
            self.cash[leg.exchange] += leg.qty * leg.cost_price

    def revert_optimistic(self, leg: Leg) -> None:
        self.pos[(leg.exchange, leg.ticker)] -= leg.signed_qty
        if leg.side == "bid":
            self.cash[leg.exchange] += leg.qty * leg.cost_price
        else:
            self.cash[leg.exchange] -= leg.qty * leg.cost_price

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
        self.stats.record_attempt(plan.strategy)
        log.info("FIRE %r", plan)
        for leg in plan.legs:
            leg.strategy = plan.strategy
            self.commit_optimistic(leg)
            if not self._send_leg(leg):
                self.revert_optimistic(leg)

    def fire_one(self, leg: Leg) -> None:
        if not self.fits(leg):
            return
        self.commit_optimistic(leg)
        self.stats.record_attempt(leg.strategy or "MM")
        if not self._send_leg(leg):
            self.revert_optimistic(leg)

    def _send_leg(self, leg: Leg) -> bool:
        conn = self.connections.get(leg.exchange)
        if conn is None or not conn.connected:
            return False
        rid = self.req_id(f"{leg.note[:16]}" if leg.note else leg.ticker)
        self.req_to_leg[rid] = leg
        # Stamp send time so the response can compute round-trip latency.
        self.req_send_time[rid] = time.monotonic()
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
    def on_welcome(self, exchange: str, msg: Optional[dict] = None) -> None:
        log.info("welcome %s", exchange)
        if msg is not None:
            rl = msg.get("round_length")
            if isinstance(rl, int) and rl > 0:
                self.round_length[exchange] = rl
            t = msg.get("time")
            if isinstance(t, int):
                self.server_time[exchange] = t
        self.ready[exchange] = False
        for tk in self.tickers_on_ex.get(exchange, set()):
            self.pos[(exchange, tk)] = 0
            book = self.books.get((exchange, tk))
            if book is not None:
                book.bids = {}
                book.asks = {}
                # Reset freshness so the stale-book guard rejects this venue
                # until the first new market_data tick lands.
                book.last_update_wall = 0.0
        self.cash[exchange] = INITIAL_CASH
        for oid in list(self.live_orders):
            if self.live_orders[oid][0] == exchange:
                self.live_orders.pop(oid, None)
        self.open_count[exchange] = 0
        for rid in [r for r, lg in self.req_to_leg.items() if lg.exchange == exchange]:
            self.req_to_leg.pop(rid, None)
            self.req_send_time.pop(rid, None)
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
        for inst, depth in msg.get("orderbook_depths", {}).items():
            ex, _, tk = inst.partition("-")
            if ex != exchange:
                continue
            book = self.books.get((ex, tk))
            if book is None:
                continue
            book.update(depth)
            # Track per-ticker mid evolution for drift estimation.
            mid = book.mid
            if mid is not None:
                hist = self.mid_history[tk]
                now = book.last_update_wall  # already monotonic
                hist.append((now, mid))
                cutoff = now - DRIFT_WINDOW_S
                while hist and hist[0][0] < cutoff:
                    hist.popleft()
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
                cash_delta = -fill_qty * fill_px if side == "bid" else fill_qty * fill_px
                self.cash[ex] += cash_delta
                self.fills_count += 1
                self.fills_by_venue[ex] += 1
                self.realized_by_venue[ex] += cash_delta
                self.stats.record_fill("MM", cash_delta)
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

        # RTT measurement — record even if we've already lost the leg mapping,
        # so the EWMA stays responsive.
        send_t = self.req_send_time.pop(rid, None)
        if send_t is not None:
            rtt_s = time.monotonic() - send_t
            cur = self.rtt_ewma.get(exchange)
            self.rtt_ewma[exchange] = (
                rtt_s if cur is None
                else RTT_EWMA_ALPHA * rtt_s + (1 - RTT_EWMA_ALPHA) * cur
            )

        if leg is None:
            return
        self.revert_optimistic(leg)

        # IOC fill-rate tracking. A successful response with no inventory
        # change means the order was accepted but the level vanished before
        # the match engine could pair us — that's the race we're trying
        # to avoid.  Limit orders rest in the book so this metric only
        # makes sense for IOC.
        if leg.order_type == "ioc":
            key = (leg.exchange, leg.ticker)
            self.fill_attempts[key] += 1
            ic_raw = data.get("immediate_inventory_change") if success else None
            if not success or ic_raw is None or int(ic_raw) == 0:
                self.fill_misses[key] += 1

        if not success:
            log.debug("order failed %s: %s", leg.note, data.get("message"))
            self.open_count[exchange] = max(0, self.open_count[exchange] - 1)
            return
        ic = data.get("immediate_inventory_change")
        bc = data.get("immediate_balance_change")
        if ic is not None:
            self.pos[(leg.exchange, leg.ticker)] += int(ic)
            self.fills_count += 1
            self.fills_by_venue[leg.exchange] += 1
        if bc is not None:
            self.cash[leg.exchange] += int(bc)
            self.realized_cents += int(bc)
            self.realized_by_venue[leg.exchange] += int(bc)
            if leg.strategy:
                self.stats.record_fill(leg.strategy, int(bc))
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

    # ─── strategy bus ────────────────────────────────────────────────
    def strategize(self) -> None:
        plans: list[Plan] = []
        plans += self.etf_basket_arbs()
        plans += self.sub_etf_arbs()
        plans += self.cross_venue_arbs()
        plans += self.settlement_unwind()
        for plan in plans:
            self.fire(plan)

        self.passive_mm_refresh()

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
                book_count, total_cash / 100, total_pos_val / 100,
                self.fills_count, self.realized_cents / 100,
                sum(self.open_count.values()), len(self.live_orders),
            )
            venue_lines = []
            for v in self.active_venues:
                quoted = sum(1 for tk in self.tickers_on_ex[v]
                             if self.books[(v, tk)].mid is not None)
                listed = len(self.tickers_on_ex[v])
                fills = self.fills_by_venue.get(v, 0)
                realized = self.realized_by_venue.get(v, 0)
                ready_mark = "" if self.ready.get(v) else " NOT-READY"
                rtt_ms = self.measured_rtt_ms(v)
                rtt_tag = f"{rtt_ms:.0f}ms" + ("" if v in self.rtt_ewma else "*")
                venue_lines.append(
                    f"{v}: books={quoted}/{listed} fills={fills} "
                    f"realized=${realized/100:+.0f} rtt={rtt_tag}{ready_mark}"
                )
            log.info("[hb-venue] %s", " | ".join(venue_lines))
            # Surface the worst-fill (venue, ticker) pairs so we can spot
            # races we're consistently losing.  Only show pairs with
            # enough attempts to be statistically meaningful.
            bad = []
            for key, attempts in self.fill_attempts.items():
                if attempts < 5:
                    continue
                misses = self.fill_misses.get(key, 0)
                fp = (attempts - misses) / attempts
                if fp < 0.5:
                    bad.append((fp, key, attempts, misses))
            bad.sort()
            if bad:
                log.info("[fill-prob] %s", " | ".join(
                    f"{v}-{t}: {fp:.0%} ({a-m}/{a})"
                    for fp, (v, t), a, m in bad[:6]
                ))
            summary = self.stats.summary()
            if summary:
                log.info("[strat] %s", summary)

    # ─── venue selection: latency-priced ─────────────────────────────
    def _best_buy_venue(self, ticker: str, _legacy_cluster: Optional[str] = None):
        """Lowest *latency-adjusted* ask across active, fresh-book venues.

        effective_price = ask + drift_per_ms × measured_rtt_ms.  A 200ms-RTT
        venue with 3¢ better nominal price will lose to a 30ms venue when
        expected drift wipes the gap.  Returns (venue, nominal_ask, qty)
        for API compatibility with v2."""
        best = None  # (effective, ask_px, qty, venue)
        for v in LISTINGS[ticker]:
            if v not in self.active_venues:
                continue
            if not self.book_is_fresh((v, ticker)):
                continue
            bk = self.books[(v, ticker)]
            ba = bk.best_ask
            if ba is None:
                continue
            eff = ba + self.latency_penalty_cents(v, ticker)
            if best is None or eff < best[0]:
                best = (eff, ba, bk.best_ask_qty, v)
        if best is None:
            return None
        return best[3], best[1], best[2]

    def _best_sell_venue(self, ticker: str, _legacy_cluster: Optional[str] = None):
        """Highest *latency-adjusted* bid across active, fresh-book venues.

        effective_price = bid - drift_per_ms × measured_rtt_ms.  Returns
        (venue, nominal_bid, qty)."""
        best = None  # (effective, bid_px, qty, venue)
        for v in LISTINGS[ticker]:
            if v not in self.active_venues:
                continue
            if not self.book_is_fresh((v, ticker)):
                continue
            bk = self.books[(v, ticker)]
            bb = bk.best_bid
            if bb is None:
                continue
            eff = bb - self.latency_penalty_cents(v, ticker)
            if best is None or eff > best[0]:
                best = (eff, bb, bk.best_bid_qty, v)
        if best is None:
            return None
        return best[3], best[1], best[2]

    # ─── strategy 1: ETF <-> basket arb ──────────────────────────────
    def etf_basket_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for etf in ETFS:
            basket = ETF_BASKETS[etf]
            n = len(basket)
            const_bid: dict[str, tuple[str, int, int]] = {}
            const_ask: dict[str, tuple[str, int, int]] = {}
            ok = True
            for tk in basket:
                bb = self._best_sell_venue(tk)
                ba = self._best_buy_venue(tk)
                if bb is None or ba is None:
                    ok = False; break
                const_bid[tk] = bb
                const_ask[tk] = ba
            if not ok:
                continue
            for etf_v in LISTINGS[etf]:
                if etf_v not in self.active_venues:
                    continue
                if not self.book_is_fresh((etf_v, etf)):
                    continue
                bk = self.books[(etf_v, etf)]
                if bk.best_bid is None or bk.best_ask is None:
                    continue

                # Direction A: ETF cheap → buy ETF, sell basket
                etf_buy_px = bk.best_ask
                etf_buy_qty = bk.best_ask_qty
                synth_sell_total = sum(const_bid[tk][1] for tk in basket)
                edge_a = synth_sell_total / n - etf_buy_px

                if edge_a > 0:
                    self.spread_tracker.record("ETF_BUY", etf, edge_a)
                threshold = self.spread_tracker.threshold("ETF_BUY", etf, BASE_ARB_EDGE)

                if edge_a >= threshold:
                    taper = self.inventory_taper(etf_v, etf, "bid")
                    k = etf_buy_qty // n
                    for tk in basket:
                        k = min(k, const_bid[tk][2])
                    etf_room = (SOFT_POS_MAX - self.pos[(etf_v, etf)]) // n
                    k = min(k, etf_room)
                    for tk in basket:
                        v, _, _ = const_bid[tk]
                        room = self.pos[(v, tk)] - SOFT_POS_MIN
                        k = min(k, room)
                    cash_room = ((self.cash[etf_v] - SOFT_CASH_FLOOR) // (n * etf_buy_px)
                                 if etf_buy_px > 0 else 0)
                    k = min(k, cash_room, ARB_MAX_K)
                    k = max(0, int(k * taper))
                    if k > 0:
                        legs = [Leg(etf_v, etf, "bid", k * n, etf_buy_px,
                                    note=f"etfarb_buy_{etf}@{etf_v}")]
                        for tk in basket:
                            v, px, _ = const_bid[tk]
                            legs.append(Leg(v, tk, "ask", k, px, note=f"etfarb_sell_{tk}@{v}"))
                        out.append(Plan(legs, edge_cents=edge_a,
                                        strategy=f"ETF-NAV {etf}@{etf_v} long"))

                # Direction B: ETF rich → sell ETF, buy basket
                etf_sell_px = bk.best_bid
                etf_sell_qty = bk.best_bid_qty
                synth_buy_total = sum(const_ask[tk][1] for tk in basket)
                edge_b = etf_sell_px - synth_buy_total / n
                if edge_b > 0:
                    self.spread_tracker.record("ETF_SELL", etf, edge_b)
                threshold = self.spread_tracker.threshold("ETF_SELL", etf, BASE_ARB_EDGE)

                if edge_b >= threshold:
                    taper = self.inventory_taper(etf_v, etf, "ask")
                    k = etf_sell_qty // n
                    for tk in basket:
                        k = min(k, const_ask[tk][2])
                    etf_short_room = self.pos[(etf_v, etf)] - SOFT_POS_MIN
                    k = min(k, etf_short_room // n)
                    for tk in basket:
                        v, px, _ = const_ask[tk]
                        room = (SOFT_POS_MAX - self.pos[(v, tk)])
                        k = min(k, room)
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
                    k = max(0, int(k * taper))
                    if k > 0:
                        legs = [Leg(etf_v, etf, "ask", k * n, etf_sell_px,
                                    note=f"etfarb_sell_{etf}@{etf_v}")]
                        for tk in basket:
                            v, px, _ = const_ask[tk]
                            legs.append(Leg(v, tk, "bid", k, px, note=f"etfarb_buy_{tk}@{v}"))
                        out.append(Plan(legs, edge_cents=edge_b,
                                        strategy=f"ETF-NAV {etf}@{etf_v} short"))
        return out

    # ─── strategy 2: sub-ETF arb ─────────────────────────────────────
    def sub_etf_arbs(self) -> list[Plan]:
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
                bb = self._best_sell_venue(tk)
                ba = self._best_buy_venue(tk)
                if bb is None or ba is None:
                    ok = False; break
                comp_bid[tk] = bb
                comp_ask[tk] = ba
            if not ok:
                continue

            def best_quote(tk, venues, side):
                """Latency-adjusted best quote on the requested side."""
                best = None  # (v, px, qty, eff)
                for v in venues:
                    if not self.book_is_fresh((v, tk)):
                        continue
                    bk = self.books[(v, tk)]
                    if side == "bid":   # we want to buy → score the ASK
                        px = bk.best_ask; qty = bk.best_ask_qty
                        if px is None: continue
                        eff = px + self.latency_penalty_cents(v, tk)
                        if best is None or eff < best[3]:
                            best = (v, px, qty, eff)
                    else:               # we want to sell → score the BID
                        px = bk.best_bid; qty = bk.best_bid_qty
                        if px is None: continue
                        eff = px - self.latency_penalty_cents(v, tk)
                        if best is None or eff > best[3]:
                            best = (v, px, qty, eff)
                return best[:3] if best else None

            sup_buy = best_quote(super_etf, sup_venues, "bid")
            sup_sell = best_quote(super_etf, sup_venues, "ask")
            sub_buy = best_quote(sub_etf, sub_venues, "bid")
            sub_sell = best_quote(sub_etf, sub_venues, "ask")
            if not (sup_buy and sup_sell and sub_buy and sub_sell):
                continue

            # Direction A: super cheap → long super, short sub + complement.
            edge_a = 3*sub_sell[1] + sum(comp_bid[tk][1] for tk in complement) - 6*sup_buy[1]
            edge_a_per_share = edge_a / 6
            if edge_a > 0:
                self.spread_tracker.record("SUB_ETF_A", super_etf, edge_a_per_share)
            thr = self.spread_tracker.threshold("SUB_ETF_A", super_etf, BASE_SUB_ETF_EDGE)
            if edge_a_per_share >= thr:
                taper = self.inventory_taper(sup_buy[0], super_etf, "bid")
                k = min(
                    sup_buy[2] // 6, sub_sell[2] // 3,
                    *(comp_bid[tk][2] for tk in complement),
                    (SOFT_POS_MAX - self.pos[(sup_buy[0], super_etf)]) // 6,
                    (self.pos[(sub_sell[0], sub_etf)] - SOFT_POS_MIN) // 3,
                    *((self.pos[(comp_bid[tk][0], tk)] - SOFT_POS_MIN) for tk in complement),
                    (self.cash[sup_buy[0]] - SOFT_CASH_FLOOR) // (6 * sup_buy[1]) if sup_buy[1] else 0,
                    ARB_MAX_K,
                )
                k = max(0, int(k * taper))
                if k > 0:
                    legs = [
                        Leg(sup_buy[0], super_etf, "bid", 6*k, sup_buy[1], note=f"sub_long_{super_etf}"),
                        Leg(sub_sell[0], sub_etf,  "ask", 3*k, sub_sell[1], note=f"sub_short_{sub_etf}"),
                    ]
                    for tk in complement:
                        v, px, _ = comp_bid[tk]
                        legs.append(Leg(v, tk, "ask", k, px, note=f"sub_short_{tk}"))
                    out.append(Plan(legs, edge_cents=edge_a_per_share,
                                    strategy=f"SUB-ETF {super_etf}/{sub_etf} long"))

            # Direction B: super rich → sell super, buy sub + complement.
            edge_b = 6*sup_sell[1] - 3*sub_buy[1] - sum(comp_ask[tk][1] for tk in complement)
            edge_b_per_share = edge_b / 6
            if edge_b > 0:
                self.spread_tracker.record("SUB_ETF_B", super_etf, edge_b_per_share)
            thr = self.spread_tracker.threshold("SUB_ETF_B", super_etf, BASE_SUB_ETF_EDGE)
            if edge_b_per_share >= thr:
                taper = self.inventory_taper(sup_sell[0], super_etf, "ask")
                buys_per_k_per_venue: dict[str, int] = defaultdict(int)
                buys_per_k_per_venue[sub_buy[0]] += 3 * sub_buy[1]
                for tk in complement:
                    v, px, _ = comp_ask[tk]
                    buys_per_k_per_venue[v] += px
                cash_k = ARB_MAX_K
                for v, per_k_cost in buys_per_k_per_venue.items():
                    if per_k_cost > 0:
                        cash_k = min(cash_k, (self.cash[v] - SOFT_CASH_FLOOR) // per_k_cost)
                k = min(
                    sup_sell[2] // 6, sub_buy[2] // 3,
                    *(comp_ask[tk][2] for tk in complement),
                    (self.pos[(sup_sell[0], super_etf)] - SOFT_POS_MIN) // 6,
                    (SOFT_POS_MAX - self.pos[(sub_buy[0], sub_etf)]) // 3,
                    *((SOFT_POS_MAX - self.pos[(comp_ask[tk][0], tk)]) for tk in complement),
                    cash_k, ARB_MAX_K,
                )
                k = max(0, int(k * taper))
                if k > 0:
                    legs = [
                        Leg(sup_sell[0], super_etf, "ask", 6*k, sup_sell[1], note=f"sub_short_{super_etf}"),
                        Leg(sub_buy[0], sub_etf,    "bid", 3*k, sub_buy[1],  note=f"sub_long_{sub_etf}"),
                    ]
                    for tk in complement:
                        v, px, _ = comp_ask[tk]
                        legs.append(Leg(v, tk, "bid", k, px, note=f"sub_long_{tk}"))
                    out.append(Plan(legs, edge_cents=edge_b_per_share,
                                    strategy=f"SUB-ETF {super_etf}/{sub_etf} short"))
        return out

    # ─── strategy 3: cross-venue arb ─────────────────────────────────
    def cross_venue_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for tk in LISTINGS:
            venues = [v for v in LISTINGS[tk] if v in self.active_venues]
            if len(venues) < 2:
                continue

            # Per-side latency-adjusted best (max effective bid, min effective ask).
            # Stale books are skipped — acting on rotten data is the worst case.
            best_b = best_a = None
            for v in venues:
                if not self.book_is_fresh((v, tk)):
                    continue
                bk = self.books[(v, tk)]
                pen = self.latency_penalty_cents(v, tk)
                if bk.best_bid is not None:
                    eff_bid = bk.best_bid - pen
                    if best_b is None or eff_bid > best_b[3]:
                        best_b = (v, bk.best_bid, bk.best_bid_qty, eff_bid)
                if bk.best_ask is not None:
                    eff_ask = bk.best_ask + pen
                    if best_a is None or eff_ask < best_a[3]:
                        best_a = (v, bk.best_ask, bk.best_ask_qty, eff_ask)
            if not best_b or not best_a or best_b[0] == best_a[0]:
                continue

            buy_v, buy_px_top, _, eff_ask = best_a
            sell_v, sell_px_top, _, eff_bid = best_b

            nominal_edge = sell_px_top - buy_px_top
            if nominal_edge <= 0:
                continue

            # Record nominal (market) spread so the adaptive threshold reflects
            # the market itself, not our latency cost.
            self.spread_tracker.record("XV", tk, nominal_edge)
            base_threshold = self.spread_tracker.threshold("XV", tk, BASE_XV_EDGE)

            # Effective edge after expected drift during our IOC's flight.
            drift_adder = self.latency_penalty_cents(buy_v, tk) + self.latency_penalty_cents(sell_v, tk)
            effective_edge = nominal_edge - drift_adder

            # Scale the bar by inverse fill probability — if we historically
            # win 30% of races on this pair, average outcomes only break
            # even with ~3.3× the nominal edge floor.
            fp_buy = self.fill_prob(buy_v, tk)
            fp_sell = self.fill_prob(sell_v, tk)
            fp = max(FILL_PROB_FLOOR, min(fp_buy, fp_sell))
            required_edge = base_threshold / fp

            if effective_edge < required_edge:
                continue

            # Depth walk: see if levels 2-3 still clear required_edge after drift.
            buy_book = self.books[(buy_v, tk)]
            sell_book = self.books[(sell_v, tk)]
            buy_limit = buy_px_top + DEPTH_WALK_CENTS
            sell_limit = sell_px_top - DEPTH_WALK_CENTS
            walk_buy_qty, buy_avg = buy_book.buy_depth_to(buy_limit)
            walk_sell_qty, sell_avg = sell_book.sell_depth_to(sell_limit)

            if (buy_avg is not None and sell_avg is not None
                    and (sell_avg - buy_avg - drift_adder) >= required_edge):
                buy_qty = walk_buy_qty
                sell_qty = walk_sell_qty
                buy_px = buy_limit
                sell_px = sell_limit
                exp_buy = int(buy_avg)
                exp_sell = int(sell_avg)
                walked_edge = sell_avg - buy_avg
            else:
                buy_qty = best_a[2]
                sell_qty = best_b[2]
                buy_px = buy_px_top
                sell_px = sell_px_top
                exp_buy = exp_sell = None
                walked_edge = nominal_edge

            qty = min(buy_qty, sell_qty, XV_MAX_QTY)

            taper_buy = self.inventory_taper(buy_v, tk, "bid")
            taper_sell = self.inventory_taper(sell_v, tk, "ask")
            taper = min(taper_buy, taper_sell)

            # RTT haircut — slow venues get smaller IOCs because the level
            # is more likely to be partially eaten by faster counterparties.
            rtt_haircut = min(self.rtt_qty_haircut(buy_v), self.rtt_qty_haircut(sell_v))

            qty = max(0, int(qty * taper * rtt_haircut))

            qty = min(qty, SOFT_POS_MAX - self.pos[(buy_v, tk)])
            qty = min(qty, self.pos[(sell_v, tk)] - SOFT_POS_MIN)
            buy_cost_per_share = exp_buy if exp_buy is not None else buy_px
            if buy_cost_per_share > 0:
                qty = min(qty, (self.cash[buy_v] - SOFT_CASH_FLOOR) // buy_cost_per_share)
            if qty > 0:
                out.append(Plan(
                    [Leg(buy_v, tk, "bid", qty, buy_px,
                         expected_avg_price=exp_buy,
                         note=f"xv_buy_{tk}@{buy_v}"),
                     Leg(sell_v, tk, "ask", qty, sell_px,
                         expected_avg_price=exp_sell,
                         note=f"xv_sell_{tk}@{sell_v}")],
                    edge_cents=walked_edge,
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
            urgency = 1.0 - remaining / EOS_UNWIND_MS
            for (ex2, tk), pos in list(self.pos.items()):
                if ex2 != ex or pos == 0:
                    continue
                bk = self.books.get((ex, tk))
                if bk is None:
                    continue
                target_qty = abs(pos)
                aggressive = remaining <= EOS_FLATTEN_MS
                if pos > 0:
                    bb = bk.best_bid
                    if bb is None: continue
                    qty = min(target_qty, bk.best_bid_qty,
                              max(1, int(target_qty * (urgency if not aggressive else 1.0))))
                    px = bb if not aggressive else max(1, bb - 5)
                    out.append(Plan(
                        [Leg(ex, tk, "ask", qty, px, note=f"unwind_long_{tk}")],
                        edge_cents=0.0, strategy=f"UNWIND long {tk}@{ex}",
                    ))
                else:
                    ba = bk.best_ask
                    if ba is None: continue
                    qty = min(target_qty, bk.best_ask_qty,
                              max(1, int(target_qty * (urgency if not aggressive else 1.0))))
                    px = ba if not aggressive else ba + 5
                    out.append(Plan(
                        [Leg(ex, tk, "bid", qty, px, note=f"unwind_short_{tk}")],
                        edge_cents=0.0, strategy=f"UNWIND short {tk}@{ex}",
                    ))
        return out

    # ─── strategy 4: passive MM ──────────────────────────────────────
    def passive_mm_refresh(self) -> None:
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
                if bb is None or ba is None or ba - bb < 2 * MM_INSIDE_TICK + 1:
                    continue
                pos = self.pos[key]

                mp = bk.microprice
                m = bk.mid
                skew = 0
                if mp is not None and m is not None:
                    if mp > m + 1:
                        skew = 1
                    elif mp < m - 1:
                        skew = -1

                bid_px = bb + MM_INSIDE_TICK + skew
                ask_px = ba - MM_INSIDE_TICK + skew
                if bid_px >= ask_px:
                    continue

                have_bid_at_px = have_ask_at_px = False
                stale_oids: list[int] = []
                for oid, (ex0, tk0, side0, _q0, px0) in self.live_orders.items():
                    if ex0 != v or tk0 != tk: continue
                    if side0 == "bid" and px0 == bid_px:
                        have_bid_at_px = True
                    elif side0 == "ask" and px0 == ask_px:
                        have_ask_at_px = True
                    else:
                        stale_oids.append(oid)
                conn = self.connections.get(v)
                if conn and conn.connected:
                    for oid in stale_oids:
                        conn.enqueue({
                            "type": "cancel_order",
                            "user_request_id": self.req_id(f"cxl-{tk}"),
                            "order_id": oid,
                            "instrument_id": f"{v}-{tk}",
                        })

                buy_taper = self.inventory_taper(v, tk, "bid")
                sell_taper = self.inventory_taper(v, tk, "ask")
                buy_qty = max(1, int(MM_QTY * buy_taper))
                sell_qty = max(1, int(MM_QTY * sell_taper))

                if (not have_bid_at_px
                        and pos < SOFT_POS_MAX - buy_qty
                        and self.cash[v] - buy_qty * bid_px > SOFT_CASH_FLOOR):
                    leg = Leg(v, tk, "bid", buy_qty, bid_px,
                              order_type="limit", note=f"mm_bid_{tk}", strategy="MM")
                    self.fire_one(leg)
                if not have_ask_at_px and pos > SOFT_POS_MIN + sell_qty:
                    leg = Leg(v, tk, "ask", sell_qty, ask_px,
                              order_type="limit", note=f"mm_ask_{tk}", strategy="MM")
                    self.fire_one(leg)


# ════════════════════════════════════════════════════════════════════════════
# Connection (unchanged from v1)
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
                async with ws_connect(self.url, open_timeout=5, ping_interval=20,
                                      ping_timeout=10, max_size=2**24) as ws:
                    self.ws = ws
                    self.connected = True
                    backoff = 0.1
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    welcome = json.loads(raw)
                    if welcome.get("type") != "welcome":
                        log.warning("%s unexpected first msg: %s", self.exchange, welcome)
                    self.hub.on_welcome(self.exchange, welcome)
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

async def auto_detect_cluster() -> str:
    async def probe(venue: str):
        host = WS_HOSTS[venue]
        t0 = time.monotonic()
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, 9001), timeout=2.0
            )
            elapsed = time.monotonic() - t0
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return elapsed
        except Exception:
            return None

    log.info("auto-detect: probing venue latencies...")
    rtts = await asyncio.gather(*(probe(v) for v in VENUES))
    pairs = [(v, r) for v, r in zip(VENUES, rtts) if r is not None]
    if not pairs:
        log.warning("auto-detect: no venues reachable (between rounds?), defaulting to NA")
        return "NA"
    pairs.sort(key=lambda x: x[1])
    fastest_v, fastest_rtt = pairs[0]
    cluster = CLUSTER_OF.get(fastest_v, "NA")
    log.info("auto-detect: fastest=%s (%.1fms) → cluster=%s",
             fastest_v, fastest_rtt * 1000, cluster)
    log.info("auto-detect rtts: %s",
             ", ".join(f"{v}={r*1000:.0f}ms" for v, r in pairs))
    return cluster


async def amain(active_venues: list[str], my_cluster: str) -> None:
    hub = Hub(active_venues, my_cluster)
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
    p = argparse.ArgumentParser()
    p.add_argument("--venues", default=",".join(VENUES),
                   help="Comma-separated venue subset")
    p.add_argument("--location", default="auto",
                   choices=["auto", "NA", "EU", "ASIA", "IN", "ZSE"],
                   help="Latency cluster the bot is running from. "
                        "'auto' probes venues at startup and picks the lowest-RTT cluster. "
                        "Override when probing is unreliable (between rounds, debugging).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    venue_canon = {v.upper(): v for v in VENUES}
    active = []
    for v in args.venues.split(","):
        v = v.strip()
        if not v:
            continue
        canon = venue_canon.get(v.upper())
        if canon is None:
            raise SystemExit(f"unknown venue: {v!r} (valid: {VENUES})")
        if canon not in active:
            active.append(canon)
    log.info("starting prism2 on venues: %s", active)

    async def run() -> None:
        if args.location == "auto":
            my_cluster = await auto_detect_cluster()
        else:
            my_cluster = args.location
            log.info("location override: cluster=%s", my_cluster)
        await amain(active, my_cluster)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
