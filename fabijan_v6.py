#!/usr/bin/env python3
"""
fabijan_v6.py — AlgoTrade 2026 trading bot, fabijan v6.

Forked from fabijan_v6 (the previous best — scalper profile, ~80% cash).
v5 worked; v6 pushes the same dial harder. Same architecture, same strategies,
just smaller bites taken more often.

The changes vs fabijan_v6:

  1. **Even tighter inventory caps.** SOFT_POS_MAX 250 → 100. Per-line peak
     deployment drops from ~$25k to ~$10k at $100/share. Across 25
     instruments × 10 exchanges, theoretical worst-case capital lock-up
     ≈ $2.5M; in practice positions sit far below this thanks to the taper
     and inventory-aware skew, so we live near full cash on every venue.

  2. **Even smaller per-fire size.** ARB_MAX_K 5 → 2, XV_MAX_QTY 30 → 10,
     MM_QTY 3 → 2. A single ETFA basket fire is now 2 ETF shares + 12
     constituent shares = $1.4k of capital, vs $3.6k in v5. The same edge
     gets re-captured many times instead of one large bite.

  3. **Faster MM refresh.** MM_REFRESH_S 0.5 → 0.2 (5 cycles/s/instrument).
     Combined with a single venue's MM universe peak of 25 instruments × 5/s
     × 2 msg/refresh = 250 msg/s, comfortably under the 450 msg/s bucket
     (raised from 400 to leave less stranded budget).

  4. **Lower thresholds.** BASE_XV_EDGE 2 → 1, BASE_ARB_EDGE 2 → 1,
     BASE_SUB_ETF_EDGE 3 → 2. ADAPT_QUANTILE 0.20 → 0.15. We fire on
     anything that clears the MM 1¢ tick — every 1¢ edge is now in scope.

  5. **Earlier taper, earlier inventory skew.** TAPER_START_FRAC 0.3 → 0.15,
     TAPER_END_FRAC 0.7 → 0.5. INV_SKEW_THRESHOLD_FRAC 0.3 → 0.15. The
     bot starts pushing back toward zero after just 15 shares of inventory
     (vs 75 in v5), so positions decay fast even between explicit unwinds.

  6. **Bucket headroom raised.** RATE_PER_S 400 → 450. Hard server limit
     is 500; 450 gives a 10% safety margin while letting more fires through
     when the universe is busy.

Inherits everything else from v5/namikv2: MM all 25 instruments, inventory-
aware MM skew, adaptive thresholds, depth walking, microprice MM, settlement-
aware unwind, per-strategy P&L stats, Euronext casing fix.

Two further additions on top of the v5→v6 scalper retune:

  7. **RTT-weighted MM cadence and size.** Until v6, every venue got the
     same MM_REFRESH_S and MM_QTY regardless of how far it was from our
     home seat. That meant NYSE quotes from HKEX (180ms RTT) — already
     stale by the time anyone sees them — burned the same rate-limit
     budget as our local HKEX quotes (~0ms). v6 now uses the full RTT
     matrix from the participant guide to slot every venue into one of
     three tiers (local <50ms, mid 50-100ms, far >100ms), and applies a
     per-venue refresh interval and quote size:

         tier        refresh   qty
         local       0.2s      3
         mid         0.5s      2
         far         1.0s      1

     This frees ~70% of the rate-limit budget at far venues for arb
     fires that genuinely need it. Arb (XV, ETF basket, sub-ETF) keeps
     its uniform sizing — those are signal-driven and worth taking even
     at distance.

  8. **Periodic cluster re-detection.** namikv2 (and v5) ran auto_detect
     once at startup only. v6 re-probes every 10 minutes (matching the
     segment cadence), so the per-venue scaling rebalances automatically
     when the team rotates between NYSE/ZSE/HKEX. Detection runs as a
     background asyncio task; on cluster change, the per-venue tier
     table is recomputed in-place.

Validation watchlist (heartbeat):
- "Message rate limit exceeded" text frames → bump MM_REFRESH_S_LOCAL to 0.25
- Per-line peak position regularly hitting 100 → bump SOFT_POS_MAX to 150
- Cash dipping below $90k/exchange routinely → tighten inventory skew further
- Cluster redetect log lines should appear every 10 min once warmed up
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

# Latency clusters (used for tie-breaking in counterparty selection).
# Within-cluster RTT is small; across is 80-180ms, which costs us drift.
CLUSTERS = {
    "NA":   {"NYSE", "NASDAQ", "TMX"},
    "EU":   {"LSE", "Euronext"},
    "ASIA": {"JPX", "HKEX", "SSE"},
    "IN":   {"NSE"},
    "ZSE":  {"ZSE"},
}
CLUSTER_OF = {v: c for c, vs in CLUSTERS.items() for v in vs}

# Round-trip-time matrix (ms) from the participant guide §7. Indexed as
# RTT_FROM_CLUSTER[home_cluster][venue]. Each cluster's row uses its
# representative venue (NA→NYSE, EU→LSE, ASIA→HKEX, IN→NSE, ZSE→ZSE)
# since the matrix is symmetric and intra-cluster RTTs are <11ms.
RTT_FROM_CLUSTER: dict[str, dict[str, int]] = {
    # NYSE row
    "NA":   {"NYSE":   0, "NASDAQ":   1, "TMX":  11, "LSE":  80, "Euronext":  84,
             "JPX":  152, "SSE":    165, "HKEX": 180, "NSE": 174, "ZSE":      96},
    # LSE row
    "EU":   {"NYSE":  80, "NASDAQ":  80, "TMX":  82, "LSE":   0, "Euronext":   6,
             "JPX":  141, "SSE":    156, "HKEX": 135, "NSE": 134, "ZSE":      24},
    # ZSE row
    "ZSE":  {"NYSE":  96, "NASDAQ":  96, "TMX":  98, "LSE":  24, "Euronext":  22,
             "JPX":  140, "SSE":    145, "HKEX": 150, "NSE":  95, "ZSE":       0},
    # HKEX row
    "ASIA": {"NYSE": 180, "NASDAQ": 180, "TMX": 174, "LSE": 135, "Euronext": 130,
             "JPX":   37, "SSE":     19, "HKEX":   0, "NSE":  53, "ZSE":     150},
    # NSE row
    "IN":   {"NYSE": 174, "NASDAQ": 174, "TMX": 174, "LSE": 134, "Euronext": 130,
             "JPX":   53, "SSE":     54, "HKEX":  53, "NSE":   0, "ZSE":      95},
}

# RTT tier boundaries used to pick MM cadence/size per venue.
RTT_LOCAL_MS = 50      # < this is "local"
RTT_MID_MS   = 100     # < this is "mid"; ≥ is "far"

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

# v6: bucket budget raised. Server hard limit is 500/s; 450 gives 10% safety
# margin while letting more fires through during busy ticks.
RATE_PER_S         =    450

# v6: even tighter per-instrument inventory cap (was 250 in v5). At $100/share,
# peak deployment per (exchange, instrument) drops to ~$10k. The 100-share cap
# combined with the earlier taper (TAPER_START_FRAC 0.15) means new orders
# start shrinking after just 15 shares of inventory — positions barely
# accumulate before being throttled or skewed back to zero.
SOFT_POS_MAX       =    100       # was 250 in v5
SOFT_POS_MIN       =   -180       # short cap unchanged (hard cap is -200)
SOFT_CASH_FLOOR    = -4_500_000

# v6: thresholds floored at 1¢ — anything that clears the MM tick is in scope.
# Adaptive layer still raises the actual threshold during wide regimes.
BASE_XV_EDGE       =      1       # was 2 in v5
BASE_ARB_EDGE      =      1       # was 2 in v5
BASE_SUB_ETF_EDGE  =      2       # was 3 in v5

# Adaptive threshold params.
ADAPT_WINDOW_SECONDS =     5.0    # drop samples older than this
ADAPT_QUANTILE       =     0.15   # was 0.20 in v5 — fire even more often
ADAPT_MIN_SAMPLES    =    20      # need this many in-window samples before activating

# v6: taper kicks in even earlier (15% vs 30% of cap) and saturates at 50%.
# With the 100-share cap, the taper is already shrinking new orders by 25
# shares — which is well under any individual XV or arb fire size. This is
# what keeps positions hovering near zero between explicit unwinds.
TAPER_START_FRAC   =      0.15
TAPER_END_FRAC     =      0.5

# Depth walking — how many extra cents past best to consider in cross-venue.
DEPTH_WALK_CENTS   =     20

# Per-leg drift cost for out-of-cluster legs (unchanged from v2/v5).
CROSS_CLUSTER_DRIFT_CENTS = 3

# v6: tiny per-fire bites. Big edges get hit many times across consecutive
# ticks rather than once in a single large fill that would strand inventory.
ARB_MAX_K          =      2       # was 5 in v5 — ETFA fire = 2 ETF + 12 constituent shares
XV_MAX_QTY         =     10       # was 30 in v5
MM_INSIDE_TICK     =      1

# v6: per-tier MM cadence and quote size. Picked at runtime per venue from
# RTT_FROM_CLUSTER[my_cluster][venue]. Local venues get fast / large quotes;
# far venues get slow / small quotes that don't waste rate-limit budget.
MM_REFRESH_S_LOCAL =      0.2     # RTT < 50ms
MM_REFRESH_S_MID   =      0.5     # 50ms ≤ RTT < 100ms
MM_REFRESH_S_FAR   =      1.0     # RTT ≥ 100ms

MM_QTY_LOCAL       =      3
MM_QTY_MID         =      2
MM_QTY_FAR         =      1

# v6: inventory-aware MM skew kicks in earlier (15% vs 30% of cap) — at the
# 100-share cap, that's just 15 shares of inventory before the skew
# starts pulling us back toward zero.
INV_SKEW_THRESHOLD_FRAC =  0.15

# Periodic cluster re-detection. Segment length is 10 min and the team
# rotates between NYSE/ZSE/HKEX every segment, so re-probing on this
# cadence keeps the per-venue tier table aligned with our actual location.
CLUSTER_REDETECT_S =    600.0

# MM the entire universe (unchanged from v5).
MM_INSTRUMENTS     = sorted(set(LISTINGS.keys()))

DEFAULT_ROUND_MS   = 600_000
EOS_UNWIND_MS      =  60_000
EOS_FLATTEN_MS     =   8_000
INVENTORY_PERIOD_S =      2.0
HEARTBEAT_LOG_S    =      5.0
MAX_BACKOFF_S      =      4.0

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOGLEVEL, logging.INFO),
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fabijan_v6")


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

    # NEW: depth walking. Returns (cumulative_qty, weighted_avg_price) up to a price limit.
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
    """Rolling time-windowed spread tracker per (strategy, ticker).

    On every strategize tick, the strategy code records the *current* observed
    spread (including 0 / negative — those mean "no arb right now", which is
    information that should pull the threshold DOWN in tight regimes). Old
    samples (>ADAPT_WINDOW_SECONDS) are evicted lazily on access. Threshold
    is the configured quantile of the in-window samples, never below the floor.

    The time-based eviction (rather than count-based) means: if spreads
    suddenly tighten mid-segment, the old wide-regime observations age out
    in ~5 seconds and the threshold drops accordingly.
    """
    def __init__(self):
        self._windows: dict[tuple[str, str], deque[tuple[float, float]]] = {}

    def _evict_old(self, q: deque, now: float) -> None:
        cutoff = now - ADAPT_WINDOW_SECONDS
        while q and q[0][0] < cutoff:
            q.popleft()

    def record(self, strategy: str, ticker: str, spread: float) -> None:
        # Record EVERYTHING including zero/negative — needed for adapt-down.
        # We clamp to 0 so the quantile math behaves; negatives mean the book
        # is currently inverted in our favor on the OTHER direction anyway.
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
    """Realized cash deltas attributed to each strategy that fired the leg."""
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
# Token bucket (unchanged)
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
    price: int                   # IOC limit price — sent to the exchange
    order_type: str = "ioc"
    note: str = ""
    strategy: str = ""           # tag legs so fill attribution works
    # Expected average fill price, used ONLY for optimistic cash reservation.
    # Defaults to limit price (conservative). When a depth walk is performed,
    # set this to the volume-weighted avg across the levels we expect to
    # sweep — this stops the bot from over-reserving cash on multi-level
    # buys and under-crediting cash on multi-level sells, which would block
    # subsequent plans on the same venue.
    expected_avg_price: Optional[int] = None

    @property
    def cost_price(self) -> int:
        """Price used for headroom / cash accounting (not the IOC limit)."""
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
        # Per-venue activity counters — surface silent venues in the heartbeat
        self.fills_by_venue: dict[str, int] = defaultdict(int)
        self.realized_by_venue: dict[str, int] = defaultdict(int)

        # NEW: adaptive thresholds + per-strategy stats
        self.spread_tracker = SpreadTracker()
        self.stats = StatsTracker()

        # Per-venue MM cadence and quote size, derived from RTT to each
        # venue from our home cluster. Recomputed by _refresh_venue_scaling
        # whenever my_cluster changes (every 10 min via cluster_redetect_loop).
        self.mm_refresh_per_venue: dict[str, float] = {}
        self.mm_qty_per_venue: dict[str, int] = {}
        self._refresh_venue_scaling()

    def _refresh_venue_scaling(self) -> None:
        """Recompute per-venue MM tier from RTT_FROM_CLUSTER[my_cluster]."""
        rtt_row = RTT_FROM_CLUSTER.get(self.my_cluster, {})
        for v in self.active_venues:
            rtt = rtt_row.get(v, 100)
            if rtt < RTT_LOCAL_MS:
                self.mm_refresh_per_venue[v] = MM_REFRESH_S_LOCAL
                self.mm_qty_per_venue[v] = MM_QTY_LOCAL
            elif rtt < RTT_MID_MS:
                self.mm_refresh_per_venue[v] = MM_REFRESH_S_MID
                self.mm_qty_per_venue[v] = MM_QTY_MID
            else:
                self.mm_refresh_per_venue[v] = MM_REFRESH_S_FAR
                self.mm_qty_per_venue[v] = MM_QTY_FAR
        log.info("venue scaling (cluster=%s): %s",
                 self.my_cluster,
                 {v: (self.mm_refresh_per_venue[v], self.mm_qty_per_venue[v])
                  for v in self.active_venues})

    def req_id(self, tag: str) -> str:
        self._req_seq += 1
        return f"p{self._req_seq:08x}-{tag}"

    # ─── inventory taper ─────────────────────────────────────────────
    def inventory_taper(self, exchange: str, ticker: str, side: str) -> float:
        """Returns sizing multiplier in [0, 1] based on how close to caps we are.

        Below TAPER_START_FRAC: 1.0 (full size)
        Above TAPER_END_FRAC:   ~0.05 (tiny)
        Quadratic in between for smooth deceleration.
        """
        pos = self.pos[(exchange, ticker)]
        if side == "bid":
            cap = SOFT_POS_MAX
            frac_used = pos / cap if cap > 0 else 0
        else:
            cap = SOFT_POS_MIN
            frac_used = pos / cap if cap < 0 else 0  # cap negative → frac_used positive when we're short
        frac_used = max(0.0, min(1.0, frac_used))
        if frac_used <= TAPER_START_FRAC:
            return 1.0
        if frac_used >= TAPER_END_FRAC:
            return 0.05
        # Quadratic decay between start and end
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
            cost = leg.qty * leg.cost_price  # use expected avg, not the IOC limit
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

    # ─── message handlers (mostly unchanged; trade event paths preserved) ─
    def on_welcome(self, exchange: str, msg: Optional[dict] = None) -> None:
        log.info("welcome %s", exchange)
        if msg is not None:
            # Welcome carries the actual segment length and server clock — pick
            # them up so settlement_unwind doesn't run on the DEFAULT_ROUND_MS
            # placeholder. Earlier versions parsed the welcome but threw the
            # data away.
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
        for inst, depth in msg.get("orderbook_depths", {}).items():
            ex, _, tk = inst.partition("-")
            if ex != exchange:
                continue
            book = self.books.get((ex, tk))
            if book is not None:
                book.update(depth)
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
                # Attribute the fill — these are usually MM resting orders.
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
            self.fills_by_venue[leg.exchange] += 1
        if bc is not None:
            self.cash[leg.exchange] += int(bc)
            self.realized_cents += int(bc)
            self.realized_by_venue[leg.exchange] += int(bc)
            # Per-strategy attribution
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
            # Per-venue activity. A silent venue (books=0/N or fills=0 long after start)
            # is the canary for connection / casing / config bugs like the Euronext one.
            venue_lines = []
            for v in self.active_venues:
                quoted = sum(1 for tk in self.tickers_on_ex[v]
                             if self.books[(v, tk)].mid is not None)
                listed = len(self.tickers_on_ex[v])
                fills = self.fills_by_venue.get(v, 0)
                realized = self.realized_by_venue.get(v, 0)
                ready_mark = "" if self.ready.get(v) else " NOT-READY"
                venue_lines.append(
                    f"{v}: books={quoted}/{listed} fills={fills} "
                    f"realized=${realized/100:+.0f}{ready_mark}"
                )
            log.info("[hb-venue] %s", " | ".join(venue_lines))
            summary = self.stats.summary()
            if summary:
                log.info("[strat] %s", summary)

    # ─── helper: best counterparty selection with cluster preference ─
    def _best_buy_venue(self, ticker: str, my_cluster: Optional[str] = None):
        """Lowest ask across venues, breaking ties by same-cluster."""
        best = None  # (px, qty, venue)
        for v in LISTINGS[ticker]:
            if v not in self.active_venues:
                continue
            bk = self.books[(v, ticker)]
            ba = bk.best_ask
            if ba is None:
                continue
            aq = bk.best_ask_qty
            score = (ba, 0 if my_cluster and CLUSTER_OF.get(v) == my_cluster else 1)
            if best is None or score < (best[0], best[3]):
                best = (ba, aq, v, score[1])
        if best is None:
            return None
        return best[2], best[0], best[1]  # (venue, price, qty)

    def _best_sell_venue(self, ticker: str, my_cluster: Optional[str] = None):
        """Highest bid across venues, breaking ties by same-cluster."""
        best = None
        for v in LISTINGS[ticker]:
            if v not in self.active_venues:
                continue
            bk = self.books[(v, ticker)]
            bb = bk.best_bid
            if bb is None:
                continue
            bq = bk.best_bid_qty
            score = (-bb, 0 if my_cluster and CLUSTER_OF.get(v) == my_cluster else 1)
            if best is None or score < (-best[0], best[3]):
                best = (bb, bq, v, score[1])
        if best is None:
            return None
        return best[2], best[0], best[1]

    # ─── strategy 1: ETF <-> basket arb (with adaptive threshold) ────
    def etf_basket_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for etf in ETFS:
            basket = ETF_BASKETS[etf]
            n = len(basket)
            const_bid: dict[str, tuple[str, int, int]] = {}
            const_ask: dict[str, tuple[str, int, int]] = {}
            ok = True
            for tk in basket:
                bb = self._best_sell_venue(tk, self.my_cluster)
                ba = self._best_buy_venue(tk, self.my_cluster)
                if bb is None or ba is None:
                    ok = False; break
                const_bid[tk] = bb
                const_ask[tk] = ba
            if not ok:
                continue
            for etf_v in LISTINGS[etf]:
                if etf_v not in self.active_venues:
                    continue
                bk = self.books[(etf_v, etf)]
                if bk.best_bid is None or bk.best_ask is None:
                    continue

                # Direction A: ETF cheap → buy ETF, sell basket
                etf_buy_px = bk.best_ask
                etf_buy_qty = bk.best_ask_qty
                synth_sell_total = sum(const_bid[tk][1] for tk in basket)
                edge_a = synth_sell_total / n - etf_buy_px

                # Record observed edge for adaptive threshold (whether or not we act)
                if edge_a > 0:
                    self.spread_tracker.record("ETF_BUY", etf, edge_a)
                threshold = self.spread_tracker.threshold("ETF_BUY", etf, BASE_ARB_EDGE)

                if edge_a >= threshold:
                    # Inventory taper on the BUY side (we're going long the ETF)
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

    # ─── strategy 2: sub-ETF arb (with adaptive threshold) ───────────
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
                bb = self._best_sell_venue(tk, self.my_cluster)
                ba = self._best_buy_venue(tk, self.my_cluster)
                if bb is None or ba is None:
                    ok = False; break
                comp_bid[tk] = bb
                comp_ask[tk] = ba
            if not ok:
                continue

            def best_quote(tk, venues, side):
                # Tie-break on cluster preference, same as _best_*_venue.
                my_cluster = self.my_cluster
                best = None  # (v, px, qty, in_cluster_flag)
                for v in venues:
                    bk = self.books[(v, tk)]
                    px = bk.best_ask if side == "bid" else bk.best_bid
                    qty = bk.best_ask_qty if side == "bid" else bk.best_bid_qty
                    if px is None: continue
                    in_cluster = 0 if CLUSTER_OF.get(v) == my_cluster else 1
                    if best is None:
                        best = (v, px, qty, in_cluster)
                        continue
                    cmp_self = (px, in_cluster) if side == "bid" else (-px, in_cluster)
                    cmp_best = (best[1], best[3]) if side == "bid" else (-best[1], best[3])
                    if cmp_self < cmp_best:
                        best = (v, px, qty, in_cluster)
                return best[:3] if best else None

            sup_buy = best_quote(super_etf, sup_venues, "bid")
            sup_sell = best_quote(super_etf, sup_venues, "ask")
            sub_buy = best_quote(sub_etf, sub_venues, "bid")
            sub_sell = best_quote(sub_etf, sub_venues, "ask")
            if not (sup_buy and sup_sell and sub_buy and sub_sell):
                continue

            # Direction A: super cheap → long super, short sub + complement.
            # Selling sub: hit the BID, which is sub_sell[1] (best_quote(side="ask")
            # returns best_bid). Earlier code used sub_buy[1] (the ASK) and emitted
            # sell IOCs at limit=ASK that never filled — fixed.
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
            # Buying sub: pay the ASK, which is sub_buy[1]. Earlier code used
            # sub_sell[1] (the BID) and emitted buy IOCs at limit=BID that never
            # filled — fixed.
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

    # ─── strategy 3: cross-venue arb (with depth walking + adaptive) ─
    def cross_venue_arbs(self) -> list[Plan]:
        out: list[Plan] = []
        for tk in LISTINGS:
            venues = [v for v in LISTINGS[tk] if v in self.active_venues]
            if len(venues) < 2:
                continue

            # Find best bid (sell into) and best ask (buy from) across all venues
            best_b = best_a = None
            for v in venues:
                bk = self.books[(v, tk)]
                if bk.best_bid is not None and (best_b is None or bk.best_bid > best_b[1]):
                    best_b = (v, bk.best_bid, bk.best_bid_qty)
                if bk.best_ask is not None and (best_a is None or bk.best_ask < best_a[1]):
                    best_a = (v, bk.best_ask, bk.best_ask_qty)
            if not best_b or not best_a or best_b[0] == best_a[0]:
                continue

            # Use microprice for signal direction in close cases — if mid says
            # spread is 5¢ but microprice says it's flipped, skip.
            top_edge = best_b[1] - best_a[1]
            if top_edge <= 0:
                continue

            # Record observed spread for this ticker for adaptive learning
            self.spread_tracker.record("XV", tk, top_edge)
            threshold = self.spread_tracker.threshold("XV", tk, BASE_XV_EDGE)

            # Drift-aware threshold: each leg outside our cluster eats expected
            # cents of drift between signal and fill. The bot's location rotates
            # each segment, so the same XV pair has a different effective bar
            # in NA vs ASIA.
            buy_v, buy_px_top, _ = best_a
            sell_v, sell_px_top, _ = best_b
            drift = 0
            if CLUSTER_OF.get(buy_v) != self.my_cluster:
                drift += CROSS_CLUSTER_DRIFT_CENTS
            if CLUSTER_OF.get(sell_v) != self.my_cluster:
                drift += CROSS_CLUSTER_DRIFT_CENTS
            effective_threshold = threshold + drift

            if top_edge < effective_threshold:
                continue

            # Depth walk: find total qty available within DEPTH_WALK_CENTS of best
            buy_book = self.books[(buy_v, tk)]
            sell_book = self.books[(sell_v, tk)]

            buy_limit = buy_px_top + DEPTH_WALK_CENTS
            sell_limit = sell_px_top - DEPTH_WALK_CENTS
            buy_qty, buy_avg = buy_book.buy_depth_to(buy_limit)
            sell_qty, sell_avg = sell_book.sell_depth_to(sell_limit)
            walked = False
            if buy_avg is not None and sell_avg is not None and (sell_avg - buy_avg) >= effective_threshold:
                buy_px = buy_limit
                sell_px = sell_limit
                walked = True
                exp_buy = int(buy_avg)    # for cash reservation
                exp_sell = int(sell_avg)
                walked_edge = sell_avg - buy_avg
            else:
                # Fall back to L1 only — single price level, no walk needed
                buy_qty = best_a[2]
                sell_qty = best_b[2]
                buy_px = buy_px_top
                sell_px = sell_px_top
                exp_buy = exp_sell = None  # let cost_price default to limit
                walked_edge = top_edge

            qty = min(buy_qty, sell_qty, XV_MAX_QTY)

            # Inventory taper on both sides
            taper_buy = self.inventory_taper(buy_v, tk, "bid")
            taper_sell = self.inventory_taper(sell_v, tk, "ask")
            taper = min(taper_buy, taper_sell)
            qty = max(0, int(qty * taper))

            qty = min(qty, SOFT_POS_MAX - self.pos[(buy_v, tk)])
            qty = min(qty, self.pos[(sell_v, tk)] - SOFT_POS_MIN)
            # Cash room uses EXPECTED avg fill price (not IOC limit) — same
            # accounting as fits() will use, so the validation is consistent.
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

    # ─── strategy 5: settlement-aware unwind (unchanged) ─────────────
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

    # ─── strategy 4: passive MM (microprice for skew) ────────────────
    def passive_mm_refresh(self) -> None:
        now = time.monotonic()
        for tk in MM_INSTRUMENTS:
            for v in LISTINGS.get(tk, set()):
                if v not in self.active_venues or not self.ready.get(v, False):
                    continue
                key = (v, tk)
                # Per-venue refresh interval — local venues re-quote 5×/s,
                # far venues 1×/s. Set by _refresh_venue_scaling().
                refresh_s = self.mm_refresh_per_venue.get(v, MM_REFRESH_S_LOCAL)
                if now - self.last_mm_refresh[key] < refresh_s:
                    continue
                self.last_mm_refresh[key] = now
                bk = self.books[key]
                bb, ba = bk.best_bid, bk.best_ask
                if bb is None or ba is None or ba - bb < 2 * MM_INSIDE_TICK + 1:
                    continue
                pos = self.pos[key]

                # Microprice-aware skew. If microprice > mid → buying pressure,
                # the next print likely lifts; shift BOTH our quotes upward so
                # we sell into the lift at a better price and reload our bid
                # closer to where the market is heading.
                mp = bk.microprice
                m = bk.mid
                skew = 0
                if mp is not None and m is not None:
                    if mp > m + 1:    # buying pressure
                        skew = 1
                    elif mp < m - 1:  # selling pressure
                        skew = -1

                # v3: inventory-aware skew. namikv2 quoted symmetrically around
                # microprice regardless of how long/short we already were on
                # this line. Top competitors stay near zero — the active force
                # behind that is biasing your own quotes toward unloading
                # whichever side you're currently heavy on. When long, we want
                # the ask to fill faster and the bid to fill slower, so shift
                # both quotes DOWN by 1¢ — the ask becomes more attractive
                # vs the book, the bid less attractive. Mirror when short.
                inv_skew = 0
                inv_threshold = max(1, int(SOFT_POS_MAX * INV_SKEW_THRESHOLD_FRAC))
                if pos > inv_threshold:
                    inv_skew = -1     # long → bleed long via cheaper ask
                elif pos < -inv_threshold:
                    inv_skew = 1      # short → bleed short via richer bid

                bid_px = bb + MM_INSIDE_TICK + skew + inv_skew
                ask_px = ba - MM_INSIDE_TICK + skew + inv_skew
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

                # Inventory-tapered MM size, scaled by per-venue tier.
                # Local venues quote 3 sh, mid 2 sh, far 1 sh — set by
                # _refresh_venue_scaling() based on RTT to this venue.
                base_qty = self.mm_qty_per_venue.get(v, MM_QTY_LOCAL)
                buy_taper = self.inventory_taper(v, tk, "bid")
                sell_taper = self.inventory_taper(v, tk, "ask")
                buy_qty = max(1, int(base_qty * buy_taper))
                sell_qty = max(1, int(base_qty * sell_taper))

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
    """Probe each venue's :9001 in parallel and return the cluster of the
    fastest responder. Team VM rotates location each segment, so this needs
    to run at startup of every segment, not once globally.

    Falls back to NA if every venue is unreachable (likely between rounds —
    docs say WS ports are filtered when no round is live).
    """
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

    async def cluster_redetect_loop():
        """Re-probe venue latencies every CLUSTER_REDETECT_S (default 600s,
        matching the 10-min segment cadence). On cluster change, refresh
        the per-venue MM scaling table in place — connections keep running."""
        while not hub.stop:
            try:
                await asyncio.sleep(CLUSTER_REDETECT_S)
            except asyncio.CancelledError:
                return
            if hub.stop:
                return
            try:
                new_cluster = await auto_detect_cluster()
            except Exception as e:
                log.warning("cluster redetect failed: %s", e)
                continue
            old = hub.my_cluster
            if new_cluster != old:
                log.info("cluster rotated: %s → %s", old, new_cluster)
                hub.my_cluster = new_cluster
            else:
                log.info("cluster re-detected, unchanged: %s", new_cluster)
            hub._refresh_venue_scaling()
    tasks.append(asyncio.create_task(cluster_redetect_loop(), name="redetect"))

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
    # Case-insensitive matching but canonicalize to the casing used by the server
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
    log.info("starting fabijan_v6 on venues: %s", active)

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