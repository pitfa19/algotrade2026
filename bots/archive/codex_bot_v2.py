#!/usr/bin/env python3
"""
AlgoTrade 2026 — improved bot, v2.

Same async/websocket infrastructure and strategy framework as v1
(``codex_bot.py``), with four targeted fixes:

  1. ``latency_cost`` in opportunity scoring now uses RTT from
     ``config.home_location`` to the venue, instead of the diagonal RTT
     (always 0). The score now actually penalizes far-away venues.
  2. The aggressor-flow window is driven by config
     (``aggressor_flow_max_trades``), not a hardcoded 20.
  3. Same-tick opportunities targeting the same ``(instrument, side)`` are
     deduped — only the highest-scored one is kept. Prevents two strategies
     from stacking the same trade.
  4. Resting limit orders are tracked by ``order_id`` with placement time;
     stale ones are swept via ``cancel_order`` (used when passive MM is on).

Default mode is dry-run. Set LIVE_TRADING=1 on the venue VM to send orders.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import statistics
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional


# ============================================================
# CONSTANTS — venue, instruments, latency matrix
# ============================================================

EXCHANGE_PORT = 9001
EXCHANGE_HOSTS: dict[str, str] = {
    "NYSE": "10.0.201.2",
    "NASDAQ": "10.0.202.2",
    "SSE": "10.0.203.2",
    "JPX": "10.0.204.2",
    "Euronext": "10.0.205.2",
    "LSE": "10.0.206.2",
    "HKEX": "10.0.207.2",
    "NSE": "10.0.208.2",
    "TMX": "10.0.209.2",
    "ZSE": "10.0.210.2",
}
EXCHANGE_ALIASES = {name.upper(): name for name in EXCHANGE_HOSTS}
ALL_EXCHANGES = list(EXCHANGE_HOSTS)

STARTING_CASH_CENTS = 10_000_000
OFFICIAL_MAX_LONG = 2_000
OFFICIAL_MAX_SHORT = -200
OFFICIAL_MIN_CASH = -5_000_000

SECTOR_A = ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"]
SECTOR_B = ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"]
INDEPENDENT = ["MDKA", "KRAS", "ZITO", "ZABA", "SIMP", "CARD"]
SAFE_HAVEN = ["GOLD", "XAG"]
SECTOR_OF: dict[str, str] = {
    **{t: "A" for t in SECTOR_A},
    **{t: "B" for t in SECTOR_B},
    **{t: "SH" for t in SAFE_HAVEN},
}
SECTOR_MEMBERS: dict[str, list[str]] = {
    "A": SECTOR_A, "B": SECTOR_B, "SH": SAFE_HAVEN,
}

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA": ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB": ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}

STOCK_LISTINGS: dict[str, list[str]] = {
    "CARD": ALL_EXCHANGES,
    "SIMP": ALL_EXCHANGES,
    "NGUP": ["NYSE", "NASDAQ", "Euronext", "TMX", "ZSE"],
    "OIT": ["LSE", "Euronext", "HKEX", "NSE", "ZSE"],
    "KTST": ["NYSE", "JPX", "TMX", "ZSE"],
    "FSR": ["NASDAQ", "LSE", "SSE", "HKEX", "ZSE"],
    "JZRO": ["NYSE", "LSE", "Euronext", "TMX", "ZSE"],
    "XFR": ["NYSE", "HKEX", "TMX", "ZSE"],
    "KOTD": ["NASDAQ", "LSE", "Euronext", "HKEX", "ZSE"],
    "INA": ["NYSE", "NASDAQ", "Euronext", "HKEX", "ZSE"],
    "HT": ["NASDAQ", "LSE", "JPX", "SSE", "TMX", "ZSE"],
    "JNAF": ["NYSE", "Euronext", "JPX", "HKEX", "ZSE"],
    "DLKV": ["NASDAQ", "LSE", "HKEX", "NSE", "ZSE"],
    "DDJH": ["NYSE", "LSE", "Euronext", "TMX", "ZSE"],
    "MDKA": ["NYSE", "LSE", "HKEX", "TMX", "ZSE"],
    "KRAS": ["NYSE", "Euronext", "SSE", "TMX", "ZSE"],
    "ZITO": ["NASDAQ", "LSE", "Euronext", "NSE", "ZSE"],
    "ZABA": ["NYSE", "LSE", "SSE", "NSE", "TMX", "ZSE"],
    "GOLD": ["NASDAQ", "Euronext", "JPX", "TMX", "ZSE"],
    "XAG": ["LSE", "Euronext", "JPX", "ZSE"],
}
ETF_LISTINGS: dict[str, list[str]] = {
    "ETFA": ["NYSE", "Euronext", "HKEX", "ZSE"],
    "ETFB": ["NASDAQ", "LSE", "HKEX", "ZSE"],
    "ETFA3": ["NYSE", "TMX", "ZSE"],
    "ETFB3": ["NASDAQ", "HKEX", "ZSE"],
    "ETFSH": ["Euronext", "JPX", "ZSE"],
}
LISTINGS: dict[str, list[str]] = {**STOCK_LISTINGS, **ETF_LISTINGS}

# Primary listing venues — these tend to lead price discovery for their region.
# Used to weight fair value and to identify "leader" exchanges in latency arb.
LEADER_EXCHANGES = {"NYSE", "NASDAQ", "SSE", "JPX", "LSE", "Euronext"}

# Per-instrument edge thresholds (cents). Calibrated roughly from observed spreads.
# Tighter-spread instruments get tighter thresholds; the bot will still hit them.
INSTRUMENT_EDGE_CENTS: dict[str, int] = {
    # tight-spread group (~2¢ typical spread)
    "NGUP": 8, "SIMP": 8, "CARD": 8, "GOLD": 8, "DDJH": 8,
    # medium-spread group (~4¢)
    "JZRO": 12, "HT": 12, "KRAS": 12, "KTST": 12, "XFR": 12,
    "MDKA": 12, "ETFA3": 12, "ETFB3": 12,
    # wider-spread group (~5-6¢)
    "ZABA": 16, "OIT": 14, "FSR": 14, "INA": 14, "JNAF": 14,
    "KOTD": 14, "DLKV": 14, "ZITO": 14, "XAG": 14,
    # full ETFs
    "ETFA": 15, "ETFB": 15, "ETFSH": 15,
}
DEFAULT_EDGE_CENTS = 15

LATENCY_RTT_MS: dict[str, dict[str, int]] = {
    "NYSE":     {"NYSE":0,"NASDAQ":1,"SSE":165,"JPX":152,"Euronext":84,"LSE":80,"HKEX":180,"NSE":174,"TMX":11,"ZSE":96},
    "NASDAQ":   {"NYSE":1,"NASDAQ":0,"SSE":165,"JPX":152,"Euronext":84,"LSE":80,"HKEX":180,"NSE":174,"TMX":11,"ZSE":96},
    "SSE":      {"NYSE":165,"NASDAQ":165,"SSE":0,"JPX":18,"Euronext":160,"LSE":156,"HKEX":19,"NSE":54,"TMX":159,"ZSE":145},
    "JPX":      {"NYSE":152,"NASDAQ":152,"SSE":18,"JPX":0,"Euronext":145,"LSE":141,"HKEX":37,"NSE":53,"TMX":145,"ZSE":140},
    "Euronext": {"NYSE":84,"NASDAQ":84,"SSE":160,"JPX":145,"Euronext":0,"LSE":6,"HKEX":130,"NSE":130,"TMX":86,"ZSE":22},
    "LSE":      {"NYSE":80,"NASDAQ":80,"SSE":156,"JPX":141,"Euronext":6,"LSE":0,"HKEX":135,"NSE":134,"TMX":82,"ZSE":24},
    "HKEX":     {"NYSE":180,"NASDAQ":180,"SSE":19,"JPX":37,"Euronext":130,"LSE":135,"HKEX":0,"NSE":53,"TMX":174,"ZSE":150},
    "NSE":      {"NYSE":174,"NASDAQ":174,"SSE":54,"JPX":53,"Euronext":130,"LSE":134,"HKEX":53,"NSE":0,"TMX":174,"ZSE":95},
    "TMX":      {"NYSE":11,"NASDAQ":11,"SSE":159,"JPX":145,"Euronext":86,"LSE":82,"HKEX":174,"NSE":174,"TMX":0,"ZSE":98},
    "ZSE":      {"NYSE":96,"NASDAQ":96,"SSE":145,"JPX":140,"Euronext":22,"LSE":24,"HKEX":150,"NSE":95,"TMX":98,"ZSE":0},
}


# ============================================================
# CORE TYPES
# ============================================================

class Side(Enum):
    BID = "bid"
    ASK = "ask"


@dataclass(frozen=True)
class BookLevel:
    price: int
    quantity: int


@dataclass(frozen=True)
class OrderBook:
    bids: tuple[BookLevel, ...] = ()
    asks: tuple[BookLevel, ...] = ()

    @property
    def best_bid(self) -> Optional[int]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return self.asks[0].price if self.asks else None

    @property
    def best_bid_qty(self) -> int:
        return self.bids[0].quantity if self.bids else 0

    @property
    def best_ask_qty(self) -> int:
        return self.asks[0].quantity if self.asks else 0

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def microprice(self) -> Optional[float]:
        """Depth-weighted fair value: leans toward the side with more size."""
        if self.best_bid is None or self.best_ask is None:
            return None
        bid_qty, ask_qty = self.best_bid_qty, self.best_ask_qty
        total = bid_qty + ask_qty
        if total <= 0:
            return self.mid
        # Weighted by *opposite* side qty (more bids → price likely up → microprice > mid)
        return (self.best_bid * ask_qty + self.best_ask * bid_qty) / total

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    def market_buy_avg_price(self, qty: int) -> Optional[float]:
        """Average price to buy `qty` walking up to 3 ask levels."""
        remaining, cost = qty, 0
        for level in self.asks:
            if remaining <= 0:
                break
            take = min(remaining, level.quantity)
            cost += take * level.price
            remaining -= take
        return cost / qty if remaining <= 0 else None


@dataclass(frozen=True)
class BookSnapshot:
    exchange: str
    instrument_id: str
    book: OrderBook
    exchange_time_ms: int
    received_monotonic: float

    @property
    def ticker(self) -> str:
        return ticker_from_instrument(self.instrument_id)

    def age_seconds(self, now_monotonic: float) -> float:
        return max(0.0, now_monotonic - self.received_monotonic)


@dataclass(frozen=True)
class Opportunity:
    exchange: str
    instrument_id: str
    side: Side
    price: int
    quantity: int
    order_type: str
    reason: str
    edge_cents: float
    score: float
    source: str  # "active_arb" | "latency_arb" | "passive" | ...


# ============================================================
# CONFIG
# ============================================================

@dataclass
class BotConfig:
    exchanges: list[str] = field(default_factory=lambda: ALL_EXCHANGES.copy())
    live_trading: bool = False
    # Geographic location of the bot. Drives latency-cost penalty in scoring.
    # Should be set per segment (NYSE | ZSE | HKEX) — the team rotates each segment.
    home_location: str = "ZSE"

    # Fair value
    use_microprice: bool = True
    leader_weight: float = 2.0  # leader exchanges get this much weight in fair value
    fv_max_age_seconds: float = 0.8

    # Active arb (cross-exchange mispricing capture)
    base_min_edge_cents: int = 12  # fallback for unknown instruments
    spread_threshold_multiplier: float = 1.2  # edge threshold = max(base, mult * own_spread)

    # Latency arb (lead-lag)
    latency_arb_enabled: bool = True
    lead_lag_lookback: int = 2          # how many snapshots back to measure leader move
    lead_lag_threshold_bps: float = 6.0  # leader move size required to fire signal
    lead_lag_min_latency_ms: int = 50    # only trade pairs with latency gap >= this
    lead_lag_target_bps: float = 4.0     # expected catch-up size, used for edge scoring

    # Sector residual arb (cross-ticker correlation)
    sector_arb_enabled: bool = True
    sector_lookback: int = 3            # snapshots over which to measure sector move
    sector_threshold_bps: float = 5.0   # sector-average move required to fire
    sector_min_members: int = 3         # at least this many siblings must report

    # ETF basket lead-lag
    etf_basket_arb_enabled: bool = True
    etf_basket_lookback: int = 2
    etf_basket_threshold_bps: float = 5.0

    # End-of-segment unwind
    unwind_enabled: bool = True
    unwind_start_ms: int = 580_000      # start aggressive close-out at this exchange time
    unwind_max_qty_per_tick: int = 50   # cap per-tick unwind size to avoid market impact

    # Order sizing
    base_order_quantity: int = 10
    max_order_quantity: int = 50
    edge_size_multiplier: float = 0.4   # qty grows by this much per unit (edge / threshold)

    # Order pacing
    max_orders_per_tick: int = 6
    max_rate_per_second: int = 350
    ioc_expiry_ms: int = 2_000

    # Passive market making (off by default)
    passive_enabled: bool = False
    passive_min_spread_cents: int = 10
    passive_edge_cents: int = 3
    passive_stop_after_ms: int = 560_000
    # Cancel resting limit orders older than this (used when passive_enabled).
    passive_max_order_age_seconds: float = 3.0

    # Session timing
    no_new_risk_after_ms: int = 590_000
    stale_after_seconds: float = 0.8

    # Risk
    max_long: int = 500
    max_short: int = -80
    min_cash_cents: int = -1_000_000
    max_open_orders_per_exchange: int = 500

    # Aggressor flow
    aggressor_flow_max_trades: int = 20
    aggressor_flow_bias_cents: int = 4

    # Costs
    latency_penalty_per_ms: float = 0.02

    # IO
    output_dir: Optional[str] = None
    reconnect_delay_seconds: float = 2.0

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            exchanges=parse_exchanges(os.environ.get("EXCHANGES", ",".join(ALL_EXCHANGES))),
            live_trading=parse_bool(os.environ.get("LIVE_TRADING"), default=False),
            home_location=parse_exchange(os.environ.get("HOME_LOCATION", "ZSE")) or "ZSE",
            base_min_edge_cents=parse_int(os.environ.get("MIN_EDGE_CENTS"), 12),
            base_order_quantity=parse_int(os.environ.get("ORDER_QTY"), 10),
            max_orders_per_tick=parse_int(os.environ.get("MAX_ORDERS_PER_TICK"), 6),
            max_rate_per_second=parse_int(os.environ.get("MAX_MSGS_PER_SEC"), 350),
            latency_arb_enabled=parse_bool(os.environ.get("LATENCY_ARB"), default=True),
            passive_enabled=parse_bool(os.environ.get("PASSIVE_ENABLED"), default=False),
            output_dir=os.environ.get("BOT_OUTPUT_DIR"),
        )


# ============================================================
# HELPERS
# ============================================================

def parse_bool(raw: Optional[str], default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_int(raw: Optional[str], default: int) -> int:
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_exchange(raw: str) -> Optional[str]:
    return EXCHANGE_ALIASES.get(raw.strip().upper())


def parse_exchanges(s: str) -> list[str]:
    out: list[str] = []
    for part in s.split(","):
        ex = parse_exchange(part)
        if ex and ex not in out:
            out.append(ex)
    return out


def ticker_from_instrument(instrument_id: str) -> str:
    if "-" not in instrument_id:
        return instrument_id
    return instrument_id.split("-", 1)[1]


def instrument_id(exchange: str, ticker: str) -> str:
    return f"{exchange}-{ticker}"


def parse_orderbook_depth(raw: dict[str, Any]) -> OrderBook:
    bids = tuple(sorted(
        (BookLevel(int(p), int(q)) for p, q in raw.get("bids", {}).items()),
        key=lambda lv: lv.price, reverse=True,
    ))
    asks = tuple(sorted(
        (BookLevel(int(p), int(q)) for p, q in raw.get("asks", {}).items()),
        key=lambda lv: lv.price,
    ))
    return OrderBook(bids=bids, asks=asks)


# ============================================================
# MARKET STATE — books, mid history, aggressor flow
# ============================================================

class MarketState:
    def __init__(self, history_len: int = 8, flow_window: int = 20) -> None:
        self._books: dict[tuple[str, str], BookSnapshot] = {}
        # Rolling history of microprices per (exchange, ticker) for lead-lag
        self._mid_history: dict[tuple[str, str], deque[float]] = {}
        # Aggressor flow per ticker: +qty per buy aggressor, -qty per sell aggressor
        self._flow: dict[str, deque[int]] = {}
        self.exchange_times: dict[str, int] = {}
        self.history_len = history_len
        self.flow_window = flow_window

    def apply_market_data(
        self,
        exchange: str,
        message: dict[str, Any],
        received_monotonic: Optional[float] = None,
    ) -> None:
        now = time.monotonic() if received_monotonic is None else received_monotonic
        ex_time_ms = int(message.get("time", self.exchange_times.get(exchange, 0)) or 0)
        self.exchange_times[exchange] = ex_time_ms

        for inst_id, raw in message.get("orderbook_depths", {}).items():
            book = parse_orderbook_depth(raw)
            self._books[(exchange, inst_id)] = BookSnapshot(
                exchange=exchange, instrument_id=inst_id, book=book,
                exchange_time_ms=ex_time_ms, received_monotonic=now,
            )
            # Track microprice history for lead-lag detection
            mp = book.microprice
            if mp is not None:
                key = (exchange, ticker_from_instrument(inst_id))
                hist = self._mid_history.setdefault(key, deque(maxlen=self.history_len))
                hist.append(mp)

        # Track aggressor flow from trade events
        for event in message.get("events", []):
            if event.get("event_type") != "trade":
                continue
            data = event.get("data", {})
            inst_id = data.get("instrumentID")
            if not inst_id:
                continue
            ticker = ticker_from_instrument(inst_id)
            # Convention: positive aggressor side qty = buy aggression
            aggressor = data.get("aggressor_side") or data.get("side")
            qty = int(data.get("quantity", 0) or 0)
            if not qty:
                continue
            sign = +1 if aggressor in ("bid", "buy", "BID", "BUY") else -1
            flow_q = self._flow.setdefault(ticker, deque(maxlen=self.flow_window))
            flow_q.append(sign * qty)

    def reset_exchange(self, exchange: str) -> None:
        for k in list(self._books):
            if k[0] == exchange:
                del self._books[k]
        for k in list(self._mid_history):
            if k[0] == exchange:
                self._mid_history[k].clear()
        self.exchange_times.pop(exchange, None)

    # --- queries ---

    def book(self, exchange: str, inst_id: str) -> Optional[OrderBook]:
        snap = self._books.get((exchange, inst_id))
        return snap.book if snap else None

    def snapshot(self, exchange: str, inst_id: str) -> Optional[BookSnapshot]:
        return self._books.get((exchange, inst_id))

    def books_on_exchange(self, exchange: str) -> list[BookSnapshot]:
        return [s for (ex, _), s in self._books.items() if ex == exchange]

    def microprice_history(self, exchange: str, ticker: str) -> Optional[deque[float]]:
        return self._mid_history.get((exchange, ticker))

    def leader_microprice_change(
        self, ticker: str, lookback: int, exclude_exchange: str
    ) -> dict[str, float]:
        """Return {leader_exchange: bps_change_over_lookback} for each leader with enough history."""
        result: dict[str, float] = {}
        for leader in LEADER_EXCHANGES:
            if leader == exclude_exchange:
                continue
            if leader not in LISTINGS.get(ticker, []):
                continue
            hist = self._mid_history.get((leader, ticker))
            if hist is None or len(hist) <= lookback:
                continue
            old, new = hist[-1 - lookback], hist[-1]
            if old <= 0:
                continue
            result[leader] = (new - old) / old * 10000.0
        return result

    def ticker_microprices(
        self,
        ticker: str,
        max_age_seconds: Optional[float] = None,
        now_monotonic: Optional[float] = None,
        exclude_exchange: Optional[str] = None,
    ) -> dict[str, float]:
        out: dict[str, float] = {}
        now = time.monotonic() if now_monotonic is None else now_monotonic
        for (ex, inst_id), snap in self._books.items():
            if ex == exclude_exchange:
                continue
            if ticker_from_instrument(inst_id) != ticker:
                continue
            if max_age_seconds is not None and snap.age_seconds(now) > max_age_seconds:
                continue
            mp = snap.book.microprice
            if mp is not None:
                out[ex] = mp
        return out

    def aggressor_pressure(self, ticker: str) -> int:
        """Net aggressor signed-volume in recent window. >0 = buying pressure."""
        q = self._flow.get(ticker)
        return sum(q) if q else 0

    def exchange_time_ms(self, exchange: str) -> int:
        return self.exchange_times.get(exchange, 0)


# ============================================================
# FAIR VALUE — leader-weighted, microprice-based
# ============================================================

class FairValueEngine:
    def __init__(self, state: MarketState, config: BotConfig) -> None:
        self.state = state
        self.config = config

    def fair_value(
        self,
        ticker: str,
        target_exchange: str,
        now_monotonic: Optional[float] = None,
    ) -> Optional[float]:
        if ticker in ETF_BASKETS:
            return self._etf_fair_value(ticker, target_exchange, now_monotonic)
        return self._stock_fair_value(ticker, target_exchange, now_monotonic)

    def _stock_fair_value(
        self, ticker: str, target: str, now_monotonic: Optional[float]
    ) -> Optional[float]:
        prices = self.state.ticker_microprices(
            ticker,
            max_age_seconds=self.config.fv_max_age_seconds,
            now_monotonic=now_monotonic,
            exclude_exchange=target,
        )
        if not prices:
            # Fall back without staleness filter
            prices = self.state.ticker_microprices(
                ticker, exclude_exchange=target,
            )
        if not prices:
            return None

        # Leader-weighted average: leader exchanges count config.leader_weight times
        total_weight = 0.0
        weighted_sum = 0.0
        for ex, price in prices.items():
            w = self.config.leader_weight if ex in LEADER_EXCHANGES else 1.0
            weighted_sum += price * w
            total_weight += w
        return weighted_sum / total_weight if total_weight > 0 else None

    def _etf_fair_value(
        self, etf_ticker: str, target: str, now_monotonic: Optional[float]
    ) -> Optional[float]:
        components: list[float] = []
        for component in ETF_BASKETS[etf_ticker]:
            value = self._stock_fair_value(component, target, now_monotonic)
            if value is None:
                return None
            components.append(value)
        return sum(components) / len(components)


# ============================================================
# STRATEGY — active arb + latency arb
# ============================================================

class StrategyEngine:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    # ---- public entry point ----

    def find_opportunities(
        self,
        state: MarketState,
        exchange: str,
        now_monotonic: Optional[float] = None,
        risk: Optional["RiskManager"] = None,
    ) -> list[Opportunity]:
        now = time.monotonic() if now_monotonic is None else now_monotonic
        fv_engine = FairValueEngine(state, self.config)
        opps: list[Opportunity] = []
        ex_time = state.exchange_time_ms(exchange)

        # Strategy 0: end-of-segment unwind. When deep into the segment,
        # close existing positions ahead of new alpha-seeking trades.
        if self.config.unwind_enabled and risk is not None and ex_time >= self.config.unwind_start_ms:
            opps.extend(self._unwind_opportunities(state, exchange, risk, now))

        for snapshot in state.books_on_exchange(exchange):
            if snapshot.age_seconds(now) > self.config.stale_after_seconds:
                continue

            ticker = snapshot.ticker
            fv = fv_engine.fair_value(ticker, exchange, now)

            # Strategy 1: active arb against fair value
            if fv is not None:
                opps.extend(self._active_arb(snapshot, ticker, fv, state))

            # Strategy 2: latency arb on same-ticker lead-lag
            if self.config.latency_arb_enabled:
                opps.extend(self._latency_arb(snapshot, ticker, state))

            # Strategy 3: sector residual — sister stocks lead, this one hasn't followed
            if self.config.sector_arb_enabled and ticker in SECTOR_OF:
                opps.extend(self._sector_residual_arb(snapshot, ticker, state))

            # Strategy 4: ETF basket lead-lag — constituents lead, ETF on slow exchange lags
            if self.config.etf_basket_arb_enabled and ticker in ETF_BASKETS:
                opps.extend(self._etf_basket_arb(snapshot, ticker, state))

            # Strategy 5: passive market making (off by default)
            if self.config.passive_enabled and fv is not None:
                opps.extend(self._passive_quotes(snapshot, ticker, fv))

        opps = [o for o in opps if o.score > 0]
        opps.sort(key=lambda o: o.score, reverse=True)

        # Dedupe: at most one opportunity per (instrument, side) per tick.
        # Higher-scored opps win; unwind opps already dominate via score=1000.
        seen: set[tuple[str, str]] = set()
        deduped: list[Opportunity] = []
        for o in opps:
            key = (o.instrument_id, o.side.value)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(o)
        return deduped

    # ---- active arb: stale book vs fair value ----

    def _active_arb(
        self, snapshot: BookSnapshot, ticker: str, fv: float, state: MarketState
    ) -> list[Opportunity]:
        book = snapshot.book
        threshold = self._edge_threshold(ticker, book)
        flow_bias = self._flow_bias(ticker, state)
        out: list[Opportunity] = []

        # Strong buy pressure → relax buy threshold, tighten sell threshold (and vice versa)
        buy_threshold = max(2, threshold - flow_bias)
        sell_threshold = max(2, threshold + flow_bias)

        if book.best_ask is not None:
            edge = fv - book.best_ask
            if edge >= buy_threshold:
                out.append(self._build_opp(
                    snapshot, Side.BID, book.best_ask, book.best_ask_qty,
                    "ioc", f"ARB buy {ticker}: ask {book.best_ask} < FV {fv:.1f}",
                    edge, threshold, source="active_arb",
                ))
        if book.best_bid is not None:
            edge = book.best_bid - fv
            if edge >= sell_threshold:
                out.append(self._build_opp(
                    snapshot, Side.ASK, book.best_bid, book.best_bid_qty,
                    "ioc", f"ARB sell {ticker}: bid {book.best_bid} > FV {fv:.1f}",
                    edge, threshold, source="active_arb",
                ))
        return out

    # ---- latency arb: leader moved, target hasn't ----

    def _latency_arb(
        self, snapshot: BookSnapshot, ticker: str, state: MarketState
    ) -> list[Opportunity]:
        book = snapshot.book
        if book.best_bid is None or book.best_ask is None:
            return []

        # Only trade pairs where there's a meaningful latency gap
        target = snapshot.exchange
        leader_moves = state.leader_microprice_change(
            ticker, self.config.lead_lag_lookback, exclude_exchange=target,
        )
        if not leader_moves:
            return []

        # Filter to leaders with significant latency from target
        rtt = LATENCY_RTT_MS.get(target, {})
        usable = {
            ex: bps for ex, bps in leader_moves.items()
            if rtt.get(ex, 0) >= self.config.lead_lag_min_latency_ms
        }
        if not usable:
            return []

        # Aggregate: average leader signal, weighted by latency gap (further leaders carry more lag-info)
        total_w, weighted_bps = 0.0, 0.0
        for ex, bps in usable.items():
            w = rtt.get(ex, 0) / 100.0  # heuristic: more lag = more stale-info edge
            weighted_bps += bps * w
            total_w += w
        leader_signal = weighted_bps / total_w if total_w > 0 else 0.0

        if abs(leader_signal) < self.config.lead_lag_threshold_bps:
            return []

        # Compute target's own move over the same window — only fire if it hasn't followed
        target_hist = state.microprice_history(target, ticker)
        if target_hist is None or len(target_hist) <= self.config.lead_lag_lookback:
            return []
        old, new = target_hist[-1 - self.config.lead_lag_lookback], target_hist[-1]
        if old <= 0:
            return []
        target_bps = (new - old) / old * 10000.0
        divergence = leader_signal - target_bps
        if abs(divergence) < self.config.lead_lag_threshold_bps:
            return []

        # Edge in cents, projected on target's current price scale
        ref_price = book.mid or new
        expected_move_cents = abs(divergence) / 10000.0 * ref_price
        # Only fire if expected move exceeds spread cost
        spread = book.spread or 0
        if expected_move_cents < spread + 1:
            return []

        threshold = self._edge_threshold(ticker, book)
        out: list[Opportunity] = []
        if divergence > 0:  # leaders up, target hasn't caught up → buy target
            out.append(self._build_opp(
                snapshot, Side.BID, book.best_ask, book.best_ask_qty, "ioc",
                f"LATARB buy {ticker}: leaders +{leader_signal:.1f}bps, target +{target_bps:.1f}bps",
                expected_move_cents, threshold, source="latency_arb",
            ))
        else:  # leaders down → sell target
            out.append(self._build_opp(
                snapshot, Side.ASK, book.best_bid, book.best_bid_qty, "ioc",
                f"LATARB sell {ticker}: leaders {leader_signal:.1f}bps, target {target_bps:.1f}bps",
                expected_move_cents, threshold, source="latency_arb",
            ))
        return out

    # ---- sector residual: sister stocks led, this one hasn't followed ----

    def _sector_residual_arb(
        self, snapshot: BookSnapshot, ticker: str, state: MarketState
    ) -> list[Opportunity]:
        book = snapshot.book
        if book.best_bid is None or book.best_ask is None:
            return []
        target = snapshot.exchange
        sector = SECTOR_OF.get(ticker)
        if sector is None:
            return []

        # Collect leader-exchange microprice changes for each sector sibling
        sibling_returns_bps: list[float] = []
        for sibling in SECTOR_MEMBERS[sector]:
            if sibling == ticker:
                continue
            leader_changes = state.leader_microprice_change(
                sibling, self.config.sector_lookback, exclude_exchange=target,
            )
            if leader_changes:
                # Average of all available leaders for this sibling
                sibling_returns_bps.append(
                    sum(leader_changes.values()) / len(leader_changes)
                )

        if len(sibling_returns_bps) < self.config.sector_min_members:
            return []

        sector_signal_bps = sum(sibling_returns_bps) / len(sibling_returns_bps)

        # Safe-haven inverts: if sector A or B moved up, expect XAU/XAG to move DOWN
        # (We don't apply this to GOLD/XAG themselves — those follow risk-off, not sector A/B)
        if abs(sector_signal_bps) < self.config.sector_threshold_bps:
            return []

        # Compare to target's own move on this exchange over same window
        target_hist = state.microprice_history(target, ticker)
        if target_hist is None or len(target_hist) <= self.config.sector_lookback:
            return []
        old, new = target_hist[-1 - self.config.sector_lookback], target_hist[-1]
        if old <= 0:
            return []
        target_bps = (new - old) / old * 10000.0
        divergence = sector_signal_bps - target_bps
        if abs(divergence) < self.config.sector_threshold_bps:
            return []

        ref_price = book.mid or new
        expected_move_cents = abs(divergence) / 10000.0 * ref_price
        spread = book.spread or 0
        if expected_move_cents < spread + 1:
            return []

        threshold = self._edge_threshold(ticker, book)
        members_str = f"{len(sibling_returns_bps)}/{len(SECTOR_MEMBERS[sector])-1}"
        if divergence > 0:
            return [self._build_opp(
                snapshot, Side.BID, book.best_ask, book.best_ask_qty, "ioc",
                f"SECTOR buy {ticker}: {members_str} sector-{sector} siblings "
                f"+{sector_signal_bps:.1f}bps, target {target_bps:+.1f}bps",
                expected_move_cents, threshold, source="sector_arb",
            )]
        return [self._build_opp(
            snapshot, Side.ASK, book.best_bid, book.best_bid_qty, "ioc",
            f"SECTOR sell {ticker}: {members_str} sector-{sector} siblings "
            f"{sector_signal_bps:.1f}bps, target {target_bps:+.1f}bps",
            expected_move_cents, threshold, source="sector_arb",
        )]

    # ---- ETF basket lead-lag: constituents moved, this ETF hasn't ----

    def _etf_basket_arb(
        self, snapshot: BookSnapshot, ticker: str, state: MarketState
    ) -> list[Opportunity]:
        book = snapshot.book
        if book.best_bid is None or book.best_ask is None:
            return []
        target = snapshot.exchange
        components = ETF_BASKETS.get(ticker, [])
        if not components:
            return []

        # Average leader-exchange change across all constituents
        component_changes: list[float] = []
        for comp in components:
            leader_changes = state.leader_microprice_change(
                comp, self.config.etf_basket_lookback, exclude_exchange=target,
            )
            if leader_changes:
                component_changes.append(
                    sum(leader_changes.values()) / len(leader_changes)
                )

        # Need most components reporting (at least half)
        if len(component_changes) < max(2, len(components) // 2):
            return []

        basket_change_bps = sum(component_changes) / len(component_changes)
        if abs(basket_change_bps) < self.config.etf_basket_threshold_bps:
            return []

        # Compare to ETF's own change on target
        target_hist = state.microprice_history(target, ticker)
        if target_hist is None or len(target_hist) <= self.config.etf_basket_lookback:
            return []
        old, new = target_hist[-1 - self.config.etf_basket_lookback], target_hist[-1]
        if old <= 0:
            return []
        target_bps = (new - old) / old * 10000.0
        divergence = basket_change_bps - target_bps
        if abs(divergence) < self.config.etf_basket_threshold_bps:
            return []

        ref_price = book.mid or new
        expected_move_cents = abs(divergence) / 10000.0 * ref_price
        spread = book.spread or 0
        if expected_move_cents < spread + 1:
            return []

        threshold = self._edge_threshold(ticker, book)
        if divergence > 0:
            return [self._build_opp(
                snapshot, Side.BID, book.best_ask, book.best_ask_qty, "ioc",
                f"ETF-BASKET buy {ticker}: basket +{basket_change_bps:.1f}bps "
                f"(from {len(component_changes)} components), ETF {target_bps:+.1f}bps",
                expected_move_cents, threshold, source="etf_basket_arb",
            )]
        return [self._build_opp(
            snapshot, Side.ASK, book.best_bid, book.best_bid_qty, "ioc",
            f"ETF-BASKET sell {ticker}: basket {basket_change_bps:.1f}bps "
            f"(from {len(component_changes)} components), ETF {target_bps:+.1f}bps",
            expected_move_cents, threshold, source="etf_basket_arb",
        )]

    # ---- end-of-segment unwind: aggressively close inventory before timeout ----

    def _unwind_opportunities(
        self, state: MarketState, exchange: str, risk: "RiskManager", now: float,
    ) -> list[Opportunity]:
        out: list[Opportunity] = []
        for snapshot in state.books_on_exchange(exchange):
            inst_id = snapshot.instrument_id
            position = risk.positions.get(inst_id, 0)
            if position == 0:
                continue
            if snapshot.age_seconds(now) > self.config.stale_after_seconds:
                continue
            book = snapshot.book
            qty_to_close = min(abs(position), self.config.unwind_max_qty_per_tick)
            if qty_to_close <= 0:
                continue

            if position > 0 and book.best_bid is not None:
                # We're long → SELL at best bid
                out.append(Opportunity(
                    exchange=exchange, instrument_id=inst_id, side=Side.ASK,
                    price=book.best_bid, quantity=qty_to_close, order_type="ioc",
                    reason=f"UNWIND long {position} {inst_id}",
                    edge_cents=0.0,
                    score=1000.0,  # priority over all alpha-seeking opps
                    source="unwind",
                ))
            elif position < 0 and book.best_ask is not None:
                # We're short → BUY at best ask
                out.append(Opportunity(
                    exchange=exchange, instrument_id=inst_id, side=Side.BID,
                    price=book.best_ask, quantity=qty_to_close, order_type="ioc",
                    reason=f"UNWIND short {position} {inst_id}",
                    edge_cents=0.0,
                    score=1000.0,
                    source="unwind",
                ))
        return out

    # ---- passive quoting (carryover, off by default) ----

    def _passive_quotes(
        self, snapshot: BookSnapshot, ticker: str, fv: float
    ) -> list[Opportunity]:
        book = snapshot.book
        if snapshot.exchange_time_ms >= self.config.passive_stop_after_ms:
            return []
        if book.best_bid is None or book.best_ask is None or book.spread is None:
            return []
        if book.spread < self.config.passive_min_spread_cents:
            return []

        out: list[Opportunity] = []
        bid_p = min(book.best_bid + 1, math.floor(fv - self.config.passive_edge_cents))
        ask_p = max(book.best_ask - 1, math.ceil(fv + self.config.passive_edge_cents))
        threshold = self._edge_threshold(ticker, book)

        if book.best_bid < bid_p < book.best_ask:
            edge = fv - bid_p
            out.append(self._build_opp(
                snapshot, Side.BID, bid_p, self.config.base_order_quantity,
                "limit", f"PASSIVE bid {ticker} near FV {fv:.1f}",
                edge, threshold, source="passive",
            ))
        if book.best_bid < ask_p < book.best_ask:
            edge = ask_p - fv
            out.append(self._build_opp(
                snapshot, Side.ASK, ask_p, self.config.base_order_quantity,
                "limit", f"PASSIVE ask {ticker} near FV {fv:.1f}",
                edge, threshold, source="passive",
            ))
        return out

    # ---- helpers ----

    def _edge_threshold(self, ticker: str, book: OrderBook) -> int:
        """Per-instrument threshold; widens with current spread."""
        base = INSTRUMENT_EDGE_CENTS.get(ticker, self.config.base_min_edge_cents)
        spread = book.spread or 0
        spread_floor = int(self.config.spread_threshold_multiplier * spread)
        return max(base, spread_floor)

    def _flow_bias(self, ticker: str, state: MarketState) -> int:
        """How many cents to bias the threshold based on aggressor flow direction."""
        pressure = state.aggressor_pressure(ticker)
        # Heuristic: noticeable pressure = >50 net signed shares in window
        if abs(pressure) < 50:
            return 0
        return self.config.aggressor_flow_bias_cents if pressure > 0 else -self.config.aggressor_flow_bias_cents

    def _build_opp(
        self,
        snapshot: BookSnapshot,
        side: Side,
        price: int,
        available_qty: int,
        order_type: str,
        reason: str,
        edge: float,
        threshold: int,
        source: str,
    ) -> Opportunity:
        # Edge-scaled sizing: bigger gap → bigger order, capped by depth and config
        edge_ratio = max(1.0, edge / max(threshold, 1))
        scaled_qty = int(self.config.base_order_quantity * (1 + (edge_ratio - 1) * self.config.edge_size_multiplier))
        depth_cap = available_qty if available_qty > 0 else self.config.base_order_quantity
        qty = max(1, min(scaled_qty, depth_cap, self.config.max_order_quantity))

        # RTT from where the bot is physically located to the venue we're trading on.
        # NOTE: home_location should reflect the team's current segment (rotates each segment).
        rtt = LATENCY_RTT_MS.get(self.config.home_location, {}).get(snapshot.exchange, 0)
        latency_cost = rtt * self.config.latency_penalty_per_ms
        spread_cost = max(0, (snapshot.book.spread or 0) * 0.05)
        score = edge - spread_cost - latency_cost

        return Opportunity(
            exchange=snapshot.exchange,
            instrument_id=snapshot.instrument_id,
            side=side,
            price=int(price),
            quantity=qty,
            order_type=order_type,
            reason=reason,
            edge_cents=float(edge),
            score=float(score),
            source=source,
        )


# ============================================================
# RISK
# ============================================================

class RiskManager:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.positions: dict[str, int] = {}
        self.cash_by_exchange: dict[str, int] = {ex: STARTING_CASH_CENTS for ex in ALL_EXCHANGES}
        self.open_orders: dict[str, int] = {ex: 0 for ex in ALL_EXCHANGES}

    def reset_exchange(self, exchange: str) -> None:
        for k in [k for k in self.positions if k.startswith(f"{exchange}-")]:
            del self.positions[k]
        self.cash_by_exchange[exchange] = STARTING_CASH_CENTS
        self.open_orders[exchange] = 0

    def update_inventory(self, exchange: str, data: dict[str, Any]) -> None:
        cash = data.get("$")
        if isinstance(cash, list) and len(cash) >= 2:
            self.cash_by_exchange[exchange] = int(cash[1])
        for inst_id, pair in data.items():
            if inst_id == "$":
                continue
            if isinstance(pair, list) and len(pair) >= 2:
                self.positions[inst_id] = int(pair[1])

    def check(self, opp: Opportunity, exchange_time_ms: int) -> tuple[bool, str]:
        if exchange_time_ms >= self.config.no_new_risk_after_ms:
            return False, "near segment end"
        if opp.quantity <= 0 or opp.price <= 0:
            return False, "invalid qty/price"
        if self.open_orders.get(opp.exchange, 0) >= self.config.max_open_orders_per_exchange:
            return False, "too many open orders"

        current = self.positions.get(opp.instrument_id, 0)
        delta = opp.quantity if opp.side is Side.BID else -opp.quantity
        projected = current + delta
        if projected > min(self.config.max_long, OFFICIAL_MAX_LONG):
            return False, "long limit"
        if projected < max(self.config.max_short, OFFICIAL_MAX_SHORT):
            return False, "short limit"

        if opp.side is Side.BID:
            cash = self.cash_by_exchange.get(opp.exchange, STARTING_CASH_CENTS)
            if cash - opp.price * opp.quantity < max(self.config.min_cash_cents, OFFICIAL_MIN_CASH):
                return False, "cash floor"
        return True, "ok"

    def reserve_live_order(self, opp: Opportunity) -> None:
        if opp.order_type == "limit":
            self.open_orders[opp.exchange] = self.open_orders.get(opp.exchange, 0) + 1

    def apply_immediate_fill(self, opp: Opportunity, data: dict[str, Any]) -> None:
        inv_change = data.get("immediate_inventory_change")
        cash_change = data.get("immediate_balance_change")
        if inv_change is not None:
            self.positions[opp.instrument_id] = self.positions.get(opp.instrument_id, 0) + int(inv_change)
        if cash_change is not None:
            self.cash_by_exchange[opp.exchange] = (
                self.cash_by_exchange.get(opp.exchange, STARTING_CASH_CENTS) + int(cash_change)
            )

    def note_cancel_or_fill(self, exchange: str) -> None:
        self.open_orders[exchange] = max(0, self.open_orders.get(exchange, 0) - 1)


# ============================================================
# INFRASTRUCTURE
# ============================================================

class TokenBucket:
    def __init__(self, rate: float, capacity: int, now: Optional[float] = None) -> None:
        self.rate = float(rate)
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.updated_at = time.monotonic() if now is None else now

    def try_acquire(self, n: int = 1, now: Optional[float] = None) -> bool:
        t = time.monotonic() if now is None else now
        self.tokens = min(self.capacity, self.tokens + max(0.0, t - self.updated_at) * self.rate)
        self.updated_at = t
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

    async def wait_for_token(self) -> None:
        while not self.try_acquire():
            await asyncio.sleep(0.002)


class JSONLRecorder:
    def __init__(self, output_dir: Optional[str]) -> None:
        self.output_dir = Path(output_dir) if output_dir else None
        self._files: dict[str, Any] = {}
        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, name: str, data: dict[str, Any]) -> None:
        if self.output_dir is None:
            return
        if name not in self._files:
            self._files[name] = open(self.output_dir / f"{name}.jsonl", "a", buffering=1)
        self._files[name].write(json.dumps(data, separators=(",", ":")) + "\n")

    def close(self) -> None:
        for h in self._files.values():
            h.close()
        self._files.clear()


# ============================================================
# EXCHANGE CLIENT (one per exchange, async)
# ============================================================

class ExchangeClient:
    def __init__(
        self,
        exchange: str,
        config: BotConfig,
        state: MarketState,
        strategy: StrategyEngine,
        risk: RiskManager,
        recorder: JSONLRecorder,
    ) -> None:
        self.exchange = exchange
        self.config = config
        self.state = state
        self.strategy = strategy
        self.risk = risk
        self.recorder = recorder
        self.bucket = TokenBucket(config.max_rate_per_second, config.max_rate_per_second)
        self.request_seq = 0
        self.inflight: dict[str, Opportunity] = {}
        # Resting limit orders: order_id → (instrument_id, place_time_monotonic).
        # Used to age out stale passive quotes when passive_enabled.
        self.resting: dict[int, tuple[str, float]] = {}

    @property
    def url(self) -> str:
        return f"ws://{EXCHANGE_HOSTS[self.exchange]}:{EXCHANGE_PORT}/trade"

    def next_request_id(self, prefix: str) -> str:
        self.request_seq += 1
        return f"{self.exchange}-{prefix}-{int(time.time()*1000)}-{self.request_seq}"

    async def run_forever(self) -> None:
        try:
            from websockets.asyncio.client import connect as ws_connect
        except ImportError:
            print("Missing dependency: pip install websockets", file=sys.stderr)
            return

        while True:
            try:
                async with ws_connect(self.url, max_size=16 * 1024 * 1024) as ws:
                    self.risk.reset_exchange(self.exchange)
                    self.state.reset_exchange(self.exchange)
                    self.resting.clear()
                    welcome = json.loads(await ws.recv())
                    print(f"[{self.exchange}] connected: {welcome.get('message', welcome)}")
                    await self.request_inventory(ws)
                    await self.listen(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{self.exchange}] error: {exc}")
            print(f"[{self.exchange}] reconnecting in {self.config.reconnect_delay_seconds:.1f}s")
            await asyncio.sleep(self.config.reconnect_delay_seconds)

    async def listen(self, ws: Any) -> None:
        async for raw in ws:
            if raw == "Message rate limit exceeded":
                print(f"[{self.exchange}] rate limit hit; reconnecting")
                return
            msg = json.loads(raw)
            mt = msg.get("type")
            if mt == "market_data_update":
                await self.on_market_data(ws, msg)
            elif mt == "add_order_response":
                self.on_add_order_response(msg)
            elif mt == "cancel_order_response":
                self.on_cancel_order_response(msg)
            elif mt == "get_inventory_response":
                self.risk.update_inventory(self.exchange, msg.get("data", {}))
            elif mt == "end_of_round":
                print(f"[{self.exchange}] segment ended")
                self.risk.reset_exchange(self.exchange)
                self.state.reset_exchange(self.exchange)
                self.resting.clear()
                return
            elif mt == "error":
                print(f"[{self.exchange}] exchange error: {msg.get('message')}")

    async def on_market_data(self, ws: Any, msg: dict[str, Any]) -> None:
        now = time.monotonic()
        self.state.apply_market_data(self.exchange, msg, received_monotonic=now)
        ex_time_ms = self.state.exchange_time_ms(self.exchange)

        # Watch trade events for fills on our resting orders, drop them from registry.
        for event in msg.get("events", []):
            if event.get("event_type") == "trade":
                pid = event.get("data", {}).get("passiveOrderID")
                if isinstance(pid, int) and pid in self.resting:
                    self.resting.pop(pid, None)
                    self.risk.note_cancel_or_fill(self.exchange)
            elif event.get("event_type") == "cancel":
                oid = event.get("data", {}).get("orderID")
                if isinstance(oid, int) and oid in self.resting:
                    self.resting.pop(oid, None)
                    self.risk.note_cancel_or_fill(self.exchange)

        # Sweep stale resting limits (only meaningful when passive is on).
        await self.sweep_stale_orders(ws, now)

        opps = self.strategy.find_opportunities(self.state, self.exchange, now_monotonic=now, risk=self.risk)
        sent = 0
        for opp in opps:
            if sent >= self.config.max_orders_per_tick:
                break
            ok, why = self.risk.check(opp, ex_time_ms)
            if not ok:
                continue
            self.recorder.write("opportunities", {
                "time": ex_time_ms, "exchange": self.exchange,
                "instrument_id": opp.instrument_id, "side": opp.side.value,
                "price": opp.price, "quantity": opp.quantity, "order_type": opp.order_type,
                "edge_cents": opp.edge_cents, "score": opp.score,
                "source": opp.source, "reason": opp.reason, "live": self.config.live_trading,
            })
            await self.place_order(ws, opp)
            sent += 1

    async def sweep_stale_orders(self, ws: Any, now: float) -> None:
        if not self.resting:
            return
        max_age = self.config.passive_max_order_age_seconds
        stale = [oid for oid, (_, t) in self.resting.items() if now - t > max_age]
        for oid in stale:
            inst_id, _ = self.resting.pop(oid)
            await self.send_cancel(ws, oid, inst_id)

    async def send_cancel(self, ws: Any, order_id: int, inst_id: str) -> None:
        if not self.config.live_trading:
            print(f"[DRY {self.exchange}] cancel order_id={order_id} {inst_id}")
            return
        await self.send_json(ws, {
            "type": "cancel_order",
            "user_request_id": self.next_request_id("cancel"),
            "order_id": order_id,
            "instrument_id": inst_id,
        })

    async def request_inventory(self, ws: Any) -> None:
        await self.send_json(ws, {
            "type": "get_inventory",
            "user_request_id": self.next_request_id("inventory"),
        })

    async def place_order(self, ws: Any, opp: Opportunity) -> None:
        if not self.config.live_trading:
            print(
                f"[DRY {opp.exchange}] {opp.source} {opp.side.value} {opp.quantity} "
                f"{opp.instrument_id} @ {opp.price} edge={opp.edge_cents:.1f} "
                f"score={opp.score:.1f} | {opp.reason}"
            )
            return

        rid = self.next_request_id("order")
        req: dict[str, Any] = {
            "type": "add_order", "user_request_id": rid,
            "instrument_id": opp.instrument_id, "side": opp.side.value,
            "quantity": opp.quantity, "order_type": opp.order_type,
        }
        if opp.order_type in {"limit", "ioc"}:
            req["price"] = int(opp.price)
            req["expiry"] = int(time.time() * 1000) + self.config.ioc_expiry_ms

        self.inflight[rid] = opp
        self.risk.reserve_live_order(opp)
        await self.send_json(ws, req)

    async def send_json(self, ws: Any, req: dict[str, Any]) -> None:
        await self.bucket.wait_for_token()
        await ws.send(json.dumps(req, separators=(",", ":")))

    def on_add_order_response(self, msg: dict[str, Any]) -> None:
        rid = msg.get("user_request_id", "")
        opp = self.inflight.pop(rid, None)
        if opp is None:
            return
        if not msg.get("success", False):
            print(f"[{self.exchange}] rejected: {msg.get('data', {}).get('message')}")
            if opp.order_type == "limit":
                self.risk.note_cancel_or_fill(self.exchange)
            return
        data = msg.get("data", {})
        self.risk.apply_immediate_fill(opp, data)
        # If a limit order rested (no immediate fill) → track it for stale-sweep.
        if opp.order_type == "limit" and data.get("immediate_inventory_change") is None:
            order_id = data.get("order_id")
            if isinstance(order_id, int):
                self.resting[order_id] = (opp.instrument_id, time.monotonic())

    def on_cancel_order_response(self, msg: dict[str, Any]) -> None:
        if msg.get("success", False):
            self.risk.note_cancel_or_fill(self.exchange)


# ============================================================
# MAIN
# ============================================================

async def run_bot(config: Optional[BotConfig] = None) -> None:
    cfg = config or BotConfig.from_env()
    if not cfg.exchanges:
        raise SystemExit("No exchanges configured")

    state = MarketState(flow_window=cfg.aggressor_flow_max_trades)
    strategy = StrategyEngine(cfg)
    risk = RiskManager(cfg)
    recorder = JSONLRecorder(cfg.output_dir)

    print(
        f"AlgoTrade bot v2: exchanges={','.join(cfg.exchanges)} home={cfg.home_location} "
        f"live={cfg.live_trading} latency_arb={cfg.latency_arb_enabled}"
    )
    if not cfg.live_trading:
        print("Dry-run mode. Set LIVE_TRADING=1 to send orders.")

    clients = [
        ExchangeClient(ex, cfg, state, strategy, risk, recorder)
        for ex in cfg.exchanges
    ]
    try:
        await asyncio.gather(*(c.run_forever() for c in clients))
    finally:
        recorder.close()


def main() -> None:
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
