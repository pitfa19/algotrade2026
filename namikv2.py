#!/usr/bin/env python3
"""
AlgoTrade 2026 — full bot.

Built on the provided sample bot, preserving all of its runtime properties:
- Async websocket per exchange, lazy-imported websockets dependency.
- Default dry-run mode (set LIVE_TRADING=1 to send orders).
- Env-driven config via BotConfig.from_env().
- JSONL recorder for opportunities and fills.
- Reconnect-on-failure loop, segment-end handling, rate limiting.

Adds the full set of strategies discussed earlier:
  1. Active arb against multi-exchange microprice fair value.
  2. Latency arb on same-ticker lead-lag across cluster boundaries.
  3. Sector residual arb — sister stocks led, this one hasn't followed.
  4. ETF basket lead-lag — constituents moved on leaders, ETF on slow exchange lags.
  5. End-of-segment unwind — close inventory before timeout.
  6. Hedge legs — paired opposite-side orders on the leader exchange via OrderRouter.
  7. ETF-implied stock fair value — back out implied price from ETF + co-constituents.
  8. Adaptive volatility-scaled edge thresholds.
  9. Inventory-aware threshold skewing — harder to add, easier to close.
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
# CONSTANTS
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
# Reverse index: stock ticker -> ETFs that contain it
ETFS_CONTAINING: dict[str, list[str]] = {}
for _etf, _basket in ETF_BASKETS.items():
    for _stock in _basket:
        ETFS_CONTAINING.setdefault(_stock, []).append(_etf)

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

LEADER_EXCHANGES = {"NYSE", "NASDAQ", "SSE", "JPX", "LSE", "Euronext"}

INSTRUMENT_EDGE_CENTS: dict[str, int] = {
    "NGUP": 8, "SIMP": 8, "CARD": 8, "GOLD": 8, "DDJH": 8,
    "JZRO": 12, "HT": 12, "KRAS": 12, "KTST": 12, "XFR": 12,
    "MDKA": 12, "ETFA3": 12, "ETFB3": 12,
    "ZABA": 16, "OIT": 14, "FSR": 14, "INA": 14, "JNAF": 14,
    "KOTD": 14, "DLKV": 14, "ZITO": 14, "XAG": 14,
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
        if self.best_bid is None or self.best_ask is None:
            return None
        bq, aq = self.best_bid_qty, self.best_ask_qty
        total = bq + aq
        if total <= 0:
            return self.mid
        return (self.best_bid * aq + self.best_ask * bq) / total

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid


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


@dataclass
class Opportunity:
    """Mutable so we can attach hedge legs after construction."""
    exchange: str
    instrument_id: str
    side: "Side"
    price: int
    quantity: int
    order_type: str
    reason: str
    edge_cents: float
    score: float
    source: str
    hedge: Optional["Opportunity"] = None


# ============================================================
# CONFIG
# ============================================================

@dataclass
class BotConfig:
    exchanges: list[str] = field(default_factory=lambda: ALL_EXCHANGES.copy())
    live_trading: bool = False
    home_location: str = "ZSE"

    # Fair value
    use_microprice: bool = True
    leader_weight: float = 2.0
    fv_max_age_seconds: float = 0.8
    etf_implied_weight: float = 0.4

    # Active arb
    base_min_edge_cents: int = 12
    spread_threshold_multiplier: float = 1.2

    # Latency arb
    latency_arb_enabled: bool = True
    lead_lag_lookback: int = 2
    lead_lag_threshold_bps: float = 6.0
    lead_lag_min_latency_ms: int = 50

    # Sector residual
    sector_arb_enabled: bool = True
    sector_lookback: int = 3
    sector_threshold_bps: float = 5.0
    sector_min_members: int = 3

    # ETF basket lead-lag
    etf_basket_arb_enabled: bool = True
    etf_basket_lookback: int = 2
    etf_basket_threshold_bps: float = 5.0

    # End-of-segment unwind
    unwind_enabled: bool = True
    unwind_start_ms: int = 580_000
    unwind_max_qty_per_tick: int = 50

    # Hedge legs
    hedge_enabled: bool = True
    hedge_min_latency_ms: int = 50
    hedge_size_ratio: float = 1.0

    # Adaptive thresholds
    adaptive_threshold_enabled: bool = True
    volatility_window: int = 30
    volatility_threshold_multiplier: float = 1.8

    # Inventory-aware skewing
    inventory_skew_enabled: bool = True
    inventory_skew_max_cents: int = 6

    # Order sizing
    base_order_quantity: int = 10
    max_order_quantity: int = 50
    edge_size_multiplier: float = 0.4

    # Pacing
    max_orders_per_tick: int = 6
    max_rate_per_second: int = 350
    ioc_expiry_ms: int = 2_000

    # Passive
    passive_enabled: bool = False
    passive_min_spread_cents: int = 10
    passive_edge_cents: int = 3
    passive_stop_after_ms: int = 560_000

    # Session
    no_new_risk_after_ms: int = 590_000
    stale_after_seconds: float = 0.8

    # Risk
    max_long: int = 500
    max_short: int = -80
    min_cash_cents: int = -1_000_000
    max_open_orders_per_exchange: int = 500

    # ML price prediction (optional — None means disabled)
    ml_model_path: Optional[str] = None
    ml_arb_enabled: bool = True       # only fires if model_path is set and loads
    ml_threshold_bps: float = 4.0     # min |predicted move| in bps to consider firing
    ml_blend_into_fv: bool = False    # if True, also blend prediction into FV

    # Aggressor flow
    aggressor_flow_window_snapshots: int = 5
    aggressor_flow_bias_cents: int = 4

    latency_penalty_per_ms: float = 0.02

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
            sector_arb_enabled=parse_bool(os.environ.get("SECTOR_ARB"), default=True),
            etf_basket_arb_enabled=parse_bool(os.environ.get("ETF_BASKET_ARB"), default=True),
            hedge_enabled=parse_bool(os.environ.get("HEDGE"), default=True),
            adaptive_threshold_enabled=parse_bool(os.environ.get("ADAPTIVE_THRESHOLD"), default=True),
            inventory_skew_enabled=parse_bool(os.environ.get("INVENTORY_SKEW"), default=True),
            unwind_enabled=parse_bool(os.environ.get("UNWIND"), default=True),
            passive_enabled=parse_bool(os.environ.get("PASSIVE_ENABLED"), default=False),
            ml_model_path=os.environ.get("MODEL_PATH"),
            ml_arb_enabled=parse_bool(os.environ.get("ML_ARB"), default=True),
            ml_blend_into_fv=parse_bool(os.environ.get("ML_BLEND_FV"), default=False),
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


def opposite_side(side: Side) -> Side:
    return Side.ASK if side is Side.BID else Side.BID


# ============================================================
# MARKET STATE
# ============================================================

class MarketState:
    def __init__(self, history_len: int = 8, vol_window: int = 30) -> None:
        self._books: dict[tuple[str, str], BookSnapshot] = {}
        self._mid_history: dict[tuple[str, str], deque[float]] = {}
        self._vol: dict[tuple[str, str], deque[float]] = {}
        self._flow: dict[str, deque[int]] = {}
        self.exchange_times: dict[str, int] = {}
        self.history_len = history_len
        self.vol_window = vol_window

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
            mp = book.microprice
            if mp is not None:
                key = (exchange, ticker_from_instrument(inst_id))
                hist = self._mid_history.setdefault(key, deque(maxlen=self.history_len))
                if hist and hist[-1] > 0:
                    delta_bps = abs(mp - hist[-1]) / hist[-1] * 10000.0
                    self._vol.setdefault(key, deque(maxlen=self.vol_window)).append(delta_bps)
                hist.append(mp)

        for event in message.get("events", []):
            if event.get("event_type") != "trade":
                continue
            data = event.get("data", {})
            inst_id = data.get("instrumentID")
            if not inst_id:
                continue
            ticker = ticker_from_instrument(inst_id)
            aggressor = data.get("aggressor_side") or data.get("side")
            qty = int(data.get("quantity", 0) or 0)
            if not qty:
                continue
            sign = +1 if aggressor in ("bid", "buy", "BID", "BUY") else -1
            self._flow.setdefault(ticker, deque(maxlen=20)).append(sign * qty)

    def reset_exchange(self, exchange: str) -> None:
        for k in list(self._books):
            if k[0] == exchange:
                del self._books[k]
        for k in list(self._mid_history):
            if k[0] == exchange:
                self._mid_history[k].clear()
        for k in list(self._vol):
            if k[0] == exchange:
                self._vol[k].clear()
        self.exchange_times.pop(exchange, None)

    def book(self, exchange: str, inst_id: str) -> Optional[OrderBook]:
        snap = self._books.get((exchange, inst_id))
        return snap.book if snap else None

    def snapshot(self, exchange: str, inst_id: str) -> Optional[BookSnapshot]:
        return self._books.get((exchange, inst_id))

    def books_on_exchange(self, exchange: str) -> list[BookSnapshot]:
        return [s for (ex, _), s in self._books.items() if ex == exchange]

    def microprice_history(self, exchange: str, ticker: str) -> Optional[deque[float]]:
        return self._mid_history.get((exchange, ticker))

    def recent_volatility_bps(self, exchange: str, ticker: str) -> Optional[float]:
        q = self._vol.get((exchange, ticker))
        if not q or len(q) < 5:
            return None
        return statistics.median(q)

    def leader_microprice_change(
        self, ticker: str, lookback: int, exclude_exchange: str
    ) -> dict[str, float]:
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

    def best_leader_for(self, ticker: str, exclude_exchange: str) -> Optional[str]:
        candidates = []
        for leader in LEADER_EXCHANGES:
            if leader == exclude_exchange:
                continue
            if leader not in LISTINGS.get(ticker, []):
                continue
            snap = self._books.get((leader, instrument_id(leader, ticker)))
            if snap is not None:
                candidates.append((snap.received_monotonic, leader))
        if not candidates:
            return None
        return max(candidates)[1]

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
        q = self._flow.get(ticker)
        return sum(q) if q else 0

    def exchange_time_ms(self, exchange: str) -> int:
        return self.exchange_times.get(exchange, 0)


# ============================================================
# FAIR VALUE
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
            prices = self.state.ticker_microprices(ticker, exclude_exchange=target)

        etf_implied = self._etf_implied_stock_value(ticker, target, now_monotonic)

        if not prices and etf_implied is None:
            return None

        if prices:
            total_w, weighted_sum = 0.0, 0.0
            for ex, price in prices.items():
                w = self.config.leader_weight if ex in LEADER_EXCHANGES else 1.0
                weighted_sum += price * w
                total_w += w
            direct_fv = weighted_sum / total_w
        else:
            direct_fv = None

        if etf_implied is not None and direct_fv is not None:
            w_etf = self.config.etf_implied_weight
            return direct_fv * (1 - w_etf) + etf_implied * w_etf
        return direct_fv if direct_fv is not None else etf_implied

    def _etf_implied_stock_value(
        self, ticker: str, target: str, now_monotonic: Optional[float],
    ) -> Optional[float]:
        etfs = ETFS_CONTAINING.get(ticker, [])
        if not etfs:
            return None

        implied_values: list[float] = []
        for etf in etfs:
            basket = ETF_BASKETS[etf]
            if ticker not in basket:
                continue

            etf_prices = self.state.ticker_microprices(
                etf, max_age_seconds=self.config.fv_max_age_seconds,
                now_monotonic=now_monotonic, exclude_exchange=target,
            )
            if not etf_prices:
                continue
            etf_mp = sum(etf_prices.values()) / len(etf_prices)

            other_prices: list[float] = []
            ok = True
            for other in basket:
                if other == ticker:
                    continue
                op = self.state.ticker_microprices(
                    other, max_age_seconds=self.config.fv_max_age_seconds,
                    now_monotonic=now_monotonic, exclude_exchange=target,
                )
                if not op:
                    ok = False
                    break
                other_prices.append(sum(op.values()) / len(op))
            if not ok:
                continue

            N = len(basket)
            implied_values.append(N * etf_mp - sum(other_prices))

        if not implied_values:
            return None
        return sum(implied_values) / len(implied_values)

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
# STRATEGY
# ============================================================

class StrategyEngine:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.price_model: Any = None  # PriceModel | None — lazy import to avoid hard dep
        if config.ml_arb_enabled and config.ml_model_path:
            try:
                from price_model import PriceModel  # noqa
                self.price_model = PriceModel.from_file(config.ml_model_path)
                print(f"Loaded price model: {self.price_model}")
            except FileNotFoundError:
                print(f"WARNING: model file not found at {config.ml_model_path}; ML disabled")
            except Exception as exc:
                print(f"WARNING: failed to load model: {exc}; ML disabled")

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

        if self.config.unwind_enabled and risk is not None and ex_time >= self.config.unwind_start_ms:
            opps.extend(self._unwind_opportunities(state, exchange, risk, now))

        for snapshot in state.books_on_exchange(exchange):
            if snapshot.age_seconds(now) > self.config.stale_after_seconds:
                continue

            ticker = snapshot.ticker
            fv = fv_engine.fair_value(ticker, exchange, now)

            if fv is not None:
                opps.extend(self._active_arb(snapshot, ticker, fv, state, risk))

            if self.config.latency_arb_enabled:
                opps.extend(self._latency_arb(snapshot, ticker, state, risk))

            if self.config.sector_arb_enabled and ticker in SECTOR_OF:
                opps.extend(self._sector_residual_arb(snapshot, ticker, state, risk))

            if self.config.etf_basket_arb_enabled and ticker in ETF_BASKETS:
                opps.extend(self._etf_basket_arb(snapshot, ticker, state, risk))

            if self.price_model is not None:
                opps.extend(self._ml_arb(snapshot, ticker, state, risk))

            if self.config.passive_enabled and fv is not None:
                opps.extend(self._passive_quotes(snapshot, ticker, fv, state, risk))

        opps = [o for o in opps if o.score > 0]
        opps.sort(key=lambda o: o.score, reverse=True)
        return opps

    # ---- active arb ----
    def _active_arb(
        self, snapshot: BookSnapshot, ticker: str, fv: float,
        state: MarketState, risk: Optional["RiskManager"],
    ) -> list[Opportunity]:
        book = snapshot.book
        threshold = self._edge_threshold(ticker, book, state, snapshot.exchange)
        flow_bias = self._flow_bias(ticker, state)
        inv_bid_skew, inv_ask_skew = self._inventory_skew(snapshot.instrument_id, risk)
        out: list[Opportunity] = []

        buy_threshold = max(2, threshold - flow_bias + inv_bid_skew)
        sell_threshold = max(2, threshold + flow_bias + inv_ask_skew)

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

    # ---- latency arb (with hedge) ----
    def _latency_arb(
        self, snapshot: BookSnapshot, ticker: str,
        state: MarketState, risk: Optional["RiskManager"],
    ) -> list[Opportunity]:
        book = snapshot.book
        if book.best_bid is None or book.best_ask is None:
            return []

        target = snapshot.exchange
        leader_moves = state.leader_microprice_change(
            ticker, self.config.lead_lag_lookback, exclude_exchange=target,
        )
        if not leader_moves:
            return []

        rtt = LATENCY_RTT_MS.get(target, {})
        usable = {ex: bps for ex, bps in leader_moves.items()
                  if rtt.get(ex, 0) >= self.config.lead_lag_min_latency_ms}
        if not usable:
            return []

        total_w, weighted_bps = 0.0, 0.0
        for ex, bps in usable.items():
            w = rtt.get(ex, 0) / 100.0
            weighted_bps += bps * w
            total_w += w
        leader_signal = weighted_bps / total_w if total_w > 0 else 0.0

        if abs(leader_signal) < self.config.lead_lag_threshold_bps:
            return []

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

        ref_price = book.mid or new
        expected_move_cents = abs(divergence) / 10000.0 * ref_price
        spread = book.spread or 0
        if expected_move_cents < spread + 1:
            return []

        threshold = self._edge_threshold(ticker, book, state, target)
        if divergence > 0:
            primary = self._build_opp(
                snapshot, Side.BID, book.best_ask, book.best_ask_qty, "ioc",
                f"LATARB buy {ticker}: leaders +{leader_signal:.1f}bps, target +{target_bps:.1f}bps",
                expected_move_cents, threshold, source="latency_arb",
            )
        else:
            primary = self._build_opp(
                snapshot, Side.ASK, book.best_bid, book.best_bid_qty, "ioc",
                f"LATARB sell {ticker}: leaders {leader_signal:.1f}bps, target {target_bps:.1f}bps",
                expected_move_cents, threshold, source="latency_arb",
            )

        primary.hedge = self._build_hedge(primary, ticker, state)
        return [primary]

    # ---- sector residual ----
    def _sector_residual_arb(
        self, snapshot: BookSnapshot, ticker: str,
        state: MarketState, risk: Optional["RiskManager"],
    ) -> list[Opportunity]:
        book = snapshot.book
        if book.best_bid is None or book.best_ask is None:
            return []
        target = snapshot.exchange
        sector = SECTOR_OF.get(ticker)
        if sector is None:
            return []

        sibling_returns_bps: list[float] = []
        for sibling in SECTOR_MEMBERS[sector]:
            if sibling == ticker:
                continue
            leader_changes = state.leader_microprice_change(
                sibling, self.config.sector_lookback, exclude_exchange=target,
            )
            if leader_changes:
                sibling_returns_bps.append(sum(leader_changes.values()) / len(leader_changes))

        if len(sibling_returns_bps) < self.config.sector_min_members:
            return []

        sector_signal_bps = sum(sibling_returns_bps) / len(sibling_returns_bps)
        if abs(sector_signal_bps) < self.config.sector_threshold_bps:
            return []

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

        threshold = self._edge_threshold(ticker, book, state, target)
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

    # ---- ETF basket arb (with hedge) ----
    def _etf_basket_arb(
        self, snapshot: BookSnapshot, ticker: str,
        state: MarketState, risk: Optional["RiskManager"],
    ) -> list[Opportunity]:
        book = snapshot.book
        if book.best_bid is None or book.best_ask is None:
            return []
        target = snapshot.exchange
        components = ETF_BASKETS.get(ticker, [])
        if not components:
            return []

        component_changes: list[float] = []
        for comp in components:
            leader_changes = state.leader_microprice_change(
                comp, self.config.etf_basket_lookback, exclude_exchange=target,
            )
            if leader_changes:
                component_changes.append(sum(leader_changes.values()) / len(leader_changes))

        if len(component_changes) < max(2, len(components) // 2):
            return []

        basket_change_bps = sum(component_changes) / len(component_changes)
        if abs(basket_change_bps) < self.config.etf_basket_threshold_bps:
            return []

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

        threshold = self._edge_threshold(ticker, book, state, target)
        if divergence > 0:
            primary = self._build_opp(
                snapshot, Side.BID, book.best_ask, book.best_ask_qty, "ioc",
                f"ETF-BASKET buy {ticker}: basket +{basket_change_bps:.1f}bps "
                f"(from {len(component_changes)} comp), ETF {target_bps:+.1f}bps",
                expected_move_cents, threshold, source="etf_basket_arb",
            )
        else:
            primary = self._build_opp(
                snapshot, Side.ASK, book.best_bid, book.best_bid_qty, "ioc",
                f"ETF-BASKET sell {ticker}: basket {basket_change_bps:.1f}bps "
                f"(from {len(component_changes)} comp), ETF {target_bps:+.1f}bps",
                expected_move_cents, threshold, source="etf_basket_arb",
            )

        primary.hedge = self._build_hedge(primary, ticker, state)
        return [primary]

    # ---- ML-based arb: use predicted next-snapshot move ----
    def _ml_arb(
        self, snapshot: BookSnapshot, ticker: str,
        state: MarketState, risk: Optional["RiskManager"],
    ) -> list[Opportunity]:
        if self.price_model is None:
            return []
        book = snapshot.book
        if book.best_bid is None or book.best_ask is None:
            return []

        from price_model import build_features
        features = build_features(state, snapshot.exchange, ticker)
        if features is None:
            return []
        pred_bps = self.price_model.predict_bps(features)
        if pred_bps is None or abs(pred_bps) < self.config.ml_threshold_bps:
            return []

        mid = book.mid
        if mid is None:
            return []
        expected_move_cents = abs(pred_bps) / 10000.0 * mid
        spread = book.spread or 0
        if expected_move_cents < spread + 1:
            return []

        threshold = self._edge_threshold(ticker, book, state, snapshot.exchange)
        if pred_bps > 0:
            return [self._build_opp(
                snapshot, Side.BID, book.best_ask, book.best_ask_qty, "ioc",
                f"ML buy {ticker}: predicted +{pred_bps:.1f}bps next snapshot",
                expected_move_cents, threshold, source="ml_arb",
            )]
        return [self._build_opp(
            snapshot, Side.ASK, book.best_bid, book.best_bid_qty, "ioc",
            f"ML sell {ticker}: predicted {pred_bps:.1f}bps next snapshot",
            expected_move_cents, threshold, source="ml_arb",
        )]

    # ---- end-of-segment unwind ----
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
                out.append(Opportunity(
                    exchange=exchange, instrument_id=inst_id, side=Side.ASK,
                    price=book.best_bid, quantity=qty_to_close, order_type="ioc",
                    reason=f"UNWIND long {position} {inst_id}",
                    edge_cents=0.0, score=1000.0, source="unwind",
                ))
            elif position < 0 and book.best_ask is not None:
                out.append(Opportunity(
                    exchange=exchange, instrument_id=inst_id, side=Side.BID,
                    price=book.best_ask, quantity=qty_to_close, order_type="ioc",
                    reason=f"UNWIND short {position} {inst_id}",
                    edge_cents=0.0, score=1000.0, source="unwind",
                ))
        return out

    # ---- passive ----
    def _passive_quotes(
        self, snapshot: BookSnapshot, ticker: str, fv: float,
        state: MarketState, risk: Optional["RiskManager"],
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
        threshold = self._edge_threshold(ticker, book, state, snapshot.exchange)

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
    def _edge_threshold(
        self, ticker: str, book: OrderBook, state: MarketState, exchange: str,
    ) -> int:
        base = INSTRUMENT_EDGE_CENTS.get(ticker, self.config.base_min_edge_cents)
        spread = book.spread or 0
        spread_floor = int(self.config.spread_threshold_multiplier * spread)
        threshold = max(base, spread_floor)

        if self.config.adaptive_threshold_enabled:
            vol_bps = state.recent_volatility_bps(exchange, ticker)
            if vol_bps is not None and book.mid is not None:
                vol_cents = (vol_bps / 10000.0) * book.mid * self.config.volatility_threshold_multiplier
                threshold = max(threshold, int(vol_cents))

        return threshold

    def _flow_bias(self, ticker: str, state: MarketState) -> int:
        pressure = state.aggressor_pressure(ticker)
        if abs(pressure) < 50:
            return 0
        return self.config.aggressor_flow_bias_cents if pressure > 0 else -self.config.aggressor_flow_bias_cents

    def _inventory_skew(
        self, instrument_id: str, risk: Optional["RiskManager"],
    ) -> tuple[int, int]:
        """Skew thresholds away from current position.

        Long → bid_skew positive (harder to add long), ask_skew negative (easier to close).
        Short → bid_skew negative (easier to cover), ask_skew positive (harder to add short).
        Ratio is computed against the side-specific limit, not the larger of both.
        """
        if not self.config.inventory_skew_enabled or risk is None:
            return 0, 0
        position = risk.positions.get(instrument_id, 0)
        if position == 0:
            return 0, 0
        if position > 0:
            limit = max(1, self.config.max_long)
            ratio = min(1.0, position / limit)
        else:
            limit = max(1, abs(self.config.max_short))
            ratio = -min(1.0, abs(position) / limit)
        # Round-half-away-from-zero so partial fractions still produce a 1¢ minimum skew
        raw = ratio * self.config.inventory_skew_max_cents
        skew = int(raw + (0.5 if raw > 0 else -0.5))
        return skew, -skew

    def _build_hedge(
        self, primary: Opportunity, ticker: str, state: MarketState,
    ) -> Optional[Opportunity]:
        if not self.config.hedge_enabled:
            return None
        leader = state.best_leader_for(ticker, exclude_exchange=primary.exchange)
        if leader is None:
            return None
        rtt = LATENCY_RTT_MS.get(primary.exchange, {}).get(leader, 0)
        if rtt < self.config.hedge_min_latency_ms:
            return None

        leader_book = state.book(leader, instrument_id(leader, ticker))
        if leader_book is None:
            return None
        hedge_side = opposite_side(primary.side)
        if hedge_side is Side.BID and leader_book.best_ask is not None:
            price = leader_book.best_ask
        elif hedge_side is Side.ASK and leader_book.best_bid is not None:
            price = leader_book.best_bid
        else:
            return None

        hedge_qty = max(1, int(primary.quantity * self.config.hedge_size_ratio))
        return Opportunity(
            exchange=leader,
            instrument_id=instrument_id(leader, ticker),
            side=hedge_side,
            price=int(price),
            quantity=hedge_qty,
            order_type="ioc",
            reason=f"HEDGE for {primary.source} {primary.instrument_id}",
            edge_cents=0.0,
            score=primary.score * 0.9,
            source=f"hedge_{primary.source}",
        )

    def _build_opp(
        self, snapshot: BookSnapshot, side: Side, price: int, available_qty: int,
        order_type: str, reason: str, edge: float, threshold: int, source: str,
    ) -> Opportunity:
        edge_ratio = max(1.0, edge / max(threshold, 1))
        scaled_qty = int(self.config.base_order_quantity * (1 + (edge_ratio - 1) * self.config.edge_size_multiplier))
        depth_cap = available_qty if available_qty > 0 else self.config.base_order_quantity
        qty = max(1, min(scaled_qty, depth_cap, self.config.max_order_quantity))

        spread_cost = max(0, (snapshot.book.spread or 0) * 0.05)
        score = edge - spread_cost

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
        # Unwind orders bypass the no-new-risk window
        if opp.source != "unwind" and exchange_time_ms >= self.config.no_new_risk_after_ms:
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
# ORDER ROUTER
# ============================================================

class OrderRouter:
    """Dispatches hedge orders to the appropriate ExchangeClient's websocket."""

    def __init__(self) -> None:
        self.clients: dict[str, "ExchangeClient"] = {}

    def register(self, exchange: str, client: "ExchangeClient") -> None:
        self.clients[exchange] = client

    async def dispatch_hedge(self, hedge: Opportunity) -> None:
        client = self.clients.get(hedge.exchange)
        if client is None:
            print(f"[router] no client registered for hedge target {hedge.exchange}")
            return
        try:
            await client.fire_external_order(hedge)
        except Exception as exc:
            print(f"[router] hedge dispatch failed: {exc}")


# ============================================================
# EXCHANGE CLIENT
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
        router: OrderRouter,
    ) -> None:
        self.exchange = exchange
        self.config = config
        self.state = state
        self.strategy = strategy
        self.risk = risk
        self.recorder = recorder
        self.router = router
        self.bucket = TokenBucket(config.max_rate_per_second, config.max_rate_per_second)
        self.request_seq = 0
        self.inflight: dict[str, Opportunity] = {}
        self._ws: Any = None
        router.register(exchange, self)

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
                    self._ws = ws
                    self.risk.reset_exchange(self.exchange)
                    self.state.reset_exchange(self.exchange)
                    welcome = json.loads(await ws.recv())
                    print(f"[{self.exchange}] connected: {welcome.get('message', welcome)}")
                    await self.request_inventory(ws)
                    await self.listen(ws)
            except asyncio.CancelledError:
                self._ws = None
                raise
            except Exception as exc:
                print(f"[{self.exchange}] error: {exc}")
            self._ws = None
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
                return
            elif mt == "error":
                print(f"[{self.exchange}] exchange error: {msg.get('message')}")

    async def on_market_data(self, ws: Any, msg: dict[str, Any]) -> None:
        now = time.monotonic()
        self.state.apply_market_data(self.exchange, msg, received_monotonic=now)
        ex_time_ms = self.state.exchange_time_ms(self.exchange)

        opps = self.strategy.find_opportunities(
            self.state, self.exchange, now_monotonic=now, risk=self.risk,
        )
        sent = 0
        for opp in opps:
            if sent >= self.config.max_orders_per_tick:
                break
            ok, why = self.risk.check(opp, ex_time_ms)
            if not ok:
                continue
            self._record_opp(opp, ex_time_ms)
            await self.place_order(ws, opp)
            sent += 1
            if opp.hedge is not None:
                # Concurrent hedge dispatch — does not block primary path
                asyncio.create_task(self.router.dispatch_hedge(opp.hedge))

    def _record_opp(self, opp: Opportunity, ex_time_ms: int) -> None:
        self.recorder.write("opportunities", {
            "time": ex_time_ms, "exchange": self.exchange,
            "instrument_id": opp.instrument_id, "side": opp.side.value,
            "price": opp.price, "quantity": opp.quantity, "order_type": opp.order_type,
            "edge_cents": opp.edge_cents, "score": opp.score,
            "source": opp.source, "reason": opp.reason,
            "has_hedge": opp.hedge is not None, "live": self.config.live_trading,
        })

    async def fire_external_order(self, opp: Opportunity) -> None:
        """Process a hedge order arriving from another client via the OrderRouter."""
        if self._ws is None:
            print(f"[{self.exchange}] hedge dropped: not connected")
            return
        ex_time_ms = self.state.exchange_time_ms(self.exchange)
        ok, why = self.risk.check(opp, ex_time_ms)
        if not ok:
            print(f"[{self.exchange}] hedge rejected: {why}")
            return
        self._record_opp(opp, ex_time_ms)
        await self.place_order(self._ws, opp)

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
                f"score={opp.score:.1f}"
                + (f" [hedge→{opp.hedge.exchange}]" if opp.hedge else "")
                + f" | {opp.reason}"
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
        self.recorder.write("fills", {
            "exchange": self.exchange, "instrument_id": opp.instrument_id,
            "side": opp.side.value, "price": opp.price, "quantity": opp.quantity,
            "source": opp.source, "data": data,
        })

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

    state = MarketState()
    strategy = StrategyEngine(cfg)
    risk = RiskManager(cfg)
    recorder = JSONLRecorder(cfg.output_dir)
    router = OrderRouter()

    print(
        f"AlgoTrade bot v3: exchanges={','.join(cfg.exchanges)} home={cfg.home_location} "
        f"live={cfg.live_trading}\n"
        f"  strategies: lat_arb={cfg.latency_arb_enabled} sector={cfg.sector_arb_enabled} "
        f"etf={cfg.etf_basket_arb_enabled} unwind={cfg.unwind_enabled} hedge={cfg.hedge_enabled}\n"
        f"  features: adaptive_thresh={cfg.adaptive_threshold_enabled} "
        f"inv_skew={cfg.inventory_skew_enabled} etf_implied_fv_w={cfg.etf_implied_weight}"
    )
    if not cfg.live_trading:
        print("Dry-run mode. Set LIVE_TRADING=1 to send orders.")

    clients = [
        ExchangeClient(ex, cfg, state, strategy, risk, recorder, router)
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