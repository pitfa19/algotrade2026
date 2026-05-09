#!/usr/bin/env python3
"""Competition-ready AlgoTrade 2026 trading bot.

The bot is intentionally standalone. It uses only the documented WebSocket API,
keeps all market prices as integer cents, defaults to dry-run, and routes every
order through a shared risk gate before it reaches the wire.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import json
import logging
import os
import random
import signal
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from statistics import median
from typing import Any, Deque, Iterable, Optional


STARTING_CASH_CENTS = 10_000_000
HARD_CASH_FLOOR_CENTS = -5_000_000
HARD_SHORT_FLOOR = -200
HARD_LONG_CEILING = 2_000
ROUND_LENGTH_MS = 600_000

EXCHANGES = [
    "NYSE",
    "NASDAQ",
    "SSE",
    "JPX",
    "EURONEXT",
    "LSE",
    "HKEX",
    "NSE",
    "TMX",
    "ZSE",
]

EXCHANGE_HOSTS = {
    "NYSE": "nyse.algotrade.hr",
    "NASDAQ": "nasdaq.algotrade.hr",
    "SSE": "sse.algotrade.hr",
    "JPX": "jpx.algotrade.hr",
    "EURONEXT": "euronext.algotrade.hr",
    "LSE": "lse.algotrade.hr",
    "HKEX": "hkex.algotrade.hr",
    "NSE": "nse.algotrade.hr",
    "TMX": "tmx.algotrade.hr",
    "ZSE": "zse.algotrade.hr",
}

SECTOR_A = ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"]
SECTOR_B = ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"]
INDEPENDENT = ["MDKA", "KRAS", "ZITO", "ZABA", "SIMP", "CARD"]
SAFE_HAVEN = ["GOLD", "XAG"]
STOCKS = SECTOR_A + SECTOR_B + INDEPENDENT + SAFE_HAVEN

ETF_BASKETS = {
    "ETFA": ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB": ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}
ETFS = list(ETF_BASKETS)

STOCK_LISTINGS = {
    "CARD": EXCHANGES[:],
    "SIMP": EXCHANGES[:],
    "NGUP": ["NYSE", "NASDAQ", "EURONEXT", "TMX", "ZSE"],
    "OIT": ["LSE", "EURONEXT", "HKEX", "NSE", "ZSE"],
    "KTST": ["NYSE", "JPX", "TMX", "ZSE"],
    "FSR": ["NASDAQ", "LSE", "SSE", "HKEX", "ZSE"],
    "JZRO": ["NYSE", "LSE", "EURONEXT", "TMX", "ZSE"],
    "XFR": ["NYSE", "HKEX", "TMX", "ZSE"],
    "KOTD": ["NASDAQ", "LSE", "EURONEXT", "HKEX", "ZSE"],
    "INA": ["NYSE", "NASDAQ", "EURONEXT", "HKEX", "ZSE"],
    "HT": ["NASDAQ", "LSE", "JPX", "SSE", "TMX", "ZSE"],
    "JNAF": ["NYSE", "EURONEXT", "JPX", "HKEX", "ZSE"],
    "DLKV": ["NASDAQ", "LSE", "HKEX", "NSE", "ZSE"],
    "DDJH": ["NYSE", "LSE", "EURONEXT", "TMX", "ZSE"],
    "MDKA": ["NYSE", "LSE", "HKEX", "TMX", "ZSE"],
    "KRAS": ["NYSE", "EURONEXT", "SSE", "TMX", "ZSE"],
    "ZITO": ["NASDAQ", "LSE", "EURONEXT", "NSE", "ZSE"],
    "ZABA": ["NYSE", "LSE", "SSE", "NSE", "TMX", "ZSE"],
    "GOLD": ["NASDAQ", "EURONEXT", "JPX", "TMX", "ZSE"],
    "XAG": ["LSE", "EURONEXT", "JPX", "ZSE"],
}

ETF_LISTINGS = {
    "ETFA": ["NYSE", "EURONEXT", "HKEX", "ZSE"],
    "ETFB": ["NASDAQ", "LSE", "HKEX", "ZSE"],
    "ETFA3": ["NYSE", "TMX", "ZSE"],
    "ETFB3": ["NASDAQ", "HKEX", "ZSE"],
    "ETFSH": ["EURONEXT", "JPX", "ZSE"],
}

TICKER_LISTINGS = {**STOCK_LISTINGS, **ETF_LISTINGS}
ALL_TICKERS = STOCKS + ETFS

LATENCY_RTT_MS = {
    "NYSE": {
        "NYSE": 0,
        "NASDAQ": 1,
        "SSE": 165,
        "JPX": 152,
        "EURONEXT": 84,
        "LSE": 80,
        "HKEX": 180,
        "NSE": 174,
        "TMX": 11,
        "ZSE": 96,
    },
    "NASDAQ": {
        "NYSE": 1,
        "NASDAQ": 0,
        "SSE": 165,
        "JPX": 152,
        "EURONEXT": 84,
        "LSE": 80,
        "HKEX": 180,
        "NSE": 174,
        "TMX": 11,
        "ZSE": 96,
    },
    "SSE": {
        "NYSE": 165,
        "NASDAQ": 165,
        "SSE": 0,
        "JPX": 18,
        "EURONEXT": 160,
        "LSE": 156,
        "HKEX": 19,
        "NSE": 54,
        "TMX": 159,
        "ZSE": 145,
    },
    "JPX": {
        "NYSE": 152,
        "NASDAQ": 152,
        "SSE": 18,
        "JPX": 0,
        "EURONEXT": 145,
        "LSE": 141,
        "HKEX": 37,
        "NSE": 53,
        "TMX": 145,
        "ZSE": 140,
    },
    "EURONEXT": {
        "NYSE": 84,
        "NASDAQ": 84,
        "SSE": 160,
        "JPX": 145,
        "EURONEXT": 0,
        "LSE": 6,
        "HKEX": 130,
        "NSE": 130,
        "TMX": 86,
        "ZSE": 22,
    },
    "LSE": {
        "NYSE": 80,
        "NASDAQ": 80,
        "SSE": 156,
        "JPX": 141,
        "EURONEXT": 6,
        "LSE": 0,
        "HKEX": 135,
        "NSE": 134,
        "TMX": 82,
        "ZSE": 24,
    },
    "HKEX": {
        "NYSE": 180,
        "NASDAQ": 180,
        "SSE": 19,
        "JPX": 37,
        "EURONEXT": 130,
        "LSE": 135,
        "HKEX": 0,
        "NSE": 53,
        "TMX": 174,
        "ZSE": 150,
    },
    "NSE": {
        "NYSE": 174,
        "NASDAQ": 174,
        "SSE": 54,
        "JPX": 53,
        "EURONEXT": 130,
        "LSE": 134,
        "HKEX": 53,
        "NSE": 0,
        "TMX": 174,
        "ZSE": 95,
    },
    "TMX": {
        "NYSE": 11,
        "NASDAQ": 11,
        "SSE": 159,
        "JPX": 145,
        "EURONEXT": 86,
        "LSE": 82,
        "HKEX": 174,
        "NSE": 174,
        "TMX": 0,
        "ZSE": 98,
    },
    "ZSE": {
        "NYSE": 96,
        "NASDAQ": 96,
        "SSE": 145,
        "JPX": 140,
        "EURONEXT": 22,
        "LSE": 24,
        "HKEX": 150,
        "NSE": 95,
        "TMX": 98,
        "ZSE": 0,
    },
}


def now_ms() -> int:
    return int(time.time() * 1000)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def parse_csv(raw: str) -> list[str]:
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


def parse_host_overrides(raw: str) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for part in raw.split(","):
        if not part.strip() or "=" not in part:
            continue
        key, value = part.split("=", 1)
        exchange = key.strip().upper()
        if exchange in EXCHANGES and value.strip():
            overrides[exchange] = value.strip()
    return overrides


def ticker_from_instrument(instrument_id: str) -> str:
    return instrument_id.split("-", 1)[1]


def exchange_from_instrument(instrument_id: str) -> str:
    return instrument_id.split("-", 1)[0].upper()


def instrument_id(exchange: str, ticker: str) -> str:
    return f"{exchange.upper()}-{ticker.upper()}"


def equal_weight_fair_cents(prices: Iterable[int]) -> int:
    values = [int(price) for price in prices]
    if not values:
        raise ValueError("at least one price is required")
    return (sum(values) + len(values) // 2) // len(values)


def median_int(values: Iterable[int], default: int = 0) -> int:
    items = sorted(int(value) for value in values)
    if not items:
        return default
    idx = len(items) // 2
    if len(items) % 2:
        return items[idx]
    return (items[idx - 1] + items[idx]) // 2


def mad_int(values: Iterable[int], center: Optional[int] = None) -> int:
    items = [int(value) for value in values]
    if not items:
        return 0
    if center is None:
        center = median_int(items)
    return median_int(abs(value - center) for value in items)


@dataclass(frozen=True)
class OrderBook:
    bids: tuple[tuple[int, int], ...] = ()
    asks: tuple[tuple[int, int], ...] = ()
    last_update_ms: int = 0

    @classmethod
    def from_depth(cls, depth: dict[str, Any], now_ms: Optional[int] = None) -> "OrderBook":
        bid_items = []
        ask_items = []
        for price_raw, qty_raw in (depth.get("bids") or {}).items():
            price = int(price_raw)
            qty = int(qty_raw)
            if qty > 0:
                bid_items.append((price, qty))
        for price_raw, qty_raw in (depth.get("asks") or {}).items():
            price = int(price_raw)
            qty = int(qty_raw)
            if qty > 0:
                ask_items.append((price, qty))
        bid_items.sort(key=lambda item: item[0], reverse=True)
        ask_items.sort(key=lambda item: item[0])
        return cls(tuple(bid_items), tuple(ask_items), now_ms if now_ms is not None else globals()["now_ms"]())

    @property
    def best_bid(self) -> Optional[int]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return self.asks[0][0] if self.asks else None

    @property
    def best_bid_qty(self) -> int:
        return self.bids[0][1] if self.bids else 0

    @property
    def best_ask_qty(self) -> int:
        return self.asks[0][1] if self.asks else 0

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) // 2

    def microprice(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return self.mid
        bid_qty = self.best_bid_qty
        ask_qty = self.best_ask_qty
        total = bid_qty + ask_qty
        if total <= 0:
            return self.mid
        return (self.best_ask * bid_qty + self.best_bid * ask_qty + total // 2) // total

    def imbalance_bps(self) -> int:
        bid_qty = self.best_bid_qty
        ask_qty = self.best_ask_qty
        total = bid_qty + ask_qty
        if total <= 0:
            return 0
        return ((bid_qty - ask_qty) * 10_000) // total

    def available_to_trade(self, side: str, limit_price: Optional[int]) -> int:
        if limit_price is None:
            levels = self.asks if side == "bid" else self.bids
            return sum(qty for _, qty in levels)
        total = 0
        if side == "bid":
            for price, qty in self.asks:
                if price <= limit_price:
                    total += qty
        else:
            for price, qty in self.bids:
                if price >= limit_price:
                    total += qty
        return total

    def vwap_to_fill(self, side: str, quantity: int, limit_price: Optional[int]) -> tuple[Optional[int], int, Optional[int]]:
        remaining = quantity
        notional = 0
        filled = 0
        worst_price: Optional[int] = None
        levels = self.asks if side == "bid" else self.bids
        for price, qty in levels:
            if limit_price is not None:
                if side == "bid" and price > limit_price:
                    break
                if side == "ask" and price < limit_price:
                    break
            take = min(remaining, qty)
            if take <= 0:
                continue
            notional += take * price
            filled += take
            remaining -= take
            worst_price = price
            if remaining <= 0:
                break
        if filled <= 0:
            return None, 0, worst_price
        return (notional + filled // 2) // filled, filled, worst_price


@dataclass
class ExchangeState:
    exchange: str
    cash_total: int = STARTING_CASH_CENTS
    cash_reserved: int = 0
    positions: dict[str, int] = field(default_factory=dict)
    reserved_positions: dict[str, int] = field(default_factory=dict)
    orderbooks: dict[str, OrderBook] = field(default_factory=dict)
    pending_orders_count: int = 0
    server_time_ms: int = -1
    round_length_ms: int = ROUND_LENGTH_MS
    connected: bool = False
    end_seen: bool = False
    last_market_update_ms: int = 0
    previous_mid: dict[str, int] = field(default_factory=dict)
    current_mid: dict[str, int] = field(default_factory=dict)
    mid_changes: dict[str, Deque[int]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=32)))
    last_inventory_ms: int = 0

    def reset_for_segment(self) -> None:
        self.cash_total = STARTING_CASH_CENTS
        self.cash_reserved = 0
        self.positions.clear()
        self.reserved_positions.clear()
        self.orderbooks.clear()
        self.pending_orders_count = 0
        self.server_time_ms = -1
        self.connected = False
        self.end_seen = False
        self.last_market_update_ms = 0
        self.previous_mid.clear()
        self.current_mid.clear()
        self.mid_changes.clear()

    def clone_for_risk(self) -> "ExchangeState":
        clone = ExchangeState(exchange=self.exchange)
        clone.cash_total = self.cash_total
        clone.cash_reserved = self.cash_reserved
        clone.positions = dict(self.positions)
        clone.reserved_positions = dict(self.reserved_positions)
        clone.pending_orders_count = self.pending_orders_count
        clone.server_time_ms = self.server_time_ms
        clone.round_length_ms = self.round_length_ms
        clone.end_seen = self.end_seen
        return clone

    def update_inventory(self, data: dict[str, Any]) -> None:
        cash = data.get("$")
        if cash is not None and len(cash) >= 2:
            self.cash_reserved = int(cash[0])
            self.cash_total = int(cash[1])
        for key, value in data.items():
            if key == "$" or not isinstance(value, list) or len(value) < 2:
                continue
            self.reserved_positions[key] = int(value[0])
            self.positions[key] = int(value[1])
        self.last_inventory_ms = now_ms()

    def update_pending_orders(self, data: dict[str, Any]) -> None:
        count = 0
        for pair in data.values():
            if not isinstance(pair, list) or len(pair) < 2:
                continue
            count += sum(1 for order in pair[0] if order.get("live", True))
            count += sum(1 for order in pair[1] if order.get("live", True))
        self.pending_orders_count = count

    def apply_market_data(self, message: dict[str, Any]) -> None:
        self.server_time_ms = int(message.get("time", self.server_time_ms))
        self.last_market_update_ms = now_ms()
        for inst, depth in (message.get("orderbook_depths") or {}).items():
            book = OrderBook.from_depth(depth, now_ms=self.last_market_update_ms)
            new_mid = book.mid
            old_mid = self.current_mid.get(inst)
            if new_mid is not None:
                if old_mid is not None and new_mid != old_mid:
                    self.previous_mid[inst] = old_mid
                    self.mid_changes[inst].append(abs(new_mid - old_mid))
                self.current_mid[inst] = new_mid
            self.orderbooks[inst] = book

    def apply_immediate_fill(self, instrument: str, inventory_change: int, balance_change: int) -> None:
        self.positions[instrument] = self.positions.get(instrument, 0) + int(inventory_change)
        self.cash_total += int(balance_change)

    def position(self, instrument: str) -> int:
        return int(self.positions.get(instrument, 0))

    def time_left_ms(self) -> int:
        if self.server_time_ms < 0:
            return self.round_length_ms
        return max(0, self.round_length_ms - self.server_time_ms)

    def volatility_cents(self, instrument: str) -> int:
        changes = self.mid_changes.get(instrument)
        if not changes:
            return 0
        return max(0, sum(changes) // len(changes))

    def estimated_nav_cents(self) -> int:
        nav = self.cash_total
        for inst, pos in self.positions.items():
            book = self.orderbooks.get(inst)
            if book and book.mid is not None:
                nav += pos * book.mid
        return nav


@dataclass(frozen=True)
class OrderIntent:
    exchange: str
    instrument_id: str
    side: str
    quantity: int
    price: Optional[int]
    order_type: str
    strategy: str
    reason: str
    priority: int = 0
    reduce_only: bool = False
    opportunity_id: str = ""
    expiry_ms: int = 1_000

    def to_message(self, user_request_id: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "add_order",
            "user_request_id": user_request_id,
            "instrument_id": self.instrument_id,
            "side": self.side,
            "quantity": int(self.quantity),
            "order_type": self.order_type,
        }
        if self.order_type != "market":
            if self.price is None:
                raise ValueError("limit and ioc orders require a price")
            payload["price"] = int(self.price)
            payload["expiry"] = now_ms() + max(1, int(self.expiry_ms))
        return payload


@dataclass(frozen=True)
class Opportunity:
    strategy: str
    opportunity_id: str
    priority: int
    reason: str
    intents: tuple[OrderIntent, ...]


@dataclass(frozen=True)
class RiskDecision:
    ok: bool
    reason: str = "ok"


@dataclass(frozen=True)
class RiskConfig:
    max_order_qty: int = 8
    max_symbol_abs_position: int = 80
    min_cash_cents: int = -2_000_000
    max_pending_orders_soft: int = 1_000
    max_long_position: int = HARD_LONG_CEILING
    max_short_position: int = HARD_SHORT_FLOOR
    max_orders_per_eval: int = 16


class RiskManager:
    def __init__(self, config: RiskConfig):
        self.config = config

    def approve(self, state: ExchangeState, intent: OrderIntent) -> RiskDecision:
        if state.end_seen:
            return RiskDecision(False, "round ended")
        if intent.exchange.upper() != state.exchange.upper():
            return RiskDecision(False, "intent exchange mismatch")
        if exchange_from_instrument(intent.instrument_id) != state.exchange.upper():
            return RiskDecision(False, "instrument exchange mismatch")
        if intent.side not in {"bid", "ask"}:
            return RiskDecision(False, "bad side")
        if intent.order_type not in {"ioc", "limit", "market"}:
            return RiskDecision(False, "bad order type")
        if intent.quantity <= 0:
            return RiskDecision(False, "non-positive quantity")
        if intent.quantity > self.config.max_order_qty:
            return RiskDecision(False, "order quantity cap")
        if intent.order_type != "market":
            if intent.price is None or intent.price <= 0 or intent.price >= 1_000_000:
                return RiskDecision(False, "bad price")
        if state.pending_orders_count >= self.config.max_pending_orders_soft and intent.order_type == "limit":
            return RiskDecision(False, "pending order budget")

        current_pos = state.position(intent.instrument_id)
        delta = intent.quantity if intent.side == "bid" else -intent.quantity
        projected_pos = current_pos + delta
        if projected_pos < self.config.max_short_position:
            return RiskDecision(False, "short floor")
        if projected_pos > self.config.max_long_position:
            return RiskDecision(False, "long ceiling")

        reducing = abs(projected_pos) < abs(current_pos)
        soft_cap = self.config.max_symbol_abs_position
        if not intent.reduce_only and not reducing and abs(projected_pos) > soft_cap:
            return RiskDecision(False, "soft position cap")

        worst_price = intent.price
        if intent.order_type == "market" and worst_price is None:
            worst_price = 120_000
        if intent.side == "bid":
            projected_cash = state.cash_total - int(worst_price or 0) * intent.quantity
            if projected_cash < HARD_CASH_FLOOR_CENTS:
                return RiskDecision(False, "hard cash floor")
            if projected_cash < self.config.min_cash_cents and not intent.reduce_only:
                return RiskDecision(False, "soft cash floor")
        return RiskDecision(True)

    def approve_bundle(self, states: dict[str, ExchangeState], opportunity: Opportunity) -> RiskDecision:
        shadows = {exchange: state.clone_for_risk() for exchange, state in states.items()}
        for intent in opportunity.intents:
            state = shadows.get(intent.exchange)
            if state is None:
                return RiskDecision(False, f"no state for {intent.exchange}")
            decision = self.approve(state, intent)
            if not decision.ok:
                return RiskDecision(False, f"{intent.instrument_id}: {decision.reason}")
            self._apply_worst_case(state, intent)
        return RiskDecision(True)

    @staticmethod
    def _apply_worst_case(state: ExchangeState, intent: OrderIntent) -> None:
        delta = intent.quantity if intent.side == "bid" else -intent.quantity
        state.positions[intent.instrument_id] = state.position(intent.instrument_id) + delta
        if intent.price is not None:
            notional = int(intent.price) * int(intent.quantity)
            state.cash_total += -notional if intent.side == "bid" else notional
        if intent.order_type == "limit":
            state.pending_orders_count += 1


class LatencyModel:
    def __init__(self, matrix: Optional[dict[str, dict[str, int]]] = None):
        self.matrix = matrix or LATENCY_RTT_MS

    def rtt(self, location: str, exchange: str) -> int:
        location = location.upper()
        exchange = exchange.upper()
        return self.matrix.get(location, {}).get(exchange, 999)

    def rank(self, location: str, exchanges: Iterable[str]) -> list[str]:
        return sorted((exchange.upper() for exchange in exchanges), key=lambda ex: (self.rtt(location, ex), ex))


@dataclass(frozen=True)
class Settings:
    venues: tuple[str, ...]
    location: str
    live_trading: bool
    no_connect: bool
    print_config: bool
    log_level: str
    host_overrides: dict[str, str]
    max_msgs_per_sec: int
    eval_interval_ms: int
    max_book_age_ms: int
    ioc_expiry_ms: int
    limit_expiry_ms: int
    inventory_poll_ms: int
    pending_poll_ms: int
    orders_per_eval: int
    arb_unit_size: int
    cross_unit_size: int
    anomaly_unit_size: int
    safe_unit_size: int
    passive_unit_size: int
    arb_base_edge_cents: int
    cross_edge_cents: int
    anomaly_edge_cents: int
    safe_edge_cents: int
    edge_safety_cents: int
    latency_cents_per_100ms: int
    flatten_window_ms: int
    panic_flatten_ms: int
    flatten_interval_ms: int
    flatten_slippage_cents: int
    enable_etf_arb: bool
    enable_cross_venue: bool
    enable_card_simp: bool
    enable_safe_haven: bool
    enable_passive_micro: bool
    micro_imbalance_bps: int
    leader_count: int
    risk: RiskConfig

    @classmethod
    def from_env(cls, args: argparse.Namespace) -> "Settings":
        venues_raw = args.venues or env_str(
            "GOD_VENUES",
            "ZSE,NYSE,NASDAQ,EURONEXT,LSE,HKEX,TMX",
        )
        venues = tuple(exchange for exchange in parse_csv(venues_raw) if exchange in EXCHANGES)
        if not venues:
            raise ValueError("GOD_VENUES selected no valid exchanges")
        live = bool(args.live) or env_bool("LIVE_TRADING", False)
        if args.dry_run:
            live = False
        max_order_qty = env_int("GOD_MAX_ORDER_QTY", 8)
        risk = RiskConfig(
            max_order_qty=max_order_qty,
            max_symbol_abs_position=env_int("GOD_MAX_SYMBOL_ABS_POS", 80),
            min_cash_cents=env_int("GOD_MIN_CASH_CENTS", -2_000_000),
            max_pending_orders_soft=env_int("GOD_MAX_PENDING_ORDERS_SOFT", 1_000),
            max_orders_per_eval=env_int("GOD_ORDERS_PER_EVAL", 16),
        )
        return cls(
            venues=venues,
            location=(args.location or env_str("GOD_LOCATION", "ZSE")).upper(),
            live_trading=live,
            no_connect=bool(args.no_connect) or env_bool("GOD_NO_CONNECT", False),
            print_config=bool(args.print_config),
            log_level=env_str("LOG_LEVEL", env_str("GOD_LOG_LEVEL", "INFO")).upper(),
            host_overrides=parse_host_overrides(env_str("GOD_HOST_OVERRIDES", "")),
            max_msgs_per_sec=env_int("GOD_MAX_MSGS_PER_SEC", 120),
            eval_interval_ms=env_int("GOD_EVAL_INTERVAL_MS", 80),
            max_book_age_ms=env_int("GOD_MAX_BOOK_AGE_MS", 650),
            ioc_expiry_ms=env_int("GOD_IOC_EXPIRY_MS", 1_000),
            limit_expiry_ms=env_int("GOD_LIMIT_EXPIRY_MS", 900),
            inventory_poll_ms=env_int("GOD_INVENTORY_POLL_MS", 2_000),
            pending_poll_ms=env_int("GOD_PENDING_POLL_MS", 10_000),
            orders_per_eval=env_int("GOD_ORDERS_PER_EVAL", 16),
            arb_unit_size=env_int("GOD_ARB_UNIT_SIZE", 1),
            cross_unit_size=env_int("GOD_CROSS_UNIT_SIZE", 3),
            anomaly_unit_size=env_int("GOD_ANOMALY_UNIT_SIZE", 3),
            safe_unit_size=env_int("GOD_SAFE_UNIT_SIZE", 2),
            passive_unit_size=env_int("GOD_PASSIVE_UNIT_SIZE", 2),
            arb_base_edge_cents=env_int("GOD_ARB_BASE_EDGE_CENTS", 3),
            cross_edge_cents=env_int("GOD_CROSS_EDGE_CENTS", 4),
            anomaly_edge_cents=env_int("GOD_ANOMALY_EDGE_CENTS", 5),
            safe_edge_cents=env_int("GOD_SAFE_EDGE_CENTS", 4),
            edge_safety_cents=env_int("GOD_EDGE_SAFETY_CENTS", 1),
            latency_cents_per_100ms=env_int("GOD_LATENCY_CENTS_PER_100MS", 1),
            flatten_window_ms=env_int("GOD_FLATTEN_WINDOW_MS", 75_000),
            panic_flatten_ms=env_int("GOD_PANIC_FLATTEN_MS", 20_000),
            flatten_interval_ms=env_int("GOD_FLATTEN_INTERVAL_MS", 1_000),
            flatten_slippage_cents=env_int("GOD_FLATTEN_SLIPPAGE_CENTS", 25),
            enable_etf_arb=env_bool("GOD_ENABLE_ETF_ARB", True),
            enable_cross_venue=env_bool("GOD_ENABLE_CROSS_VENUE", True),
            enable_card_simp=env_bool("GOD_ENABLE_CARD_SIMP", True),
            enable_safe_haven=env_bool("GOD_ENABLE_SAFE_HAVEN", True),
            enable_passive_micro=env_bool("GOD_ENABLE_PASSIVE_MICRO", False),
            micro_imbalance_bps=env_int("GOD_MICRO_IMBALANCE_BPS", 6_000),
            leader_count=env_int("GOD_LEADER_COUNT", 2),
            risk=risk,
        )

    def to_log_dict(self) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        data["risk"] = dataclasses.asdict(self.risk)
        return data


class JsonLogger:
    def __init__(self, name: str, level: str):
        logging.basicConfig(level=getattr(logging, level, logging.INFO), format="%(message)s")
        self.logger = logging.getLogger(name)

    def event(self, level: int, event: str, **fields: Any) -> None:
        payload = {"ts": now_ms(), "event": event, **fields}
        self.logger.log(level, json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str))

    def info(self, event: str, **fields: Any) -> None:
        self.event(logging.INFO, event, **fields)

    def debug(self, event: str, **fields: Any) -> None:
        self.event(logging.DEBUG, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self.event(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self.event(logging.ERROR, event, **fields)


class RateLimiter:
    def __init__(self, max_per_second: int):
        self.max_per_second = max(1, int(max_per_second))
        self.sent_at: Deque[int] = deque()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self.lock:
            while True:
                current = now_ms()
                while self.sent_at and current - self.sent_at[0] >= 1_000:
                    self.sent_at.popleft()
                if len(self.sent_at) < self.max_per_second:
                    self.sent_at.append(current)
                    return
                wait_ms = max(1, 1_000 - (current - self.sent_at[0]))
                await asyncio.sleep(wait_ms / 1_000)


@dataclass(frozen=True)
class ExecutionQuote:
    exchange: str
    instrument_id: str
    side: str
    price: int
    available_qty: int
    latency_ms: int
    book: OrderBook


class GlobalMarketState:
    def __init__(self, venues: Iterable[str], latency: LatencyModel):
        self.states = {exchange: ExchangeState(exchange=exchange) for exchange in venues}
        self.latency = latency

    def state(self, exchange: str) -> ExchangeState:
        return self.states[exchange.upper()]

    def active_exchanges(self) -> list[str]:
        return list(self.states)

    def ticker_venues(self, ticker: str) -> list[str]:
        listed = TICKER_LISTINGS.get(ticker.upper(), [])
        return [exchange for exchange in listed if exchange in self.states]

    def book(self, exchange: str, ticker: str) -> Optional[OrderBook]:
        state = self.states.get(exchange.upper())
        if not state:
            return None
        return state.orderbooks.get(instrument_id(exchange, ticker))

    def fresh_book(self, exchange: str, ticker: str, max_age_ms: int) -> Optional[OrderBook]:
        book = self.book(exchange, ticker)
        if not book:
            return None
        if now_ms() - book.last_update_ms > max_age_ms:
            return None
        if book.best_bid is None or book.best_ask is None:
            return None
        if book.best_bid >= book.best_ask:
            return None
        return book

    def volatility_cents(self, exchange: str, ticker: str) -> int:
        inst = instrument_id(exchange, ticker)
        state = self.states[exchange]
        return state.volatility_cents(inst)

    def best_execution_quote(
        self,
        ticker: str,
        side: str,
        quantity: int,
        candidate_venues: Iterable[str],
        settings: Settings,
    ) -> Optional[ExecutionQuote]:
        quotes: list[ExecutionQuote] = []
        for exchange in candidate_venues:
            exchange = exchange.upper()
            if exchange not in self.states:
                continue
            book = self.fresh_book(exchange, ticker, settings.max_book_age_ms)
            if not book:
                continue
            price = book.best_ask if side == "bid" else book.best_bid
            if price is None:
                continue
            available = book.available_to_trade(side, price)
            if available <= 0:
                continue
            quotes.append(
                ExecutionQuote(
                    exchange=exchange,
                    instrument_id=instrument_id(exchange, ticker),
                    side=side,
                    price=price,
                    available_qty=available,
                    latency_ms=self.latency.rtt(settings.location, exchange),
                    book=book,
                )
            )
        if not quotes:
            return None
        if side == "bid":
            quotes.sort(key=lambda quote: (quote.price, quote.latency_ms, -quote.available_qty))
        else:
            quotes.sort(key=lambda quote: (-quote.price, quote.latency_ms, -quote.available_qty))
        best = quotes[0]
        if best.available_qty < quantity:
            return best
        return best

    def ticker_microprices(self, ticker: str, settings: Settings) -> list[tuple[str, int, OrderBook]]:
        values: list[tuple[str, int, OrderBook]] = []
        for exchange in self.ticker_venues(ticker):
            book = self.fresh_book(exchange, ticker, settings.max_book_age_ms)
            if not book:
                continue
            micro = book.microprice()
            if micro is not None:
                values.append((exchange, micro, book))
        return values

    def ticker_momentum(self, ticker: str) -> int:
        moves = []
        for exchange in self.ticker_venues(ticker):
            inst = instrument_id(exchange, ticker)
            state = self.states[exchange]
            current = state.current_mid.get(inst)
            previous = state.previous_mid.get(inst)
            if current is not None and previous is not None:
                moves.append(current - previous)
        return median_int(moves)

    def min_time_left_ms(self) -> int:
        active = [state.time_left_ms() for state in self.states.values() if state.connected and not state.end_seen]
        if not active:
            return ROUND_LENGTH_MS
        return min(active)

    def estimated_total_nav_cents(self) -> int:
        return sum(state.estimated_nav_cents() for state in self.states.values())


class Strategy:
    name = "base"

    def opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        raise NotImplementedError


class ETFBasketArbStrategy(Strategy):
    name = "etf_basket_arb"

    def opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        if not settings.enable_etf_arb:
            return []
        out: list[Opportunity] = []
        for etf, basket in ETF_BASKETS.items():
            n = len(basket)
            for etf_exchange in market.ticker_venues(etf):
                etf_book = market.fresh_book(etf_exchange, etf, settings.max_book_age_ms)
                if not etf_book or etf_book.best_bid is None or etf_book.best_ask is None:
                    continue
                cheap = self._cheap_etf_opportunity(market, settings, etf, basket, n, etf_exchange, etf_book)
                rich = self._rich_etf_opportunity(market, settings, etf, basket, n, etf_exchange, etf_book)
                if cheap:
                    out.append(cheap)
                if rich:
                    out.append(rich)
        return out

    def _threshold_num(
        self,
        market: GlobalMarketState,
        settings: Settings,
        etf_exchange: str,
        etf: str,
        n: int,
        quotes: list[ExecutionQuote],
        etf_book: OrderBook,
    ) -> int:
        spreads = [quote.book.spread or 0 for quote in quotes]
        spreads.append(etf_book.spread or 0)
        spread_guard = max(0, median_int(spreads) // 2)
        vol = market.volatility_cents(etf_exchange, etf)
        for quote in quotes:
            vol = max(vol, market.volatility_cents(quote.exchange, ticker_from_instrument(quote.instrument_id)))
        latency_guard = max((quote.latency_ms * settings.latency_cents_per_100ms) // 100 for quote in quotes)
        per_share = max(settings.arb_base_edge_cents, spread_guard + vol + latency_guard + settings.edge_safety_cents)
        return per_share * n

    def _cheap_etf_opportunity(
        self,
        market: GlobalMarketState,
        settings: Settings,
        etf: str,
        basket: list[str],
        n: int,
        etf_exchange: str,
        etf_book: OrderBook,
    ) -> Optional[Opportunity]:
        component_quotes: list[ExecutionQuote] = []
        for ticker in basket:
            quote = market.best_execution_quote(
                ticker,
                "ask",
                settings.arb_unit_size,
                market.ticker_venues(ticker),
                settings,
            )
            if not quote:
                return None
            component_quotes.append(quote)
        etf_ask = etf_book.best_ask
        if etf_ask is None:
            return None
        edge_num = sum(quote.price for quote in component_quotes) - n * etf_ask
        threshold = self._threshold_num(market, settings, etf_exchange, etf, n, component_quotes, etf_book)
        if edge_num < threshold:
            return None
        unit = min(settings.arb_unit_size, etf_book.available_to_trade("bid", etf_ask) // n)
        for quote in component_quotes:
            unit = min(unit, quote.available_qty)
        if unit <= 0:
            return None
        opp_id = f"arb-{etf}-{etf_exchange}-cheap-{uuid.uuid4().hex[:8]}"
        intents = [
            OrderIntent(
                exchange=etf_exchange,
                instrument_id=instrument_id(etf_exchange, etf),
                side="bid",
                quantity=n * unit,
                price=etf_ask,
                order_type="ioc",
                strategy=self.name,
                reason=f"buy cheap {etf}; edge_num={edge_num}; threshold={threshold}",
                priority=edge_num,
                opportunity_id=opp_id,
                expiry_ms=settings.ioc_expiry_ms,
            )
        ]
        intents.extend(
            OrderIntent(
                exchange=quote.exchange,
                instrument_id=quote.instrument_id,
                side="ask",
                quantity=unit,
                price=quote.price,
                order_type="ioc",
                strategy=self.name,
                reason=f"hedge cheap {etf}; edge_num={edge_num}; threshold={threshold}",
                priority=edge_num,
                opportunity_id=opp_id,
                expiry_ms=settings.ioc_expiry_ms,
            )
            for quote in component_quotes
        )
        return Opportunity(self.name, opp_id, edge_num, "ETF below executable basket", tuple(intents))

    def _rich_etf_opportunity(
        self,
        market: GlobalMarketState,
        settings: Settings,
        etf: str,
        basket: list[str],
        n: int,
        etf_exchange: str,
        etf_book: OrderBook,
    ) -> Optional[Opportunity]:
        component_quotes: list[ExecutionQuote] = []
        for ticker in basket:
            quote = market.best_execution_quote(
                ticker,
                "bid",
                settings.arb_unit_size,
                market.ticker_venues(ticker),
                settings,
            )
            if not quote:
                return None
            component_quotes.append(quote)
        etf_bid = etf_book.best_bid
        if etf_bid is None:
            return None
        edge_num = n * etf_bid - sum(quote.price for quote in component_quotes)
        threshold = self._threshold_num(market, settings, etf_exchange, etf, n, component_quotes, etf_book)
        if edge_num < threshold:
            return None
        unit = min(settings.arb_unit_size, etf_book.available_to_trade("ask", etf_bid) // n)
        for quote in component_quotes:
            unit = min(unit, quote.available_qty)
        if unit <= 0:
            return None
        opp_id = f"arb-{etf}-{etf_exchange}-rich-{uuid.uuid4().hex[:8]}"
        intents = [
            OrderIntent(
                exchange=etf_exchange,
                instrument_id=instrument_id(etf_exchange, etf),
                side="ask",
                quantity=n * unit,
                price=etf_bid,
                order_type="ioc",
                strategy=self.name,
                reason=f"sell rich {etf}; edge_num={edge_num}; threshold={threshold}",
                priority=edge_num,
                opportunity_id=opp_id,
                expiry_ms=settings.ioc_expiry_ms,
            )
        ]
        intents.extend(
            OrderIntent(
                exchange=quote.exchange,
                instrument_id=quote.instrument_id,
                side="bid",
                quantity=unit,
                price=quote.price,
                order_type="ioc",
                strategy=self.name,
                reason=f"hedge rich {etf}; edge_num={edge_num}; threshold={threshold}",
                priority=edge_num,
                opportunity_id=opp_id,
                expiry_ms=settings.ioc_expiry_ms,
            )
            for quote in component_quotes
        )
        return Opportunity(self.name, opp_id, edge_num, "ETF above executable basket", tuple(intents))


class CrossVenueLeaderStrategy(Strategy):
    name = "cross_venue_leader"

    def opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        if not settings.enable_cross_venue:
            return []
        out: list[Opportunity] = []
        tradable = [ticker for ticker in ALL_TICKERS if len(market.ticker_venues(ticker)) >= 2]
        for ticker in tradable:
            values = market.ticker_microprices(ticker, settings)
            if len(values) < 2:
                continue
            ranked = market.latency.rank(settings.location, [exchange for exchange, _, _ in values])
            leader_venues = set(ranked[: max(1, settings.leader_count)])
            leader_prices = [price for exchange, price, _ in values if exchange in leader_venues]
            if not leader_prices:
                continue
            leader_micro = median_int(leader_prices)
            for exchange, _, book in values:
                if exchange in leader_venues and len(values) > settings.leader_count:
                    continue
                state = market.state(exchange)
                latency_guard = (market.latency.rtt(settings.location, exchange) * settings.latency_cents_per_100ms) // 100
                threshold = (
                    settings.cross_edge_cents
                    + (book.spread or 0) // 2
                    + state.volatility_cents(instrument_id(exchange, ticker))
                    + latency_guard
                    + settings.edge_safety_cents
                )
                if book.best_ask is not None and leader_micro - book.best_ask >= threshold:
                    qty = min(settings.cross_unit_size, book.available_to_trade("bid", book.best_ask))
                    if qty > 0:
                        edge = leader_micro - book.best_ask
                        opp_id = f"lead-{ticker}-{exchange}-buy-{uuid.uuid4().hex[:8]}"
                        out.append(
                            Opportunity(
                                self.name,
                                opp_id,
                                edge,
                                "leader venue above stale ask",
                                (
                                    OrderIntent(
                                        exchange=exchange,
                                        instrument_id=instrument_id(exchange, ticker),
                                        side="bid",
                                        quantity=qty,
                                        price=book.best_ask,
                                        order_type="ioc",
                                        strategy=self.name,
                                        reason=f"leader_micro={leader_micro}; edge={edge}; threshold={threshold}",
                                        priority=edge,
                                        opportunity_id=opp_id,
                                        expiry_ms=settings.ioc_expiry_ms,
                                    ),
                                ),
                            )
                        )
                if book.best_bid is not None and book.best_bid - leader_micro >= threshold:
                    qty = min(settings.cross_unit_size, book.available_to_trade("ask", book.best_bid))
                    if qty > 0:
                        edge = book.best_bid - leader_micro
                        opp_id = f"lead-{ticker}-{exchange}-sell-{uuid.uuid4().hex[:8]}"
                        out.append(
                            Opportunity(
                                self.name,
                                opp_id,
                                edge,
                                "leader venue below stale bid",
                                (
                                    OrderIntent(
                                        exchange=exchange,
                                        instrument_id=instrument_id(exchange, ticker),
                                        side="ask",
                                        quantity=qty,
                                        price=book.best_bid,
                                        order_type="ioc",
                                        strategy=self.name,
                                        reason=f"leader_micro={leader_micro}; edge={edge}; threshold={threshold}",
                                        priority=edge,
                                        opportunity_id=opp_id,
                                        expiry_ms=settings.ioc_expiry_ms,
                                    ),
                                ),
                            )
                        )
        return out


class CardSimpAnomalyStrategy(Strategy):
    name = "card_simp_anomaly"

    def opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        if not settings.enable_card_simp:
            return []
        out: list[Opportunity] = []
        for ticker in ("CARD", "SIMP"):
            mids = []
            books: list[tuple[str, OrderBook]] = []
            for exchange in market.ticker_venues(ticker):
                book = market.fresh_book(exchange, ticker, settings.max_book_age_ms)
                if book and book.mid is not None:
                    mids.append(book.mid)
                    books.append((exchange, book))
            if len(mids) < 4:
                continue
            fair = median_int(mids)
            threshold = max(settings.anomaly_edge_cents, 2 * mad_int(mids, fair) + settings.edge_safety_cents)
            for exchange, book in books:
                if book.best_ask is not None and fair - book.best_ask >= threshold:
                    qty = min(settings.anomaly_unit_size, book.available_to_trade("bid", book.best_ask))
                    if qty > 0:
                        edge = fair - book.best_ask
                        opp_id = f"anom-{ticker}-{exchange}-buy-{uuid.uuid4().hex[:8]}"
                        out.append(
                            Opportunity(
                                self.name,
                                opp_id,
                                edge,
                                "cross-venue median above local ask",
                                (
                                    OrderIntent(
                                        exchange,
                                        instrument_id(exchange, ticker),
                                        "bid",
                                        qty,
                                        book.best_ask,
                                        "ioc",
                                        self.name,
                                        f"median={fair}; edge={edge}; threshold={threshold}",
                                        edge,
                                        False,
                                        opp_id,
                                        settings.ioc_expiry_ms,
                                    ),
                                ),
                            )
                        )
                if book.best_bid is not None and book.best_bid - fair >= threshold:
                    qty = min(settings.anomaly_unit_size, book.available_to_trade("ask", book.best_bid))
                    if qty > 0:
                        edge = book.best_bid - fair
                        opp_id = f"anom-{ticker}-{exchange}-sell-{uuid.uuid4().hex[:8]}"
                        out.append(
                            Opportunity(
                                self.name,
                                opp_id,
                                edge,
                                "cross-venue median below local bid",
                                (
                                    OrderIntent(
                                        exchange,
                                        instrument_id(exchange, ticker),
                                        "ask",
                                        qty,
                                        book.best_bid,
                                        "ioc",
                                        self.name,
                                        f"median={fair}; edge={edge}; threshold={threshold}",
                                        edge,
                                        False,
                                        opp_id,
                                        settings.ioc_expiry_ms,
                                    ),
                                ),
                            )
                        )
        out.extend(self._pair_spread_opportunities(market, settings))
        return out

    def _pair_spread_opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        spreads: list[int] = []
        pairs: list[tuple[str, OrderBook, OrderBook, int]] = []
        for exchange in market.active_exchanges():
            card = market.fresh_book(exchange, "CARD", settings.max_book_age_ms)
            simp = market.fresh_book(exchange, "SIMP", settings.max_book_age_ms)
            if card and simp and card.mid is not None and simp.mid is not None:
                spread = card.mid - simp.mid
                spreads.append(spread)
                pairs.append((exchange, card, simp, spread))
        if len(spreads) < 4:
            return []
        center = median_int(spreads)
        threshold = max(settings.anomaly_edge_cents, 2 * mad_int(spreads, center) + settings.edge_safety_cents)
        out: list[Opportunity] = []
        for exchange, card, simp, spread in pairs:
            if spread - center >= threshold and card.best_bid is not None and simp.best_ask is not None:
                qty = min(
                    settings.anomaly_unit_size,
                    card.available_to_trade("ask", card.best_bid),
                    simp.available_to_trade("bid", simp.best_ask),
                )
                if qty > 0:
                    edge = spread - center
                    opp_id = f"pair-card-simp-{exchange}-wide-{uuid.uuid4().hex[:8]}"
                    out.append(
                        Opportunity(
                            self.name,
                            opp_id,
                            edge,
                            "CARD/SIMP pair spread too wide",
                            (
                                OrderIntent(exchange, instrument_id(exchange, "CARD"), "ask", qty, card.best_bid, "ioc", self.name, "pair spread wide: sell CARD", edge, False, opp_id, settings.ioc_expiry_ms),
                                OrderIntent(exchange, instrument_id(exchange, "SIMP"), "bid", qty, simp.best_ask, "ioc", self.name, "pair spread wide: buy SIMP", edge, False, opp_id, settings.ioc_expiry_ms),
                            ),
                        )
                    )
            if center - spread >= threshold and card.best_ask is not None and simp.best_bid is not None:
                qty = min(
                    settings.anomaly_unit_size,
                    card.available_to_trade("bid", card.best_ask),
                    simp.available_to_trade("ask", simp.best_bid),
                )
                if qty > 0:
                    edge = center - spread
                    opp_id = f"pair-card-simp-{exchange}-tight-{uuid.uuid4().hex[:8]}"
                    out.append(
                        Opportunity(
                            self.name,
                            opp_id,
                            edge,
                            "CARD/SIMP pair spread too tight",
                            (
                                OrderIntent(exchange, instrument_id(exchange, "CARD"), "bid", qty, card.best_ask, "ioc", self.name, "pair spread tight: buy CARD", edge, False, opp_id, settings.ioc_expiry_ms),
                                OrderIntent(exchange, instrument_id(exchange, "SIMP"), "ask", qty, simp.best_bid, "ioc", self.name, "pair spread tight: sell SIMP", edge, False, opp_id, settings.ioc_expiry_ms),
                            ),
                        )
                    )
        return out


class SafeHavenStrategy(Strategy):
    name = "safe_haven_rotation"

    def opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        if not settings.enable_safe_haven:
            return []
        risky_moves = [market.ticker_momentum(ticker) for ticker in SECTOR_A + SECTOR_B + ["MDKA", "KRAS", "ZITO", "ZABA"]]
        risky_moves = [move for move in risky_moves if move != 0]
        if len(risky_moves) < 4:
            return []
        market_move = median_int(risky_moves)
        safe_moves = [market.ticker_momentum(ticker) for ticker in ["GOLD", "XAG", "ETFSH"]]
        safe_move = median_int([move for move in safe_moves if move != 0])
        out: list[Opportunity] = []
        if market_move <= -settings.safe_edge_cents and safe_move < abs(market_move) // 2:
            out.extend(self._safe_direction(market, settings, "bid", abs(market_move) - safe_move, "risk-off safe haven lag"))
        elif market_move >= settings.safe_edge_cents and safe_move > -abs(market_move) // 2:
            out.extend(self._safe_direction(market, settings, "ask", market_move + safe_move, "risk-on safe haven fade"))
        return out

    def _safe_direction(
        self,
        market: GlobalMarketState,
        settings: Settings,
        side: str,
        edge: int,
        reason: str,
    ) -> list[Opportunity]:
        if edge < settings.safe_edge_cents:
            return []
        out: list[Opportunity] = []
        for ticker in ("GOLD", "XAG", "ETFSH"):
            quote = market.best_execution_quote(ticker, side, settings.safe_unit_size, market.ticker_venues(ticker), settings)
            if not quote:
                continue
            qty = min(settings.safe_unit_size, quote.available_qty)
            if qty <= 0:
                continue
            opp_id = f"safe-{ticker}-{quote.exchange}-{side}-{uuid.uuid4().hex[:8]}"
            out.append(
                Opportunity(
                    self.name,
                    opp_id,
                    edge,
                    reason,
                    (
                        OrderIntent(
                            quote.exchange,
                            quote.instrument_id,
                            side,
                            qty,
                            quote.price,
                            "ioc",
                            self.name,
                            f"{reason}; signal_edge={edge}",
                            edge,
                            False,
                            opp_id,
                            settings.ioc_expiry_ms,
                        ),
                    ),
                )
            )
        return out


class PassiveMicropriceStrategy(Strategy):
    name = "passive_microprice"

    def opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        if not settings.enable_passive_micro:
            return []
        out: list[Opportunity] = []
        for exchange in market.active_exchanges():
            state = market.state(exchange)
            if state.time_left_ms() <= settings.flatten_window_ms:
                continue
            for inst, book in list(state.orderbooks.items()):
                ticker = ticker_from_instrument(inst)
                if ticker not in ALL_TICKERS:
                    continue
                if now_ms() - book.last_update_ms > settings.max_book_age_ms:
                    continue
                if book.best_bid is None or book.best_ask is None:
                    continue
                spread = book.spread or 0
                if spread < 2 or spread > 12:
                    continue
                imbalance = book.imbalance_bps()
                if imbalance >= settings.micro_imbalance_bps:
                    price = book.best_bid
                    side = "bid"
                    edge = imbalance // 1_000
                elif imbalance <= -settings.micro_imbalance_bps:
                    price = book.best_ask
                    side = "ask"
                    edge = abs(imbalance) // 1_000
                else:
                    continue
                qty = settings.passive_unit_size
                opp_id = f"micro-{inst}-{side}-{uuid.uuid4().hex[:8]}"
                out.append(
                    Opportunity(
                        self.name,
                        opp_id,
                        edge,
                        "short-expiry passive microprice imbalance",
                        (
                            OrderIntent(
                                exchange,
                                inst,
                                side,
                                qty,
                                price,
                                "limit",
                                self.name,
                                f"imbalance_bps={imbalance}; spread={spread}",
                                edge,
                                False,
                                opp_id,
                                settings.limit_expiry_ms,
                            ),
                        ),
                    )
                )
        return out


class FlattenStrategy(Strategy):
    name = "flatten"

    def __init__(self) -> None:
        self.last_run_ms: dict[str, int] = defaultdict(int)

    def opportunities(self, market: GlobalMarketState, settings: Settings) -> list[Opportunity]:
        out: list[Opportunity] = []
        current = now_ms()
        for exchange in market.active_exchanges():
            state = market.state(exchange)
            time_left = state.time_left_ms()
            if time_left > settings.flatten_window_ms:
                continue
            if current - self.last_run_ms[exchange] < settings.flatten_interval_ms:
                continue
            self.last_run_ms[exchange] = current
            panic = time_left <= settings.panic_flatten_ms
            for inst, pos in list(state.positions.items()):
                if pos == 0:
                    continue
                book = state.orderbooks.get(inst)
                if not book or book.best_bid is None or book.best_ask is None:
                    continue
                side = "ask" if pos > 0 else "bid"
                qty = min(abs(pos), settings.risk.max_order_qty)
                if qty <= 0:
                    continue
                if panic:
                    if side == "ask":
                        price = max(1, book.best_bid - settings.flatten_slippage_cents)
                    else:
                        price = book.best_ask + settings.flatten_slippage_cents
                    order_type = "market"
                elif side == "ask":
                    price = max(1, book.best_bid - settings.flatten_slippage_cents)
                    order_type = "ioc"
                else:
                    price = book.best_ask + settings.flatten_slippage_cents
                    order_type = "ioc"
                opp_id = f"flat-{inst}-{side}-{uuid.uuid4().hex[:8]}"
                out.append(
                    Opportunity(
                        self.name,
                        opp_id,
                        1_000_000 + abs(pos),
                        "end-of-segment inventory flattening",
                        (
                            OrderIntent(
                                exchange,
                                inst,
                                side,
                                qty,
                                price,
                                order_type,
                                self.name,
                                f"time_left_ms={time_left}; position={pos}; panic={panic}",
                                1_000_000 + abs(pos),
                                True,
                                opp_id,
                                settings.ioc_expiry_ms,
                            ),
                        ),
                    )
                )
        return out


class StrategyEngine:
    def __init__(self, market: GlobalMarketState, risk: RiskManager, settings: Settings, logger: JsonLogger):
        self.market = market
        self.risk = risk
        self.settings = settings
        self.logger = logger
        self.clients: dict[str, ExchangeClient] = {}
        self.lock = asyncio.Lock()
        self.last_eval_ms = 0
        self.strategies: list[Strategy] = [
            FlattenStrategy(),
            ETFBasketArbStrategy(),
            CrossVenueLeaderStrategy(),
            CardSimpAnomalyStrategy(),
            SafeHavenStrategy(),
            PassiveMicropriceStrategy(),
        ]

    def register_client(self, client: "ExchangeClient") -> None:
        self.clients[client.exchange] = client

    async def on_market_data(self, source_exchange: str) -> None:
        if self.lock.locked():
            return
        current = now_ms()
        if current - self.last_eval_ms < self.settings.eval_interval_ms:
            return
        async with self.lock:
            self.last_eval_ms = current
            flattening = self.market.min_time_left_ms() <= self.settings.flatten_window_ms
            opportunities: list[Opportunity] = []
            for strategy in self.strategies:
                if flattening and strategy.name != "flatten":
                    continue
                try:
                    opportunities.extend(strategy.opportunities(self.market, self.settings))
                except Exception as exc:  # pragma: no cover - live safety logging
                    self.logger.error("strategy_error", strategy=strategy.name, error=repr(exc))
            if not opportunities:
                return
            opportunities.sort(key=lambda opp: opp.priority, reverse=True)
            submitted_orders = 0
            for opportunity in opportunities:
                if submitted_orders + len(opportunity.intents) > self.settings.orders_per_eval:
                    continue
                decision = self.risk.approve_bundle(self.market.states, opportunity)
                if not decision.ok:
                    self.logger.debug(
                        "opportunity_rejected",
                        strategy=opportunity.strategy,
                        opportunity_id=opportunity.opportunity_id,
                        reason=decision.reason,
                    )
                    continue
                self.logger.info(
                    "opportunity",
                    strategy=opportunity.strategy,
                    opportunity_id=opportunity.opportunity_id,
                    priority=opportunity.priority,
                    reason=opportunity.reason,
                    legs=len(opportunity.intents),
                )
                for intent in opportunity.intents:
                    client = self.clients.get(intent.exchange)
                    if not client:
                        self.logger.warning("order_rejected_no_client", exchange=intent.exchange, instrument=intent.instrument_id)
                        continue
                    await client.submit_order(intent)
                    submitted_orders += 1


class ExchangeClient:
    def __init__(
        self,
        exchange: str,
        host: str,
        state: ExchangeState,
        risk: RiskManager,
        settings: Settings,
        engine: StrategyEngine,
        logger: JsonLogger,
    ):
        self.exchange = exchange
        self.host = host
        self.url = f"ws://{host}:9001/trade"
        self.state = state
        self.risk = risk
        self.settings = settings
        self.engine = engine
        self.logger = logger
        self.limiter = RateLimiter(settings.max_msgs_per_sec)
        self.websocket: Any = None
        self.request_to_intent: dict[str, OrderIntent] = {}
        self.stop_event = asyncio.Event()
        self.inventory_task: Optional[asyncio.Task[None]] = None
        self.pending_task: Optional[asyncio.Task[None]] = None

    async def connect_loop(self) -> None:
        if self.settings.no_connect:
            self.logger.info("client_no_connect", exchange=self.exchange, url=self.url)
            return
        ws_connect = self._load_ws_connect()
        backoff = 0.25
        while not self.stop_event.is_set():
            try:
                self.logger.info("connect_attempt", exchange=self.exchange, url=self.url)
                async with ws_connect(self.url, max_size=16 * 1024 * 1024, compression=None) as websocket:
                    self.websocket = websocket
                    self.state.connected = True
                    self.state.end_seen = False
                    backoff = 0.25
                    self.inventory_task = asyncio.create_task(self._poll_inventory())
                    self.pending_task = asyncio.create_task(self._poll_pending_orders())
                    await self.request_inventory()
                    await self.request_pending_orders()
                    async for raw in websocket:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        if raw == "Message rate limit exceeded":
                            self.logger.error("server_rate_limit_close", exchange=self.exchange)
                            break
                        await self._handle_message(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("connect_loop_error", exchange=self.exchange, error=repr(exc))
            finally:
                self.websocket = None
                self.state.connected = False
                await self._stop_pollers()
            if not self.stop_event.is_set():
                delay = min(8.0, backoff) + random.random() * 0.25
                self.logger.info("reconnect_backoff", exchange=self.exchange, delay_ms=int(delay * 1_000))
                await asyncio.sleep(delay)
                backoff = min(8.0, backoff * 1.8)

    @staticmethod
    def _load_ws_connect() -> Any:
        try:
            from websockets.asyncio.client import connect as ws_connect
        except ImportError:  # pragma: no cover - compatibility path
            from websockets import connect as ws_connect
        return ws_connect

    async def stop(self) -> None:
        self.stop_event.set()
        await self._stop_pollers()
        if self.websocket is not None:
            with contextlib.suppress(Exception):
                await self.websocket.close()

    async def _stop_pollers(self) -> None:
        for task in (self.inventory_task, self.pending_task):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self.inventory_task = None
        self.pending_task = None

    async def _poll_inventory(self) -> None:
        while True:
            await asyncio.sleep(self.settings.inventory_poll_ms / 1_000)
            await self.request_inventory()
            self.logger.info(
                "inventory_snapshot",
                exchange=self.exchange,
                cash=self.state.cash_total,
                nav=self.state.estimated_nav_cents(),
                positions={key: value for key, value in self.state.positions.items() if value},
            )

    async def _poll_pending_orders(self) -> None:
        while True:
            await asyncio.sleep(self.settings.pending_poll_ms / 1_000)
            await self.request_pending_orders()

    async def _handle_message(self, raw: str) -> None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.warning("bad_json", exchange=self.exchange, raw=raw[:200])
            return
        msg_type = message.get("type")
        if msg_type == "welcome":
            self.logger.info("connected", exchange=self.exchange, message=message.get("message"))
        elif msg_type == "market_data_update":
            self.state.apply_market_data(message)
            await self.engine.on_market_data(self.exchange)
        elif msg_type == "add_order_response":
            self._handle_add_order_response(message)
        elif msg_type == "cancel_order_response":
            self.logger.info("cancel_response", exchange=self.exchange, success=message.get("success"), message=message.get("message"))
        elif msg_type == "get_inventory_response":
            self.state.update_inventory(message.get("data") or {})
        elif msg_type == "get_pending_orders_response":
            self.state.update_pending_orders(message.get("data") or {})
        elif msg_type == "error":
            self.logger.warning("server_error", exchange=self.exchange, user_request_id=message.get("user_request_id"), message=message.get("message"))
        elif msg_type == "end_of_round":
            self.logger.info("end_of_round", exchange=self.exchange)
            self.state.end_seen = True
            self.state.reset_for_segment()
        else:
            self.logger.debug("unknown_message", exchange=self.exchange, type=msg_type)

    def _handle_add_order_response(self, message: dict[str, Any]) -> None:
        request_id = str(message.get("user_request_id", ""))
        intent = self.request_to_intent.pop(request_id, None)
        data = message.get("data") or {}
        if not message.get("success"):
            self.logger.warning(
                "order_reject",
                exchange=self.exchange,
                user_request_id=request_id,
                strategy=intent.strategy if intent else None,
                instrument=intent.instrument_id if intent else None,
                message=data.get("message"),
            )
            return
        inv_change = data.get("immediate_inventory_change")
        bal_change = data.get("immediate_balance_change")
        if intent and inv_change is not None and bal_change is not None:
            self.state.apply_immediate_fill(intent.instrument_id, int(inv_change), int(bal_change))
        if intent and intent.order_type == "limit" and inv_change is None:
            self.state.pending_orders_count += 1
        self.logger.info(
            "order_ack",
            exchange=self.exchange,
            user_request_id=request_id,
            order_id=data.get("order_id"),
            strategy=intent.strategy if intent else None,
            instrument=intent.instrument_id if intent else None,
            side=intent.side if intent else None,
            quantity=intent.quantity if intent else None,
            price=intent.price if intent else None,
            inv_change=inv_change,
            balance_change=bal_change,
        )

    async def request_inventory(self) -> None:
        await self._send({"type": "get_inventory", "user_request_id": f"inv-{self.exchange}-{uuid.uuid4().hex[:8]}"})

    async def request_pending_orders(self) -> None:
        await self._send({"type": "get_pending_orders", "user_request_id": f"pend-{self.exchange}-{uuid.uuid4().hex[:8]}"})

    async def submit_order(self, intent: OrderIntent) -> None:
        decision = self.risk.approve(self.state, intent)
        if not decision.ok:
            self.logger.debug(
                "order_risk_reject",
                exchange=self.exchange,
                instrument=intent.instrument_id,
                strategy=intent.strategy,
                reason=decision.reason,
            )
            return
        request_id = f"ord-{self.exchange}-{uuid.uuid4().hex[:10]}"
        payload = intent.to_message(request_id)
        if not self.settings.live_trading:
            self.logger.info(
                "dry_order",
                exchange=self.exchange,
                user_request_id=request_id,
                strategy=intent.strategy,
                opportunity_id=intent.opportunity_id,
                instrument=intent.instrument_id,
                side=intent.side,
                quantity=intent.quantity,
                price=intent.price,
                order_type=intent.order_type,
                reason=intent.reason,
            )
            return
        self.request_to_intent[request_id] = intent
        await self._send(payload)
        self.logger.info(
            "order_submit",
            exchange=self.exchange,
            user_request_id=request_id,
            strategy=intent.strategy,
            opportunity_id=intent.opportunity_id,
            instrument=intent.instrument_id,
            side=intent.side,
            quantity=intent.quantity,
            price=intent.price,
            order_type=intent.order_type,
            reason=intent.reason,
        )

    async def _send(self, payload: dict[str, Any]) -> None:
        if self.websocket is None:
            return
        await self.limiter.acquire()
        await self.websocket.send(json.dumps(payload, separators=(",", ":")))


class AlgoTradeBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.logger = JsonLogger("god_bot", settings.log_level)
        self.latency = LatencyModel()
        self.market = GlobalMarketState(settings.venues, self.latency)
        self.risk = RiskManager(settings.risk)
        self.engine = StrategyEngine(self.market, self.risk, settings, self.logger)
        self.clients: list[ExchangeClient] = []
        for exchange in settings.venues:
            host = settings.host_overrides.get(exchange, EXCHANGE_HOSTS[exchange])
            client = ExchangeClient(
                exchange=exchange,
                host=host,
                state=self.market.state(exchange),
                risk=self.risk,
                settings=settings,
                engine=self.engine,
                logger=self.logger,
            )
            self.engine.register_client(client)
            self.clients.append(client)

    async def run(self) -> None:
        self.logger.info("bot_start", config=self.settings.to_log_dict())
        if not self.settings.live_trading:
            self.logger.warning("dry_run_mode", message="LIVE_TRADING=1 is required before any order is sent")
        if self.settings.no_connect:
            self.logger.info("no_connect_exit")
            return
        tasks = [asyncio.create_task(client.connect_loop(), name=f"client-{client.exchange}") for client in self.clients]
        stop_event = asyncio.Event()
        self._install_signal_handlers(stop_event)
        stopper = asyncio.create_task(stop_event.wait(), name="signal-wait")
        try:
            await stopper
        finally:
            for client in self.clients:
                await client.stop()
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            self.logger.info("bot_stop", nav_cents=self.market.estimated_total_nav_cents())

    @staticmethod
    def _install_signal_handlers(stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop_event.set)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AlgoTrade 2026 multi-strategy trading bot")
    parser.add_argument("--live", action="store_true", help="send orders even if LIVE_TRADING is not set")
    parser.add_argument("--dry-run", action="store_true", help="force dry-run mode")
    parser.add_argument("--venues", help="comma-separated exchanges, e.g. ZSE,NYSE,NASDAQ")
    parser.add_argument("--location", choices=["NYSE", "ZSE", "HKEX"], help="current team location for latency routing")
    parser.add_argument("--print-config", action="store_true", help="print resolved config and exit")
    parser.add_argument("--no-connect", action="store_true", help="do not open exchange connections")
    return parser


async def async_main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    settings = Settings.from_env(args)
    if settings.print_config:
        print(json.dumps(settings.to_log_dict(), indent=2, sort_keys=True, default=str))
        return 0
    bot = AlgoTradeBot(settings)
    await bot.run()
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
