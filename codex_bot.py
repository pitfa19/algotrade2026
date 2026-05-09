#!/usr/bin/env python3
"""
AlgoTrade 2026 competition bot.

The bot is intentionally self-contained. It uses only the Python standard
library at import time, and imports websockets lazily when live network code
runs. That keeps the strategy and risk logic testable off the venue network.

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
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional


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

LATENCY_RTT_MS: dict[str, dict[str, int]] = {
    "NYSE": {
        "NYSE": 0,
        "NASDAQ": 1,
        "SSE": 165,
        "JPX": 152,
        "Euronext": 84,
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
        "Euronext": 84,
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
        "Euronext": 160,
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
        "Euronext": 145,
        "LSE": 141,
        "HKEX": 37,
        "NSE": 53,
        "TMX": 145,
        "ZSE": 140,
    },
    "Euronext": {
        "NYSE": 84,
        "NASDAQ": 84,
        "SSE": 160,
        "JPX": 145,
        "Euronext": 0,
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
        "Euronext": 6,
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
        "Euronext": 130,
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
        "Euronext": 130,
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
        "Euronext": 86,
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
        "Euronext": 22,
        "LSE": 24,
        "HKEX": 150,
        "NSE": 95,
        "TMX": 98,
        "ZSE": 0,
    },
}


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


@dataclass
class BotConfig:
    exchanges: list[str] = field(default_factory=lambda: ALL_EXCHANGES.copy())
    live_trading: bool = False
    home_location: str = "ZSE"
    min_edge_cents: int = 25
    etf_edge_cents: int = 18
    order_quantity: int = 10
    max_orders_per_tick: int = 4
    max_rate_per_second: int = 350
    ioc_expiry_ms: int = 2_000
    passive_enabled: bool = False
    passive_min_spread_cents: int = 10
    passive_edge_cents: int = 3
    passive_stop_after_ms: int = 560_000
    no_new_risk_after_ms: int = 590_000
    stale_after_seconds: float = 0.8
    latency_penalty_per_ms: float = 0.03
    max_long: int = 500
    max_short: int = -80
    min_cash_cents: int = -1_000_000
    max_open_orders_per_exchange: int = 500
    reconnect_delay_seconds: float = 2.0
    output_dir: Optional[str] = None

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            exchanges=parse_exchanges(os.environ.get("EXCHANGES", ",".join(ALL_EXCHANGES))),
            live_trading=parse_bool(os.environ.get("LIVE_TRADING"), default=False),
            home_location=parse_exchange(os.environ.get("HOME_LOCATION", "ZSE")) or "ZSE",
            min_edge_cents=parse_int(os.environ.get("MIN_EDGE_CENTS"), 25),
            etf_edge_cents=parse_int(os.environ.get("ETF_EDGE_CENTS"), 18),
            order_quantity=parse_int(os.environ.get("ORDER_QTY"), 10),
            max_orders_per_tick=parse_int(os.environ.get("MAX_ORDERS_PER_TICK"), 4),
            max_rate_per_second=parse_int(os.environ.get("MAX_MSGS_PER_SEC"), 350),
            passive_enabled=parse_bool(os.environ.get("PASSIVE_ENABLED"), default=False),
            output_dir=os.environ.get("BOT_OUTPUT_DIR"),
        )


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


def parse_exchanges(exchanges_str: str) -> list[str]:
    result: list[str] = []
    for part in exchanges_str.split(","):
        exchange = parse_exchange(part)
        if exchange and exchange not in result:
            result.append(exchange)
    return result


def ticker_from_instrument(instrument_id: str) -> str:
    if "-" not in instrument_id:
        return instrument_id
    return instrument_id.split("-", 1)[1]


def instrument_id(exchange: str, ticker: str) -> str:
    return f"{exchange}-{ticker}"


def parse_orderbook_depth(raw: dict[str, Any]) -> OrderBook:
    bids = tuple(
        sorted(
            (BookLevel(int(price), int(qty)) for price, qty in raw.get("bids", {}).items()),
            key=lambda level: level.price,
            reverse=True,
        )
    )
    asks = tuple(
        sorted(
            (BookLevel(int(price), int(qty)) for price, qty in raw.get("asks", {}).items()),
            key=lambda level: level.price,
        )
    )
    return OrderBook(bids=bids, asks=asks)


class MarketState:
    def __init__(self) -> None:
        self._books: dict[tuple[str, str], BookSnapshot] = {}
        self.exchange_times: dict[str, int] = {}
        self.trade_counts: dict[str, int] = {}
        self.cancel_counts: dict[str, int] = {}

    def apply_market_data(
        self,
        exchange: str,
        message: dict[str, Any],
        received_monotonic: Optional[float] = None,
    ) -> None:
        now = time.monotonic() if received_monotonic is None else received_monotonic
        exchange_time_ms = int(message.get("time", self.exchange_times.get(exchange, 0)) or 0)
        self.exchange_times[exchange] = exchange_time_ms

        for inst_id, raw_book in message.get("orderbook_depths", {}).items():
            self._books[(exchange, inst_id)] = BookSnapshot(
                exchange=exchange,
                instrument_id=inst_id,
                book=parse_orderbook_depth(raw_book),
                exchange_time_ms=exchange_time_ms,
                received_monotonic=now,
            )

        for event in message.get("events", []):
            event_type = event.get("event_type")
            data = event.get("data", {})
            inst_id = data.get("instrumentID")
            if not inst_id:
                continue
            if event_type == "trade":
                self.trade_counts[inst_id] = self.trade_counts.get(inst_id, 0) + 1
            elif event_type == "cancel":
                self.cancel_counts[inst_id] = self.cancel_counts.get(inst_id, 0) + 1

    def reset_exchange(self, exchange: str) -> None:
        for key in [key for key in self._books if key[0] == exchange]:
            del self._books[key]
        self.exchange_times.pop(exchange, None)

    def book(self, exchange: str, inst_id: str) -> Optional[OrderBook]:
        snapshot = self._books.get((exchange, inst_id))
        return snapshot.book if snapshot else None

    def snapshot(self, exchange: str, inst_id: str) -> Optional[BookSnapshot]:
        return self._books.get((exchange, inst_id))

    def books_on_exchange(self, exchange: str) -> list[BookSnapshot]:
        return [snapshot for (ex, _), snapshot in self._books.items() if ex == exchange]

    def ticker_mids(
        self,
        ticker: str,
        max_age_seconds: Optional[float] = None,
        now_monotonic: Optional[float] = None,
        exclude_exchange: Optional[str] = None,
    ) -> dict[str, float]:
        result: dict[str, float] = {}
        now = time.monotonic() if now_monotonic is None else now_monotonic
        for (exchange, inst_id), snapshot in self._books.items():
            if exchange == exclude_exchange or ticker_from_instrument(inst_id) != ticker:
                continue
            if max_age_seconds is not None and snapshot.age_seconds(now) > max_age_seconds:
                continue
            mid = snapshot.book.mid
            if mid is not None:
                result[exchange] = mid
        return result

    def exchange_time_ms(self, exchange: str) -> int:
        return self.exchange_times.get(exchange, 0)


class FairValueEngine:
    def __init__(self, state: MarketState, max_age_seconds: float = 0.8) -> None:
        self.state = state
        self.max_age_seconds = max_age_seconds

    def fair_value(
        self,
        ticker: str,
        target_exchange: str,
        now_monotonic: Optional[float] = None,
    ) -> Optional[float]:
        if ticker in ETF_BASKETS:
            component_values = [
                self.component_value(component, target_exchange, now_monotonic)
                for component in ETF_BASKETS[ticker]
            ]
            if any(value is None for value in component_values):
                return None
            return sum(value for value in component_values if value is not None) / len(component_values)
        return self.robust_ticker_value(ticker, target_exchange, now_monotonic)

    def component_value(
        self,
        ticker: str,
        target_exchange: str,
        now_monotonic: Optional[float],
    ) -> Optional[float]:
        zse_snapshot = self.state.snapshot("ZSE", instrument_id("ZSE", ticker))
        if zse_snapshot and (
            now_monotonic is None or zse_snapshot.age_seconds(now_monotonic) <= self.max_age_seconds
        ):
            return zse_snapshot.book.mid
        return self.robust_ticker_value(ticker, target_exchange, now_monotonic)

    def robust_ticker_value(
        self,
        ticker: str,
        target_exchange: str,
        now_monotonic: Optional[float],
    ) -> Optional[float]:
        max_age_seconds = self.max_age_seconds if now_monotonic is not None else None
        values = self.state.ticker_mids(
            ticker,
            max_age_seconds=max_age_seconds,
            now_monotonic=now_monotonic,
            exclude_exchange=target_exchange,
        )
        if not values:
            values = self.state.ticker_mids(
                ticker,
                max_age_seconds=max_age_seconds,
                now_monotonic=now_monotonic,
            )
        if not values:
            return None
        return float(statistics.median(values.values()))


class StrategyEngine:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def find_opportunities(
        self,
        state: MarketState,
        exchange: str,
        now_monotonic: Optional[float] = None,
    ) -> list[Opportunity]:
        now = time.monotonic() if now_monotonic is None else now_monotonic
        fair_values = FairValueEngine(state, self.config.stale_after_seconds)
        opportunities: list[Opportunity] = []

        for snapshot in state.books_on_exchange(exchange):
            if snapshot.age_seconds(now) > self.config.stale_after_seconds:
                continue
            ticker = snapshot.ticker
            fv = fair_values.fair_value(ticker, exchange, now)
            if fv is None:
                continue
            opportunities.extend(self._active_opportunities(snapshot, ticker, fv))
            if self.config.passive_enabled:
                opportunities.extend(self._passive_opportunities(snapshot, ticker, fv))

        opportunities = [opp for opp in opportunities if opp.score > 0]
        opportunities.sort(key=lambda opp: opp.score, reverse=True)
        return opportunities

    def _active_opportunities(
        self,
        snapshot: BookSnapshot,
        ticker: str,
        fair_value_cents: float,
    ) -> list[Opportunity]:
        book = snapshot.book
        threshold = self.config.etf_edge_cents if ticker in ETF_BASKETS else self.config.min_edge_cents
        result: list[Opportunity] = []

        if book.best_ask is not None:
            edge = fair_value_cents - book.best_ask
            if edge >= threshold:
                result.append(
                    self._opportunity(
                        snapshot=snapshot,
                        side=Side.BID,
                        price=book.best_ask,
                        available_qty=book.best_ask_qty,
                        order_type="ioc",
                        reason=f"buy underpriced {ticker}: ask {book.best_ask} below FV {fair_value_cents:.1f}",
                        edge=edge,
                    )
                )

        if book.best_bid is not None:
            edge = book.best_bid - fair_value_cents
            if edge >= threshold:
                result.append(
                    self._opportunity(
                        snapshot=snapshot,
                        side=Side.ASK,
                        price=book.best_bid,
                        available_qty=book.best_bid_qty,
                        order_type="ioc",
                        reason=f"sell overpriced {ticker}: bid {book.best_bid} above FV {fair_value_cents:.1f}",
                        edge=edge,
                    )
                )

        return result

    def _passive_opportunities(
        self,
        snapshot: BookSnapshot,
        ticker: str,
        fair_value_cents: float,
    ) -> list[Opportunity]:
        book = snapshot.book
        if snapshot.exchange_time_ms >= self.config.passive_stop_after_ms:
            return []
        if book.best_bid is None or book.best_ask is None or book.spread is None:
            return []
        if book.spread < self.config.passive_min_spread_cents:
            return []

        result: list[Opportunity] = []
        bid_price = min(book.best_bid + 1, math.floor(fair_value_cents - self.config.passive_edge_cents))
        ask_price = max(book.best_ask - 1, math.ceil(fair_value_cents + self.config.passive_edge_cents))

        if book.best_bid < bid_price < book.best_ask:
            edge = fair_value_cents - bid_price
            result.append(
                self._opportunity(
                    snapshot=snapshot,
                    side=Side.BID,
                    price=bid_price,
                    available_qty=self.config.order_quantity,
                    order_type="limit",
                    reason=f"passive bid {ticker} near FV {fair_value_cents:.1f}",
                    edge=edge,
                )
            )
        if book.best_bid < ask_price < book.best_ask:
            edge = ask_price - fair_value_cents
            result.append(
                self._opportunity(
                    snapshot=snapshot,
                    side=Side.ASK,
                    price=ask_price,
                    available_qty=self.config.order_quantity,
                    order_type="limit",
                    reason=f"passive ask {ticker} near FV {fair_value_cents:.1f}",
                    edge=edge,
                )
            )
        return result

    def _opportunity(
        self,
        snapshot: BookSnapshot,
        side: Side,
        price: int,
        available_qty: int,
        order_type: str,
        reason: str,
        edge: float,
    ) -> Opportunity:
        quantity = max(1, min(self.config.order_quantity, available_qty or self.config.order_quantity))
        rtt = LATENCY_RTT_MS.get(self.config.home_location, {}).get(snapshot.exchange, 100)
        spread_cost = max(0, snapshot.book.spread or 0) * 0.05
        latency_cost = rtt * self.config.latency_penalty_per_ms
        score = edge - spread_cost - latency_cost
        return Opportunity(
            exchange=snapshot.exchange,
            instrument_id=snapshot.instrument_id,
            side=side,
            price=int(price),
            quantity=quantity,
            order_type=order_type,
            reason=reason,
            edge_cents=float(edge),
            score=float(score),
        )


class RiskManager:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.positions: dict[str, int] = {}
        self.cash_by_exchange: dict[str, int] = {exchange: STARTING_CASH_CENTS for exchange in ALL_EXCHANGES}
        self.open_orders_by_exchange: dict[str, int] = {exchange: 0 for exchange in ALL_EXCHANGES}

    def reset_exchange(self, exchange: str) -> None:
        for inst_id in [inst_id for inst_id in self.positions if inst_id.startswith(f"{exchange}-")]:
            del self.positions[inst_id]
        self.cash_by_exchange[exchange] = STARTING_CASH_CENTS
        self.open_orders_by_exchange[exchange] = 0

    def update_inventory(self, exchange: str, data: dict[str, Any]) -> None:
        cash = data.get("$")
        if isinstance(cash, list) and len(cash) >= 2:
            self.cash_by_exchange[exchange] = int(cash[1])
        for inst_id, pair in data.items():
            if inst_id == "$":
                continue
            if isinstance(pair, list) and len(pair) >= 2:
                self.positions[inst_id] = int(pair[1])

    def check(self, opportunity: Opportunity, exchange_time_ms: int) -> tuple[bool, str]:
        if exchange_time_ms >= self.config.no_new_risk_after_ms:
            return False, "blocked near segment end"
        if opportunity.quantity <= 0:
            return False, "quantity must be positive"
        if opportunity.price <= 0:
            return False, "price must be positive"
        if self.open_orders_by_exchange.get(opportunity.exchange, 0) >= self.config.max_open_orders_per_exchange:
            return False, "too many open orders"

        current_position = self.positions.get(opportunity.instrument_id, 0)
        delta = opportunity.quantity if opportunity.side is Side.BID else -opportunity.quantity
        projected_position = current_position + delta
        if projected_position > min(self.config.max_long, OFFICIAL_MAX_LONG):
            return False, "long position limit"
        if projected_position < max(self.config.max_short, OFFICIAL_MAX_SHORT):
            return False, "short position limit"

        if opportunity.side is Side.BID:
            cash = self.cash_by_exchange.get(opportunity.exchange, STARTING_CASH_CENTS)
            projected_cash = cash - opportunity.price * opportunity.quantity
            if projected_cash < max(self.config.min_cash_cents, OFFICIAL_MIN_CASH):
                return False, "cash floor"

        return True, "ok"

    def reserve_live_order(self, opportunity: Opportunity) -> None:
        if opportunity.order_type == "limit":
            self.open_orders_by_exchange[opportunity.exchange] = (
                self.open_orders_by_exchange.get(opportunity.exchange, 0) + 1
            )

    def apply_immediate_fill(self, opportunity: Opportunity, data: dict[str, Any]) -> None:
        inv_change = data.get("immediate_inventory_change")
        cash_change = data.get("immediate_balance_change")
        if inv_change is not None:
            self.positions[opportunity.instrument_id] = (
                self.positions.get(opportunity.instrument_id, 0) + int(inv_change)
            )
        if cash_change is not None:
            self.cash_by_exchange[opportunity.exchange] = (
                self.cash_by_exchange.get(opportunity.exchange, STARTING_CASH_CENTS) + int(cash_change)
            )

    def note_cancel_or_fill(self, exchange: str) -> None:
        current = self.open_orders_by_exchange.get(exchange, 0)
        self.open_orders_by_exchange[exchange] = max(0, current - 1)


class TokenBucket:
    def __init__(self, rate_per_second: float, capacity: int, now: Optional[float] = None) -> None:
        self.rate_per_second = float(rate_per_second)
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.updated_at = time.monotonic() if now is None else now

    def try_acquire(self, tokens: int = 1, now: Optional[float] = None) -> bool:
        current_time = time.monotonic() if now is None else now
        elapsed = max(0.0, current_time - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
        self.updated_at = current_time
        if self.tokens >= tokens:
            self.tokens -= tokens
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
        for handle in self._files.values():
            handle.close()
        self._files.clear()


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
        self.inflight_orders: dict[str, Opportunity] = {}

    @property
    def url(self) -> str:
        return f"ws://{EXCHANGE_HOSTS[self.exchange]}:{EXCHANGE_PORT}/trade"

    def next_request_id(self, prefix: str) -> str:
        self.request_seq += 1
        return f"{self.exchange}-{prefix}-{int(time.time() * 1000)}-{self.request_seq}"

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
                    welcome = json.loads(await ws.recv())
                    print(f"[{self.exchange}] connected: {welcome.get('message', welcome)}")
                    await self.request_inventory(ws)
                    await self.listen(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{self.exchange}] connection error: {exc}")

            print(f"[{self.exchange}] reconnecting in {self.config.reconnect_delay_seconds:.1f}s")
            await asyncio.sleep(self.config.reconnect_delay_seconds)

    async def listen(self, ws: Any) -> None:
        async for raw_message in ws:
            if raw_message == "Message rate limit exceeded":
                print(f"[{self.exchange}] rate limit exceeded; connection will close")
                return
            message = json.loads(raw_message)
            msg_type = message.get("type")

            if msg_type == "market_data_update":
                await self.on_market_data(ws, message)
            elif msg_type == "add_order_response":
                self.on_add_order_response(message)
            elif msg_type == "cancel_order_response":
                self.on_cancel_order_response(message)
            elif msg_type == "get_inventory_response":
                self.risk.update_inventory(self.exchange, message.get("data", {}))
            elif msg_type == "end_of_round":
                print(f"[{self.exchange}] segment ended")
                self.risk.reset_exchange(self.exchange)
                self.state.reset_exchange(self.exchange)
                return
            elif msg_type == "error":
                print(f"[{self.exchange}] exchange error: {message.get('message')}")

    async def on_market_data(self, ws: Any, message: dict[str, Any]) -> None:
        now = time.monotonic()
        self.state.apply_market_data(self.exchange, message, received_monotonic=now)
        exchange_time_ms = self.state.exchange_time_ms(self.exchange)

        opportunities = self.strategy.find_opportunities(self.state, self.exchange, now_monotonic=now)
        sent = 0
        for opportunity in opportunities:
            if sent >= self.config.max_orders_per_tick:
                break
            allowed, reason = self.risk.check(opportunity, exchange_time_ms)
            if not allowed:
                continue
            self.recorder.write(
                "opportunities",
                {
                    "time": exchange_time_ms,
                    "exchange": self.exchange,
                    "instrument_id": opportunity.instrument_id,
                    "side": opportunity.side.value,
                    "price": opportunity.price,
                    "quantity": opportunity.quantity,
                    "order_type": opportunity.order_type,
                    "edge_cents": opportunity.edge_cents,
                    "score": opportunity.score,
                    "reason": opportunity.reason,
                    "live": self.config.live_trading,
                },
            )
            await self.place_order(ws, opportunity)
            sent += 1

    async def request_inventory(self, ws: Any) -> None:
        request = {"type": "get_inventory", "user_request_id": self.next_request_id("inventory")}
        await self.send_json(ws, request)

    async def place_order(self, ws: Any, opportunity: Opportunity) -> None:
        if not self.config.live_trading:
            print(
                f"[DRY {opportunity.exchange}] {opportunity.side.value} "
                f"{opportunity.quantity} {opportunity.instrument_id} @ {opportunity.price} "
                f"{opportunity.order_type} edge={opportunity.edge_cents:.1f} "
                f"score={opportunity.score:.1f} {opportunity.reason}"
            )
            return

        request_id = self.next_request_id("order")
        request: dict[str, Any] = {
            "type": "add_order",
            "user_request_id": request_id,
            "instrument_id": opportunity.instrument_id,
            "side": opportunity.side.value,
            "quantity": opportunity.quantity,
            "order_type": opportunity.order_type,
        }
        if opportunity.order_type in {"limit", "ioc"}:
            request["price"] = int(opportunity.price)
            request["expiry"] = int(time.time() * 1000) + self.config.ioc_expiry_ms

        self.inflight_orders[request_id] = opportunity
        self.risk.reserve_live_order(opportunity)
        await self.send_json(ws, request)

    async def send_json(self, ws: Any, request: dict[str, Any]) -> None:
        await self.bucket.wait_for_token()
        await ws.send(json.dumps(request, separators=(",", ":")))

    def on_add_order_response(self, message: dict[str, Any]) -> None:
        request_id = message.get("user_request_id", "")
        opportunity = self.inflight_orders.pop(request_id, None)
        if opportunity is None:
            return
        if not message.get("success", False):
            print(f"[{self.exchange}] order rejected: {message.get('data', {}).get('message')}")
            if opportunity.order_type == "limit":
                self.risk.note_cancel_or_fill(self.exchange)
            return
        data = message.get("data", {})
        self.risk.apply_immediate_fill(opportunity, data)

    def on_cancel_order_response(self, message: dict[str, Any]) -> None:
        if message.get("success", False):
            self.risk.note_cancel_or_fill(self.exchange)


async def run_bot(config: Optional[BotConfig] = None) -> None:
    cfg = config or BotConfig.from_env()
    if not cfg.exchanges:
        raise SystemExit("No valid exchanges configured")

    state = MarketState()
    strategy = StrategyEngine(cfg)
    risk = RiskManager(cfg)
    recorder = JSONLRecorder(cfg.output_dir)

    print(
        f"AlgoTrade bot starting: exchanges={','.join(cfg.exchanges)} "
        f"home={cfg.home_location} live={cfg.live_trading} "
        f"min_edge={cfg.min_edge_cents} etf_edge={cfg.etf_edge_cents}"
    )
    if not cfg.live_trading:
        print("Dry-run mode. Set LIVE_TRADING=1 to send orders.")

    clients = [
        ExchangeClient(exchange, cfg, state, strategy, risk, recorder)
        for exchange in cfg.exchanges
    ]
    try:
        await asyncio.gather(*(client.run_forever() for client in clients))
    finally:
        recorder.close()


def main() -> None:
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
