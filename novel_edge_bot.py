#!/usr/bin/env python3
"""Novel AlgoTrade 2026 edge bot.

The strategy is intentionally self-contained: no dependency on the existing bot
files in this repository, and no external services. It trades three edge classes:

1. ZSE ETF basket arbitrage with integer hedge ratios.
2. Same-ticker cross-venue IOC pairs.
3. Single-leg fair-value dislocations, with a small lead-lag adjustment.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


EXCHANGES = [
    "NYSE",
    "NASDAQ",
    "SSE",
    "JPX",
    "Euronext",
    "LSE",
    "HKEX",
    "NSE",
    "TMX",
    "ZSE",
]

HOSTS = {
    "NYSE": "nyse.algotrade.hr",
    "NASDAQ": "nasdaq.algotrade.hr",
    "SSE": "sse.algotrade.hr",
    "JPX": "jpx.algotrade.hr",
    "Euronext": "euronext.algotrade.hr",
    "LSE": "lse.algotrade.hr",
    "HKEX": "hkex.algotrade.hr",
    "NSE": "nse.algotrade.hr",
    "TMX": "tmx.algotrade.hr",
    "ZSE": "zse.algotrade.hr",
}

ETF_BASKETS = {
    "ETFA": ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"),
    "ETFB": ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"),
    "ETFA3": ("NGUP", "KTST", "XFR"),
    "ETFB3": ("KOTD", "INA", "DLKV"),
    "ETFSH": ("GOLD", "XAG"),
}

SECTORS = {
    "A": ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"),
    "B": ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"),
    "SAFE": ("GOLD", "XAG"),
}

STOCK_LISTINGS = {
    "CARD": EXCHANGES,
    "SIMP": EXCHANGES,
    "NGUP": ("NYSE", "NASDAQ", "Euronext", "TMX", "ZSE"),
    "OIT": ("LSE", "Euronext", "HKEX", "NSE", "ZSE"),
    "KTST": ("NYSE", "JPX", "TMX", "ZSE"),
    "FSR": ("NASDAQ", "LSE", "SSE", "HKEX", "ZSE"),
    "JZRO": ("NYSE", "LSE", "Euronext", "TMX", "ZSE"),
    "XFR": ("NYSE", "HKEX", "TMX", "ZSE"),
    "KOTD": ("NASDAQ", "LSE", "Euronext", "HKEX", "ZSE"),
    "INA": ("NYSE", "NASDAQ", "Euronext", "HKEX", "ZSE"),
    "HT": ("NASDAQ", "LSE", "JPX", "SSE", "TMX", "ZSE"),
    "JNAF": ("NYSE", "Euronext", "JPX", "HKEX", "ZSE"),
    "DLKV": ("NASDAQ", "LSE", "HKEX", "NSE", "ZSE"),
    "DDJH": ("NYSE", "LSE", "Euronext", "TMX", "ZSE"),
    "MDKA": ("NYSE", "LSE", "HKEX", "TMX", "ZSE"),
    "KRAS": ("NYSE", "Euronext", "SSE", "TMX", "ZSE"),
    "ZITO": ("NASDAQ", "LSE", "Euronext", "NSE", "ZSE"),
    "ZABA": ("NYSE", "LSE", "SSE", "NSE", "TMX", "ZSE"),
    "GOLD": ("NASDAQ", "Euronext", "JPX", "TMX", "ZSE"),
    "XAG": ("LSE", "Euronext", "JPX", "ZSE"),
}

ETF_LISTINGS = {
    "ETFA": ("NYSE", "Euronext", "HKEX", "ZSE"),
    "ETFB": ("NASDAQ", "LSE", "HKEX", "ZSE"),
    "ETFA3": ("NYSE", "TMX", "ZSE"),
    "ETFB3": ("NASDAQ", "HKEX", "ZSE"),
    "ETFSH": ("Euronext", "JPX", "ZSE"),
}

LISTINGS_BY_TICKER = {**STOCK_LISTINGS, **ETF_LISTINGS}
TICKERS = tuple(LISTINGS_BY_TICKER.keys())
ROUND_LENGTH_MS = 600_000


class Side(str, Enum):
    BID = "bid"
    ASK = "ask"

    @property
    def position_delta(self) -> int:
        return 1 if self is Side.BID else -1


@dataclass(frozen=True)
class BotConfig:
    live_trading: bool = False
    max_msgs_per_second: int = 350
    max_book_age_seconds: float = 1.25
    min_single_edge_cents: int = 35
    min_pair_edge_cents: int = 70
    min_basket_edge_cents: int = 22
    single_leg_quantity: int = 8
    pair_quantity: int = 6
    max_basket_units: int = 3
    max_groups_per_tick: int = 3
    no_new_risk_after_ms: int = 575_000
    ioc_expiry_ms: int = 2_500
    max_long: int = 350
    max_short: int = -120
    min_cash_cents: int = -2_500_000
    lead_lag_weight: float = 0.22
    max_spread_cents: int = 120
    group_cooldown_seconds: float = 0.35
    inventory_poll_seconds: float = 3.0
    reconnect_initial_seconds: float = 0.5
    reconnect_max_seconds: float = 8.0

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            live_trading=env_bool("LIVE_TRADING", False),
            max_msgs_per_second=env_int("MAX_MSGS_PER_SEC", 350),
            max_book_age_seconds=env_float("MAX_BOOK_AGE_SECONDS", 1.25),
            min_single_edge_cents=env_int("SINGLE_EDGE_CENTS", 35),
            min_pair_edge_cents=env_int("PAIR_EDGE_CENTS", 70),
            min_basket_edge_cents=env_int("BASKET_EDGE_CENTS", 22),
            single_leg_quantity=env_int("SINGLE_QTY", 8),
            pair_quantity=env_int("PAIR_QTY", 6),
            max_basket_units=env_int("BASKET_UNITS", 3),
            max_groups_per_tick=env_int("MAX_GROUPS_PER_TICK", 3),
            no_new_risk_after_ms=env_int("NO_NEW_RISK_AFTER_MS", 575_000),
            ioc_expiry_ms=env_int("IOC_EXPIRY_MS", 2_500),
            max_long=env_int("MAX_LONG", 350),
            max_short=env_int("MAX_SHORT", -120),
            min_cash_cents=env_int("MIN_CASH_CENTS", -2_500_000),
            lead_lag_weight=env_float("LEAD_LAG_WEIGHT", 0.22),
            max_spread_cents=env_int("MAX_SPREAD_CENTS", 120),
            group_cooldown_seconds=env_float("GROUP_COOLDOWN_SECONDS", 0.35),
            inventory_poll_seconds=env_float("INVENTORY_POLL_SECONDS", 3.0),
        )


@dataclass(frozen=True)
class Level:
    price: int
    quantity: int


@dataclass(frozen=True)
class OrderBook:
    bids: tuple[Level, ...] = ()
    asks: tuple[Level, ...] = ()

    @property
    def best_bid(self) -> int | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> int | None:
        return self.asks[0].price if self.asks else None

    @property
    def best_bid_qty(self) -> int:
        return self.bids[0].quantity if self.bids else 0

    @property
    def best_ask_qty(self) -> int:
        return self.asks[0].quantity if self.asks else 0

    @property
    def spread(self) -> int | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    def bid_quantity_at_or_better(self, price: int) -> int:
        return sum(level.quantity for level in self.bids if level.price >= price)

    def ask_quantity_at_or_better(self, price: int) -> int:
        return sum(level.quantity for level in self.asks if level.price <= price)


@dataclass(frozen=True)
class BookSnapshot:
    exchange: str
    ticker: str
    instrument_id: str
    book: OrderBook
    exchange_time_ms: int
    received_monotonic: float


@dataclass(frozen=True)
class TradeLeg:
    exchange: str
    instrument_id: str
    side: Side
    price: int
    quantity: int
    order_type: str = "ioc"


@dataclass(frozen=True)
class TradeGroup:
    kind: str
    legs: tuple[TradeLeg, ...]
    edge_cents: float
    score: float
    reason: str

    @property
    def key(self) -> tuple[Any, ...]:
        return (
            self.kind,
            tuple((leg.instrument_id, leg.side.value, leg.price) for leg in self.legs),
        )


def parse_orderbook_depth(raw: dict[str, Any]) -> OrderBook:
    bids = tuple(
        Level(int(price), int(qty))
        for price, qty in sorted(
            (raw.get("bids") or {}).items(), key=lambda item: int(item[0]), reverse=True
        )
        if int(qty) > 0
    )
    asks = tuple(
        Level(int(price), int(qty))
        for price, qty in sorted((raw.get("asks") or {}).items(), key=lambda item: int(item[0]))
        if int(qty) > 0
    )
    return OrderBook(bids=bids, asks=asks)


class MarketState:
    def __init__(self) -> None:
        self._books: dict[str, BookSnapshot] = {}
        self._by_exchange_ticker: dict[tuple[str, str], str] = {}
        self._by_ticker: dict[str, set[str]] = defaultdict(set)
        self._mid_history: dict[str, deque[tuple[float, float]]] = defaultdict(lambda: deque(maxlen=20))
        self.exchange_time_ms: dict[str, int] = {}

    def apply_market_data(
        self, exchange: str, message: dict[str, Any], received_monotonic: float
    ) -> None:
        exchange_time = int(message.get("time") or 0)
        self.exchange_time_ms[exchange] = exchange_time
        for instrument_id, raw_depth in (message.get("orderbook_depths") or {}).items():
            inst_exchange, ticker = split_instrument(instrument_id)
            if inst_exchange != exchange:
                continue
            book = parse_orderbook_depth(raw_depth)
            snapshot = BookSnapshot(
                exchange=exchange,
                ticker=ticker,
                instrument_id=instrument_id,
                book=book,
                exchange_time_ms=exchange_time,
                received_monotonic=received_monotonic,
            )
            self._books[instrument_id] = snapshot
            self._by_exchange_ticker[(exchange, ticker)] = instrument_id
            self._by_ticker[ticker].add(instrument_id)
            if book.mid is not None:
                self._mid_history[instrument_id].append((received_monotonic, book.mid))

    def snapshot(self, exchange: str, ticker_or_instrument: str) -> BookSnapshot | None:
        if "-" in ticker_or_instrument:
            return self._books.get(ticker_or_instrument)
        instrument_id = self._by_exchange_ticker.get((exchange, ticker_or_instrument))
        return self._books.get(instrument_id) if instrument_id else None

    def book(self, exchange: str, ticker_or_instrument: str) -> OrderBook | None:
        snapshot = self.snapshot(exchange, ticker_or_instrument)
        return snapshot.book if snapshot else None

    def fresh_snapshot(
        self, exchange: str, ticker: str, now: float, max_age_seconds: float
    ) -> BookSnapshot | None:
        snapshot = self.snapshot(exchange, ticker)
        if snapshot is None:
            return None
        if now - snapshot.received_monotonic > max_age_seconds:
            return None
        if snapshot.book.mid is None:
            return None
        return snapshot

    def fresh_snapshots_for_ticker(
        self, ticker: str, now: float, max_age_seconds: float
    ) -> list[BookSnapshot]:
        snapshots = []
        for instrument_id in self._by_ticker.get(ticker, ()):
            snapshot = self._books.get(instrument_id)
            if snapshot and now - snapshot.received_monotonic <= max_age_seconds:
                if snapshot.book.mid is not None:
                    snapshots.append(snapshot)
        return snapshots

    def fresh_snapshots_for_exchange(
        self, exchange: str, now: float, max_age_seconds: float
    ) -> list[BookSnapshot]:
        snapshots = []
        for (book_exchange, _ticker), instrument_id in self._by_exchange_ticker.items():
            if book_exchange != exchange:
                continue
            snapshot = self._books.get(instrument_id)
            if snapshot and now - snapshot.received_monotonic <= max_age_seconds:
                if snapshot.book.best_bid is not None and snapshot.book.best_ask is not None:
                    snapshots.append(snapshot)
        return snapshots

    def mid_delta(self, instrument_id: str, lookback_seconds: float, now: float) -> float | None:
        history = self._mid_history.get(instrument_id)
        if not history or len(history) < 2:
            return None
        current = history[-1][1]
        baseline = None
        cutoff = now - lookback_seconds
        for ts, mid in reversed(history):
            baseline = mid
            if ts <= cutoff:
                break
        if baseline is None:
            return None
        return current - baseline


class FairValueEngine:
    def __init__(self, state: MarketState, config: BotConfig) -> None:
        self.state = state
        self.config = config

    def fair_value(self, ticker: str, preferred_exchange: str, now: float) -> float | None:
        if ticker in ETF_BASKETS:
            return self._etf_fair_value(ticker, preferred_exchange, now)
        return self._stock_fair_value(ticker, preferred_exchange, now)

    def _etf_fair_value(self, ticker: str, preferred_exchange: str, now: float) -> float | None:
        constituent_values = []
        for component in ETF_BASKETS[ticker]:
            value = self._best_component_value(component, preferred_exchange, now)
            if value is None:
                return None
            constituent_values.append(value)
        return sum(constituent_values) / len(constituent_values)

    def _best_component_value(
        self, ticker: str, preferred_exchange: str, now: float
    ) -> float | None:
        for exchange in (preferred_exchange, "ZSE"):
            snapshot = self.state.fresh_snapshot(
                exchange, ticker, now, self.config.max_book_age_seconds
            )
            if snapshot and snapshot.book.mid is not None:
                return snapshot.book.mid
        return self._stock_fair_value(ticker, preferred_exchange, now)

    def _stock_fair_value(self, ticker: str, preferred_exchange: str, now: float) -> float | None:
        snapshots = self.state.fresh_snapshots_for_ticker(
            ticker, now, self.config.max_book_age_seconds
        )
        mids = [snapshot.book.mid for snapshot in snapshots if snapshot.book.mid is not None]
        if not mids:
            return None
        base = statistics.median(mids)
        deltas = []
        for snapshot in snapshots:
            if snapshot.exchange == preferred_exchange:
                continue
            delta = self.state.mid_delta(snapshot.instrument_id, lookback_seconds=0.8, now=now)
            if delta is not None:
                deltas.append(delta)
        if deltas:
            base += statistics.median(deltas) * self.config.lead_lag_weight
        return base


class StrategyEngine:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def make_single_leg_group(
        self,
        exchange: str,
        instrument_id: str,
        side: Side,
        price: int,
        quantity: int,
        edge_cents: float,
        reason: str,
    ) -> TradeGroup:
        leg = TradeLeg(
            exchange=exchange,
            instrument_id=instrument_id,
            side=side,
            price=int(price),
            quantity=int(quantity),
        )
        return TradeGroup(
            kind="single",
            legs=(leg,),
            edge_cents=float(edge_cents),
            score=float(edge_cents),
            reason=reason,
        )

    def find_zse_basket_arbs(self, state: MarketState, now: float) -> list[TradeGroup]:
        groups: list[TradeGroup] = []
        exchange = "ZSE"
        for etf, components in ETF_BASKETS.items():
            etf_snapshot = state.fresh_snapshot(
                exchange, etf, now, self.config.max_book_age_seconds
            )
            if etf_snapshot is None:
                continue
            component_snapshots = [
                state.fresh_snapshot(exchange, component, now, self.config.max_book_age_seconds)
                for component in components
            ]
            if any(snapshot is None for snapshot in component_snapshots):
                continue
            component_books = [snapshot.book for snapshot in component_snapshots if snapshot]
            etf_book = etf_snapshot.book
            if not has_crossable_book(etf_book) or not all(has_crossable_book(b) for b in component_books):
                continue

            component_bid_sum = sum(book.best_bid for book in component_books if book.best_bid is not None)
            component_ask_sum = sum(book.best_ask for book in component_books if book.best_ask is not None)
            n = len(components)
            rich_edge = (etf_book.best_bid or 0) - component_ask_sum / n
            cheap_edge = component_bid_sum / n - (etf_book.best_ask or 0)

            if rich_edge >= self.config.min_basket_edge_cents:
                units = min(
                    self.config.max_basket_units,
                    etf_book.best_bid_qty // n,
                    *(book.best_ask_qty for book in component_books),
                )
                if units > 0:
                    legs = [
                        TradeLeg(exchange, etf_snapshot.instrument_id, Side.ASK, etf_book.best_bid, units * n)
                    ]
                    legs.extend(
                        TradeLeg(exchange, f"{exchange}-{component}", Side.BID, book.best_ask, units)
                        for component, book in zip(components, component_books)
                    )
                    gross_edge = n * (etf_book.best_bid or 0) - component_ask_sum
                    groups.append(
                        TradeGroup(
                            kind="zse_basket",
                            legs=tuple(legs),
                            edge_cents=rich_edge,
                            score=gross_edge * units,
                            reason=f"{etf} rich to basket by {rich_edge:.1f}c",
                        )
                    )

            if cheap_edge >= self.config.min_basket_edge_cents:
                units = min(
                    self.config.max_basket_units,
                    etf_book.best_ask_qty // n,
                    *(book.best_bid_qty for book in component_books),
                )
                if units > 0:
                    legs = [
                        TradeLeg(exchange, etf_snapshot.instrument_id, Side.BID, etf_book.best_ask, units * n)
                    ]
                    legs.extend(
                        TradeLeg(exchange, f"{exchange}-{component}", Side.ASK, book.best_bid, units)
                        for component, book in zip(components, component_books)
                    )
                    gross_edge = component_bid_sum - n * (etf_book.best_ask or 0)
                    groups.append(
                        TradeGroup(
                            kind="zse_basket",
                            legs=tuple(legs),
                            edge_cents=cheap_edge,
                            score=gross_edge * units,
                            reason=f"{etf} cheap to basket by {cheap_edge:.1f}c",
                        )
                    )
        return sorted(groups, key=lambda group: group.score, reverse=True)

    def find_cross_venue_pairs(self, state: MarketState, now: float) -> list[TradeGroup]:
        groups: list[TradeGroup] = []
        for ticker in TICKERS:
            snapshots = state.fresh_snapshots_for_ticker(
                ticker, now, self.config.max_book_age_seconds
            )
            crossable = [snapshot for snapshot in snapshots if has_crossable_book(snapshot.book)]
            for buy_snapshot in crossable:
                buy_ask = buy_snapshot.book.best_ask
                if buy_ask is None:
                    continue
                for sell_snapshot in crossable:
                    if sell_snapshot.exchange == buy_snapshot.exchange:
                        continue
                    sell_bid = sell_snapshot.book.best_bid
                    if sell_bid is None:
                        continue
                    edge = sell_bid - buy_ask
                    if edge < self.config.min_pair_edge_cents:
                        continue
                    quantity = min(
                        self.config.pair_quantity,
                        buy_snapshot.book.best_ask_qty,
                        sell_snapshot.book.best_bid_qty,
                    )
                    if quantity <= 0:
                        continue
                    groups.append(
                        TradeGroup(
                            kind="cross_pair",
                            legs=(
                                TradeLeg(
                                    buy_snapshot.exchange,
                                    buy_snapshot.instrument_id,
                                    Side.BID,
                                    buy_ask,
                                    quantity,
                                ),
                                TradeLeg(
                                    sell_snapshot.exchange,
                                    sell_snapshot.instrument_id,
                                    Side.ASK,
                                    sell_bid,
                                    quantity,
                                ),
                            ),
                            edge_cents=edge,
                            score=edge * quantity,
                            reason=(
                                f"{ticker} {buy_snapshot.exchange} ask {buy_ask} "
                                f"vs {sell_snapshot.exchange} bid {sell_bid}"
                            ),
                        )
                    )
        return best_distinct_tickers(groups)

    def find_single_leg_dislocations(
        self, state: MarketState, exchange: str, now: float
    ) -> list[TradeGroup]:
        fair_engine = FairValueEngine(state, self.config)
        groups: list[TradeGroup] = []
        for snapshot in state.fresh_snapshots_for_exchange(
            exchange, now, self.config.max_book_age_seconds
        ):
            book = snapshot.book
            if not has_crossable_book(book):
                continue
            if book.spread is not None and book.spread > self.config.max_spread_cents:
                continue
            fair = fair_engine.fair_value(snapshot.ticker, exchange, now)
            if fair is None:
                continue
            if book.best_ask is not None:
                buy_edge = fair - book.best_ask
                if buy_edge >= self.config.min_single_edge_cents:
                    quantity = min(self.config.single_leg_quantity, book.best_ask_qty)
                    if quantity > 0:
                        groups.append(
                            self.make_single_leg_group(
                                exchange,
                                snapshot.instrument_id,
                                Side.BID,
                                book.best_ask,
                                quantity,
                                buy_edge,
                                f"{snapshot.ticker} ask below fair {fair:.1f}",
                            )
                        )
            if book.best_bid is not None:
                sell_edge = book.best_bid - fair
                if sell_edge >= self.config.min_single_edge_cents:
                    quantity = min(self.config.single_leg_quantity, book.best_bid_qty)
                    if quantity > 0:
                        groups.append(
                            self.make_single_leg_group(
                                exchange,
                                snapshot.instrument_id,
                                Side.ASK,
                                book.best_bid,
                                quantity,
                                sell_edge,
                                f"{snapshot.ticker} bid above fair {fair:.1f}",
                            )
                        )
        return sorted(groups, key=lambda group: group.score, reverse=True)

    def find_groups_for_tick(
        self, state: MarketState, trigger_exchange: str, now: float
    ) -> list[TradeGroup]:
        groups = []
        if trigger_exchange == "ZSE":
            groups.extend(self.find_zse_basket_arbs(state, now))
        groups.extend(self.find_cross_venue_pairs(state, now))
        groups.extend(self.find_single_leg_dislocations(state, trigger_exchange, now))
        return sorted(groups, key=lambda group: group.score, reverse=True)


class RiskManager:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.positions: dict[str, int] = defaultdict(int)
        self.cash_total: dict[str, int] = defaultdict(lambda: 10_000_000)

    def update_inventory(self, exchange: str, data: dict[str, Any]) -> None:
        for instrument_id, pair in data.items():
            if not isinstance(pair, list) or len(pair) != 2:
                continue
            total = int(pair[1])
            if instrument_id == "$":
                self.cash_total[exchange] = total
            else:
                self.positions[instrument_id] = total

    def apply_add_order_response(
        self, exchange: str, instrument_id: str, response: dict[str, Any]
    ) -> None:
        data = response.get("data") or {}
        inventory_delta = data.get("immediate_inventory_change")
        balance_delta = data.get("immediate_balance_change")
        if inventory_delta is not None:
            self.positions[instrument_id] += int(inventory_delta)
        if balance_delta is not None:
            self.cash_total[exchange] += int(balance_delta)

    def check_group(self, group: TradeGroup, exchange_time_ms: int) -> tuple[bool, str]:
        if exchange_time_ms >= self.config.no_new_risk_after_ms:
            return False, "blocked near segment end"

        projected_positions = defaultdict(int, self.positions)
        projected_cash = defaultdict(lambda: 10_000_000, self.cash_total)
        for leg in group.legs:
            projected_positions[leg.instrument_id] += leg.side.position_delta * leg.quantity
            if projected_positions[leg.instrument_id] > self.config.max_long:
                return False, f"long cap {leg.instrument_id}"
            if projected_positions[leg.instrument_id] < self.config.max_short:
                return False, f"short cap {leg.instrument_id}"
            cash_delta = -leg.price * leg.quantity if leg.side is Side.BID else leg.price * leg.quantity
            projected_cash[leg.exchange] += cash_delta
            if projected_cash[leg.exchange] < self.config.min_cash_cents:
                return False, f"cash floor {leg.exchange}"
        return True, "ok"


class TokenBucket:
    def __init__(self, rate_per_second: int, capacity: int | None = None, now: float | None = None) -> None:
        self.rate = float(rate_per_second)
        self.capacity = float(capacity if capacity is not None else rate_per_second)
        self.tokens = self.capacity
        self.updated_at = time.monotonic() if now is None else now

    def try_acquire(self, tokens: float = 1.0, now: float | None = None) -> bool:
        current = time.monotonic() if now is None else now
        elapsed = max(0.0, current - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated_at = current
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    async def wait_acquire(self, tokens: float = 1.0) -> None:
        while not self.try_acquire(tokens):
            await asyncio.sleep(max(0.001, tokens / max(self.rate, 1.0) / 2.0))


class ExchangeSession:
    def __init__(self, exchange: str, coordinator: "BotCoordinator", config: BotConfig) -> None:
        self.exchange = exchange
        self.coordinator = coordinator
        self.config = config
        self.bucket = TokenBucket(config.max_msgs_per_second, capacity=config.max_msgs_per_second)
        self.ws: Any = None
        self.connected = False
        self.last_inventory_poll = 0.0
        self.pending_request_instruments: dict[str, str] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        try:
            from websockets.asyncio.client import connect as ws_connect
        except ImportError as exc:
            raise RuntimeError("Install websockets>=12 from requirements.txt") from exc

        backoff = self.config.reconnect_initial_seconds
        while not stop_event.is_set():
            url = f"ws://{HOSTS[self.exchange]}:9001/trade"
            try:
                logging.info("connecting exchange=%s url=%s", self.exchange, url)
                async with ws_connect(url, max_size=16 * 1024 * 1024, compression=None) as ws:
                    self.ws = ws
                    self.connected = True
                    self.coordinator.sessions[self.exchange] = self
                    backoff = self.config.reconnect_initial_seconds
                    await self._request_inventory()
                    await self._request_market_data()
                    async for raw in ws:
                        if stop_event.is_set():
                            break
                        await self._handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.warning("exchange=%s disconnected: %s", self.exchange, exc)
            finally:
                self.connected = False
                if self.coordinator.sessions.get(self.exchange) is self:
                    self.coordinator.sessions.pop(self.exchange, None)
                self.ws = None
            await asyncio.sleep(backoff)
            backoff = min(self.config.reconnect_max_seconds, backoff * 1.7)

    async def send(self, message: dict[str, Any]) -> None:
        if self.ws is None:
            return
        await self.bucket.wait_acquire()
        await self.ws.send(json.dumps(message, separators=(",", ":")))

    async def _handle_raw(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if raw == "Message rate limit exceeded":
            logging.error("server closed %s for message rate limit", self.exchange)
            return
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            logging.debug("non-json frame exchange=%s raw=%r", self.exchange, raw[:120])
            return

        msg_type = message.get("type")
        now = time.monotonic()
        if msg_type == "market_data_update":
            self.coordinator.state.apply_market_data(self.exchange, message, now)
            await self._maybe_poll_inventory(now)
            await self.coordinator.on_market_data(self.exchange, now)
        elif msg_type == "get_inventory_response":
            self.coordinator.risk.update_inventory(self.exchange, message.get("data") or {})
        elif msg_type == "add_order_response":
            request_id = str(message.get("user_request_id") or "")
            instrument_id = self.pending_request_instruments.pop(request_id, "")
            if instrument_id:
                self.coordinator.risk.apply_add_order_response(self.exchange, instrument_id, message)
            if not message.get("success", False):
                logging.info("order rejected exchange=%s response=%s", self.exchange, message)
        elif msg_type == "end_of_round":
            logging.info("end_of_round exchange=%s", self.exchange)
        elif msg_type == "welcome":
            logging.info("welcome exchange=%s", self.exchange)
        elif msg_type == "error":
            logging.warning("api error exchange=%s message=%s", self.exchange, message)

    async def _maybe_poll_inventory(self, now: float) -> None:
        if now - self.last_inventory_poll >= self.config.inventory_poll_seconds:
            await self._request_inventory()

    async def _request_inventory(self) -> None:
        self.last_inventory_poll = time.monotonic()
        await self.send(
            {
                "type": "get_inventory",
                "user_request_id": f"inv-{self.exchange}-{int(time.time() * 1000)}",
            }
        )

    async def _request_market_data(self) -> None:
        await self.send(
            {
                "type": "get_market_data",
                "user_request_id": f"md-{self.exchange}-{int(time.time() * 1000)}",
            }
        )


class BotCoordinator:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.state = MarketState()
        self.strategy = StrategyEngine(config)
        self.risk = RiskManager(config)
        self.sessions: dict[str, ExchangeSession] = {}
        self._last_group_sent: dict[tuple[Any, ...], float] = {}
        self._request_seq = 0

    async def on_market_data(self, trigger_exchange: str, now: float) -> None:
        groups = self.strategy.find_groups_for_tick(self.state, trigger_exchange, now)
        sent = 0
        for group in groups:
            if sent >= self.config.max_groups_per_tick:
                break
            if self._cooling_down(group, now):
                continue
            exchange_time = max(
                self.state.exchange_time_ms.get(leg.exchange, 0) for leg in group.legs
            )
            allowed, reason = self.risk.check_group(group, exchange_time)
            if not allowed:
                logging.debug("risk blocked kind=%s reason=%s", group.kind, reason)
                continue
            await self._dispatch_group(group)
            self._last_group_sent[group.key] = now
            sent += 1

    def _cooling_down(self, group: TradeGroup, now: float) -> bool:
        last = self._last_group_sent.get(group.key)
        return last is not None and now - last < self.config.group_cooldown_seconds

    async def _dispatch_group(self, group: TradeGroup) -> None:
        if not self.config.live_trading:
            logging.info(
                "DRY kind=%s edge=%.1f score=%.1f reason=%s legs=%s",
                group.kind,
                group.edge_cents,
                group.score,
                group.reason,
                [
                    (leg.exchange, leg.instrument_id, leg.side.value, leg.price, leg.quantity)
                    for leg in group.legs
                ],
            )
            return
        logging.info(
            "LIVE kind=%s edge=%.1f score=%.1f reason=%s",
            group.kind,
            group.edge_cents,
            group.score,
            group.reason,
        )
        await asyncio.gather(*(self._send_leg(leg, group.kind) for leg in group.legs))

    async def _send_leg(self, leg: TradeLeg, kind: str) -> None:
        session = self.sessions.get(leg.exchange)
        if session is None or not session.connected:
            logging.debug("skip disconnected leg=%s", leg)
            return
        self._request_seq += 1
        request_id = f"{kind}-{self._request_seq}-{leg.exchange}"
        expiry_ms = int(time.time() * 1000) + self.config.ioc_expiry_ms
        message = build_order_message(
            request_id=request_id,
            instrument_id=leg.instrument_id,
            side=leg.side,
            price=leg.price,
            quantity=leg.quantity,
            expiry_ms=expiry_ms,
        )
        session.pending_request_instruments[request_id] = leg.instrument_id
        await session.send(message)


def build_order_message(
    request_id: str,
    instrument_id: str,
    side: Side,
    price: int,
    quantity: int,
    expiry_ms: int,
) -> dict[str, Any]:
    return {
        "type": "add_order",
        "user_request_id": request_id,
        "instrument_id": instrument_id,
        "price": int(price),
        "expiry": int(expiry_ms),
        "side": side.value,
        "quantity": int(quantity),
        "order_type": "ioc",
    }


def has_crossable_book(book: OrderBook) -> bool:
    return book.best_bid is not None and book.best_ask is not None


def split_instrument(instrument_id: str) -> tuple[str, str]:
    exchange, ticker = instrument_id.split("-", 1)
    return exchange, ticker


def best_distinct_tickers(groups: list[TradeGroup]) -> list[TradeGroup]:
    sorted_groups = sorted(groups, key=lambda group: group.score, reverse=True)
    used_tickers: set[str] = set()
    result = []
    for group in sorted_groups:
        ticker = split_instrument(group.legs[0].instrument_id)[1]
        if ticker in used_tickers:
            continue
        used_tickers.add(ticker)
        result.append(group)
    return result


def parse_exchanges(raw: str | None) -> list[str]:
    if not raw:
        return list(EXCHANGES)
    aliases = {exchange.upper(): exchange for exchange in EXCHANGES}
    chosen = []
    for part in raw.split(","):
        key = part.strip().upper()
        if key in aliases and aliases[key] not in chosen:
            chosen.append(aliases[key])
    return chosen or list(EXCHANGES)


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "live"}


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


async def async_main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = BotConfig.from_env()
    exchanges = parse_exchanges(os.environ.get("EXCHANGES"))
    coordinator = BotCoordinator(config)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    logging.info(
        "starting novel_edge_bot live=%s exchanges=%s max_msgs_per_sec=%s",
        config.live_trading,
        ",".join(exchanges),
        config.max_msgs_per_second,
    )
    tasks = [
        asyncio.create_task(ExchangeSession(exchange, coordinator, config).run(stop_event))
        for exchange in exchanges
    ]
    try:
        await stop_event.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
