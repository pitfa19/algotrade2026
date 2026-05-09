#!/usr/bin/env python3
"""Aggressive IOC edge bot and offline scanner for AlgoTrade 2026.

The file is deliberately standalone: it does not import or depend on any of the
existing strategy files in this repository.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from websockets.asyncio.client import connect as ws_connect
except Exception:  # pragma: no cover - backtests do not need websockets.
    ws_connect = None


STARTING_CASH = 10_000_000
CASH_FLOOR = -5_000_000
LONG_LIMIT = 2_000
SHORT_LIMIT = -200
ROUND_LENGTH_MS = 600_000

ETF_BASKETS = {
    "ETFA": ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"),
    "ETFB": ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"),
    "ETFA3": ("NGUP", "KTST", "XFR"),
    "ETFB3": ("KOTD", "INA", "DLKV"),
    "ETFSH": ("GOLD", "XAG"),
}

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

CLOSE_CLUSTERS = {
    "NYSE": ("NYSE", "NASDAQ", "TMX"),
    "ZSE": ("ZSE", "Euronext", "LSE"),
    "HKEX": ("HKEX", "SSE", "JPX", "NSE"),
    "ALL": tuple(HOSTS),
}

SPECIAL_TICKERS = ("CARD", "SIMP")


@dataclass
class Book:
    bids: List[Tuple[int, int]] = field(default_factory=list)
    asks: List[Tuple[int, int]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.bids = sorted([(int(p), int(q)) for p, q in self.bids if q > 0], reverse=True)
        self.asks = sorted([(int(p), int(q)) for p, q in self.asks if q > 0])

    @property
    def best_bid(self) -> Optional[int]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid


@dataclass
class OrderLeg:
    exchange: str
    instrument_id: str
    side: str
    quantity: int
    price: int
    label: str = ""


@dataclass
class Opportunity:
    label: str
    edge_cents: int
    legs: List[OrderLeg]
    priority: int = 0


@dataclass
class SimAccount:
    cash: int = STARTING_CASH
    cash_floor: int = CASH_FLOOR
    long_limit: int = LONG_LIMIT
    short_limit: int = SHORT_LIMIT
    positions: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def buy_capacity(self, instrument_id: str, price: int) -> int:
        pos_capacity = self.long_limit - self.positions[instrument_id]
        cash_capacity = (self.cash - self.cash_floor) // price
        return max(0, min(pos_capacity, cash_capacity))

    def sell_capacity(self, instrument_id: str) -> int:
        return max(0, self.positions[instrument_id] - self.short_limit)

    def apply_ioc(self, instrument_id: str, side: str, book: Book, quantity: int) -> int:
        if quantity <= 0:
            return 0
        filled = 0
        cash_delta = 0
        if side == "bid":
            remaining = min(quantity, self.long_limit - self.positions[instrument_id])
            for price, level_qty in book.asks:
                if remaining <= 0:
                    break
                affordable = (self.cash - cash_delta - self.cash_floor) // price
                take = min(remaining, level_qty, affordable)
                if take <= 0:
                    break
                filled += take
                cash_delta += take * price
                remaining -= take
            self.positions[instrument_id] += filled
            self.cash -= cash_delta
        elif side == "ask":
            remaining = min(quantity, self.positions[instrument_id] - self.short_limit)
            for price, level_qty in book.bids:
                if remaining <= 0:
                    break
                take = min(remaining, level_qty)
                filled += take
                cash_delta += take * price
                remaining -= take
            self.positions[instrument_id] -= filled
            self.cash += cash_delta
        else:
            raise ValueError(f"unknown side {side!r}")
        return filled

    def mark_to_market(self, final_mids: Dict[str, float]) -> int:
        value = self.cash
        for instrument_id, quantity in self.positions.items():
            value += int(round(quantity * final_mids.get(instrument_id, 0)))
        return value - STARTING_CASH


def ticker(instrument_id: str) -> str:
    return instrument_id.split("-", 1)[1]


def exchange(instrument_id: str) -> str:
    return instrument_id.split("-", 1)[0]


def instrument_id(exchange_name: str, ticker_name: str) -> str:
    return f"{exchange_name}-{ticker_name}"


def unit_prices(levels: Sequence[Tuple[int, int]], limit: Optional[int] = None) -> List[int]:
    out: List[int] = []
    for price, qty in levels:
        take = qty if limit is None else min(qty, max(0, limit - len(out)))
        if take <= 0:
            break
        out.extend([price] * take)
        if limit is not None and len(out) >= limit:
            break
    return out


def limit_price_for_quantity(levels: Sequence[Tuple[int, int]], quantity: int) -> Optional[int]:
    seen = 0
    last = None
    for price, qty in levels:
        seen += qty
        last = price
        if seen >= quantity:
            return price
    return last if seen > 0 else None


def special_target(
    *,
    mid: float,
    current_position: int,
    anchor: int = 10_000,
    enter: int = 500,
    exit: int = 100,
) -> int:
    if mid <= anchor - enter:
        return LONG_LIMIT
    if mid >= anchor + enter:
        return SHORT_LIMIT
    if abs(mid - anchor) <= exit:
        return 0
    return current_position


def find_local_basket_arbs(
    exchange_name: str,
    books: Dict[str, Book],
    accounts: Dict[str, SimAccount],
    *,
    min_edge: int = 25,
    max_baskets: int = 400,
) -> List[Opportunity]:
    account = accounts[exchange_name]
    opportunities: List[Opportunity] = []
    for etf, components in ETF_BASKETS.items():
        etf_id = instrument_id(exchange_name, etf)
        component_ids = [instrument_id(exchange_name, name) for name in components]
        if etf_id not in books or any(component_id not in books for component_id in component_ids):
            continue

        n = len(components)
        etf_book = books[etf_id]

        # Cheap ETF: buy n ETF shares and sell one share of each component.
        max_count = min(
            len(unit_prices(etf_book.asks)) // n,
            *[len(unit_prices(books[component_id].bids)) for component_id in component_ids],
            (account.long_limit - account.positions[etf_id]) // n,
            *[account.sell_capacity(component_id) for component_id in component_ids],
            max_baskets,
        )
        best = _best_cheap_basket_prefix(etf_book, [books[i] for i in component_ids], n, max_count)
        if best and best[1] > min_edge:
            count, edge = best
            etf_qty = count * n
            etf_price = limit_price_for_quantity(etf_book.asks, etf_qty)
            if etf_price is not None:
                legs = [OrderLeg(exchange_name, etf_id, "bid", etf_qty, etf_price, f"cheap:{etf}")]
                for component_id in component_ids:
                    price = limit_price_for_quantity(books[component_id].bids, count)
                    if price is not None:
                        legs.append(OrderLeg(exchange_name, component_id, "ask", count, price, f"cheap:{etf}"))
                if len(legs) == len(component_ids) + 1:
                    opportunities.append(Opportunity(f"cheap:{etf}", edge, legs, priority=90))

        # Rich ETF: sell n ETF shares and buy one share of each component.
        max_count = min(
            len(unit_prices(etf_book.bids)) // n,
            *[len(unit_prices(books[component_id].asks)) for component_id in component_ids],
            account.sell_capacity(etf_id) // n,
            *[account.long_limit - account.positions[component_id] for component_id in component_ids],
            max_baskets,
        )
        best = _best_rich_basket_prefix(etf_book, [books[i] for i in component_ids], n, max_count)
        if best and best[1] > min_edge:
            count, edge = best
            etf_qty = count * n
            etf_price = limit_price_for_quantity(etf_book.bids, etf_qty)
            if etf_price is not None:
                legs = [OrderLeg(exchange_name, etf_id, "ask", etf_qty, etf_price, f"rich:{etf}")]
                for component_id in component_ids:
                    price = limit_price_for_quantity(books[component_id].asks, count)
                    if price is not None:
                        legs.append(OrderLeg(exchange_name, component_id, "bid", count, price, f"rich:{etf}"))
                if len(legs) == len(component_ids) + 1:
                    opportunities.append(Opportunity(f"rich:{etf}", edge, legs, priority=90))

    opportunities.sort(key=lambda item: (item.priority, item.edge_cents), reverse=True)
    return opportunities


def _best_cheap_basket_prefix(
    etf_book: Book,
    component_books: Sequence[Book],
    component_count: int,
    max_count: int,
) -> Optional[Tuple[int, int]]:
    if max_count <= 0:
        return None
    etf_asks = unit_prices(etf_book.asks, max_count * component_count)
    component_bids = [unit_prices(book.bids, max_count) for book in component_books]
    best_count = 0
    best_edge = 0
    edge = 0
    for count in range(1, max_count + 1):
        etf_cost = sum(etf_asks[(count - 1) * component_count : count * component_count])
        component_revenue = sum(prices[count - 1] for prices in component_bids)
        edge += component_revenue - etf_cost
        if edge > best_edge:
            best_count = count
            best_edge = edge
    return (best_count, best_edge) if best_count else None


def _best_rich_basket_prefix(
    etf_book: Book,
    component_books: Sequence[Book],
    component_count: int,
    max_count: int,
) -> Optional[Tuple[int, int]]:
    if max_count <= 0:
        return None
    etf_bids = unit_prices(etf_book.bids, max_count * component_count)
    component_asks = [unit_prices(book.asks, max_count) for book in component_books]
    best_count = 0
    best_edge = 0
    edge = 0
    for count in range(1, max_count + 1):
        etf_revenue = sum(etf_bids[(count - 1) * component_count : count * component_count])
        component_cost = sum(prices[count - 1] for prices in component_asks)
        edge += etf_revenue - component_cost
        if edge > best_edge:
            best_count = count
            best_edge = edge
    return (best_count, best_edge) if best_count else None


def find_cross_venue_arbs(
    states: Dict[str, Dict[str, Book]],
    accounts: Dict[str, SimAccount],
    *,
    venues: Sequence[str],
    min_edge: int = 20,
    max_quantity: int = 300,
) -> List[Opportunity]:
    selected = set(venues)
    by_ticker: Dict[str, List[Tuple[str, str, Book]]] = defaultdict(list)
    for venue, books in states.items():
        if venue not in selected:
            continue
        for inst, book in books.items():
            if book.best_bid is not None and book.best_ask is not None:
                by_ticker[ticker(inst)].append((venue, inst, book))

    opportunities: List[Opportunity] = []
    for ticker_name, rows in by_ticker.items():
        for buy_venue, buy_inst, buy_book in rows:
            if buy_book.best_ask is None:
                continue
            for sell_venue, sell_inst, sell_book in rows:
                if buy_venue == sell_venue or sell_book.best_bid is None:
                    continue
                per_share = sell_book.best_bid - buy_book.best_ask
                if per_share <= min_edge:
                    continue
                buy_account = accounts[buy_venue]
                sell_account = accounts[sell_venue]
                quantity = min(
                    max_quantity,
                    sum(q for _, q in buy_book.asks),
                    sum(q for _, q in sell_book.bids),
                    buy_account.buy_capacity(buy_inst, buy_book.best_ask),
                    sell_account.sell_capacity(sell_inst),
                )
                if quantity <= 0:
                    continue
                buy_price = limit_price_for_quantity(buy_book.asks, quantity)
                sell_price = limit_price_for_quantity(sell_book.bids, quantity)
                if buy_price is None or sell_price is None:
                    continue
                total_edge = (sell_price - buy_price) * quantity
                if total_edge <= min_edge:
                    continue
                label = f"cross:{ticker_name}:{buy_venue}->{sell_venue}"
                opportunities.append(
                    Opportunity(
                        label,
                        total_edge,
                        [
                            OrderLeg(buy_venue, buy_inst, "bid", quantity, buy_price, label),
                            OrderLeg(sell_venue, sell_inst, "ask", quantity, sell_price, label),
                        ],
                        priority=80,
                    )
                )
    opportunities.sort(key=lambda item: (item.priority, item.edge_cents), reverse=True)
    return opportunities


def build_target_opportunity(
    exchange_name: str,
    inst: str,
    book: Book,
    account: SimAccount,
    target: int,
    label: str,
    priority: int,
    max_quantity: int = 350,
) -> Optional[Opportunity]:
    current = account.positions[inst]
    if target == current:
        return None
    if target > current and book.best_ask is not None:
        quantity = min(max_quantity, target - current, account.buy_capacity(inst, book.best_ask))
        price = limit_price_for_quantity(book.asks, quantity)
        side = "bid"
    elif target < current and book.best_bid is not None:
        quantity = min(max_quantity, current - target, account.sell_capacity(inst))
        price = limit_price_for_quantity(book.bids, quantity)
        side = "ask"
    else:
        return None
    if quantity <= 0 or price is None:
        return None
    edge = int(abs(target - current))
    return Opportunity(label, edge, [OrderLeg(exchange_name, inst, side, quantity, price, label)], priority=priority)


def visible_quantity_at_limit(book: Book, side: str, price: int) -> int:
    if side == "bid":
        return sum(quantity for ask_price, quantity in book.asks if ask_price <= price)
    if side == "ask":
        return sum(quantity for bid_price, quantity in book.bids if bid_price >= price)
    return 0


def safe_leg_quantity(leg: OrderLeg, states: Dict[str, Dict[str, Book]], accounts: Dict[str, SimAccount]) -> int:
    if leg.quantity <= 0 or leg.price <= 0:
        return 0
    account = accounts.get(leg.exchange)
    book = states.get(leg.exchange, {}).get(leg.instrument_id)
    if account is None or book is None:
        return 0
    visible = visible_quantity_at_limit(book, leg.side, leg.price)
    if leg.side == "bid":
        cash_capacity = (account.cash - account.cash_floor) // leg.price
        position_capacity = account.long_limit - account.positions[leg.instrument_id]
        return max(0, min(leg.quantity, visible, cash_capacity, position_capacity))
    if leg.side == "ask":
        return max(0, min(leg.quantity, visible, account.sell_capacity(leg.instrument_id)))
    return 0


def clip_opportunity_for_live(
    opportunity: Opportunity,
    states: Dict[str, Dict[str, Book]],
    accounts: Dict[str, SimAccount],
) -> Optional[Opportunity]:
    if not opportunity.legs:
        return None

    safe_quantities = [safe_leg_quantity(leg, states, accounts) for leg in opportunity.legs]
    if any(quantity <= 0 for quantity in safe_quantities):
        return None

    if len(opportunity.legs) == 1:
        leg = opportunity.legs[0]
        quantity = min(leg.quantity, safe_quantities[0])
        if quantity <= 0:
            return None
        return Opportunity(
            opportunity.label,
            opportunity.edge_cents,
            [OrderLeg(leg.exchange, leg.instrument_id, leg.side, quantity, leg.price, leg.label)],
            opportunity.priority,
        )

    base_units = _quantity_gcd([leg.quantity for leg in opportunity.legs])
    if base_units <= 0:
        return None
    ratios = [leg.quantity // base_units for leg in opportunity.legs]
    clipped_units = min(safe // ratio for safe, ratio in zip(safe_quantities, ratios))
    if clipped_units <= 0:
        return None

    clipped_legs = [
        OrderLeg(leg.exchange, leg.instrument_id, leg.side, ratio * clipped_units, leg.price, leg.label)
        for leg, ratio in zip(opportunity.legs, ratios)
    ]
    clipped_edge = opportunity.edge_cents * clipped_units // base_units
    return Opportunity(opportunity.label, clipped_edge, clipped_legs, opportunity.priority)


def _quantity_gcd(values: Sequence[int]) -> int:
    result = 0
    for value in values:
        value = abs(value)
        while value:
            result, value = value, result % value
    return result


def median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("median requires values")
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


class TokenBucket:
    def __init__(self, rate_per_second: int = 450) -> None:
        self.rate = rate_per_second
        self.tokens = float(rate_per_second)
        self.updated = time.monotonic()

    def take(self, count: int = 1) -> bool:
        self._refill()
        if self.tokens >= count:
            self.tokens -= count
            return True
        return False

    def can_take(self, count: int = 1) -> bool:
        self._refill()
        return self.tokens >= count

    def _refill(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.rate, self.tokens + (now - self.updated) * self.rate)
        self.updated = now


@dataclass
class StrategyConfig:
    venues: Tuple[str, ...]
    cross_edge: int = 10
    basket_edge: int = 20
    special_enter: int = 500
    special_exit: int = 160
    momentum_lookback_ms: int = 10_000
    momentum_enter: int = 120
    hammer_after_ms: int = 570_000
    max_orders_per_pulse: int = 24
    enable_cross: bool = True
    enable_basket: bool = True
    enable_special: bool = False
    enable_momentum: bool = False


class EdgeHammerEngine:
    def __init__(self, config: StrategyConfig) -> None:
        self.config = config
        self.accounts: Dict[str, SimAccount] = {venue: SimAccount() for venue in config.venues}
        self.states: Dict[str, Dict[str, Book]] = {venue: {} for venue in config.venues}
        self.history: Dict[str, Deque[Tuple[int, float]]] = defaultdict(deque)
        self.clients: Dict[str, "ExchangeClient"] = {}
        self._fire_lock = asyncio.Lock()

    def update_books(self, venue: str, books: Dict[str, Book], server_time: int) -> None:
        self.states[venue] = books
        for inst, book in books.items():
            if book.mid is None:
                continue
            hist = self.history[inst]
            hist.append((server_time, book.mid))
            cutoff = server_time - self.config.momentum_lookback_ms - 1_000
            while hist and hist[0][0] < cutoff:
                hist.popleft()

    def opportunities(self, server_time: int) -> List[Opportunity]:
        opportunities: List[Opportunity] = []
        if self.config.enable_basket:
            for venue in self.config.venues:
                opportunities.extend(
                    find_local_basket_arbs(
                        venue,
                        self.states.get(venue, {}),
                        self.accounts,
                        min_edge=self.config.basket_edge,
                    )
                )
        if self.config.enable_cross:
            opportunities.extend(
                find_cross_venue_arbs(
                    self.states,
                    self.accounts,
                    venues=self.config.venues,
                    min_edge=self.config.cross_edge,
                )
            )
        if self.config.enable_special:
            opportunities.extend(self._special_opportunities())
        if self.config.enable_momentum:
            opportunities.extend(self._momentum_opportunities(server_time))
        opportunities.sort(key=lambda item: (item.priority, item.edge_cents), reverse=True)
        return opportunities[: self.config.max_orders_per_pulse]

    def _special_opportunities(self) -> List[Opportunity]:
        opportunities: List[Opportunity] = []
        anchors = self._global_anchors()
        for venue, books in self.states.items():
            account = self.accounts[venue]
            for special in SPECIAL_TICKERS:
                inst = instrument_id(venue, special)
                book = books.get(inst)
                if book is None or book.mid is None:
                    continue
                anchor = int(anchors.get(special, 10_000))
                target = special_target(
                    mid=book.mid,
                    current_position=account.positions[inst],
                    anchor=anchor,
                    enter=self.config.special_enter,
                    exit=self.config.special_exit,
                )
                opp = build_target_opportunity(venue, inst, book, account, target, f"special:{special}", 65)
                if opp:
                    opportunities.append(opp)
        return opportunities

    def _momentum_opportunities(self, server_time: int) -> List[Opportunity]:
        opportunities: List[Opportunity] = []
        aggressive = server_time >= self.config.hammer_after_ms
        for venue, books in self.states.items():
            account = self.accounts[venue]
            for inst, book in books.items():
                if book.mid is None or book.spread is None or book.spread > 80:
                    continue
                past = self._past_mid(inst, server_time - self.config.momentum_lookback_ms)
                if past is None:
                    continue
                impulse = book.mid - past
                threshold = self.config.momentum_enter
                if abs(impulse) < threshold:
                    continue
                target = LONG_LIMIT if impulse > 0 else SHORT_LIMIT
                max_quantity = 600 if aggressive else 250
                label = "hammer" if aggressive else "momentum"
                opp = build_target_opportunity(venue, inst, book, account, target, label, 55, max_quantity)
                if opp:
                    opp.edge_cents = int(abs(impulse) * opp.legs[0].quantity)
                    opportunities.append(opp)
        return opportunities

    def _past_mid(self, inst: str, cutoff: int) -> Optional[float]:
        value = None
        for ts, mid in self.history.get(inst, ()):
            if ts <= cutoff:
                value = mid
            else:
                break
        return value

    def _global_anchors(self) -> Dict[str, float]:
        anchors: Dict[str, float] = {}
        for special in SPECIAL_TICKERS:
            values = []
            for venue, books in self.states.items():
                book = books.get(instrument_id(venue, special))
                if book and book.mid is not None:
                    values.append(book.mid)
            if values:
                # Blend the structural $100 anchor with cross-venue median so a
                # single manipulated venue does not drag every other venue.
                anchors[special] = (10_000 + median(values)) / 2
        return anchors

    def apply_optimistic_leg(self, leg: OrderLeg) -> int:
        book = self.states.get(leg.exchange, {}).get(leg.instrument_id)
        if book is None:
            return 0
        return self.accounts[leg.exchange].apply_ioc(leg.instrument_id, leg.side, book, leg.quantity)

    async def fire_live(self, server_time: int) -> None:
        async with self._fire_lock:
            for opp in self.opportunities(server_time):
                clipped = clip_opportunity_for_live(opp, self.states, self.accounts)
                if clipped is None:
                    continue
                required_tokens = Counter(leg.exchange for leg in clipped.legs)
                if any(
                    (client := self.clients.get(venue)) is None
                    or client.ws is None
                    or not client.inventory_ready
                    or not client.bucket.can_take(count)
                    for venue, count in required_tokens.items()
                ):
                    continue
                sent_legs = []
                for leg in clipped.legs:
                    client = self.clients[leg.exchange]
                    if await client.send_ioc(leg, leg.quantity):
                        sent_legs.append(leg)
                if len(sent_legs) != len(clipped.legs):
                    continue
                for leg in sent_legs:
                    self.apply_optimistic_leg(leg)


class ExchangeClient:
    def __init__(self, venue: str, engine: EdgeHammerEngine, host: Optional[str] = None) -> None:
        self.venue = venue
        self.engine = engine
        self.host = host or HOSTS[venue]
        self.url = f"ws://{self.host}:9001/trade"
        self.bucket = TokenBucket()
        self.ws = None
        self.request_id = 0
        self.last_inventory_request = 0.0
        self.server_time = 0
        self.inventory_ready = False

    async def run(self) -> None:
        if ws_connect is None:
            raise RuntimeError("websockets is not installed")
        backoff = 0.25
        while True:
            try:
                async with ws_connect(self.url, compression=None, max_size=16 * 1024 * 1024) as ws:
                    self.ws = ws
                    self.inventory_ready = False
                    backoff = 0.25
                    await ws.recv()
                    await self.request_inventory(force=True)
                    async for raw in ws:
                        await self.handle_message(raw)
            except Exception as exc:
                print(f"[{self.venue}] reconnect after {backoff:.2f}s: {exc}", flush=True)
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 1.6)

    async def handle_message(self, raw: str) -> None:
        if raw == "Message rate limit exceeded":
            print(f"[{self.venue}] exchange closed for rate limit", flush=True)
            return
        msg = json.loads(raw)
        kind = msg.get("type")
        if kind == "market_data_update":
            self.server_time = int(msg.get("time", 0))
            books = parse_live_books(msg.get("orderbook_depths", {}))
            self.engine.update_books(self.venue, books, self.server_time)
            await self.maybe_request_inventory()
            if self.inventory_ready:
                await self.fire()
        elif kind == "get_inventory_response":
            self.apply_inventory(msg.get("data", {}))
        elif kind == "add_order_response" and not msg.get("success", False):
            data = msg.get("data") or {}
            message = str(data.get("message"))
            print(f"[{self.venue}] order failed: {message}", flush=True)
            lowered = message.lower()
            if "insufficient" in lowered or "limit" in lowered:
                self.inventory_ready = False
                await self.request_inventory(force=True)
        elif kind == "end_of_round":
            print(f"[{self.venue}] segment ended", flush=True)

    async def maybe_request_inventory(self) -> None:
        await self.request_inventory(force=False)

    async def request_inventory(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_inventory_request < 1.0:
            return
        self.last_inventory_request = now
        await self.send({"type": "get_inventory", "user_request_id": self.next_id("inv")})

    def apply_inventory(self, data: Dict[str, List[int]]) -> None:
        account = self.engine.accounts[self.venue]
        prefix = f"{self.venue}-"
        for key in list(account.positions.keys()):
            if key.startswith(prefix):
                del account.positions[key]
        for key, pair in data.items():
            if not isinstance(pair, list) or len(pair) != 2:
                continue
            total = int(pair[1])
            if key == "$":
                account.cash = total
            else:
                account.positions[key] = total
        self.inventory_ready = True

    async def fire(self) -> None:
        await self.engine.fire_live(self.server_time)

    async def send_ioc(self, leg: OrderLeg, quantity: int) -> bool:
        return await self.send(
            {
                "type": "add_order",
                "user_request_id": self.next_id("ioc"),
                "instrument_id": leg.instrument_id,
                "price": int(leg.price),
                "expiry": int(time.time() * 1000) + 2_000,
                "side": leg.side,
                "quantity": int(quantity),
                "order_type": "ioc",
            }
        )

    async def send(self, payload: Dict[str, object]) -> bool:
        if self.ws is None:
            return False
        if not self.bucket.take():
            return False
        await self.ws.send(json.dumps(payload, separators=(",", ":")))
        return True

    def next_id(self, prefix: str) -> str:
        self.request_id += 1
        return f"{self.venue}-{prefix}-{self.request_id}"


def parse_live_books(depths: Dict[str, Dict[str, Dict[str, int]]]) -> Dict[str, Book]:
    books = {}
    for inst, depth in depths.items():
        bids = [(int(price), int(qty)) for price, qty in (depth.get("bids") or {}).items()]
        asks = [(int(price), int(qty)) for price, qty in (depth.get("asks") or {}).items()]
        books[inst] = Book(bids=bids, asks=asks)
    return books


def parse_csv_book(row: Dict[str, str]) -> Optional[Book]:
    bids = []
    asks = []
    try:
        for index in range(1, 4):
            bid_price = row.get(f"bid{index}_price", "")
            bid_qty = row.get(f"bid{index}_qty", "")
            ask_price = row.get(f"ask{index}_price", "")
            ask_qty = row.get(f"ask{index}_qty", "")
            if bid_price and bid_qty:
                bids.append((int(bid_price), int(bid_qty)))
            if ask_price and ask_qty:
                asks.append((int(ask_price), int(ask_qty)))
    except ValueError:
        return None
    if not bids or not asks:
        return None
    return Book(bids=bids, asks=asks)


def load_csv_updates(data_dir: Path) -> Tuple[Dict[int, Dict[Tuple[str, str], Book]], Dict[str, Dict[str, float]]]:
    updates: Dict[int, Dict[Tuple[str, str], Book]] = defaultdict(dict)
    final_mids: Dict[str, Dict[str, float]] = defaultdict(dict)
    for path in sorted(data_dir.glob("*_orderbooks.csv")):
        venue = path.name.split("_", 1)[0]
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                book = parse_csv_book(row)
                if book is None or book.mid is None:
                    continue
                bucket = (int(row["time"]) // 100) * 100
                inst = row["instrument"]
                updates[bucket][(venue, inst)] = book
                final_mids[venue][inst] = book.mid
    return updates, final_mids


def backtest(data_dir: Path, config: StrategyConfig) -> Dict[str, object]:
    updates, final_mids = load_csv_updates(data_dir)
    engine = EdgeHammerEngine(config)
    for bucket in sorted(updates):
        changed_by_venue: Dict[str, Dict[str, Book]] = defaultdict(dict)
        for (venue, inst), book in updates[bucket].items():
            if venue in config.venues:
                changed_by_venue[venue][inst] = book
        for venue, changed in changed_by_venue.items():
            merged = dict(engine.states.get(venue, {}))
            merged.update(changed)
            engine.update_books(venue, merged, bucket)
        for opp in engine.opportunities(bucket):
            for leg in opp.legs:
                engine.apply_optimistic_leg(leg)

    exchange_pnl = {}
    total = 0
    for venue in config.venues:
        pnl = engine.accounts[venue].mark_to_market(final_mids.get(venue, {}))
        exchange_pnl[venue] = pnl
        total += pnl
    positions = {
        venue: {inst: qty for inst, qty in account.positions.items() if qty}
        for venue, account in engine.accounts.items()
    }
    return {
        "total_pnl_cents": total,
        "total_pnl_dollars": round(total / 100, 2),
        "exchange_pnl_dollars": {venue: round(pnl / 100, 2) for venue, pnl in exchange_pnl.items()},
        "positions": positions,
    }


def backtest_cross_sweep(data_dir: Path, config: StrategyConfig, max_quantity: int = 300) -> Dict[str, object]:
    """Backtest the highest-confidence edge: same-ticker crossed venues.

    This mode consumes each visible book snapshot at most once per 100 ms bucket,
    which is closer to IOC reality than repeatedly assuming the same CSV depth is
    still there after earlier simulated orders.
    """
    updates, final_mids = load_csv_updates(data_dir)
    accounts = {venue: SimAccount() for venue in config.venues}
    state: Dict[Tuple[str, str], Book] = {}
    trades = 0
    gross_edge = 0

    for bucket in sorted(updates):
        for (venue, inst), book in updates[bucket].items():
            if venue in accounts:
                state[(venue, inst)] = book

        available = {
            key: Book(bids=list(book.bids), asks=list(book.asks))
            for key, book in state.items()
            if key[0] in accounts
        }

        by_ticker: Dict[str, List[Tuple[str, str, Book]]] = defaultdict(list)
        for (venue, inst), book in available.items():
            if book.best_bid is not None and book.best_ask is not None:
                by_ticker[ticker(inst)].append((venue, inst, book))

        opportunities = []
        for ticker_name, rows in by_ticker.items():
            for buy_venue, buy_inst, buy_book in rows:
                if buy_book.best_ask is None:
                    continue
                for sell_venue, sell_inst, sell_book in rows:
                    if buy_venue == sell_venue or sell_book.best_bid is None:
                        continue
                    spread = sell_book.best_bid - buy_book.best_ask
                    if spread > config.cross_edge:
                        opportunities.append((spread, ticker_name, buy_venue, buy_inst, sell_venue, sell_inst))
        opportunities.sort(reverse=True)

        for _spread, _ticker_name, buy_venue, buy_inst, sell_venue, sell_inst in opportunities:
            buy_book = available.get((buy_venue, buy_inst))
            sell_book = available.get((sell_venue, sell_inst))
            if buy_book is None or sell_book is None or buy_book.best_ask is None or sell_book.best_bid is None:
                continue
            if sell_book.best_bid - buy_book.best_ask <= config.cross_edge:
                continue

            buy_account = accounts[buy_venue]
            sell_account = accounts[sell_venue]
            max_fill = min(
                max_quantity,
                sum(q for _, q in buy_book.asks),
                sum(q for _, q in sell_book.bids),
                buy_account.buy_capacity(buy_inst, buy_book.best_ask),
                sell_account.sell_capacity(sell_inst),
            )
            if max_fill <= 0:
                continue

            quantity, edge = _best_cross_prefix(buy_book, sell_book, max_fill, buy_account, buy_inst)
            if quantity <= 0 or edge <= config.cross_edge:
                continue

            buy_filled, buy_cash = _consume_asks(buy_book, quantity, buy_account, buy_inst)
            sell_filled, sell_cash = _consume_bids(sell_book, quantity, sell_account, sell_inst)
            filled = min(buy_filled, sell_filled)
            if filled <= 0:
                continue
            # _best_cross_prefix and capacities keep these equal in normal use.
            if buy_filled != sell_filled:
                raise RuntimeError("cross sweep leg mismatch in backtest")
            trades += 1
            gross_edge += sell_cash - buy_cash

    exchange_pnl = {}
    total = 0
    positions = {}
    for venue, account in accounts.items():
        pnl = account.mark_to_market(final_mids.get(venue, {}))
        exchange_pnl[venue] = pnl
        total += pnl
        positions[venue] = {inst: qty for inst, qty in account.positions.items() if qty}

    return {
        "mode": "cross",
        "trades": trades,
        "gross_edge_dollars": round(gross_edge / 100, 2),
        "total_pnl_cents": total,
        "total_pnl_dollars": round(total / 100, 2),
        "exchange_pnl_dollars": {venue: round(pnl / 100, 2) for venue, pnl in exchange_pnl.items()},
        "positions": positions,
    }


def _best_cross_prefix(
    buy_book: Book,
    sell_book: Book,
    max_fill: int,
    buy_account: SimAccount,
    buy_inst: str,
) -> Tuple[int, int]:
    asks = unit_prices(buy_book.asks, max_fill)
    bids = unit_prices(sell_book.bids, max_fill)
    best_quantity = 0
    best_edge = 0
    edge = 0
    cost = 0
    for index, (ask, bid) in enumerate(zip(asks, bids), start=1):
        if buy_account.cash - cost - ask < buy_account.cash_floor:
            break
        if buy_account.positions[buy_inst] + index > buy_account.long_limit:
            break
        cost += ask
        edge += bid - ask
        if edge > best_edge:
            best_quantity = index
            best_edge = edge
    return best_quantity, best_edge


def _consume_asks(book: Book, quantity: int, account: SimAccount, inst: str) -> Tuple[int, int]:
    filled = 0
    cash = 0
    new_asks = []
    remaining = quantity
    for index, (price, level_qty) in enumerate(book.asks):
        take = min(level_qty, remaining)
        affordable = (account.cash - cash - account.cash_floor) // price
        take = min(take, affordable)
        if take > 0:
            filled += take
            cash += take * price
            remaining -= take
        leftover = level_qty - take
        if leftover > 0:
            new_asks.append((price, leftover))
        if remaining <= 0:
            new_asks.extend(book.asks[index + 1 :])
            break
    book.asks = new_asks
    account.cash -= cash
    account.positions[inst] += filled
    return filled, cash


def _consume_bids(book: Book, quantity: int, account: SimAccount, inst: str) -> Tuple[int, int]:
    filled = 0
    cash = 0
    new_bids = []
    remaining = min(quantity, account.sell_capacity(inst))
    for index, (price, level_qty) in enumerate(book.bids):
        take = min(level_qty, remaining)
        if take > 0:
            filled += take
            cash += take * price
            remaining -= take
        leftover = level_qty - take
        if leftover > 0:
            new_bids.append((price, leftover))
        if remaining <= 0:
            new_bids.extend(book.bids[index + 1 :])
            break
    book.bids = new_bids
    account.cash += cash
    account.positions[inst] -= filled
    return filled, cash


def config_from_env() -> StrategyConfig:
    location = os.environ.get("ALGO_LOCATION", "ZSE").strip()
    venues_raw = os.environ.get("ALGO_VENUES", "")
    if venues_raw:
        venues = tuple(item.strip() for item in venues_raw.split(",") if item.strip())
    else:
        venues = CLOSE_CLUSTERS.get(location, CLOSE_CLUSTERS["ZSE"])
    return StrategyConfig(
        venues=venues,
        cross_edge=int(os.environ.get("CROSS_EDGE", "10")),
        basket_edge=int(os.environ.get("BASKET_EDGE", "20")),
        special_enter=int(os.environ.get("SPECIAL_ENTER", "500")),
        special_exit=int(os.environ.get("SPECIAL_EXIT", "160")),
        momentum_lookback_ms=int(os.environ.get("MOM_LOOKBACK_MS", "10000")),
        momentum_enter=int(os.environ.get("MOM_ENTER", "120")),
        hammer_after_ms=int(os.environ.get("HAMMER_AFTER_MS", "570000")),
        max_orders_per_pulse=int(os.environ.get("MAX_ORDERS_PER_PULSE", "24")),
        enable_cross=os.environ.get("ENABLE_CROSS", "1") != "0",
        enable_basket=os.environ.get("ENABLE_BASKET", "1") != "0",
        enable_special=os.environ.get("ENABLE_SPECIAL", "0") == "1",
        enable_momentum=os.environ.get("ENABLE_MOMENTUM", "0") == "1",
    )


async def run_live(config: StrategyConfig) -> None:
    engine = EdgeHammerEngine(config)
    clients = [ExchangeClient(venue, engine) for venue in config.venues]
    engine.clients = {client.venue: client for client in clients}
    print(f"edgehammer venues={','.join(config.venues)}", flush=True)
    await asyncio.gather(*(client.run() for client in clients))


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggressive AlgoTrade IOC bot and backtester")
    parser.add_argument("--backtest", type=Path, help="Path to market_data directory")
    parser.add_argument("--venues", help="Comma-separated venue list or cluster name: NYSE, ZSE, HKEX, ALL")
    parser.add_argument("--mode", choices=("cross", "combined"), default="cross", help="Backtest mode")
    args = parser.parse_args()

    config = config_from_env()
    if args.venues:
        config.venues = CLOSE_CLUSTERS.get(args.venues, tuple(item.strip() for item in args.venues.split(",")))

    if args.backtest:
        if args.mode == "cross":
            result = backtest_cross_sweep(args.backtest, config)
        else:
            result = backtest(args.backtest, config)
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    asyncio.run(run_live(config))


if __name__ == "__main__":
    main()
