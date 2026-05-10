#!/usr/bin/env python3
"""
Voidmaker: an aggressive passive-landmine AlgoTrade bot.

The core edge is deliberately simple:
  - Rest tiny bids at the legal floor (1 cent) and asks at the legal ceiling
    (999999 cents), plus optional wide ladders.
  - If an oversized market/IOC order sweeps into those levels, immediately
    unwind against the market maker on the same exchange.

This file is standalone and does not import any existing bots in the repo.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal

Side = Literal["bid", "ask"]

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

INSTRUMENTS_BY_EXCHANGE = {
    "Euronext": ["CARD", "DDJH", "ETFA", "ETFSH", "GOLD", "INA", "JNAF", "JZRO", "KOTD", "KRAS", "NGUP", "OIT", "SIMP", "XAG", "ZITO"],
    "HKEX": ["CARD", "DLKV", "ETFA", "ETFB", "ETFB3", "FSR", "INA", "JNAF", "KOTD", "MDKA", "OIT", "SIMP", "XFR"],
    "JPX": ["CARD", "ETFSH", "GOLD", "HT", "JNAF", "KTST", "SIMP", "XAG"],
    "LSE": ["CARD", "DDJH", "DLKV", "ETFB", "FSR", "HT", "JZRO", "KOTD", "MDKA", "OIT", "SIMP", "XAG", "ZABA", "ZITO"],
    "NASDAQ": ["CARD", "DLKV", "ETFB", "ETFB3", "FSR", "GOLD", "HT", "INA", "KOTD", "NGUP", "SIMP", "ZITO"],
    "NSE": ["CARD", "DLKV", "OIT", "SIMP", "ZABA", "ZITO"],
    "NYSE": ["CARD", "DDJH", "ETFA", "ETFA3", "INA", "JNAF", "JZRO", "KRAS", "KTST", "MDKA", "NGUP", "SIMP", "XFR", "ZABA"],
    "SSE": ["CARD", "FSR", "HT", "KRAS", "SIMP", "ZABA"],
    "TMX": ["CARD", "DDJH", "ETFA3", "GOLD", "HT", "JZRO", "KRAS", "KTST", "MDKA", "NGUP", "SIMP", "XFR", "ZABA"],
    "ZSE": ["CARD", "DDJH", "DLKV", "ETFA", "ETFA3", "ETFB", "ETFB3", "ETFSH", "FSR", "GOLD", "HT", "INA", "JNAF", "JZRO", "KOTD", "KRAS", "KTST", "MDKA", "NGUP", "OIT", "SIMP", "XAG", "XFR", "ZABA", "ZITO"],
}

LATENCY_RTT_MS = {
    "NYSE": {"NYSE": 0, "NASDAQ": 1, "SSE": 165, "JPX": 152, "Euronext": 84, "LSE": 80, "HKEX": 180, "NSE": 174, "TMX": 11, "ZSE": 96},
    "NASDAQ": {"NYSE": 1, "NASDAQ": 0, "SSE": 165, "JPX": 152, "Euronext": 84, "LSE": 80, "HKEX": 180, "NSE": 174, "TMX": 11, "ZSE": 96},
    "SSE": {"NYSE": 165, "NASDAQ": 165, "SSE": 0, "JPX": 18, "Euronext": 160, "LSE": 156, "HKEX": 19, "NSE": 54, "TMX": 159, "ZSE": 145},
    "JPX": {"NYSE": 152, "NASDAQ": 152, "SSE": 18, "JPX": 0, "Euronext": 145, "LSE": 141, "HKEX": 37, "NSE": 53, "TMX": 145, "ZSE": 140},
    "Euronext": {"NYSE": 84, "NASDAQ": 84, "SSE": 160, "JPX": 145, "Euronext": 0, "LSE": 6, "HKEX": 130, "NSE": 130, "TMX": 86, "ZSE": 22},
    "LSE": {"NYSE": 80, "NASDAQ": 80, "SSE": 156, "JPX": 141, "Euronext": 6, "LSE": 0, "HKEX": 135, "NSE": 134, "TMX": 82, "ZSE": 24},
    "HKEX": {"NYSE": 180, "NASDAQ": 180, "SSE": 19, "JPX": 37, "Euronext": 130, "LSE": 135, "HKEX": 0, "NSE": 53, "TMX": 174, "ZSE": 150},
    "NSE": {"NYSE": 174, "NASDAQ": 174, "SSE": 54, "JPX": 53, "Euronext": 130, "LSE": 134, "HKEX": 53, "NSE": 0, "TMX": 174, "ZSE": 95},
    "TMX": {"NYSE": 11, "NASDAQ": 11, "SSE": 159, "JPX": 145, "Euronext": 86, "LSE": 82, "HKEX": 174, "NSE": 174, "TMX": 0, "ZSE": 98},
    "ZSE": {"NYSE": 96, "NASDAQ": 96, "SSE": 145, "JPX": 140, "Euronext": 22, "LSE": 24, "HKEX": 150, "NSE": 95, "TMX": 98, "ZSE": 0},
}


@dataclass(frozen=True)
class BookTop:
    best_bid: int | None
    best_ask: int | None
    best_bid_qty: int = 0
    best_ask_qty: int = 0


@dataclass(frozen=True)
class PlannedOrder:
    exchange: str
    ticker: str
    side: Side
    price: int
    quantity: int
    order_type: Literal["limit", "ioc"] = "limit"

    @property
    def instrument_id(self) -> str:
        return f"{self.exchange}-{self.ticker}"


@dataclass(frozen=True)
class LandmineFill:
    exchange: str
    ticker: str
    side: Side
    price: int
    quantity: int


@dataclass(frozen=True)
class ArbOpportunity:
    ticker: str
    buy_exchange: str
    sell_exchange: str
    buy_price: int
    sell_price: int
    quantity: int

    @property
    def spread(self) -> int:
        return self.sell_price - self.buy_price

    @property
    def edge_cents(self) -> int:
        return self.spread * self.quantity


@dataclass(frozen=True)
class LandmineConfig:
    extreme_qty: int = 10
    wide_qty: int = 5
    bid_ladder: tuple[int, ...] = (1, 2, 3, 4, 5, 5_000)
    ask_ladder: tuple[int, ...] = (20_000, 30_000, 50_000, 250_000, 999_999)
    reseed_interval_s: float = 4.0
    inventory_interval_s: float = 1.0
    pending_interval_s: float = 5.0
    liquidation_ioc_qty: int = 100
    max_cover_price: int = 100_000
    min_exit_price: int = 100
    max_msgs_per_second: int = 450
    arb_min_spread_cents: int = 25
    arb_clip_qty: int = 25
    arb_interval_s: float = 0.05
    arb_cooldown_s: float = 0.15
    arb_max_per_cycle: int = 8
    allow_short: bool = False
    min_cash: int = -5_000_000
    cash_buffer: int = 100_000
    short_floor: int = -200
    expiry_ms: int = 20 * 60 * 1000
    request_timeout_ms: int = 5_000


def select_close_exchanges(location: str, max_rtt_ms: int) -> list[str]:
    if location not in LATENCY_RTT_MS:
        raise ValueError(f"unknown location {location!r}; expected one of {sorted(LATENCY_RTT_MS)}")
    row = LATENCY_RTT_MS[location]
    return [exchange for exchange, _ in sorted(row.items(), key=lambda item: (item[1], EXCHANGES.index(item[0]))) if row[exchange] <= max_rtt_ms]


def _unique_sorted(values: Iterable[int], reverse: bool = False) -> list[int]:
    return sorted(set(values), reverse=reverse)


def build_landmine_orders(exchange: str, ticker: str, book: BookTop, cfg: LandmineConfig) -> list[PlannedOrder]:
    orders: list[PlannedOrder] = []

    for price in _unique_sorted(cfg.bid_ladder, reverse=True):
        if price <= 0:
            continue
        if book.best_ask is not None and price >= book.best_ask:
            continue
        qty = cfg.extreme_qty if price == min(cfg.bid_ladder) else cfg.wide_qty
        orders.append(PlannedOrder(exchange, ticker, "bid", price, qty))

    for price in _unique_sorted(cfg.ask_ladder):
        if price >= 1_000_000:
            continue
        if book.best_bid is not None and price <= book.best_bid:
            continue
        qty = cfg.extreme_qty if price == max(cfg.ask_ladder) else cfg.wide_qty
        orders.append(PlannedOrder(exchange, ticker, "ask", price, qty))

    return orders


def estimate_landmine_profit_cents(
    fills: Iterable[LandmineFill],
    conservative_cover_cents: int = 10_000,
) -> int:
    profit = 0
    for fill in fills:
        if fill.side == "ask":
            profit += (fill.price - conservative_cover_cents) * fill.quantity
        else:
            profit += (conservative_cover_cents - fill.price) * fill.quantity
    return profit


def find_cross_venue_arbs(
    books_by_exchange: dict[str, dict[str, BookTop]],
    exchanges: Iterable[str],
    min_spread_cents: int,
    max_qty: int,
) -> list[ArbOpportunity]:
    selected = [exchange for exchange in exchanges if exchange in books_by_exchange]
    tickers = sorted({ticker for exchange in selected for ticker in books_by_exchange.get(exchange, {})})
    opportunities: list[ArbOpportunity] = []

    for ticker in tickers:
        venues = [exchange for exchange in selected if ticker in books_by_exchange.get(exchange, {})]
        for buy_exchange in venues:
            buy_book = books_by_exchange[buy_exchange][ticker]
            if buy_book.best_ask is None or buy_book.best_ask_qty <= 0:
                continue
            for sell_exchange in venues:
                if sell_exchange == buy_exchange:
                    continue
                sell_book = books_by_exchange[sell_exchange][ticker]
                if sell_book.best_bid is None or sell_book.best_bid_qty <= 0:
                    continue
                spread = sell_book.best_bid - buy_book.best_ask
                if spread < min_spread_cents:
                    continue
                qty = min(max_qty, buy_book.best_ask_qty, sell_book.best_bid_qty)
                if qty <= 0:
                    continue
                opportunities.append(
                    ArbOpportunity(
                        ticker=ticker,
                        buy_exchange=buy_exchange,
                        sell_exchange=sell_exchange,
                        buy_price=buy_book.best_ask,
                        sell_price=sell_book.best_bid,
                        quantity=qty,
                    )
                )

    opportunities.sort(key=lambda opp: (opp.edge_cents, opp.spread, opp.quantity), reverse=True)
    return opportunities


def unprotected_position(actual: int, protected: int) -> int:
    if actual == 0 or protected == 0 or (actual > 0) != (protected > 0):
        return actual
    if abs(actual) <= abs(protected):
        return 0
    return actual - protected


def available_cash_to_spend(cash_total: int, cash_reserved: int, min_cash: int, cash_buffer: int) -> int:
    return max(0, cash_total - cash_reserved - min_cash - cash_buffer)


def available_inventory_to_sell(
    position_total: int,
    position_reserved: int,
    allow_short: bool,
    short_floor: int = -200,
) -> int:
    if allow_short:
        return max(0, position_total - position_reserved - short_floor)
    return max(0, position_total - position_reserved)


def can_submit_order(
    order: PlannedOrder,
    cash_total: int,
    cash_reserved: int,
    position_total: int,
    position_reserved: int,
    allow_short: bool,
    min_cash: int = -5_000_000,
    cash_buffer: int = 100_000,
    short_floor: int = -200,
) -> bool:
    if order.side == "bid":
        if order.price <= 0:
            return False
        spendable = available_cash_to_spend(cash_total, cash_reserved, min_cash, cash_buffer)
        return order.price * order.quantity <= spendable
    sellable = available_inventory_to_sell(position_total, position_reserved, allow_short, short_floor)
    return order.quantity <= sellable


class TokenBucket:
    def __init__(self, rate_per_second: int) -> None:
        self.rate = float(rate_per_second)
        self.capacity = float(rate_per_second)
        self.tokens = float(rate_per_second)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def take(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait_s = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(wait_s)


@dataclass
class ExchangeState:
    books: dict[str, BookTop] = field(default_factory=dict)
    positions: dict[str, int] = field(default_factory=dict)
    position_reserved: dict[str, int] = field(default_factory=dict)
    arb_inventory: dict[str, int] = field(default_factory=dict)
    cash: int = 10_000_000
    cash_reserved: int = 0
    active_order_keys: set[tuple[str, Side, int]] = field(default_factory=set)
    request_keys: dict[str, tuple[str, Side, int]] = field(default_factory=dict)
    request_tags: dict[str, str] = field(default_factory=dict)
    last_seed_s: float = 0.0
    last_inventory_s: float = 0.0
    last_pending_s: float = 0.0
    server_time_ms: int = 0


class ExchangeClient:
    def __init__(self, exchange: str, cfg: LandmineConfig, active: bool = True) -> None:
        self.exchange = exchange
        self.cfg = cfg
        self.active = active
        self.url = f"ws://{HOSTS[exchange]}:9001/trade"
        self.state = ExchangeState()
        self.bucket = TokenBucket(cfg.max_msgs_per_second)
        self.seq = 0
        self.ws: Any = None
        self.stopped = asyncio.Event()

    def next_id(self, prefix: str) -> str:
        self.seq += 1
        return f"vm-{self.exchange}-{prefix}-{self.seq}-{int(time.time() * 1000)}"

    async def send(self, payload: dict[str, Any]) -> None:
        if self.ws is None:
            return
        await self.bucket.take()
        await self.ws.send(json.dumps(payload, separators=(",", ":")))

    async def run_forever(self) -> None:
        from websockets.asyncio.client import connect as ws_connect

        backoff = 0.25
        while not self.stopped.is_set():
            try:
                async with ws_connect(self.url, max_size=16 * 1024 * 1024) as ws:
                    self.ws = ws
                    backoff = 0.25
                    print(f"[{self.exchange}] connected {self.url}", flush=True)
                    await self.handle_messages()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{self.exchange}] disconnected: {exc!r}; retrying in {backoff:.1f}s", flush=True)
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 1.8)
            finally:
                self.ws = None

    async def handle_messages(self) -> None:
        async for raw in self.ws:
            if isinstance(raw, bytes):
                raw = raw.decode()
            if raw == "Message rate limit exceeded":
                raise RuntimeError(raw)
            msg = json.loads(raw)
            msg_type = msg.get("type")
            if msg_type == "welcome":
                await self.request_inventory()
                await self.request_pending()
            elif msg_type == "market_data_update":
                await self.on_market_data(msg)
            elif msg_type == "add_order_response":
                self.on_add_order_response(msg)
            elif msg_type == "get_inventory_response":
                self.on_inventory(msg)
            elif msg_type == "get_pending_orders_response":
                self.on_pending(msg)
            elif msg_type == "end_of_round":
                print(f"[{self.exchange}] end_of_round", flush=True)
                break
            elif msg_type == "error":
                print(f"[{self.exchange}] error {msg}", flush=True)

    async def on_market_data(self, msg: dict[str, Any]) -> None:
        self.state.server_time_ms = int(msg.get("time") or 0)
        depths = msg.get("orderbook_depths") or {}
        for instrument_id, depth in depths.items():
            bid_items = [(int(price), int(qty)) for price, qty in (depth.get("bids") or {}).items()]
            ask_items = [(int(price), int(qty)) for price, qty in (depth.get("asks") or {}).items()]
            best_bid = max(bid_items, default=(None, 0), key=lambda item: -1 if item[0] is None else item[0])
            best_ask = min(ask_items, default=(None, 0), key=lambda item: 1_000_001 if item[0] is None else item[0])
            self.state.books[instrument_id] = BookTop(
                best_bid=best_bid[0],
                best_ask=best_ask[0],
                best_bid_qty=best_bid[1],
                best_ask_qty=best_ask[1],
            )

        now = time.monotonic()
        if now - self.state.last_inventory_s >= self.cfg.inventory_interval_s:
            self.state.last_inventory_s = now
            await self.request_inventory()
        if now - self.state.last_pending_s >= self.cfg.pending_interval_s:
            self.state.last_pending_s = now
            await self.request_pending()
        if now - self.state.last_seed_s >= self.cfg.reseed_interval_s:
            self.state.last_seed_s = now
            await self.seed_landmines()

        if self.active:
            await self.liquidate_inventory()

    async def request_inventory(self) -> None:
        await self.send({"type": "get_inventory", "user_request_id": self.next_id("inv")})

    async def request_pending(self) -> None:
        await self.send({"type": "get_pending_orders", "user_request_id": self.next_id("pending")})

    def on_inventory(self, msg: dict[str, Any]) -> None:
        data = msg.get("data") or {}
        cash = data.get("$")
        if isinstance(cash, list) and len(cash) >= 2:
            self.state.cash_reserved = int(cash[0])
            self.state.cash = int(cash[1])
        for instrument_id, pair in data.items():
            if instrument_id == "$" or not isinstance(pair, list) or len(pair) < 2:
                continue
            self.state.position_reserved[instrument_id] = int(pair[0])
            self.state.positions[instrument_id] = int(pair[1])

    def on_pending(self, msg: dict[str, Any]) -> None:
        active: set[tuple[str, Side, int]] = set()
        data = msg.get("data") or {}
        for instrument_id, sides in data.items():
            if not isinstance(sides, list) or len(sides) != 2:
                continue
            for side_name, orders in (("bid", sides[0]), ("ask", sides[1])):
                for order in orders or []:
                    if order.get("live", True):
                        active.add((instrument_id, side_name, int(order["price"])))
        self.state.active_order_keys = active

    def on_add_order_response(self, msg: dict[str, Any]) -> None:
        req_id = msg.get("user_request_id", "")
        tag = self.state.request_tags.pop(req_id, "")
        key = self.state.request_keys.pop(req_id, None)
        if msg.get("success") and key is not None and tag == "landmine":
            self.state.active_order_keys.add(key)
        if msg.get("success") and tag == "arb":
            data = msg.get("data") or {}
            change = data.get("immediate_inventory_change")
            if change and key is not None:
                instrument_id = key[0]
                self.state.arb_inventory[instrument_id] = self.state.arb_inventory.get(instrument_id, 0) + int(change)
        if not msg.get("success"):
            data = msg.get("data") or {}
            message = data.get("message") or msg.get("message")
            if message:
                print(f"[{self.exchange}] add rejected: {message}", flush=True)

    def can_submit(self, order: PlannedOrder) -> bool:
        return can_submit_order(
            order,
            cash_total=self.state.cash,
            cash_reserved=self.state.cash_reserved,
            position_total=self.state.positions.get(order.instrument_id, 0),
            position_reserved=self.state.position_reserved.get(order.instrument_id, 0),
            allow_short=self.cfg.allow_short,
            min_cash=self.cfg.min_cash,
            cash_buffer=self.cfg.cash_buffer,
            short_floor=self.cfg.short_floor,
        )

    async def add_order(self, order: PlannedOrder, expiry_ms: int | None = None, tag: str = "") -> None:
        if not self.can_submit(order):
            return
        req_id = self.next_id("add")
        key = (order.instrument_id, order.side, order.price)
        self.state.request_tags[req_id] = tag
        self.state.request_keys[req_id] = key
        payload: dict[str, Any] = {
            "type": "add_order",
            "user_request_id": req_id,
            "instrument_id": order.instrument_id,
            "side": order.side,
            "quantity": order.quantity,
            "order_type": order.order_type,
        }
        if order.order_type in ("limit", "ioc"):
            payload["price"] = order.price
            payload["expiry"] = expiry_ms or (int(time.time() * 1000) + self.cfg.expiry_ms)
        await self.send(payload)

    async def seed_landmines(self) -> None:
        for ticker in INSTRUMENTS_BY_EXCHANGE[self.exchange]:
            instrument_id = f"{self.exchange}-{ticker}"
            book = self.state.books.get(instrument_id, BookTop(None, None))
            for order in build_landmine_orders(self.exchange, ticker, book, self.cfg):
                key = (order.instrument_id, order.side, order.price)
                if key in self.state.active_order_keys or key in self.state.request_keys.values():
                    continue
                await self.add_order(order, tag="landmine")

    async def liquidate_inventory(self) -> None:
        for instrument_id, pos in list(self.state.positions.items()):
            if not instrument_id.startswith(f"{self.exchange}-") or pos == 0:
                continue
            pos = unprotected_position(pos, self.state.arb_inventory.get(instrument_id, 0))
            if pos == 0:
                continue
            book = self.state.books.get(instrument_id)
            if book is None:
                continue
            ticker = instrument_id.split("-", 1)[1]
            if pos > 0 and book.best_bid is not None and book.best_bid >= self.cfg.min_exit_price:
                sellable = available_inventory_to_sell(
                    self.state.positions.get(instrument_id, 0),
                    self.state.position_reserved.get(instrument_id, 0),
                    self.cfg.allow_short,
                    self.cfg.short_floor,
                )
                qty = min(pos, sellable, self.cfg.liquidation_ioc_qty, max(1, book.best_bid_qty))
                if qty <= 0:
                    continue
                await self.add_order(
                    PlannedOrder(self.exchange, ticker, "ask", book.best_bid, qty, "ioc"),
                    expiry_ms=int(time.time() * 1000) + self.cfg.request_timeout_ms,
                )
            elif pos < 0 and book.best_ask is not None and book.best_ask <= self.cfg.max_cover_price:
                qty = min(-pos, self.cfg.liquidation_ioc_qty, max(1, book.best_ask_qty))
                await self.add_order(
                    PlannedOrder(self.exchange, ticker, "bid", book.best_ask, qty, "ioc"),
                    expiry_ms=int(time.time() * 1000) + self.cfg.request_timeout_ms,
                )


class MarketHub:
    def __init__(self, clients: dict[str, ExchangeClient], cfg: LandmineConfig) -> None:
        self.clients = clients
        self.cfg = cfg
        self.last_sent: dict[tuple[str, str, str], float] = {}

    def books_by_exchange(self) -> dict[str, dict[str, BookTop]]:
        books: dict[str, dict[str, BookTop]] = {}
        for exchange, client in self.clients.items():
            per_ticker: dict[str, BookTop] = {}
            for instrument_id, book in client.state.books.items():
                if not instrument_id.startswith(f"{exchange}-"):
                    continue
                per_ticker[instrument_id.split("-", 1)[1]] = book
            books[exchange] = per_ticker
        return books

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.arb_interval_s)
            await self.scan_once()

    async def scan_once(self) -> None:
        books = self.books_by_exchange()
        opportunities = find_cross_venue_arbs(
            books,
            self.clients.keys(),
            min_spread_cents=self.cfg.arb_min_spread_cents,
            max_qty=self.cfg.arb_clip_qty,
        )
        now = time.monotonic()
        sent = 0
        used_exchanges: set[str] = set()
        used_tickers: set[str] = set()
        for opp in opportunities:
            key = (opp.ticker, opp.buy_exchange, opp.sell_exchange)
            if now - self.last_sent.get(key, 0.0) < self.cfg.arb_cooldown_s:
                continue
            if opp.ticker in used_tickers:
                continue
            if opp.buy_exchange in used_exchanges or opp.sell_exchange in used_exchanges:
                continue
            buy_client = self.clients[opp.buy_exchange]
            sell_client = self.clients[opp.sell_exchange]
            buy_order = PlannedOrder(opp.buy_exchange, opp.ticker, "bid", opp.buy_price, opp.quantity, "ioc")
            sell_order = PlannedOrder(opp.sell_exchange, opp.ticker, "ask", opp.sell_price, opp.quantity, "ioc")
            if not buy_client.can_submit(buy_order) or not sell_client.can_submit(sell_order):
                continue
            await asyncio.gather(
                buy_client.add_order(
                    buy_order,
                    expiry_ms=int(time.time() * 1000) + self.cfg.request_timeout_ms,
                    tag="arb",
                ),
                sell_client.add_order(
                    sell_order,
                    expiry_ms=int(time.time() * 1000) + self.cfg.request_timeout_ms,
                    tag="arb",
                ),
            )
            self.last_sent[key] = now
            used_exchanges.update((opp.buy_exchange, opp.sell_exchange))
            used_tickers.add(opp.ticker)
            sent += 1
            if sent >= self.cfg.arb_max_per_cycle:
                break


def config_from_env(args: argparse.Namespace) -> LandmineConfig:
    def int_env(name: str, default: int) -> int:
        return int(os.environ.get(name, default))

    return LandmineConfig(
        extreme_qty=int_env("VOID_EXTREME_QTY", args.extreme_qty),
        wide_qty=int_env("VOID_WIDE_QTY", args.wide_qty),
        max_msgs_per_second=int_env("VOID_MSG_RATE", args.msg_rate),
        arb_min_spread_cents=int_env("VOID_ARB_MIN_SPREAD", args.arb_min_spread),
        arb_clip_qty=int_env("VOID_ARB_CLIP_QTY", args.arb_clip_qty),
        allow_short=bool(int(os.environ.get("VOID_ALLOW_SHORT", "1" if args.allow_short else "0"))),
        cash_buffer=int_env("VOID_CASH_BUFFER", args.cash_buffer),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggressive AlgoTrade passive-landmine bot")
    parser.add_argument("--location", default=os.environ.get("BOT_LOCATION", "ZSE"), choices=EXCHANGES)
    parser.add_argument("--max-rtt-ms", type=int, default=int(os.environ.get("BOT_MAX_RTT_MS", "60")))
    parser.add_argument("--all-exchanges", action="store_true", default=os.environ.get("BOT_ALL_EXCHANGES") == "1")
    parser.add_argument("--extreme-qty", type=int, default=10)
    parser.add_argument("--wide-qty", type=int, default=5)
    parser.add_argument("--msg-rate", type=int, default=450)
    parser.add_argument("--arb-min-spread", type=int, default=25)
    parser.add_argument("--arb-clip-qty", type=int, default=25)
    parser.add_argument("--cash-buffer", type=int, default=100_000)
    parser.add_argument("--allow-short", action="store_true", default=os.environ.get("VOID_ALLOW_SHORT") == "1")
    parser.add_argument("--no-arb", action="store_true", default=os.environ.get("VOID_NO_ARB") == "1")
    parser.add_argument("--dry-plan", action="store_true", help="print selected exchanges and planned instruments, then exit")
    return parser.parse_args()


async def amain() -> None:
    args = parse_args()
    cfg = config_from_env(args)
    exchanges = EXCHANGES[:] if args.all_exchanges else select_close_exchanges(args.location, args.max_rtt_ms)
    if not exchanges:
        raise SystemExit("no exchanges selected")

    print(f"location={args.location} max_rtt={args.max_rtt_ms} exchanges={','.join(exchanges)}", flush=True)
    print(f"landmine ladders bids={cfg.bid_ladder} asks={cfg.ask_ladder} qty extreme={cfg.extreme_qty} wide={cfg.wide_qty}", flush=True)
    if args.dry_plan:
        for exchange in exchanges:
            print(f"{exchange}: {','.join(INSTRUMENTS_BY_EXCHANGE[exchange])}")
        return

    clients = {exchange: ExchangeClient(exchange, cfg, active=True) for exchange in exchanges}
    tasks = [asyncio.create_task(client.run_forever()) for client in clients.values()]
    if not args.no_arb and len(clients) > 1:
        tasks.append(asyncio.create_task(MarketHub(clients, cfg).run()))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    await stop.wait()
    for client in clients.values():
        client.stopped.set()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
