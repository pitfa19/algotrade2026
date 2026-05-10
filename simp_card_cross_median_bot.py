#!/usr/bin/env python3
"""Aggressive SIMP/CARD edge bot for AlgoTrade 2026.

Fresh implementation: it uses only the public websocket protocol and local
market-data conclusions. Orders are IOC taker orders; no spoofing, no resting
layering, and no dependency on any reference bot framework.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable


LOG = logging.getLogger("simp_card_edge")

EXCHANGES: tuple[str, ...] = (
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
)

HOSTS: dict[str, str] = {
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

CLOSE_TARGETS: dict[str, tuple[str, ...]] = {
    "NYSE": ("NYSE", "NASDAQ", "TMX"),
    "ZSE": ("ZSE", "Euronext", "LSE"),
    "HKEX": ("HKEX", "SSE", "JPX", "NSE"),
}

SIMP_THRESHOLDS: dict[str, int] = {
    "NYSE": 3,
    "ZSE": 5,
    "HKEX": 10,
}

CARD_THRESHOLDS: dict[str, int] = {
    "NYSE": 100,
    "ZSE": 125,
    "HKEX": 175,
}

STARTING_CASH = 10_000_000
CASH_FLOOR = -5_000_000
DEFAULT_MAX_LONG = 1_800
DEFAULT_MIN_SHORT = -200
SEGMENT_MS = 600_000
ROUND_MS = 1_800_000

WsConnect = Callable[..., Awaitable[Any]]
_WS_CONNECT: WsConnect | None = None


def websocket_connect() -> WsConnect:
    global _WS_CONNECT
    if _WS_CONNECT is not None:
        return _WS_CONNECT
    try:
        from websockets.asyncio.client import connect as connect
    except ImportError:  # pragma: no cover - compatibility with older websockets
        from websockets import connect as connect  # type: ignore
    _WS_CONNECT = connect
    return connect


def median_int(values: Iterable[int]) -> int:
    """Integer median, preserving doubled-cent arithmetic when possible."""

    ordered = sorted(values)
    if not ordered:
        raise ValueError("median_int() needs at least one value")
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) // 2


def ceil_div(a: int, b: int) -> int:
    return -(-a // b)


@dataclass(frozen=True)
class Book:
    exchange: str
    ticker: str
    server_time_ms: int
    bids: list[tuple[int, int]]
    asks: list[tuple[int, int]]
    received_at: float

    @property
    def instrument_id(self) -> str:
        return f"{self.exchange}-{self.ticker}"

    @property
    def mid2(self) -> int:
        if not self.bids or not self.asks:
            raise ValueError(f"{self.instrument_id} has an empty book side")
        return self.bids[0][0] + self.asks[0][0]

    def bid_quantity_at_or_above(self, limit_price: int) -> int:
        return sum(qty for price, qty in self.bids if price >= limit_price)

    def ask_quantity_at_or_below(self, limit_price: int) -> int:
        return sum(qty for price, qty in self.asks if price <= limit_price)

    def worst_bid_for_visible_flatten(self) -> int | None:
        return self.bids[-1][0] if self.bids else None

    def worst_ask_for_visible_flatten(self) -> int | None:
        return self.asks[-1][0] if self.asks else None


@dataclass
class Inventory:
    cash: int = STARTING_CASH
    positions: dict[str, int] = field(default_factory=dict)

    def position(self, instrument_id: str) -> int:
        return self.positions.get(instrument_id, 0)

    def apply_fill(self, instrument_id: str, qty_delta: int, cash_delta: int) -> None:
        self.positions[instrument_id] = self.position(instrument_id) + qty_delta
        self.cash += cash_delta


@dataclass(frozen=True)
class OrderIntent:
    exchange: str
    instrument_id: str
    ticker: str
    side: str
    price: int
    quantity: int
    reason: str
    server_time_ms: int


@dataclass(frozen=True)
class EdgeConfig:
    home: str = "ZSE"
    targets: tuple[str, ...] | None = None
    max_long: int = DEFAULT_MAX_LONG
    min_short: int = DEFAULT_MIN_SHORT
    cash_floor: int = CASH_FLOOR
    book_fresh_seconds: float = 0.9
    card_min_fresh: int = 5
    flatten_window_ms: int = 20_000
    segment_ms: int = SEGMENT_MS
    round_ms: int = ROUND_MS
    cooldown_ms: int = 80
    simp_thresholds: dict[str, int] = field(default_factory=lambda: dict(SIMP_THRESHOLDS))
    card_thresholds: dict[str, int] = field(default_factory=lambda: dict(CARD_THRESHOLDS))

    def __post_init__(self) -> None:
        if self.home not in CLOSE_TARGETS:
            raise ValueError(f"home must be one of {sorted(CLOSE_TARGETS)}")
        if self.targets is None:
            object.__setattr__(self, "targets", CLOSE_TARGETS[self.home])
        else:
            invalid = [target for target in self.targets if target not in EXCHANGES]
            if invalid:
                raise ValueError(f"unknown target exchange(s): {invalid}")

    @property
    def target_set(self) -> set[str]:
        return set(self.targets or ())

    def threshold(self, ticker: str) -> int:
        if ticker == "SIMP":
            return self.simp_thresholds.get(self.home, 5)
        if ticker == "CARD":
            return self.card_thresholds.get(self.home, 150)
        raise ValueError(f"unsupported ticker {ticker}")

    def is_flatten_time(self, server_time_ms: int) -> bool:
        if server_time_ms <= 0:
            return False
        segment_pos = server_time_ms % self.segment_ms
        round_left = self.round_ms - server_time_ms
        return (
            segment_pos >= self.segment_ms - self.flatten_window_ms
            or 0 <= round_left <= self.flatten_window_ms
        )


class StrategyCore:
    def __init__(self, config: EdgeConfig):
        self.config = config
        self.books: dict[tuple[str, str], Book] = {}
        self.last_sent_at: dict[tuple[str, str, str], int] = defaultdict(lambda: -10**12)

    def update_book(self, book: Book) -> None:
        if book.bids and book.asks:
            self.books[(book.exchange, book.ticker)] = book

    def fresh_books(self, ticker: str, now: float) -> list[Book]:
        return [
            book
            for (exchange, book_ticker), book in self.books.items()
            if book_ticker == ticker and now - book.received_at <= self.config.book_fresh_seconds
        ]

    def fair2(self, ticker: str, now: float) -> int | None:
        if ticker == "SIMP":
            return 20_000
        if ticker != "CARD":
            return None
        books = self.fresh_books("CARD", now)
        if len(books) < self.config.card_min_fresh:
            return None
        return median_int(book.mid2 for book in books)

    def plan_orders(
        self,
        exchange: str,
        book: Book,
        inventory: Inventory,
        now: float | None = None,
    ) -> list[OrderIntent]:
        if now is None:
            now = time.monotonic()
        if exchange not in self.config.target_set:
            return []
        if book.ticker not in {"SIMP", "CARD"}:
            return []
        fair2 = self.fair2(book.ticker, now)
        if fair2 is None:
            return []

        threshold = self.config.threshold(book.ticker)
        buy_limit = (fair2 - 2 * threshold) // 2
        sell_limit = ceil_div(fair2 + 2 * threshold, 2)
        instrument_id = book.instrument_id
        position = inventory.position(instrument_id)
        orders: list[OrderIntent] = []
        reason = "SIMP_FIXED_FAIR" if book.ticker == "SIMP" else "CARD_CROSS_MEDIAN"

        sell_qty = min(
            book.bid_quantity_at_or_above(sell_limit),
            max(0, position - self.config.min_short),
        )
        if sell_qty > 0 and self._can_send(book, "ask"):
            orders.append(
                OrderIntent(
                    exchange=exchange,
                    instrument_id=instrument_id,
                    ticker=book.ticker,
                    side="ask",
                    price=sell_limit,
                    quantity=sell_qty,
                    reason=reason,
                    server_time_ms=book.server_time_ms,
                )
            )

        cash_room = max(0, inventory.cash - self.config.cash_floor)
        buy_cash_cap = cash_room // max(1, buy_limit)
        buy_qty = min(
            book.ask_quantity_at_or_below(buy_limit),
            max(0, self.config.max_long - position),
            buy_cash_cap,
        )
        if buy_qty > 0 and self._can_send(book, "bid"):
            orders.append(
                OrderIntent(
                    exchange=exchange,
                    instrument_id=instrument_id,
                    ticker=book.ticker,
                    side="bid",
                    price=buy_limit,
                    quantity=buy_qty,
                    reason=reason,
                    server_time_ms=book.server_time_ms,
                )
            )
        return orders

    def plan_flatten(self, exchange: str, book: Book, inventory: Inventory) -> list[OrderIntent]:
        if exchange not in self.config.target_set or book.ticker not in {"SIMP", "CARD"}:
            return []
        position = inventory.position(book.instrument_id)
        if position > 0 and book.bids and self._can_send(book, "ask"):
            visible_qty = sum(qty for _, qty in book.bids)
            return [
                OrderIntent(
                    exchange=exchange,
                    instrument_id=book.instrument_id,
                    ticker=book.ticker,
                    side="ask",
                    price=book.worst_bid_for_visible_flatten() or book.bids[0][0],
                    quantity=min(position, visible_qty),
                    reason="FLATTEN",
                    server_time_ms=book.server_time_ms,
                )
            ]
        if position < 0 and book.asks and self._can_send(book, "bid"):
            visible_qty = sum(qty for _, qty in book.asks)
            return [
                OrderIntent(
                    exchange=exchange,
                    instrument_id=book.instrument_id,
                    ticker=book.ticker,
                    side="bid",
                    price=book.worst_ask_for_visible_flatten() or book.asks[0][0],
                    quantity=min(-position, visible_qty),
                    reason="FLATTEN",
                    server_time_ms=book.server_time_ms,
                )
            ]
        return []

    def mark_sent(self, order: OrderIntent) -> None:
        self.last_sent_at[(order.exchange, order.ticker, order.side)] = order.server_time_ms

    def _can_send(self, book: Book, side: str) -> bool:
        last = self.last_sent_at[(book.exchange, book.ticker, side)]
        return book.server_time_ms - last >= self.config.cooldown_ms


class RateLimiter:
    def __init__(self, rate_per_second: int = 450):
        self.rate_per_second = rate_per_second
        self.tokens = float(rate_per_second)
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.updated_at = now
                self.tokens = min(
                    float(self.rate_per_second),
                    self.tokens + elapsed * self.rate_per_second,
                )
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self.tokens) / self.rate_per_second)


def parse_depth(exchange: str, instrument_id: str, depth: dict[str, Any], server_time_ms: int) -> Book | None:
    try:
        ticker = instrument_id.split("-", 1)[1]
    except IndexError:
        return None

    def parse_side(raw: dict[str, Any], reverse: bool) -> list[tuple[int, int]]:
        levels = []
        for price_s, qty in raw.items():
            price = int(price_s)
            quantity = int(qty)
            if quantity > 0:
                levels.append((price, quantity))
        levels.sort(key=lambda item: item[0], reverse=reverse)
        return levels

    bids = parse_side(depth.get("bids", {}), True)
    asks = parse_side(depth.get("asks", {}), False)
    if not bids or not asks:
        return None
    return Book(
        exchange=exchange,
        ticker=ticker,
        server_time_ms=server_time_ms,
        bids=bids,
        asks=asks,
        received_at=time.monotonic(),
    )


class ExchangeRunner:
    def __init__(
        self,
        exchange: str,
        core: StrategyCore,
        inventory: Inventory,
        dry_run: bool = False,
        rate_per_second: int = 450,
    ):
        self.exchange = exchange
        self.core = core
        self.inventory = inventory
        self.dry_run = dry_run
        self.rate_limiter = RateLimiter(rate_per_second)
        self.request_seq = 0
        self.last_inventory_request = 0.0
        self.pending_instruments: dict[str, str] = {}

    async def run_forever(self) -> None:
        backoff = 0.5
        while True:
            try:
                await self._run_once()
                backoff = 0.5
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("%s connection loop failed: %s", self.exchange, exc)
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 1.5)

    async def _run_once(self) -> None:
        url = f"ws://{HOSTS[self.exchange]}:9001/trade"
        LOG.info("connecting %s", url)
        async with websocket_connect()(url, max_size=16 * 1024 * 1024) as ws:
            raw = await ws.recv()
            LOG.info("%s welcome: %s", self.exchange, raw)
            await self.request_inventory(ws)
            async for raw_message in ws:
                if isinstance(raw_message, bytes):
                    raw_message = raw_message.decode()
                if raw_message == "Message rate limit exceeded":
                    raise RuntimeError(f"{self.exchange} message rate limit exceeded")
                message = json.loads(raw_message)
                await self.handle_message(ws, message)

    async def handle_message(self, ws: Any, message: dict[str, Any]) -> None:
        msg_type = message.get("type")
        if msg_type == "market_data_update":
            await self.handle_market_data(ws, message)
        elif msg_type == "add_order_response":
            self.handle_add_order_response(message)
        elif msg_type == "get_inventory_response":
            self.handle_inventory_response(message)
        elif msg_type == "end_of_round":
            LOG.info("%s end_of_round", self.exchange)
            raise RuntimeError("segment ended")
        elif msg_type == "error":
            LOG.warning("%s server error: %s", self.exchange, message)

    async def handle_market_data(self, ws: Any, message: dict[str, Any]) -> None:
        server_time_ms = int(message.get("time", 0))
        orderbook_depths = message.get("orderbook_depths", {})
        candidate_books: list[Book] = []
        for instrument_id, depth in orderbook_depths.items():
            book = parse_depth(self.exchange, instrument_id, depth, server_time_ms)
            if book is None:
                continue
            self.core.update_book(book)
            if book.exchange == self.exchange and book.ticker in {"SIMP", "CARD"}:
                candidate_books.append(book)

        now = time.monotonic()
        if now - self.last_inventory_request >= 1.0:
            await self.request_inventory(ws)

        for book in candidate_books:
            if self.core.config.is_flatten_time(book.server_time_ms):
                orders = self.core.plan_flatten(self.exchange, book, self.inventory)
            else:
                orders = self.core.plan_orders(self.exchange, book, self.inventory, now)
            for order in orders:
                await self.send_order(ws, order)

    async def send_order(self, ws: Any, order: OrderIntent) -> None:
        if order.quantity <= 0:
            return
        self.core.mark_sent(order)
        payload = {
            "type": "add_order",
            "user_request_id": self.next_request_id(order.reason, order.instrument_id),
            "instrument_id": order.instrument_id,
            "side": order.side,
            "quantity": int(order.quantity),
            "order_type": "ioc",
            "price": int(order.price),
            "expiry": int(time.time() * 1000) + 2_000,
        }
        LOG.info(
            "%s %s %s %s x%d @ %d",
            order.exchange,
            order.reason,
            order.instrument_id,
            order.side,
            order.quantity,
            order.price,
        )
        if self.dry_run:
            return
        await self.rate_limiter.wait()
        await ws.send(json.dumps(payload, separators=(",", ":")))

    async def request_inventory(self, ws: Any) -> None:
        self.last_inventory_request = time.monotonic()
        payload = {
            "type": "get_inventory",
            "user_request_id": self.next_request_id("inventory"),
        }
        if self.dry_run:
            return
        await self.rate_limiter.wait()
        await ws.send(json.dumps(payload, separators=(",", ":")))

    def handle_add_order_response(self, message: dict[str, Any]) -> None:
        if not message.get("success"):
            LOG.debug("%s order rejected: %s", self.exchange, message)
            return
        data = message.get("data") or {}
        qty_delta = data.get("immediate_inventory_change")
        cash_delta = data.get("immediate_balance_change")
        request_id = str(message.get("user_request_id", ""))
        instrument_id = self._instrument_from_request_id(request_id)
        if instrument_id and qty_delta is not None and cash_delta is not None:
            self.inventory.apply_fill(instrument_id, int(qty_delta), int(cash_delta))

    def handle_inventory_response(self, message: dict[str, Any]) -> None:
        data = message.get("data") or {}
        cash_pair = data.get("$")
        if cash_pair:
            self.inventory.cash = int(cash_pair[1])
        for instrument_id, pair in data.items():
            if instrument_id == "$":
                continue
            if isinstance(pair, list) and len(pair) >= 2:
                self.inventory.positions[instrument_id] = int(pair[1])

    def next_request_id(self, reason: str, instrument_id: str | None = None) -> str:
        self.request_seq += 1
        request_id = f"{self.exchange}:{reason}:{self.request_seq}"
        if instrument_id is not None:
            self.pending_instruments[request_id] = instrument_id
        return request_id

    def _instrument_from_request_id(self, request_id: str) -> str | None:
        return self.pending_instruments.pop(request_id, None)


def parse_targets(home: str, raw: str | None) -> tuple[str, ...]:
    if not raw or raw == "close":
        return CLOSE_TARGETS[home]
    if raw == "all":
        return EXCHANGES
    targets = tuple(part.strip() for part in raw.split(",") if part.strip())
    invalid = [target for target in targets if target not in EXCHANGES]
    if invalid:
        raise ValueError(f"unknown target exchange(s): {invalid}")
    return targets


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--home",
        default=os.getenv("ALGO_HOME", "ZSE"),
        choices=sorted(CLOSE_TARGETS),
        help="current team location; controls close targets and CARD/SIMP thresholds",
    )
    parser.add_argument(
        "--targets",
        default=os.getenv("ALGO_TARGETS", "close"),
        help="'close', 'all', or comma-separated exchange names",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.getenv("DRY_RUN", "0") == "1",
        help="connect and log intended orders without sending",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Python logging level",
    )
    return parser


async def amain(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    targets = parse_targets(args.home, args.targets)
    config = EdgeConfig(home=args.home, targets=targets)
    core = StrategyCore(config)
    inventories = {exchange: Inventory() for exchange in EXCHANGES}
    LOG.info("home=%s targets=%s dry_run=%s", args.home, ",".join(targets), args.dry_run)
    runners = [
        ExchangeRunner(exchange, core, inventories[exchange], dry_run=args.dry_run)
        for exchange in EXCHANGES
    ]
    await asyncio.gather(*(runner.run_forever() for runner in runners))


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
