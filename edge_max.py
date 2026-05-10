#!/usr/bin/env python3
"""Aggressive AlgoTrade strategy focused on passive tail traps and stale quotes.

The core objects in this file are intentionally importable without a live
exchange connection. The websocket runner at the bottom is only used when the
file is executed as a script.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple


EXCHANGES = (
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

CLOSE_EXCHANGES = {
    "NYSE": ("NYSE", "NASDAQ", "TMX"),
    "ZSE": ("ZSE", "Euronext", "LSE"),
    "HKEX": ("HKEX", "SSE", "JPX", "NSE"),
}

ETF_BASKETS = {
    "ETFA": ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"),
    "ETFB": ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"),
    "ETFA3": ("NGUP", "KTST", "XFR"),
    "ETFB3": ("KOTD", "INA", "DLKV"),
    "ETFSH": ("GOLD", "XAG"),
}


def split_instrument(instrument_id: str) -> Tuple[str, str]:
    exchange, ticker = instrument_id.split("-", 1)
    return exchange, ticker


@dataclass(frozen=True)
class Book:
    exchange: str
    instrument_id: str
    ticker: str
    time_ms: int
    bids: Tuple[Tuple[int, int], ...]
    asks: Tuple[Tuple[int, int], ...]

    @classmethod
    def from_lists(
        cls,
        exchange: str,
        instrument_id: str,
        time_ms: int,
        bids: Sequence[Tuple[int, int]],
        asks: Sequence[Tuple[int, int]],
    ) -> "Book":
        _, ticker = split_instrument(instrument_id)
        clean_bids = tuple(sorted(((int(p), int(q)) for p, q in bids if q > 0), reverse=True))
        clean_asks = tuple(sorted((int(p), int(q)) for p, q in asks if q > 0))
        return cls(exchange, instrument_id, ticker, int(time_ms), clean_bids, clean_asks)

    @classmethod
    def from_depth(cls, exchange: str, instrument_id: str, time_ms: int, depth: Dict[str, Any]) -> "Book":
        bids = [(int(price), int(qty)) for price, qty in (depth.get("bids") or {}).items()]
        asks = [(int(price), int(qty)) for price, qty in (depth.get("asks") or {}).items()]
        return cls.from_lists(exchange, instrument_id, time_ms, bids, asks)

    @property
    def bid(self) -> Optional[int]:
        return self.bids[0][0] if self.bids else None

    @property
    def ask(self) -> Optional[int]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2.0

    def bid_qty_at_or_above(self, limit_price: int) -> int:
        return sum(qty for price, qty in self.bids if price >= limit_price)

    def ask_qty_at_or_below(self, limit_price: int) -> int:
        return sum(qty for price, qty in self.asks if price <= limit_price)


@dataclass(frozen=True)
class OrderIntent:
    exchange: str
    instrument_id: str
    side: str
    quantity: int
    order_type: str
    reason: str
    price: Optional[int] = None
    expiry_ms: Optional[int] = None


@dataclass
class ActiveOrder:
    order_id: int
    intent: OrderIntent
    remaining: int


@dataclass
class AggressiveConfig:
    location: str = "ZSE"
    trade_all: bool = False
    opening_all_exchanges: bool = True
    max_long: int = 2000
    max_short: int = -200
    starting_cash: int = 10_000_000
    cash_floor: int = -5_000_000
    opening_window_ms: int = 15_000
    passive_expiry_ms: int = 18_000
    trap_refresh_ms: int = 4_000
    min_profit_cents: int = 1
    median_threshold_cents: int = 20
    etf_threshold_cents: int = 30
    max_ioc_per_update: int = 10
    max_orders_per_market_update: int = 45
    opening_bid_price: int = 100
    dynamic_bid_offsets: Tuple[int, ...] = (5000, 3000, 1500)
    dynamic_ask_offsets: Tuple[int, ...] = (5000, 3000, 1500)
    fixed_bid_prices: Tuple[int, ...] = (10_000, 15_000)
    fixed_ask_prices: Tuple[int, ...] = (18_000, 20_000)
    fixed_min_gap_cents: int = 700
    fresh_book_ms: int = 650

    @property
    def target_exchanges(self) -> Tuple[str, ...]:
        if self.trade_all:
            return EXCHANGES
        return CLOSE_EXCHANGES.get(self.location, CLOSE_EXCHANGES["ZSE"])


class TokenBucket:
    def __init__(self, rate_per_sec: int, burst: Optional[int] = None) -> None:
        self.rate_per_sec = rate_per_sec
        self.capacity = burst or rate_per_sec
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()

    async def wait(self) -> None:
        while True:
            now = time.monotonic()
            elapsed = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            await asyncio.sleep((1.0 - self.tokens) / self.rate_per_sec)


class AggressiveEdgeEngine:
    def __init__(self, config: AggressiveConfig) -> None:
        self.config = config
        self.books: Dict[str, Book] = {}
        self.books_by_ticker: Dict[str, Dict[str, Book]] = defaultdict(dict)
        self.positions: Dict[str, int] = defaultdict(int)
        self.cash: Dict[str, int] = defaultdict(lambda: self.config.starting_cash)
        self.long_cost: Dict[str, int] = defaultdict(int)
        self.short_proceeds: Dict[str, int] = defaultdict(int)
        self.active_orders: Dict[int, ActiveOrder] = {}
        self.request_intents: Dict[str, OrderIntent] = {}
        self.last_intent_at: Dict[Tuple[str, str, int, str], int] = {}
        self.sent_log: Deque[str] = deque(maxlen=200)

    def target_exchange(self, exchange: str) -> bool:
        return self.config.trade_all or exchange in self.config.target_exchanges

    def on_book(self, book: Book, server_time_ms: int) -> List[OrderIntent]:
        self.books[book.instrument_id] = book
        self.books_by_ticker[book.ticker][book.exchange] = book
        if not self.target_exchange(book.exchange):
            if self.config.opening_all_exchanges and server_time_ms <= self.config.opening_window_ms:
                return self._opening_bid_trap(book, server_time_ms)
            return []

        intents: List[OrderIntent] = []
        intents.extend(self._position_exits(book))
        intents.extend(self._tail_traps(book, server_time_ms))
        intents.extend(self._cross_median_sweeps(book, server_time_ms))
        intents.extend(self._etf_sweeps(book, server_time_ms))
        return self._limit_iocs(intents)

    def note_request(self, request_id: str, intent: OrderIntent) -> None:
        self.request_intents[request_id] = intent
        self.sent_log.append(f"{request_id} {intent.exchange} {intent.instrument_id} {intent.side} {intent.quantity} {intent.price} {intent.reason}")

    def on_order_response(self, request_id: str, success: bool, data: Dict[str, Any]) -> None:
        intent = self.request_intents.pop(request_id, None)
        if intent is None:
            return
        inv_change = data.get("immediate_inventory_change")
        cash_change = data.get("immediate_balance_change")
        if inv_change and cash_change:
            qty = abs(int(inv_change))
            avg_price = abs(int(cash_change)) // max(1, qty)
            fill_side = "bid" if int(inv_change) > 0 else "ask"
            self._apply_fill(intent.instrument_id, fill_side, avg_price, qty)
        else:
            if inv_change:
                self.positions[intent.instrument_id] += int(inv_change)
            if cash_change:
                self.cash[intent.exchange] += int(cash_change)

        order_id = data.get("order_id")
        if not success or not order_id or intent.order_type != "limit":
            return

        filled = abs(int(inv_change or 0))
        remaining = max(0, intent.quantity - filled)
        if remaining:
            self.active_orders[int(order_id)] = ActiveOrder(int(order_id), intent, remaining)

    def on_inventory(self, exchange: str, inventory: Dict[str, Sequence[int]]) -> None:
        for instrument_id, pair in inventory.items():
            if instrument_id == "$":
                self.cash[exchange] = int(pair[1])
            else:
                if int(pair[1]) == 0:
                    self.long_cost[instrument_id] = 0
                    self.short_proceeds[instrument_id] = 0
                self.positions[instrument_id] = int(pair[1])

    def on_cancel(self, order_id: int) -> None:
        self.active_orders.pop(order_id, None)

    def clamp_intent(self, intent: OrderIntent) -> Optional[OrderIntent]:
        price = int(intent.price or 0)
        if intent.side == "bid":
            if intent.reason.startswith("exit-short"):
                qty = min(intent.quantity, max(0, -self.positions[intent.instrument_id]))
            else:
                qty = min(intent.quantity, self._long_room(intent.instrument_id))
            if price > 0:
                qty = self._affordable_bid_qty(intent.exchange, price, qty)
        else:
            if intent.reason.startswith("exit-long"):
                qty = min(intent.quantity, max(0, self.positions[intent.instrument_id]))
            else:
                qty = min(intent.quantity, self._short_room(intent.instrument_id))
        if qty <= 0:
            return None
        if qty == intent.quantity:
            return intent
        return replace(intent, quantity=qty)

    def on_trade_event(
        self,
        instrument_id: str,
        passive_order_id: int,
        quantity: int,
        price: int,
    ) -> List[OrderIntent]:
        active = self.active_orders.get(passive_order_id)
        if active is None:
            return []

        fill_qty = min(quantity, active.remaining)
        active.remaining -= fill_qty
        if active.remaining <= 0:
            self.active_orders.pop(passive_order_id, None)
        self._apply_fill(instrument_id, active.intent.side, price, fill_qty)
        return self.unwind_after_fill(instrument_id, active.intent.side, price, fill_qty)

    def unwind_after_fill(self, instrument_id: str, filled_side: str, fill_price: int, quantity: int) -> List[OrderIntent]:
        book = self.books.get(instrument_id)
        if book is None or quantity <= 0:
            return []
        if filled_side == "bid":
            if not book.bids:
                return []
            limit = fill_price + self.config.min_profit_cents
            if book.bid is None or book.bid < limit:
                return []
            return [
                OrderIntent(
                    exchange=book.exchange,
                    instrument_id=instrument_id,
                    side="ask",
                    quantity=quantity,
                    order_type="ioc",
                    price=limit,
                    reason=f"unwind-long-from-{fill_price}",
                )
            ]
        if not book.asks:
            return []
        limit = fill_price - self.config.min_profit_cents
        if limit <= 0 or book.ask is None or book.ask > limit:
            return []
        return [
            OrderIntent(
                exchange=book.exchange,
                instrument_id=instrument_id,
                side="bid",
                quantity=quantity,
                order_type="ioc",
                price=limit,
                reason=f"unwind-short-from-{fill_price}",
            )
        ]

    def _apply_fill(self, instrument_id: str, side: str, price: int, quantity: int) -> None:
        exchange, _ = split_instrument(instrument_id)
        before = self.positions[instrument_id]
        if side == "bid":
            cover_qty = min(quantity, max(0, -before))
            if cover_qty:
                avg_short = self.short_proceeds[instrument_id] // max(1, -before)
                self.short_proceeds[instrument_id] -= avg_short * cover_qty
            new_long = quantity - cover_qty
            if new_long:
                self.long_cost[instrument_id] += price * new_long
            self.positions[instrument_id] += quantity
            self.cash[exchange] -= price * quantity
        else:
            sell_from_long = min(quantity, max(0, before))
            if sell_from_long:
                avg_long = self.long_cost[instrument_id] // max(1, before)
                self.long_cost[instrument_id] -= avg_long * sell_from_long
            new_short = quantity - sell_from_long
            if new_short:
                self.short_proceeds[instrument_id] += price * new_short
            self.positions[instrument_id] -= quantity
            self.cash[exchange] += price * quantity

    def _position_exits(self, book: Book) -> List[OrderIntent]:
        if book.bid is None or book.ask is None:
            return []
        position = self.positions[book.instrument_id]
        if position > 0 and self.long_cost[book.instrument_id] > 0:
            avg_cost = self.long_cost[book.instrument_id] // position
            limit = avg_cost + self.config.min_profit_cents
            if book.bid < limit:
                return []
            qty = min(position, book.bid_qty_at_or_above(limit))
            if qty <= 0:
                return []
            return [
                OrderIntent(
                    book.exchange,
                    book.instrument_id,
                    "ask",
                    qty,
                    "ioc",
                    f"exit-long-avg-{avg_cost}",
                    limit,
                )
            ]
        if position < 0 and self.short_proceeds[book.instrument_id] > 0:
            avg_sale = self.short_proceeds[book.instrument_id] // (-position)
            limit = avg_sale - self.config.min_profit_cents
            if limit <= 0 or book.ask > limit:
                return []
            qty = min(-position, book.ask_qty_at_or_below(limit))
            if qty <= 0:
                return []
            return [
                OrderIntent(
                    book.exchange,
                    book.instrument_id,
                    "bid",
                    qty,
                    "ioc",
                    f"exit-short-avg-{avg_sale}",
                    limit,
                )
            ]
        return []

    def _tail_traps(self, book: Book, server_time_ms: int) -> List[OrderIntent]:
        if book.mid is None or book.bid is None or book.ask is None:
            return []
        intents: List[OrderIntent] = []
        mid = int(book.mid)

        long_room = self._long_room(book.instrument_id)
        if server_time_ms <= self.config.opening_window_ms and long_room > 0:
            opening = self._opening_bid_trap(book, server_time_ms)
            intents.extend(opening)
            long_room -= sum(intent.quantity for intent in opening)

        if long_room > 0 and server_time_ms > self.config.opening_window_ms:
            candidates = []
            for offset in self.config.dynamic_bid_offsets:
                candidates.append(max(self.config.opening_bid_price, mid - offset))
            for price in self.config.fixed_bid_prices:
                if price <= mid - self.config.fixed_min_gap_cents:
                    candidates.append(price)
            for price in sorted(set(candidates)):
                if long_room <= 0:
                    break
                if price <= 0 or price >= book.bid:
                    continue
                if not self._can_send(book, "bid", price, server_time_ms):
                    continue
                desired = min(long_room, 700)
                qty = self._affordable_bid_qty(book.exchange, price, desired)
                if qty <= 0:
                    continue
                intents.append(self._limit(book, "bid", qty, price, f"tail-bid-{price}", server_time_ms))
                long_room -= qty

        short_room = self._short_room(book.instrument_id)
        if short_room > 0:
            candidates = [mid + offset for offset in self.config.dynamic_ask_offsets]
            for price in self.config.fixed_ask_prices:
                if price >= mid + self.config.fixed_min_gap_cents:
                    candidates.append(price)
            for price in sorted(set(candidates), reverse=True):
                if short_room <= 0:
                    break
                if price <= book.ask:
                    continue
                if not self._can_send(book, "ask", price, server_time_ms):
                    continue
                qty = min(short_room, 200)
                intents.append(self._limit(book, "ask", qty, price, f"tail-ask-{price}", server_time_ms))
                short_room -= qty
        return intents

    def _opening_bid_trap(self, book: Book, server_time_ms: int) -> List[OrderIntent]:
        if book.bid is None:
            return []
        long_room = self._long_room(book.instrument_id)
        price = self.config.opening_bid_price
        if long_room <= 0 or price >= book.bid:
            return []
        if not self._can_send(book, "bid", price, server_time_ms):
            return []
        qty = self._affordable_bid_qty(book.exchange, price, min(long_room, self.config.max_long))
        if qty <= 0:
            return []
        return [self._limit(book, "bid", qty, price, "opening-dollar-bid-trap", server_time_ms)]

    def _cross_median_sweeps(self, book: Book, server_time_ms: int) -> List[OrderIntent]:
        fair = self._ticker_fair(book.ticker, server_time_ms)
        if fair is None or book.bid is None or book.ask is None:
            return []
        intents: List[OrderIntent] = []
        buy_limit = int(fair - self.config.median_threshold_cents)
        if book.ask <= buy_limit:
            qty = min(book.ask_qty_at_or_below(buy_limit), self._long_room(book.instrument_id))
            qty = self._affordable_bid_qty(book.exchange, buy_limit, qty)
            if qty > 0:
                intents.append(
                    OrderIntent(
                        book.exchange,
                        book.instrument_id,
                        "bid",
                        qty,
                        "ioc",
                        f"cross-median-buy-fair-{int(fair)}",
                        buy_limit,
                    )
                )

        sell_limit = int(fair + self.config.median_threshold_cents)
        if book.bid >= sell_limit:
            qty = min(book.bid_qty_at_or_above(sell_limit), self._short_room(book.instrument_id))
            if qty > 0:
                intents.append(
                    OrderIntent(
                        book.exchange,
                        book.instrument_id,
                        "ask",
                        qty,
                        "ioc",
                        f"cross-median-sell-fair-{int(fair)}",
                        sell_limit,
                    )
                )
        return intents

    def _etf_sweeps(self, book: Book, server_time_ms: int) -> List[OrderIntent]:
        basket = ETF_BASKETS.get(book.ticker)
        if basket is None or book.bid is None or book.ask is None:
            return []
        mids = []
        for ticker in basket:
            component = self.books.get(f"{book.exchange}-{ticker}")
            if component is None or component.mid is None:
                return []
            if server_time_ms - component.time_ms > self.config.fresh_book_ms:
                return []
            mids.append(component.mid)
        fair = sum(mids) / len(mids)
        intents: List[OrderIntent] = []
        buy_limit = int(fair - self.config.etf_threshold_cents)
        if book.ask <= buy_limit:
            qty = min(book.ask_qty_at_or_below(buy_limit), self._long_room(book.instrument_id))
            qty = self._affordable_bid_qty(book.exchange, buy_limit, qty)
            if qty > 0:
                intents.append(OrderIntent(book.exchange, book.instrument_id, "bid", qty, "ioc", f"etf-buy-fair-{int(fair)}", buy_limit))
        sell_limit = int(fair + self.config.etf_threshold_cents)
        if book.bid >= sell_limit:
            qty = min(book.bid_qty_at_or_above(sell_limit), self._short_room(book.instrument_id))
            if qty > 0:
                intents.append(OrderIntent(book.exchange, book.instrument_id, "ask", qty, "ioc", f"etf-sell-fair-{int(fair)}", sell_limit))
        return intents

    def _ticker_fair(self, ticker: str, server_time_ms: int) -> Optional[float]:
        mids = []
        for other in self.books_by_ticker.get(ticker, {}).values():
            if other.mid is None:
                continue
            if server_time_ms - other.time_ms > self.config.fresh_book_ms:
                continue
            mids.append(other.mid)
        if len(mids) < 2:
            return None
        return statistics.median(mids)

    def _long_room(self, instrument_id: str) -> int:
        active_bid = sum(
            order.remaining
            for order in self.active_orders.values()
            if order.intent.instrument_id == instrument_id and order.intent.side == "bid"
        )
        return max(0, self.config.max_long - self.positions[instrument_id] - active_bid)

    def _short_room(self, instrument_id: str) -> int:
        active_ask = sum(
            order.remaining
            for order in self.active_orders.values()
            if order.intent.instrument_id == instrument_id and order.intent.side == "ask"
        )
        return max(0, self.positions[instrument_id] - self.config.max_short - active_ask)

    def _affordable_bid_qty(self, exchange: str, price: int, desired: int) -> int:
        if desired <= 0 or price <= 0:
            return 0
        reserved_cash = sum(
            order.intent.price * order.remaining
            for order in self.active_orders.values()
            if order.intent.side == "bid"
            and order.intent.price is not None
            and order.intent.exchange == exchange
        )
        spendable = self.cash[exchange] - self.config.cash_floor - reserved_cash
        return max(0, min(desired, spendable // price))

    def _can_send(self, book: Book, side: str, price: int, server_time_ms: int) -> bool:
        key = (book.instrument_id, side, price, "limit")
        last = self.last_intent_at.get(key)
        if last is not None and server_time_ms - last < self.config.trap_refresh_ms:
            return False
        self.last_intent_at[key] = server_time_ms
        return True

    def _limit(self, book: Book, side: str, qty: int, price: int, reason: str, server_time_ms: int) -> OrderIntent:
        return OrderIntent(
            exchange=book.exchange,
            instrument_id=book.instrument_id,
            side=side,
            quantity=qty,
            order_type="limit",
            price=price,
            expiry_ms=server_time_ms + self.config.passive_expiry_ms,
            reason=reason,
        )

    def _limit_iocs(self, intents: List[OrderIntent]) -> List[OrderIntent]:
        ioc_count = 0
        out = []
        for intent in intents:
            if intent.order_type == "ioc":
                ioc_count += 1
                if ioc_count > self.config.max_ioc_per_update:
                    continue
            out.append(intent)
        return out


class ExchangeSession:
    def __init__(self, exchange: str, engine: AggressiveEdgeEngine, dry_run: bool = False, rate: int = 450) -> None:
        self.exchange = exchange
        self.engine = engine
        self.dry_run = dry_run
        self.bucket = TokenBucket(rate)
        self.seq = 0
        self.last_inventory_request = 0.0

    async def run_forever(self) -> None:
        try:
            from websockets.asyncio.client import connect as ws_connect
        except Exception:
            import websockets

            ws_connect = websockets.connect

        url = f"ws://{HOSTS[self.exchange]}:9001/trade"
        backoff = 0.5
        while True:
            try:
                async with ws_connect(url, compression=None, max_size=16 * 1024 * 1024) as ws:
                    print(f"[{self.exchange}] connected {url}", flush=True)
                    backoff = 0.5
                    async for raw in ws:
                        if raw == "Message rate limit exceeded":
                            raise RuntimeError("exchange closed us for message rate")
                        await self._handle_message(ws, raw)
            except Exception as exc:
                print(f"[{self.exchange}] disconnected: {exc}; reconnecting in {backoff:.1f}s", flush=True)
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 1.7)

    async def _handle_message(self, ws: Any, raw: str) -> None:
        msg = json.loads(raw)
        typ = msg.get("type")
        if typ == "market_data_update":
            server_time_ms = int(msg.get("time", 0))
            depths = msg.get("orderbook_depths") or {}
            queued: List[OrderIntent] = []
            for instrument_id, depth in depths.items():
                book = Book.from_depth(self.exchange, instrument_id, server_time_ms, depth)
                queued.extend(self.engine.on_book(book, server_time_ms))
            for event in msg.get("events") or []:
                if event.get("event_type") == "trade":
                    data = event.get("data") or {}
                    queued.extend(
                        self.engine.on_trade_event(
                            data.get("instrumentID", ""),
                            int(data.get("passiveOrderID", 0)),
                            int(data.get("quantity", 0)),
                            int(data.get("price", 0)),
                        )
                    )
                elif event.get("event_type") == "cancel":
                    data = event.get("data") or {}
                    self.engine.on_cancel(int(data.get("orderID", 0)))
            queued = self._prioritize(queued, server_time_ms)
            for intent in queued[: self.engine.config.max_orders_per_market_update]:
                clamped = self.engine.clamp_intent(intent)
                if clamped is not None:
                    await self._send_intent(ws, clamped)
            now = time.monotonic()
            if now - self.last_inventory_request > 2.0:
                self.last_inventory_request = now
                await self._send_json(ws, {"type": "get_inventory", "user_request_id": self._request_id("inv")})
        elif typ == "add_order_response":
            self.engine.on_order_response(
                str(msg.get("user_request_id", "")),
                bool(msg.get("success")),
                msg.get("data") or {},
            )
            if not msg.get("success"):
                print(f"[{self.exchange}] add rejected {msg}", flush=True)
        elif typ == "get_inventory_response":
            self.engine.on_inventory(self.exchange, msg.get("data") or {})
        elif typ == "end_of_round":
            raise RuntimeError("end_of_round")
        elif typ == "welcome":
            return
        elif typ == "error":
            print(f"[{self.exchange}] error {msg}", flush=True)

    async def _send_intent(self, ws: Any, intent: OrderIntent) -> None:
        request_id = self._request_id("ord")
        payload: Dict[str, Any] = {
            "type": "add_order",
            "user_request_id": request_id,
            "instrument_id": intent.instrument_id,
            "side": intent.side,
            "quantity": int(intent.quantity),
            "order_type": intent.order_type,
        }
        if intent.order_type in ("limit", "ioc"):
            payload["price"] = int(intent.price or 0)
            expiry_delta = self.engine.config.passive_expiry_ms if intent.order_type == "limit" else 3_000
            payload["expiry"] = int(time.time() * 1000) + expiry_delta
        if self.dry_run:
            print(f"[DRY {self.exchange}] {payload} {intent.reason}", flush=True)
            return
        self.engine.note_request(request_id, intent)
        await self._send_json(ws, payload)

    async def _send_json(self, ws: Any, payload: Dict[str, Any]) -> None:
        if self.dry_run:
            return
        await self.bucket.wait()
        await ws.send(json.dumps(payload, separators=(",", ":")))

    def _prioritize(self, intents: Sequence[OrderIntent], server_time_ms: int) -> List[OrderIntent]:
        def score(intent: OrderIntent) -> Tuple[int, int]:
            if server_time_ms <= self.engine.config.opening_window_ms and "opening-dollar" in intent.reason:
                return (0, -intent.quantity)
            if intent.order_type == "ioc" and intent.reason.startswith("unwind"):
                return (1, -intent.quantity)
            if intent.order_type == "ioc":
                return (2, -intent.quantity)
            return (3, -intent.quantity)

        return sorted(intents, key=score)

    def _request_id(self, prefix: str) -> str:
        self.seq += 1
        return f"{self.exchange}-{prefix}-{int(time.time() * 1000)}-{self.seq}"


async def run_bot(args: argparse.Namespace) -> None:
    config = AggressiveConfig(location=args.location, trade_all=args.all)
    engine = AggressiveEdgeEngine(config)
    exchanges = tuple(args.exchanges.split(",")) if args.exchanges else EXCHANGES
    print(
        f"edge_max location={config.location} exchanges={','.join(exchanges)} "
        f"opening_bid=${config.opening_bid_price / 100:.2f} trade_all={config.trade_all}",
        flush=True,
    )
    await asyncio.gather(*(ExchangeSession(exchange, engine, args.dry_run, args.rate).run_forever() for exchange in exchanges))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggressive AlgoTrade edge bot")
    parser.add_argument("--location", default=os.environ.get("BOT_LOCATION", "ZSE"), choices=tuple(CLOSE_EXCHANGES))
    parser.add_argument("--all", action="store_true", help="trade every exchange instead of only normal-trading the close latency cluster")
    parser.add_argument("--exchanges", help="comma-separated exchange override, e.g. ZSE,Euronext,LSE")
    parser.add_argument("--dry-run", action="store_true", help="connect and print orders without sending them")
    parser.add_argument("--rate", type=int, default=450, help="per-exchange outgoing message rate cap")
    return parser.parse_args(argv)


if __name__ == "__main__":
    asyncio.run(run_bot(parse_args()))
