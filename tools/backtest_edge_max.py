#!/usr/bin/env python3
"""Cash/position-aware historical backtest for edge_max.py.

This is intentionally conservative in two ways:
- Passive traps only fill when a recorded active-order burst swept through the
  trap price.
- IOC orders only fill against the visible top-3 levels in the CSV snapshot.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import glob
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from edge_max import AggressiveConfig, AggressiveEdgeEngine, Book, EXCHANGES, OrderIntent, split_instrument


@dataclass(frozen=True)
class BacktestBook:
    time_ms: int
    instrument: str
    bids: Tuple[Tuple[int, int], ...]
    asks: Tuple[Tuple[int, int], ...]

    @property
    def exchange(self) -> str:
        exchange, _ = split_instrument(self.instrument)
        return exchange

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

    def to_engine_book(self) -> Book:
        return Book.from_lists(self.exchange, self.instrument, self.time_ms, self.bids, self.asks)


@dataclass
class SimOrder:
    order_id: int
    instrument: str
    side: str
    price: int
    remaining: int
    expiry_ms: int


@dataclass(frozen=True)
class TradeBurst:
    time_ms: int
    instrument: str
    trades: Tuple[Tuple[int, int], ...]

    @property
    def min_price(self) -> int:
        return min(price for price, _ in self.trades)

    @property
    def max_price(self) -> int:
        return max(price for price, _ in self.trades)

    def qty_at_or_below(self, price: int) -> int:
        return sum(qty for trade_price, qty in self.trades if trade_price <= price)

    def qty_at_or_above(self, price: int) -> int:
        return sum(qty for trade_price, qty in self.trades if trade_price >= price)


class SimAccount:
    def __init__(self) -> None:
        self.cash: Dict[str, int] = defaultdict(lambda: 10_000_000)
        self.positions: Dict[str, int] = defaultdict(int)
        self.orders: Dict[int, SimOrder] = {}
        self.next_order_id = 1
        self.realized_cents = 0
        self.long_cost: Dict[str, int] = defaultdict(int)
        self.short_proceeds: Dict[str, int] = defaultdict(int)
        self.limit_orders_placed = 0
        self.ioc_orders_sent = 0
        self.ioc_filled_qty = 0
        self.passive_filled_qty = 0
        self.rejected_orders = 0

    def place_limit(self, instrument: str, side: str, price: int, quantity: int, expiry_ms: int = 30_000) -> Optional[int]:
        exchange, _ = split_instrument(instrument)
        quantity = self._allowed_qty(instrument, side, price, quantity)
        if quantity <= 0:
            self.rejected_orders += 1
            return None
        order_id = self.next_order_id
        self.next_order_id += 1
        self.orders[order_id] = SimOrder(order_id, instrument, side, price, quantity, expiry_ms)
        self.limit_orders_placed += 1
        _ = self.cash[exchange]
        return order_id

    def execute_ioc(self, book: BacktestBook, side: str, price: int, quantity: int) -> int:
        quantity = self._allowed_qty(book.instrument, side, price, quantity)
        if quantity <= 0:
            self.rejected_orders += 1
            return 0
        self.ioc_orders_sent += 1
        remaining = quantity
        filled = 0
        if side == "bid":
            for ask_price, ask_qty in book.asks:
                if ask_price > price or remaining <= 0:
                    break
                qty = min(remaining, ask_qty)
                self._fill(book.instrument, "bid", ask_price, qty)
                remaining -= qty
                filled += qty
        else:
            for bid_price, bid_qty in book.bids:
                if bid_price < price or remaining <= 0:
                    break
                qty = min(remaining, bid_qty)
                self._fill(book.instrument, "ask", bid_price, qty)
                remaining -= qty
                filled += qty
        self.ioc_filled_qty += filled
        return filled

    def passive_fill(self, order_id: int, quantity: int, price: Optional[int] = None) -> int:
        order = self.orders.get(order_id)
        if order is None or quantity <= 0:
            return 0
        qty = min(order.remaining, quantity)
        self._fill(order.instrument, order.side, price if price is not None else order.price, qty)
        order.remaining -= qty
        if order.remaining <= 0:
            self.orders.pop(order_id, None)
        self.passive_filled_qty += qty
        return qty

    def expire(self, now_ms: int) -> List[int]:
        expired = [order_id for order_id, order in self.orders.items() if order.expiry_ms <= now_ms]
        for order_id in expired:
            self.orders.pop(order_id, None)
        return expired

    def inventory_for_exchange(self, exchange: str) -> Dict[str, Tuple[int, int]]:
        data: Dict[str, Tuple[int, int]] = {"$": (0, self.cash[exchange])}
        prefix = exchange + "-"
        for instrument, position in self.positions.items():
            if instrument.startswith(prefix):
                data[instrument] = (0, position)
        return data

    def total_pnl(self, marks: Dict[str, int]) -> int:
        active_exchanges = set(self.cash)
        active_exchanges.update(split_instrument(instrument)[0] for instrument, pos in self.positions.items() if pos)
        pnl = sum(self.cash[exchange] - 10_000_000 for exchange in active_exchanges)
        for instrument, position in self.positions.items():
            if position:
                pnl += position * marks.get(instrument, 0)
        return int(pnl)

    def _allowed_qty(self, instrument: str, side: str, price: int, desired: int) -> int:
        if desired <= 0 or price <= 0:
            return 0
        exchange, _ = split_instrument(instrument)
        position = self.positions[instrument]
        pending_bid = sum(order.remaining for order in self.orders.values() if order.instrument == instrument and order.side == "bid")
        pending_ask = sum(order.remaining for order in self.orders.values() if order.instrument == instrument and order.side == "ask")
        if side == "bid":
            long_room = max(0, 2000 - position - pending_bid)
            reserved_cash = sum(
                order.price * order.remaining
                for order in self.orders.values()
                if split_instrument(order.instrument)[0] == exchange and order.side == "bid"
            )
            spendable = self.cash[exchange] - (-5_000_000) - reserved_cash
            return max(0, min(desired, long_room, spendable // price))
        short_room = max(0, position - (-200) - pending_ask)
        return max(0, min(desired, short_room))

    def _fill(self, instrument: str, side: str, price: int, quantity: int) -> None:
        exchange, _ = split_instrument(instrument)
        before = self.positions[instrument]
        if side == "bid":
            cover_qty = min(quantity, max(0, -before))
            if cover_qty:
                avg_short = self.short_proceeds[instrument] // max(1, -before)
                self.realized_cents += (avg_short - price) * cover_qty
                self.short_proceeds[instrument] -= avg_short * cover_qty
            new_long = quantity - cover_qty
            if new_long:
                self.long_cost[instrument] += price * new_long
            self.positions[instrument] += quantity
            self.cash[exchange] -= price * quantity
        else:
            sell_from_long = min(quantity, max(0, before))
            if sell_from_long:
                avg_long = self.long_cost[instrument] // max(1, before)
                self.realized_cents += (price - avg_long) * sell_from_long
                self.long_cost[instrument] -= avg_long * sell_from_long
            new_short = quantity - sell_from_long
            if new_short:
                self.short_proceeds[instrument] += price * new_short
            self.positions[instrument] -= quantity
            self.cash[exchange] += price * quantity


def load_books(data_dir: str) -> Tuple[Dict[int, List[BacktestBook]], Dict[str, BacktestBook]]:
    by_time: Dict[int, List[BacktestBook]] = defaultdict(list)
    latest: Dict[str, BacktestBook] = {}
    for path in sorted(glob.glob(os.path.join(data_dir, "*_orderbooks.csv"))):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                bids = []
                asks = []
                for idx in (1, 2, 3):
                    bp = row.get(f"bid{idx}_price")
                    bq = row.get(f"bid{idx}_qty")
                    ap = row.get(f"ask{idx}_price")
                    aq = row.get(f"ask{idx}_qty")
                    if bp and bq:
                        bids.append((int(bp), int(bq)))
                    if ap and aq:
                        asks.append((int(ap), int(aq)))
                if not bids or not asks:
                    continue
                book = BacktestBook(
                    time_ms=int(row["time"]),
                    instrument=row["instrument"],
                    bids=tuple(sorted(bids, reverse=True)),
                    asks=tuple(sorted(asks)),
                )
                by_time[book.time_ms].append(book)
                latest[book.instrument] = book
    for rows in by_time.values():
        rows.sort(key=lambda book: book.instrument)
    return by_time, latest


def load_trade_bursts(data_dir: str) -> Dict[int, List[TradeBurst]]:
    grouped: Dict[Tuple[str, str], List[Tuple[int, int, int]]] = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(data_dir, "*_trades.csv"))):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                grouped[(row["instrument"], row["active_order_id"])].append(
                    (int(row["time"]), int(row["price"]), int(row["quantity"]))
                )
    by_time: Dict[int, List[TradeBurst]] = defaultdict(list)
    for (instrument, _), rows in grouped.items():
        rows.sort()
        time_ms = rows[0][0]
        burst = TradeBurst(time_ms, instrument, tuple((price, qty) for _, price, qty in rows))
        by_time[time_ms].append(burst)
    for rows in by_time.values():
        rows.sort(key=lambda burst: (burst.instrument, burst.min_price, burst.max_price))
    return by_time


def sync_engine(engine: AggressiveEdgeEngine, account: SimAccount, exchange: str) -> None:
    engine.on_inventory(exchange, account.inventory_for_exchange(exchange))


def execute_intent(
    engine: AggressiveEdgeEngine,
    account: SimAccount,
    latest_books: Dict[str, BacktestBook],
    intent: OrderIntent,
    now_ms: int,
) -> int:
    if intent.order_type == "limit":
        order_id = account.place_limit(intent.instrument_id, intent.side, int(intent.price or 0), intent.quantity, now_ms + 18_000)
        if order_id is None:
            return 0
        request_id = f"bt-{order_id}"
        engine.note_request(request_id, intent)
        engine.on_order_response(request_id, True, {"order_id": order_id})
        return 0
    book = latest_books.get(intent.instrument_id)
    if book is None:
        account.rejected_orders += 1
        return 0
    exchange, _ = split_instrument(intent.instrument_id)
    before_position = account.positions[intent.instrument_id]
    before_cash = account.cash[exchange]
    filled = account.execute_ioc(book, intent.side, int(intent.price or 0), intent.quantity)
    if filled:
        request_id = f"bt-ioc-{now_ms}-{account.ioc_orders_sent}"
        engine.note_request(request_id, intent)
        engine.on_order_response(
            request_id,
            True,
            {
                "immediate_inventory_change": account.positions[intent.instrument_id] - before_position,
                "immediate_balance_change": account.cash[exchange] - before_cash,
            },
        )
    return filled


def process_passive_burst(
    engine: AggressiveEdgeEngine,
    account: SimAccount,
    latest_books: Dict[str, BacktestBook],
    burst: TradeBurst,
) -> int:
    orders = [order for order in account.orders.values() if order.instrument == burst.instrument]
    if not orders:
        return 0
    current_book = latest_books.get(burst.instrument)
    mid = current_book.mid if current_book and current_book.mid is not None else None
    intents: List[OrderIntent] = []
    fills = 0

    if mid is None or burst.min_price < mid:
        consumed = 0
        for order in sorted((o for o in orders if o.side == "bid"), key=lambda o: o.price, reverse=True):
            available = max(0, burst.qty_at_or_below(order.price) - consumed)
            if available <= 0:
                continue
            qty = account.passive_fill(order.order_id, available, order.price)
            if qty:
                consumed += qty
                fills += qty
                intents.extend(engine.on_trade_event(order.instrument, order.order_id, qty, order.price))

    if mid is None or burst.max_price > mid:
        consumed = 0
        for order in sorted((o for o in orders if o.side == "ask"), key=lambda o: o.price):
            available = max(0, burst.qty_at_or_above(order.price) - consumed)
            if available <= 0:
                continue
            qty = account.passive_fill(order.order_id, available, order.price)
            if qty:
                consumed += qty
                fills += qty
                intents.extend(engine.on_trade_event(order.instrument, order.order_id, qty, order.price))

    for intent in intents:
        execute_intent(engine, account, latest_books, intent, burst.time_ms)
    if fills:
        exchange, _ = split_instrument(burst.instrument)
        sync_engine(engine, account, exchange)
    return fills


def run_backtest(args: argparse.Namespace) -> int:
    books_by_time, final_books = load_books(args.data_dir)
    bursts_by_time = load_trade_bursts(args.data_dir)
    times = sorted(set(books_by_time) | set(bursts_by_time))
    latest_books: Dict[str, BacktestBook] = {}
    account = SimAccount()
    config = AggressiveConfig(location=args.location, trade_all=args.all)
    if args.median_threshold is not None:
        config.median_threshold_cents = args.median_threshold
    if args.etf_threshold is not None:
        config.etf_threshold_cents = args.etf_threshold
    engine = AggressiveEdgeEngine(config)
    emitted_intents = 0

    for now_ms in times:
        for order_id in account.expire(now_ms):
            engine.on_cancel(order_id)

        held_intents: List[OrderIntent] = []
        for book in books_by_time.get(now_ms, []):
            latest_books[book.instrument] = book
            held_intents.extend(engine.on_book(book.to_engine_book(), now_ms))

        for burst in bursts_by_time.get(now_ms, []):
            process_passive_burst(engine, account, latest_books, burst)

        held_intents.sort(key=lambda intent: (0 if intent.order_type == "ioc" else 1, -intent.quantity))
        for intent in held_intents[: config.max_orders_per_market_update]:
            intent = engine.clamp_intent(intent)
            if intent is None:
                continue
            emitted_intents += 1
            execute_intent(engine, account, latest_books, intent, now_ms)

    marks = {
        instrument: int(book.mid)
        for instrument, book in {**final_books, **latest_books}.items()
        if book.mid is not None
    }
    pnl = account.total_pnl(marks)
    open_positions = {instrument: pos for instrument, pos in account.positions.items() if pos}
    print(f"location={args.location} all={args.all} median={config.median_threshold_cents} etf={config.etf_threshold_cents}")
    print(f"pnl_cents={pnl} dollars={pnl / 100:.2f}")
    print(f"realized_cents={account.realized_cents} realized_dollars={account.realized_cents / 100:.2f}")
    print(
        "orders "
        f"intents={emitted_intents} limits={account.limit_orders_placed} "
        f"iocs={account.ioc_orders_sent} rejected={account.rejected_orders}"
    )
    print(f"fills passive_qty={account.passive_filled_qty} ioc_qty={account.ioc_filled_qty}")
    print(f"open_positions={len(open_positions)} gross_abs_position={sum(abs(v) for v in open_positions.values())}")
    for instrument, position in sorted(open_positions.items(), key=lambda item: -abs(item[1]))[:20]:
        mark = marks.get(instrument, 0)
        print(f"  {instrument:16s} pos={position:6d} mark={mark:6d} mtm={position * mark:10d}")
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest edge_max against market_data CSV files")
    parser.add_argument("--data-dir", default="market_data")
    parser.add_argument("--location", default="ZSE", choices=("NYSE", "ZSE", "HKEX"))
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--median-threshold", type=int)
    parser.add_argument("--etf-threshold", type=int)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(run_backtest(parse_args()))
