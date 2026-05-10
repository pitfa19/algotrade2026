#!/usr/bin/env python3
"""Offline edge scanner for edge_max.py using recorded market_data CSV files."""

from __future__ import annotations

import argparse
import bisect
import csv
import glob
import os
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from edge_max import CLOSE_EXCHANGES, ETF_BASKETS, split_instrument


@dataclass(frozen=True)
class CsvBook:
    time_ms: int
    instrument: str
    bid: int
    ask: int
    bid3: int
    ask3: int
    bids: Tuple[Tuple[int, int], ...]
    asks: Tuple[Tuple[int, int], ...]

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(frozen=True)
class Burst:
    instrument: str
    active_order_id: str
    time_ms: int
    trades: Tuple[Tuple[int, int, int], ...]
    prev_book: CsvBook
    next_book: CsvBook

    @property
    def min_price(self) -> int:
        return min(price for _, price, _ in self.trades)

    @property
    def max_price(self) -> int:
        return max(price for _, price, _ in self.trades)

    def qty_at_or_below(self, price: int) -> int:
        return sum(qty for _, trade_price, qty in self.trades if trade_price <= price)

    def qty_at_or_above(self, price: int) -> int:
        return sum(qty for _, trade_price, qty in self.trades if trade_price >= price)


def load_books(data_dir: str) -> Dict[str, List[CsvBook]]:
    books: Dict[str, List[CsvBook]] = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(data_dir, "*_orderbooks.csv"))):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                if not row["bid1_price"] or not row["ask1_price"]:
                    continue
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
                books[row["instrument"]].append(
                    CsvBook(
                        time_ms=int(row["time"]),
                        instrument=row["instrument"],
                        bid=bids[0][0],
                        ask=asks[0][0],
                        bid3=bids[-1][0],
                        ask3=asks[-1][0],
                        bids=tuple(sorted(bids, reverse=True)),
                        asks=tuple(sorted(asks)),
                    )
                )
    for arr in books.values():
        arr.sort(key=lambda b: b.time_ms)
    return books


def load_bursts(data_dir: str, books: Dict[str, List[CsvBook]]) -> List[Burst]:
    grouped: Dict[Tuple[str, str], List[Tuple[int, int, int]]] = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(data_dir, "*_trades.csv"))):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                grouped[(row["instrument"], row["active_order_id"])].append(
                    (int(row["time"]), int(row["price"]), int(row["quantity"]))
                )

    bursts: List[Burst] = []
    for (instrument, active_order_id), trades in grouped.items():
        arr = books.get(instrument)
        if not arr:
            continue
        trades.sort()
        times = [book.time_ms for book in arr]
        start = trades[0][0]
        end = trades[-1][0]
        prev_idx = bisect.bisect_right(times, start) - 1
        next_idx = bisect.bisect_right(times, end)
        if prev_idx < 0 or next_idx >= len(arr):
            continue
        bursts.append(
            Burst(
                instrument=instrument,
                active_order_id=active_order_id,
                time_ms=start,
                trades=tuple(trades),
                prev_book=arr[prev_idx],
                next_book=arr[next_idx],
            )
        )
    return bursts


def target_filter(location: str, trade_all: bool):
    targets = set(CLOSE_EXCHANGES[location]) if not trade_all else None

    def accept(instrument: str) -> bool:
        exchange, _ = split_instrument(instrument)
        return targets is None or exchange in targets

    return accept


def replay_tail_traps(
    bursts: Sequence[Burst],
    location: str,
    trade_all: bool,
    bid_offsets: Sequence[int],
    ask_offsets: Sequence[int],
    fixed_bids: Sequence[int],
    fixed_asks: Sequence[int],
    min_gap: int,
    min_profit: int,
) -> Tuple[int, List[Tuple[int, str]]]:
    accept = target_filter(location, trade_all)
    total = 0
    examples: List[Tuple[int, str]] = []

    for burst in bursts:
        if not accept(burst.instrument):
            continue
        book = burst.prev_book
        mid = int(book.mid)

        bid_prices = {max(100, mid - offset) for offset in bid_offsets}
        bid_prices.update(price for price in fixed_bids if price <= mid - min_gap)
        for price in sorted(bid_prices):
            if price <= 0 or price >= book.bid or burst.min_price > price:
                continue
            qty = min(2000, burst.qty_at_or_below(price))
            if qty <= 0:
                continue
            pnl = (burst.next_book.bid - price) * qty
            if pnl >= min_profit * qty:
                total += pnl
                examples.append((pnl, f"bid {burst.instrument} t={burst.time_ms} price={price} qty={qty} next_bid={burst.next_book.bid}"))

        ask_prices = {mid + offset for offset in ask_offsets}
        ask_prices.update(price for price in fixed_asks if price >= mid + min_gap)
        for price in sorted(ask_prices, reverse=True):
            if price <= book.ask or burst.max_price < price:
                continue
            qty = min(200, burst.qty_at_or_above(price))
            if qty <= 0:
                continue
            pnl = (price - burst.next_book.ask) * qty
            if pnl >= min_profit * qty:
                total += pnl
                examples.append((pnl, f"ask {burst.instrument} t={burst.time_ms} price={price} qty={qty} next_ask={burst.next_book.ask}"))

    return total, sorted(examples, reverse=True)[:20]


def replay_cross_median(books: Dict[str, List[CsvBook]], location: str, trade_all: bool, threshold: int) -> Tuple[int, List[Tuple[int, str]]]:
    accept = target_filter(location, trade_all)
    by_time: Dict[int, List[CsvBook]] = defaultdict(list)
    for arr in books.values():
        for book in arr:
            if accept(book.instrument):
                by_time[book.time_ms].append(book)

    latest: Dict[str, CsvBook] = {}
    total = 0
    examples: List[Tuple[int, str]] = []
    for time_ms in sorted(by_time):
        for book in by_time[time_ms]:
            latest[book.instrument] = book
        by_ticker: Dict[str, List[CsvBook]] = defaultdict(list)
        for book in latest.values():
            _, ticker = split_instrument(book.instrument)
            if time_ms - book.time_ms <= 650:
                by_ticker[ticker].append(book)

        for book in by_time[time_ms]:
            exchange, ticker = split_instrument(book.instrument)
            refs = by_ticker[ticker]
            if len(refs) < 2:
                continue
            fair = statistics.median(ref.mid for ref in refs)
            buy_limit = int(fair - threshold)
            if book.ask <= buy_limit:
                qty = min(2000, sum(q for p, q in book.asks if p <= buy_limit))
                pnl = int((fair - book.ask) * qty)
                if qty > 0 and pnl > 0:
                    total += pnl
                    examples.append((pnl, f"median-buy {book.instrument} t={time_ms} ask={book.ask} fair={int(fair)} qty={qty}"))
            sell_limit = int(fair + threshold)
            if book.bid >= sell_limit:
                qty = min(200, sum(q for p, q in book.bids if p >= sell_limit))
                pnl = int((book.bid - fair) * qty)
                if qty > 0 and pnl > 0:
                    total += pnl
                    examples.append((pnl, f"median-sell {book.instrument} t={time_ms} bid={book.bid} fair={int(fair)} qty={qty}"))
    return total, sorted(examples, reverse=True)[:20]


def replay_etf(books: Dict[str, List[CsvBook]], location: str, trade_all: bool, threshold: int) -> Tuple[int, List[Tuple[int, str]]]:
    accept = target_filter(location, trade_all)
    by_time: Dict[int, List[CsvBook]] = defaultdict(list)
    for arr in books.values():
        for book in arr:
            if accept(book.instrument):
                by_time[book.time_ms].append(book)
    latest: Dict[str, CsvBook] = {}
    total = 0
    examples: List[Tuple[int, str]] = []
    for time_ms in sorted(by_time):
        for book in by_time[time_ms]:
            latest[book.instrument] = book
        for book in by_time[time_ms]:
            exchange, ticker = split_instrument(book.instrument)
            basket = ETF_BASKETS.get(ticker)
            if not basket:
                continue
            components = []
            for component in basket:
                comp_book = latest.get(f"{exchange}-{component}")
                if comp_book is None or time_ms - comp_book.time_ms > 650:
                    components = []
                    break
                components.append(comp_book.mid)
            if not components:
                continue
            fair = sum(components) / len(components)
            buy_limit = int(fair - threshold)
            if book.ask <= buy_limit:
                qty = min(2000, sum(q for p, q in book.asks if p <= buy_limit))
                pnl = int((fair - book.ask) * qty)
                if qty > 0 and pnl > 0:
                    total += pnl
                    examples.append((pnl, f"etf-buy {book.instrument} t={time_ms} ask={book.ask} fair={int(fair)} qty={qty}"))
            sell_limit = int(fair + threshold)
            if book.bid >= sell_limit:
                qty = min(200, sum(q for p, q in book.bids if p >= sell_limit))
                pnl = int((book.bid - fair) * qty)
                if qty > 0 and pnl > 0:
                    total += pnl
                    examples.append((pnl, f"etf-sell {book.instrument} t={time_ms} bid={book.bid} fair={int(fair)} qty={qty}"))
    return total, sorted(examples, reverse=True)[:20]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Replay edge_max ideas on market_data CSV files")
    parser.add_argument("--data-dir", default="market_data")
    parser.add_argument("--location", default="ZSE", choices=tuple(CLOSE_EXCHANGES))
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--median-threshold", type=int, default=20)
    parser.add_argument("--etf-threshold", type=int, default=30)
    args = parser.parse_args(argv)

    books = load_books(args.data_dir)
    bursts = load_bursts(args.data_dir, books)
    tail_pnl, tail_examples = replay_tail_traps(
        bursts,
        args.location,
        args.all,
        bid_offsets=(5000, 3000, 1500),
        ask_offsets=(5000, 3000, 1500),
        fixed_bids=(10_000, 15_000),
        fixed_asks=(18_000, 20_000),
        min_gap=700,
        min_profit=1,
    )
    median_pnl, median_examples = replay_cross_median(books, args.location, args.all, args.median_threshold)
    etf_pnl, etf_examples = replay_etf(books, args.location, args.all, args.etf_threshold)

    print(f"location={args.location} all={args.all} instruments={len(books)} bursts={len(bursts)}")
    print(f"tail_trap_pnl_cents={tail_pnl} dollars={tail_pnl / 100:.2f}")
    print(f"cross_median_pnl_cents={median_pnl} dollars={median_pnl / 100:.2f}")
    print(f"etf_pnl_cents={etf_pnl} dollars={etf_pnl / 100:.2f}")
    print(f"combined_pnl_cents={tail_pnl + median_pnl + etf_pnl} dollars={(tail_pnl + median_pnl + etf_pnl) / 100:.2f}")
    for title, examples in (("tail", tail_examples), ("median", median_examples), ("etf", etf_examples)):
        print(f"\nTop {title} examples")
        for pnl, text in examples[:10]:
            print(f"{pnl:10d} {pnl / 100:10.2f} {text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
