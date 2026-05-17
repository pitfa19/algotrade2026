#!/usr/bin/env python3
"""
Replay scorer for Voidmaker's passive-landmine edge.

This is not a full exchange simulator. It answers one narrow question:
if we had passive floor bids / ceiling asks resting before the observed
oversized sweeps, how much gross edge is visible in the recorded trades?
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOTS = os.path.join(ROOT, "bots")
for _p in (BOTS, ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from voidmaker import LandmineFill, estimate_landmine_profit_cents


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score observed Voidmaker landmine fills")
    parser.add_argument("--data-dir", default=os.path.join(ROOT, "market_data"))
    parser.add_argument("--cover-cents", type=int, default=10_000)
    parser.add_argument("--low-max", type=int, default=5, help="prices <= this are passive floor-bid fills")
    parser.add_argument("--high-min", type=int, default=20_000, help="prices >= this are passive high-ask fills")
    parser.add_argument("--top", type=int, default=30)
    return parser.parse_args()


def iter_fills(data_dir: str, low_max: int, high_min: int):
    for path in sorted(glob.glob(os.path.join(data_dir, "*_trades.csv"))):
        exchange = os.path.basename(path).split("_", 1)[0]
        with open(path, newline="") as handle:
            for row in csv.DictReader(handle):
                price = int(row["price"])
                if price > low_max and price < high_min:
                    continue
                ticker = row["instrument"].split("-", 1)[1]
                side = "bid" if price <= low_max else "ask"
                yield int(row["time"]), LandmineFill(
                    exchange=exchange,
                    ticker=ticker,
                    side=side,
                    price=price,
                    quantity=int(row["quantity"]),
                )


def main() -> None:
    args = parse_args()
    fills_with_time = list(iter_fills(args.data_dir, args.low_max, args.high_min))
    fills = [fill for _, fill in fills_with_time]
    profit = estimate_landmine_profit_cents(fills, args.cover_cents)

    by_exchange: dict[str, int] = defaultdict(int)
    by_key: Counter[tuple[str, str, str, int]] = Counter()
    qty_by_key: Counter[tuple[str, str, str, int]] = Counter()
    for _, fill in fills_with_time:
        edge = estimate_landmine_profit_cents([fill], args.cover_cents)
        by_exchange[fill.exchange] += edge
        key = (fill.exchange, fill.ticker, fill.side, fill.price)
        by_key[key] += edge
        qty_by_key[key] += fill.quantity

    print(f"fills={len(fills)} quantity={sum(fill.quantity for fill in fills)}")
    print(f"gross_edge_cents={profit}")
    print(f"gross_edge_dollars={profit / 100:,.2f}")
    print()
    print("by_exchange")
    for exchange, edge in sorted(by_exchange.items(), key=lambda item: item[1], reverse=True):
        print(f"{exchange:8s} {edge / 100:12,.2f}")
    print()
    print("top_landmine_levels")
    for key, edge in by_key.most_common(args.top):
        exchange, ticker, side, price = key
        print(f"{exchange:8s} {ticker:6s} {side:3s} price={price:6d} qty={qty_by_key[key]:4d} edge=${edge / 100:,.2f}")
    print()
    print("chronological_extremes")
    for timestamp, fill in fills_with_time[: args.top]:
        edge = estimate_landmine_profit_cents([fill], args.cover_cents)
        print(f"{timestamp:7d} {fill.exchange:8s} {fill.ticker:6s} {fill.side:3s} price={fill.price:6d} qty={fill.quantity:4d} edge=${edge / 100:,.2f}")


if __name__ == "__main__":
    main()
