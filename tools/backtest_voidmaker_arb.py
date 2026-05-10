#!/usr/bin/env python3
"""
Replay scorer for Voidmaker close-cluster cross-venue lag.

This computes gross paired IOC edge from top-of-book snapshots using the same
opportunity selector as the live bot. It is a signal/backtest, not a perfect
matching-engine replay: it assumes both IOC legs fill at the displayed top.
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from voidmaker import BookTop, find_cross_venue_arbs, select_close_exchanges


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score close-cluster cross-venue arb")
    parser.add_argument("--data-dir", default=os.path.join(ROOT, "market_data"))
    parser.add_argument("--location", choices=["NYSE", "ZSE", "HKEX"], default="ZSE")
    parser.add_argument("--max-rtt-ms", type=int, default=60)
    parser.add_argument("--min-spread", type=int, default=25)
    parser.add_argument("--clip-qty", type=int, default=25)
    parser.add_argument("--cooldown-ms", type=int, default=150)
    parser.add_argument("--max-per-update", type=int, default=8)
    parser.add_argument("--top", type=int, default=25)
    return parser.parse_args()


def load_rows(data_dir: str, exchanges: set[str]):
    rows = []
    for path in sorted(glob.glob(os.path.join(data_dir, "*_orderbooks.csv"))):
        exchange = os.path.basename(path).split("_", 1)[0]
        if exchange not in exchanges:
            continue
        with open(path, newline="") as handle:
            for row in csv.DictReader(handle):
                bid_items = []
                ask_items = []
                for level in range(1, 4):
                    bp = row.get(f"bid{level}_price")
                    bq = row.get(f"bid{level}_qty")
                    ap = row.get(f"ask{level}_price")
                    aq = row.get(f"ask{level}_qty")
                    if bp and bq:
                        bid_items.append((int(bp), int(bq)))
                    if ap and aq:
                        ask_items.append((int(ap), int(aq)))
                if not bid_items and not ask_items:
                    continue
                best_bid = max(bid_items, default=(None, 0), key=lambda item: item[0] or -1)
                best_ask = min(ask_items, default=(None, 0), key=lambda item: item[0] or 1_000_001)
                rows.append(
                    (
                        int(row["time"]),
                        exchange,
                        row["instrument"].split("-", 1)[1],
                        BookTop(best_bid[0], best_ask[0], best_bid[1], best_ask[1]),
                    )
                )
    rows.sort()
    return rows


def main() -> None:
    args = parse_args()
    exchanges = select_close_exchanges(args.location, args.max_rtt_ms)
    latest = {exchange: {} for exchange in exchanges}
    last_sent: dict[tuple[str, str, str], int] = {}
    gross = 0
    count = 0
    by_key: Counter[tuple[str, str, str]] = Counter()
    top = []

    for timestamp, exchange, ticker, book in load_rows(args.data_dir, set(exchanges)):
        latest[exchange][ticker] = book
        opportunities = find_cross_venue_arbs(latest, exchanges, args.min_spread, args.clip_qty)
        sent = 0
        used_exchanges = set()
        used_tickers = set()
        for opp in opportunities:
            key = (opp.ticker, opp.buy_exchange, opp.sell_exchange)
            if timestamp - last_sent.get(key, -10**12) < args.cooldown_ms:
                continue
            if opp.ticker in used_tickers:
                continue
            if opp.buy_exchange in used_exchanges or opp.sell_exchange in used_exchanges:
                continue
            gross += opp.edge_cents
            count += 1
            by_key[key] += opp.edge_cents
            last_sent[key] = timestamp
            used_tickers.add(opp.ticker)
            used_exchanges.update((opp.buy_exchange, opp.sell_exchange))
            top.append((opp.edge_cents, timestamp, opp))
            top = sorted(top, key=lambda item: item[0])[-args.top :]
            sent += 1
            if sent >= args.max_per_update:
                break

    print(f"location={args.location} exchanges={','.join(exchanges)}")
    print(f"min_spread={args.min_spread} clip_qty={args.clip_qty} cooldown_ms={args.cooldown_ms}")
    print(f"paired_iocs={count}")
    print(f"gross_edge_cents={gross}")
    print(f"gross_edge_dollars={gross / 100:,.2f}")
    print()
    print("top_pairs")
    for key, edge in by_key.most_common(args.top):
        ticker, buy_exchange, sell_exchange = key
        print(f"{ticker:6s} buy={buy_exchange:8s} sell={sell_exchange:8s} edge=${edge / 100:,.2f}")
    print()
    print("top_events")
    for edge, timestamp, opp in sorted(top, key=lambda item: item[0], reverse=True):
        print(
            f"{timestamp:7d} {opp.ticker:6s} {opp.buy_exchange:8s}->{opp.sell_exchange:8s} "
            f"buy={opp.buy_price:6d} sell={opp.sell_price:6d} qty={opp.quantity:3d} edge=${edge / 100:,.2f}"
        )


if __name__ == "__main__":
    main()
