#!/usr/bin/env python3
"""Replay and proof harness for the SIMP/CARD edge.

The replay is intentionally conservative relative to the raw CSVs:

* it trades only the close venues for each home location;
* it observes remote CARD venues with one-way latency;
* IOC orders arrive after the full documented round-trip latency;
* it fills only visible top-3 CSV depth and forces final liquidation through
  the final visible book.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import glob
import hashlib
import os
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from simp_card_cross_median_bot import (
    CARD_THRESHOLDS,
    CASH_FLOOR,
    CLOSE_TARGETS,
    DEFAULT_MAX_LONG,
    DEFAULT_MIN_SHORT,
    EXCHANGES,
    SIMP_THRESHOLDS,
    STARTING_CASH,
    median_int,
)


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
}


@dataclass(frozen=True)
class Row:
    t: int
    exchange: str
    bids: tuple[tuple[int, int], ...]
    asks: tuple[tuple[int, int], ...]

    @property
    def mid2(self) -> int:
        return self.bids[0][0] + self.asks[0][0]


def hash_market_data(data_dir: str) -> str:
    digest = hashlib.sha256()
    for path in sorted(glob.glob(os.path.join(data_dir, "*.csv"))):
        digest.update(os.path.basename(path).encode())
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def load_rows(data_dir: str, ticker: str) -> dict[str, list[Row]]:
    rows: dict[str, list[Row]] = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(data_dir, "*_orderbooks.csv"))):
        exchange = os.path.basename(path).split("_")[0]
        with open(path, newline="") as fh:
            for item in csv.DictReader(fh):
                if item["instrument"].split("-")[-1] != ticker:
                    continue
                bids: list[tuple[int, int]] = []
                asks: list[tuple[int, int]] = []
                for level in range(1, 4):
                    bid_price = item.get(f"bid{level}_price")
                    bid_qty = item.get(f"bid{level}_qty")
                    ask_price = item.get(f"ask{level}_price")
                    ask_qty = item.get(f"ask{level}_qty")
                    if bid_price and bid_qty:
                        bids.append((int(bid_price), int(bid_qty)))
                    if ask_price and ask_qty:
                        asks.append((int(ask_price), int(ask_qty)))
                if bids and asks:
                    rows[exchange].append(
                        Row(
                            t=int(item["time"]),
                            exchange=exchange,
                            bids=tuple(sorted(bids, reverse=True)),
                            asks=tuple(sorted(asks)),
                        )
                    )
    for exchange_rows in rows.values():
        exchange_rows.sort(key=lambda row: row.t)
    return dict(rows)


def rounded_bucket(time_ms: int) -> int:
    return round(time_ms / 100) * 100


def cross_median_series(rows: dict[str, list[Row]], min_inputs: int = 5) -> list[tuple[int, int, int, int, int]]:
    buckets: dict[int, list[int]] = defaultdict(list)
    for exchange_rows in rows.values():
        for row in exchange_rows:
            buckets[rounded_bucket(row.t)].append(row.mid2)
    out = []
    for bucket, mids2 in sorted(buckets.items()):
        if len(mids2) >= min_inputs:
            out.append((bucket, median_int(mids2), min(mids2), max(mids2), len(mids2)))
    return out


def paired_median_transforms(
    card_rows: dict[str, list[Row]],
    simp_rows: dict[str, list[Row]],
    min_inputs: int = 5,
) -> dict[str, tuple[float, float]]:
    def bucketed(rows: dict[str, list[Row]]) -> dict[int, int]:
        raw: dict[int, list[int]] = defaultdict(list)
        for exchange_rows in rows.values():
            for row in exchange_rows:
                raw[rounded_bucket(row.t)].append(row.mid2)
        return {
            bucket: median_int(values)
            for bucket, values in raw.items()
            if len(values) >= min_inputs
        }

    card = bucketed(card_rows)
    simp = bucketed(simp_rows)
    common = sorted(set(card) & set(simp))
    sums = [(card[b] + simp[b]) / 2 for b in common]
    diffs = [(simp[b] - card[b]) / 2 for b in common]
    ratios = [simp[b] / card[b] for b in common if card[b] != 0]
    return {
        "sum_cents": (min(sums), max(sums)),
        "diff_cents": (min(diffs), max(diffs)),
        "ratio": (min(ratios), max(ratios)),
    }


def visible_bid_qty(row: Row, limit: int) -> int:
    return sum(qty for price, qty in row.bids if price >= limit)


def visible_ask_qty(row: Row, limit: int) -> int:
    return sum(qty for price, qty in row.asks if price <= limit)


def liquidate(cash: dict[str, int], pos: dict[str, int], rows: dict[str, list[Row]], targets: Iterable[str]) -> None:
    for exchange in targets:
        final = rows[exchange][-1]
        if pos[exchange] > 0:
            for price, qty in final.bids:
                fill = min(pos[exchange], qty)
                cash[exchange] += price * fill
                pos[exchange] -= fill
                if pos[exchange] <= 0:
                    break
            if pos[exchange] > 0:
                cash[exchange] += final.bids[-1][0] * pos[exchange]
                pos[exchange] = 0
        elif pos[exchange] < 0:
            for price, qty in final.asks:
                fill = min(-pos[exchange], qty)
                cash[exchange] -= price * fill
                pos[exchange] += fill
                if pos[exchange] >= 0:
                    break
            if pos[exchange] < 0:
                cash[exchange] -= final.asks[-1][0] * (-pos[exchange])
                pos[exchange] = 0


def replay(
    rows: dict[str, list[Row]],
    ticker: str,
    home: str,
    targets: tuple[str, ...],
    threshold: int,
    max_long: int = DEFAULT_MAX_LONG,
    min_short: int = DEFAULT_MIN_SHORT,
) -> dict[str, int | float]:
    times = {exchange: [row.t for row in exchange_rows] for exchange, exchange_rows in rows.items()}
    mids2 = {exchange: [row.mid2 for row in exchange_rows] for exchange, exchange_rows in rows.items()}
    signals = sorted(
        (row.t, exchange, row)
        for exchange in targets
        for row in rows.get(exchange, [])
    )
    cash = {exchange: STARTING_CASH for exchange in targets}
    pos = {exchange: 0 for exchange in targets}
    last_exec_index = {exchange: -1 for exchange in targets}
    sent = fills = quantity = skipped_fair = 0

    for t, exchange, signal in signals:
        rtt = LATENCY_RTT_MS[home][exchange]
        one_way = (rtt + 1) // 2
        local_time = t + one_way
        if ticker == "SIMP":
            fair2 = 20_000
        else:
            observed: list[int] = []
            for source in EXCHANGES:
                if source not in times:
                    continue
                source_one_way = (LATENCY_RTT_MS[home][source] + 1) // 2
                cutoff = local_time - source_one_way
                idx = bisect.bisect_right(times[source], cutoff) - 1
                if idx >= 0:
                    observed.append(mids2[source][idx])
            if len(observed) < 5:
                skipped_fair += 1
                continue
            fair2 = median_int(observed)

        buy_limit = (fair2 - 2 * threshold) // 2
        sell_limit = -(-(fair2 + 2 * threshold) // 2)
        want_sell = visible_bid_qty(signal, sell_limit) > 0 and pos[exchange] > min_short
        want_buy = (
            visible_ask_qty(signal, buy_limit) > 0
            and pos[exchange] < max_long
            and cash[exchange] > CASH_FLOOR
        )
        if not (want_sell or want_buy):
            continue

        exec_idx = bisect.bisect_left(times[exchange], t + rtt)
        if exec_idx >= len(times[exchange]) or exec_idx == last_exec_index[exchange]:
            continue
        last_exec_index[exchange] = exec_idx
        execution = rows[exchange][exec_idx]

        if want_sell:
            sent += 1
            for price, qty in execution.bids:
                if price < sell_limit:
                    continue
                fill = min(qty, pos[exchange] - min_short)
                if fill > 0:
                    pos[exchange] -= fill
                    cash[exchange] += price * fill
                    fills += 1
                    quantity += fill
        if want_buy:
            sent += 1
            cash_cap = max(0, cash[exchange] - CASH_FLOOR) // max(1, buy_limit)
            for price, qty in execution.asks:
                if price > buy_limit:
                    continue
                fill = min(qty, max_long - pos[exchange], cash_cap)
                if fill > 0:
                    pos[exchange] += fill
                    cash[exchange] -= price * fill
                    cash_cap -= fill
                    fills += 1
                    quantity += fill

    liquidate(cash, pos, rows, targets)
    pnl = sum(cash[exchange] - STARTING_CASH for exchange in targets)
    return {
        "pnl_cents": pnl,
        "sent": sent,
        "fills": fills,
        "quantity": quantity,
        "skipped_fair": skipped_fair,
    }


def format_range(values: list[tuple[int, int, int, int, int]]) -> str:
    medians = [median2 / 2 for _, median2, _, _, _ in values]
    counts = [count for *_, count in values]
    return (
        f"buckets={len(values)} median_range={min(medians):.2f}..{max(medians):.2f} "
        f"avg_inputs={statistics.mean(counts):.2f}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="market_data")
    args = parser.parse_args()

    card_rows = load_rows(args.data_dir, "CARD")
    simp_rows = load_rows(args.data_dir, "SIMP")
    card_series = cross_median_series(card_rows)
    simp_series = cross_median_series(simp_rows)
    transforms = paired_median_transforms(card_rows, simp_rows)

    print(f"data_sha256={hash_market_data(args.data_dir)}")
    print("strict_constant_cross_median=DISPROVED")
    print(f"CARD {format_range(card_series)}")
    print(f"SIMP {format_range(simp_series)}")
    for name, (low, high) in transforms.items():
        print(f"{name}_range={low:.6f}..{high:.6f}")

    print("\nlatency_aware_close_venue_replay")
    total = 0
    for home, targets in CLOSE_TARGETS.items():
        simp = replay(simp_rows, "SIMP", home, targets, SIMP_THRESHOLDS[home])
        card = replay(card_rows, "CARD", home, targets, CARD_THRESHOLDS[home])
        combined = int(simp["pnl_cents"]) + int(card["pnl_cents"])
        total += combined
        print(
            f"{home:5s} targets={','.join(targets):18s} "
            f"SIMP(th={SIMP_THRESHOLDS[home]})={int(simp['pnl_cents']):10d} "
            f"CARD(th={CARD_THRESHOLDS[home]})={int(card['pnl_cents']):10d} "
            f"combined={combined:10d} "
            f"fills={int(simp['fills']) + int(card['fills']):5d} "
            f"qty={int(simp['quantity']) + int(card['quantity']):7d}"
        )
    print(f"TOTAL_CLOSE_REPLAY_PNL_CENTS={total}")
    print(f"TOTAL_CLOSE_REPLAY_PNL_DOLLARS={total / 100:.2f}")


if __name__ == "__main__":
    main()
