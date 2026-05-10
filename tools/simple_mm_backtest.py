#!/usr/bin/env python3
"""Local replay for simple_mm_bot.py over market_data CSVs."""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import simple_mm_bot as bot


def to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


@dataclass
class Result:
    pnl: int
    cash: int
    positions: dict[str, int]
    fills: int
    shares: int
    sent: int
    rejected: int
    cancels: int


class ExchangeReplay:
    def __init__(self, exchange: str, configs: list[bot.InstrumentConfig]) -> None:
        self.exchange = exchange
        self.configs = configs
        self.strategy = bot.SimpleMarketMaker(configs)
        self.state = bot.ExchangeState(exchange)
        self.state.inventory_synced = True
        self.state.pending_orders_synced = True
        self.next_order_id = 1
        self.fills = 0
        self.shares = 0
        self.sent = 0
        self.rejected = 0
        self.cancels = 0
        self.last_mid = {config.instrument: 10_000 for config in configs}

    def score(self) -> int:
        marked_inventory = sum(
            self.state.position(instrument) * mid
            for instrument, mid in self.last_mid.items()
        )
        return self.state.cash + marked_inventory - bot.INITIAL_CASH

    @staticmethod
    def levels(book: dict[str, str], side: str) -> list[tuple[int, int]]:
        prefix = "bid" if side == "bid" else "ask"
        return [
            (to_int(book.get(f"{prefix}{level}_price")), to_int(book.get(f"{prefix}{level}_qty")))
            for level in (1, 2, 3)
        ]

    def consume_book(
        self,
        instrument: str,
        side: str,
        quantity: int,
        limit_price: int,
        order_type: str,
        depth: dict[str, dict[str, int]],
    ) -> int:
        levels = depth.get("asks", {}) if side == "bid" else depth.get("bids", {})
        ordered = sorted((int(price), int(qty)) for price, qty in levels.items())
        if side == "ask":
            ordered.reverse()
        filled = 0
        notional = 0
        for price, available in ordered:
            if quantity <= 0 or price <= 0 or available <= 0:
                break
            if order_type != "market":
                if side == "bid" and price > limit_price:
                    continue
                if side == "ask" and price < limit_price:
                    continue
            take = min(quantity, available)
            quantity -= take
            filled += take
            notional += take * price
        if filled:
            if side == "bid":
                before = self.state.position(instrument)
                self.state.positions[instrument] = before + filled
                if before == 0:
                    self.state.hold_since_ms[instrument] = 0
                self.state.cash -= notional
            else:
                self.state.positions[instrument] = self.state.position(instrument) - filled
                self.state.cash += notional
                if self.state.position(instrument) == 0:
                    self.state.hold_since_ms.pop(instrument, None)
            self.fills += 1
            self.shares += filled
        return filled

    def add_order(
        self,
        order: dict[str, Any],
        now_ms: int,
        depths: dict[str, dict[str, dict[str, int]]],
    ) -> None:
        self.sent += 1
        instrument = order["instrument_id"]
        side = order["side"]
        price = int(order.get("price") or 0)
        quantity = int(order["quantity"])
        order_type = order["order_type"]
        if side == "bid" and order_type in {"limit", "ioc"}:
            if quantity * price > self.state.free_cash():
                self.rejected += 1
                return
        if side == "ask" and quantity > self.state.free_qty(instrument):
            self.rejected += 1
            return

        depth = depths.get(instrument, {"bids": {}, "asks": {}})
        if order_type in {"ioc", "market"}:
            self.consume_book(instrument, side, quantity, price, order_type, depth)
            return

        filled = self.consume_book(instrument, side, quantity, price, "ioc", depth)
        remaining = quantity - filled
        if remaining <= 0:
            return
        order_id = self.next_order_id
        self.next_order_id += 1
        self.state.track_order(
            bot.LiveOrder(
                f"replay-{order_id}",
                order_id,
                instrument,
                side,
                price,
                remaining,
                order["role"],
                now_ms,
            ),
            reserve=True,
        )

    def cancel_order(self, order_id: int) -> None:
        if order_id in self.state.live_orders:
            self.state.drop_order(order_id, release_reserved=True)
            self.cancels += 1

    def on_trade(self, instrument: str, price: int, quantity: int, now_ms: int) -> None:
        for order in list(self.state.live_orders.values()):
            if order.instrument != instrument:
                continue
            crosses = (order.side == "bid" and price <= order.price) or (
                order.side == "ask" and price >= order.price
            )
            if not crosses:
                continue
            take = min(quantity, order.remaining)
            if take <= 0:
                continue
            self.state.release_order_reservation(order, take)
            if order.side == "bid":
                if self.state.position(instrument) == 0:
                    self.state.hold_since_ms[instrument] = now_ms
                self.state.positions[instrument] = self.state.position(instrument) + take
                self.state.cash -= take * order.price
            else:
                self.state.positions[instrument] = self.state.position(instrument) - take
                self.state.cash += take * order.price
            order.remaining -= take
            quantity -= take
            self.fills += 1
            self.shares += take
            if order.remaining <= 0:
                self.state.drop_order(order.order_id)
            if quantity <= 0:
                break

    def on_books(
        self,
        depths: dict[str, dict[str, dict[str, int]]],
        mids: dict[str, int],
        now_ms: int,
    ) -> None:
        self.last_mid.update(mids)
        for order in self.strategy.plan(self.state, depths, now_ms):
            if order.get("action") == "cancel":
                self.cancel_order(int(order["order_id"]))
            else:
                self.add_order(order, now_ms, depths)

    def result(self) -> Result:
        return Result(
            pnl=self.score(),
            cash=self.state.cash,
            positions={instrument: self.state.position(instrument) for instrument in self.last_mid},
            fills=self.fills,
            shares=self.shares,
            sent=self.sent,
            rejected=self.rejected,
            cancels=self.cancels,
        )


def load_books(data_dir: Path, exchange: str, instruments: set[str]):
    by_time: dict[int, tuple[dict[str, dict[str, dict[str, int]]], dict[str, int]]] = {}
    with (data_dir / f"{exchange}_orderbooks.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            instrument = row["instrument"]
            if instrument not in instruments or not row.get("bid1_price") or not row.get("ask1_price"):
                continue
            now = to_int(row["time"])
            depths, mids = by_time.setdefault(now, ({}, {}))
            depths[instrument] = {
                "bids": {
                    str(price): qty
                    for price, qty in ExchangeReplay.levels(row, "bid")
                    if price > 0 and qty > 0
                },
                "asks": {
                    str(price): qty
                    for price, qty in ExchangeReplay.levels(row, "ask")
                    if price > 0 and qty > 0
                },
            }
            mids[instrument] = (to_int(row["bid1_price"]) + to_int(row["ask1_price"])) // 2
    return by_time


def load_trades(data_dir: Path, exchange: str, instruments: set[str]):
    trades = []
    with (data_dir / f"{exchange}_trades.csv").open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["instrument"] in instruments:
                trades.append(
                    (
                        to_int(row["time"]),
                        row["instrument"],
                        to_int(row["price"]),
                        to_int(row["quantity"]),
                    )
                )
    return trades


def replay_exchange(data_dir: Path, exchange: str, configs: list[bot.InstrumentConfig]) -> Result:
    instruments = {config.instrument for config in configs}
    books = load_books(data_dir, exchange, instruments)
    trades = load_trades(data_dir, exchange, instruments)
    replay = ExchangeReplay(exchange, configs)
    events = [(time, 0, idx) for idx, (time, *_rest) in enumerate(trades)]
    events.extend((time, 1, time) for time in books)
    events.sort()
    for _time, kind, ref in events:
        if kind == 0:
            now, instrument, price, quantity = trades[ref]
            replay.on_trade(instrument, price, quantity, now)
        else:
            depths, mids = books[ref]
            replay.on_books(depths, mids, ref)
    return replay.result()


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay simple_mm_bot over local market_data.")
    parser.add_argument("--data-dir", default="market_data")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    configs = bot.default_configs()
    total = 0
    min_time = None
    max_time = None
    for exchange, cfgs in configs.items():
        result = replay_exchange(data_dir, exchange, cfgs)
        total += result.pnl
        print(
            f"{exchange:8s} pnl={result.pnl:10d} dollars={result.pnl / 100:10.2f} "
            f"fills={result.fills:5d} shares={result.shares:7d} sent={result.sent:4d} "
            f"cancels={result.cancels:4d} rejected={result.rejected:3d} pos={result.positions}"
        )
        with (data_dir / f"{exchange}_orderbooks.csv").open(newline="") as handle:
            for row in csv.DictReader(handle):
                now = to_int(row["time"])
                min_time = now if min_time is None else min(min_time, now)
                max_time = now if max_time is None else max(max_time, now)
    span_s = ((max_time or 0) - (min_time or 0)) / 1000
    projection = total / 100 * 1800 / span_s if span_s > 0 else 0
    print(f"TOTAL cents={total} dollars={total / 100:.2f}")
    print(f"SPAN seconds={span_s:.3f}")
    print(f"PROJECTED_30M dollars={projection:.2f}")


if __name__ == "__main__":
    main()
