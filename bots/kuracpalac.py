#!/usr/bin/env python3
"""Rail-liquidity bot for the AlgoTrade 2026 exchange API.

The edge is deliberately simple:

* keep a passive bid just above the low rail on the selected instrument;
* keep a passive ask at the high rail on the same instrument;
* when either passive rail order fills, close the inventory with IOC orders
  only against visible depth that is back in the normal market.

This file is standalone on purpose. It does not depend on any reference bot
framework, so it can be copied to the team VM and run directly.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

try:
    import websockets
except ImportError:  # pragma: no cover - exercised only on machines without deps.
    websockets = None


INITIAL_CASH = 10_000_000
CASH_FLOOR = -5_000_000        # hard server floor (-$50k)
SOFT_CASH_FLOOR = -4_500_000   # soft floor (-$45k) — keeps a $5k buffer
MAX_LONG = 2_000
SOFT_MAX_LONG = 1_800          # soft long cap — keeps a 200-share buffer
MAX_SHORT = -200
SOFT_MAX_SHORT = -180          # soft short cap
DEFAULT_ORDER_TTL_MS = 20_000


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


@dataclass(frozen=True)
class RailConfig:
    exchange: str
    symbol: str
    low_bid_price: int
    high_ask_price: int
    close_bid_min: int
    close_ask_max: int
    # ---- v2 knobs (defaults preserve old behavior on unspecified configs) ----
    # Hard cap per individual rail ticket. Smaller = capacity can't be blown
    # in a single fill; rail re-posts after each fill via the regular planner.
    lot_size: int = 200
    # Whether the high rail may short via a *resting* limit ask. The exchange
    # validates sufficient inventory for resting asks, so the default is off;
    # the high rail becomes an exit for long inventory, not an unsupported
    # naked short.
    allow_short_rail: bool = False
    # If we have been holding inventory for longer than this, flatten with a
    # market order. The rail edge was already locked in at the rail-fill price;
    # this just frees capital for the next cycle.
    force_close_after_ms: int = 5_000
    # Resting rails that survive this long are usually dead capital. Cancel
    # them proactively instead of waiting for manual cleanup or server expiry.
    rail_cancel_after_ms: int = 15_000
    # Cross-median mode: CARD rails move with the fresh cross-venue median,
    # while SIMP intentionally uses a fixed slow anchor near $100.
    use_cross_median: bool = True
    cross_median_mode: str = "full"
    median_anchor_base: int = 10_000
    median_min_venues: int = 3
    median_max_age_ms: int = 1_000
    rail_reprice_threshold: int = 200
    rail_reprice_min_age_ms: int = 2_000

    @property
    def instrument(self) -> str:
        return f"{self.exchange}-{self.symbol}"


@dataclass
class LiveOrder:
    local_id: str
    order_id: int
    instrument: str
    side: str
    price: int
    remaining: int
    role: str
    created_ms: int = -1


@dataclass
class PendingOrder:
    local_id: str
    instrument: str
    side: str
    price: int | None
    quantity: int
    order_type: str
    role: str
    created_ms: int = -1


@dataclass
class ExchangeState:
    exchange: str
    cash: int = INITIAL_CASH
    # Truth from get_inventory_response:
    #   "$": [reserved_cash, total_cash]
    #   instrument: [reserved_ask_qty, net_position]
    # New order sizing must subtract server-side reservations as well as our
    # local in-flight messages, otherwise the exchange rejects immediately.
    reserved_cash: int = 0
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    reserved_qty: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    live_orders: dict[int, LiveOrder] = field(default_factory=dict)
    inflight_orders: dict[str, PendingOrder] = field(default_factory=dict)
    inflight_cancels: set[int] = field(default_factory=set)
    # last time (server ms) at which `position(instrument)` crossed back to 0
    last_flat_ms: dict[str, int] = field(default_factory=dict)
    inventory_synced: bool = False
    pending_orders_synced: bool = False

    def position(self, instrument: str) -> int:
        return int(self.positions.get(instrument, 0))

    def reserved_for(self, instrument: str) -> int:
        return int(self.reserved_qty.get(instrument, 0))

    def is_synced(self) -> bool:
        return self.inventory_synced and self.pending_orders_synced

    def apply_inventory(self, data: dict[str, list[int]]) -> None:
        cash_pair = data.get("$")
        if cash_pair is not None:
            self.reserved_cash = int(cash_pair[0])
            self.cash = int(cash_pair[1])

        prefix = f"{self.exchange}-"
        for instrument in list(self.positions):
            if instrument.startswith(prefix) and instrument not in data:
                self.positions.pop(instrument, None)
                self.reserved_qty.pop(instrument, None)

        for instrument, pair in data.items():
            if instrument == "$":
                continue
            self.reserved_qty[instrument] = int(pair[0])
            self.positions[instrument] = int(pair[1])
        self.inventory_synced = True

    def track_order(
        self,
        local_id: str,
        order_id: int,
        instrument: str,
        side: str,
        price: int,
        quantity: int,
        role: str,
        created_ms: int = -1,
        reserve: bool = False,
    ) -> None:
        order = LiveOrder(
            local_id=local_id,
            order_id=int(order_id),
            instrument=instrument,
            side=side,
            price=int(price),
            remaining=int(quantity),
            role=role,
            created_ms=int(created_ms),
        )
        self.live_orders[int(order_id)] = order
        if reserve:
            self.reserve_order(order)

    def reserve_order(self, order: LiveOrder) -> None:
        quantity = max(0, int(order.remaining))
        if quantity <= 0:
            return
        if order.side == "bid":
            self.reserved_cash += int(order.price) * quantity
        else:
            self.reserved_qty[order.instrument] += quantity

    def release_order_reservation(
        self, order: LiveOrder, quantity: int | None = None
    ) -> None:
        release_qty = order.remaining if quantity is None else min(order.remaining, int(quantity))
        release_qty = max(0, int(release_qty))
        if release_qty <= 0:
            return
        if order.side == "bid":
            self.reserved_cash = max(
                0, self.reserved_cash - int(order.price) * release_qty
            )
        else:
            self.reserved_qty[order.instrument] = max(
                0, self.reserved_qty[order.instrument] - release_qty
            )

    def drop_order(self, order_id: int, *, release_reserved: bool = False) -> None:
        order = self.live_orders.get(int(order_id))
        if release_reserved and order is not None:
            self.release_order_reservation(order)
        self.live_orders.pop(int(order_id), None)
        self.inflight_cancels.discard(int(order_id))

    def mark_unsynced(self) -> None:
        self.inventory_synced = False
        self.pending_orders_synced = False

    def track_inflight(self, order: PendingOrder) -> None:
        self.inflight_orders[order.local_id] = order

    def drop_inflight(self, local_id: str | None) -> None:
        if local_id is not None:
            self.inflight_orders.pop(local_id, None)

    def track_cancel(self, order_id: int) -> None:
        self.inflight_cancels.add(int(order_id))

    def drop_cancel(self, order_id: int) -> None:
        self.inflight_cancels.discard(int(order_id))

    def live_ask_orders(self, instrument: str) -> list[LiveOrder]:
        return [
            order
            for order in self.live_orders.values()
            if order.instrument == instrument and order.side == "ask"
        ]

    def live_rail_orders(self, instrument: str) -> list[LiveOrder]:
        return [
            order
            for order in self.live_orders.values()
            if order.instrument == instrument and order.role == "rail"
        ]

    def pending_qty(
        self,
        instrument: str,
        side: str | None = None,
        price: int | None = None,
        role: str | None = None,
    ) -> int:
        total = 0
        for order in self.live_orders.values():
            if order.instrument != instrument:
                continue
            if side is not None and order.side != side:
                continue
            if price is not None and order.price != price:
                continue
            if role is not None and order.role != role:
                continue
            total += max(0, order.remaining)
        for order in self.inflight_orders.values():
            if order.instrument != instrument:
                continue
            if side is not None and order.side != side:
                continue
            if price is not None and order.price != price:
                continue
            if role is not None and order.role != role:
                continue
            total += max(0, order.quantity)
        return total

    def pending_bid_value(self) -> int:
        live_value = sum(
            max(0, order.remaining) * order.price
            for order in self.live_orders.values()
            if order.side == "bid"
        )
        inflight_value = sum(
            max(0, order.quantity) * int(order.price or 0)
            for order in self.inflight_orders.values()
            if order.side == "bid"
        )
        return live_value + inflight_value

    def inflight_bid_value(self) -> int:
        return sum(
            max(0, order.quantity) * int(order.price or 0)
            for order in self.inflight_orders.values()
            if order.side == "bid"
        )

    def inflight_ask_qty(self, instrument: str) -> int:
        return sum(
            max(0, order.quantity)
            for order in self.inflight_orders.values()
            if order.side == "ask" and order.instrument == instrument
        )

    def free_cash(self) -> int:
        return self.cash - self.reserved_cash - self.inflight_bid_value() - CASH_FLOOR

    def free_qty(self, instrument: str) -> int:
        return (
            self.position(instrument)
            - self.reserved_for(instrument)
            - self.inflight_ask_qty(instrument)
        )


@dataclass(frozen=True)
class ActivePrices:
    rail_bid: int
    high_ask: int
    close_bid: int
    close_ask: int
    anchor: int | None = None


class CrossMedianOracle:
    def __init__(self, *, min_venues: int = 3, max_age_ms: int = 1_000) -> None:
        self.min_venues = int(min_venues)
        self.max_age_ms = int(max_age_ms)
        self._mids: dict[str, dict[str, tuple[int, int]]] = defaultdict(dict)

    @staticmethod
    def _split_instrument(instrument: str) -> tuple[str, str] | None:
        parts = instrument.split("-", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None
        return parts[0], parts[1]

    @staticmethod
    def _best_bid(levels: dict[str, int]) -> int:
        bids = [
            int(price)
            for price, qty in levels.items()
            if int(price) > 0 and int(qty) > 0
        ]
        return max(bids) if bids else 0

    @staticmethod
    def _best_ask(levels: dict[str, int]) -> int:
        asks = [
            int(price)
            for price, qty in levels.items()
            if int(price) > 0 and int(qty) > 0
        ]
        return min(asks) if asks else 0

    def update(
        self,
        instrument: str,
        depth: dict[str, dict[str, int]] | None,
        now_ms: int,
    ) -> None:
        if not depth:
            return
        split = self._split_instrument(instrument)
        if split is None:
            return
        exchange, symbol = split
        bid = self._best_bid(depth.get("bids", {}))
        ask = self._best_ask(depth.get("asks", {}))
        if bid <= 0 or ask <= 0:
            return
        self._mids[symbol][exchange] = ((bid + ask) // 2, int(now_ms))

    def update_many(
        self,
        depths: dict[str, dict[str, dict[str, int]]],
        now_ms: int,
    ) -> None:
        for instrument, depth in (depths or {}).items():
            self.update(instrument, depth, now_ms)

    def median(
        self,
        symbol: str,
        now_ms: int,
        *,
        min_venues: int | None = None,
        max_age_ms: int | None = None,
    ) -> int | None:
        min_count = self.min_venues if min_venues is None else int(min_venues)
        max_age = self.max_age_ms if max_age_ms is None else int(max_age_ms)
        fresh = [
            mid
            for mid, seen_ms in self._mids.get(symbol, {}).values()
            if int(now_ms) - int(seen_ms) <= max_age
        ]
        if len(fresh) < min_count:
            return None
        fresh.sort()
        middle = len(fresh) // 2
        if len(fresh) % 2:
            return fresh[middle]
        return (fresh[middle - 1] + fresh[middle]) // 2



def default_rail_configs() -> dict[str, RailConfig]:
    # Offline-optimized against local market_data replay. Capital is per
    # exchange, so each venue gets its own rail, close, lot, and recycle
    # timing. The common shape is deliberately aggressive: buy deep downside
    # sweeps, then exit near/above the normal $100-$104 touch.
    return {
        "NASDAQ":   RailConfig("NASDAQ",   "CARD", 9700, 11000, 10100, 10100,
                                lot_size=2000, force_close_after_ms=8000,
                                rail_cancel_after_ms=20000,
                                use_cross_median=False),
        "ZSE":      RailConfig("ZSE",      "CARD", 9500, 11000, 10600, 10100,
                                lot_size=2000, force_close_after_ms=5000,
                                rail_cancel_after_ms=20000,
                                cross_median_mode="floor_bid_static_close"),
        "SSE":      RailConfig("SSE",      "CARD", 9500, 10500, 10100, 10100,
                                lot_size=2000, force_close_after_ms=8000,
                                rail_cancel_after_ms=20000,
                                cross_median_mode="static_bid_dynamic_close"),
        "LSE":      RailConfig("LSE",      "CARD", 9500, 11000, 10600, 10100,
                                lot_size=2000, force_close_after_ms=10000,
                                rail_cancel_after_ms=20000,
                                use_cross_median=False),
        "JPX":      RailConfig("JPX",      "CARD", 9100, 10500, 10500, 10100,
                                lot_size=2000, force_close_after_ms=5000,
                                rail_cancel_after_ms=20000,
                                use_cross_median=False),
        "NSE":      RailConfig("NSE",      "SIMP", 9990, 12999, 10000, 10100,
                                lot_size=200, force_close_after_ms=10000,
                                rail_cancel_after_ms=5000),
        "HKEX":     RailConfig("HKEX",     "SIMP", 9930, 12900, 10100, 10100,
                                lot_size=800, force_close_after_ms=8000,
                                rail_cancel_after_ms=5000),
        "NYSE":     RailConfig("NYSE",     "CARD", 9200, 11000, 10400, 10100,
                                lot_size=2000, force_close_after_ms=2000,
                                rail_cancel_after_ms=15000,
                                cross_median_mode="floor_bid_static_close"),
        "TMX":      RailConfig("TMX",      "CARD", 9100, 10500, 10500, 10100,
                                lot_size=2000, force_close_after_ms=5000,
                                rail_cancel_after_ms=20000,
                                use_cross_median=False),
        "Euronext": RailConfig("Euronext", "SIMP", 9970, 12999, 10050, 10100,
                                lot_size=2000, force_close_after_ms=8000,
                                rail_cancel_after_ms=20000),
    }


class RailEdgeStrategy:
    def __init__(self, config: RailConfig, oracle: CrossMedianOracle | None = None) -> None:
        self.config = config
        self.oracle = oracle

    def apply_trade_event(self, state: ExchangeState, event: dict[str, Any]) -> None:
        data = event.get("data", event)
        order_id = data.get("passiveOrderID")
        if order_id is None:
            return

        order = state.live_orders.get(int(order_id))
        if order is None:
            return

        quantity = min(int(data["quantity"]), order.remaining)
        price = int(data.get("price", order.price))
        if quantity <= 0:
            return

        state.release_order_reservation(order, quantity)
        if order.side == "bid":
            state.positions[order.instrument] = state.position(order.instrument) + quantity
            state.cash -= price * quantity
        else:
            state.positions[order.instrument] = state.position(order.instrument) - quantity
            state.cash += price * quantity

        order.remaining -= quantity
        if order.remaining <= 0:
            state.drop_order(order.order_id)

    def apply_immediate_fill(
        self, state: ExchangeState, order: PendingOrder, response_data: dict[str, Any]
    ) -> None:
        inv_change = response_data.get("immediate_inventory_change")
        cash_change = response_data.get("immediate_balance_change")
        if inv_change is None and cash_change is None:
            return

        if inv_change is not None:
            state.positions[order.instrument] = (
                state.position(order.instrument) + int(inv_change)
            )
        if cash_change is not None:
            state.cash += int(cash_change)

    def plan_orders(
        self,
        state: ExchangeState,
        depth: dict[str, dict[str, int]] | None,
        now_ms: int,
    ) -> list[dict[str, Any]]:
        if not state.is_synced():
            return []

        instrument = self.config.instrument
        prices = self._active_prices(now_ms)
        orders: list[dict[str, Any]] = []

        orders.extend(self._cancel_stale_rail_orders(state, now_ms, prices))

        force_close = self._plan_force_close(state, now_ms, prices)
        if force_close is not None:
            orders.append(force_close)

        close_order = self._plan_close_order(state, depth, prices)
        blocked_by_ask_reservation = self._normal_close_blocked_by_ask_reservation(
            state, depth, prices
        )
        if close_order is not None:
            orders.append(close_order)
        elif blocked_by_ask_reservation:
            orders.extend(self._cancel_ask_orders_to_free_inventory(state))

        extra_ask_qty = 0
        extra_bid_value = 0
        for order in orders:
            if order.get("action") == "cancel":
                continue
            if order["side"] == "ask":
                extra_ask_qty += int(order["quantity"])
            else:
                extra_bid_value += int(order["quantity"]) * int(order.get("price") or 0)

        bid_qty = self._rail_bid_quantity(state, prices.rail_bid, extra_bid_value)
        if bid_qty > 0:
            orders.append(
                self._order(
                    instrument=instrument,
                    side="bid",
                    price=prices.rail_bid,
                    quantity=bid_qty,
                    order_type="limit",
                    role="rail",
                )
            )

        ask_qty = self._rail_ask_quantity(state, extra_ask_qty)
        if ask_qty > 0 and not blocked_by_ask_reservation:
            orders.append(
                self._order(
                    instrument=instrument,
                    side="ask",
                    price=prices.high_ask,
                    quantity=ask_qty,
                    order_type="limit",
                    role="rail",
                )
            )

        return orders

    def _active_prices(self, now_ms: int) -> ActivePrices:
        anchor = self._cross_anchor(now_ms)
        if anchor is None:
            return ActivePrices(
                rail_bid=self.config.low_bid_price,
                high_ask=self.config.high_ask_price,
                close_bid=self.config.close_bid_min,
                close_ask=self.config.close_ask_max,
                anchor=None,
            )

        base = int(self.config.median_anchor_base)
        rail_discount = max(0, base - int(self.config.low_bid_price))
        high_premium = int(self.config.high_ask_price) - base
        close_bid_premium = int(self.config.close_bid_min) - base
        close_ask_premium = int(self.config.close_ask_max) - base

        dynamic_rail_bid = max(1, int(anchor) - rail_discount)
        dynamic_close_bid = max(1, int(anchor) + close_bid_premium)
        dynamic_close_ask = max(1, int(anchor) + close_ask_premium)
        dynamic_high_ask = max(dynamic_close_bid + 1, int(anchor) + high_premium)

        mode = self.config.cross_median_mode
        if mode in {"off", "static"}:
            return ActivePrices(
                rail_bid=self.config.low_bid_price,
                high_ask=self.config.high_ask_price,
                close_bid=self.config.close_bid_min,
                close_ask=self.config.close_ask_max,
                anchor=int(anchor),
            )
        if mode == "floor_bid_static_close":
            rail_bid = max(self.config.low_bid_price, dynamic_rail_bid)
            close_bid = self.config.close_bid_min
            close_ask = self.config.close_ask_max
            high_ask = self.config.high_ask_price
        elif mode == "floor_bid_dynamic_close":
            rail_bid = max(self.config.low_bid_price, dynamic_rail_bid)
            close_bid = max(self.config.close_bid_min, dynamic_close_bid)
            close_ask = max(self.config.close_ask_max, dynamic_close_ask)
            high_ask = max(self.config.high_ask_price, dynamic_high_ask)
        elif mode == "static_bid_dynamic_close":
            rail_bid = self.config.low_bid_price
            close_bid = max(self.config.close_bid_min, dynamic_close_bid)
            close_ask = max(self.config.close_ask_max, dynamic_close_ask)
            high_ask = self.config.high_ask_price
        else:
            rail_bid = dynamic_rail_bid
            close_bid = dynamic_close_bid
            close_ask = dynamic_close_ask
            high_ask = dynamic_high_ask
        return ActivePrices(
            rail_bid=rail_bid,
            high_ask=high_ask,
            close_bid=close_bid,
            close_ask=close_ask,
            anchor=int(anchor),
        )

    def _cross_anchor(self, now_ms: int) -> int | None:
        if not self.config.use_cross_median:
            return None
        if self.config.symbol == "SIMP":
            return int(self.config.median_anchor_base)
        if self.oracle is None:
            return None
        return self.oracle.median(
            self.config.symbol,
            now_ms,
            min_venues=self.config.median_min_venues,
            max_age_ms=self.config.median_max_age_ms,
        )

    def _cancel_stale_rail_orders(
        self, state: ExchangeState, now_ms: int, prices: ActivePrices
    ) -> list[dict[str, Any]]:
        cancels = []
        max_age = int(self.config.rail_cancel_after_ms)
        for order in state.live_rail_orders(self.config.instrument):
            if order.order_id in state.inflight_cancels:
                continue
            target_price = prices.rail_bid if order.side == "bid" else prices.high_ask
            age_ms = int(now_ms) - int(order.created_ms) if order.created_ms >= 0 else 0
            old_enough_to_reprice = (
                order.created_ms >= 0
                and age_ms >= int(self.config.rail_reprice_min_age_ms)
            )
            needs_reprice = old_enough_to_reprice and abs(
                int(order.price) - int(target_price)
            ) >= int(self.config.rail_reprice_threshold)
            is_stale = order.created_ms >= 0 and age_ms >= max_age
            if not needs_reprice and not is_stale:
                continue
            cancels.append(
                {
                    "action": "cancel",
                    "instrument_id": order.instrument,
                    "order_id": order.order_id,
                    "role": "reprice_rail" if needs_reprice else "stale_rail",
                }
            )
        return cancels

    def _plan_force_close(
        self, state: ExchangeState, now_ms: int, prices: ActivePrices
    ) -> dict[str, Any] | None:
        """If we have been holding inventory past `force_close_after_ms`,
        flatten with a market order. Captures whatever liquidity is there,
        guarantees we recycle capital."""
        instrument = self.config.instrument
        pos = state.position(instrument)
        if pos == 0:
            state.last_flat_ms[instrument] = int(now_ms)
            return None
        last_flat = state.last_flat_ms.get(instrument)
        if last_flat is None:
            # first non-zero observation — anchor the timer here so the polite
            # IOC close path gets a chance before we fall back to market.
            state.last_flat_ms[instrument] = int(now_ms)
            return None
        if int(now_ms) - last_flat < int(self.config.force_close_after_ms):
            return None
        # past timeout — flatten, market style.
        if pos > 0:
            qty = min(pos, max(0, state.free_qty(instrument)))
            if qty <= 0:
                return None
            return self._order(
                instrument=instrument,
                side="ask",
                price=0,                # ignored for market
                quantity=qty,
                order_type="market",
                role="force_close",
            )
        qty = min(-pos, max(0, state.free_cash() // max(1, prices.close_ask)))
        if qty <= 0:
            return None
        return self._order(
            instrument=instrument,
            side="bid",
            price=0,
            quantity=qty,
            order_type="market",
            role="force_close",
        )

    def _plan_close_order(
        self,
        state: ExchangeState,
        depth: dict[str, dict[str, int]] | None,
        prices: ActivePrices,
    ) -> dict[str, Any] | None:
        if not depth:
            return None

        instrument = self.config.instrument
        pos = state.position(instrument)
        if pos > 0:
            free = max(0, state.free_qty(instrument))
            qty = min(
                free,
                pos,
                visible_qty_at_or_better(
                    depth.get("bids", {}), prices.close_bid, "bid"
                ),
            )
            if qty > 0:
                return self._order(
                    instrument=instrument,
                    side="ask",
                    price=prices.close_bid,
                    quantity=qty,
                    order_type="ioc",
                    role="close",
                )

        if pos < 0:
            visible = visible_qty_at_or_better(
                depth.get("asks", {}), prices.close_ask, "ask"
            )
            cash_qty = state.free_cash() // prices.close_ask
            qty = max(0, min(-pos, visible, cash_qty))
            if qty > 0:
                return self._order(
                    instrument=instrument,
                    side="bid",
                    price=prices.close_ask,
                    quantity=qty,
                    order_type="ioc",
                    role="close",
                )

        return None

    def _normal_close_blocked_by_ask_reservation(
        self,
        state: ExchangeState,
        depth: dict[str, dict[str, int]] | None,
        prices: ActivePrices,
    ) -> bool:
        if not depth:
            return False

        instrument = self.config.instrument
        if state.position(instrument) <= 0:
            return False

        visible_qty = visible_qty_at_or_better(
            depth.get("bids", {}), prices.close_bid, "bid"
        )
        return visible_qty > 0 and state.pending_qty(instrument, side="ask") > 0

    def _cancel_ask_orders_to_free_inventory(self, state: ExchangeState) -> list[dict[str, Any]]:
        cancels = []
        for order in state.live_ask_orders(self.config.instrument):
            if order.order_id in state.inflight_cancels:
                continue
            cancels.append(
                {
                    "action": "cancel",
                    "instrument_id": order.instrument,
                    "order_id": order.order_id,
                    "role": "free_inventory",
                }
            )
        return cancels

    def _rail_bid_quantity(
        self, state: ExchangeState, bid_price: int, extra_reserved_value: int = 0
    ) -> int:
        instrument = self.config.instrument
        pending_qty = state.pending_qty(instrument, side="bid", role="rail")
        target_room = max(0, int(self.config.lot_size) - pending_qty)
        position_room = max(0, MAX_LONG - state.position(instrument) - pending_qty)
        free = state.free_cash() - int(extra_reserved_value)
        cash_qty = max(0, free // max(1, int(bid_price)))
        return max(0, min(target_room, position_room, cash_qty))

    def _rail_ask_quantity(self, state: ExchangeState, extra_reserved_qty: int = 0) -> int:
        instrument = self.config.instrument
        pending_qty = state.pending_qty(instrument, side="ask", role="rail")
        target_room = max(0, int(self.config.lot_size) - pending_qty)
        free = state.free_qty(instrument) - int(extra_reserved_qty)
        if self.config.allow_short_rail:
            free = max(
                free,
                state.position(instrument)
                - MAX_SHORT
                - state.reserved_for(instrument)
                - state.inflight_ask_qty(instrument)
                - int(extra_reserved_qty),
            )
        return max(0, min(target_room, free))

    @staticmethod
    def _order(
        instrument: str,
        side: str,
        price: int,
        quantity: int,
        order_type: str,
        role: str,
    ) -> dict[str, Any]:
        return {
            "instrument_id": instrument,
            "side": side,
            "price": int(price),
            "quantity": int(quantity),
            "order_type": order_type,
            "role": role,
        }


def visible_qty_at_or_better(levels: dict[str, int], threshold: int, side: str) -> int:
    total = 0
    for raw_price, raw_qty in levels.items():
        price = int(raw_price)
        qty = int(raw_qty)
        if side == "bid" and price >= threshold:
            total += qty
        elif side == "ask" and price <= threshold:
            total += qty
    return total


def build_add_order(
    request_id: str,
    instrument_id: str,
    side: str,
    price: int | None,
    quantity: int,
    order_type: str,
    ttl_ms: int = DEFAULT_ORDER_TTL_MS,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "type": "add_order",
        "user_request_id": request_id,
        "instrument_id": instrument_id,
        "side": side,
        "quantity": int(quantity),
        "order_type": order_type,
    }
    if order_type in {"limit", "ioc"}:
        if price is None:
            raise ValueError("limit and IOC orders require an integer price")
        message["price"] = int(price)
        message["expiry"] = int(time.time() * 1000) + int(ttl_ms)
    return message


def build_cancel_order(request_id: str, instrument_id: str, order_id: int) -> dict[str, Any]:
    return {
        "type": "cancel_order",
        "user_request_id": request_id,
        "instrument_id": instrument_id,
        "order_id": int(order_id),
    }


class TokenBucket:
    def __init__(self, rate_per_second: int, burst: int | None = None) -> None:
        self.rate_per_second = float(rate_per_second)
        self.capacity = float(burst if burst is not None else rate_per_second)
        self.tokens = self.capacity
        self.updated_at = 0.0

    def try_take(self, now: float | None = None, tokens: int = 1) -> bool:
        now = time.monotonic() if now is None else now
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
        self.updated_at = now
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


class RailEdgeBot:
    def __init__(
        self,
        configs: dict[str, RailConfig],
        *,
        rate_limit: int = 450,
        order_ttl_ms: int = DEFAULT_ORDER_TTL_MS,
        inventory_every_ms: int = 1_000,
    ) -> None:
        self.configs = configs
        self.rate_limit = rate_limit
        self.order_ttl_ms = order_ttl_ms
        self.inventory_every_ms = inventory_every_ms
        self.oracle = CrossMedianOracle()

    async def run(self) -> None:
        await asyncio.gather(
            *(self._run_exchange(exchange, config) for exchange, config in self.configs.items())
        )

    async def _run_exchange(self, exchange: str, config: RailConfig) -> None:
        if websockets is None:
            raise RuntimeError("Install requirements.txt before running the live bot.")

        url = f"ws://{exchange.lower()}.algotrade.hr:9001/trade"
        backoff = 1.0
        while True:
            state = ExchangeState(exchange=exchange)
            strategy = RailEdgeStrategy(config, self.oracle)
            limiter = TokenBucket(self.rate_limit, burst=min(self.rate_limit, 100))
            pending: dict[str, PendingOrder] = {}
            pending_cancels: dict[str, int] = {}
            next_inventory_ms = 0
            seq = 0
            sync_logged = False
            try:
                async with websockets.connect(url, max_size=16 * 1024 * 1024) as ws:
                    print(f"[{exchange}] connected {url}", flush=True)
                    backoff = 1.0
                    # On (re)connect, ask for both inventory and any orders
                    # already resting on the server. The pending-orders sync
                    # is critical: rail bids carried over from a previous
                    # session would otherwise appear as server-reserved cash
                    # we don't know about, leading to "insufficient balance"
                    # when we try to add new bids.
                    await self._send_json(
                        ws, limiter,
                        {"type": "get_inventory", "user_request_id": f"{exchange}-inventory-0"},
                    )
                    await self._send_json(
                        ws, limiter,
                        {"type": "get_pending_orders", "user_request_id": f"{exchange}-pending-0"},
                    )

                    async for raw in ws:
                        if raw == "Message rate limit exceeded":
                            raise RuntimeError(raw)
                        message = json.loads(raw)
                        msg_type = message.get("type")

                        if msg_type == "end_of_round":
                            print(f"[{exchange}] end_of_round", flush=True)
                            break

                        if msg_type == "get_inventory_response":
                            state.apply_inventory(message.get("data", {}))
                            if state.is_synced() and not sync_logged:
                                sync_logged = True
                                print(
                                    f"[{exchange}] synced cash={state.cash} "
                                    f"reserved_cash={state.reserved_cash} "
                                    f"live_orders={len(state.live_orders)}",
                                    flush=True,
                                )
                            continue

                        if msg_type == "get_pending_orders_response":
                            self._hydrate_live_orders(state, message.get("data", {}))
                            if state.is_synced() and not sync_logged:
                                sync_logged = True
                                print(
                                    f"[{exchange}] synced cash={state.cash} "
                                    f"reserved_cash={state.reserved_cash} "
                                    f"live_orders={len(state.live_orders)}",
                                    flush=True,
                                )
                            continue

                        if msg_type == "add_order_response":
                            had_immediate_fill = self._handle_add_order_response(
                                exchange, state, strategy, pending, message
                            )
                            if had_immediate_fill:
                                # A fill or rejection changes server-side
                                # reservations; refresh both views before
                                # planning the next ticket.
                                seq += 1
                                await self._send_json(
                                    ws, limiter,
                                    {"type": "get_inventory",
                                     "user_request_id": f"{exchange}-{seq}-postfill"},
                                )
                                seq += 1
                                await self._send_json(
                                    ws, limiter,
                                    {"type": "get_pending_orders",
                                     "user_request_id": f"{exchange}-{seq}-postfill-pend"},
                                )
                            continue

                        if msg_type == "cancel_order_response":
                            self._handle_cancel_order_response(
                                exchange, state, pending_cancels, message
                            )
                            continue

                        if msg_type != "market_data_update":
                            continue

                        now_ms = int(message.get("time", 0))
                        for event in message.get("events", []):
                            if event.get("event_type") == "trade":
                                strategy.apply_trade_event(state, event)
                            elif event.get("event_type") == "cancel":
                                data = event.get("data", {})
                                order_id = data.get("orderID")
                                if order_id is not None:
                                    state.drop_order(int(order_id), release_reserved=True)

                        depths = message.get("orderbook_depths", {})
                        self.oracle.update_many(depths, now_ms)
                        depth = depths.get(config.instrument)
                        planned = strategy.plan_orders(state, depth, now_ms)
                        for order in planned:
                            seq += 1
                            request_id = f"{exchange}-{seq}-{order['role']}"
                            if order.get("action") == "cancel":
                                pending_cancels[request_id] = int(order["order_id"])
                                state.track_cancel(order["order_id"])
                                print(
                                    f"[{exchange}] cancel {order['role']} "
                                    f"{order['instrument_id']} order_id={order['order_id']}",
                                    flush=True,
                                )
                                await self._send_json(
                                    ws,
                                    limiter,
                                    build_cancel_order(
                                        request_id=request_id,
                                        instrument_id=order["instrument_id"],
                                        order_id=order["order_id"],
                                    ),
                                )
                                continue

                            pending_order = PendingOrder(
                                local_id=request_id,
                                instrument=order["instrument_id"],
                                side=order["side"],
                                price=order["price"],
                                quantity=order["quantity"],
                                order_type=order["order_type"],
                                role=order["role"],
                                created_ms=now_ms,
                            )
                            pending[request_id] = pending_order
                            state.track_inflight(pending_order)
                            print(
                                f"[{exchange}] send {order['role']} {order['order_type']} "
                                f"{order['side']} {order['quantity']} "
                                f"{order['instrument_id']}@{order['price']}",
                                flush=True,
                            )
                            await self._send_json(
                                ws,
                                limiter,
                                build_add_order(
                                    request_id=request_id,
                                    instrument_id=order["instrument_id"],
                                    side=order["side"],
                                    price=order["price"],
                                    quantity=order["quantity"],
                                    order_type=order["order_type"],
                                    ttl_ms=self.order_ttl_ms,
                                ),
                            )

                        if now_ms >= next_inventory_ms:
                            seq += 1
                            next_inventory_ms = now_ms + self.inventory_every_ms
                            await self._send_json(
                                ws,
                                limiter,
                                {
                                    "type": "get_inventory",
                                    "user_request_id": f"{exchange}-{seq}-inventory",
                                },
                            )

            except (OSError, websockets.WebSocketException, RuntimeError, json.JSONDecodeError) as exc:
                print(f"[{exchange}] disconnected: {exc}", flush=True)

            await asyncio.sleep(backoff)
            backoff = min(10.0, backoff * 1.5)

    def _handle_add_order_response(
        self,
        exchange: str,
        state: ExchangeState,
        strategy: RailEdgeStrategy,
        pending: dict[str, PendingOrder],
        message: dict[str, Any],
    ) -> bool:
        """Returns True iff the response carried an immediate fill that
        changed our cash view (caller should re-poll inventory)."""
        request_id = message.get("user_request_id")
        order = pending.pop(request_id, None)
        state.drop_inflight(request_id)
        if order is None:
            return False

        data = message.get("data", {})
        if not message.get("success"):
            msg = (data or {}).get("message") if isinstance(data, dict) else None
            print(f"[{exchange}] add_order failed: {msg}", flush=True)
            state.mark_unsynced()
            return True

        had_fill = bool(
            data.get("immediate_inventory_change")
            or data.get("immediate_balance_change")
        )

        if order.role == "rail":
            # Rail orders may also cross at placement if the touch briefly
            # reaches the rail. Apply that fill before tracking the remainder.
            if had_fill:
                strategy.apply_immediate_fill(state, order, data)
            order_id = data.get("order_id")
            if order_id is not None:
                inv_change = int(data.get("immediate_inventory_change") or 0)
                resting_qty = max(0, order.quantity - abs(inv_change))
                if resting_qty > 0:
                    state.track_order(
                        local_id=order.local_id,
                        order_id=int(order_id),
                        instrument=order.instrument,
                        side=order.side,
                        price=int(order.price or 0),
                        quantity=resting_qty,
                        role=order.role,
                        created_ms=order.created_ms,
                        reserve=True,
                    )
        else:
            strategy.apply_immediate_fill(state, order, data)
        if had_fill:
            state.mark_unsynced()
        return had_fill

    def _hydrate_live_orders(self, state: ExchangeState, data: dict[str, Any]) -> None:
        """Repopulate live_orders from a get_pending_orders_response so our
        local view matches what the server has resting. Without this, any
        rail order that survived a reconnect would be invisible to
        pending_bid_value() and we'd over-deploy cash."""
        state.live_orders.clear()
        state.pending_orders_synced = True
        for instrument, sides in (data or {}).items():
            if not isinstance(sides, list) or len(sides) != 2:
                continue
            bid_orders, ask_orders = sides
            for side_name, group in (("bid", bid_orders), ("ask", ask_orders)):
                for entry in group or []:
                    try:
                        oid = int(entry["orderID"])
                        price = int(entry["price"])
                        unfilled = int(entry["unfilled_quantity"])
                        created_ms = int(entry.get("time", -1))
                    except (KeyError, TypeError, ValueError):
                        continue
                    if unfilled <= 0:
                        continue
                    state.live_orders[oid] = LiveOrder(
                        local_id=f"hydrated-{oid}",
                        order_id=oid,
                        instrument=instrument,
                        side=side_name,
                        price=price,
                        remaining=unfilled,
                        role="rail",
                        created_ms=created_ms,
                    )

    def _handle_cancel_order_response(
        self,
        exchange: str,
        state: ExchangeState,
        pending_cancels: dict[str, int],
        message: dict[str, Any],
    ) -> None:
        request_id = message.get("user_request_id")
        order_id = pending_cancels.pop(request_id, None)
        if order_id is None:
            return

        if message.get("success"):
            state.drop_order(order_id, release_reserved=True)
        else:
            print(f"[{exchange}] cancel_order failed: {message.get('message')}", flush=True)
            state.drop_cancel(order_id)

    @staticmethod
    async def _send_json(ws: Any, limiter: TokenBucket, payload: dict[str, Any]) -> None:
        while not limiter.try_take():
            await asyncio.sleep(0.002)
        await ws.send(json.dumps(payload, separators=(",", ":")))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the AlgoTrade rail edge bot.")
    parser.add_argument(
        "--exchanges",
        default=",".join(EXCHANGES),
        help="Comma-separated exchanges to trade; default is all configured exchanges.",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=450,
        help="Client-side messages per second per exchange, below the 500 hard cap.",
    )
    parser.add_argument(
        "--order-ttl-ms",
        type=int,
        default=DEFAULT_ORDER_TTL_MS,
        help="Expiry horizon for resting rail orders and IOC requests.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    wanted = {item.strip() for item in args.exchanges.split(",") if item.strip()}
    configs = {
        exchange: config
        for exchange, config in default_rail_configs().items()
        if exchange in wanted
    }
    if not configs:
        raise SystemExit("No configured exchanges selected.")

    bot = RailEdgeBot(
        configs,
        rate_limit=args.rate_limit,
        order_ttl_ms=args.order_ttl_ms,
    )
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
