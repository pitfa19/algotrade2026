#!/usr/bin/env python3
"""Cross-median scalping rail for the AlgoTrade 2026 exchange API.

This is the active version of parallaxv2.py. It keeps the same safety shape:
one WebSocket per exchange, one shared `ExchangeState` per exchange, and one
strategy per (exchange, instrument). The difference is price discovery. Instead
of waiting with a static $70.01 bid, it builds a fresh cross-venue median for
CARD/SIMP and posts small bid rungs just below that stable anchor.

The goal is high turnover with small, repeatable edge: buy passive dips below
cross median, sell long inventory back near median, and cancel/reprice stale
quotes often enough that the bot does not sit dead for most of a segment.

Standalone on purpose: copy to the team VM and run directly.
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
MAX_LONG = 2_000
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
    # Rungs are expressed as (discount_from_cross_median_cents, target_qty).
    # With a 10_000c median, the defaults quote 9_970, 9_940, and 9_900.
    bid_rungs: tuple[tuple[int, int], ...] = ((30, 60), (60, 80), (100, 100))
    # Sell long inventory as soon as visible bids are within this many cents
    # below cross median. Replay showed 5c was too eager and fed the market
    # force-close path; 20c kept turnover high without dumping every wiggle.
    close_discount_cents: int = 20
    # Fallback anchor until enough venues have fresh books.
    fallback_anchor_price: int = 10_000
    median_min_venues: int = 3
    median_max_age_ms: int = 1_000
    force_close_after_ms: int = 30_000
    rail_cancel_after_ms: int = 8_000
    rail_reprice_threshold: int = 10
    rail_reprice_min_age_ms: int = 1_000
    min_price_cents: int = 100

    @property
    def instrument(self) -> str:
        return f"{self.exchange}-{self.symbol}"


@dataclass(frozen=True)
class PricePlan:
    anchor: int
    bid_rungs: tuple[tuple[int, int], ...]
    close_bid: int
    close_ask: int


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
    ) -> None:
        self.live_orders[int(order_id)] = LiveOrder(
            local_id=local_id,
            order_id=int(order_id),
            instrument=instrument,
            side=side,
            price=int(price),
            remaining=int(quantity),
            role=role,
            created_ms=int(created_ms),
        )

    def drop_order(self, order_id: int) -> None:
        self.live_orders.pop(int(order_id), None)
        self.inflight_cancels.discard(int(order_id))

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
    def _best_bid(levels: dict[str, int]) -> int | None:
        prices = [
            int(price)
            for price, qty in levels.items()
            if int(price) > 0 and int(qty) > 0
        ]
        return max(prices) if prices else None

    @staticmethod
    def _best_ask(levels: dict[str, int]) -> int | None:
        prices = [
            int(price)
            for price, qty in levels.items()
            if int(price) > 0 and int(qty) > 0
        ]
        return min(prices) if prices else None

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
        if bid is None or ask is None:
            return
        self._mids[symbol][exchange] = ((int(bid) + int(ask)) // 2, int(now_ms))

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


def default_rail_configs() -> dict[str, list[RailConfig]]:
    """For every exchange, run CARD and SIMP median-scalp rails in parallel."""
    out: dict[str, list[RailConfig]] = {}
    for ex in EXCHANGES:
        out[ex] = [RailConfig(ex, "CARD"), RailConfig(ex, "SIMP")]
    return out


class RailEdgeStrategy:
    def __init__(
        self, config: RailConfig, oracle: CrossMedianOracle | None = None
    ) -> None:
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
        prices = self._price_plan(depth, now_ms)
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

        extra_bid_value = 0
        for order in orders:
            if order.get("action") == "cancel":
                continue
            if order["side"] == "bid":
                extra_bid_value += int(order["quantity"]) * int(order.get("price") or 0)

        for price, target_qty in prices.bid_rungs:
            bid_qty = self._rail_bid_quantity(
                state,
                price=price,
                target_qty=target_qty,
                extra_reserved_value=extra_bid_value,
            )
            if bid_qty <= 0:
                continue
            orders.append(
                self._order(
                    instrument=instrument,
                    side="bid",
                    price=price,
                    quantity=bid_qty,
                    order_type="limit",
                    role="rail",
                )
            )
            extra_bid_value += bid_qty * price

        return orders

    def _price_plan(
        self, depth: dict[str, dict[str, int]] | None, now_ms: int
    ) -> PricePlan:
        anchor = self._cross_anchor(now_ms)
        if anchor is None:
            anchor = self._local_anchor(depth) or int(self.config.fallback_anchor_price)

        close_bid = max(
            int(self.config.min_price_cents),
            int(anchor) - int(self.config.close_discount_cents),
        )
        close_ask = int(anchor) + int(self.config.close_discount_cents)
        rungs = tuple(
            (
                max(int(self.config.min_price_cents), int(anchor) - int(discount)),
                int(quantity),
            )
            for discount, quantity in self.config.bid_rungs
            if int(quantity) > 0
        )
        return PricePlan(
            anchor=int(anchor),
            bid_rungs=rungs,
            close_bid=close_bid,
            close_ask=close_ask,
        )

    def _cross_anchor(self, now_ms: int) -> int | None:
        if self.oracle is None:
            return None
        return self.oracle.median(
            self.config.symbol,
            now_ms,
            min_venues=self.config.median_min_venues,
            max_age_ms=self.config.median_max_age_ms,
        )

    @staticmethod
    def _local_anchor(depth: dict[str, dict[str, int]] | None) -> int | None:
        if not depth:
            return None
        bid = CrossMedianOracle._best_bid(depth.get("bids", {}))
        ask = CrossMedianOracle._best_ask(depth.get("asks", {}))
        if bid is None or ask is None:
            return None
        return (int(bid) + int(ask)) // 2

    def _cancel_stale_rail_orders(
        self, state: ExchangeState, now_ms: int, prices: PricePlan
    ) -> list[dict[str, Any]]:
        cancels = []
        max_age = int(self.config.rail_cancel_after_ms)
        target_bid_prices = {price for price, _qty in prices.bid_rungs}
        for order in state.live_rail_orders(self.config.instrument):
            if order.order_id in state.inflight_cancels:
                continue
            if order.created_ms < 0:
                continue
            age_ms = int(now_ms) - int(order.created_ms)
            is_stale = age_ms >= max_age
            should_reprice = (
                order.side == "bid"
                and bool(target_bid_prices)
                and order.price not in target_bid_prices
                and age_ms >= int(self.config.rail_reprice_min_age_ms)
                and min(abs(int(order.price) - int(price)) for price in target_bid_prices)
                >= int(self.config.rail_reprice_threshold)
            )
            if not is_stale and not should_reprice:
                continue
            cancels.append(
                {
                    "action": "cancel",
                    "instrument_id": order.instrument,
                    "order_id": order.order_id,
                    "role": "reprice_rail" if should_reprice else "stale_rail",
                }
            )
        return cancels

    def _plan_force_close(
        self, state: ExchangeState, now_ms: int, prices: PricePlan
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
        prices: PricePlan,
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
        prices: PricePlan,
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
        self,
        state: ExchangeState,
        *,
        price: int,
        target_qty: int,
        extra_reserved_value: int = 0,
    ) -> int:
        instrument = self.config.instrument
        pending_qty = state.pending_qty(
            instrument, side="bid", price=price, role="rail"
        )
        target_room = max(0, int(target_qty) - pending_qty)
        position_room = max(0, MAX_LONG - state.position(instrument) - pending_qty)
        free = state.free_cash() - int(extra_reserved_value)
        cash_qty = max(0, free // int(price))
        return max(0, min(target_room, position_room, cash_qty))

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
        configs: dict[str, list[RailConfig]],
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
            *(self._run_exchange(exchange, cfgs) for exchange, cfgs in self.configs.items())
        )

    async def _run_exchange(self, exchange: str, configs: list[RailConfig]) -> None:
        if websockets is None:
            raise RuntimeError("Install requirements.txt before running the live bot.")

        url = f"ws://{exchange.lower()}.algotrade.hr:9001/trade"
        backoff = 1.0
        while True:
            state = ExchangeState(exchange=exchange)
            # one strategy per (exchange, instrument). Shared state means
            # cash and rate-limit are coordinated; each strategy sees the
            # other's just-issued orders via state.inflight_*.
            strategies = [RailEdgeStrategy(cfg, self.oracle) for cfg in configs]
            symbols = ",".join(s.config.symbol for s in strategies)
            limiter = TokenBucket(self.rate_limit, burst=min(self.rate_limit, 100))
            pending: dict[str, PendingOrder] = {}
            pending_cancels: dict[str, int] = {}
            next_inventory_ms = 0
            seq = 0
            sync_logged = False
            try:
                async with websockets.connect(url, max_size=16 * 1024 * 1024) as ws:
                    print(f"[{exchange}] connected {url} (symbols={symbols})", flush=True)
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
                            # apply_immediate_fill / apply_trade_event don't
                            # use self.config — any strategy on this state
                            # works. Use the first one as a stand-in.
                            had_immediate_fill = self._handle_add_order_response(
                                exchange, state, strategies[0], pending, message
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
                                # apply_trade_event uses the order's stored
                                # instrument, not self.config — one call
                                # handles fills for any strategy.
                                strategies[0].apply_trade_event(state, event)
                            elif event.get("event_type") == "cancel":
                                data = event.get("data", {})
                                order_id = data.get("orderID")
                                if order_id is not None:
                                    state.drop_order(int(order_id))

                        depths = message.get("orderbook_depths", {})
                        self.oracle.update_many(depths, now_ms)
                        # Plan and dispatch each strategy in sequence; each
                        # strategy's track_inflight call updates state so the
                        # next strategy's free_cash() / free_qty() reflect
                        # the previous strategy's just-issued orders.
                        for strategy in strategies:
                            depth = depths.get(strategy.config.instrument)
                            planned = strategy.plan_orders(state, depth, now_ms)
                            for order in planned:
                                seq += 1
                                request_id = (
                                    f"{exchange}-{seq}-{order['role']}-"
                                    f"{strategy.config.symbol}"
                                )
                                if order.get("action") == "cancel":
                                    pending_cancels[request_id] = int(order["order_id"])
                                    state.track_cancel(order["order_id"])
                                    print(
                                        f"[{exchange}] cancel {order['role']} "
                                        f"{order['instrument_id']} "
                                        f"order_id={order['order_id']}",
                                        flush=True,
                                    )
                                    await self._send_json(
                                        ws, limiter,
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
                                    f"[{exchange}] send {order['role']} "
                                    f"{order['order_type']} {order['side']} "
                                    f"{order['quantity']} "
                                    f"{order['instrument_id']}@{order['price']}",
                                    flush=True,
                                )
                                await self._send_json(
                                    ws, limiter,
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
                    )
        else:
            strategy.apply_immediate_fill(state, order, data)
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
            state.drop_order(order_id)
        else:
            print(f"[{exchange}] cancel_order failed: {message.get('message')}", flush=True)
            state.drop_cancel(order_id)

    @staticmethod
    async def _send_json(ws: Any, limiter: TokenBucket, payload: dict[str, Any]) -> None:
        while not limiter.try_take():
            await asyncio.sleep(0.002)
        await ws.send(json.dumps(payload, separators=(",", ":")))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the AlgoTrade cross-median scalping rail bot."
    )
    parser.add_argument(
        "--exchanges",
        default=",".join(EXCHANGES),
        help="Comma-separated exchanges to trade; default is all configured exchanges.",
    )
    parser.add_argument(
        "--symbols",
        default="CARD,SIMP",
        help="Comma-separated instrument symbols to trade per exchange.",
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
    wanted_ex  = {x.strip()         for x in args.exchanges.split(",") if x.strip()}
    wanted_sym = {s.strip().upper() for s in args.symbols.split(",")   if s.strip()}
    configs: dict[str, list[RailConfig]] = {}
    for exchange, cfg_list in default_rail_configs().items():
        if exchange not in wanted_ex:
            continue
        keep = [c for c in cfg_list if c.symbol in wanted_sym]
        if keep:
            configs[exchange] = keep
    if not configs:
        raise SystemExit("No (exchange, symbol) pairs selected.")

    bot = RailEdgeBot(
        configs,
        rate_limit=args.rate_limit,
        order_ttl_ms=args.order_ttl_ms,
    )
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
