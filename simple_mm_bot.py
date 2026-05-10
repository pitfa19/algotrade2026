#!/usr/bin/env python3
"""First-principles CARD/SIMP market maker for AlgoTrade 2026.

The strategy is intentionally small:

* CARD is the primary edge on every exchange.
* SIMP is traded only in tiny, replay-positive lots and never before CARD.
* The bot posts one passive bid per instrument, never posts naked asks, and
  sells long inventory back with IOC/market orders against visible bids.
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
except ImportError:  # pragma: no cover
    websockets = None


INITIAL_CASH = 10_000_000
CASH_FLOOR = -5_000_000
MAX_LONG = 2_000
DEFAULT_RATE_LIMIT = 120
DEFAULT_TTL_MS = 20_000
INVENTORY_SYNC_MS = 500

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
class InstrumentConfig:
    exchange: str
    symbol: str
    bid_price: int
    close_bid_min: int
    lot_size: int
    force_close_after_ms: int
    stale_bid_ms: int

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
    price: int
    quantity: int
    order_type: str
    role: str
    created_ms: int


@dataclass
class ExchangeState:
    exchange: str
    cash: int = INITIAL_CASH
    reserved_cash: int = 0
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    reserved_qty: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    live_orders: dict[int, LiveOrder] = field(default_factory=dict)
    inflight_orders: dict[str, PendingOrder] = field(default_factory=dict)
    inflight_cancels: set[int] = field(default_factory=set)
    hold_since_ms: dict[str, int] = field(default_factory=dict)
    inventory_synced: bool = False
    pending_orders_synced: bool = False
    needs_sync: bool = False

    def synced(self) -> bool:
        return self.inventory_synced and self.pending_orders_synced

    def position(self, instrument: str) -> int:
        return int(self.positions.get(instrument, 0))

    def reserved_for(self, instrument: str) -> int:
        return int(self.reserved_qty.get(instrument, 0))

    def inflight_bid_value(self) -> int:
        return sum(
            max(0, order.quantity) * int(order.price)
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
        self.needs_sync = False

    def track_order(self, order: LiveOrder, *, reserve: bool = False) -> None:
        self.live_orders[int(order.order_id)] = order
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
            self.reserved_cash = max(0, self.reserved_cash - int(order.price) * release_qty)
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

    def track_inflight(self, order: PendingOrder) -> None:
        self.inflight_orders[order.local_id] = order

    def drop_inflight(self, local_id: str | None) -> None:
        if local_id is not None:
            self.inflight_orders.pop(local_id, None)

    def pending_bid_qty(self, instrument: str) -> int:
        total = 0
        for order in self.live_orders.values():
            if order.instrument == instrument and order.side == "bid" and order.role == "bid":
                total += max(0, order.remaining)
        for order in self.inflight_orders.values():
            if order.instrument == instrument and order.side == "bid" and order.role == "bid":
                total += max(0, order.quantity)
        return total

    def mark_unsynced(self) -> None:
        self.inventory_synced = False
        self.needs_sync = True


def default_configs() -> dict[str, list[InstrumentConfig]]:
    card = {
        "NYSE": (9200, 10550, 500, 20000, 3000),
        "NASDAQ": (9100, 10450, 500, 20000, 3000),
        "SSE": (9500, 10150, 2000, 15000, 3000),
        "JPX": (9150, 10550, 1000, 20000, 3000),
        "Euronext": (9100, 10450, 1000, 20000, 3000),
        "LSE": (9500, 10850, 1500, 20000, 3000),
        "HKEX": (9150, 10500, 500, 20000, 3000),
        "NSE": (9100, 10550, 2000, 20000, 3000),
        "TMX": (9100, 10500, 1000, 20000, 3000),
        "ZSE": (9150, 10450, 500, 20000, 3000),
    }
    simp = {
        "NYSE": (9950, 10000, 100, 10000, 8000),
        "NASDAQ": (9970, 10020, 100, 10000, 8000),
        "SSE": (9950, 10040, 50, 10000, 5000),
        "LSE": (9960, 10000, 50, 10000, 3000),
        "NSE": (9950, 10000, 200, 5000, 3000),
        "TMX": (9960, 10000, 50, 10000, 3000),
        "ZSE": (9970, 10000, 50, 8000, 3000),
    }
    configs: dict[str, list[InstrumentConfig]] = {}
    for exchange in EXCHANGES:
        items = [
            InstrumentConfig(exchange, "CARD", *card[exchange]),
        ]
        if exchange in simp:
            items.append(InstrumentConfig(exchange, "SIMP", *simp[exchange]))
        configs[exchange] = items
    return configs


class SimpleMarketMaker:
    def __init__(self, configs: list[InstrumentConfig]) -> None:
        self.configs = configs

    def apply_trade_event(self, state: ExchangeState, event: dict[str, Any]) -> None:
        data = event.get("data", event)
        order_id = data.get("passiveOrderID")
        if order_id is None:
            return
        order = state.live_orders.get(int(order_id))
        if order is None:
            return
        quantity = min(int(data["quantity"]), order.remaining)
        if quantity <= 0:
            return
        state.release_order_reservation(order, quantity)
        if order.side == "bid":
            if state.position(order.instrument) == 0:
                state.hold_since_ms[order.instrument] = int(data.get("time", 0))
            state.positions[order.instrument] = state.position(order.instrument) + quantity
            state.cash -= quantity * int(order.price)
        else:
            state.positions[order.instrument] = state.position(order.instrument) - quantity
            state.cash += quantity * int(order.price)
        order.remaining -= quantity
        state.needs_sync = True
        if order.remaining <= 0:
            state.drop_order(order.order_id)

    def apply_immediate_fill(
        self, state: ExchangeState, order: PendingOrder, data: dict[str, Any]
    ) -> None:
        inv_change = data.get("immediate_inventory_change")
        cash_change = data.get("immediate_balance_change")
        if inv_change is not None:
            before = state.position(order.instrument)
            state.positions[order.instrument] = before + int(inv_change)
            if before == 0 and state.position(order.instrument) > 0:
                state.hold_since_ms[order.instrument] = order.created_ms
            if state.position(order.instrument) == 0:
                state.hold_since_ms.pop(order.instrument, None)
        if cash_change is not None:
            state.cash += int(cash_change)
        if inv_change is not None or cash_change is not None:
            state.needs_sync = True

    def plan(
        self,
        state: ExchangeState,
        depths: dict[str, dict[str, dict[str, int]]],
        now_ms: int,
    ) -> list[dict[str, Any]]:
        if not state.synced():
            return []

        orders: list[dict[str, Any]] = []
        extra_bid_value = 0
        for config in self.configs:
            depth = depths.get(config.instrument)
            orders.extend(self._cancel_stale_bids(state, config, now_ms))

            close = self._plan_close(state, config, depth, now_ms)
            if close is not None:
                orders.append(close)

            pending_bid_qty = state.pending_bid_qty(config.instrument)
            target_room = max(0, config.lot_size - pending_bid_qty)
            position_room = max(0, MAX_LONG - state.position(config.instrument) - pending_bid_qty)
            cash_qty = max(0, (state.free_cash() - extra_bid_value) // config.bid_price)
            qty = min(target_room, position_room, cash_qty)
            if qty > 0:
                order = self._order(
                    config.instrument,
                    "bid",
                    config.bid_price,
                    qty,
                    "limit",
                    "bid",
                )
                orders.append(order)
                extra_bid_value += qty * config.bid_price
        return orders

    def _cancel_stale_bids(
        self, state: ExchangeState, config: InstrumentConfig, now_ms: int
    ) -> list[dict[str, Any]]:
        cancels = []
        for order in state.live_orders.values():
            if order.instrument != config.instrument or order.side != "bid":
                continue
            if order.order_id in state.inflight_cancels:
                continue
            if order.created_ms >= 0 and int(now_ms) - int(order.created_ms) >= config.stale_bid_ms:
                cancels.append(
                    {
                        "action": "cancel",
                        "instrument_id": order.instrument,
                        "order_id": order.order_id,
                        "role": "stale_bid",
                    }
                )
        return cancels

    def _plan_close(
        self,
        state: ExchangeState,
        config: InstrumentConfig,
        depth: dict[str, dict[str, int]] | None,
        now_ms: int,
    ) -> dict[str, Any] | None:
        pos = state.position(config.instrument)
        if pos <= 0 or not depth:
            return None

        hold_since = state.hold_since_ms.get(config.instrument)
        force = hold_since is not None and int(now_ms) - int(hold_since) >= config.force_close_after_ms
        if force:
            qty = min(pos, max(0, state.free_qty(config.instrument)))
            if qty > 0:
                return self._order(config.instrument, "ask", 0, qty, "market", "force_close")
            return None

        qty = min(
            pos,
            max(0, state.free_qty(config.instrument)),
            visible_qty_at_or_above(depth.get("bids", {}), config.close_bid_min),
        )
        if qty <= 0:
            return None
        return self._order(config.instrument, "ask", config.close_bid_min, qty, "ioc", "close")

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


def visible_qty_at_or_above(levels: dict[str, int], threshold: int) -> int:
    total = 0
    for price, qty in levels.items():
        if int(price) >= int(threshold):
            total += int(qty)
    return total


def build_add_order(
    request_id: str,
    instrument: str,
    side: str,
    price: int,
    quantity: int,
    order_type: str,
    ttl_ms: int = DEFAULT_TTL_MS,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "add_order",
        "user_request_id": request_id,
        "instrument_id": instrument,
        "side": side,
        "quantity": int(quantity),
        "order_type": order_type,
    }
    if order_type in {"limit", "ioc"}:
        payload["price"] = int(price)
        payload["expiry"] = int(time.time() * 1000) + int(ttl_ms)
    return payload


def build_cancel_order(request_id: str, instrument: str, order_id: int) -> dict[str, Any]:
    return {
        "type": "cancel_order",
        "user_request_id": request_id,
        "instrument_id": instrument,
        "order_id": int(order_id),
    }


class TokenBucket:
    def __init__(self, rate: int, burst: int | None = None) -> None:
        self.rate = float(rate)
        self.capacity = float(burst if burst is not None else rate)
        self.tokens = self.capacity
        self.updated_at = time.monotonic()

    def take(self) -> bool:
        now = time.monotonic()
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated_at = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


class SimpleMMBot:
    def __init__(self, configs: dict[str, list[InstrumentConfig]], rate_limit: int) -> None:
        self.configs = configs
        self.rate_limit = rate_limit

    async def run(self) -> None:
        await asyncio.gather(
            *(self._run_exchange(exchange, configs) for exchange, configs in self.configs.items())
        )

    async def _run_exchange(self, exchange: str, configs: list[InstrumentConfig]) -> None:
        if websockets is None:
            raise RuntimeError("Install websockets before running the live bot.")
        url = f"ws://{exchange.lower()}.algotrade.hr:9001/trade"
        backoff = 1.0
        while True:
            state = ExchangeState(exchange)
            strategy = SimpleMarketMaker(configs)
            limiter = TokenBucket(self.rate_limit, burst=min(50, self.rate_limit))
            pending: dict[str, PendingOrder] = {}
            pending_cancels: dict[str, int] = {}
            seq = 0
            next_sync_ms = 0
            try:
                async with websockets.connect(url, max_size=16 * 1024 * 1024) as ws:
                    print(f"[{exchange}] connected", flush=True)
                    backoff = 1.0
                    await self._send(ws, limiter, {"type": "get_inventory", "user_request_id": f"{exchange}-inv-0"})
                    await self._send(ws, limiter, {"type": "get_pending_orders", "user_request_id": f"{exchange}-pend-0"})

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
                            continue
                        if msg_type == "get_pending_orders_response":
                            self._hydrate_orders(state, message.get("data", {}))
                            continue
                        if msg_type == "cancel_order_response":
                            self._handle_cancel_response(state, pending_cancels, message)
                            continue
                        if msg_type == "add_order_response":
                            strategy.apply_immediate_fill(
                                state,
                                pending.get(message.get("user_request_id"), PendingOrder("", "", "", 0, 0, "", "", 0)),
                                message.get("data", {}) or {},
                            )
                            self._handle_add_response(state, pending, message)
                            continue
                        if msg_type != "market_data_update":
                            continue

                        now_ms = int(message.get("time", 0))
                        for event in message.get("events", []):
                            if event.get("event_type") == "trade":
                                strategy.apply_trade_event(state, event)
                            elif event.get("event_type") == "cancel":
                                order_id = (event.get("data") or {}).get("orderID")
                                if order_id is not None:
                                    state.drop_order(int(order_id), release_reserved=True)

                        depths = message.get("orderbook_depths", {})
                        for order in strategy.plan(state, depths, now_ms):
                            seq += 1
                            request_id = f"{exchange}-{seq}-{order['role']}"
                            if order.get("action") == "cancel":
                                pending_cancels[request_id] = int(order["order_id"])
                                state.inflight_cancels.add(int(order["order_id"]))
                                await self._send(
                                    ws,
                                    limiter,
                                    build_cancel_order(request_id, order["instrument_id"], order["order_id"]),
                                )
                                continue

                            pending_order = PendingOrder(
                                request_id,
                                order["instrument_id"],
                                order["side"],
                                int(order["price"]),
                                int(order["quantity"]),
                                order["order_type"],
                                order["role"],
                                now_ms,
                            )
                            pending[request_id] = pending_order
                            state.track_inflight(pending_order)
                            await self._send(
                                ws,
                                limiter,
                                build_add_order(
                                    request_id,
                                    order["instrument_id"],
                                    order["side"],
                                    int(order["price"]),
                                    int(order["quantity"]),
                                    order["order_type"],
                                ),
                            )

                        if now_ms >= next_sync_ms or state.needs_sync:
                            seq += 1
                            next_sync_ms = now_ms + INVENTORY_SYNC_MS
                            state.needs_sync = False
                            await self._send(
                                ws,
                                limiter,
                                {"type": "get_inventory", "user_request_id": f"{exchange}-{seq}-inv"},
                            )
            except (OSError, RuntimeError, json.JSONDecodeError, websockets.WebSocketException) as exc:
                print(f"[{exchange}] disconnected: {exc}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(10.0, backoff * 1.5)

    @staticmethod
    def _handle_add_response(
        state: ExchangeState,
        pending: dict[str, PendingOrder],
        message: dict[str, Any],
    ) -> None:
        request_id = message.get("user_request_id")
        order = pending.pop(request_id, None)
        state.drop_inflight(request_id)
        if order is None:
            return
        data = message.get("data", {}) or {}
        if not message.get("success"):
            print(f"[{state.exchange}] add_order failed: {data.get('message')}", flush=True)
            state.mark_unsynced()
            return
        if order.order_type != "limit" or order.role != "bid":
            return
        order_id = data.get("order_id")
        if order_id is None:
            return
        inv_change = abs(int(data.get("immediate_inventory_change") or 0))
        resting = max(0, order.quantity - inv_change)
        if resting > 0:
            state.track_order(
                LiveOrder(
                    order.local_id,
                    int(order_id),
                    order.instrument,
                    order.side,
                    order.price,
                    resting,
                    order.role,
                    order.created_ms,
                ),
                reserve=True,
            )

    @staticmethod
    def _handle_cancel_response(
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
            state.inflight_cancels.discard(order_id)

    @staticmethod
    def _hydrate_orders(state: ExchangeState, data: dict[str, Any]) -> None:
        state.live_orders.clear()
        state.pending_orders_synced = True
        for instrument, sides in (data or {}).items():
            if not isinstance(sides, list) or len(sides) != 2:
                continue
            for side_name, group in (("bid", sides[0]), ("ask", sides[1])):
                for entry in group or []:
                    try:
                        unfilled = int(entry["unfilled_quantity"])
                        if unfilled <= 0:
                            continue
                        state.track_order(
                            LiveOrder(
                                f"hydrated-{entry['orderID']}",
                                int(entry["orderID"]),
                                instrument,
                                side_name,
                                int(entry["price"]),
                                unfilled,
                                "bid" if side_name == "bid" else "ask",
                                int(entry.get("time", -1)),
                            )
                        )
                    except (KeyError, TypeError, ValueError):
                        continue

    @staticmethod
    async def _send(ws: Any, limiter: TokenBucket, payload: dict[str, Any]) -> None:
        while not limiter.take():
            await asyncio.sleep(0.002)
        await ws.send(json.dumps(payload, separators=(",", ":")))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run simple CARD/SIMP market maker.")
    parser.add_argument("--exchanges", default=",".join(EXCHANGES))
    parser.add_argument("--rate-limit", type=int, default=DEFAULT_RATE_LIMIT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    wanted = {item.strip() for item in args.exchanges.split(",") if item.strip()}
    configs = {
        exchange: cfgs
        for exchange, cfgs in default_configs().items()
        if exchange in wanted
    }
    if not configs:
        raise SystemExit("No configured exchanges selected.")
    asyncio.run(SimpleMMBot(configs, args.rate_limit).run())


if __name__ == "__main__":
    main()
