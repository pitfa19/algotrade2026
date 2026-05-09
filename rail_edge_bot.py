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
CASH_FLOOR = -5_000_000
MAX_LONG = 2_000
MAX_SHORT = -200
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


@dataclass
class PendingOrder:
    local_id: str
    instrument: str
    side: str
    price: int | None
    quantity: int
    order_type: str
    role: str


@dataclass
class ExchangeState:
    exchange: str
    cash: int = INITIAL_CASH
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    live_orders: dict[int, LiveOrder] = field(default_factory=dict)
    inflight_orders: dict[str, PendingOrder] = field(default_factory=dict)

    def position(self, instrument: str) -> int:
        return int(self.positions.get(instrument, 0))

    def apply_inventory(self, data: dict[str, list[int]]) -> None:
        cash_pair = data.get("$")
        if cash_pair is not None:
            self.cash = int(cash_pair[1])

        for instrument, pair in data.items():
            if instrument == "$":
                continue
            self.positions[instrument] = int(pair[1])

    def track_order(
        self,
        local_id: str,
        order_id: int,
        instrument: str,
        side: str,
        price: int,
        quantity: int,
        role: str,
    ) -> None:
        self.live_orders[int(order_id)] = LiveOrder(
            local_id=local_id,
            order_id=int(order_id),
            instrument=instrument,
            side=side,
            price=int(price),
            remaining=int(quantity),
            role=role,
        )

    def drop_order(self, order_id: int) -> None:
        self.live_orders.pop(int(order_id), None)

    def track_inflight(self, order: PendingOrder) -> None:
        self.inflight_orders[order.local_id] = order

    def drop_inflight(self, local_id: str | None) -> None:
        if local_id is not None:
            self.inflight_orders.pop(local_id, None)

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


def default_rail_configs() -> dict[str, RailConfig]:
    return {
        "NASDAQ": RailConfig("NASDAQ", "CARD", 7001, 11000, 10100, 10050),
        "ZSE": RailConfig("ZSE", "CARD", 7001, 11000, 10500, 10050),
        "SSE": RailConfig("SSE", "CARD", 7001, 10500, 10500, 10050),
        "LSE": RailConfig("LSE", "CARD", 7001, 11000, 10500, 10050),
        "JPX": RailConfig("JPX", "CARD", 7001, 10500, 10500, 10050),
        "NSE": RailConfig("NSE", "SIMP", 7001, 12999, 10000, 10050),
        "HKEX": RailConfig("HKEX", "SIMP", 7001, 12900, 10000, 10050),
        "NYSE": RailConfig("NYSE", "CARD", 7001, 11000, 10500, 10050),
        "TMX": RailConfig("TMX", "CARD", 7001, 10500, 10500, 10050),
        "Euronext": RailConfig("Euronext", "SIMP", 7001, 12999, 10000, 10050),
    }


class RailEdgeStrategy:
    def __init__(self, config: RailConfig) -> None:
        self.config = config

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
        del now_ms
        instrument = self.config.instrument
        orders: list[dict[str, Any]] = []

        close_order = self._plan_close_order(state, depth)
        if close_order is not None:
            orders.append(close_order)

        bid_qty = self._rail_bid_quantity(state)
        if bid_qty > 0:
            orders.append(
                self._order(
                    instrument=instrument,
                    side="bid",
                    price=self.config.low_bid_price,
                    quantity=bid_qty,
                    order_type="limit",
                    role="rail",
                )
            )

        ask_qty = self._rail_ask_quantity(state)
        if ask_qty > 0:
            orders.append(
                self._order(
                    instrument=instrument,
                    side="ask",
                    price=self.config.high_ask_price,
                    quantity=ask_qty,
                    order_type="limit",
                    role="rail",
                )
            )

        return orders

    def _plan_close_order(
        self, state: ExchangeState, depth: dict[str, dict[str, int]] | None
    ) -> dict[str, Any] | None:
        if not depth:
            return None

        instrument = self.config.instrument
        pos = state.position(instrument)
        if pos > 0:
            qty = visible_qty_at_or_better(depth.get("bids", {}), self.config.close_bid_min, "bid")
            qty = min(pos, qty)
            if qty > 0:
                return self._order(
                    instrument=instrument,
                    side="ask",
                    price=self.config.close_bid_min,
                    quantity=qty,
                    order_type="ioc",
                    role="close",
                )

        if pos < 0:
            qty = visible_qty_at_or_better(depth.get("asks", {}), self.config.close_ask_max, "ask")
            qty = min(-pos, qty)
            if qty > 0:
                return self._order(
                    instrument=instrument,
                    side="bid",
                    price=self.config.close_ask_max,
                    quantity=qty,
                    order_type="ioc",
                    role="close",
                )

        return None

    def _rail_bid_quantity(self, state: ExchangeState) -> int:
        instrument = self.config.instrument
        position_room = MAX_LONG - state.position(instrument)
        pending_qty = state.pending_qty(
            instrument, side="bid", price=self.config.low_bid_price, role="rail"
        )
        cash_room = state.cash - CASH_FLOOR - state.pending_bid_value()
        cash_qty = max(0, cash_room // self.config.low_bid_price)
        return max(0, min(position_room - pending_qty, cash_qty))

    def _rail_ask_quantity(self, state: ExchangeState) -> int:
        instrument = self.config.instrument
        short_room = state.position(instrument) - MAX_SHORT
        pending_qty = state.pending_qty(
            instrument, side="ask", price=self.config.high_ask_price, role="rail"
        )
        return max(0, short_room - pending_qty)

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
            strategy = RailEdgeStrategy(config)
            limiter = TokenBucket(self.rate_limit, burst=min(self.rate_limit, 100))
            pending: dict[str, PendingOrder] = {}
            next_inventory_ms = 0
            seq = 0
            try:
                async with websockets.connect(url, max_size=16 * 1024 * 1024) as ws:
                    print(f"[{exchange}] connected {url}", flush=True)
                    backoff = 1.0
                    await self._send_json(
                        ws,
                        limiter,
                        {"type": "get_inventory", "user_request_id": f"{exchange}-inventory-0"},
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
                            continue

                        if msg_type == "add_order_response":
                            self._handle_add_order_response(
                                exchange, state, strategy, pending, message
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
                                    state.drop_order(int(order_id))

                        depths = message.get("orderbook_depths", {})
                        depth = depths.get(config.instrument)
                        planned = strategy.plan_orders(state, depth, now_ms)
                        for order in planned:
                            seq += 1
                            request_id = f"{exchange}-{seq}-{order['role']}"
                            pending_order = PendingOrder(
                                local_id=request_id,
                                instrument=order["instrument_id"],
                                side=order["side"],
                                price=order["price"],
                                quantity=order["quantity"],
                                order_type=order["order_type"],
                                role=order["role"],
                            )
                            pending[request_id] = pending_order
                            state.track_inflight(pending_order)
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
    ) -> None:
        request_id = message.get("user_request_id")
        order = pending.pop(request_id, None)
        state.drop_inflight(request_id)
        if order is None:
            return

        data = message.get("data", {})
        if not message.get("success"):
            print(f"[{exchange}] add_order failed: {data.get('message')}", flush=True)
            return

        if order.role == "rail":
            order_id = data.get("order_id")
            if order_id is not None:
                state.track_order(
                    local_id=order.local_id,
                    order_id=int(order_id),
                    instrument=order.instrument,
                    side=order.side,
                    price=int(order.price or 0),
                    quantity=order.quantity,
                    role=order.role,
                )
        else:
            strategy.apply_immediate_fill(state, order, data)

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
