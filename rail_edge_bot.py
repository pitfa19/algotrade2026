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
    # Whether the high rail may short via a *resting* limit ask. Many
    # matching engines (this one included, per server "Insufficient
    # inventory" rejections) only accept resting asks against owned shares.
    # Default off — the symmetric upside catch must come via a crossing
    # IOC instead, which `_plan_close_order` already does for short
    # inventory we accumulate from real fills.
    allow_short_rail: bool = False
    # If we have been holding inventory for longer than this, flatten with a
    # market order. The rail edge was already locked in at the rail-fill price;
    # this just frees capital for the next cycle.
    force_close_after_ms: int = 5_000

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
    # Truth from the most recent get_inventory_response.
    #   reserved_cash  = data["$"][0]       — cash locked in resting bids
    #   reserved_qty[I]= data[I][0]         — shares locked in resting asks of I
    # We use these to compute EXACT free cash / free shares so a new order
    # cannot overshoot the server's hard limits.
    reserved_cash: int = 0
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    reserved_qty: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    live_orders: dict[int, LiveOrder] = field(default_factory=dict)
    inflight_orders: dict[str, PendingOrder] = field(default_factory=dict)
    # last time (server ms) at which `position(instrument)` crossed back to 0
    last_flat_ms: dict[str, int] = field(default_factory=dict)
    inventory_synced: bool = False
    pending_orders_synced: bool = False
    # diagnostic counter — number of add_order rejections since (re)connect
    rejects: int = 0

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

    def inflight_bid_value(self) -> int:
        """Bids we've sent but the server has not yet ack'd (and therefore
        not yet added to its `reserved_cash` accounting). Anything in
        `live_orders` is *already* in server `reserved_cash`."""
        return sum(
            max(0, order.quantity) * int(order.price or 0)
            for order in self.inflight_orders.values()
            if order.side == "bid"
        )

    def inflight_ask_qty(self, instrument: str) -> int:
        """Asks we've sent but the server has not yet ack'd."""
        return sum(
            max(0, order.quantity)
            for order in self.inflight_orders.values()
            if order.side == "ask" and order.instrument == instrument
        )

    def free_cash(self) -> int:
        """Cash the server would still let us reserve right now: total
        balance − what's already reserved server-side − what we've sent
        and not yet had ack'd. Subtract the hard floor to get the
        maximum bid value we can still issue."""
        return self.cash - self.reserved_cash - self.inflight_bid_value() - CASH_FLOOR

    def free_qty(self, instrument: str) -> int:
        """Shares we can put into a NEW resting ask: position −
        already-reserved-by-resting-asks − inflight-asks."""
        return (
            self.position(instrument)
            - self.reserved_for(instrument)
            - self.inflight_ask_qty(instrument)
        )



def default_rail_configs() -> dict[str, RailConfig]:
    # Rails are TAIL CATCHERS: they should only fill when a market order
    # walks the book deep enough to leave the touch. Prices like $105 or
    # $129 sit too close to where CARD/SIMP actually trade, so the rail
    # ends up crossing the touch on every other tick — which on the
    # short-resting side fails as "Insufficient inventory" against a flat
    # account. Pushing them well outside the typical range fixes this.
    #
    # low_bid  = $50  → only fills if a giant market-sell walks through every
    #                   higher resting bid (rare, but the whole edge).
    # high_ask = $150 → only fills when we already own shares AND a giant
    #                   market-buy lifts the asks past $150.
    # close thresholds straddle the natural ~$100 mid so the polite IOC
    # close fires reliably; the 5s force-close is the backstop.
    return {
        "NASDAQ":   RailConfig("NASDAQ",   "CARD", 5000, 15000, 9900, 10100),
        "ZSE":      RailConfig("ZSE",      "CARD", 5000, 15000, 9900, 10100),
        "SSE":      RailConfig("SSE",      "CARD", 5000, 15000, 9900, 10100),
        "LSE":      RailConfig("LSE",      "CARD", 5000, 15000, 9900, 10100),
        "JPX":      RailConfig("JPX",      "CARD", 5000, 15000, 9900, 10100),
        "NSE":      RailConfig("NSE",      "SIMP", 5000, 15000, 9900, 10100),
        "HKEX":     RailConfig("HKEX",     "SIMP", 5000, 15000, 9900, 10100),
        "NYSE":     RailConfig("NYSE",     "CARD", 5000, 15000, 9900, 10100),
        "TMX":      RailConfig("TMX",      "CARD", 5000, 15000, 9900, 10100),
        "Euronext": RailConfig("Euronext", "SIMP", 5000, 15000, 9900, 10100),
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
        # If we haven't had our first inventory + pending-orders snapshot
        # yet, do not plan: any sizing would be against ghost defaults and
        # the server will reject (Insufficient balance / inventory).
        if not state.is_synced():
            return []

        instrument = self.config.instrument
        orders: list[dict[str, Any]] = []

        # 1. Force-flatten path: cancel conflicting rail + market-out.
        orders.extend(self._plan_force_close(state, now_ms))

        # 2. Polite IOC close at the configured "fair" thresholds.
        close_order = self._plan_close_order(state, depth)
        if close_order is not None:
            orders.append(close_order)

        # Reservations made by close + force_close in the units each rail
        # sizer needs. Cancel-actions don't reserve anything (they free a
        # resting order), so we skip them. We also exclude `force_close`
        # market orders from the sizers because their qty is the position
        # we already account for via state.free_*.
        extra_ask_qty   = 0
        extra_bid_value = 0
        for o in orders:
            if o.get("type") == "cancel_order":
                continue
            if o.get("role") == "force_close":
                continue
            if o["side"] == "ask":
                extra_ask_qty   += int(o["quantity"])
            else:
                extra_bid_value += int(o["quantity"]) * int(o.get("price") or 0)

        # 3. Re-arm rails.
        bid_qty = self._rail_bid_quantity(state, extra_bid_value)
        if bid_qty > 0:
            orders.append(
                self._order(
                    instrument=instrument, side="bid",
                    price=self.config.low_bid_price, quantity=bid_qty,
                    order_type="limit", role="rail",
                )
            )

        ask_qty = self._rail_ask_quantity(state, extra_ask_qty)
        if ask_qty > 0:
            orders.append(
                self._order(
                    instrument=instrument, side="ask",
                    price=self.config.high_ask_price, quantity=ask_qty,
                    order_type="limit", role="rail",
                )
            )

        return orders

    def _plan_force_close(
        self, state: ExchangeState, now_ms: int
    ) -> list[dict[str, Any]]:
        """If inventory has been held past `force_close_after_ms`, send an
        aggressive IOC just outside our own rail so we flatten without
        self-trading and without depending on the book having any
        particular liquidity.

        - When long: IOC ask at low_bid_price + 1, qty = pos.
            ‣ matches every bid ≥ that price (MM, other teams), so it
              clears whatever real liquidity exists at fair value.
            ‣ does NOT match our own rail bid at low_bid_price (strictly
              less), so we cannot self-trade.
            ‣ if NO bid exists at that price either, the IOC cancels;
              the timer fires again next tick and we retry.
        - When short: IOC bid at high_ask_price − 1, qty = −pos. (mirror)

        Rails on both sides stay live, so the very next spike still gets
        caught — no cooldown.
        """
        instrument = self.config.instrument
        pos = state.position(instrument)
        if pos == 0:
            state.last_flat_ms[instrument] = int(now_ms)
            return []
        last_flat = state.last_flat_ms.get(instrument)
        if last_flat is None:
            # first non-zero observation — anchor the timer here so the
            # polite IOC close path gets a chance first.
            state.last_flat_ms[instrument] = int(now_ms)
            return []
        if int(now_ms) - last_flat < int(self.config.force_close_after_ms):
            return []

        if pos > 0:
            free = state.free_qty(instrument)
            qty = max(0, min(free, pos))
            if qty <= 0:
                return []
            return [self._order(
                instrument=instrument, side="ask",
                price=int(self.config.low_bid_price) + 1,
                quantity=qty, order_type="ioc", role="force_close",
            )]
        # short side
        cash_qty = state.free_cash() // max(1, int(self.config.high_ask_price) - 1)
        qty = max(0, min(-pos, cash_qty))
        if qty <= 0:
            return []
        return [self._order(
            instrument=instrument, side="bid",
            price=int(self.config.high_ask_price) - 1,
            quantity=qty, order_type="ioc", role="force_close",
        )]

    def _plan_close_order(
        self, state: ExchangeState, depth: dict[str, dict[str, int]] | None
    ) -> dict[str, Any] | None:
        if not depth:
            return None

        instrument = self.config.instrument
        pos = state.position(instrument)

        if pos > 0:
            # SELL into ≥ close_bid_min bids. Cap at FREE shares — i.e.
            # owned minus what's already reserved by other resting/inflight
            # asks — so we never request to sell more than we own.
            free = state.free_qty(instrument)
            qty = min(free, pos,
                      visible_qty_at_or_better(depth.get("bids", {}),
                                               self.config.close_bid_min, "bid"))
            if qty > 0:
                return self._order(
                    instrument=instrument, side="ask",
                    price=self.config.close_bid_min, quantity=qty,
                    order_type="ioc", role="close",
                )

        if pos < 0:
            # BUY against ≤ close_ask_max asks. The cash needed = qty * limit;
            # cap by free_cash so we never overshoot the cash floor.
            need_qty = -pos
            visible = visible_qty_at_or_better(depth.get("asks", {}),
                                               self.config.close_ask_max, "ask")
            cash_qty = state.free_cash() // self.config.close_ask_max
            qty = max(0, min(need_qty, visible, cash_qty))
            if qty > 0:
                return self._order(
                    instrument=instrument, side="bid",
                    price=self.config.close_ask_max, quantity=qty,
                    order_type="ioc", role="close",
                )

        return None

    def _rail_bid_quantity(self, state: ExchangeState, extra_reserved_value: int = 0) -> int:
        """Exact bid sizing using server truth.

        `state.free_cash()` returns the most cash we can still spend: total −
        already-reserved − inflight-bids − hard floor. The new bid's value
        plus any same-tick close reservation MUST stay within that.
        """
        instrument = self.config.instrument
        # 1. respect the resting-target cap (don't stack)
        pending_qty = state.pending_qty(
            instrument, side="bid", price=self.config.low_bid_price, role="rail"
        )
        target_room = max(0, int(self.config.lot_size) - pending_qty)
        # 2. respect the position ceiling
        position_room = max(0, MAX_LONG - state.position(instrument) - pending_qty)
        # 3. EXACT cash check: take the server's free cash, subtract any
        #    cash we've reserved THIS tick (close BIDs covering shorts),
        #    and divide by the bid price.
        free = state.free_cash() - int(extra_reserved_value)
        cash_qty = max(0, free // self.config.low_bid_price)
        return max(0, min(target_room, position_room, cash_qty))

    def _rail_ask_quantity(self, state: ExchangeState, extra_reserved_qty: int = 0) -> int:
        """Exact ask sizing: only sell shares we *actually own and aren't
        already trying to sell*. `state.free_qty()` is position −
        server-reserved-by-resting-asks − inflight-asks."""
        instrument = self.config.instrument
        pending_qty = state.pending_qty(instrument, side="ask", role="rail")
        target_room = max(0, int(self.config.lot_size) - pending_qty)
        free = state.free_qty(instrument) - int(extra_reserved_qty)
        if self.config.allow_short_rail:
            # If shorting via resting asks is permitted on this venue,
            # extend capacity down to MAX_SHORT.
            free = max(free, state.position(instrument) - MAX_SHORT
                       - state.reserved_for(instrument)
                       - state.inflight_ask_qty(instrument)
                       - int(extra_reserved_qty))
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
        verbose: bool = False,
        heartbeat_every_s: float = 5.0,
    ) -> None:
        self.configs = configs
        self.rate_limit = rate_limit
        self.order_ttl_ms = order_ttl_ms
        self.inventory_every_ms = inventory_every_ms
        self.verbose = verbose
        self.heartbeat_every_s = heartbeat_every_s

    async def run(self) -> None:
        await asyncio.gather(
            *(self._run_exchange(exchange, config) for exchange, config in self.configs.items())
        )

    async def _run_exchange(self, exchange: str, config: RailConfig) -> None:
        if websockets is None:
            raise RuntimeError("Install requirements.txt before running the live bot.")

        url = f"ws://{exchange.lower()}.algotrade.hr:9001/trade"
        backoff = 1.0
        # session-wide counters for heartbeat
        sess_fills = 0
        sess_rejects = 0
        sess_realized = 0  # cumulative cash delta from fills
        last_hb = time.monotonic()
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
                            continue

                        if msg_type == "get_pending_orders_response":
                            self._hydrate_live_orders(state, message.get("data", {}))
                            continue

                        if msg_type == "add_order_response":
                            had_immediate_fill = self._handle_add_order_response(
                                exchange, state, strategy, pending, message
                            )
                            if had_immediate_fill:
                                # Re-sync BOTH inventory and pending orders —
                                # a fill or rejection means our local
                                # reserved_cash / reserved_qty are now stale
                                # relative to the server.
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

                        if msg_type != "market_data_update":
                            continue

                        now_ms = int(message.get("time", 0))

                        # snapshot state before event processing so we can
                        # log fills as state diffs after the loop
                        pre_pos  = state.position(config.instrument)
                        pre_cash = state.cash

                        for event in message.get("events", []):
                            if event.get("event_type") == "trade":
                                strategy.apply_trade_event(state, event)
                            elif event.get("event_type") == "cancel":
                                data = event.get("data", {})
                                order_id = data.get("orderID")
                                if order_id is not None:
                                    state.drop_order(int(order_id))

                        post_pos  = state.position(config.instrument)
                        post_cash = state.cash
                        if post_pos != pre_pos or post_cash != pre_cash:
                            sess_fills += 1
                            d_pos  = post_pos - pre_pos
                            d_cash = post_cash - pre_cash
                            sess_realized += d_cash
                            print(
                                f"[{exchange}] FILL {config.symbol} "
                                f"pos {pre_pos:+5d}→{post_pos:+5d} ({d_pos:+5d}) "
                                f"cash ${pre_cash/100:>9,.0f}→${post_cash/100:>9,.0f} "
                                f"({d_cash/100:+,.2f})",
                                flush=True,
                            )

                        # heartbeat: periodic state dump per exchange
                        if time.monotonic() - last_hb >= self.heartbeat_every_s:
                            last_hb = time.monotonic()
                            print(
                                f"[{exchange}] hb {config.symbol} "
                                f"pos={state.position(config.instrument):+d} "
                                f"cash=${state.cash/100:,.0f} "
                                f"live={len(state.live_orders)} "
                                f"fills={sess_fills} rejects={sess_rejects} "
                                f"realized=${sess_realized/100:+,.0f}",
                                flush=True,
                            )

                        depths = message.get("orderbook_depths", {})
                        depth = depths.get(config.instrument)
                        planned = strategy.plan_orders(state, depth, now_ms)
                        for action in planned:
                            seq += 1
                            request_id = f"{exchange}-{seq}-{action['role']}"
                            if action.get("type") == "cancel_order":
                                # drop the live order locally now so the
                                # next plan can't double-cancel.
                                state.drop_order(int(action["order_id"]))
                                await self._send_json(
                                    ws, limiter,
                                    build_cancel_order(
                                        request_id=request_id,
                                        instrument_id=action["instrument_id"],
                                        order_id=int(action["order_id"]),
                                    ),
                                )
                                continue
                            pending_order = PendingOrder(
                                local_id=request_id,
                                instrument=action["instrument_id"],
                                side=action["side"],
                                price=action["price"],
                                quantity=action["quantity"],
                                order_type=action["order_type"],
                                role=action["role"],
                            )
                            pending[request_id] = pending_order
                            state.track_inflight(pending_order)
                            await self._send_json(
                                ws,
                                limiter,
                                build_add_order(
                                    request_id=request_id,
                                    instrument_id=action["instrument_id"],
                                    side=action["side"],
                                    price=action["price"],
                                    quantity=action["quantity"],
                                    order_type=action["order_type"],
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
            state.rejects += 1
            print(f"[{exchange}] add_order failed [{order.role}]: {msg}", flush=True)
            return True

        had_fill = bool(
            data.get("immediate_inventory_change")
            or data.get("immediate_balance_change")
        )

        if order.role == "rail":
            # Rail orders may also have crossed at placement (rare for $70
            # bids but possible for the $110 short-rail when the touch is
            # crossed). Apply that fill before tracking the resting remainder.
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
                    )
        else:
            strategy.apply_immediate_fill(state, order, data)
        return had_fill

    def _hydrate_live_orders(self, state: ExchangeState, data: dict[str, Any]) -> None:
        """Repopulate live_orders from a get_pending_orders_response so our
        local view matches what the server has resting. Without this, any
        rail order that survived a reconnect would be invisible to
        pending_bid_value() and we'd over-deploy cash."""
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
                    )

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
