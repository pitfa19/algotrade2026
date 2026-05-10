#!/usr/bin/env python3
"""Rail-liquidity bot for the AlgoTrade 2026 exchange API — v3 adaptive.

Same edge as v2/v3 (CARD AND SIMP per exchange, bid LADDER per
instrument) but every price — rungs, close, and force-close IOC — is
expressed RELATIVE to the venue's current touch. Static $70 / $99
thresholds break the moment an instrument floats anywhere other than
~$100; here a SIMP trading at $77 still gets a meaningful ladder
($72 / $67 / $62 / $57) and a close at the right exit price ($76).

Default ladder per instrument (drops from current touch_bid):

    -$5  × 200   ←  small dips     (catches every meaningful sweep)
    -$10 × 200   ←  mid drops
    -$15 × 200   ←  big drops
    -$20 × 200   ←  deep crashes   (most edge per share)

Close fires when there's a bid within $1 of the touch (so we always
exit at near-fair). Force-close uses an IOC priced one cent ABOVE the
deepest live rung — it can never self-trade against our own ladder
but does cross every external bid above that price.

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
    # Bid LADDER expressed as DROPS from the live touch_bid: tuple of
    # (drop_cents, qty). live rung price = round(touch_bid - drop).
    # A market sell that sweeps the touch fills every rung the sweep
    # reaches, in order. Sort deepest first if you want big-edge rungs
    # to win cash priority when the budget is tight.
    bid_rung_drops: tuple[tuple[int, int], ...]
    # Ask LADDER (lifts ABOVE touch_ask). Empty by default — close path
    # handles long-side exit.
    ask_rung_lifts: tuple[tuple[int, int], ...]
    # Close fires when there's bid liquidity within close_bid_buffer_cents
    # of touch_bid (so we exit at ~fair) or ask liquidity within
    # close_ask_buffer_cents above touch_ask (mirror for shorts).
    close_bid_buffer_cents: int = 100
    close_ask_buffer_cents: int = 100
    # If we haven't seen the touch yet (first ticks after connect),
    # use these absolute prices as fallback. Should be roughly the
    # ~$100 starting price of every instrument in this venue's book.
    fallback_touch_bid: int = 9900
    fallback_touch_ask: int = 10100
    # Round adaptive prices to this grid (in cents) so a 1-cent touch
    # jitter doesn't churn cancel/re-issue every tick.
    price_grid_cents: int = 10
    # Floor for any computed price — refuses to issue an order at, e.g.,
    # zero if touch_bid is somehow tiny.
    min_price_cents: int = 100
    # If we have been holding inventory for longer than this, force-flatten.
    force_close_after_ms: int = 5_000
    # Resting rails that survive this long get proactively cancelled
    # so a stale price doesn't sit on the book forever.
    rail_cancel_after_ms: int = 15_000

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
    # Live touch per instrument, refreshed by the bot on every
    # market_data_update. The strategy reads these to compute adaptive
    # rung / close / force-close prices.
    touch_bid: dict[str, int] = field(default_factory=dict)
    touch_ask: dict[str, int] = field(default_factory=dict)

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

    def update_book(self, instrument: str, depth: dict[str, dict[str, int]]) -> None:
        bids = depth.get("bids") or {}
        asks = depth.get("asks") or {}
        if bids:
            try:
                self.touch_bid[instrument] = max(int(p) for p in bids.keys())
            except ValueError:
                pass
        if asks:
            try:
                self.touch_ask[instrument] = min(int(p) for p in asks.keys())
            except ValueError:
                pass



DEFAULT_BID_RUNG_DROPS: tuple[tuple[int, int], ...] = (
    # (drop_cents_below_touch_bid, qty)  — deepest first.
    # Live rung price = round(touch_bid - drop). On a venue where SIMP
    # touches $77, the ladder becomes $57 / $62 / $67 / $72 — still
    # ~25-30% below market for the deepest rung, ~$5 below for the
    # tightest. On a venue where CARD touches $99 you get $79 / $84 /
    # $89 / $94. The ladder always tracks where price actually is.
    (2000, 200),  # -$20  × 200  ←  catches deep crashes (most edge / share)
    (1500, 200),  # -$15  × 200
    (1000, 200),  # -$10  × 200
    (500,  200),  # -$5   × 200  ←  catches every meaningful sweep
)


def default_rail_configs() -> dict[str, list[RailConfig]]:
    """CARD AND SIMP traded on every exchange, with a 4-rung adaptive
    bid ladder per instrument."""
    out: dict[str, list[RailConfig]] = {}
    for ex in EXCHANGES:
        out[ex] = [
            RailConfig(ex, "CARD", DEFAULT_BID_RUNG_DROPS, ()),
            RailConfig(ex, "SIMP", DEFAULT_BID_RUNG_DROPS, ()),
        ]
    return out


class RailEdgeStrategy:
    def __init__(self, config: RailConfig) -> None:
        self.config = config

    # ─── adaptive prices, all derived from state.touch_* ────────────────
    def _round_down(self, p: int) -> int:
        g = max(1, int(self.config.price_grid_cents))
        return max(int(self.config.min_price_cents), (int(p) // g) * g)

    def _round_up(self, p: int) -> int:
        g = max(1, int(self.config.price_grid_cents))
        return max(int(self.config.min_price_cents),
                   ((int(p) + g - 1) // g) * g)

    def live_touch_bid(self, state: ExchangeState) -> int:
        return int(state.touch_bid.get(
            self.config.instrument, self.config.fallback_touch_bid))

    def live_touch_ask(self, state: ExchangeState) -> int:
        return int(state.touch_ask.get(
            self.config.instrument, self.config.fallback_touch_ask))

    def live_bid_rung_prices(self, state: ExchangeState) -> list[tuple[int, int]]:
        tb = self.live_touch_bid(state)
        return [(self._round_down(tb - drop), int(qty))
                for drop, qty in self.config.bid_rung_drops]

    def live_ask_rung_prices(self, state: ExchangeState) -> list[tuple[int, int]]:
        ta = self.live_touch_ask(state)
        return [(self._round_up(ta + lift), int(qty))
                for lift, qty in self.config.ask_rung_lifts]

    def live_close_bid_min(self, state: ExchangeState) -> int:
        return self._round_down(
            self.live_touch_bid(state) - self.config.close_bid_buffer_cents)

    def live_close_ask_max(self, state: ExchangeState) -> int:
        return self._round_up(
            self.live_touch_ask(state) + self.config.close_ask_buffer_cents)

    def live_max_bid_rung_price(self, state: ExchangeState) -> int:
        return max((p for p, _ in self.live_bid_rung_prices(state)), default=0)

    def live_min_ask_rung_price(self, state: ExchangeState) -> int:
        return min((p for p, _ in self.live_ask_rung_prices(state)), default=10**9)

    # ────────────────────────────────────────────────────────────────────
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
        orders: list[dict[str, Any]] = []

        orders.extend(self._cancel_stale_rail_orders(state, now_ms))

        force_close = self._plan_force_close(state, now_ms)
        if force_close is not None:
            orders.append(force_close)

        close_order = self._plan_close_order(state, depth)
        blocked_by_ask_reservation = self._normal_close_blocked_by_ask_reservation(
            state, depth
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

        # Bid LADDER: one rail bid per (price, qty) rung. Cash budget is
        # decremented as we plan, so deepest (highest-edge) rungs always
        # get filled first if there's not enough room for everything.
        orders.extend(self._plan_rail_bid_rungs(state, extra_bid_value))

        if not blocked_by_ask_reservation:
            orders.extend(self._plan_rail_ask_rungs(state, extra_ask_qty))

        return orders

    def _cancel_stale_rail_orders(
        self, state: ExchangeState, now_ms: int
    ) -> list[dict[str, Any]]:
        """Cancel rails for two reasons:
          1. Age — order older than `rail_cancel_after_ms`.
          2. Price drift — touch moved, our rail price no longer matches
             the current target rung price.
        Either way the next planning pass re-issues at the right price.
        """
        cancels = []
        max_age = int(self.config.rail_cancel_after_ms)
        target_bids = {p for p, _ in self.live_bid_rung_prices(state)}
        target_asks = {p for p, _ in self.live_ask_rung_prices(state)}
        for order in state.live_rail_orders(self.config.instrument):
            if order.order_id in state.inflight_cancels:
                continue
            stale_by_age = (
                order.created_ms >= 0
                and int(now_ms) - int(order.created_ms) >= max_age
            )
            targets = target_bids if order.side == "bid" else target_asks
            stale_by_price = bool(targets) and int(order.price) not in targets
            if not (stale_by_age or stale_by_price):
                continue
            cancels.append({
                "action": "cancel",
                "instrument_id": order.instrument,
                "order_id": order.order_id,
                "role": "stale_rail" if stale_by_age else "rail_refresh",
            })
        return cancels

    def _plan_force_close(
        self, state: ExchangeState, now_ms: int
    ) -> dict[str, Any] | None:
        """If we have been holding inventory past `force_close_after_ms`,
        flatten with an IOC priced just OUTSIDE our own ladder so we
        cannot self-trade against the resting rungs."""
        instrument = self.config.instrument
        pos = state.position(instrument)
        if pos == 0:
            state.last_flat_ms[instrument] = int(now_ms)
            return None
        last_flat = state.last_flat_ms.get(instrument)
        if last_flat is None:
            # first non-zero observation — anchor the timer here so the polite
            # IOC close path gets a chance before we fall back to a hard exit.
            state.last_flat_ms[instrument] = int(now_ms)
            return None
        if int(now_ms) - last_flat < int(self.config.force_close_after_ms):
            return None

        if pos > 0:
            # IOC ASK at live_max_bid_rung_price + 1: cannot match any of
            # our own bid rungs (all at <= max), but crosses every external
            # bid >= that price.
            ioc_price = max(1, self.live_max_bid_rung_price(state) + 1)
            qty = min(pos, max(0, state.free_qty(instrument)))
            if qty <= 0:
                return None
            return self._order(
                instrument=instrument, side="ask",
                price=ioc_price, quantity=qty,
                order_type="ioc", role="force_close",
            )

        # short side
        ioc_price = max(2, self.live_min_ask_rung_price(state) - 1)
        qty = min(-pos, max(0, state.free_cash() // max(1, ioc_price)))
        if qty <= 0:
            return None
        return self._order(
            instrument=instrument, side="bid",
            price=ioc_price, quantity=qty,
            order_type="ioc", role="force_close",
        )

    def _plan_close_order(
        self, state: ExchangeState, depth: dict[str, dict[str, int]] | None
    ) -> dict[str, Any] | None:
        if not depth:
            return None

        instrument = self.config.instrument
        pos = state.position(instrument)
        if pos > 0:
            close_px = self.live_close_bid_min(state)
            free = max(0, state.free_qty(instrument))
            qty = min(
                free, pos,
                visible_qty_at_or_better(depth.get("bids", {}), close_px, "bid"),
            )
            if qty > 0:
                return self._order(
                    instrument=instrument, side="ask",
                    price=close_px, quantity=qty,
                    order_type="ioc", role="close",
                )

        if pos < 0:
            close_px = self.live_close_ask_max(state)
            visible = visible_qty_at_or_better(depth.get("asks", {}), close_px, "ask")
            cash_qty = state.free_cash() // max(1, close_px)
            qty = max(0, min(-pos, visible, cash_qty))
            if qty > 0:
                return self._order(
                    instrument=instrument, side="bid",
                    price=close_px, quantity=qty,
                    order_type="ioc", role="close",
                )
        return None

    def _normal_close_blocked_by_ask_reservation(
        self, state: ExchangeState, depth: dict[str, dict[str, int]] | None
    ) -> bool:
        if not depth:
            return False
        instrument = self.config.instrument
        if state.position(instrument) <= 0:
            return False
        visible_qty = visible_qty_at_or_better(
            depth.get("bids", {}), self.live_close_bid_min(state), "bid"
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

    def _plan_rail_bid_rungs(
        self, state: ExchangeState, extra_reserved_value: int = 0
    ) -> list[dict[str, Any]]:
        """Place one rail bid per LIVE (price, qty) rung. Prices are
        recomputed every tick from state.touch_bid via
        live_bid_rung_prices, so the ladder follows the actual market."""
        instrument = self.config.instrument
        out: list[dict[str, Any]] = []
        free = state.free_cash() - int(extra_reserved_value)
        total_bid_pending = state.pending_qty(instrument, side="bid", role="rail")
        running_position = state.position(instrument) + total_bid_pending
        for price, target_qty in self.live_bid_rung_prices(state):
            pending_at_rung = state.pending_qty(
                instrument, side="bid", price=price, role="rail"
            )
            target_room = max(0, int(target_qty) - pending_at_rung)
            if target_room <= 0:
                continue
            position_room = max(0, MAX_LONG - running_position)
            cash_qty = max(0, free // max(1, int(price)))
            send_qty = min(target_room, position_room, cash_qty)
            if send_qty <= 0:
                continue
            out.append(self._order(
                instrument=instrument, side="bid",
                price=int(price), quantity=int(send_qty),
                order_type="limit", role="rail",
            ))
            free -= send_qty * int(price)
            running_position += send_qty
        return out

    def _plan_rail_ask_rungs(
        self, state: ExchangeState, extra_reserved_qty: int = 0
    ) -> list[dict[str, Any]]:
        """Same shape as the bid ladder, prices live from touch_ask."""
        instrument = self.config.instrument
        out: list[dict[str, Any]] = []
        free = state.free_qty(instrument) - int(extra_reserved_qty)
        for price, target_qty in self.live_ask_rung_prices(state):
            pending_at_rung = state.pending_qty(
                instrument, side="ask", price=price, role="rail"
            )
            target_room = max(0, int(target_qty) - pending_at_rung)
            send_qty = min(target_room, max(0, free))
            if send_qty <= 0:
                continue
            out.append(self._order(
                instrument=instrument, side="ask",
                price=int(price), quantity=int(send_qty),
                order_type="limit", role="rail",
            ))
            free -= send_qty
        return out

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
            strategies = [RailEdgeStrategy(cfg) for cfg in configs]
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
                        # Refresh the live touch for every instrument BEFORE
                        # any strategy plans, so the adaptive rung / close /
                        # force-close prices reflect the latest book.
                        for strategy in strategies:
                            d = depths.get(strategy.config.instrument)
                            if d is not None:
                                state.update_book(strategy.config.instrument, d)
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
    parser = argparse.ArgumentParser(description="Run the AlgoTrade rail edge bot v3.")
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
