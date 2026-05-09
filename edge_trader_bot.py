#!/usr/bin/env python3
"""
AlgoTrade 2026 edge trader.

Trades the edges found by analyzerbot.py:
  1. ETF fair-value dislocations versus ZSE basket constituents.
  2. Recurring cross-venue routes from the market_data capture.
  3. Large CARD cross-venue divergence.

Default mode is dry-run. Set LIVE_TRADING=1 to send orders.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


EXCHANGE_PORT = 9001
EXCHANGE_HOSTS: dict[str, str] = {
    "NYSE": "10.0.201.2",
    "NASDAQ": "10.0.202.2",
    "SSE": "10.0.203.2",
    "JPX": "10.0.204.2",
    "Euronext": "10.0.205.2",
    "LSE": "10.0.206.2",
    "HKEX": "10.0.207.2",
    "NSE": "10.0.208.2",
    "TMX": "10.0.209.2",
    "ZSE": "10.0.210.2",
}
ALL_EXCHANGES = list(EXCHANGE_HOSTS)

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA": ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB": ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}

ETF_LISTINGS: dict[str, list[str]] = {
    "ETFA": ["NYSE", "Euronext", "HKEX", "ZSE"],
    "ETFB": ["NASDAQ", "LSE", "HKEX", "ZSE"],
    "ETFA3": ["NYSE", "TMX", "ZSE"],
    "ETFB3": ["NASDAQ", "HKEX", "ZSE"],
    "ETFSH": ["Euronext", "JPX", "ZSE"],
}

STARTING_CASH_CENTS = 10_000_000


@dataclass(frozen=True)
class Route:
    ticker: str
    buy_exchange: str
    sell_exchange: str
    threshold_cents: int


# Recurrent routes found in the local market_data capture. Thresholds are
# deliberately below the observed average edge but above ordinary spread noise.
ROUTES: list[Route] = [
    Route("INA", "HKEX", "NASDAQ", 90),
    Route("ETFB3", "HKEX", "NASDAQ", 35),
    Route("KOTD", "NASDAQ", "HKEX", 25),
    Route("ETFB", "HKEX", "NASDAQ", 30),
    Route("OIT", "HKEX", "NSE", 30),
    Route("KRAS", "NYSE", "SSE", 55),
    Route("ZITO", "NSE", "NASDAQ", 35),
    Route("FSR", "SSE", "NASDAQ", 60),
    Route("JNAF", "NYSE", "JPX", 70),
    Route("XFR", "HKEX", "NYSE", 45),
    Route("ETFSH", "ZSE", "JPX", 20),
    Route("XAG", "ZSE", "JPX", 40),
    Route("GOLD", "JPX", "NASDAQ", 45),
    Route("KTST", "JPX", "ZSE", 70),
    Route("DDJH", "LSE", "NYSE", 60),
]


class Side(Enum):
    BID = "bid"
    ASK = "ask"


@dataclass(frozen=True)
class Book:
    bids: tuple[tuple[int, int], ...] = ()
    asks: tuple[tuple[int, int], ...] = ()

    @property
    def best_bid(self) -> Optional[int]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return self.asks[0][0] if self.asks else None

    @property
    def best_bid_qty(self) -> int:
        return self.bids[0][1] if self.bids else 0

    @property
    def best_ask_qty(self) -> int:
        return self.asks[0][1] if self.asks else 0

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid


@dataclass(frozen=True)
class Snapshot:
    exchange: str
    instrument_id: str
    book: Book
    exchange_time_ms: int
    monotonic_time: float

    @property
    def ticker(self) -> str:
        return ticker_from_instrument(self.instrument_id)


@dataclass(frozen=True)
class Opportunity:
    source: str
    group_id: str
    exchange: str
    instrument_id: str
    side: Side
    price: int
    quantity: int
    edge_cents: float
    reason: str
    reduce_only: bool = False


@dataclass(frozen=True)
class OpportunityGroup:
    group_id: str
    source: str
    edge_cents: float
    legs: tuple[Opportunity, ...]


@dataclass
class BotConfig:
    exchanges: list[str] = field(default_factory=lambda: ALL_EXCHANGES.copy())
    live_trading: bool = False
    order_quantity: int = 8
    max_groups_per_tick: int = 3
    max_msgs_per_second: int = 350
    etf_threshold_cents: int = 45
    route_threshold_floor_cents: int = 25
    card_threshold_cents: int = 140
    stale_after_seconds: float = 0.7
    cooldown_seconds: float = 0.35
    segment_length_ms: int = 600_000
    flatten_after_ms: int = 575_000
    no_new_risk_after_ms: int = 590_000
    max_long: int = 120
    max_short: int = -80
    min_cash_cents: int = -1_000_000
    expiry_ms: int = 2_000
    reconnect_delay_seconds: float = 2.0
    inventory_interval_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            exchanges=parse_exchanges(os.environ.get("EXCHANGES", ",".join(ALL_EXCHANGES))),
            live_trading=parse_bool(os.environ.get("LIVE_TRADING")),
            order_quantity=parse_int(os.environ.get("EDGE_QTY"), 8),
            max_groups_per_tick=parse_int(os.environ.get("MAX_GROUPS_PER_TICK"), 3),
            max_msgs_per_second=parse_int(os.environ.get("MAX_MSGS_PER_SEC"), 350),
            etf_threshold_cents=parse_int(os.environ.get("ETF_THRESHOLD_CENTS"), 45),
            card_threshold_cents=parse_int(os.environ.get("CARD_THRESHOLD_CENTS"), 140),
            max_long=parse_int(os.environ.get("MAX_LONG"), 120),
            max_short=parse_int(os.environ.get("MAX_SHORT"), -80),
        )


class MarketState:
    def __init__(self) -> None:
        self.books: dict[tuple[str, str], Snapshot] = {}
        self.exchange_times: dict[str, int] = {}

    def apply_market_data(self, exchange: str, message: dict[str, Any]) -> None:
        now = time.monotonic()
        exchange_time_ms = int(message.get("time") or self.exchange_times.get(exchange, 0) or 0)
        self.exchange_times[exchange] = exchange_time_ms
        for instrument_id, raw_book in message.get("orderbook_depths", {}).items():
            self.books[(exchange, instrument_id)] = Snapshot(
                exchange=exchange,
                instrument_id=instrument_id,
                book=parse_orderbook(raw_book),
                exchange_time_ms=exchange_time_ms,
                monotonic_time=now,
            )

    def reset_exchange(self, exchange: str) -> None:
        for key in [key for key in self.books if key[0] == exchange]:
            del self.books[key]
        self.exchange_times.pop(exchange, None)

    def snapshot(self, exchange: str, instrument_id: str) -> Optional[Snapshot]:
        return self.books.get((exchange, instrument_id))

    def fresh_snapshot(
        self,
        exchange: str,
        instrument_id: str,
        now: float,
        max_age_seconds: float,
    ) -> Optional[Snapshot]:
        snapshot = self.snapshot(exchange, instrument_id)
        if snapshot is None:
            return None
        if now - snapshot.monotonic_time > max_age_seconds:
            return None
        if snapshot.book.best_bid is None or snapshot.book.best_ask is None:
            return None
        return snapshot

    def fresh_ticker_quotes(self, ticker: str, now: float, max_age_seconds: float) -> list[Snapshot]:
        result = []
        for snapshot in self.books.values():
            if snapshot.ticker == ticker and now - snapshot.monotonic_time <= max_age_seconds:
                if snapshot.book.best_bid is not None and snapshot.book.best_ask is not None:
                    result.append(snapshot)
        return result

    def zse_component_fair_value(
        self,
        components: list[str],
        now: float,
        max_age_seconds: float,
    ) -> Optional[float]:
        mids = []
        for component in components:
            snapshot = self.fresh_snapshot(
                "ZSE",
                f"ZSE-{component}",
                now,
                max_age_seconds,
            )
            if snapshot is None or snapshot.book.mid is None:
                return None
            mids.append(snapshot.book.mid)
        return sum(mids) / len(mids)

    def segment_time_ms(self, exchange: str, segment_length_ms: int) -> int:
        raw = self.exchange_times.get(exchange, 0)
        return raw % segment_length_ms if segment_length_ms > 0 else raw


class EdgeStrategy:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def find_groups(self, state: MarketState, trigger_exchange: str) -> list[OpportunityGroup]:
        now = time.monotonic()
        groups: list[OpportunityGroup] = []
        groups.extend(self.flatten_groups(state, trigger_exchange, now))
        if self.is_no_new_risk_time(state, trigger_exchange):
            return groups
        groups.extend(self.etf_groups(state, now))
        groups.extend(self.route_groups(state, now))
        groups.extend(self.card_groups(state, now))
        groups.sort(key=lambda group: group.edge_cents, reverse=True)
        return groups

    def is_no_new_risk_time(self, state: MarketState, exchange: str) -> bool:
        return state.segment_time_ms(exchange, self.config.segment_length_ms) >= self.config.no_new_risk_after_ms

    def etf_groups(self, state: MarketState, now: float) -> list[OpportunityGroup]:
        groups: list[OpportunityGroup] = []
        for etf, components in ETF_BASKETS.items():
            fair_value = state.zse_component_fair_value(components, now, self.config.stale_after_seconds)
            if fair_value is None:
                continue
            for exchange in ETF_LISTINGS[etf]:
                snapshot = state.fresh_snapshot(exchange, f"{exchange}-{etf}", now, self.config.stale_after_seconds)
                if snapshot is None:
                    continue
                book = snapshot.book
                if book.best_ask is not None:
                    edge = fair_value - book.best_ask
                    if edge >= self.config.etf_threshold_cents:
                        quantity = min(self.config.order_quantity, book.best_ask_qty)
                        groups.append(
                            self.single_leg_group(
                                "etf_fair_value",
                                f"etf:{exchange}:{etf}:buy",
                                exchange,
                                f"{exchange}-{etf}",
                                Side.BID,
                                book.best_ask,
                                quantity,
                                edge,
                                f"ETF ask below ZSE basket fair value {fair_value:.2f}",
                            )
                        )
                if book.best_bid is not None:
                    edge = book.best_bid - fair_value
                    if edge >= self.config.etf_threshold_cents:
                        quantity = min(self.config.order_quantity, book.best_bid_qty)
                        groups.append(
                            self.single_leg_group(
                                "etf_fair_value",
                                f"etf:{exchange}:{etf}:sell",
                                exchange,
                                f"{exchange}-{etf}",
                                Side.ASK,
                                book.best_bid,
                                quantity,
                                edge,
                                f"ETF bid above ZSE basket fair value {fair_value:.2f}",
                            )
                        )
        return groups

    def route_groups(self, state: MarketState, now: float) -> list[OpportunityGroup]:
        groups: list[OpportunityGroup] = []
        for route in ROUTES:
            buy_snapshot = state.fresh_snapshot(
                route.buy_exchange,
                f"{route.buy_exchange}-{route.ticker}",
                now,
                self.config.stale_after_seconds,
            )
            sell_snapshot = state.fresh_snapshot(
                route.sell_exchange,
                f"{route.sell_exchange}-{route.ticker}",
                now,
                self.config.stale_after_seconds,
            )
            if buy_snapshot is None or sell_snapshot is None:
                continue
            buy_price = buy_snapshot.book.best_ask
            sell_price = sell_snapshot.book.best_bid
            if buy_price is None or sell_price is None:
                continue
            edge = sell_price - buy_price
            threshold = max(route.threshold_cents, self.config.route_threshold_floor_cents)
            if edge < threshold:
                continue
            quantity = min(self.config.order_quantity, buy_snapshot.book.best_ask_qty, sell_snapshot.book.best_bid_qty)
            if quantity <= 0:
                continue
            group_id = f"route:{route.ticker}:{route.buy_exchange}->{route.sell_exchange}"
            groups.append(
                OpportunityGroup(
                    group_id=group_id,
                    source="route_arb",
                    edge_cents=float(edge),
                    legs=(
                        Opportunity(
                            "route_arb",
                            group_id,
                            route.buy_exchange,
                            f"{route.buy_exchange}-{route.ticker}",
                            Side.BID,
                            buy_price,
                            quantity,
                            float(edge),
                            f"buy cheap side of analyzer route, sell {route.sell_exchange}",
                        ),
                        Opportunity(
                            "route_arb",
                            group_id,
                            route.sell_exchange,
                            f"{route.sell_exchange}-{route.ticker}",
                            Side.ASK,
                            sell_price,
                            quantity,
                            float(edge),
                            f"sell rich side of analyzer route, buy {route.buy_exchange}",
                        ),
                    ),
                )
            )
        return groups

    def card_groups(self, state: MarketState, now: float) -> list[OpportunityGroup]:
        quotes = state.fresh_ticker_quotes("CARD", now, self.config.stale_after_seconds)
        if len(quotes) < 2:
            return []
        cheap = min(quotes, key=lambda snapshot: snapshot.book.best_ask or 10**12)
        rich = max(quotes, key=lambda snapshot: snapshot.book.best_bid or -1)
        if cheap.exchange == rich.exchange:
            return []
        buy_price = cheap.book.best_ask
        sell_price = rich.book.best_bid
        if buy_price is None or sell_price is None:
            return []
        edge = sell_price - buy_price
        if edge < self.config.card_threshold_cents:
            return []
        quantity = min(self.config.order_quantity, cheap.book.best_ask_qty, rich.book.best_bid_qty)
        if quantity <= 0:
            return []
        group_id = f"card:{cheap.exchange}->{rich.exchange}"
        return [
            OpportunityGroup(
                group_id=group_id,
                source="card_divergence",
                edge_cents=float(edge),
                legs=(
                    Opportunity(
                        "card_divergence",
                        group_id,
                        cheap.exchange,
                        cheap.instrument_id,
                        Side.BID,
                        buy_price,
                        quantity,
                        float(edge),
                        f"CARD cheap on {cheap.exchange}, rich on {rich.exchange}",
                    ),
                    Opportunity(
                        "card_divergence",
                        group_id,
                        rich.exchange,
                        rich.instrument_id,
                        Side.ASK,
                        sell_price,
                        quantity,
                        float(edge),
                        f"CARD rich on {rich.exchange}, cheap on {cheap.exchange}",
                    ),
                ),
            )
        ]

    def flatten_groups(self, state: MarketState, trigger_exchange: str, now: float) -> list[OpportunityGroup]:
        if state.segment_time_ms(trigger_exchange, self.config.segment_length_ms) < self.config.flatten_after_ms:
            return []
        # Actual inventory sits in RiskManager. Flatten groups are generated there.
        return []

    def single_leg_group(
        self,
        source: str,
        group_id: str,
        exchange: str,
        instrument_id: str,
        side: Side,
        price: int,
        quantity: int,
        edge: float,
        reason: str,
    ) -> OpportunityGroup:
        leg = Opportunity(source, group_id, exchange, instrument_id, side, price, quantity, float(edge), reason)
        return OpportunityGroup(group_id, source, float(edge), (leg,))


class RiskManager:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.positions: dict[str, int] = {}
        self.cash_by_exchange: dict[str, int] = {exchange: STARTING_CASH_CENTS for exchange in ALL_EXCHANGES}

    def reset_exchange(self, exchange: str) -> None:
        for instrument_id in [key for key in self.positions if key.startswith(f"{exchange}-")]:
            del self.positions[instrument_id]
        self.cash_by_exchange[exchange] = STARTING_CASH_CENTS

    def update_inventory(self, exchange: str, data: dict[str, Any]) -> None:
        cash = data.get("$")
        if isinstance(cash, list) and len(cash) >= 2:
            self.cash_by_exchange[exchange] = int(cash[1])
        for instrument_id, pair in data.items():
            if instrument_id == "$":
                continue
            if isinstance(pair, list) and len(pair) >= 2:
                self.positions[instrument_id] = int(pair[1])

    def apply_immediate_fill(self, opportunity: Opportunity, data: dict[str, Any]) -> None:
        inventory_change = data.get("immediate_inventory_change")
        balance_change = data.get("immediate_balance_change")
        if inventory_change is not None:
            self.positions[opportunity.instrument_id] = (
                self.positions.get(opportunity.instrument_id, 0) + int(inventory_change)
            )
        if balance_change is not None:
            self.cash_by_exchange[opportunity.exchange] = (
                self.cash_by_exchange.get(opportunity.exchange, STARTING_CASH_CENTS) + int(balance_change)
            )

    def check_group(self, group: OpportunityGroup, state: MarketState) -> tuple[bool, str]:
        projected_positions = dict(self.positions)
        projected_cash = dict(self.cash_by_exchange)
        for leg in group.legs:
            segment_time = state.segment_time_ms(leg.exchange, self.config.segment_length_ms)
            if not leg.reduce_only and segment_time >= self.config.no_new_risk_after_ms:
                return False, f"{leg.exchange} near segment end"
            if leg.quantity <= 0:
                return False, "quantity not positive"
            if leg.price <= 0:
                return False, "price not positive"

            delta = leg.quantity if leg.side is Side.BID else -leg.quantity
            current_position = projected_positions.get(leg.instrument_id, 0)
            projected_position = current_position + delta
            if projected_position > self.config.max_long:
                return False, f"{leg.instrument_id} long limit"
            if projected_position < self.config.max_short:
                return False, f"{leg.instrument_id} short limit"
            projected_positions[leg.instrument_id] = projected_position

            if leg.side is Side.BID:
                cash = projected_cash.get(leg.exchange, STARTING_CASH_CENTS)
                cash -= leg.price * leg.quantity
                if cash < self.config.min_cash_cents:
                    return False, f"{leg.exchange} cash floor"
                projected_cash[leg.exchange] = cash
            else:
                projected_cash[leg.exchange] = projected_cash.get(leg.exchange, STARTING_CASH_CENTS) + (
                    leg.price * leg.quantity
                )
        return True, "ok"


class TokenBucket:
    def __init__(self, rate: int) -> None:
        self.rate = float(rate)
        self.capacity = float(rate)
        self.tokens = float(rate)
        self.updated_at = time.monotonic()

    async def wait(self) -> None:
        while True:
            now = time.monotonic()
            elapsed = max(0.0, now - self.updated_at)
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.updated_at = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            await asyncio.sleep(0.002)


class ExchangeClient:
    def __init__(self, exchange: str, bot: "EdgeTraderBot") -> None:
        self.exchange = exchange
        self.bot = bot
        self.ws: Any = None
        self.bucket = TokenBucket(bot.config.max_msgs_per_second)

    @property
    def connected(self) -> bool:
        return self.ws is not None

    @property
    def url(self) -> str:
        return f"ws://{EXCHANGE_HOSTS[self.exchange]}:{EXCHANGE_PORT}/trade"

    async def run(self) -> None:
        try:
            from websockets.asyncio.client import connect as ws_connect
        except ImportError:
            from websockets.client import connect as ws_connect  # type: ignore

        while True:
            try:
                async with ws_connect(self.url, max_size=16 * 1024 * 1024) as ws:
                    self.ws = ws
                    self.bot.state.reset_exchange(self.exchange)
                    self.bot.risk.reset_exchange(self.exchange)
                    welcome = json.loads(await ws.recv())
                    print(f"[{self.exchange}] connected: {welcome.get('message', welcome)}")
                    await self.request_inventory()
                    inventory_task = asyncio.create_task(self.inventory_loop())
                    try:
                        async for raw in ws:
                            if raw == "Message rate limit exceeded":
                                print(f"[{self.exchange}] rate limit exceeded")
                                break
                            await self.handle_message(json.loads(raw))
                    finally:
                        inventory_task.cancel()
                        self.ws = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ws = None
                print(f"[{self.exchange}] connection error: {exc}")

            await asyncio.sleep(self.bot.config.reconnect_delay_seconds)

    async def handle_message(self, message: dict[str, Any]) -> None:
        msg_type = message.get("type")
        if msg_type == "market_data_update":
            self.bot.state.apply_market_data(self.exchange, message)
            await self.bot.trade_from_latest(self.exchange)
        elif msg_type == "get_inventory_response":
            self.bot.risk.update_inventory(self.exchange, message.get("data", {}))
        elif msg_type == "add_order_response":
            self.bot.on_add_order_response(message)
        elif msg_type == "end_of_round":
            print(f"[{self.exchange}] segment ended")
            self.bot.state.reset_exchange(self.exchange)
            self.bot.risk.reset_exchange(self.exchange)
        elif msg_type == "error":
            print(f"[{self.exchange}] error: {message.get('message')}")

    async def inventory_loop(self) -> None:
        while True:
            await asyncio.sleep(self.bot.config.inventory_interval_seconds)
            await self.request_inventory()

    async def request_inventory(self) -> None:
        if self.ws is None:
            return
        request = {"type": "get_inventory", "user_request_id": self.bot.next_request_id(self.exchange, "inventory")}
        await self.send_json(request)

    async def send_order(self, opportunity: Opportunity) -> None:
        if self.ws is None:
            return
        request_id = self.bot.next_request_id(self.exchange, "order")
        request = build_add_order(request_id, opportunity, int(time.time() * 1000), self.bot.config.expiry_ms)
        self.bot.inflight[request_id] = opportunity
        await self.send_json(request)

    async def send_json(self, request: dict[str, Any]) -> None:
        await self.bucket.wait()
        await self.ws.send(json.dumps(request, separators=(",", ":")))


class EdgeTraderBot:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.state = MarketState()
        self.strategy = EdgeStrategy(config)
        self.risk = RiskManager(config)
        self.clients: dict[str, ExchangeClient] = {}
        self.inflight: dict[str, Opportunity] = {}
        self.last_sent: dict[str, float] = {}
        self.request_seq = 0

    def next_request_id(self, exchange: str, kind: str) -> str:
        self.request_seq += 1
        return f"{exchange}-{kind}-{int(time.time() * 1000)}-{self.request_seq}"

    async def run(self) -> None:
        self.clients = {exchange: ExchangeClient(exchange, self) for exchange in self.config.exchanges}
        print(
            "edge_trader_bot starting "
            f"exchanges={','.join(self.config.exchanges)} "
            f"live={self.config.live_trading} qty={self.config.order_quantity} "
            f"ETF_THRESHOLD={self.config.etf_threshold_cents} "
            f"CARD_THRESHOLD={self.config.card_threshold_cents}"
        )
        if not self.config.live_trading:
            print("dry-run mode: set LIVE_TRADING=1 to send orders")
        await asyncio.gather(*(client.run() for client in self.clients.values()))

    async def trade_from_latest(self, trigger_exchange: str) -> None:
        if self.state.segment_time_ms(trigger_exchange, self.config.segment_length_ms) >= self.config.flatten_after_ms:
            groups = self.flatten_groups()
        else:
            groups = self.strategy.find_groups(self.state, trigger_exchange)
        sent = 0
        now = time.monotonic()
        for group in groups:
            if sent >= self.config.max_groups_per_tick:
                break
            if now - self.last_sent.get(group.group_id, 0.0) < self.config.cooldown_seconds:
                continue
            if any(leg.exchange not in self.clients or not self.clients[leg.exchange].connected for leg in group.legs):
                continue
            allowed, reason = self.risk.check_group(group, self.state)
            if not allowed:
                continue

            self.last_sent[group.group_id] = now
            if not self.config.live_trading:
                self.print_group(group)
            else:
                await asyncio.gather(*(self.clients[leg.exchange].send_order(leg) for leg in group.legs))
            sent += 1

    def flatten_groups(self) -> list[OpportunityGroup]:
        groups: list[OpportunityGroup] = []
        now = time.monotonic()
        for instrument_id, position in sorted(self.risk.positions.items()):
            if position == 0 or "-" not in instrument_id:
                continue
            exchange = instrument_id.split("-", 1)[0]
            snapshot = self.state.fresh_snapshot(exchange, instrument_id, now, self.config.stale_after_seconds)
            if snapshot is None:
                continue
            if position > 0 and snapshot.book.best_bid is not None:
                quantity = min(abs(position), self.config.order_quantity, snapshot.book.best_bid_qty)
                side = Side.ASK
                price = snapshot.book.best_bid
            elif position < 0 and snapshot.book.best_ask is not None:
                quantity = min(abs(position), self.config.order_quantity, snapshot.book.best_ask_qty)
                side = Side.BID
                price = snapshot.book.best_ask
            else:
                continue
            if quantity <= 0:
                continue
            group_id = f"flatten:{instrument_id}"
            leg = Opportunity(
                "flatten",
                group_id,
                exchange,
                instrument_id,
                side,
                price,
                quantity,
                0.0,
                "end-of-segment inventory reduction",
                reduce_only=True,
            )
            groups.append(OpportunityGroup(group_id, "flatten", 0.0, (leg,)))
        return groups

    def print_group(self, group: OpportunityGroup) -> None:
        legs = " | ".join(
            f"{leg.exchange} {leg.side.value} {leg.quantity} {leg.instrument_id}@{leg.price}"
            for leg in group.legs
        )
        print(f"[DRY] {group.source} edge={group.edge_cents:.1f}c {legs}")

    def on_add_order_response(self, message: dict[str, Any]) -> None:
        request_id = message.get("user_request_id", "")
        opportunity = self.inflight.pop(request_id, None)
        if opportunity is None:
            return
        if not message.get("success", False):
            print(f"[{opportunity.exchange}] rejected {opportunity.instrument_id}: {message.get('data', {}).get('message')}")
            return
        self.risk.apply_immediate_fill(opportunity, message.get("data", {}))


def build_add_order(
    request_id: str,
    opportunity: Opportunity,
    now_ms: int,
    expiry_ms: int,
) -> dict[str, Any]:
    return {
        "type": "add_order",
        "user_request_id": request_id,
        "instrument_id": opportunity.instrument_id,
        "side": opportunity.side.value,
        "quantity": int(opportunity.quantity),
        "order_type": "ioc",
        "price": int(opportunity.price),
        "expiry": int(now_ms + expiry_ms),
    }


def parse_orderbook(raw: dict[str, Any]) -> Book:
    bids = tuple(
        sorted(
            ((int(price), int(quantity)) for price, quantity in raw.get("bids", {}).items()),
            key=lambda item: item[0],
            reverse=True,
        )
    )
    asks = tuple(
        sorted(
            ((int(price), int(quantity)) for price, quantity in raw.get("asks", {}).items()),
            key=lambda item: item[0],
        )
    )
    return Book(bids=bids, asks=asks)


def ticker_from_instrument(instrument_id: str) -> str:
    return instrument_id.split("-", 1)[1] if "-" in instrument_id else instrument_id


def parse_bool(raw: Optional[str]) -> bool:
    return raw is not None and raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def parse_int(raw: Optional[str], default: int) -> int:
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_exchanges(raw: str) -> list[str]:
    aliases = {exchange.upper(): exchange for exchange in ALL_EXCHANGES}
    result = []
    for part in raw.split(","):
        exchange = aliases.get(part.strip().upper())
        if exchange and exchange not in result:
            result.append(exchange)
    return result


def main() -> None:
    config = BotConfig.from_env()
    if not config.exchanges:
        raise SystemExit("No valid exchanges configured")
    asyncio.run(EdgeTraderBot(config).run())


if __name__ == "__main__":
    main()
