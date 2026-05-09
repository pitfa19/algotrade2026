#!/usr/bin/env python3
"""
HEGE: latency-aware multi-venue AlgoTrade 2026 bot.

Default mode is dry-run. Set LIVE_TRADING=1 to allow order submission.
The core is intentionally dependency-light: only the live network layer imports
websockets/aiohttp, while the pricing and risk code can be tested offline.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import random
import signal as signal_module
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from statistics import median
from typing import Any, Deque, Iterable, Mapping, MutableMapping, Optional


EXCHANGES: tuple[str, ...] = (
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

EXCHANGE_ALIASES = {name.upper(): name for name in EXCHANGES}
EXCHANGE_ALIASES["EURONEXT"] = "Euronext"

HOSTS: dict[str, str] = {
    "NYSE": "nyse.algotrade.hr",
    "NASDAQ": "nasdaq.algotrade.hr",
    "SSE": "sse.algotrade.hr",
    "JPX": "jpx.algotrade.hr",
    "Euronext": "euronext.algotrade.hr",
    "LSE": "lse.algotrade.hr",
    "HKEX": "hkex.algotrade.hr",
    "NSE": "nse.algotrade.hr",
    "TMX": "tmx.algotrade.hr",
    "ZSE": "zse.algotrade.hr",
}

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
    "NASDAQ": {
        "NYSE": 1,
        "NASDAQ": 0,
        "SSE": 165,
        "JPX": 152,
        "Euronext": 84,
        "LSE": 80,
        "HKEX": 180,
        "NSE": 174,
        "TMX": 11,
        "ZSE": 96,
    },
    "SSE": {
        "NYSE": 165,
        "NASDAQ": 165,
        "SSE": 0,
        "JPX": 18,
        "Euronext": 160,
        "LSE": 156,
        "HKEX": 19,
        "NSE": 54,
        "TMX": 159,
        "ZSE": 145,
    },
    "JPX": {
        "NYSE": 152,
        "NASDAQ": 152,
        "SSE": 18,
        "JPX": 0,
        "Euronext": 145,
        "LSE": 141,
        "HKEX": 37,
        "NSE": 53,
        "TMX": 145,
        "ZSE": 140,
    },
    "Euronext": {
        "NYSE": 84,
        "NASDAQ": 84,
        "SSE": 160,
        "JPX": 145,
        "Euronext": 0,
        "LSE": 6,
        "HKEX": 130,
        "NSE": 130,
        "TMX": 86,
        "ZSE": 22,
    },
    "LSE": {
        "NYSE": 80,
        "NASDAQ": 80,
        "SSE": 156,
        "JPX": 141,
        "Euronext": 6,
        "LSE": 0,
        "HKEX": 135,
        "NSE": 134,
        "TMX": 82,
        "ZSE": 24,
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
    "NSE": {
        "NYSE": 174,
        "NASDAQ": 174,
        "SSE": 54,
        "JPX": 53,
        "Euronext": 130,
        "LSE": 134,
        "HKEX": 53,
        "NSE": 0,
        "TMX": 174,
        "ZSE": 95,
    },
    "TMX": {
        "NYSE": 11,
        "NASDAQ": 11,
        "SSE": 159,
        "JPX": 145,
        "Euronext": 86,
        "LSE": 82,
        "HKEX": 174,
        "NSE": 174,
        "TMX": 0,
        "ZSE": 98,
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
}

SECTOR_A = ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR")
SECTOR_B = ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH")
INDEPENDENTS = ("MDKA", "KRAS", "ZITO", "ZABA", "SIMP", "CARD")
SAFE_HAVENS = ("GOLD", "XAG")
BROAD_MARKET = SECTOR_A + SECTOR_B + ("MDKA", "KRAS", "ZITO", "ZABA")

ETF_BASKETS: dict[str, tuple[str, ...]] = {
    "ETFA": SECTOR_A,
    "ETFB": SECTOR_B,
    "ETFA3": ("NGUP", "KTST", "XFR"),
    "ETFB3": ("KOTD", "INA", "DLKV"),
    "ETFSH": SAFE_HAVENS,
}

ALL = set(EXCHANGES)
STOCK_LISTINGS: dict[str, set[str]] = {
    "CARD": set(ALL),
    "SIMP": set(ALL),
    "NGUP": {"NYSE", "NASDAQ", "Euronext", "TMX", "ZSE"},
    "OIT": {"LSE", "Euronext", "HKEX", "NSE", "ZSE"},
    "KTST": {"NYSE", "JPX", "TMX", "ZSE"},
    "FSR": {"NASDAQ", "LSE", "SSE", "HKEX", "ZSE"},
    "JZRO": {"NYSE", "LSE", "Euronext", "TMX", "ZSE"},
    "XFR": {"NYSE", "HKEX", "TMX", "ZSE"},
    "KOTD": {"NASDAQ", "LSE", "Euronext", "HKEX", "ZSE"},
    "INA": {"NYSE", "NASDAQ", "Euronext", "HKEX", "ZSE"},
    "HT": {"NASDAQ", "LSE", "JPX", "SSE", "TMX", "ZSE"},
    "JNAF": {"NYSE", "Euronext", "JPX", "HKEX", "ZSE"},
    "DLKV": {"NASDAQ", "LSE", "HKEX", "NSE", "ZSE"},
    "DDJH": {"NYSE", "LSE", "Euronext", "TMX", "ZSE"},
    "MDKA": {"NYSE", "LSE", "HKEX", "TMX", "ZSE"},
    "KRAS": {"NYSE", "Euronext", "SSE", "TMX", "ZSE"},
    "ZITO": {"NASDAQ", "LSE", "Euronext", "NSE", "ZSE"},
    "ZABA": {"NYSE", "LSE", "SSE", "NSE", "TMX", "ZSE"},
    "GOLD": {"NASDAQ", "Euronext", "JPX", "TMX", "ZSE"},
    "XAG": {"LSE", "Euronext", "JPX", "ZSE"},
}

ETF_LISTINGS: dict[str, set[str]] = {
    "ETFA": {"NYSE", "Euronext", "HKEX", "ZSE"},
    "ETFB": {"NASDAQ", "LSE", "HKEX", "ZSE"},
    "ETFA3": {"NYSE", "TMX", "ZSE"},
    "ETFB3": {"NASDAQ", "HKEX", "ZSE"},
    "ETFSH": {"Euronext", "JPX", "ZSE"},
}

ALL_TICKERS = tuple(
    sorted(set(STOCK_LISTINGS) | set(ETF_LISTINGS))
)

INITIAL_CASH = 10_000_000
HARD_CASH_FLOOR = -5_000_000
HARD_LONG_LIMIT = 2_000
HARD_SHORT_LIMIT = -200
DEFAULT_ROUND_LENGTH_MS = 600_000


def now_epoch_ms() -> int:
    return time.time_ns() // 1_000_000


def now_mono_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def env_bool(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def canonical_exchange(name: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        raise ValueError("empty exchange name")
    return EXCHANGE_ALIASES.get(cleaned.upper(), cleaned)


def parse_venues(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return EXCHANGES
    venues: list[str] = []
    for part in raw.split(","):
        venue = canonical_exchange(part)
        if venue not in EXCHANGES:
            raise ValueError(f"unknown venue {part!r}")
        if venue not in venues:
            venues.append(venue)
    return tuple(venues)


def instrument_id(exchange: str, ticker: str) -> str:
    return f"{exchange}-{ticker}"


def split_instrument(instrument: str) -> tuple[str, str]:
    exchange, ticker = instrument.split("-", 1)
    return canonical_exchange(exchange), ticker


@dataclass(frozen=True)
class Config:
    live_trading: bool = False
    dry_run: bool = True
    venues: tuple[str, ...] = EXCHANGES
    bot_home: str = "AUTO"
    default_home: str = "ZSE"
    physical_latency_removed: bool = True
    log_json: bool = True
    log_level: str = "INFO"

    max_msgs_per_sec: int = 120
    strategy_tick_ms: int = 100
    signal_cooldown_ms: int = 300
    max_orders_per_tick: int = 18
    max_orders_per_exchange_tick: int = 6
    max_inflight_per_exchange: int = 120
    max_pending_orders_soft: int = 500

    order_size: int = 4
    max_order_qty: int = 25
    arb_lots: int = 1
    flatten_order_qty: int = 20
    ioc_expiry_ms: int = 900
    ioc_slip_cents: int = 1
    flatten_slip_cents: int = 8

    max_symbol_position: int = 120
    max_symbol_short: int = 50
    cash_floor_cents: int = HARD_CASH_FLOOR
    cash_buffer_cents: int = 500_000

    etf_edge_floor_cents: int = 10
    cross_edge_floor_cents: int = 9
    safe_edge_floor_cents: int = 10
    micro_edge_floor_cents: int = 6
    max_trade_spread_cents: int = 80
    min_confidence_bps: int = 2500
    stale_book_ms: int = 750
    reference_stale_ms: int = 1_200
    no_open_last_ms: int = 45_000
    flatten_last_ms: int = 35_000
    flatten_interval_ms: int = 900

    enable_etf_arb: bool = True
    enable_cross_venue: bool = True
    enable_safe_haven: bool = True
    enable_microprice: bool = True
    enable_passive_resting: bool = False
    passive_order_qty: int = 2
    passive_expiry_ms: int = 1_200

    safe_haven_beta_bps: int = 4_500
    momentum_window_ms: int = 3_000

    inventory_poll_ms: int = 1_000
    pending_poll_ms: int = 5_000
    connect_timeout_ms: int = 2_000
    diagnostics_interval_ms: int = 2_000

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "Config":
        env = dict(os.environ if env is None else env)
        live = env_bool(env, "LIVE_TRADING", False)
        explicit_dry = env.get("DRY_RUN")
        dry = not live if explicit_dry is None else env_bool(env, "DRY_RUN", not live)
        bot_home = env.get("BOT_HOME", "AUTO").strip().upper()
        if bot_home != "AUTO":
            bot_home = canonical_exchange(bot_home)
        default_home = canonical_exchange(env.get("DEFAULT_HOME_LOCATION", "ZSE"))
        return cls(
            live_trading=live,
            dry_run=dry or not live,
            venues=parse_venues(env.get("VENUES")),
            bot_home=bot_home,
            default_home=default_home,
            physical_latency_removed=env_bool(env, "PHYSICAL_LATENCY_REMOVED", True),
            log_json=env_bool(env, "LOG_JSON", True),
            log_level=env.get("LOG_LEVEL", "INFO").upper(),
            max_msgs_per_sec=env_int(env, "MAX_MSGS_PER_SEC", 120),
            strategy_tick_ms=env_int(env, "STRATEGY_TICK_MS", 100),
            signal_cooldown_ms=env_int(env, "SIGNAL_COOLDOWN_MS", 300),
            max_orders_per_tick=env_int(env, "MAX_ORDERS_PER_TICK", 18),
            max_orders_per_exchange_tick=env_int(env, "MAX_ORDERS_PER_EXCHANGE_TICK", 6),
            max_inflight_per_exchange=env_int(env, "MAX_INFLIGHT_PER_EXCHANGE", 120),
            max_pending_orders_soft=env_int(env, "MAX_PENDING_ORDERS_SOFT", 500),
            order_size=env_int(env, "ORDER_SIZE", 4),
            max_order_qty=env_int(env, "MAX_ORDER_QTY", 25),
            arb_lots=env_int(env, "ARB_LOTS", 1),
            flatten_order_qty=env_int(env, "FLATTEN_ORDER_QTY", 20),
            ioc_expiry_ms=env_int(env, "IOC_EXPIRY_MS", 900),
            ioc_slip_cents=env_int(env, "IOC_SLIP_CENTS", 1),
            flatten_slip_cents=env_int(env, "FLATTEN_SLIP_CENTS", 8),
            max_symbol_position=env_int(env, "MAX_SYMBOL_POSITION", 120),
            max_symbol_short=env_int(env, "MAX_SYMBOL_SHORT", 50),
            cash_floor_cents=env_int(env, "CASH_FLOOR_CENTS", HARD_CASH_FLOOR),
            cash_buffer_cents=env_int(env, "CASH_BUFFER_CENTS", 500_000),
            etf_edge_floor_cents=env_int(env, "ETF_EDGE_FLOOR_CENTS", 10),
            cross_edge_floor_cents=env_int(env, "CROSS_EDGE_FLOOR_CENTS", 9),
            safe_edge_floor_cents=env_int(env, "SAFE_EDGE_FLOOR_CENTS", 10),
            micro_edge_floor_cents=env_int(env, "MICRO_EDGE_FLOOR_CENTS", 6),
            max_trade_spread_cents=env_int(env, "MAX_TRADE_SPREAD_CENTS", 80),
            min_confidence_bps=env_int(env, "MIN_CONFIDENCE_BPS", 2500),
            stale_book_ms=env_int(env, "STALE_BOOK_MS", 750),
            reference_stale_ms=env_int(env, "REFERENCE_STALE_MS", 1200),
            no_open_last_ms=env_int(env, "NO_OPEN_LAST_MS", 45_000),
            flatten_last_ms=env_int(env, "FLATTEN_LAST_MS", 35_000),
            flatten_interval_ms=env_int(env, "FLATTEN_INTERVAL_MS", 900),
            enable_etf_arb=env_bool(env, "ENABLE_ETF_ARB", True),
            enable_cross_venue=env_bool(env, "ENABLE_CROSS_VENUE", True),
            enable_safe_haven=env_bool(env, "ENABLE_SAFE_HAVEN", True),
            enable_microprice=env_bool(env, "ENABLE_MICROPRICE", True),
            enable_passive_resting=env_bool(env, "ENABLE_PASSIVE_RESTING", False),
            passive_order_qty=env_int(env, "PASSIVE_ORDER_QTY", 2),
            passive_expiry_ms=env_int(env, "PASSIVE_EXPIRY_MS", 1200),
            safe_haven_beta_bps=env_int(env, "SAFE_HAVEN_BETA_BPS", 4500),
            momentum_window_ms=env_int(env, "MOMENTUM_WINDOW_MS", 3000),
            inventory_poll_ms=env_int(env, "INVENTORY_POLL_MS", 1000),
            pending_poll_ms=env_int(env, "PENDING_POLL_MS", 5000),
            connect_timeout_ms=env_int(env, "CONNECT_TIMEOUT_MS", 2000),
            diagnostics_interval_ms=env_int(env, "DIAGNOSTICS_INTERVAL_MS", 2000),
        )

    @property
    def active_home(self) -> str:
        return self.default_home if self.bot_home == "AUTO" else self.bot_home


@dataclass(frozen=True)
class Book:
    instrument_id: str
    bids: tuple[tuple[int, int], ...]
    asks: tuple[tuple[int, int], ...]
    server_time_ms: int
    recv_mono_ms: int

    @classmethod
    def from_depth(
        cls,
        instrument_id_value: str,
        depth: Mapping[str, Mapping[str, int]],
        now_ms: int,
        recv_ms: Optional[int] = None,
    ) -> "Book":
        bids = tuple(
            sorted(
                ((int(price), int(qty)) for price, qty in depth.get("bids", {}).items() if int(qty) > 0),
                reverse=True,
            )
        )
        asks = tuple(
            sorted((int(price), int(qty)) for price, qty in depth.get("asks", {}).items() if int(qty) > 0)
        )
        return cls(instrument_id_value, bids, asks, int(now_ms), now_mono_ms() if recv_ms is None else recv_ms)

    @property
    def complete(self) -> bool:
        return bool(self.bids and self.asks)

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
    def spread(self) -> int:
        if not self.complete:
            return 1_000_000
        return self.asks[0][0] - self.bids[0][0]

    @property
    def mid(self) -> Optional[int]:
        if not self.complete:
            return None
        return (self.asks[0][0] + self.bids[0][0]) // 2

    @property
    def microprice(self) -> Optional[int]:
        if not self.complete:
            return None
        bid, bid_qty = self.bids[0]
        ask, ask_qty = self.asks[0]
        total = bid_qty + ask_qty
        if total <= 0:
            return self.mid
        return (bid * ask_qty + ask * bid_qty) // total

    @property
    def imbalance_bps(self) -> int:
        bid_qty = self.best_bid_qty
        ask_qty = self.best_ask_qty
        total = bid_qty + ask_qty
        if total <= 0:
            return 0
        return ((bid_qty - ask_qty) * 10_000) // total

    def age_ms(self) -> int:
        return max(0, now_mono_ms() - self.recv_mono_ms)

    def buy_capacity_at(self, limit_price: int) -> int:
        return sum(qty for price, qty in self.asks if price <= limit_price)

    def sell_capacity_at(self, limit_price: int) -> int:
        return sum(qty for price, qty in self.bids if price >= limit_price)

    def average_buy_price(self, quantity: int) -> Optional[int]:
        return self._average_price(self.asks, quantity)

    def average_sell_price(self, quantity: int) -> Optional[int]:
        return self._average_price(self.bids, quantity)

    @staticmethod
    def _average_price(levels: Iterable[tuple[int, int]], quantity: int) -> Optional[int]:
        if quantity <= 0:
            return None
        remaining = quantity
        notional = 0
        filled = 0
        for price, qty in levels:
            take = min(remaining, qty)
            notional += take * price
            filled += take
            remaining -= take
            if remaining == 0:
                break
        if filled < quantity:
            return None
        return notional // quantity


@dataclass
class MarketState:
    books: dict[str, dict[str, Book]] = field(default_factory=lambda: defaultdict(dict))
    server_time_ms: dict[str, int] = field(default_factory=dict)
    round_length_ms: dict[str, int] = field(default_factory=lambda: defaultdict(lambda: DEFAULT_ROUND_LENGTH_MS))
    last_message_mono_ms: dict[str, int] = field(default_factory=dict)
    mid_history: dict[tuple[str, str], Deque[tuple[int, int]]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=256)))
    vol_bps: dict[tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))
    trade_pressure_bps: dict[tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))

    def update_book(
        self,
        exchange: str,
        instrument: str,
        depth: Mapping[str, Mapping[str, int]],
        now_ms: int,
        recv_ms: Optional[int] = None,
    ) -> Book:
        exchange = canonical_exchange(exchange)
        recv_ms = now_mono_ms() if recv_ms is None else recv_ms
        book = Book.from_depth(instrument, depth, now_ms=now_ms, recv_ms=recv_ms)
        self.books[exchange][instrument] = book
        self.server_time_ms[exchange] = int(now_ms)
        self.last_message_mono_ms[exchange] = recv_ms
        _ex, ticker = split_instrument(instrument)
        mid = book.mid
        if mid is not None and mid > 0:
            key = (exchange, ticker)
            hist = self.mid_history[key]
            if hist:
                old_mid = hist[-1][1]
                if old_mid > 0:
                    ret = abs(mid - old_mid) * 10_000 // old_mid
                    self.vol_bps[key] = (self.vol_bps[key] * 4 + ret) // 5
            hist.append((recv_ms, mid))
        return book

    def update_from_market_data(self, exchange: str, payload: Mapping[str, Any]) -> None:
        exchange = canonical_exchange(exchange)
        recv = now_mono_ms()
        server_now = int(payload.get("time", self.server_time_ms.get(exchange, 0)))
        self.server_time_ms[exchange] = server_now
        self.last_message_mono_ms[exchange] = recv
        if "round_length" in payload:
            self.round_length_ms[exchange] = int(payload["round_length"])
        for instrument, depth in payload.get("orderbook_depths", {}).items():
            self.update_book(exchange, instrument, depth, now_ms=server_now, recv_ms=recv)
        self._update_events(exchange, payload.get("events", ()))
        self._trim_history(recv)

    def reset_exchange(self, exchange: str) -> None:
        exchange = canonical_exchange(exchange)
        self.books.pop(exchange, None)
        self.server_time_ms.pop(exchange, None)
        self.round_length_ms.pop(exchange, None)
        self.last_message_mono_ms.pop(exchange, None)
        for key in list(self.mid_history):
            if key[0] == exchange:
                del self.mid_history[key]
        for key in list(self.vol_bps):
            if key[0] == exchange:
                del self.vol_bps[key]
        for key in list(self.trade_pressure_bps):
            if key[0] == exchange:
                del self.trade_pressure_bps[key]

    def _update_events(self, exchange: str, events: Iterable[Mapping[str, Any]]) -> None:
        for event in events:
            if event.get("event_type") != "trade":
                continue
            data = event.get("data", {})
            instrument = data.get("instrumentID")
            if not instrument:
                continue
            try:
                _ex, ticker = split_instrument(instrument)
            except ValueError:
                continue
            price = data.get("price")
            qty = int(data.get("quantity", 0) or 0)
            book = self.books.get(exchange, {}).get(instrument)
            mid = book.mid if book else None
            if mid is None or price is None or qty <= 0:
                continue
            signed = 0
            if int(price) >= mid:
                signed = min(10_000, qty * 200)
            elif int(price) < mid:
                signed = -min(10_000, qty * 200)
            key = (exchange, ticker)
            self.trade_pressure_bps[key] = (self.trade_pressure_bps[key] * 3 + signed) // 4

    def _trim_history(self, now_ms: int) -> None:
        cutoff = now_ms - 15_000
        for hist in self.mid_history.values():
            while len(hist) > 2 and hist[0][0] < cutoff:
                hist.popleft()

    def book(self, exchange: str, ticker: str) -> Optional[Book]:
        exchange = canonical_exchange(exchange)
        return self.books.get(exchange, {}).get(instrument_id(exchange, ticker))

    def fresh_book(self, exchange: str, ticker: str, max_age_ms: int) -> Optional[Book]:
        book = self.book(exchange, ticker)
        if not book or not book.complete or book.age_ms() > max_age_ms:
            return None
        return book

    def time_remaining_ms(self, exchange: str) -> int:
        exchange = canonical_exchange(exchange)
        length = self.round_length_ms.get(exchange, DEFAULT_ROUND_LENGTH_MS)
        return max(0, length - self.server_time_ms.get(exchange, 0))

    def segment_live(self, exchange: str) -> bool:
        remaining = self.time_remaining_ms(exchange)
        return remaining > 0 or exchange not in self.server_time_ms

    def venue_mids(
        self,
        ticker: str,
        venues: Iterable[str],
        max_age_ms: int,
        use_microprice: bool = True,
    ) -> list[tuple[str, int]]:
        out: list[tuple[str, int]] = []
        for venue in venues:
            book = self.fresh_book(venue, ticker, max_age_ms)
            if not book:
                continue
            mid = book.microprice if use_microprice else book.mid
            if mid is not None:
                out.append((venue, mid))
        return out

    def robust_reference(
        self,
        ticker: str,
        venues: Iterable[str],
        max_age_ms: int,
        home: str,
        exclude: Optional[str] = None,
        physical_latency_removed: bool = True,
    ) -> Optional[int]:
        points: list[int] = []
        weighted: list[tuple[int, int]] = []
        for venue, mid in self.venue_mids(ticker, venues, max_age_ms):
            if exclude is not None and venue == exclude:
                continue
            if physical_latency_removed:
                weight = 1
            else:
                latency = LATENCY_RTT_MS.get(home, LATENCY_RTT_MS["ZSE"]).get(venue, 120)
                weight = max(1, 220 - latency)
            points.append(mid)
            weighted.append((mid, weight))
        if len(points) < 2:
            return points[0] if points else None
        med = int(median(points))
        filtered = [(mid, weight) for mid, weight in weighted if abs(mid - med) <= max(40, med // 500)]
        if not filtered:
            filtered = weighted
        numer = sum(mid * weight for mid, weight in filtered)
        denom = sum(weight for _mid, weight in filtered)
        return numer // denom if denom else med

    def momentum_bps(self, exchange: str, ticker: str, window_ms: int) -> int:
        key = (canonical_exchange(exchange), ticker)
        hist = self.mid_history.get(key)
        if not hist or len(hist) < 2:
            return 0
        latest_time, latest_mid = hist[-1]
        cutoff = latest_time - window_ms
        old_mid = hist[0][1]
        for ts, mid in hist:
            if ts >= cutoff:
                old_mid = mid
                break
        if old_mid <= 0:
            return 0
        return (latest_mid - old_mid) * 10_000 // old_mid

    def broad_market_momentum_bps(self, exchange: str, window_ms: int) -> Optional[int]:
        values: list[int] = []
        for ticker in BROAD_MARKET:
            if self.book(exchange, ticker):
                values.append(self.momentum_bps(exchange, ticker, window_ms))
        if len(values) < 5:
            return None
        return int(median(values))


def compute_basket_fair_value(state: MarketState, exchange: str, basket: Iterable[str]) -> Optional[int]:
    values: list[int] = []
    for ticker in basket:
        book = state.book(exchange, ticker)
        if not book or not book.complete:
            return None
        mid = book.mid
        if mid is None:
            return None
        values.append(mid)
    return sum(values) // len(values) if values else None


def executable_basket_average(
    state: MarketState,
    exchange: str,
    basket: Iterable[str],
    side: str,
    component_qty: int,
    max_age_ms: int,
) -> Optional[int]:
    prices: list[int] = []
    for ticker in basket:
        book = state.fresh_book(exchange, ticker, max_age_ms)
        if not book:
            return None
        price = book.average_sell_price(component_qty) if side == "sell_basket" else book.average_buy_price(component_qty)
        if price is None:
            return None
        prices.append(price)
    return sum(prices) // len(prices)


@dataclass(frozen=True)
class RiskDecision:
    ok: bool
    reason: str = ""


@dataclass
class RiskManager:
    cfg: Config
    cash: dict[str, int] = field(default_factory=lambda: defaultdict(lambda: INITIAL_CASH))
    positions: dict[tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))
    pending_cash_delta: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    pending_pos_delta: dict[tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))
    inflight_by_exchange: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    pending_count_by_exchange: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def sync_inventory(self, exchange: str, data: Mapping[str, Any]) -> None:
        exchange = canonical_exchange(exchange)
        for key, pair in data.items():
            if key == "$":
                self.cash[exchange] = int(pair[1])
                continue
            self.positions[(exchange, key)] = int(pair[1])

    def position(self, exchange: str, instrument: str) -> int:
        return self.positions[(canonical_exchange(exchange), instrument)]

    def projected_position(self, exchange: str, instrument: str) -> int:
        key = (canonical_exchange(exchange), instrument)
        return self.positions[key] + self.pending_pos_delta[key]

    def projected_cash(self, exchange: str) -> int:
        exchange = canonical_exchange(exchange)
        return self.cash[exchange] + self.pending_cash_delta[exchange]

    def approve(
        self,
        exchange: str,
        instrument: str,
        side: str,
        price: int,
        quantity: int,
        reduce_only: bool = False,
    ) -> RiskDecision:
        exchange = canonical_exchange(exchange)
        if quantity <= 0:
            return RiskDecision(False, "non_positive_quantity")
        if not isinstance(price, int) or price <= 0:
            return RiskDecision(False, "bad_price")
        if self.inflight_by_exchange[exchange] >= self.cfg.max_inflight_per_exchange:
            return RiskDecision(False, "too_many_inflight")
        if self.pending_count_by_exchange[exchange] >= self.cfg.max_pending_orders_soft:
            return RiskDecision(False, "too_many_pending")
        pos = self.projected_position(exchange, instrument)
        cash = self.projected_cash(exchange)
        delta = quantity if side == "bid" else -quantity
        new_pos = pos + delta

        if reduce_only:
            if side == "bid" and pos >= 0:
                return RiskDecision(False, "buy_not_reducing")
            if side == "ask" and pos <= 0:
                return RiskDecision(False, "sell_not_reducing")
            if abs(new_pos) >= abs(pos):
                return RiskDecision(False, "not_reducing")
        else:
            long_cap = min(HARD_LONG_LIMIT, self.cfg.max_symbol_position)
            short_cap = -min(abs(HARD_SHORT_LIMIT), self.cfg.max_symbol_short)
            if new_pos > long_cap:
                return RiskDecision(False, "long_cap")
            if new_pos < short_cap:
                return RiskDecision(False, "short_cap")

        if side == "bid":
            projected_cash = cash - price * quantity
            buffer_cents = 0 if reduce_only else self.cfg.cash_buffer_cents
            if projected_cash < self.cfg.cash_floor_cents + buffer_cents:
                return RiskDecision(False, "cash_floor")

        return RiskDecision(True, "ok")

    def reserve(self, exchange: str, instrument: str, side: str, price: int, quantity: int) -> None:
        exchange = canonical_exchange(exchange)
        key = (exchange, instrument)
        self.inflight_by_exchange[exchange] += 1
        if side == "bid":
            self.pending_cash_delta[exchange] -= price * quantity
            self.pending_pos_delta[key] += quantity
        else:
            self.pending_pos_delta[key] -= quantity

    def release(self, exchange: str, instrument: str, side: str, price: int, quantity: int) -> None:
        exchange = canonical_exchange(exchange)
        key = (exchange, instrument)
        self.inflight_by_exchange[exchange] = max(0, self.inflight_by_exchange[exchange] - 1)
        if side == "bid":
            self.pending_cash_delta[exchange] += price * quantity
            self.pending_pos_delta[key] -= quantity
        else:
            self.pending_pos_delta[key] += quantity

    def apply_immediate_change(
        self,
        exchange: str,
        instrument: str,
        inventory_delta: Optional[int],
        balance_delta: Optional[int],
    ) -> None:
        exchange = canonical_exchange(exchange)
        if inventory_delta is not None:
            self.positions[(exchange, instrument)] += int(inventory_delta)
        if balance_delta is not None:
            self.cash[exchange] += int(balance_delta)

    def reset_exchange(self, exchange: str) -> None:
        exchange = canonical_exchange(exchange)
        self.cash[exchange] = INITIAL_CASH
        for key in list(self.positions):
            if key[0] == exchange:
                del self.positions[key]
        self.pending_cash_delta[exchange] = 0
        for key in list(self.pending_pos_delta):
            if key[0] == exchange:
                del self.pending_pos_delta[key]
        self.inflight_by_exchange[exchange] = 0
        self.pending_count_by_exchange[exchange] = 0


@dataclass(frozen=True)
class Signal:
    strategy: str
    exchange: str
    instrument_id: str
    side: str
    price: int
    quantity: int
    edge_cents: int
    confidence_bps: int
    reduce_only: bool
    reason: str
    order_type: str = "ioc"
    group_id: str = ""


def score_signal(signal_value: Signal, cfg: Config) -> int:
    latency = execution_latency_ms(signal_value.exchange, cfg)
    reduce_bonus = 80_000 if signal_value.reduce_only else 0
    return (
        signal_value.edge_cents * 10_000
        + signal_value.confidence_bps
        + reduce_bonus
        - latency * 50
        - max(0, signal_value.quantity - cfg.order_size) * 80
    )


def execution_latency_ms(exchange: str, cfg: Config) -> int:
    if cfg.physical_latency_removed:
        return 0
    home = cfg.active_home
    return LATENCY_RTT_MS.get(home, LATENCY_RTT_MS["ZSE"]).get(exchange, 120)


def confidence_from_edge(edge_cents: int, threshold_cents: int, book: Book) -> int:
    if threshold_cents <= 0:
        return 10_000
    excess = max(0, edge_cents - threshold_cents)
    base = min(9_000, 2_500 + excess * 700)
    spread_penalty = min(2_000, max(0, book.spread - 5) * 35)
    imbalance_bonus = min(1_500, abs(book.imbalance_bps) // 4)
    return max(0, min(10_000, base + imbalance_bonus - spread_penalty))


def adaptive_threshold(
    cfg: Config,
    base_cents: int,
    book: Book,
    state: MarketState,
    exchange: str,
    ticker: str,
    home: str,
) -> int:
    latency = 0 if cfg.physical_latency_removed else LATENCY_RTT_MS.get(home, LATENCY_RTT_MS["ZSE"]).get(exchange, 120)
    vol = state.vol_bps.get((exchange, ticker), 0)
    mid = book.mid or 10_000
    vol_cents = min(30, vol * mid // 10_000)
    return base_cents + max(1, book.spread // 2) + vol_cents + latency // 60


def clip_top_qty(book: Book, side: str, limit_price: int, desired_qty: int) -> int:
    capacity = book.buy_capacity_at(limit_price) if side == "bid" else book.sell_capacity_at(limit_price)
    return max(0, min(desired_qty, capacity))


class Strategy:
    name = "base"

    def generate(self, state: MarketState, risk: RiskManager, cfg: Config) -> list[Signal]:
        raise NotImplementedError


class ETFArbStrategy(Strategy):
    name = "etf_arb"

    def generate(self, state: MarketState, risk: RiskManager, cfg: Config) -> list[Signal]:
        if not cfg.enable_etf_arb:
            return []
        signals: list[Signal] = []
        home = cfg.active_home
        basket_venues = [venue for venue in ("ZSE", "Euronext", "LSE", "NYSE", "NASDAQ", "HKEX", "TMX") if venue in cfg.venues]
        for etf, basket in ETF_BASKETS.items():
            n = len(basket)
            for exchange in cfg.venues:
                if exchange not in ETF_LISTINGS.get(etf, set()):
                    continue
                etf_book = state.fresh_book(exchange, etf, cfg.stale_book_ms)
                if not etf_book or etf_book.spread > cfg.max_trade_spread_cents:
                    continue
                hedge_exchange = self._choose_hedge_exchange(state, cfg, basket, basket_venues, exchange)
                if not hedge_exchange:
                    continue
                component_qty = max(1, cfg.arb_lots)
                etf_qty = component_qty * n
                if etf_qty > cfg.max_order_qty:
                    component_qty = max(1, cfg.max_order_qty // n)
                    etf_qty = component_qty * n
                group = f"etf-{etf}-{exchange}-{now_mono_ms()}"

                basket_sell = executable_basket_average(
                    state, hedge_exchange, basket, "sell_basket", component_qty, cfg.reference_stale_ms
                )
                if basket_sell is not None and etf_book.best_ask is not None:
                    edge = basket_sell - etf_book.best_ask
                    threshold = adaptive_threshold(cfg, cfg.etf_edge_floor_cents, etf_book, state, exchange, etf, home)
                    if edge >= threshold:
                        qty = clip_top_qty(etf_book, "bid", etf_book.best_ask + cfg.ioc_slip_cents, etf_qty)
                        if qty >= n:
                            qty = (qty // n) * n
                            component_qty = qty // n
                            conf = confidence_from_edge(edge, threshold, etf_book)
                            if conf >= cfg.min_confidence_bps:
                                signals.append(
                                    Signal(
                                        self.name,
                                        exchange,
                                        instrument_id(exchange, etf),
                                        "bid",
                                        etf_book.best_ask + cfg.ioc_slip_cents,
                                        qty,
                                        edge,
                                        conf,
                                        False,
                                        f"buy {etf} below executable basket bid on {hedge_exchange}",
                                        group_id=group,
                                    )
                                )
                                signals.extend(
                                    self._basket_hedge_signals(
                                        state,
                                        cfg,
                                        hedge_exchange,
                                        basket,
                                        "ask",
                                        component_qty,
                                        edge,
                                        conf,
                                        group,
                                    )
                                )

                basket_buy = executable_basket_average(
                    state, hedge_exchange, basket, "buy_basket", component_qty, cfg.reference_stale_ms
                )
                if basket_buy is not None and etf_book.best_bid is not None:
                    edge = etf_book.best_bid - basket_buy
                    threshold = adaptive_threshold(cfg, cfg.etf_edge_floor_cents, etf_book, state, exchange, etf, home)
                    if edge >= threshold:
                        qty = clip_top_qty(etf_book, "ask", etf_book.best_bid - cfg.ioc_slip_cents, etf_qty)
                        if qty >= n:
                            qty = (qty // n) * n
                            component_qty = qty // n
                            conf = confidence_from_edge(edge, threshold, etf_book)
                            if conf >= cfg.min_confidence_bps:
                                signals.append(
                                    Signal(
                                        self.name,
                                        exchange,
                                        instrument_id(exchange, etf),
                                        "ask",
                                        max(1, etf_book.best_bid - cfg.ioc_slip_cents),
                                        qty,
                                        edge,
                                        conf,
                                        False,
                                        f"sell {etf} above executable basket ask on {hedge_exchange}",
                                        group_id=group,
                                    )
                                )
                                signals.extend(
                                    self._basket_hedge_signals(
                                        state,
                                        cfg,
                                        hedge_exchange,
                                        basket,
                                        "bid",
                                        component_qty,
                                        edge,
                                        conf,
                                        group,
                                    )
                                )
        return signals

    @staticmethod
    def _choose_hedge_exchange(
        state: MarketState,
        cfg: Config,
        basket: Iterable[str],
        candidates: Iterable[str],
        etf_exchange: str,
    ) -> Optional[str]:
        ordered = [etf_exchange] + [venue for venue in candidates if venue != etf_exchange]
        best: Optional[tuple[int, str]] = None
        for venue in ordered:
            if venue not in cfg.venues:
                continue
            if not all(venue in STOCK_LISTINGS.get(ticker, set()) for ticker in basket):
                continue
            books = [state.fresh_book(venue, ticker, cfg.reference_stale_ms) for ticker in basket]
            if not all(books):
                continue
            total_spread = sum(book.spread for book in books if book)
            if best is None or total_spread < best[0]:
                best = (total_spread, venue)
        return best[1] if best else None

    @staticmethod
    def _basket_hedge_signals(
        state: MarketState,
        cfg: Config,
        exchange: str,
        basket: Iterable[str],
        side: str,
        quantity: int,
        edge: int,
        confidence: int,
        group: str,
    ) -> list[Signal]:
        out: list[Signal] = []
        for ticker in basket:
            book = state.fresh_book(exchange, ticker, cfg.reference_stale_ms)
            if not book:
                continue
            if side == "ask":
                price = max(1, (book.best_bid or 1) - cfg.ioc_slip_cents)
            else:
                price = (book.best_ask or 1) + cfg.ioc_slip_cents
            clipped = clip_top_qty(book, side, price, quantity)
            if clipped <= 0:
                continue
            out.append(
                Signal(
                    "etf_hedge",
                    exchange,
                    instrument_id(exchange, ticker),
                    side,
                    price,
                    clipped,
                    max(1, edge // 2),
                    max(2_000, confidence - 1_000),
                    False,
                    "basket hedge leg",
                    group_id=group,
                )
            )
        return out


class CrossVenueLeadLagStrategy(Strategy):
    name = "cross_venue"

    def generate(self, state: MarketState, risk: RiskManager, cfg: Config) -> list[Signal]:
        if not cfg.enable_cross_venue:
            return []
        signals: list[Signal] = []
        home = cfg.active_home
        tickers = [ticker for ticker in ALL_TICKERS if len((STOCK_LISTINGS.get(ticker) or ETF_LISTINGS.get(ticker) or set()) & set(cfg.venues)) >= 2]
        for ticker in tickers:
            listings = (STOCK_LISTINGS.get(ticker) or ETF_LISTINGS.get(ticker) or set()) & set(cfg.venues)
            if len(listings) < 2:
                continue
            for exchange in listings:
                book = state.fresh_book(exchange, ticker, cfg.stale_book_ms)
                if not book or book.spread > cfg.max_trade_spread_cents:
                    continue
                ref = state.robust_reference(
                    ticker,
                    listings,
                    cfg.reference_stale_ms,
                    home=home,
                    exclude=exchange,
                    physical_latency_removed=cfg.physical_latency_removed,
                )
                if ref is None:
                    continue
                threshold = adaptive_threshold(cfg, cfg.cross_edge_floor_cents, book, state, exchange, ticker, home)
                pressure = state.trade_pressure_bps.get((exchange, ticker), 0)
                if book.best_ask is not None:
                    edge = ref - book.best_ask
                    if edge >= threshold and pressure >= -4_000:
                        conf = confidence_from_edge(edge, threshold, book)
                        qty = clip_top_qty(book, "bid", book.best_ask + cfg.ioc_slip_cents, cfg.order_size)
                        if qty > 0 and conf >= cfg.min_confidence_bps:
                            signals.append(
                                Signal(
                                    self.name,
                                    exchange,
                                    instrument_id(exchange, ticker),
                                    "bid",
                                    book.best_ask + cfg.ioc_slip_cents,
                                    min(qty, cfg.max_order_qty),
                                    edge,
                                    conf,
                                    False,
                                    f"{ticker} cheap vs robust cross-venue reference {ref}",
                                )
                            )
                if book.best_bid is not None:
                    edge = book.best_bid - ref
                    if edge >= threshold and pressure <= 4_000:
                        conf = confidence_from_edge(edge, threshold, book)
                        qty = clip_top_qty(book, "ask", max(1, book.best_bid - cfg.ioc_slip_cents), cfg.order_size)
                        if qty > 0 and conf >= cfg.min_confidence_bps:
                            signals.append(
                                Signal(
                                    self.name,
                                    exchange,
                                    instrument_id(exchange, ticker),
                                    "ask",
                                    max(1, book.best_bid - cfg.ioc_slip_cents),
                                    min(qty, cfg.max_order_qty),
                                    edge,
                                    conf,
                                    False,
                                    f"{ticker} rich vs robust cross-venue reference {ref}",
                                )
                            )
        return signals


class SafeHavenRotationStrategy(Strategy):
    name = "safe_haven"

    def generate(self, state: MarketState, risk: RiskManager, cfg: Config) -> list[Signal]:
        if not cfg.enable_safe_haven:
            return []
        signals: list[Signal] = []
        home = cfg.active_home
        ref_exchange = "ZSE" if "ZSE" in cfg.venues else cfg.venues[0]
        broad_mom = state.broad_market_momentum_bps(ref_exchange, cfg.momentum_window_ms)
        if broad_mom is None or abs(broad_mom) < 8:
            return []
        expected_safe_mom = -broad_mom * cfg.safe_haven_beta_bps // 10_000
        for ticker in ("GOLD", "XAG", "ETFSH"):
            venues = (STOCK_LISTINGS.get(ticker) or ETF_LISTINGS.get(ticker) or set()) & set(cfg.venues)
            for exchange in venues:
                book = state.fresh_book(exchange, ticker, cfg.stale_book_ms)
                if not book or book.spread > cfg.max_trade_spread_cents:
                    continue
                mid = book.mid
                if mid is None:
                    continue
                actual_mom = state.momentum_bps(exchange, ticker, cfg.momentum_window_ms)
                surprise_bps = expected_safe_mom - actual_mom
                predicted = mid + (mid * surprise_bps // 10_000)
                threshold = adaptive_threshold(cfg, cfg.safe_edge_floor_cents, book, state, exchange, ticker, home)
                if book.best_ask is not None:
                    edge = predicted - book.best_ask
                    if edge >= threshold:
                        conf = confidence_from_edge(edge, threshold, book)
                        qty = clip_top_qty(book, "bid", book.best_ask + cfg.ioc_slip_cents, cfg.order_size)
                        if qty > 0 and conf >= cfg.min_confidence_bps:
                            signals.append(
                                Signal(
                                    self.name,
                                    exchange,
                                    instrument_id(exchange, ticker),
                                    "bid",
                                    book.best_ask + cfg.ioc_slip_cents,
                                    min(qty, cfg.max_order_qty),
                                    edge,
                                    conf,
                                    False,
                                    f"{ticker} lagging inverse broad move {broad_mom}bps",
                                )
                            )
                if book.best_bid is not None:
                    edge = book.best_bid - predicted
                    if edge >= threshold:
                        conf = confidence_from_edge(edge, threshold, book)
                        qty = clip_top_qty(book, "ask", max(1, book.best_bid - cfg.ioc_slip_cents), cfg.order_size)
                        if qty > 0 and conf >= cfg.min_confidence_bps:
                            signals.append(
                                Signal(
                                    self.name,
                                    exchange,
                                    instrument_id(exchange, ticker),
                                    "ask",
                                    max(1, book.best_bid - cfg.ioc_slip_cents),
                                    min(qty, cfg.max_order_qty),
                                    edge,
                                    conf,
                                    False,
                                    f"{ticker} overreacted to inverse broad move {broad_mom}bps",
                                )
                            )
        return signals


class MicropriceStrategy(Strategy):
    name = "microprice"

    def generate(self, state: MarketState, risk: RiskManager, cfg: Config) -> list[Signal]:
        if not cfg.enable_microprice:
            return []
        signals: list[Signal] = []
        home = cfg.active_home
        for exchange in cfg.venues:
            for instrument, book in state.books.get(exchange, {}).items():
                if not book.complete or book.age_ms() > cfg.stale_book_ms:
                    continue
                if book.spread <= 1 or book.spread > min(cfg.max_trade_spread_cents, 35):
                    continue
                _ex, ticker = split_instrument(instrument)
                micro = book.microprice
                mid = book.mid
                if micro is None or mid is None:
                    continue
                listings = (STOCK_LISTINGS.get(ticker) or ETF_LISTINGS.get(ticker) or set()) & set(cfg.venues)
                ref = state.robust_reference(
                    ticker,
                    listings,
                    cfg.reference_stale_ms,
                    home=home,
                    exclude=None,
                    physical_latency_removed=cfg.physical_latency_removed,
                )
                if ref is None:
                    ref = mid
                threshold = adaptive_threshold(cfg, cfg.micro_edge_floor_cents, book, state, exchange, ticker, home)
                pressure = state.trade_pressure_bps.get((exchange, ticker), 0)
                if book.imbalance_bps > 4_000 and pressure >= -2_000 and book.best_ask is not None:
                    predicted = max(micro, ref)
                    edge = predicted - book.best_ask
                    if edge >= threshold:
                        qty = clip_top_qty(book, "bid", book.best_ask + cfg.ioc_slip_cents, min(2, cfg.order_size))
                        conf = confidence_from_edge(edge, threshold, book)
                        if qty > 0 and conf >= cfg.min_confidence_bps:
                            signals.append(
                                Signal(
                                    self.name,
                                    exchange,
                                    instrument,
                                    "bid",
                                    book.best_ask + cfg.ioc_slip_cents,
                                    qty,
                                    edge,
                                    conf,
                                    False,
                                    "positive book imbalance with reference confirmation",
                                )
                            )
                if book.imbalance_bps < -4_000 and pressure <= 2_000 and book.best_bid is not None:
                    predicted = min(micro, ref)
                    edge = book.best_bid - predicted
                    if edge >= threshold:
                        qty = clip_top_qty(book, "ask", max(1, book.best_bid - cfg.ioc_slip_cents), min(2, cfg.order_size))
                        conf = confidence_from_edge(edge, threshold, book)
                        if qty > 0 and conf >= cfg.min_confidence_bps:
                            signals.append(
                                Signal(
                                    self.name,
                                    exchange,
                                    instrument,
                                    "ask",
                                    max(1, book.best_bid - cfg.ioc_slip_cents),
                                    qty,
                                    edge,
                                    conf,
                                    False,
                                    "negative book imbalance with reference confirmation",
                                )
                            )
        return signals


class Flattener:
    name = "flattener"

    def __init__(self) -> None:
        self.last_flatten_ms: dict[str, int] = defaultdict(int)

    def generate(self, state: MarketState, risk: RiskManager, cfg: Config) -> list[Signal]:
        signals: list[Signal] = []
        now_ms = now_mono_ms()
        for exchange in cfg.venues:
            if state.time_remaining_ms(exchange) > cfg.flatten_last_ms:
                continue
            if now_ms - self.last_flatten_ms[exchange] < cfg.flatten_interval_ms:
                continue
            self.last_flatten_ms[exchange] = now_ms
            for (ex, instrument), pos in list(risk.positions.items()):
                if ex != exchange or pos == 0:
                    continue
                _venue, ticker = split_instrument(instrument)
                book = state.fresh_book(exchange, ticker, cfg.stale_book_ms)
                if not book or not book.complete:
                    continue
                qty = min(abs(pos), cfg.flatten_order_qty)
                if pos > 0 and book.best_bid is not None:
                    price = max(1, book.best_bid - cfg.flatten_slip_cents)
                    qty = min(qty, max(1, book.sell_capacity_at(price)))
                    signals.append(
                        Signal(
                            self.name,
                            exchange,
                            instrument,
                            "ask",
                            price,
                            qty,
                            0,
                            10_000,
                            True,
                            "end-segment long flatten",
                        )
                    )
                elif pos < 0 and book.best_ask is not None:
                    price = book.best_ask + cfg.flatten_slip_cents
                    qty = min(qty, max(1, book.buy_capacity_at(price)))
                    signals.append(
                        Signal(
                            self.name,
                            exchange,
                            instrument,
                            "bid",
                            price,
                            qty,
                            0,
                            10_000,
                            True,
                            "end-segment short flatten",
                        )
                    )
        return signals


class TokenBucketRateLimiter:
    def __init__(self, rate_per_sec: int) -> None:
        self.rate = max(1, rate_per_sec)
        self.capacity = max(1, rate_per_sec)
        self.tokens = self.capacity
        self.updated_ns = time.monotonic_ns()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now_ns = time.monotonic_ns()
                elapsed_ns = now_ns - self.updated_ns
                refill = elapsed_ns * self.rate // 1_000_000_000
                if refill > 0:
                    self.tokens = min(self.capacity, self.tokens + refill)
                    self.updated_ns += refill * 1_000_000_000 // self.rate
                if self.tokens > 0:
                    self.tokens -= 1
                    return
                needed_ns = max(1, 1_000_000_000 // self.rate)
            await asyncio.sleep(needed_ns / 1_000_000_000)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": now_epoch_ms(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        extra = getattr(record, "extra_payload", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def setup_logging(cfg: Config) -> logging.Logger:
    logger = logging.getLogger("hege")
    logger.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    logger.handlers.clear()
    handler = logging.StreamHandler()
    if cfg.log_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    payload = {"event": event}
    payload.update(fields)
    logger.log(level, event, extra={"extra_payload": payload})


@dataclass
class Submission:
    signal: Signal
    request_id: str
    sent_mono_ms: int


class StrategyEngine:
    def __init__(self, state: MarketState, risk: RiskManager, cfg: Config, logger: logging.Logger) -> None:
        self.state = state
        self.risk = risk
        self.cfg = cfg
        self.logger = logger
        self.strategies: list[Strategy] = [
            ETFArbStrategy(),
            CrossVenueLeadLagStrategy(),
            SafeHavenRotationStrategy(),
            MicropriceStrategy(),
        ]
        self.flattener = Flattener()
        self.clients: dict[str, ExchangeClient] = {}
        self.lock = asyncio.Lock()
        self.last_run_ms = 0
        self.last_diag_ms = 0
        self.last_signal_ms: dict[tuple[str, str, str, str], int] = defaultdict(int)

    def set_clients(self, clients: Mapping[str, "ExchangeClient"]) -> None:
        self.clients = dict(clients)

    async def on_market_data(self, exchange: str) -> None:
        now_ms = now_mono_ms()
        if now_ms - self.last_run_ms < self.cfg.strategy_tick_ms:
            return
        async with self.lock:
            now_ms = now_mono_ms()
            if now_ms - self.last_run_ms < self.cfg.strategy_tick_ms:
                return
            self.last_run_ms = now_ms
            await self._run_once()

    async def _run_once(self) -> None:
        signals: list[Signal] = []
        strategy_counts: dict[str, int] = {}
        if self._should_flatten():
            signals.extend(self.flattener.generate(self.state, self.risk, self.cfg))
            strategy_counts[self.flattener.name] = len(signals)
        else:
            for strategy in self.strategies:
                try:
                    before = len(signals)
                    signals.extend(strategy.generate(self.state, self.risk, self.cfg))
                    strategy_counts[strategy.name] = len(signals) - before
                except Exception as exc:
                    strategy_counts[strategy.name] = -1
                    log_event(
                        self.logger,
                        logging.WARNING,
                        "strategy_exception",
                        strategy=strategy.name,
                        error=repr(exc),
                    )
            before = len(signals)
            signals.extend(self._risk_compression_signals())
            strategy_counts["risk_compression"] = len(signals) - before
        if not signals:
            self._maybe_log_diagnostics("no_signals", strategy_counts, {})
            return
        signals.sort(key=lambda sig: score_signal(sig, self.cfg), reverse=True)
        sent_total = 0
        sent_by_exchange: dict[str, int] = defaultdict(int)
        skip_counts: dict[str, int] = defaultdict(int)
        for sig in signals:
            if sent_total >= self.cfg.max_orders_per_tick:
                skip_counts["max_orders_per_tick"] += 1
                break
            if sent_by_exchange[sig.exchange] >= self.cfg.max_orders_per_exchange_tick:
                skip_counts["max_orders_per_exchange_tick"] += 1
                continue
            if not sig.reduce_only and self.state.time_remaining_ms(sig.exchange) <= self.cfg.no_open_last_ms:
                skip_counts["no_open_last_ms"] += 1
                continue
            if not self._cooldown_ok(sig):
                skip_counts["cooldown"] += 1
                continue
            decision = self.risk.approve(sig.exchange, sig.instrument_id, sig.side, sig.price, sig.quantity, sig.reduce_only)
            if not decision.ok:
                skip_counts[f"risk_{decision.reason}"] += 1
                log_event(
                    self.logger,
                    logging.DEBUG,
                    "risk_block",
                    strategy=sig.strategy,
                    exchange=sig.exchange,
                    instrument=sig.instrument_id,
                    side=sig.side,
                    qty=sig.quantity,
                    price=sig.price,
                    reason=decision.reason,
                )
                continue
            client = self.clients.get(sig.exchange)
            if not client:
                skip_counts["no_client"] += 1
                continue
            self.last_signal_ms[(sig.strategy, sig.exchange, sig.instrument_id, sig.side)] = now_mono_ms()
            await client.submit_order(sig)
            sent_total += 1
            sent_by_exchange[sig.exchange] += 1
        self._maybe_log_diagnostics(
            "orders_sent" if sent_total else "signals_blocked",
            strategy_counts,
            skip_counts,
            total_signals=len(signals),
            sent_total=sent_total,
            sent_by_exchange=dict(sent_by_exchange),
        )

    def _should_flatten(self) -> bool:
        return any(self.state.time_remaining_ms(exchange) <= self.cfg.flatten_last_ms for exchange in self.cfg.venues)

    def _cooldown_ok(self, sig: Signal) -> bool:
        if sig.reduce_only:
            return True
        key = (sig.strategy, sig.exchange, sig.instrument_id, sig.side)
        return now_mono_ms() - self.last_signal_ms[key] >= self.cfg.signal_cooldown_ms

    def _maybe_log_diagnostics(
        self,
        event: str,
        strategy_counts: Mapping[str, int],
        skip_counts: Mapping[str, int],
        **extra: Any,
    ) -> None:
        now_ms = now_mono_ms()
        if now_ms - self.last_diag_ms < self.cfg.diagnostics_interval_ms:
            return
        self.last_diag_ms = now_ms
        total_books = 0
        fresh_books = 0
        exchange_books: dict[str, int] = {}
        for exchange in self.cfg.venues:
            books = self.state.books.get(exchange, {})
            exchange_books[exchange] = len(books)
            total_books += len(books)
            fresh_books += sum(1 for book in books.values() if book.complete and book.age_ms() <= self.cfg.stale_book_ms)
        remaining = {
            exchange: self.state.time_remaining_ms(exchange)
            for exchange in self.cfg.venues
            if exchange in self.state.server_time_ms
        }
        log_event(
            self.logger,
            logging.INFO,
            event,
            live_trading=self.cfg.live_trading,
            dry_run=self.cfg.dry_run,
            total_books=total_books,
            fresh_books=fresh_books,
            exchange_books=exchange_books,
            strategy_counts=dict(strategy_counts),
            skip_counts=dict(skip_counts),
            time_remaining_ms=remaining,
            **extra,
        )

    def _risk_compression_signals(self) -> list[Signal]:
        out: list[Signal] = []
        soft_long = self.cfg.max_symbol_position * 8 // 10
        soft_short = -self.cfg.max_symbol_short * 8 // 10
        for (exchange, instr), pos in list(self.risk.positions.items()):
            if exchange not in self.cfg.venues:
                continue
            if soft_short < pos < soft_long:
                continue
            _venue, ticker = split_instrument(instr)
            book = self.state.fresh_book(exchange, ticker, self.cfg.stale_book_ms)
            if not book:
                continue
            qty = min(abs(pos) // 4 + 1, self.cfg.order_size, self.cfg.flatten_order_qty)
            if pos > soft_long and book.best_bid is not None:
                out.append(
                    Signal(
                        "risk_compression",
                        exchange,
                        instr,
                        "ask",
                        max(1, book.best_bid - self.cfg.ioc_slip_cents),
                        qty,
                        0,
                        8_000,
                        True,
                        "soft long inventory compression",
                    )
                )
            elif pos < soft_short and book.best_ask is not None:
                out.append(
                    Signal(
                        "risk_compression",
                        exchange,
                        instr,
                        "bid",
                        book.best_ask + self.cfg.ioc_slip_cents,
                        qty,
                        0,
                        8_000,
                        True,
                        "soft short inventory compression",
                    )
                )
        return out


class ExchangeClient:
    def __init__(
        self,
        exchange: str,
        state: MarketState,
        risk: RiskManager,
        engine: StrategyEngine,
        cfg: Config,
        logger: logging.Logger,
    ) -> None:
        self.exchange = canonical_exchange(exchange)
        self.state = state
        self.risk = risk
        self.engine = engine
        self.cfg = cfg
        self.logger = logger
        self.host = os.environ.get(f"{self.exchange.upper()}_HOST", HOSTS[self.exchange])
        self.url = f"ws://{self.host}:9001/trade"
        self.rate_limiter = TokenBucketRateLimiter(cfg.max_msgs_per_sec)
        self.ws: Any = None
        self.connected = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.request_seq = 0
        self.submissions: dict[str, Submission] = {}
        self.periodic_tasks: list[asyncio.Task[Any]] = []

    async def run(self) -> None:
        backoff_ms = 250
        while not self.stop_event.is_set():
            try:
                await self._connect_once()
                backoff_ms = 250
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected.clear()
                log_event(
                    self.logger,
                    logging.WARNING,
                    "connection_error",
                    exchange=self.exchange,
                    error=repr(exc),
                    backoff_ms=backoff_ms,
                )
                await asyncio.sleep(backoff_ms / 1000)
                backoff_ms = min(5_000, int(backoff_ms * 1.7) + random.randint(0, 200))

    async def _connect_once(self) -> None:
        import websockets

        log_event(self.logger, logging.INFO, "connect", exchange=self.exchange, url=self.url)
        try:
            async with websockets.connect(
                self.url,
                max_size=16 * 1024 * 1024,
                compression=None,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=self.cfg.connect_timeout_ms / 1000,
            ) as ws:
                self.ws = ws
                self.state.reset_exchange(self.exchange)
                self.connected.set()
                self.periodic_tasks = [
                    asyncio.create_task(self._inventory_loop(), name=f"inventory-{self.exchange}"),
                    asyncio.create_task(self._pending_loop(), name=f"pending-{self.exchange}"),
                ]
                await self._send({"type": "get_market_data", "user_request_id": self._next_request_id("snapshot")})
                async for raw in ws:
                    await self._handle_message(raw)
        finally:
            self.connected.clear()
            self.ws = None
            for task in self.periodic_tasks:
                task.cancel()
            for task in self.periodic_tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            self.periodic_tasks.clear()

    async def _handle_message(self, raw: Any) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str) and raw == "Message rate limit exceeded":
            log_event(self.logger, logging.ERROR, "rate_limit_disconnect", exchange=self.exchange)
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log_event(self.logger, logging.WARNING, "bad_json", exchange=self.exchange, raw=str(raw)[:200])
            return
        msg_type = data.get("type")
        if msg_type == "welcome":
            log_event(self.logger, logging.INFO, "welcome", exchange=self.exchange)
        elif msg_type == "market_data_update":
            self.state.update_from_market_data(self.exchange, data)
            await self.engine.on_market_data(self.exchange)
        elif msg_type == "add_order_response":
            self._handle_order_response(data)
        elif msg_type == "cancel_order_response":
            log_event(
                self.logger,
                logging.INFO if data.get("success") else logging.WARNING,
                "cancel_response",
                exchange=self.exchange,
                success=bool(data.get("success")),
                request_id=data.get("user_request_id"),
                message=data.get("message"),
            )
        elif msg_type == "get_inventory_response":
            payload = data.get("data", {})
            if isinstance(payload, dict):
                self.risk.sync_inventory(self.exchange, payload)
                self._log_inventory_snapshot(payload)
        elif msg_type == "get_pending_orders_response":
            self._handle_pending_orders(data.get("data", {}))
        elif msg_type == "end_of_round":
            log_event(self.logger, logging.INFO, "end_of_round", exchange=self.exchange)
            self.state.reset_exchange(self.exchange)
            self.risk.reset_exchange(self.exchange)
        elif msg_type == "error":
            log_event(
                self.logger,
                logging.WARNING,
                "server_error",
                exchange=self.exchange,
                request_id=data.get("user_request_id"),
                message=data.get("message"),
            )
        else:
            log_event(self.logger, logging.DEBUG, "unknown_message", exchange=self.exchange, type=msg_type)

    async def submit_order(self, sig: Signal) -> None:
        request_id = self._next_request_id(sig.strategy)
        log_event(
            self.logger,
            logging.INFO,
            "opportunity",
            strategy=sig.strategy,
            exchange=sig.exchange,
            instrument=sig.instrument_id,
            side=sig.side,
            price=sig.price,
            qty=sig.quantity,
            edge_cents=sig.edge_cents,
            confidence_bps=sig.confidence_bps,
            reduce_only=sig.reduce_only,
            reason=sig.reason,
            score=score_signal(sig, self.cfg),
            group_id=sig.group_id,
        )
        if self.cfg.dry_run or not self.cfg.live_trading:
            log_event(
                self.logger,
                logging.INFO,
                "dry_order",
                exchange=self.exchange,
                request_id=request_id,
                instrument=sig.instrument_id,
                side=sig.side,
                price=sig.price,
                qty=sig.quantity,
                order_type=sig.order_type,
            )
            return
        self.risk.reserve(sig.exchange, sig.instrument_id, sig.side, sig.price, sig.quantity)
        self.submissions[request_id] = Submission(sig, request_id, now_mono_ms())
        order: dict[str, Any] = {
            "type": "add_order",
            "user_request_id": request_id,
            "instrument_id": sig.instrument_id,
            "side": sig.side,
            "quantity": sig.quantity,
            "order_type": sig.order_type,
        }
        if sig.order_type in {"ioc", "limit"}:
            order["price"] = sig.price
            order["expiry"] = now_epoch_ms() + (self.cfg.passive_expiry_ms if sig.order_type == "limit" else self.cfg.ioc_expiry_ms)
        await self._send(order)
        log_event(
            self.logger,
            logging.INFO,
            "submit_order",
            exchange=self.exchange,
            request_id=request_id,
            instrument=sig.instrument_id,
            side=sig.side,
            price=sig.price,
            qty=sig.quantity,
            order_type=sig.order_type,
        )

    def _handle_order_response(self, data: Mapping[str, Any]) -> None:
        request_id = str(data.get("user_request_id", ""))
        sub = self.submissions.pop(request_id, None)
        payload = data.get("data", {}) if isinstance(data.get("data"), dict) else {}
        if sub is not None:
            sig = sub.signal
            self.risk.release(sig.exchange, sig.instrument_id, sig.side, sig.price, sig.quantity)
        success = bool(data.get("success"))
        if success and sub is not None:
            inv_change = payload.get("immediate_inventory_change")
            cash_change = payload.get("immediate_balance_change")
            self.risk.apply_immediate_change(self.exchange, sub.signal.instrument_id, inv_change, cash_change)
            log_event(
                self.logger,
                logging.INFO,
                "order_response",
                exchange=self.exchange,
                request_id=request_id,
                success=True,
                order_id=payload.get("order_id"),
                instrument=sub.signal.instrument_id,
                side=sub.signal.side,
                qty=sub.signal.quantity,
                price=sub.signal.price,
                immediate_inventory_change=inv_change,
                immediate_balance_change=cash_change,
                latency_ms=now_mono_ms() - sub.sent_mono_ms,
            )
        else:
            log_event(
                self.logger,
                logging.WARNING,
                "order_reject",
                exchange=self.exchange,
                request_id=request_id,
                success=success,
                message=payload.get("message") or data.get("message"),
            )

    async def _inventory_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.inventory_poll_ms / 1000)
            if self.connected.is_set():
                await self._send({"type": "get_inventory", "user_request_id": self._next_request_id("inv")})

    async def _pending_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.pending_poll_ms / 1000)
            if self.connected.is_set():
                await self._send({"type": "get_pending_orders", "user_request_id": self._next_request_id("pend")})

    def _handle_pending_orders(self, data: Mapping[str, Any]) -> None:
        count = 0
        if isinstance(data, dict):
            for pair in data.values():
                if isinstance(pair, list) and len(pair) == 2:
                    count += len(pair[0]) + len(pair[1])
        self.risk.pending_count_by_exchange[self.exchange] = count
        log_event(self.logger, logging.DEBUG, "pending_orders", exchange=self.exchange, count=count)

    def _log_inventory_snapshot(self, payload: Mapping[str, Any]) -> None:
        cash_pair = payload.get("$", [0, self.risk.cash[self.exchange]])
        nonzero = {
            key: int(value[1])
            for key, value in payload.items()
            if key != "$" and isinstance(value, list) and len(value) >= 2 and int(value[1]) != 0
        }
        log_event(
            self.logger,
            logging.INFO,
            "inventory",
            exchange=self.exchange,
            cash=int(cash_pair[1]) if isinstance(cash_pair, list) and len(cash_pair) >= 2 else self.risk.cash[self.exchange],
            nonzero_positions=nonzero,
        )

    async def _send(self, payload: Mapping[str, Any]) -> None:
        if self.ws is None:
            return
        await self.rate_limiter.acquire()
        await self.ws.send(json.dumps(payload, separators=(",", ":")))

    def _next_request_id(self, prefix: str) -> str:
        self.request_seq += 1
        return f"hege-{self.exchange}-{prefix}-{now_epoch_ms()}-{self.request_seq}"

    async def stop(self) -> None:
        self.stop_event.set()
        if self.ws is not None:
            await self.ws.close()


async def probe_home_location(cfg: Config, logger: logging.Logger) -> str:
    if cfg.physical_latency_removed:
        log_event(
            logger,
            logging.INFO,
            "home_probe_skipped",
            reason="physical_latency_removed",
            default_home=cfg.default_home,
        )
        return cfg.active_home
    if cfg.bot_home != "AUTO":
        return cfg.bot_home
    candidates = ("NYSE", "ZSE", "HKEX")
    probes = [venue for venue in cfg.venues if venue in HOSTS]
    if len(probes) < 3:
        return cfg.default_home
    try:
        import aiohttp
    except Exception:
        return cfg.default_home

    timeout = aiohttp.ClientTimeout(total=cfg.connect_timeout_ms / 1000)
    observed: dict[str, int] = {}
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def one(venue: str) -> None:
            host = os.environ.get(f"{venue.upper()}_HOST", HOSTS[venue])
            url = f"http://{host}:9001/health"
            started = time.perf_counter_ns()
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        await resp.read()
                        observed[venue] = (time.perf_counter_ns() - started) // 1_000_000
            except Exception:
                return

        await asyncio.gather(*(one(venue) for venue in probes))
    if len(observed) < 3:
        log_event(logger, logging.INFO, "home_probe_fallback", observed=observed, default_home=cfg.default_home)
        return cfg.default_home
    best_home = cfg.default_home
    best_error: Optional[int] = None
    for home in candidates:
        expected = LATENCY_RTT_MS[home]
        error = 0
        for venue, rtt in observed.items():
            error += abs(rtt - expected.get(venue, 120))
        if best_error is None or error < best_error:
            best_error = error
            best_home = home
    log_event(logger, logging.INFO, "home_probe", observed=observed, selected_home=best_home, error=best_error)
    return best_home


async def run_bot(cfg: Config) -> None:
    logger = setup_logging(cfg)
    selected_home = await probe_home_location(cfg, logger)
    if cfg.bot_home == "AUTO":
        cfg = Config(**{**cfg.__dict__, "default_home": selected_home})
    state = MarketState()
    risk = RiskManager(cfg)
    engine = StrategyEngine(state, risk, cfg, logger)
    clients = {venue: ExchangeClient(venue, state, risk, engine, cfg, logger) for venue in cfg.venues}
    engine.set_clients(clients)

    log_event(
        logger,
        logging.INFO,
        "startup",
        live_trading=cfg.live_trading,
        dry_run=cfg.dry_run,
        venues=cfg.venues,
        home=cfg.active_home,
        physical_latency_removed=cfg.physical_latency_removed,
        max_msgs_per_sec=cfg.max_msgs_per_sec,
        order_size=cfg.order_size,
        arb_lots=cfg.arb_lots,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(getattr(signal_module, sig_name), stop.set)

    tasks = [asyncio.create_task(client.run(), name=f"client-{venue}") for venue, client in clients.items()]
    await stop.wait()
    log_event(logger, logging.INFO, "shutdown_requested")
    for client in clients.values():
        await client.stop()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def explain() -> str:
    return (
        "HEGE runs four independent signal modules: executable ETF/basket IOC arbitrage, "
        "robust cross-venue lead/lag outlier trading, safe-haven inverse-momentum lag trading, "
        "and microprice/order-book imbalance confirmation. By default venue routing assumes "
        "physical bot-to-exchange latency has been removed, while cross-venue data-delay "
        "dislocations remain tradable. Every signal must pass local cash, position, in-flight order, stale-book, "
        "spread, message-budget, and end-segment gates before an order can leave the process."
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HEGE AlgoTrade 2026 bot")
    parser.add_argument("--explain", action="store_true", help="print strategy summary and exit")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.explain:
        print(explain())
        return 0
    cfg = Config.from_env()
    try:
        asyncio.run(run_bot(cfg))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
