#!/usr/bin/env python3
"""
Competition-grade AlgoTrade 2026 bot.

Built only from the local .firecrawl docs:
  - WebSocket JSON API, no auth token, source-IP auth.
  - Prices are integer cents.
  - Market data broadcasts every 100 ms.
  - IOC / limit / market orders.
  - Segment boundaries reset cash, positions, and orders.

Default mode is dry-run. Set LIVE_TRADING=1 to send orders.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from typing import Any, DefaultDict, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


EXCHANGE_HOSTS: Dict[str, str] = {
    "NYSE": "nyse.algotrade.hr",
    "NASDAQ": "nasdaq.algotrade.hr",
    "SSE": "sse.algotrade.hr",
    "JPX": "jpx.algotrade.hr",
    "EURONEXT": "euronext.algotrade.hr",
    "LSE": "lse.algotrade.hr",
    "HKEX": "hkex.algotrade.hr",
    "NSE": "nse.algotrade.hr",
    "TMX": "tmx.algotrade.hr",
    "ZSE": "zse.algotrade.hr",
}

LATENCY_RTT_MS: Dict[str, Dict[str, int]] = {
    "NYSE": {"NYSE": 0, "NASDAQ": 1, "SSE": 165, "JPX": 152, "EURONEXT": 84, "LSE": 80, "HKEX": 180, "NSE": 174, "TMX": 11, "ZSE": 96},
    "NASDAQ": {"NYSE": 1, "NASDAQ": 0, "SSE": 165, "JPX": 152, "EURONEXT": 84, "LSE": 80, "HKEX": 180, "NSE": 174, "TMX": 11, "ZSE": 96},
    "SSE": {"NYSE": 165, "NASDAQ": 165, "SSE": 0, "JPX": 18, "EURONEXT": 160, "LSE": 156, "HKEX": 19, "NSE": 54, "TMX": 159, "ZSE": 145},
    "JPX": {"NYSE": 152, "NASDAQ": 152, "SSE": 18, "JPX": 0, "EURONEXT": 145, "LSE": 141, "HKEX": 37, "NSE": 53, "TMX": 145, "ZSE": 140},
    "EURONEXT": {"NYSE": 84, "NASDAQ": 84, "SSE": 160, "JPX": 145, "EURONEXT": 0, "LSE": 6, "HKEX": 130, "NSE": 130, "TMX": 86, "ZSE": 22},
    "LSE": {"NYSE": 80, "NASDAQ": 80, "SSE": 156, "JPX": 141, "EURONEXT": 6, "LSE": 0, "HKEX": 135, "NSE": 134, "TMX": 82, "ZSE": 24},
    "HKEX": {"NYSE": 180, "NASDAQ": 180, "SSE": 19, "JPX": 37, "EURONEXT": 130, "LSE": 135, "HKEX": 0, "NSE": 53, "TMX": 174, "ZSE": 150},
    "NSE": {"NYSE": 174, "NASDAQ": 174, "SSE": 54, "JPX": 53, "EURONEXT": 130, "LSE": 134, "HKEX": 53, "NSE": 0, "TMX": 174, "ZSE": 95},
    "TMX": {"NYSE": 11, "NASDAQ": 11, "SSE": 159, "JPX": 145, "EURONEXT": 86, "LSE": 82, "HKEX": 174, "NSE": 174, "TMX": 0, "ZSE": 98},
    "ZSE": {"NYSE": 96, "NASDAQ": 96, "SSE": 145, "JPX": 140, "EURONEXT": 22, "LSE": 24, "HKEX": 150, "NSE": 95, "TMX": 98, "ZSE": 0},
}

SECTOR_A = ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR")
SECTOR_B = ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH")
INDEPENDENTS = ("MDKA", "KRAS", "ZITO", "ZABA", "SIMP", "CARD")
SAFE_HAVENS = ("GOLD", "XAG", "ETFSH")
BROAD_MARKET_TICKERS = SECTOR_A + SECTOR_B + INDEPENDENTS

ETF_BASKETS: Dict[str, Tuple[str, ...]] = {
    "ETFA": ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"),
    "ETFB": ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"),
    "ETFA3": ("NGUP", "KTST", "XFR"),
    "ETFB3": ("KOTD", "INA", "DLKV"),
    "ETFSH": ("GOLD", "XAG"),
}

STARTING_CASH_CENTS = 10_000_000
HARD_CASH_FLOOR_CENTS = -5_000_000
HARD_LONG_LIMIT = 2_000
HARD_SHORT_LIMIT = -200
DEFAULT_ROUND_LENGTH_MS = 600_000


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        value = int(raw)
    if lo is not None:
        value = max(lo, value)
    if hi is not None:
        value = min(hi, value)
    return value


def env_list(name: str, default: Sequence[str]) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return tuple(default)
    values = tuple(item.strip().upper() for item in raw.split(",") if item.strip())
    return values or tuple(default)


def now_ms() -> int:
    return int(time.time() * 1000)


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, value))


def split_instrument(instrument_id: str) -> Tuple[str, str]:
    if "-" not in instrument_id:
        return "", instrument_id
    exchange, ticker = instrument_id.split("-", 1)
    return exchange.upper(), ticker.upper()


def median_int(values: Sequence[int]) -> Optional[int]:
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) // 2


def setup_logging() -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger("god_bot")
    logger.setLevel(getattr(logging, level_name, logging.INFO))
    logger.handlers.clear()

    formatter = logging.Formatter("%(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    log_file = os.getenv("LOG_FILE", "").strip()
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


LOGGER = setup_logging()


def jlog(event: str, **fields: Any) -> None:
    payload = {"ts": now_ms(), "event": event}
    payload.update(fields)
    LOGGER.info(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))


@dataclass
class Config:
    venues: Tuple[str, ...]
    live: bool
    bot_location: str
    active_location: str
    latency_probe: bool
    rate_limit_per_sec: int
    base_order_qty: int
    max_order_qty: int
    flatten_order_qty: int
    max_position_per_instrument: int
    max_short_per_instrument: int
    max_gross_notional_per_exchange: int
    min_cash_cushion_cents: int
    max_orders_per_tick: int
    order_cooldown_ms: int
    ioc_ttl_ms: int
    max_signal_age_ms: int
    min_edge_cents: int
    etf_extra_edge_cents: int
    cross_extra_edge_cents: int
    micro_extra_edge_cents: int
    card_simp_edge_cents: int
    safe_extra_edge_cents: int
    latency_edge_ms_per_cent: int
    take_depth_fraction_bps: int
    vol_edge_mult_bps: int
    confidence_size_mult_bps: int
    inventory_skew_bps: int
    flatten_start_ms: int
    risk_compress_start_ms: int
    inventory_poll_sec: int
    round_length_ms: int
    enable_etf_arb: bool
    enable_cross_venue: bool
    enable_microprice: bool
    enable_safe_haven: bool
    enable_card_simp: bool
    enable_flatten: bool
    enable_etf_hedge: bool
    safe_haven_beta_bps: int
    safe_haven_signal_bps: int
    micro_min_imbalance_bps: int
    micro_max_spread_cents: int
    dry_run_log_orders: bool

    @classmethod
    def from_env(cls) -> "Config":
        default_venues = ("NYSE", "NASDAQ", "SSE", "JPX", "EURONEXT", "LSE", "HKEX", "NSE", "TMX", "ZSE")
        venues = tuple(v for v in env_list("VENUES", default_venues) if v in EXCHANGE_HOSTS)
        if len(venues) > 10:
            venues = venues[:10]

        live = env_bool("LIVE_TRADING", False)
        bot_location = os.getenv("BOT_LOCATION", "AUTO").strip().upper() or "AUTO"
        active_location = "ZSE" if bot_location == "AUTO" else bot_location
        return cls(
            venues=venues,
            live=live,
            bot_location=bot_location,
            active_location=active_location if active_location in EXCHANGE_HOSTS else "ZSE",
            latency_probe=env_bool("LATENCY_PROBE", True),
            rate_limit_per_sec=env_int("RATE_LIMIT_PER_SEC", 150, 1, 450),
            base_order_qty=env_int("BASE_ORDER_QTY", 4, 1, 2_000),
            max_order_qty=env_int("MAX_ORDER_QTY", 25, 1, 2_000),
            flatten_order_qty=env_int("FLATTEN_ORDER_QTY", 75, 1, 2_000),
            max_position_per_instrument=env_int("MAX_POS_PER_INSTRUMENT", 250, 1, HARD_LONG_LIMIT),
            max_short_per_instrument=env_int("MAX_SHORT_PER_INSTRUMENT", 60, 0, abs(HARD_SHORT_LIMIT)),
            max_gross_notional_per_exchange=env_int("MAX_GROSS_NOTIONAL_PER_EXCHANGE", 4_000_000, 100_000, 20_000_000),
            min_cash_cushion_cents=env_int("MIN_CASH_CUSHION_CENTS", 500_000, 0, 10_000_000),
            max_orders_per_tick=env_int("MAX_ORDERS_PER_TICK", 18, 1, 200),
            order_cooldown_ms=env_int("ORDER_COOLDOWN_MS", 120, 0, 10_000),
            ioc_ttl_ms=env_int("IOC_TTL_MS", 1_500, 100, 60_000),
            max_signal_age_ms=env_int("MAX_SIGNAL_AGE_MS", 850, 100, 10_000),
            min_edge_cents=env_int("MIN_EDGE_CENTS", 4, 1, 10_000),
            etf_extra_edge_cents=env_int("ETF_EXTRA_EDGE_CENTS", 1, 0, 10_000),
            cross_extra_edge_cents=env_int("CROSS_EXTRA_EDGE_CENTS", 2, 0, 10_000),
            micro_extra_edge_cents=env_int("MICRO_EXTRA_EDGE_CENTS", 3, 0, 10_000),
            card_simp_edge_cents=env_int("CARD_SIMP_EDGE_CENTS", 7, 1, 10_000),
            safe_extra_edge_cents=env_int("SAFE_EXTRA_EDGE_CENTS", 4, 0, 10_000),
            latency_edge_ms_per_cent=env_int("LATENCY_EDGE_MS_PER_CENT", 35, 1, 10_000),
            take_depth_fraction_bps=env_int("TAKE_DEPTH_FRACTION_BPS", 2_500, 1, 10_000),
            vol_edge_mult_bps=env_int("VOL_EDGE_MULT_BPS", 8_000, 0, 100_000),
            confidence_size_mult_bps=env_int("CONFIDENCE_SIZE_MULT_BPS", 12_000, 1_000, 100_000),
            inventory_skew_bps=env_int("INVENTORY_SKEW_BPS", 3_000, 0, 20_000),
            flatten_start_ms=env_int("FLATTEN_START_MS", 55_000, 0, DEFAULT_ROUND_LENGTH_MS),
            risk_compress_start_ms=env_int("RISK_COMPRESS_START_MS", 95_000, 0, DEFAULT_ROUND_LENGTH_MS),
            inventory_poll_sec=env_int("INVENTORY_POLL_SEC", 2, 1, 60),
            round_length_ms=env_int("ROUND_LENGTH_MS", DEFAULT_ROUND_LENGTH_MS, 60_000, 3_600_000),
            enable_etf_arb=env_bool("ENABLE_ETF_ARB", True),
            enable_cross_venue=env_bool("ENABLE_CROSS_VENUE", True),
            enable_microprice=env_bool("ENABLE_MICROPRICE", True),
            enable_safe_haven=env_bool("ENABLE_SAFE_HAVEN", True),
            enable_card_simp=env_bool("ENABLE_CARD_SIMP", True),
            enable_flatten=env_bool("ENABLE_FLATTEN", True),
            enable_etf_hedge=env_bool("ENABLE_ETF_HEDGE", False),
            safe_haven_beta_bps=env_int("SAFE_HAVEN_BETA_BPS", 8_000, 0, 50_000),
            safe_haven_signal_bps=env_int("SAFE_HAVEN_SIGNAL_BPS", 6, 0, 10_000),
            micro_min_imbalance_bps=env_int("MICRO_MIN_IMBALANCE_BPS", 3_500, 0, 10_000),
            micro_max_spread_cents=env_int("MICRO_MAX_SPREAD_CENTS", 14, 1, 10_000),
            dry_run_log_orders=env_bool("DRY_RUN_LOG_ORDERS", True),
        )


@dataclass(frozen=True)
class Book:
    bids: Tuple[Tuple[int, int], ...] = ()
    asks: Tuple[Tuple[int, int], ...] = ()

    @classmethod
    def from_depth(cls, depth: Mapping[str, Any]) -> "Book":
        raw_bids = depth.get("bids") or {}
        raw_asks = depth.get("asks") or {}
        bids = tuple(sorted(((int(price), int(qty)) for price, qty in raw_bids.items() if int(qty) > 0), reverse=True))
        asks = tuple(sorted(((int(price), int(qty)) for price, qty in raw_asks.items() if int(qty) > 0)))
        return cls(bids=bids, asks=asks)

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
    def is_crossed_or_empty(self) -> bool:
        return self.best_bid is None or self.best_ask is None or self.best_bid >= self.best_ask

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) // 2

    @property
    def microprice(self) -> Optional[int]:
        if self.best_bid is None or self.best_ask is None:
            return None
        total = self.best_bid_qty + self.best_ask_qty
        if total <= 0:
            return self.mid
        return (self.best_ask * self.best_bid_qty + self.best_bid * self.best_ask_qty) // total

    @property
    def imbalance_bps(self) -> int:
        total = self.best_bid_qty + self.best_ask_qty
        if total <= 0:
            return 0
        return ((self.best_bid_qty - self.best_ask_qty) * 10_000) // total

    def executable_qty(self, side: str, limit_price: int, cap_qty: int) -> int:
        qty_left = max(0, cap_qty)
        done = 0
        levels = self.asks if side == "bid" else self.bids
        for price, qty in levels:
            crosses = price <= limit_price if side == "bid" else price >= limit_price
            if not crosses or qty_left <= 0:
                break
            take = min(qty_left, qty)
            done += take
            qty_left -= take
        return done

    def top_depth_qty_for_taker(self, side: str) -> int:
        return self.best_ask_qty if side == "bid" else self.best_bid_qty


@dataclass
class InstrumentState:
    exchange: str
    ticker: str
    book: Book = field(default_factory=Book)
    last_mid: Optional[int] = None
    prev_mid: Optional[int] = None
    last_update_mono_ms: int = 0
    last_server_time_ms: int = 0
    vol_cents_ewma: float = 0.0
    return_bps_ewma: float = 0.0
    signed_flow_ewma: float = 0.0
    candles: Deque[Mapping[str, Any]] = field(default_factory=lambda: deque(maxlen=32))
    last_trade_prices: Deque[int] = field(default_factory=lambda: deque(maxlen=32))

    @property
    def instrument_id(self) -> str:
        return f"{self.exchange}-{self.ticker}"

    def update_book(self, book: Book, server_time_ms: int) -> None:
        self.book = book
        mid = book.mid
        if mid is not None:
            if self.last_mid is not None and self.last_mid > 0:
                move = mid - self.last_mid
                self.prev_mid = self.last_mid
                self.vol_cents_ewma = 0.85 * self.vol_cents_ewma + 0.15 * abs(move)
                ret_bps = (move * 10_000) / self.last_mid
                self.return_bps_ewma = 0.75 * self.return_bps_ewma + 0.25 * ret_bps
            self.last_mid = mid
        self.last_update_mono_ms = monotonic_ms()
        self.last_server_time_ms = int(server_time_ms)

    def update_candles(self, candles: Iterable[Mapping[str, Any]]) -> None:
        for candle in candles:
            self.candles.append(candle)
            close = candle.get("close")
            open_ = candle.get("open")
            if isinstance(close, int) and isinstance(open_, int) and open_ > 0:
                move = close - open_
                self.vol_cents_ewma = max(self.vol_cents_ewma, abs(move) * 0.5)

    def update_trade(self, price: int, qty: int) -> None:
        self.last_trade_prices.append(price)
        mid = self.book.mid or self.last_mid
        if mid is None:
            return
        signed = qty if price >= mid else -qty
        self.signed_flow_ewma = 0.80 * self.signed_flow_ewma + 0.20 * signed

    def age_ms(self) -> int:
        if not self.last_update_mono_ms:
            return 1_000_000
        return max(0, monotonic_ms() - self.last_update_mono_ms)


class MarketState:
    def __init__(self) -> None:
        self.instruments: Dict[str, InstrumentState] = {}
        self.by_exchange: DefaultDict[str, Dict[str, InstrumentState]] = defaultdict(dict)
        self.last_exchange_time: Dict[str, int] = {}
        self.round_length_ms: Dict[str, int] = defaultdict(lambda: DEFAULT_ROUND_LENGTH_MS)

    def reset_exchange(self, exchange: str) -> None:
        exchange = exchange.upper()
        for instrument_id in list(self.instruments):
            ex, ticker = split_instrument(instrument_id)
            if ex == exchange:
                self.instruments.pop(instrument_id, None)
                self.by_exchange[exchange].pop(ticker, None)
        self.last_exchange_time.pop(exchange, None)

    def get_or_create(self, exchange: str, ticker: str) -> InstrumentState:
        exchange = exchange.upper()
        ticker = ticker.upper()
        instrument_id = f"{exchange}-{ticker}"
        state = self.instruments.get(instrument_id)
        if state is None:
            state = InstrumentState(exchange=exchange, ticker=ticker)
            self.instruments[instrument_id] = state
            self.by_exchange[exchange][ticker] = state
        return state

    def update_from_market_data(self, exchange: str, msg: Mapping[str, Any]) -> None:
        server_time = int(msg.get("time") or 0)
        self.last_exchange_time[exchange] = server_time
        for instrument_id, depth in (msg.get("orderbook_depths") or {}).items():
            ex, ticker = split_instrument(instrument_id)
            if not ex:
                ex = exchange
            state = self.get_or_create(ex, ticker)
            state.update_book(Book.from_depth(depth), server_time)

        tradeable = ((msg.get("candles") or {}).get("tradeable") or {})
        for instrument_id, candles in tradeable.items():
            ex, ticker = split_instrument(instrument_id)
            if candles:
                self.get_or_create(ex or exchange, ticker).update_candles(candles)

        for event in msg.get("events") or []:
            if event.get("event_type") != "trade":
                continue
            data = event.get("data") or {}
            instrument_id = data.get("instrumentID")
            if not instrument_id:
                continue
            ex, ticker = split_instrument(str(instrument_id))
            price = data.get("price")
            qty = data.get("quantity")
            if isinstance(price, int) and isinstance(qty, int):
                self.get_or_create(ex or exchange, ticker).update_trade(price, qty)

    def exchange_time_left_ms(self, exchange: str, cfg: Config) -> int:
        server_time = self.last_exchange_time.get(exchange, 0)
        length = self.round_length_ms.get(exchange, cfg.round_length_ms)
        return max(0, length - server_time)

    def state(self, exchange: str, ticker: str) -> Optional[InstrumentState]:
        return self.by_exchange.get(exchange.upper(), {}).get(ticker.upper())

    def tickers_on_exchange(self, exchange: str) -> Tuple[str, ...]:
        return tuple(self.by_exchange.get(exchange.upper(), {}).keys())

    def fresh_states(self, ticker: str, cfg: Config, exclude_exchange: Optional[str] = None) -> List[InstrumentState]:
        ticker = ticker.upper()
        exclude = exclude_exchange.upper() if exclude_exchange else None
        out: List[InstrumentState] = []
        for by_ticker in self.by_exchange.values():
            state = by_ticker.get(ticker)
            if state is None or state.last_mid is None:
                continue
            if exclude and state.exchange == exclude:
                continue
            if state.age_ms() <= cfg.max_signal_age_ms and not state.book.is_crossed_or_empty:
                out.append(state)
        return out

    def fair_for_ticker(
        self,
        ticker: str,
        cfg: Config,
        exclude_exchange: Optional[str] = None,
        preferred_exchange: Optional[str] = None,
    ) -> Optional[int]:
        preferred: Optional[InstrumentState] = None
        if preferred_exchange:
            preferred = self.state(preferred_exchange, ticker)
            if preferred and preferred.last_mid is not None and preferred.age_ms() <= cfg.max_signal_age_ms:
                return preferred.last_mid

        states = self.fresh_states(ticker, cfg, exclude_exchange=exclude_exchange)
        if not states:
            return None

        mids = [int(s.last_mid) for s in states if s.last_mid is not None]
        med = median_int(mids)
        if med is None:
            return None

        weighted_num = 0
        weighted_den = 0
        for s in states:
            if s.last_mid is None:
                continue
            latency = LATENCY_RTT_MS.get(cfg.active_location, {}).get(s.exchange, 120)
            age = s.age_ms()
            depth = max(1, s.book.best_bid_qty + s.book.best_ask_qty)
            weight = max(1, (depth * 1_000) // max(25, age + latency + 25))
            weighted_num += int(s.last_mid) * weight
            weighted_den += weight

        if weighted_den <= 0:
            return med
        weighted = weighted_num // weighted_den
        band = max(8, int(self.ticker_vol_cents(ticker, cfg) * 4), cfg.min_edge_cents * 3)
        if abs(weighted - med) > band:
            return (weighted + med) // 2
        return weighted

    def ticker_vol_cents(self, ticker: str, cfg: Config) -> int:
        states = self.fresh_states(ticker, cfg)
        if not states:
            return 0
        return int(sum(s.vol_cents_ewma for s in states) / len(states))

    def broad_market_return_bps(self, cfg: Config) -> int:
        values: List[float] = []
        for ticker in BROAD_MARKET_TICKERS:
            states = self.fresh_states(ticker, cfg)
            if states:
                values.append(sum(s.return_bps_ewma for s in states) / len(states))
        if not values:
            return 0
        return int(sum(values) / len(values))


@dataclass(frozen=True)
class OrderIntent:
    exchange: str
    ticker: str
    side: str
    price: int
    qty: int
    strategy: str
    edge_cents: int
    confidence_bps: int
    reason: str
    order_type: str = "ioc"
    flatten: bool = False

    @property
    def instrument_id(self) -> str:
        return f"{self.exchange}-{self.ticker}"


@dataclass(frozen=True)
class TradeBundle:
    strategy: str
    legs: Tuple[OrderIntent, ...]
    score: int
    reason: str


@dataclass
class PendingRisk:
    exchange: str
    ticker: str
    side: str
    price: int
    qty: int
    cash_reserved: int
    pos_delta: int


@dataclass
class ExchangeAccount:
    cash: int = STARTING_CASH_CENTS
    reserved_cash: int = 0
    positions: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))
    reserved_positions: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))
    pending_pos_delta: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))
    pending_cash_delta: int = 0
    last_inventory_mono_ms: int = 0

    def reset(self) -> None:
        self.cash = STARTING_CASH_CENTS
        self.reserved_cash = 0
        self.positions.clear()
        self.reserved_positions.clear()
        self.pending_pos_delta.clear()
        self.pending_cash_delta = 0
        self.last_inventory_mono_ms = 0


class RiskManager:
    def __init__(self, cfg: Config, state: MarketState) -> None:
        self.cfg = cfg
        self.state = state
        self.accounts: DefaultDict[str, ExchangeAccount] = defaultdict(ExchangeAccount)
        self.pending: Dict[str, PendingRisk] = {}

    def reset_exchange(self, exchange: str) -> None:
        self.accounts[exchange].reset()
        for req_id, pending in list(self.pending.items()):
            if pending.exchange == exchange:
                self.pending.pop(req_id, None)

    def apply_inventory(self, exchange: str, data: Mapping[str, Any]) -> None:
        account = self.accounts[exchange]
        if "$" in data and isinstance(data["$"], list) and len(data["$"]) >= 2:
            account.reserved_cash = int(data["$"][0])
            account.cash = int(data["$"][1])
        seen: set[str] = set()
        for instrument_id, pair in data.items():
            if instrument_id == "$" or not isinstance(pair, list) or len(pair) < 2:
                continue
            ex, ticker = split_instrument(instrument_id)
            if ex != exchange:
                continue
            account.reserved_positions[ticker] = int(pair[0])
            account.positions[ticker] = int(pair[1])
            seen.add(ticker)
        account.last_inventory_mono_ms = monotonic_ms()
        jlog("inventory", exchange=exchange, cash=account.cash, positions=dict(account.positions))

    def mark_to_mid_gross(self, exchange: str) -> int:
        account = self.accounts[exchange]
        gross = 0
        for ticker, pos in account.positions.items():
            state = self.state.state(exchange, ticker)
            price = state.book.mid if state and state.book.mid is not None else 10_000
            gross += abs(pos) * int(price)
        return gross

    def approve_bundle(self, bundle: TradeBundle) -> Tuple[bool, str]:
        shadow_pos: DefaultDict[Tuple[str, str], int] = defaultdict(int)
        shadow_cash: DefaultDict[str, int] = defaultdict(int)
        for leg in bundle.legs:
            ok, reason = self._approve_leg(leg, shadow_pos, shadow_cash)
            if not ok:
                return False, f"{leg.instrument_id}:{reason}"
            direction = 1 if leg.side == "bid" else -1
            shadow_pos[(leg.exchange, leg.ticker)] += direction * leg.qty
            if leg.side == "bid":
                shadow_cash[leg.exchange] -= leg.price * leg.qty
        return True, "ok"

    def _approve_leg(
        self,
        leg: OrderIntent,
        shadow_pos: Mapping[Tuple[str, str], int],
        shadow_cash: Mapping[str, int],
    ) -> Tuple[bool, str]:
        if leg.price <= 0 or leg.price >= 1_000_000:
            return False, "bad_price"
        if leg.qty <= 0:
            return False, "bad_qty"
        if leg.side not in {"bid", "ask"}:
            return False, "bad_side"

        account = self.accounts[leg.exchange]
        current_pos = account.positions[leg.ticker] + account.pending_pos_delta[leg.ticker]
        current_pos += shadow_pos.get((leg.exchange, leg.ticker), 0)
        pos_after = current_pos + (leg.qty if leg.side == "bid" else -leg.qty)

        long_cap = HARD_LONG_LIMIT if leg.flatten else min(HARD_LONG_LIMIT, self.cfg.max_position_per_instrument)
        short_floor = HARD_SHORT_LIMIT if leg.flatten else max(HARD_SHORT_LIMIT, -self.cfg.max_short_per_instrument)
        if pos_after > long_cap:
            return False, f"long_cap pos_after={pos_after} cap={long_cap}"
        if pos_after < short_floor:
            return False, f"short_cap pos_after={pos_after} floor={short_floor}"

        projected_cash = account.cash + account.pending_cash_delta + shadow_cash.get(leg.exchange, 0)
        if leg.side == "bid":
            projected_cash -= leg.price * leg.qty
        cash_floor = HARD_CASH_FLOOR_CENTS + self.cfg.min_cash_cushion_cents
        if projected_cash < cash_floor:
            return False, f"cash_floor cash_after={projected_cash} floor={cash_floor}"

        gross = self.mark_to_mid_gross(leg.exchange) + leg.price * leg.qty
        if not leg.flatten and gross > self.cfg.max_gross_notional_per_exchange:
            return False, f"gross_cap gross={gross}"

        state = self.state.state(leg.exchange, leg.ticker)
        if state and state.book.mid is not None and not leg.flatten:
            inventory_pressure = abs(pos_after) * 10_000 // max(1, self.cfg.max_position_per_instrument)
            if inventory_pressure > 8_000:
                return False, f"inventory_pressure={inventory_pressure}"
        return True, "ok"

    def reserve(self, request_id: str, leg: OrderIntent) -> None:
        account = self.accounts[leg.exchange]
        pos_delta = leg.qty if leg.side == "bid" else -leg.qty
        cash_reserved = leg.price * leg.qty if leg.side == "bid" else 0
        account.pending_pos_delta[leg.ticker] += pos_delta
        account.pending_cash_delta -= cash_reserved
        self.pending[request_id] = PendingRisk(
            exchange=leg.exchange,
            ticker=leg.ticker,
            side=leg.side,
            price=leg.price,
            qty=leg.qty,
            cash_reserved=cash_reserved,
            pos_delta=pos_delta,
        )

    def release(self, request_id: str) -> Optional[PendingRisk]:
        pending = self.pending.pop(request_id, None)
        if pending is None:
            return None
        account = self.accounts[pending.exchange]
        account.pending_pos_delta[pending.ticker] -= pending.pos_delta
        account.pending_cash_delta += pending.cash_reserved
        return pending

    def apply_order_response(self, exchange: str, request_id: str, success: bool, data: Mapping[str, Any]) -> None:
        pending = self.release(request_id)
        if pending is None:
            return
        if not success:
            return
        inv_change = data.get("immediate_inventory_change")
        bal_change = data.get("immediate_balance_change")
        account = self.accounts[exchange]
        if isinstance(inv_change, int):
            account.positions[pending.ticker] += inv_change
        if isinstance(bal_change, int):
            account.cash += bal_change


class TokenBucket:
    def __init__(self, rate_per_sec: int) -> None:
        self.rate = float(max(1, rate_per_sec))
        self.capacity = float(max(1, rate_per_sec))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                missing = 1.0 - self.tokens
                delay = missing / self.rate
            await asyncio.sleep(delay)


def adaptive_edge(cfg: Config, state: InstrumentState, strategy_extra: int) -> int:
    spread = state.book.spread or 0
    vol = int(state.vol_cents_ewma)
    latency = LATENCY_RTT_MS.get(cfg.active_location, {}).get(state.exchange, 120)
    latency_penalty = latency // cfg.latency_edge_ms_per_cent
    vol_penalty = (vol * cfg.vol_edge_mult_bps) // 10_000
    return max(cfg.min_edge_cents, strategy_extra + (spread // 2) + vol_penalty + latency_penalty)


def opportunity_qty(cfg: Config, book: Book, side: str, edge: int, threshold: int, flatten: bool = False) -> int:
    top = book.top_depth_qty_for_taker(side)
    depth_qty = max(1, (top * cfg.take_depth_fraction_bps) // 10_000)
    base = cfg.flatten_order_qty if flatten else cfg.base_order_qty
    confidence_bonus = max(0, edge - threshold) * cfg.confidence_size_mult_bps // max(1, threshold)
    target = base + (base * confidence_bonus // 10_000)
    cap = cfg.flatten_order_qty if flatten else cfg.max_order_qty
    return max(1, min(cap, depth_qty, target))


class Strategy:
    name = "base"

    def generate(self, exchange: str, state: MarketState, risk: RiskManager, cfg: Config) -> List[TradeBundle]:
        raise NotImplementedError


class ETFArbStrategy(Strategy):
    name = "etf_arb"

    def generate(self, exchange: str, state: MarketState, risk: RiskManager, cfg: Config) -> List[TradeBundle]:
        out: List[TradeBundle] = []
        for etf, basket in ETF_BASKETS.items():
            etf_state = state.state(exchange, etf)
            if etf_state is None or etf_state.book.is_crossed_or_empty:
                continue
            prices: List[int] = []
            for ticker in basket:
                fair = state.fair_for_ticker(ticker, cfg, preferred_exchange=exchange)
                if fair is None:
                    break
                prices.append(fair)
            if len(prices) != len(basket):
                continue
            fair_value = sum(prices) // len(prices)
            threshold = adaptive_edge(cfg, etf_state, cfg.etf_extra_edge_cents)
            ask = etf_state.book.best_ask
            bid = etf_state.book.best_bid
            if ask is not None:
                edge = fair_value - ask
                if edge >= threshold:
                    qty = opportunity_qty(cfg, etf_state.book, "bid", edge, threshold)
                    leg = OrderIntent(exchange, etf, "bid", ask, qty, self.name, edge, 10_000, f"etf_cheap fair={fair_value} threshold={threshold}")
                    legs = [leg]
                    if cfg.enable_etf_hedge:
                        legs.extend(self._hedge_legs(exchange, basket, "ask", qty, fair_value, state, cfg))
                    out.append(TradeBundle(self.name, tuple(legs), edge * qty, leg.reason))
            if bid is not None:
                edge = bid - fair_value
                if edge >= threshold:
                    qty = opportunity_qty(cfg, etf_state.book, "ask", edge, threshold)
                    leg = OrderIntent(exchange, etf, "ask", bid, qty, self.name, edge, 10_000, f"etf_rich fair={fair_value} threshold={threshold}")
                    legs = [leg]
                    if cfg.enable_etf_hedge:
                        legs.extend(self._hedge_legs(exchange, basket, "bid", qty, fair_value, state, cfg))
                    out.append(TradeBundle(self.name, tuple(legs), edge * qty, leg.reason))
        return out

    def _hedge_legs(
        self,
        exchange: str,
        basket: Sequence[str],
        hedge_side: str,
        etf_qty: int,
        fair_value: int,
        state: MarketState,
        cfg: Config,
    ) -> List[OrderIntent]:
        hedge_qty = max(1, etf_qty // max(1, len(basket)))
        legs: List[OrderIntent] = []
        for ticker in basket:
            s = state.state(exchange, ticker) or state.state("ZSE", ticker)
            if s is None or s.book.is_crossed_or_empty:
                continue
            price = s.book.best_bid if hedge_side == "ask" else s.book.best_ask
            if price is None:
                continue
            legs.append(OrderIntent(s.exchange, ticker, hedge_side, price, hedge_qty, self.name, 1, 5_000, f"etf_hedge etf_fair={fair_value}"))
        return legs


class CrossVenueStrategy(Strategy):
    name = "cross_venue"

    def generate(self, exchange: str, state: MarketState, risk: RiskManager, cfg: Config) -> List[TradeBundle]:
        out: List[TradeBundle] = []
        for ticker in state.tickers_on_exchange(exchange):
            inst = state.state(exchange, ticker)
            if inst is None or inst.book.is_crossed_or_empty:
                continue
            fair = state.fair_for_ticker(ticker, cfg, exclude_exchange=exchange)
            if fair is None:
                continue
            threshold = adaptive_edge(cfg, inst, cfg.cross_extra_edge_cents)
            ask = inst.book.best_ask
            bid = inst.book.best_bid
            if ask is not None:
                edge = fair - ask
                if edge >= threshold:
                    qty = opportunity_qty(cfg, inst.book, "bid", edge, threshold)
                    reason = f"stale_ask fair={fair} threshold={threshold} location={cfg.active_location}"
                    leg = OrderIntent(exchange, ticker, "bid", ask, qty, self.name, edge, 9_000, reason)
                    out.append(TradeBundle(self.name, (leg,), edge * qty, reason))
            if bid is not None:
                edge = bid - fair
                if edge >= threshold:
                    qty = opportunity_qty(cfg, inst.book, "ask", edge, threshold)
                    reason = f"stale_bid fair={fair} threshold={threshold} location={cfg.active_location}"
                    leg = OrderIntent(exchange, ticker, "ask", bid, qty, self.name, edge, 9_000, reason)
                    out.append(TradeBundle(self.name, (leg,), edge * qty, reason))
        return out


class MicropriceStrategy(Strategy):
    name = "microprice"

    def generate(self, exchange: str, state: MarketState, risk: RiskManager, cfg: Config) -> List[TradeBundle]:
        out: List[TradeBundle] = []
        for ticker in state.tickers_on_exchange(exchange):
            inst = state.state(exchange, ticker)
            if inst is None or inst.book.is_crossed_or_empty or inst.book.mid is None:
                continue
            spread = inst.book.spread or 0
            if spread > cfg.micro_max_spread_cents:
                continue
            imbalance = inst.book.imbalance_bps
            if abs(imbalance) < cfg.micro_min_imbalance_bps:
                continue
            fair = state.fair_for_ticker(ticker, cfg)
            fair_bias = 0 if fair is None else (fair - inst.book.mid) // 2
            flow_bias = int(inst.signed_flow_ewma // 20)
            expected = inst.book.mid + ((imbalance * max(1, spread)) // 10_000) + fair_bias + flow_bias
            threshold = adaptive_edge(cfg, inst, cfg.micro_extra_edge_cents)
            ask = inst.book.best_ask
            bid = inst.book.best_bid
            if imbalance > 0 and ask is not None:
                edge = expected - ask
                if edge >= threshold:
                    qty = opportunity_qty(cfg, inst.book, "bid", edge, threshold)
                    reason = f"bid_pressure expected={expected} imbalance_bps={imbalance} threshold={threshold}"
                    leg = OrderIntent(exchange, ticker, "bid", ask, qty, self.name, edge, 7_500, reason)
                    out.append(TradeBundle(self.name, (leg,), edge * qty, reason))
            if imbalance < 0 and bid is not None:
                edge = bid - expected
                if edge >= threshold:
                    qty = opportunity_qty(cfg, inst.book, "ask", edge, threshold)
                    reason = f"ask_pressure expected={expected} imbalance_bps={imbalance} threshold={threshold}"
                    leg = OrderIntent(exchange, ticker, "ask", bid, qty, self.name, edge, 7_500, reason)
                    out.append(TradeBundle(self.name, (leg,), edge * qty, reason))
        return out


class SafeHavenStrategy(Strategy):
    name = "safe_haven"

    def generate(self, exchange: str, state: MarketState, risk: RiskManager, cfg: Config) -> List[TradeBundle]:
        broad_bps = state.broad_market_return_bps(cfg)
        if abs(broad_bps) < cfg.safe_haven_signal_bps:
            return []
        out: List[TradeBundle] = []
        for ticker in SAFE_HAVENS:
            inst = state.state(exchange, ticker)
            if inst is None or inst.book.is_crossed_or_empty:
                continue
            base_fair = state.fair_for_ticker(ticker, cfg, exclude_exchange=None)
            if base_fair is None:
                continue
            adjustment = -(broad_bps * base_fair * cfg.safe_haven_beta_bps) // 100_000_000
            fair = base_fair + adjustment
            threshold = adaptive_edge(cfg, inst, cfg.safe_extra_edge_cents)
            ask = inst.book.best_ask
            bid = inst.book.best_bid
            if ask is not None:
                edge = fair - ask
                if edge >= threshold:
                    qty = opportunity_qty(cfg, inst.book, "bid", edge, threshold)
                    reason = f"risk_off_safehaven broad_bps={broad_bps} adjusted_fair={fair} threshold={threshold}"
                    leg = OrderIntent(exchange, ticker, "bid", ask, qty, self.name, edge, 8_000, reason)
                    out.append(TradeBundle(self.name, (leg,), edge * qty, reason))
            if bid is not None:
                edge = bid - fair
                if edge >= threshold:
                    qty = opportunity_qty(cfg, inst.book, "ask", edge, threshold)
                    reason = f"risk_on_safehaven broad_bps={broad_bps} adjusted_fair={fair} threshold={threshold}"
                    leg = OrderIntent(exchange, ticker, "ask", bid, qty, self.name, edge, 8_000, reason)
                    out.append(TradeBundle(self.name, (leg,), edge * qty, reason))
        return out


class CardSimpStrategy(Strategy):
    name = "card_simp_pair"

    def generate(self, exchange: str, state: MarketState, risk: RiskManager, cfg: Config) -> List[TradeBundle]:
        spreads: List[int] = []
        for ex in EXCHANGE_HOSTS:
            card = state.state(ex, "CARD")
            simp = state.state(ex, "SIMP")
            if card and simp and card.book.mid is not None and simp.book.mid is not None:
                if card.age_ms() <= cfg.max_signal_age_ms and simp.age_ms() <= cfg.max_signal_age_ms:
                    spreads.append(card.book.mid - simp.book.mid)
        fair_spread = median_int(spreads)
        if fair_spread is None:
            return []

        card = state.state(exchange, "CARD")
        simp = state.state(exchange, "SIMP")
        if card is None or simp is None or card.book.is_crossed_or_empty or simp.book.is_crossed_or_empty:
            return []

        threshold = cfg.card_simp_edge_cents + max(int(card.vol_cents_ewma + simp.vol_cents_ewma), cfg.min_edge_cents)
        out: List[TradeBundle] = []
        card_ask = card.book.best_ask
        card_bid = card.book.best_bid
        simp_ask = simp.book.best_ask
        simp_bid = simp.book.best_bid

        if card_ask is not None and simp_bid is not None:
            entry_spread = card_ask - simp_bid
            edge = fair_spread - entry_spread
            if edge >= threshold:
                qty = min(
                    opportunity_qty(cfg, card.book, "bid", edge, threshold),
                    opportunity_qty(cfg, simp.book, "ask", edge, threshold),
                )
                legs = (
                    OrderIntent(exchange, "CARD", "bid", card_ask, qty, self.name, edge // 2, 8_500, f"pair_spread_low fair={fair_spread} entry={entry_spread}"),
                    OrderIntent(exchange, "SIMP", "ask", simp_bid, qty, self.name, edge // 2, 8_500, f"pair_spread_low fair={fair_spread} entry={entry_spread}"),
                )
                out.append(TradeBundle(self.name, legs, edge * qty, "long_card_short_simp"))

        if card_bid is not None and simp_ask is not None:
            entry_spread = card_bid - simp_ask
            edge = entry_spread - fair_spread
            if edge >= threshold:
                qty = min(
                    opportunity_qty(cfg, card.book, "ask", edge, threshold),
                    opportunity_qty(cfg, simp.book, "bid", edge, threshold),
                )
                legs = (
                    OrderIntent(exchange, "CARD", "ask", card_bid, qty, self.name, edge // 2, 8_500, f"pair_spread_high fair={fair_spread} entry={entry_spread}"),
                    OrderIntent(exchange, "SIMP", "bid", simp_ask, qty, self.name, edge // 2, 8_500, f"pair_spread_high fair={fair_spread} entry={entry_spread}"),
                )
                out.append(TradeBundle(self.name, legs, edge * qty, "short_card_long_simp"))
        return out


class StrategyEngine:
    def __init__(self, cfg: Config, state: MarketState, risk: RiskManager) -> None:
        self.cfg = cfg
        self.state = state
        self.risk = risk
        self.clients: Dict[str, "ExchangeClient"] = {}
        self.lock = asyncio.Lock()
        self.last_order_mono: Dict[Tuple[str, str, str, str], int] = {}
        self.strategies: List[Strategy] = []
        if cfg.enable_etf_arb:
            self.strategies.append(ETFArbStrategy())
        if cfg.enable_cross_venue:
            self.strategies.append(CrossVenueStrategy())
        if cfg.enable_microprice:
            self.strategies.append(MicropriceStrategy())
        if cfg.enable_safe_haven:
            self.strategies.append(SafeHavenStrategy())
        if cfg.enable_card_simp:
            self.strategies.append(CardSimpStrategy())

    def register_client(self, client: "ExchangeClient") -> None:
        self.clients[client.exchange] = client

    async def on_market_data(self, exchange: str, msg: Mapping[str, Any]) -> None:
        self.state.update_from_market_data(exchange, msg)
        async with self.lock:
            bundles = self._flatten_bundles(exchange)
            time_left = self.state.exchange_time_left_ms(exchange, self.cfg)
            if time_left > self.cfg.flatten_start_ms:
                for strategy in self.strategies:
                    try:
                        bundles.extend(strategy.generate(exchange, self.state, self.risk, self.cfg))
                    except Exception as exc:
                        jlog("strategy_error", exchange=exchange, strategy=strategy.name, error=repr(exc))

            bundles.sort(key=lambda b: b.score, reverse=True)
            sent = 0
            for bundle in bundles:
                if sent >= self.cfg.max_orders_per_tick:
                    break
                compressed = self._compress_bundle_for_time(exchange, bundle)
                if compressed is None:
                    continue
                bundle = compressed
                if not bundle.legs:
                    continue
                if self._cooldown_blocks(bundle):
                    continue
                ok, reason = self.risk.approve_bundle(bundle)
                if not ok:
                    jlog("risk_reject", exchange=exchange, strategy=bundle.strategy, reason=reason, score=bundle.score)
                    continue
                leg_count = await self._execute_bundle(bundle)
                sent += leg_count

    def _flatten_bundles(self, exchange: str) -> List[TradeBundle]:
        if not self.cfg.enable_flatten:
            return []
        time_left = self.state.exchange_time_left_ms(exchange, self.cfg)
        if time_left > self.cfg.flatten_start_ms:
            return []
        account = self.risk.accounts[exchange]
        out: List[TradeBundle] = []
        urgency = max(1, (self.cfg.flatten_start_ms - time_left) // 5_000 + 1)
        for ticker, pos in list(account.positions.items()):
            if pos == 0:
                continue
            inst = self.state.state(exchange, ticker)
            if inst is None or inst.book.is_crossed_or_empty:
                continue
            if pos > 0:
                side = "ask"
                price = inst.book.best_bid
                raw_qty = min(pos, self.cfg.flatten_order_qty * urgency)
            else:
                side = "bid"
                price = inst.book.best_ask
                raw_qty = min(-pos, self.cfg.flatten_order_qty * urgency)
            if price is None or raw_qty <= 0:
                continue
            qty = min(raw_qty, max(1, inst.book.top_depth_qty_for_taker(side)))
            leg = OrderIntent(exchange, ticker, side, price, qty, "flatten", 0, 10_000, f"segment_flatten time_left={time_left}", flatten=True)
            out.append(TradeBundle("flatten", (leg,), 1_000_000 + abs(pos), leg.reason))
        return out

    def _compress_bundle_for_time(self, exchange: str, bundle: TradeBundle) -> Optional[TradeBundle]:
        if any(leg.flatten for leg in bundle.legs):
            return bundle
        time_left = self.state.exchange_time_left_ms(exchange, self.cfg)
        if time_left >= self.cfg.risk_compress_start_ms:
            return bundle
        if time_left <= self.cfg.flatten_start_ms:
            return None

        bps = max(500, (time_left * 10_000) // max(1, self.cfg.risk_compress_start_ms))
        legs: List[OrderIntent] = []
        for leg in bundle.legs:
            account = self.risk.accounts[leg.exchange]
            pos = account.positions[leg.ticker] + account.pending_pos_delta[leg.ticker]
            direction = 1 if leg.side == "bid" else -1
            reduces_inventory = abs(pos + direction * leg.qty) < abs(pos)
            qty = leg.qty if reduces_inventory else max(1, (leg.qty * bps) // 10_000)
            legs.append(replace(leg, qty=qty, reason=f"{leg.reason} risk_compress_bps={bps}"))
        score = max(1, (bundle.score * bps) // 10_000)
        return TradeBundle(bundle.strategy, tuple(legs), score, f"{bundle.reason} risk_compress_bps={bps}")

    def _cooldown_blocks(self, bundle: TradeBundle) -> bool:
        current = monotonic_ms()
        if any(leg.flatten for leg in bundle.legs):
            return False
        for leg in bundle.legs:
            key = (leg.exchange, leg.ticker, leg.side, leg.strategy)
            last = self.last_order_mono.get(key, 0)
            if current - last < self.cfg.order_cooldown_ms:
                return True
        return False

    async def _execute_bundle(self, bundle: TradeBundle) -> int:
        if not self.cfg.live:
            if self.cfg.dry_run_log_orders:
                for leg in bundle.legs:
                    jlog(
                        "dry_run_order",
                        exchange=leg.exchange,
                        instrument=leg.instrument_id,
                        side=leg.side,
                        price=leg.price,
                        qty=leg.qty,
                        strategy=leg.strategy,
                        edge_cents=leg.edge_cents,
                        reason=leg.reason,
                        bundle_score=bundle.score,
                    )
                    self._mark_cooldown(leg)
            return len(bundle.legs)

        sent = 0
        for leg in bundle.legs:
            client = self.clients.get(leg.exchange)
            if client is None:
                jlog("missing_client", exchange=leg.exchange, instrument=leg.instrument_id)
                continue
            await client.place_order(leg)
            self._mark_cooldown(leg)
            sent += 1
        return sent

    def _mark_cooldown(self, leg: OrderIntent) -> None:
        self.last_order_mono[(leg.exchange, leg.ticker, leg.side, leg.strategy)] = monotonic_ms()


class ExchangeClient:
    def __init__(self, exchange: str, cfg: Config, state: MarketState, risk: RiskManager, engine: StrategyEngine) -> None:
        self.exchange = exchange
        self.host = EXCHANGE_HOSTS[exchange]
        self.url = f"ws://{self.host}:9001/trade"
        self.cfg = cfg
        self.state = state
        self.risk = risk
        self.engine = engine
        self.limiter = TokenBucket(cfg.rate_limit_per_sec)
        self.ws: Any = None
        self.request_seq = 0
        self.stop_event = asyncio.Event()
        self.connected = False

    def stop(self) -> None:
        self.stop_event.set()

    def next_request_id(self, prefix: str) -> str:
        self.request_seq += 1
        return f"{self.exchange}-{prefix}-{monotonic_ms()}-{self.request_seq}"

    async def run(self) -> None:
        backoff = 0.5
        while not self.stop_event.is_set():
            try:
                await self._connect_once()
                backoff = 0.5
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                jlog("connection_error", exchange=self.exchange, error=repr(exc), backoff=round(backoff, 2))
            await asyncio.sleep(backoff + random.random() * 0.25)
            backoff = min(8.0, backoff * 1.6)

    async def _connect_once(self) -> None:
        try:
            from websockets.asyncio.client import connect as ws_connect
        except Exception:
            from websockets import connect as ws_connect  # type: ignore

        jlog("connect_attempt", exchange=self.exchange, url=self.url)
        async with ws_connect(
            self.url,
            max_size=16 * 1024 * 1024,
            compression=None,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=2,
        ) as ws:
            self.ws = ws
            self.connected = True
            jlog("connected", exchange=self.exchange)
            poller = asyncio.create_task(self._inventory_poller())
            try:
                await self.request_inventory()
                await self.request_market_data()
                async for raw in ws:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", "replace")
                    if raw == "Message rate limit exceeded":
                        jlog("rate_limit_disconnect", exchange=self.exchange)
                        break
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        jlog("bad_json", exchange=self.exchange, raw=str(raw)[:200])
                        continue
                    should_break = await self.handle_message(msg)
                    if should_break:
                        break
            finally:
                poller.cancel()
                self.connected = False
                self.ws = None
                try:
                    await poller
                except asyncio.CancelledError:
                    pass
                jlog("disconnected", exchange=self.exchange)

    async def _inventory_poller(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.inventory_poll_sec)
            if self.connected:
                await self.request_inventory()

    async def handle_message(self, msg: Mapping[str, Any]) -> bool:
        msg_type = msg.get("type")
        if msg_type == "welcome":
            jlog("welcome", exchange=self.exchange, message=msg.get("message"))
        elif msg_type == "market_data_update":
            await self.engine.on_market_data(self.exchange, msg)
        elif msg_type == "add_order_response":
            await self._handle_add_order_response(msg)
        elif msg_type == "cancel_order_response":
            jlog("cancel_response", exchange=self.exchange, success=msg.get("success"), message=msg.get("message"))
        elif msg_type == "get_inventory_response":
            data = msg.get("data") or {}
            if isinstance(data, Mapping):
                self.risk.apply_inventory(self.exchange, data)
        elif msg_type == "get_pending_orders_response":
            jlog("pending_orders", exchange=self.exchange, count=sum(len(side) for pair in (msg.get("data") or {}).values() for side in pair))
        elif msg_type == "error":
            jlog("server_error", exchange=self.exchange, request_id=msg.get("user_request_id"), message=msg.get("message"))
        elif msg_type == "end_of_round":
            jlog("end_of_round", exchange=self.exchange)
            self.state.reset_exchange(self.exchange)
            self.risk.reset_exchange(self.exchange)
            return True
        else:
            jlog("unknown_message", exchange=self.exchange, msg_type=msg_type)
        return False

    async def _handle_add_order_response(self, msg: Mapping[str, Any]) -> None:
        request_id = str(msg.get("user_request_id") or "")
        success = bool(msg.get("success"))
        data = msg.get("data") or {}
        if isinstance(data, Mapping):
            self.risk.apply_order_response(self.exchange, request_id, success, data)
        jlog(
            "order_response",
            exchange=self.exchange,
            request_id=request_id,
            success=success,
            order_id=data.get("order_id") if isinstance(data, Mapping) else None,
            message=data.get("message") if isinstance(data, Mapping) else None,
            immediate_inventory_change=data.get("immediate_inventory_change") if isinstance(data, Mapping) else None,
            immediate_balance_change=data.get("immediate_balance_change") if isinstance(data, Mapping) else None,
        )

    async def send_json(self, payload: Mapping[str, Any]) -> None:
        if self.ws is None:
            raise RuntimeError(f"{self.exchange} websocket is not connected")
        await self.limiter.wait()
        await self.ws.send(json.dumps(payload, separators=(",", ":")))

    async def request_inventory(self) -> None:
        request_id = self.next_request_id("inv")
        await self.send_json({"type": "get_inventory", "user_request_id": request_id})

    async def request_market_data(self) -> None:
        request_id = self.next_request_id("md")
        await self.send_json({"type": "get_market_data", "user_request_id": request_id})

    async def place_order(self, leg: OrderIntent) -> None:
        request_id = self.next_request_id("ord")
        payload = {
            "type": "add_order",
            "user_request_id": request_id,
            "instrument_id": leg.instrument_id,
            "side": leg.side,
            "quantity": int(leg.qty),
            "order_type": leg.order_type,
        }
        if leg.order_type in {"limit", "ioc"}:
            payload["price"] = int(leg.price)
            payload["expiry"] = now_ms() + self.cfg.ioc_ttl_ms

        self.risk.reserve(request_id, leg)
        try:
            await self.send_json(payload)
        except Exception:
            self.risk.release(request_id)
            raise
        jlog(
            "order_submit",
            exchange=leg.exchange,
            request_id=request_id,
            instrument=leg.instrument_id,
            side=leg.side,
            price=leg.price,
            qty=leg.qty,
            order_type=leg.order_type,
            strategy=leg.strategy,
            edge_cents=leg.edge_cents,
            reason=leg.reason,
        )


async def latency_probe_loop(cfg: Config, stop_event: asyncio.Event) -> None:
    if not cfg.latency_probe or cfg.bot_location != "AUTO":
        return
    while not stop_event.is_set():
        best = await probe_location_once(cfg)
        if best and best != cfg.active_location:
            old = cfg.active_location
            cfg.active_location = best
            jlog("location_update", old_location=old, active_location=best)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass


async def probe_location_once(cfg: Config) -> Optional[str]:
    try:
        import aiohttp
    except Exception as exc:
        jlog("latency_probe_disabled", error=repr(exc))
        return None

    candidates = ("NYSE", "ZSE", "HKEX")
    timeout = aiohttp.ClientTimeout(total=0.8)
    results: Dict[str, float] = {}
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def one(exchange: str) -> None:
            url = f"http://{EXCHANGE_HOSTS[exchange]}:9001/health"
            start = time.perf_counter()
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        await resp.read()
                        results[exchange] = (time.perf_counter() - start) * 1_000
            except Exception:
                return

        await asyncio.gather(*(one(ex) for ex in candidates))
    if not results:
        return None
    best = min(results, key=results.get)
    jlog("latency_probe", results={k: round(v, 2) for k, v in results.items()}, chosen=best)
    return best


def build_synthetic_depth(mid: int, spread: int = 2, qty: int = 100) -> Dict[str, Dict[str, int]]:
    bid = mid - spread // 2
    ask = mid + (spread - spread // 2)
    return {
        "bids": {str(bid): qty, str(bid - 1): qty, str(bid - 2): qty},
        "asks": {str(ask): qty, str(ask + 1): qty, str(ask + 2): qty},
    }


def inject_book(state: MarketState, exchange: str, ticker: str, mid: int, spread: int = 2, qty: int = 100, server_time: int = 1_000) -> None:
    instrument = f"{exchange}-{ticker}"
    state.update_from_market_data(
        exchange,
        {
            "type": "market_data_update",
            "time": server_time,
            "orderbook_depths": {instrument: build_synthetic_depth(mid, spread=spread, qty=qty)},
            "candles": {"tradeable": {}},
            "events": [],
        },
    )


def run_self_test() -> None:
    cfg = Config.from_env()
    cfg.venues = ("NYSE", "ZSE", "HKEX")
    cfg.live = False
    cfg.active_location = "ZSE"
    cfg.min_edge_cents = 2
    cfg.etf_extra_edge_cents = 0
    cfg.cross_extra_edge_cents = 0
    cfg.card_simp_edge_cents = 3
    cfg.max_signal_age_ms = 10_000
    cfg.rate_limit_per_sec = 100

    state = MarketState()
    risk = RiskManager(cfg, state)

    book = Book.from_depth({"bids": {"10000": 50, "9999": 50}, "asks": {"10004": 25, "10005": 50}})
    assert book.best_bid == 10_000
    assert book.best_ask == 10_004
    assert book.mid == 10_002
    assert book.microprice is not None
    assert book.executable_qty("bid", 10_004, 10) == 10

    for ticker in ETF_BASKETS["ETFA3"]:
        inject_book(state, "ZSE", ticker, 10_000)
    inject_book(state, "ZSE", "ETFA3", 9_970)
    etf_bundles = ETFArbStrategy().generate("ZSE", state, risk, cfg)
    assert any(bundle.legs[0].side == "bid" and bundle.legs[0].ticker == "ETFA3" for bundle in etf_bundles), etf_bundles

    inject_book(state, "NYSE", "CARD", 10_050)
    inject_book(state, "ZSE", "CARD", 10_000)
    cv_bundles = CrossVenueStrategy().generate("ZSE", state, risk, cfg)
    assert any(bundle.legs[0].ticker == "CARD" and bundle.legs[0].side == "bid" for bundle in cv_bundles), cv_bundles

    inject_book(state, "NYSE", "CARD", 10_100)
    inject_book(state, "NYSE", "SIMP", 10_000)
    inject_book(state, "ZSE", "CARD", 9_990)
    inject_book(state, "ZSE", "SIMP", 10_000)
    pair_bundles = CardSimpStrategy().generate("ZSE", state, risk, cfg)
    assert any(len(bundle.legs) == 2 for bundle in pair_bundles), pair_bundles

    leg = OrderIntent("ZSE", "CARD", "bid", 10_000, 5, "test", 10, 10_000, "unit")
    ok, reason = risk.approve_bundle(TradeBundle("test", (leg,), 50, "unit"))
    assert ok, reason

    too_big = OrderIntent("ZSE", "CARD", "bid", 10_000, HARD_LONG_LIMIT + 1, "test", 10, 10_000, "unit")
    ok, _ = risk.approve_bundle(TradeBundle("test", (too_big,), 50, "unit"))
    assert not ok

    print("self-test ok")


async def async_main() -> None:
    cfg = Config.from_env()
    if not cfg.venues:
        raise SystemExit("No valid venues selected. Set VENUES=NYSE,ZSE,...")

    state = MarketState()
    risk = RiskManager(cfg, state)
    engine = StrategyEngine(cfg, state, risk)
    stop_event = asyncio.Event()

    def stop() -> None:
        stop_event.set()
        for client in engine.clients.values():
            client.stop()

    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signame):
            try:
                loop.add_signal_handler(getattr(signal, signame), stop)
            except NotImplementedError:
                pass

    jlog(
        "startup",
        live=cfg.live,
        venues=cfg.venues,
        bot_location=cfg.bot_location,
        active_location=cfg.active_location,
        strategies=[s.name for s in engine.strategies],
        base_order_qty=cfg.base_order_qty,
        max_order_qty=cfg.max_order_qty,
        max_position_per_instrument=cfg.max_position_per_instrument,
        max_short_per_instrument=cfg.max_short_per_instrument,
        rate_limit_per_sec=cfg.rate_limit_per_sec,
    )
    if not cfg.live:
        jlog("dry_run_mode", message="Set LIVE_TRADING=1 to send orders")

    clients = [ExchangeClient(exchange, cfg, state, risk, engine) for exchange in cfg.venues]
    for client in clients:
        engine.register_client(client)

    tasks = [asyncio.create_task(client.run(), name=f"client-{client.exchange}") for client in clients]
    tasks.append(asyncio.create_task(latency_probe_loop(cfg, stop_event), name="latency-probe"))

    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    jlog("shutdown")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AlgoTrade 2026 multi-exchange trading bot")
    parser.add_argument("--self-test", action="store_true", help="run local deterministic checks and exit")
    parser.add_argument("--print-config", action="store_true", help="print env-derived configuration and exit")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.self_test:
        run_self_test()
        return
    if args.print_config:
        cfg = Config.from_env()
        print(json.dumps({k: v for k, v in cfg.__dict__.items()}, indent=2, default=str))
        return
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
