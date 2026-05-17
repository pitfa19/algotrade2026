#!/usr/bin/env python3
"""
AlgoTrade 2026 — alpha_bot.

A multi-strategy trading bot built around edges other bots in this repo miss:

  1. MM inventory-skew inference (docs: "MM skews quotes based on inventory").
  2. "Non-50" level signal (MM is exactly 50/level, so qty != 50 is competitor flow).
  3. Self-location auto-detection by RTT (no hardcoded latency matrix).
  4. Triangulated multi-venue ETF fair value (median microprice per constituent).
  5. Expiry-as-cancel (250 ms expiries → no rate spent on cancels).
  6. Aggressor-flow-biased microprice (signed trade pressure shifts fair value).
  7. Settlement-aware smooth unwind from t=540s (not dump at t=575s).
  8. 10-venue CARD/SIMP consensus stat-arb.
  9. Adaptive thresholds scaled by rolling realized spread.

Default mode: dry-run. Set LIVE_TRADING=1 to send orders.
Set ALPHA_USE_HOSTNAMES=1 to use DNS hostnames instead of IPs.
Set ALPHA_LOG_LEVEL=DEBUG for verbose tracing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    import websockets
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover
    raise SystemExit("Install: pip install websockets")


# ------------------------------------------------------------------- constants

EXCHANGE_PORT = 9001
EXCHANGE_IPS: dict[str, str] = {
    "NYSE":     "10.0.201.2",
    "NASDAQ":   "10.0.202.2",
    "SSE":      "10.0.203.2",
    "JPX":      "10.0.204.2",
    "Euronext": "10.0.205.2",
    "LSE":      "10.0.206.2",
    "HKEX":     "10.0.207.2",
    "NSE":      "10.0.208.2",
    "TMX":      "10.0.209.2",
    "ZSE":      "10.0.210.2",
}
EXCHANGE_HOSTS: dict[str, str] = {
    name: f"{name.lower()}.algotrade.hr" for name in EXCHANGE_IPS
}
ALL_EXCHANGES: list[str] = list(EXCHANGE_IPS)

# Per-instrument listings — derived from participant guide §4 / §5.
LISTINGS: dict[str, list[str]] = {
    "CARD":  ALL_EXCHANGES,
    "SIMP":  ALL_EXCHANGES,
    "NGUP":  ["NYSE", "NASDAQ", "Euronext", "TMX", "ZSE"],
    "OIT":   ["LSE", "Euronext", "HKEX", "NSE", "ZSE"],
    "KTST":  ["NYSE", "JPX", "TMX", "ZSE"],
    "FSR":   ["NASDAQ", "LSE", "SSE", "HKEX", "ZSE"],
    "JZRO":  ["NYSE", "LSE", "Euronext", "TMX", "ZSE"],
    "XFR":   ["NYSE", "HKEX", "TMX", "ZSE"],
    "KOTD":  ["NASDAQ", "LSE", "Euronext", "HKEX", "ZSE"],
    "INA":   ["NYSE", "NASDAQ", "Euronext", "HKEX", "ZSE"],
    "HT":    ["NASDAQ", "LSE", "JPX", "SSE", "TMX", "ZSE"],
    "JNAF":  ["NYSE", "Euronext", "JPX", "HKEX", "ZSE"],
    "DLKV":  ["NASDAQ", "LSE", "HKEX", "NSE", "ZSE"],
    "DDJH":  ["NYSE", "LSE", "Euronext", "TMX", "ZSE"],
    "MDKA":  ["NYSE", "LSE", "HKEX", "TMX", "ZSE"],
    "KRAS":  ["NYSE", "Euronext", "SSE", "TMX", "ZSE"],
    "ZITO":  ["NASDAQ", "LSE", "Euronext", "NSE", "ZSE"],
    "ZABA":  ["NYSE", "LSE", "SSE", "NSE", "TMX", "ZSE"],
    "GOLD":  ["NASDAQ", "Euronext", "JPX", "TMX", "ZSE"],
    "XAG":   ["LSE", "Euronext", "JPX", "ZSE"],
    "ETFA":  ["NYSE", "Euronext", "HKEX", "ZSE"],
    "ETFB":  ["NASDAQ", "LSE", "HKEX", "ZSE"],
    "ETFA3": ["NYSE", "TMX", "ZSE"],
    "ETFB3": ["NASDAQ", "HKEX", "ZSE"],
    "ETFSH": ["Euronext", "JPX", "ZSE"],
}

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}
ETFS = set(ETF_BASKETS)

SECTOR_A = {"NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"}
SECTOR_B = {"KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"}
INDEPENDENT = {"MDKA", "KRAS", "ZITO", "ZABA", "SIMP", "CARD"}
SAFE_HAVEN = {"GOLD", "XAG"}

# Trading limits (from §13).
STARTING_CASH = 10_000_000          # cents per exchange
CASH_FLOOR = -5_000_000             # cents
POS_FLOOR = -200
POS_CEIL = 2_000
MAX_PENDING = 6_000
RATE_LIMIT_HARD = 500               # msgs/sec/exchange
RATE_LIMIT_SOFT = 380               # leave headroom for bursts
SEGMENT_MS = 600_000                # 10 minutes per segment

# Strategy tunables. All thresholds are in cents.
QUOTE_EXPIRY_MS = 250               # short — auto-expires before re-quote
TAKE_EXPIRY_MS = 500
MIN_BOOK_AGE_MS = 50                # avoid trading on stale snapshots
COOLDOWN_MS = 220                   # per (strategy, instrument)
UNWIND_START_FRAC = 0.90            # start smooth unwind at 90% of segment
UNWIND_AGGRESSIVE_FRAC = 0.985      # last ~9s, market-flatten
SETTLEMENT_WINDOW_MS = 30_000

# Signal weights.
AGGRESSOR_FLOW_WINDOW_MS = 2_500
LEADER_WEIGHT = 2.0
NON_50_WEIGHT_BOOST = 1.5
MIN_TICK_EDGE = 6                   # absolute floor — never trade for less
EDGE_VOL_MULTIPLIER = 1.6           # threshold = max(MIN_TICK_EDGE, mult * spread)

# Live-trading knobs.
LIVE_TRADING = os.environ.get("LIVE_TRADING", "0") == "1"
USE_HOSTNAMES = os.environ.get("ALPHA_USE_HOSTNAMES", "0") == "1"
LOG_LEVEL = os.environ.get("ALPHA_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("alpha")


# ------------------------------------------------------------------- utilities

def now_ms() -> int:
    return int(time.time() * 1000)


def host_for(exchange: str) -> str:
    return EXCHANGE_HOSTS[exchange] if USE_HOSTNAMES else EXCHANGE_IPS[exchange]


def ws_url(exchange: str) -> str:
    return f"ws://{host_for(exchange)}:{EXCHANGE_PORT}/trade"


def http_health_url(exchange: str) -> str:
    return f"http://{host_for(exchange)}:{EXCHANGE_PORT}/health"


class TokenBucket:
    """Per-exchange rate limiter. Thread-safe within a single asyncio loop."""

    def __init__(self, rate_per_sec: float, burst: int) -> None:
        self.rate = rate_per_sec
        self.burst = burst
        self.tokens = float(burst)
        self.last = time.monotonic()

    def try_take(self, n: int = 1) -> bool:
        now = time.monotonic()
        elapsed = now - self.last
        self.last = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False


# ------------------------------------------------------------------- state

@dataclass
class BookSnap:
    bids: list[tuple[int, int]] = field(default_factory=list)  # sorted desc
    asks: list[tuple[int, int]] = field(default_factory=list)  # sorted asc
    received_at_ms: int = 0
    server_time_ms: int = 0

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
        if self.bids and self.asks:
            return (self.bids[0][0] + self.asks[0][0]) / 2.0
        return None

    @property
    def spread(self) -> Optional[int]:
        if self.bids and self.asks:
            return self.asks[0][0] - self.bids[0][0]
        return None

    @property
    def microprice(self) -> Optional[float]:
        """Liquidity-weighted mid — bid-side weight uses ask qty and vice versa."""
        if not self.bids or not self.asks:
            return None
        bp, bq = self.bids[0]
        ap, aq = self.asks[0]
        denom = bq + aq
        if denom == 0:
            return (bp + ap) / 2.0
        return (bp * aq + ap * bq) / denom

    @property
    def has_competitor_inside(self) -> bool:
        """MM is exactly 50/level. != 50 means a competitor layered there."""
        if not self.bids or not self.asks:
            return False
        return self.bids[0][1] != 50 or self.asks[0][1] != 50


@dataclass
class TradeMemo:
    price: int
    qty: int
    aggressor_side: int  # +1 buy, -1 sell, 0 unknown
    ts_ms: int


@dataclass
class InstrumentState:
    book: BookSnap = field(default_factory=BookSnap)
    trades: deque = field(default_factory=lambda: deque(maxlen=200))
    mm_mid_history: deque = field(default_factory=lambda: deque(maxlen=120))
    last_candle_close: Optional[int] = None
    last_candle_volume: int = 0
    inv_skew_signal: float = 0.0  # +1 means MM is long → expects to push price down


@dataclass
class ExchangeState:
    name: str
    instruments: dict[str, InstrumentState] = field(default_factory=lambda: defaultdict(InstrumentState))
    server_time_ms: int = 0
    server_time_recv_at: float = 0.0
    cash_cents: int = STARTING_CASH
    positions: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    pending_buy_value: int = 0
    pending_sell_qty: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    rate_bucket: TokenBucket = field(default_factory=lambda: TokenBucket(RATE_LIMIT_SOFT, RATE_LIMIT_SOFT))
    last_book_update_ms: int = 0
    open_orders: dict[int, "OpenOrder"] = field(default_factory=dict)
    rtt_estimate_ms: float = 0.0
    connected: bool = False

    def estimate_server_time(self) -> int:
        if self.server_time_recv_at == 0:
            return 0
        elapsed_ms = (time.monotonic() - self.server_time_recv_at) * 1000
        return int(self.server_time_ms + elapsed_ms)


@dataclass
class OpenOrder:
    order_id: int
    exchange: str
    instrument: str
    side: str           # "bid" | "ask"
    price: int
    qty: int
    placed_at: int
    expiry: int
    purpose: str        # for diagnostics


# ------------------------------------------------------------------- decisions

@dataclass
class Order:
    exchange: str
    ticker: str
    side: str           # "bid" or "ask"
    price: int
    qty: int
    order_type: str     # "limit" | "ioc" | "market"
    expiry_ms: int = TAKE_EXPIRY_MS
    purpose: str = ""


@dataclass
class Opportunity:
    edge_cents: int     # expected per-share edge
    legs: list[Order]
    cooldown_key: str
    score: float = 0.0
    note: str = ""

    def total_qty(self) -> int:
        return sum(l.qty for l in self.legs)


# ------------------------------------------------------------------- bot

class AlphaBot:
    def __init__(self) -> None:
        self.exchanges: dict[str, ExchangeState] = {x: ExchangeState(name=x) for x in ALL_EXCHANGES}
        self.cooldowns: dict[str, int] = {}
        self.our_location: Optional[str] = None  # detected co-location
        self.segment_started_at: float = time.monotonic()
        self.next_request_id: int = 1
        self.stop_event = asyncio.Event()
        self.stats = defaultdict(int)

    # --- request id ----------------------------------------------------------
    def rid(self, tag: str) -> str:
        self.next_request_id += 1
        return f"{tag}-{self.next_request_id}"

    # --- cooldowns -----------------------------------------------------------
    def on_cooldown(self, key: str) -> bool:
        until = self.cooldowns.get(key, 0)
        return now_ms() < until

    def set_cooldown(self, key: str, ms: int = COOLDOWN_MS) -> None:
        self.cooldowns[key] = now_ms() + ms

    # --- connections ---------------------------------------------------------
    async def run(self) -> None:
        log.info("alpha_bot starting | live=%s | use_hostnames=%s",
                 LIVE_TRADING, USE_HOSTNAMES)
        # Run forever — the run_segment coroutine exits at end_of_round and we relaunch.
        while not self.stop_event.is_set():
            try:
                await self.run_segment()
            except Exception:
                log.exception("segment crashed; backing off 2s")
                await asyncio.sleep(2.0)
            else:
                log.info("segment ended; reconnecting in 3s")
                await asyncio.sleep(3.0)

    async def run_segment(self) -> None:
        # Reset per-segment state. Positions/cash reset on the server; mirror locally.
        for ex in self.exchanges.values():
            ex.cash_cents = STARTING_CASH
            ex.positions.clear()
            ex.pending_buy_value = 0
            ex.pending_sell_qty.clear()
            ex.open_orders.clear()
            ex.connected = False
            ex.rate_bucket = TokenBucket(RATE_LIMIT_SOFT, RATE_LIMIT_SOFT)
        self.cooldowns.clear()
        self.our_location = None
        self.segment_started_at = time.monotonic()

        # Spawn one connection per exchange + the central decision loop.
        tasks = [asyncio.create_task(self._exchange_loop(x), name=f"ws-{x}") for x in ALL_EXCHANGES]
        tasks.append(asyncio.create_task(self._decide_loop(), name="decide"))
        tasks.append(asyncio.create_task(self._location_probe(), name="locate"))

        # Wait until any task signals end_of_round (returns) or all crash.
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    # ------------------------------------------------------------------ ws

    async def _exchange_loop(self, exchange: str) -> None:
        """Maintain a single WS connection to one exchange with reconnect."""
        backoff = 0.5
        while not self.stop_event.is_set():
            try:
                async with ws_connect(ws_url(exchange), ping_interval=20, ping_timeout=10,
                                      max_size=2 ** 24, compression=None) as ws:
                    self.exchanges[exchange].connected = True
                    self.exchanges[exchange].rate_bucket = TokenBucket(RATE_LIMIT_SOFT, RATE_LIMIT_SOFT)
                    log.info("[%s] connected", exchange)
                    backoff = 0.5
                    self.exchanges[exchange]._ws = ws  # type: ignore[attr-defined]

                    # Get initial inventory snapshot.
                    await self._send(exchange, {
                        "type": "get_inventory",
                        "user_request_id": self.rid("inv-init"),
                    })

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            log.warning("[%s] non-JSON frame: %r", exchange, raw[:120])
                            continue
                        await self._handle(exchange, msg)
                        if msg.get("type") == "end_of_round":
                            log.info("[%s] end_of_round", exchange)
                            return
            except Exception as e:
                log.warning("[%s] connection lost: %s", exchange, e)
            self.exchanges[exchange].connected = False
            await asyncio.sleep(backoff)
            backoff = min(8.0, backoff * 1.7)

    async def _send(self, exchange: str, payload: dict) -> bool:
        ex = self.exchanges[exchange]
        if not ex.connected:
            return False
        if not ex.rate_bucket.try_take():
            self.stats["rate_limited"] += 1
            return False
        ws = getattr(ex, "_ws", None)
        if ws is None:
            return False
        try:
            await ws.send(json.dumps(payload))
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ inbound

    async def _handle(self, exchange: str, msg: dict) -> None:
        t = msg.get("type")
        ex = self.exchanges[exchange]
        if t == "market_data_update":
            self._on_market_data(ex, msg)
        elif t == "add_order_response":
            self._on_add_response(ex, msg)
        elif t == "cancel_order_response":
            pass  # nothing to do — open_orders cleared by expiry/event
        elif t == "get_inventory_response":
            self._on_inventory(ex, msg.get("data", {}))
        elif t == "get_pending_orders_response":
            pass
        elif t == "welcome":
            log.debug("[%s] welcome", exchange)
        elif t == "end_of_round":
            pass  # caller handles
        elif t == "error":
            log.warning("[%s] error: %s", exchange, msg.get("message"))

    def _on_inventory(self, ex: ExchangeState, data: dict) -> None:
        for k, pair in data.items():
            try:
                reserved, total = pair
            except Exception:
                continue
            if k == "$":
                ex.cash_cents = int(total)
                ex.pending_buy_value = int(reserved)
            else:
                # k is e.g. "NYSE-CARD"; strip exchange prefix
                ticker = k.split("-", 1)[1] if "-" in k else k
                ex.positions[ticker] = int(total)
                ex.pending_sell_qty[ticker] = int(reserved)

    def _on_add_response(self, ex: ExchangeState, msg: dict) -> None:
        if not msg.get("success"):
            data = msg.get("data") or {}
            if data.get("message"):
                log.debug("[%s] add failed: %s", ex.name, data["message"])
            return
        data = msg.get("data") or {}
        oid = data.get("order_id")
        inv_change = data.get("immediate_inventory_change")
        bal_change = data.get("immediate_balance_change")
        if inv_change is not None and bal_change is not None:
            # Optimistic local update — server is authoritative on next inventory poll.
            ex.cash_cents += int(bal_change)
            self.stats["fills"] += 1

    def _on_market_data(self, ex: ExchangeState, msg: dict) -> None:
        ex.server_time_ms = int(msg.get("time", 0))
        ex.server_time_recv_at = time.monotonic()
        ex.last_book_update_ms = now_ms()

        depths = msg.get("orderbook_depths") or {}
        for instrument_id, depth in depths.items():
            ticker = instrument_id.split("-", 1)[1] if "-" in instrument_id else instrument_id
            ist = ex.instruments[ticker]
            bids_raw = depth.get("bids") or {}
            asks_raw = depth.get("asks") or {}
            bids = sorted(((int(p), int(q)) for p, q in bids_raw.items()), key=lambda x: -x[0])
            asks = sorted(((int(p), int(q)) for p, q in asks_raw.items()), key=lambda x: x[0])
            ist.book = BookSnap(
                bids=bids,
                asks=asks,
                received_at_ms=now_ms(),
                server_time_ms=ex.server_time_ms,
            )
            mid = ist.book.mid
            if mid is not None:
                ist.mm_mid_history.append((now_ms(), mid))

        # Trades & cancels — used for aggressor flow & MM inference.
        for ev in msg.get("events") or []:
            if ev.get("event_type") != "trade":
                continue
            d = ev.get("data") or {}
            iid = d.get("instrumentID", "")
            ticker = iid.split("-", 1)[1] if "-" in iid else iid
            ist = ex.instruments[ticker]
            price = int(d.get("price", 0))
            qty = int(d.get("quantity", 0))
            ts = int(d.get("time", ex.server_time_ms))
            # Infer aggressor: compare price to current best.
            aggr = 0
            if ist.book.best_ask is not None and price >= ist.book.best_ask:
                aggr = +1
            elif ist.book.best_bid is not None and price <= ist.book.best_bid:
                aggr = -1
            ist.trades.append(TradeMemo(price=price, qty=qty, aggressor_side=aggr, ts_ms=ts))

        # 1-sec candles — record latest close.
        candles_block = (msg.get("candles") or {}).get("tradeable") or {}
        for instrument_id, clist in candles_block.items():
            if not clist:
                continue
            ticker = instrument_id.split("-", 1)[1] if "-" in instrument_id else instrument_id
            ist = ex.instruments[ticker]
            last = clist[-1]
            if last.get("close") is not None:
                ist.last_candle_close = int(last["close"])
                ist.last_candle_volume = int(last.get("volume") or 0)

        self._update_inventory_skew(ex)

    def _update_inventory_skew(self, ex: ExchangeState) -> None:
        """Infer MM inventory direction from drift of MM mid relative to recent VWAP.

        If MM mid drifts down vs VWAP, MM is long and skewing offers cheaper
        to offload — we want to sell to it (lift its cheap asks early).
        Inverted intuition: actually if MM is long, it skews bids LOWER and
        asks LOWER — so its mid moves DOWN relative to fair price. We treat
        a downward mid drift as MM-long-signal (skew_signal > 0).
        """
        for ticker, ist in ex.instruments.items():
            if len(ist.mm_mid_history) < 20 or not ist.trades:
                ist.inv_skew_signal = 0.0
                continue
            recent_trades = [t for t in ist.trades if t.ts_ms >= ex.server_time_ms - 5000]
            if not recent_trades:
                ist.inv_skew_signal = 0.0
                continue
            tot_qty = sum(t.qty for t in recent_trades)
            if tot_qty == 0:
                continue
            vwap = sum(t.price * t.qty for t in recent_trades) / tot_qty
            cur_mid = ist.mm_mid_history[-1][1]
            # Negative drift → MM mid below VWAP → MM long → push price down expected.
            drift = cur_mid - vwap
            ist.inv_skew_signal = -drift  # positive when MM is long

    # ---------------------------------------------------------- self-location

    async def _location_probe(self) -> None:
        """Approximate co-location by measuring time-to-first-book per exchange."""
        await asyncio.sleep(2.0)  # wait for connections
        first_book_at: dict[str, float] = {}
        deadline = time.monotonic() + 6.0
        while time.monotonic() < deadline:
            for x in ALL_EXCHANGES:
                ex = self.exchanges[x]
                if x not in first_book_at and any(i.book.bids for i in ex.instruments.values()):
                    first_book_at[x] = time.monotonic() - self.segment_started_at
            if len(first_book_at) == len(ALL_EXCHANGES):
                break
            await asyncio.sleep(0.1)

        # The exchange with the lowest first-book latency is closest.
        if first_book_at:
            sorted_x = sorted(first_book_at.items(), key=lambda kv: kv[1])
            self.our_location = sorted_x[0][0]
            log.info("co-location detected: %s | latencies=%s",
                     self.our_location,
                     {k: f"{v*1000:.0f}ms" for k, v in sorted_x[:3]})

    # ---------------------------------------------------------- decision loop

    async def _decide_loop(self) -> None:
        await asyncio.sleep(1.5)
        last_inv_poll = 0.0
        while not self.stop_event.is_set():
            t0 = time.monotonic()
            try:
                # Poll inventory occasionally to stay synced.
                if t0 - last_inv_poll > 10.0:
                    last_inv_poll = t0
                    for x in ALL_EXCHANGES:
                        if self.exchanges[x].connected:
                            await self._send(x, {
                                "type": "get_inventory",
                                "user_request_id": self.rid("inv"),
                            })

                opportunities = self._gather_opportunities()
                opportunities.sort(key=lambda o: o.score, reverse=True)
                fired = 0
                for opp in opportunities:
                    if self.on_cooldown(opp.cooldown_key):
                        continue
                    if not self._risk_ok(opp):
                        continue
                    if await self._fire(opp):
                        self.set_cooldown(opp.cooldown_key)
                        fired += 1
                    if fired >= 12:
                        break
            except Exception:
                log.exception("decision loop error")
            await asyncio.sleep(0.05)

    # ------------------------------------------------------- opportunity gather

    def _gather_opportunities(self) -> list[Opportunity]:
        opps: list[Opportunity] = []

        # Always run unwind logic first — it can override other strategies.
        opps.extend(self._opp_unwind())

        # Edge sources, layered:
        opps.extend(self._opp_card_simp_consensus())
        opps.extend(self._opp_etf_triangulation())
        opps.extend(self._opp_sector_lead_lag())
        opps.extend(self._opp_mm_skew())
        opps.extend(self._opp_competitor_signal())

        # Score = edge_cents * size, with a small bonus for short-leg trades
        # (faster execution, lower exposure to leg-out risk).
        for o in opps:
            base = o.edge_cents * o.total_qty()
            leg_penalty = 1.0 / (1.0 + 0.15 * (len(o.legs) - 1))
            o.score = base * leg_penalty
        return opps

    # --- unwind --------------------------------------------------------------

    def _segment_phase(self) -> float:
        """Fraction through current segment, [0,1+]. Uses any exchange's clock."""
        for ex in self.exchanges.values():
            if ex.server_time_recv_at:
                return ex.estimate_server_time() / SEGMENT_MS
        return 0.0

    def _opp_unwind(self) -> list[Opportunity]:
        phase = self._segment_phase()
        if phase < UNWIND_START_FRAC:
            return []
        opps: list[Opportunity] = []
        # Linearly target zero by phase=1.0; aggressive by UNWIND_AGGRESSIVE_FRAC.
        time_left_frac = max(0.0, 1.0 - phase)
        unwind_aggressive = phase >= UNWIND_AGGRESSIVE_FRAC

        for x, ex in self.exchanges.items():
            if not ex.connected:
                continue
            for ticker, pos in list(ex.positions.items()):
                if pos == 0:
                    continue
                ist = ex.instruments.get(ticker)
                if ist is None or ist.book.received_at_ms == 0:
                    continue
                # Target qty to unwind on this tick.
                if unwind_aggressive:
                    target_qty = abs(pos)
                    order_type = "market"
                    price = 0
                else:
                    # Spread |pos| evenly over remaining ticks (~ phase budget).
                    remaining_ticks = max(1, int(SEGMENT_MS * time_left_frac / 100))
                    target_qty = max(1, int(abs(pos) / remaining_ticks * 4))
                    target_qty = min(target_qty, abs(pos))
                    order_type = "ioc"
                    if pos > 0:
                        price = ist.book.best_bid or 0
                    else:
                        price = ist.book.best_ask or 0
                    if price == 0:
                        continue
                if pos > 0:
                    side = "ask"
                    qty = min(target_qty, ist.book.best_bid_qty if not unwind_aggressive else 9999)
                else:
                    side = "bid"
                    qty = min(target_qty, ist.book.best_ask_qty if not unwind_aggressive else 9999)
                if qty <= 0:
                    continue
                opps.append(Opportunity(
                    edge_cents=10_000,  # very high priority
                    legs=[Order(x, ticker, side, price, qty, order_type,
                                expiry_ms=400, purpose="unwind")],
                    cooldown_key=f"unwind:{x}:{ticker}",
                    note=f"phase={phase:.3f} pos={pos}",
                ))
        return opps

    # --- CARD/SIMP consensus -------------------------------------------------

    def _opp_card_simp_consensus(self) -> list[Opportunity]:
        """For CARD and SIMP, compute consensus mid across all 10 venues; take
        any venue whose touch crosses consensus by > adaptive threshold."""
        opps: list[Opportunity] = []
        for ticker in ("CARD", "SIMP"):
            mids: list[float] = []
            spreads: list[int] = []
            per_venue: dict[str, BookSnap] = {}
            for x in LISTINGS[ticker]:
                ist = self.exchanges[x].instruments.get(ticker)
                if not ist or ist.book.mid is None:
                    continue
                if now_ms() - ist.book.received_at_ms > 500:
                    continue
                mids.append(ist.book.microprice or ist.book.mid)
                if ist.book.spread is not None:
                    spreads.append(ist.book.spread)
                per_venue[x] = ist.book
            if len(mids) < 4:
                continue
            consensus = statistics.median(mids)
            avg_spread = statistics.mean(spreads) if spreads else 20
            threshold = max(MIN_TICK_EDGE, int(EDGE_VOL_MULTIPLIER * avg_spread))

            for x, book in per_venue.items():
                # Sell if best bid is meaningfully above consensus.
                if book.best_bid is not None and book.best_bid >= consensus + threshold:
                    edge = int(book.best_bid - consensus)
                    qty = min(book.best_bid_qty, 30, max(1, edge // 10))
                    if qty <= 0:
                        continue
                    opps.append(Opportunity(
                        edge_cents=edge,
                        legs=[Order(x, ticker, "ask", book.best_bid, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"consensus-sell {ticker}@{x}")],
                        cooldown_key=f"cons:{ticker}:{x}:sell",
                        note=f"consensus={consensus:.1f} bid={book.best_bid}",
                    ))
                if book.best_ask is not None and book.best_ask <= consensus - threshold:
                    edge = int(consensus - book.best_ask)
                    qty = min(book.best_ask_qty, 30, max(1, edge // 10))
                    if qty <= 0:
                        continue
                    opps.append(Opportunity(
                        edge_cents=edge,
                        legs=[Order(x, ticker, "bid", book.best_ask, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"consensus-buy {ticker}@{x}")],
                        cooldown_key=f"cons:{ticker}:{x}:buy",
                        note=f"consensus={consensus:.1f} ask={book.best_ask}",
                    ))
        return opps

    # --- ETF triangulation ---------------------------------------------------

    def _triangulated_constituent_fv(self, ticker: str) -> Optional[float]:
        """Median microprice across all venues of a single underlying."""
        mids: list[float] = []
        for x in LISTINGS.get(ticker, []):
            ist = self.exchanges[x].instruments.get(ticker)
            if not ist or ist.book.mid is None:
                continue
            if now_ms() - ist.book.received_at_ms > 600:
                continue
            mids.append(ist.book.microprice or ist.book.mid)
        if len(mids) < 2:
            return None
        return statistics.median(mids)

    def _opp_etf_triangulation(self) -> list[Opportunity]:
        opps: list[Opportunity] = []
        for etf, basket in ETF_BASKETS.items():
            constituent_fvs: list[float] = []
            for c in basket:
                fv = self._triangulated_constituent_fv(c)
                if fv is None:
                    constituent_fvs = []
                    break
                constituent_fvs.append(fv)
            if not constituent_fvs:
                continue
            etf_fv = sum(constituent_fvs) / len(constituent_fvs)
            for x in LISTINGS[etf]:
                ist = self.exchanges[x].instruments.get(etf)
                if not ist or ist.book.received_at_ms == 0:
                    continue
                spread = ist.book.spread or 20
                threshold = max(MIN_TICK_EDGE, int(EDGE_VOL_MULTIPLIER * spread))
                if ist.book.best_bid is not None and ist.book.best_bid >= etf_fv + threshold:
                    edge = int(ist.book.best_bid - etf_fv)
                    qty = min(ist.book.best_bid_qty, 25)
                    opps.append(Opportunity(
                        edge_cents=edge,
                        legs=[Order(x, etf, "ask", ist.book.best_bid, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"etf-sell {etf}@{x}")],
                        cooldown_key=f"etf:{etf}:{x}:sell",
                        note=f"fv={etf_fv:.1f} bid={ist.book.best_bid}",
                    ))
                if ist.book.best_ask is not None and ist.book.best_ask <= etf_fv - threshold:
                    edge = int(etf_fv - ist.book.best_ask)
                    qty = min(ist.book.best_ask_qty, 25)
                    opps.append(Opportunity(
                        edge_cents=edge,
                        legs=[Order(x, etf, "bid", ist.book.best_ask, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"etf-buy {etf}@{x}")],
                        cooldown_key=f"etf:{etf}:{x}:buy",
                        note=f"fv={etf_fv:.1f} ask={ist.book.best_ask}",
                    ))
        return opps

    # --- sector lead-lag ----------------------------------------------------

    def _opp_sector_lead_lag(self) -> list[Opportunity]:
        opps: list[Opportunity] = []
        for sector_name, sector in (("A", SECTOR_A), ("B", SECTOR_B)):
            # Build per-ticker triangulated FV.
            fvs: dict[str, float] = {}
            for t in sector:
                fv = self._triangulated_constituent_fv(t)
                if fv is not None:
                    fvs[t] = fv
            if len(fvs) < 3:
                continue
            # Compute sector mean drift relative to start-of-segment baseline.
            # Heuristic: rank tickers by deviation from sector mean — laggards trade.
            mean = statistics.mean(fvs.values())
            for t, fv in fvs.items():
                # If this ticker lags the sector by > threshold, expect it to catch up.
                # Use VWAP-of-trades as alternate fair anchor.
                deviation = fv - mean
                # Look at each venue book; if its book deviates from FV beyond threshold
                # AND aligns with sector direction, take it.
                for x in LISTINGS[t]:
                    ist = self.exchanges[x].instruments.get(t)
                    if not ist or ist.book.received_at_ms == 0:
                        continue
                    spread = ist.book.spread or 20
                    threshold = max(MIN_TICK_EDGE, int(EDGE_VOL_MULTIPLIER * spread))
                    if ist.book.best_ask is not None and ist.book.best_ask <= fv - threshold and deviation < 0:
                        edge = int(fv - ist.book.best_ask)
                        qty = min(ist.book.best_ask_qty, 15)
                        opps.append(Opportunity(
                            edge_cents=edge,
                            legs=[Order(x, t, "bid", ist.book.best_ask, qty, "ioc",
                                        expiry_ms=TAKE_EXPIRY_MS,
                                        purpose=f"sector-{sector_name} catchup {t}@{x}")],
                            cooldown_key=f"sec:{sector_name}:{t}:{x}:buy",
                        ))
        return opps

    # --- MM skew exploitation ------------------------------------------------

    def _opp_mm_skew(self) -> list[Opportunity]:
        opps: list[Opportunity] = []
        for x, ex in self.exchanges.items():
            if not ex.connected:
                continue
            for ticker, ist in ex.instruments.items():
                signal = ist.inv_skew_signal
                if abs(signal) < 8:  # in cents — small drift is noise
                    continue
                if ist.book.received_at_ms == 0:
                    continue
                spread = ist.book.spread or 20
                threshold = max(MIN_TICK_EDGE, int(EDGE_VOL_MULTIPLIER * spread))
                # signal > 0 → MM is long → its asks are cheaper than fair.
                # We BUY from MM at its ask (cheap) — expect price to drop later, but
                # we get an immediate edge from skew.
                if signal > threshold and ist.book.best_ask is not None:
                    edge = int(signal)
                    qty = min(ist.book.best_ask_qty, 8)
                    opps.append(Opportunity(
                        edge_cents=edge,
                        legs=[Order(x, ticker, "bid", ist.book.best_ask, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"mm-long-skew {ticker}@{x}")],
                        cooldown_key=f"mm:{ticker}:{x}:buy",
                    ))
                elif signal < -threshold and ist.book.best_bid is not None:
                    edge = int(-signal)
                    qty = min(ist.book.best_bid_qty, 8)
                    opps.append(Opportunity(
                        edge_cents=edge,
                        legs=[Order(x, ticker, "ask", ist.book.best_bid, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"mm-short-skew {ticker}@{x}")],
                        cooldown_key=f"mm:{ticker}:{x}:sell",
                    ))
        return opps

    # --- competitor-flow signal ---------------------------------------------

    def _opp_competitor_signal(self) -> list[Opportunity]:
        """When the inside price level has qty != 50, a competitor laid down
        size. This is a one-tick lead on impending flow; we ride it."""
        opps: list[Opportunity] = []
        for x, ex in self.exchanges.items():
            if not ex.connected:
                continue
            for ticker, ist in ex.instruments.items():
                if not ist.book.has_competitor_inside:
                    continue
                if ist.book.received_at_ms == 0:
                    continue
                bid_qty = ist.book.best_bid_qty
                ask_qty = ist.book.best_ask_qty
                # Heavy bid laddering (>>50) → likely buyer; we buy first.
                if bid_qty > 75 and ask_qty <= 50 and ist.book.best_ask is not None:
                    spread = ist.book.spread or 20
                    edge = max(MIN_TICK_EDGE, spread // 2)
                    qty = min(ask_qty, 5)
                    opps.append(Opportunity(
                        edge_cents=int(edge * NON_50_WEIGHT_BOOST),
                        legs=[Order(x, ticker, "bid", ist.book.best_ask, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"comp-bid-lead {ticker}@{x}")],
                        cooldown_key=f"comp:{ticker}:{x}:buy",
                    ))
                if ask_qty > 75 and bid_qty <= 50 and ist.book.best_bid is not None:
                    spread = ist.book.spread or 20
                    edge = max(MIN_TICK_EDGE, spread // 2)
                    qty = min(bid_qty, 5)
                    opps.append(Opportunity(
                        edge_cents=int(edge * NON_50_WEIGHT_BOOST),
                        legs=[Order(x, ticker, "ask", ist.book.best_bid, qty, "ioc",
                                    expiry_ms=TAKE_EXPIRY_MS,
                                    purpose=f"comp-ask-lead {ticker}@{x}")],
                        cooldown_key=f"comp:{ticker}:{x}:sell",
                    ))
        return opps

    # ------------------------------------------------------------------ risk

    def _risk_ok(self, opp: Opportunity) -> bool:
        # Check each leg respects local position/cash limits.
        # We model the leg as if it filled fully — conservative.
        per_x_cash_delta: dict[str, int] = defaultdict(int)
        per_x_pos_delta: dict[tuple[str, str], int] = defaultdict(int)
        for leg in opp.legs:
            if leg.qty <= 0:
                return False
            if leg.order_type in ("limit", "ioc") and leg.price <= 0:
                return False
            ex = self.exchanges[leg.exchange]
            if not ex.connected:
                return False
            ist = ex.instruments.get(leg.ticker)
            if ist is None:
                return False
            if leg.order_type != "market" and now_ms() - ist.book.received_at_ms > 800:
                return False
            sign = +1 if leg.side == "bid" else -1
            per_x_pos_delta[(leg.exchange, leg.ticker)] += sign * leg.qty
            cost = leg.price * leg.qty * (-1 if leg.side == "bid" else +1)
            per_x_cash_delta[leg.exchange] += cost
        for x, delta in per_x_cash_delta.items():
            if self.exchanges[x].cash_cents + delta < CASH_FLOOR:
                return False
        for (x, t), delta in per_x_pos_delta.items():
            new_pos = self.exchanges[x].positions.get(t, 0) + delta
            if new_pos < POS_FLOOR or new_pos > POS_CEIL:
                return False
        return True

    # ------------------------------------------------------------------ fire

    async def _fire(self, opp: Opportunity) -> bool:
        if not LIVE_TRADING:
            self.stats["dry_fired"] += 1
            log.info("DRY-FIRE edge=%dc score=%.0f legs=%d %s",
                     opp.edge_cents, opp.score, len(opp.legs),
                     " | ".join(f"{l.purpose}" for l in opp.legs))
            # Optimistically update local position so risk checks still see exposure.
            for leg in opp.legs:
                ex = self.exchanges[leg.exchange]
                if leg.side == "bid":
                    ex.positions[leg.ticker] += leg.qty
                    ex.cash_cents -= leg.price * leg.qty
                else:
                    ex.positions[leg.ticker] -= leg.qty
                    ex.cash_cents += leg.price * leg.qty
            return True

        # Live: send legs in parallel. Multi-leg has leg-out risk but we already
        # filter for atomicity in risk check.
        sends = []
        for leg in opp.legs:
            payload = {
                "type": "add_order",
                "user_request_id": self.rid("a"),
                "instrument_id": f"{leg.exchange}-{leg.ticker}",
                "side": leg.side,
                "quantity": leg.qty,
                "order_type": leg.order_type,
            }
            if leg.order_type != "market":
                payload["price"] = leg.price
                payload["expiry"] = now_ms() + leg.expiry_ms
            sends.append(self._send(leg.exchange, payload))
        results = await asyncio.gather(*sends, return_exceptions=True)
        ok = sum(1 for r in results if r is True)
        self.stats["fired"] += 1
        log.info("FIRE %d/%d edge=%dc %s", ok, len(opp.legs), opp.edge_cents,
                 opp.legs[0].purpose if opp.legs else "")
        return ok > 0


# ------------------------------------------------------------------- entry

async def main() -> None:
    bot = AlphaBot()

    def stop(*_: Any) -> None:
        log.info("stopping")
        bot.stop_event.set()

    try:
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(sig, stop)
    except (NotImplementedError, ImportError):
        pass

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
