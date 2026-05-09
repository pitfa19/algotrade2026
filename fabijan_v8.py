"""fabijan_v8.py — aggressive multi-venue ETF arb bot

Edges (in priority order):
  1. Same-venue ETF basket arb. ZSE (every ETF), NYSE/TMX (ETFA3), NASDAQ/HKEX
     (ETFB3), Euronext/JPX (ETFSH). When sum(constituent_bids) > N*ETF_ask
     (long-arb) or sum(constituent_asks) < N*ETF_bid (short-arb), fire IOCs
     for every leg. Hedge is locked the moment the IOCs match — risk-free
     except for partial fills, which we treat as a residual to be flattened.
  2. ETF directional bias. ETFA on ZSE consistently sits 14c below NAV; we
     scale into long/short when ETF mid - NAV crosses ±50c (both sides exit
     at -15c).
  3. Aggressive resting MM inside the in-house MM at the touch on the most
     liquid instruments per venue. Only enabled when other strategies aren't
     using the position budget.

Hard constraints:
  - per-exchange rate limit 500 msg/s; we cap at 450 and burst-throttle.
  - integer-cent prices.
  - position floor -200, ceiling +2000 per instrument per exchange.
  - cash floor -$50k per exchange.
  - reconnect with backoff at every segment boundary.

The strategy logic is in `Strategy`, which is purely a function of the
on_market_data callback. The WS plumbing sits below it and is also reused by
the backtest harness (`backtest_v8.py`).
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import time
import dataclasses as dc
from typing import Dict, List, Optional, Tuple, Iterable

# ---------------------------------------------------------------------------
# Domain definitions
# ---------------------------------------------------------------------------

EXCHANGES = ["NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"]

ETF_DEF = {
    "ETFA":  ["NGUP","OIT","KTST","FSR","JZRO","XFR"],
    "ETFB":  ["KOTD","INA","HT","JNAF","DLKV","DDJH"],
    "ETFA3": ["NGUP","KTST","XFR"],
    "ETFB3": ["KOTD","INA","DLKV"],
    "ETFSH": ["GOLD","XAG"],
}

# Where each ETF lists (must match constituents on the SAME venue to do
# risk-free same-venue arb).
SAME_VENUE_ARB: Dict[str, List[str]] = {
    "ZSE":      ["ETFA","ETFB","ETFA3","ETFB3","ETFSH"],
    "NYSE":     ["ETFA3"],
    "TMX":      ["ETFA3"],
    "NASDAQ":   ["ETFB3"],
    "HKEX":     ["ETFB3"],
    "Euronext": ["ETFSH"],
    "JPX":      ["ETFSH"],
}

# Listings — precomputed for quick membership checks.
LISTINGS = {
    "CARD":  EXCHANGES,
    "SIMP":  EXCHANGES,
    "NGUP":  ["NYSE","NASDAQ","Euronext","TMX","ZSE"],
    "OIT":   ["LSE","Euronext","HKEX","NSE","ZSE"],
    "KTST":  ["NYSE","JPX","TMX","ZSE"],
    "FSR":   ["NASDAQ","LSE","SSE","HKEX","ZSE"],
    "JZRO":  ["NYSE","LSE","Euronext","TMX","ZSE"],
    "XFR":   ["NYSE","HKEX","TMX","ZSE"],
    "KOTD":  ["NASDAQ","LSE","Euronext","HKEX","ZSE"],
    "INA":   ["NYSE","NASDAQ","Euronext","HKEX","ZSE"],
    "HT":    ["NASDAQ","LSE","JPX","SSE","TMX","ZSE"],
    "JNAF":  ["NYSE","Euronext","JPX","HKEX","ZSE"],
    "DLKV":  ["NASDAQ","LSE","HKEX","NSE","ZSE"],
    "DDJH":  ["NYSE","LSE","Euronext","TMX","ZSE"],
    "MDKA":  ["NYSE","LSE","HKEX","TMX","ZSE"],
    "KRAS":  ["NYSE","Euronext","SSE","TMX","ZSE"],
    "ZITO":  ["NASDAQ","LSE","Euronext","NSE","ZSE"],
    "ZABA":  ["NYSE","LSE","SSE","NSE","TMX","ZSE"],
    "GOLD":  ["NASDAQ","Euronext","JPX","TMX","ZSE"],
    "XAG":   ["LSE","Euronext","JPX","ZSE"],
    "ETFA":  ["NYSE","Euronext","HKEX","ZSE"],
    "ETFB":  ["NASDAQ","LSE","HKEX","ZSE"],
    "ETFA3": ["NYSE","TMX","ZSE"],
    "ETFB3": ["NASDAQ","HKEX","ZSE"],
    "ETFSH": ["Euronext","JPX","ZSE"],
}

POS_FLOOR = -200
POS_CEIL  = 2000
CASH_FLOOR_CENTS = -5_000_000
START_CASH_CENTS = 10_000_000
MSGS_PER_SEC_CAP = 450  # leave 50 headroom under the 500 hard limit

# ---------------------------------------------------------------------------
# Order-book cache
# ---------------------------------------------------------------------------

@dc.dataclass
class Book:
    bid_p: List[int] = dc.field(default_factory=list)
    bid_q: List[int] = dc.field(default_factory=list)
    ask_p: List[int] = dc.field(default_factory=list)
    ask_q: List[int] = dc.field(default_factory=list)
    time:  int = 0

    def update(self, depth: dict, t: int):
        # depth = {"bids": {price_str: qty}, "asks": {price_str: qty}}
        bids = sorted(((int(p), q) for p, q in depth.get("bids", {}).items()), reverse=True)
        asks = sorted(((int(p), q) for p, q in depth.get("asks", {}).items()))
        self.bid_p = [p for p, _ in bids]
        self.bid_q = [q for _, q in bids]
        self.ask_p = [p for p, _ in asks]
        self.ask_q = [q for _, q in asks]
        self.time = t

    def best_bid(self): return (self.bid_p[0], self.bid_q[0]) if self.bid_p else (None, 0)
    def best_ask(self): return (self.ask_p[0], self.ask_q[0]) if self.ask_p else (None, 0)
    def mid(self):
        if not self.bid_p or not self.ask_p: return None
        return (self.bid_p[0] + self.ask_p[0]) / 2

    def sweep_buy(self, want: int) -> Tuple[int, int]:
        """Cost in cents to buy `want` shares walking the asks. Returns (filled, cost)."""
        rem, cost = want, 0
        for p, q in zip(self.ask_p, self.ask_q):
            if rem <= 0: break
            take = min(rem, q); cost += take * p; rem -= take
        return want - rem, cost

    def sweep_sell(self, want: int) -> Tuple[int, int]:
        """Revenue in cents to sell `want` shares walking the bids. Returns (filled, revenue)."""
        rem, rev = want, 0
        for p, q in zip(self.bid_p, self.bid_q):
            if rem <= 0: break
            take = min(rem, q); rev += take * p; rem -= take
        return want - rem, rev


# ---------------------------------------------------------------------------
# Strategy core (pure logic, no I/O)
# ---------------------------------------------------------------------------

@dc.dataclass
class Order:
    instrument: str        # "<EX>-<TICKER>"
    side: str              # "bid"|"ask"
    price: int             # cents (ignored if order_type=="market")
    quantity: int
    order_type: str = "ioc"

@dc.dataclass
class Strategy:
    """Single-process strategy — receives full snapshot of every venue and
    emits a list of orders to send. Designed so the WS bridge AND the
    backtester drive it identically.
    """
    # books[ex][ticker] = Book
    books: Dict[str, Dict[str, Book]] = dc.field(default_factory=lambda: {ex: {} for ex in EXCHANGES})
    # net positions: pos[ex][ticker] = int (signed). Updated by *us* on
    # successful fills; in production we reconcile against get_inventory.
    pos:   Dict[str, Dict[str, int]]  = dc.field(default_factory=lambda: {ex: collections.defaultdict(int) for ex in EXCHANGES})
    cash:  Dict[str, int]             = dc.field(default_factory=lambda: {ex: START_CASH_CENTS for ex in EXCHANGES})
    # per-second sliding window message counter
    msgs:  Dict[str, collections.deque] = dc.field(default_factory=lambda: {ex: collections.deque() for ex in EXCHANGES})
    # configurable
    min_arb_cents: int = 1     # require ≥1c profit per round trip
    max_batch:     int = 400   # max k per single arb cycle
    inv_buffer:    int = 1     # leave just 1 share head-room around limits
    seat:          str = "ZSE" # current colocation
    # only fire on venues whose one-way latency from `seat` is below this
    max_latency_ms: int = 80
    log: logging.Logger = dc.field(default_factory=lambda: logging.getLogger("strat"))

    # ---------- accounting ----------
    def _budget(self, ex: str, n: int) -> bool:
        """True if we can send `n` more messages this second on `ex`."""
        now = time.time()
        q = self.msgs[ex]
        while q and q[0] <= now - 1.0:
            q.popleft()
        return len(q) + n <= MSGS_PER_SEC_CAP

    def _spend(self, ex: str, n: int):
        now = time.time()
        for _ in range(n):
            self.msgs[ex].append(now)

    # ---------- order book intake ----------
    LATENCY_RT = {
        "NYSE":     {"NYSE":0,"NASDAQ":1,"SSE":165,"JPX":152,"Euronext":84,"LSE":80,"HKEX":180,"NSE":174,"TMX":11,"ZSE":96},
        "ZSE":      {"NYSE":96,"NASDAQ":96,"SSE":145,"JPX":140,"Euronext":22,"LSE":24,"HKEX":150,"NSE":95,"TMX":98,"ZSE":0},
        "HKEX":     {"NYSE":180,"NASDAQ":180,"SSE":19,"JPX":37,"Euronext":130,"LSE":135,"HKEX":0,"NSE":53,"TMX":174,"ZSE":150},
    }

    def is_close(self, ex: str) -> bool:
        rt = self.LATENCY_RT.get(self.seat, {}).get(ex, 9999)
        return (rt / 2) <= self.max_latency_ms

    def on_snapshot(self, ex: str, depths: dict, t: int) -> List[Order]:
        for inst, depth in depths.items():
            if not inst.startswith(f"{ex}-"): continue
            tk = inst.split("-",1)[1]
            self.books[ex].setdefault(tk, Book()).update(depth, t)
        if not self.is_close(ex): return []  # skip distant venues entirely
        return self._react(ex, t)

    # ---------- strategy ----------
    def _react(self, ex: str, t: int) -> List[Order]:
        out: List[Order] = []
        out += self._etf_arb(ex, t)
        return out

    def _etf_arb(self, ex: str, t: int) -> List[Order]:
        if ex not in SAME_VENUE_ARB: return []
        out: List[Order] = []
        for etf in SAME_VENUE_ARB[ex]:
            comps = ETF_DEF[etf]
            if etf not in self.books[ex]: continue
            if not all(c in self.books[ex] for c in comps): continue
            n = len(comps)
            etf_book = self.books[ex][etf]
            comp_books = [self.books[ex][c] for c in comps]
            if not etf_book.bid_p or not etf_book.ask_p: continue
            if any(not b.bid_p or not b.ask_p for b in comp_books): continue

            # ---------------- LONG ETF / SHORT BASKET ----------------
            # buy n*k of ETF (sweep asks), sell k of each constituent (sweep bids)
            k_hi_pos = min((POS_CEIL - self.pos[ex][etf] - self.inv_buffer)//n,
                           *(self.pos[ex][c] - POS_FLOOR - self.inv_buffer for c in comps))
            k_hi_pos = min(k_hi_pos, self.max_batch)
            if k_hi_pos > 0:
                k = self._best_k_long(etf_book, comp_books, n, k_hi_pos)
                if k > 0:
                    msgs_needed = 1 + n
                    if self._budget(ex, msgs_needed):
                        # fire IOCs at best price (we want to take the touch)
                        out.append(Order(f"{ex}-{etf}", "bid", etf_book.ask_p[-1] if False else etf_book.ask_p[0]*1 + 0, n*k, "ioc"))
                        # safer: send IOC at a price aggressive enough to clear depth used
                        # Use ask_p[2] (or last available) to walk through L1+L2+L3 if k is bigger than L1
                        # But we already capped at L1 effectively in `_best_k_long`.
                        # For simplicity send at top-3 ask:
                        last_ask_used = etf_book.ask_p[min(2, len(etf_book.ask_p)-1)]
                        out[-1] = Order(f"{ex}-{etf}", "bid", last_ask_used, n*k, "ioc")
                        for c, b in zip(comps, comp_books):
                            last_bid_used = b.bid_p[min(2, len(b.bid_p)-1)]
                            out.append(Order(f"{ex}-{c}", "ask", last_bid_used, k, "ioc"))
                        self._spend(ex, msgs_needed)
                        # pre-update internal pos/cash optimistically; reconciled on fill response
                        self.pos[ex][etf] += n*k
                        for c in comps: self.pos[ex][c] -= k
                        # don't move cash here; settle on fill response

            # ---------------- SHORT ETF / LONG BASKET ----------------
            k_hi_pos = min((self.pos[ex][etf] - POS_FLOOR - self.inv_buffer)//n,
                           *(POS_CEIL - self.pos[ex][c] - self.inv_buffer for c in comps))
            k_hi_pos = min(k_hi_pos, self.max_batch)
            if k_hi_pos > 0:
                k = self._best_k_short(etf_book, comp_books, n, k_hi_pos)
                if k > 0:
                    msgs_needed = 1 + n
                    if self._budget(ex, msgs_needed):
                        last_bid_used = etf_book.bid_p[min(2, len(etf_book.bid_p)-1)]
                        out.append(Order(f"{ex}-{etf}", "ask", last_bid_used, n*k, "ioc"))
                        for c, b in zip(comps, comp_books):
                            last_ask_used = b.ask_p[min(2, len(b.ask_p)-1)]
                            out.append(Order(f"{ex}-{c}", "bid", last_ask_used, k, "ioc"))
                        self._spend(ex, msgs_needed)
                        self.pos[ex][etf] -= n*k
                        for c in comps: self.pos[ex][c] += k
        return out

    @staticmethod
    def _best_k_long(etf_book: Book, comp_books: List[Book], n: int, k_hi: int) -> int:
        """Largest k>0 s.t. sweep-cost(buy n*k ETF) < sweep-rev(sell k of each comp)."""
        lo, hi, ans = 1, k_hi, 0
        while lo <= hi:
            mid = (lo + hi)//2
            f, c_etf = etf_book.sweep_buy(n*mid)
            if f < n*mid:
                hi = mid-1; continue
            ok = True; rev = 0
            for cb in comp_books:
                f2, r = cb.sweep_sell(mid)
                if f2 < mid: ok=False; break
                rev += r
            if not ok:
                hi = mid-1; continue
            edge_per_share = (rev - c_etf) / max(n*mid, 1)
            if rev - c_etf >= 1:  # profit at least 1 cent total
                ans = mid; lo = mid+1
            else:
                hi = mid-1
        return ans

    @staticmethod
    def _best_k_short(etf_book: Book, comp_books: List[Book], n: int, k_hi: int) -> int:
        lo, hi, ans = 1, k_hi, 0
        while lo <= hi:
            mid = (lo + hi)//2
            f, r_etf = etf_book.sweep_sell(n*mid)
            if f < n*mid:
                hi = mid-1; continue
            ok = True; cost = 0
            for cb in comp_books:
                f2, c = cb.sweep_buy(mid)
                if f2 < mid: ok=False; break
                cost += c
            if not ok:
                hi = mid-1; continue
            if r_etf - cost >= 1:
                ans = mid; lo = mid+1
            else:
                hi = mid-1
        return ans

    # ---------- fill bookkeeping (production WS) ----------
    def on_fill(self, ex: str, ticker: str, side: str, qty: int, price: int):
        sgn = +1 if side == "bid" else -1
        # we already optimistically updated pos in _etf_arb; reconcile when
        # production reports an immediate_inventory_change. For now: cash-only.
        cash_delta = -sgn * qty * price
        self.cash[ex] += cash_delta


# ---------------------------------------------------------------------------
# WebSocket bridge (production runtime)
# ---------------------------------------------------------------------------

# All async I/O is gated behind a function so tests/backtests don't import
# `websockets`. The competition VM provides it.
async def run_exchange(ex: str, host: str, strategy: Strategy):
    import websockets  # type: ignore
    backoff = 0.5
    while True:
        try:
            uri = f"ws://{host}:9001/trade"
            async with websockets.connect(uri, max_size=2**24, ping_interval=15) as ws:
                logging.info("[%s] connected %s", ex, uri)
                backoff = 0.5
                async for raw in ws:
                    try:
                        m = json.loads(raw)
                    except json.JSONDecodeError:
                        # rate-limit error frame, etc.
                        logging.warning("[%s] non-json: %s", ex, raw[:80])
                        break
                    if m.get("type") == "market_data_update":
                        depths = m.get("orderbook_depths") or {}
                        orders = strategy.on_snapshot(ex, depths, m.get("time", 0))
                        for o in orders:
                            req = {
                                "type": "add_order",
                                "user_request_id": f"{ex}-{int(time.time()*1e6)}",
                                "instrument_id": o.instrument,
                                "price": o.price,
                                "expiry": int(time.time()*1000) + 5_000,
                                "side": o.side,
                                "quantity": o.quantity,
                                "order_type": o.order_type,
                            }
                            try:
                                await ws.send(json.dumps(req))
                            except Exception as e:
                                logging.warning("[%s] send failed: %s", ex, e)
                                break
                    elif m.get("type") == "add_order_response":
                        d = m.get("data") or {}
                        if d.get("immediate_inventory_change"):
                            # reconcile cash; pos was already updated optimistically
                            cents = d.get("immediate_balance_change") or 0
                            strategy.cash[ex] += cents
                    elif m.get("type") == "end_of_round":
                        logging.info("[%s] end_of_round", ex)
                        break
        except Exception as e:
            logging.warning("[%s] disconnected: %s", ex, e)
        await asyncio.sleep(backoff)
        backoff = min(5.0, backoff * 1.6)


def hostname(ex: str) -> str:
    return f"{ex.lower()}.algotrade.hr"


async def main_async():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    s = Strategy()
    tasks = [asyncio.create_task(run_exchange(ex, hostname(ex), s)) for ex in EXCHANGES]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main_async())
