#!/usr/bin/env python3
"""backtest.py — replay AlgoTrade market_data CSVs against a bot's strategy code.

Loads <venue>_orderbooks.csv and <venue>_trades.csv per active venue, drives
the bot's Hub synchronously (no asyncio, no WebSocket), simulates fills, and
prints P&L + per-strategy breakdown at the end.

The bot is loaded as a Python module — its Hub class is invoked directly with
mock Connection objects substituted for the real WS layer. Outbound orders the
bot enqueues are intercepted, simulated against the current replayed book,
and the response is fed back through the bot's normal handlers.

Usage:
    python backtest.py namikv2.py
    python backtest.py namikv2.py --venues NYSE,NASDAQ,LSE
    python backtest.py namikv2.py --venues all --cluster ASIA --max-events 50000

Assumptions / known limitations:
  - IOC orders fill against the current book at submission time, walking
    levels up to their limit price. Filled liquidity is depleted from our
    local book copy so a subsequent IOC at the same tick can't double-fill.
  - Limit orders that immediately cross fill via the IOC path; the rest
    sits on a per-venue resting book and fills when a market trade in the
    replay crosses our price (best-case queue: we're always at the front).
    Realistic queue position would shave fill rate further — read the
    numbers as an upper bound for MM strategies.
  - The replay does NOT model our own market impact on later snapshots.
  - End-of-segment unwinding triggers based on the bot's own server_time
    tracking; the welcome we synthesize advertises round_length=600_000.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import logging
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Optional


# ──────────────────────────────────────────────────────────────────────
# CSV loaders
# ──────────────────────────────────────────────────────────────────────

@dataclass
class OBSnapshot:
    time: int
    venue: str
    ticker: str
    bids: dict[int, int]
    asks: dict[int, int]


def load_orderbooks(venue: str, market_dir: Path) -> list[OBSnapshot]:
    path = market_dir / f"{venue}_orderbooks.csv"
    if not path.exists():
        return []
    def _i(s: str) -> int:
        s = (s or "").strip()
        return int(s) if s else 0

    out: list[OBSnapshot] = []
    with open(path) as f:
        for d in csv.DictReader(f):
            inst = d["instrument"]
            ex, _, tk = inst.partition("-")
            bids: dict[int, int] = {}
            asks: dict[int, int] = {}
            for i in (1, 2, 3):
                bp = _i(d.get(f"bid{i}_price")); bq = _i(d.get(f"bid{i}_qty"))
                ap = _i(d.get(f"ask{i}_price")); aq = _i(d.get(f"ask{i}_qty"))
                if bp > 0 and bq > 0:
                    bids[bp] = bq
                if ap > 0 and aq > 0:
                    asks[ap] = aq
            out.append(OBSnapshot(_i(d["time"]), ex, tk, bids, asks))
    return out


def load_trades(venue: str, market_dir: Path) -> list[dict]:
    path = market_dir / f"{venue}_trades.csv"
    if not path.exists():
        return []
    out: list[dict] = []
    with open(path) as f:
        for d in csv.DictReader(f):
            inst = d["instrument"]
            ex, _, tk = inst.partition("-")
            out.append({
                "time": int(d["time"]),
                "venue": ex,
                "ticker": tk,
                "price": int(d["price"]),
                "qty": int(d["quantity"]),
            })
    return out


# ──────────────────────────────────────────────────────────────────────
# Mock connection — captures bot's outbound msgs into an in-memory queue
# ──────────────────────────────────────────────────────────────────────

class MockConnection:
    def __init__(self, exchange: str):
        self.exchange = exchange
        self.connected = True
        self.outbound: deque[dict] = deque()

    def enqueue(self, msg: dict) -> None:
        self.outbound.append(msg)


# ──────────────────────────────────────────────────────────────────────
# Sim engine
# ──────────────────────────────────────────────────────────────────────

class SimEngine:
    """Drives the bot's Hub through replayed data and a simulated matcher."""

    def __init__(self, bot_module, venues: list[str], my_cluster: str):
        self.bot = bot_module
        self.venues = venues

        # Force a few timing knobs to play nicely with sub-real-time stepping
        # (otherwise MM never refreshes since wallclock barely advances).
        for name, value in [
            ("MM_REFRESH_S", 0.0),
            ("INVENTORY_PERIOD_S", 0.0),
            ("HEARTBEAT_LOG_S", 5.0),
        ]:
            if hasattr(bot_module, name):
                setattr(bot_module, name, value)

        # Build the Hub. Newer bots (namikv2) take my_cluster; older ones don't.
        try:
            self.hub = bot_module.Hub(venues, my_cluster)
        except TypeError:
            self.hub = bot_module.Hub(venues)

        # Replace WS connections with mocks; keep buckets as-is (their .acquire
        # is async and we never call it because we never run a sender_loop).
        for v in venues:
            self.hub.connections[v] = MockConnection(v)

        # Resting orders we own (per venue): {oid: (ticker, side, qty_remaining, price)}
        self.resting: dict[str, dict[int, tuple[str, str, int, int]]] = {v: {} for v in venues}

        # Local view of each venue's book — we deplete this as IOCs eat
        # liquidity within a single tick, so we don't double-fill against
        # the same listed quantity.
        self.venue_books: dict[str, dict[str, dict[str, dict[int, int]]]] = {
            v: {} for v in venues
        }

        self._oid = 1_000_000

        # Stat counters (in addition to the bot's own)
        self.ioc_orders = 0
        self.limit_orders = 0
        self.cancels = 0
        self.resting_fills = 0

        # Synthesize welcome + zero-inventory so the bot enters its ready state.
        for v in venues:
            self.hub.on_welcome(v, {
                "type": "welcome",
                "round_length": 600_000,
                "time": 0,
            })
            inv_data = {f"{v}-{tk}": [0, 0] for tk in self.hub.tickers_on_ex[v]}
            inv_data["$"] = [0, getattr(bot_module, "INITIAL_CASH", 10_000_000)]
            self.hub.on_inventory(v, {"data": inv_data})
            # Drain the get_pending_orders msg the bot sent during on_welcome.
            self._drain_admin(v)

    # ── feeding the bot ──
    def feed_md(self, venue: str, time_ms: int, ticker_to_depth: dict[str, dict]):
        msg = {
            "type": "market_data_update",
            "time": time_ms,
            "orderbook_depths": {
                f"{venue}-{tk}": depth for tk, depth in ticker_to_depth.items()
            },
            "events": [],
        }
        self.hub.on_md(venue, msg)

    def step_strategize(self):
        try:
            self.hub.strategize()
        except Exception as e:
            logging.error("strategize error: %s", e, exc_info=True)

    # ── draining bot's outbound queue ──
    def drain_outbound(self, venue: str):
        conn = self.hub.connections[venue]
        while conn.outbound:
            msg = conn.outbound.popleft()
            mtype = msg.get("type")
            if mtype == "add_order":
                self._handle_add(venue, msg)
            elif mtype == "cancel_order":
                self._handle_cancel(venue, msg)
            elif mtype in ("get_inventory", "get_pending_orders"):
                # No-op in sim — bot's own state is authoritative.
                pass

    def _drain_admin(self, venue: str):
        conn = self.hub.connections[venue]
        keep: deque[dict] = deque()
        while conn.outbound:
            msg = conn.outbound.popleft()
            if msg.get("type") in ("get_inventory", "get_pending_orders"):
                continue
            keep.append(msg)
        conn.outbound = keep

    # ── order matching ──
    def _handle_add(self, venue: str, msg: dict):
        rid = msg.get("user_request_id", "")
        inst = msg["instrument_id"]
        _ex, _, tk = inst.partition("-")
        side = msg["side"]
        qty = int(msg["quantity"])
        ot = msg.get("order_type", "ioc")
        px = int(msg.get("price", 0))

        book = self.venue_books[venue].get(tk)
        if book is None:
            # No book yet for this ticker on this venue — order can't fill
            self._respond_add(venue, rid, success=True, ic=0, bc=0, oid=None)
            return

        if ot == "ioc":
            self.ioc_orders += 1
            filled_qty, total_cents = self._consume_book(book, side, qty, px)
            ic = filled_qty if side == "bid" else -filled_qty
            bc = -total_cents if side == "bid" else total_cents
            self._respond_add(venue, rid, success=True, ic=ic, bc=bc, oid=None)
        elif ot == "limit":
            self.limit_orders += 1
            # Cross-fill what we can immediately, rest the remainder
            crossed_qty, crossed_cents = self._consume_book(book, side, qty, px)
            ic = crossed_qty if side == "bid" else -crossed_qty
            bc = -crossed_cents if side == "bid" else crossed_cents
            remaining = qty - crossed_qty
            self._oid += 1
            oid = self._oid
            self._respond_add(venue, rid, success=True, ic=ic, bc=bc, oid=oid)
            if remaining > 0:
                self.resting[venue][oid] = (tk, side, remaining, px)
        else:
            # Market or unknown — treat like IOC at extreme price
            self.ioc_orders += 1
            sweep_px = 10**9 if side == "bid" else 0
            filled_qty, total_cents = self._consume_book(book, side, qty, sweep_px)
            ic = filled_qty if side == "bid" else -filled_qty
            bc = -total_cents if side == "bid" else total_cents
            self._respond_add(venue, rid, success=True, ic=ic, bc=bc, oid=None)

    def _consume_book(self, book: dict, side: str, qty: int, limit_px: int) -> tuple[int, int]:
        """Walk the book and consume liquidity in-place. Returns (filled, total_cents)."""
        rem = qty
        total = 0
        if side == "bid":  # buying — hit asks ≤ limit
            for ap in sorted(list(book.get("asks", {}))):
                if ap > limit_px:
                    break
                aq = book["asks"][ap]
                take = min(rem, aq)
                total += take * ap
                book["asks"][ap] -= take
                if book["asks"][ap] == 0:
                    del book["asks"][ap]
                rem -= take
                if rem == 0:
                    break
        else:  # selling — hit bids ≥ limit
            for bp in sorted(list(book.get("bids", {})), reverse=True):
                if bp < limit_px:
                    break
                bq = book["bids"][bp]
                take = min(rem, bq)
                total += take * bp
                book["bids"][bp] -= take
                if book["bids"][bp] == 0:
                    del book["bids"][bp]
                rem -= take
                if rem == 0:
                    break
        return qty - rem, total

    def _respond_add(self, venue: str, rid: str, success: bool, ic: int, bc: int,
                     oid: Optional[int]):
        data: dict = {}
        if ic:
            data["immediate_inventory_change"] = ic
        if bc:
            data["immediate_balance_change"] = bc
        if oid is not None:
            data["order_id"] = oid
        self.hub.on_add_order_response(venue, {
            "type": "add_order_response",
            "user_request_id": rid,
            "success": success,
            "data": data,
        })

    def _handle_cancel(self, venue: str, msg: dict):
        self.cancels += 1
        oid = int(msg.get("order_id", 0))
        if oid in self.resting[venue]:
            del self.resting[venue][oid]
        self.hub.on_cancel_order_response(venue, {
            "type": "cancel_order_response",
            "user_request_id": msg.get("user_request_id", ""),
            "success": True,
        })

    # ── resting-order fills triggered by replayed trades ──
    def fill_resting_against_trade(self, venue: str, tk: str, trade_px: int, trade_qty: int):
        """A market trade printed at trade_px — fill our resting orders that
        would have been hit (best-case queue position, we're always at front)."""
        for oid, (rtk, side, rqty, rpx) in list(self.resting[venue].items()):
            if rtk != tk or rqty <= 0:
                continue
            crossed = (side == "bid" and trade_px <= rpx) or (side == "ask" and trade_px >= rpx)
            if not crossed:
                continue
            take = min(rqty, trade_qty)
            self._inject_trade_fill(venue, oid, side, take, rpx)  # fill at OUR limit
            self.resting_fills += 1
            new_rem = rqty - take
            if new_rem == 0:
                self.resting[venue].pop(oid, None)
            else:
                self.resting[venue][oid] = (rtk, side, new_rem, rpx)
            trade_qty -= take
            if trade_qty <= 0:
                break

    def _inject_trade_fill(self, venue: str, oid: int, side: str, qty: int, px: int):
        """Send a synthetic trade event matching one of our resting limit orders
        through the bot's on_md path so it updates pos/cash and stats."""
        msg = {
            "type": "market_data_update",
            "time": 0,
            "orderbook_depths": {},
            "events": [{
                "event_type": "trade",
                "data": {
                    "passiveOrderID": oid,
                    "activeOrderID": -1,
                    "price": px,
                    "quantity": qty,
                },
            }],
        }
        self.hub.on_md(venue, msg)

    # ── update local book view from a fresh snapshot ──
    def update_local_book(self, venue: str, tk: str, bids: dict[int, int], asks: dict[int, int]):
        self.venue_books[venue][tk] = {"bids": dict(bids), "asks": dict(asks)}


# ──────────────────────────────────────────────────────────────────────
# Reporting
# ──────────────────────────────────────────────────────────────────────

def report(sim: SimEngine, elapsed: float):
    hub = sim.hub
    print()
    print("═" * 76)
    print(" BACKTEST RESULTS")
    print("═" * 76)
    print(f" runtime: {elapsed:.1f}s")
    print(f" orders sent: ioc={sim.ioc_orders} limit={sim.limit_orders} cancel={sim.cancels}")
    print(f" total fills: {hub.fills_count}  (resting fills: {sim.resting_fills})")
    print(f" realized cash delta: ${hub.realized_cents/100:+,.2f}")
    print()

    # Per-venue: cash, mtm of position
    total_cash = 0
    total_mtm = 0
    print(" per-venue:")
    print(f"   {'venue':<10} {'cash':>14} {'mtm':>14} {'total':>14} {'pos_n':>7}")
    for v in sim.venues:
        cash = hub.cash[v]
        total_cash += cash
        mtm = 0
        nz = 0
        for tk in hub.tickers_on_ex[v]:
            pos = hub.pos.get((v, tk), 0)
            if pos == 0:
                continue
            nz += 1
            book = sim.venue_books[v].get(tk)
            if not book or not book.get("bids") or not book.get("asks"):
                continue
            mid = (max(book["bids"]) + min(book["asks"])) / 2
            mtm += int(pos * mid)
        total_mtm += mtm
        bal = (cash + mtm) - getattr(sim.bot, "INITIAL_CASH", 10_000_000)
        print(f"   {v:<10} ${cash/100:>12,.0f} ${mtm/100:>12,.0f} ${bal/100:>+12,.0f} {nz:>7}")
    starting = len(sim.venues) * getattr(sim.bot, "INITIAL_CASH", 10_000_000)
    pnl = (total_cash + total_mtm) - starting
    print(f"   {'TOTAL':<10} ${total_cash/100:>12,.0f} ${total_mtm/100:>12,.0f} ${pnl/100:>+12,.0f}")
    print()

    if hub.stats.realized_by_strategy:
        print(" per-strategy:")
        items = sorted(hub.stats.realized_by_strategy.items(),
                       key=lambda kv: -kv[1])
        for s, cents in items[:25]:
            fills = hub.stats.fills_by_strategy.get(s, 0)
            attempts = hub.stats.attempts_by_strategy.get(s, 0)
            print(f"   ${cents/100:>+10,.2f}  fills={fills:<5} attempts={attempts:<5} {s}")
        if len(items) > 25:
            print(f"   ... {len(items) - 25} more")
        print()

    print("═" * 76)


# ──────────────────────────────────────────────────────────────────────
# Driver
# ──────────────────────────────────────────────────────────────────────

def import_bot(path: str):
    spec = importlib.util.spec_from_file_location("bot_under_test", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load bot module from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bot_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("bot", help="path to bot .py file (e.g., namikv2.py)")
    p.add_argument("--venues", default="all",
                   help="comma-separated venue list, or 'all' (default)")
    p.add_argument("--market-dir", default="market_data",
                   help="directory containing <venue>_orderbooks.csv etc.")
    p.add_argument("--max-events", type=int, default=None,
                   help="cap total replayed events (for quick smoke runs)")
    p.add_argument("--cluster", default="NA",
                   help="latency cluster the bot thinks it's in (NA/EU/ASIA/IN/ZSE)")
    p.add_argument("--quiet", action="store_true",
                   help="suppress bot's INFO log (only WARNING+ shown)")
    args = p.parse_args()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    bot = import_bot(args.bot)

    if args.venues == "all":
        venues = list(bot.VENUES)
    else:
        canon = {v.upper(): v for v in bot.VENUES}
        venues = []
        for v in args.venues.split(","):
            v = v.strip()
            if not v:
                continue
            c = canon.get(v.upper())
            if c is None:
                raise SystemExit(f"unknown venue {v!r} (valid: {bot.VENUES})")
            venues.append(c)

    md_dir = Path(args.market_dir)
    print(f"loading market data from {md_dir}/ for {venues}...")
    obs = {v: load_orderbooks(v, md_dir) for v in venues}
    trs = {v: load_trades(v, md_dir) for v in venues}
    obs_n = sum(len(x) for x in obs.values())
    trs_n = sum(len(x) for x in trs.values())
    print(f"  orderbook snapshots: {obs_n:,}  trades: {trs_n:,}")
    if obs_n == 0:
        raise SystemExit("no orderbook data found — check --market-dir")

    # Build unified event stream
    stream: list[tuple[int, str, str, object]] = []
    for v in venues:
        for ob in obs[v]:
            stream.append((ob.time, v, "ob", ob))
        for tr in trs[v]:
            stream.append((tr["time"], v, "tr", tr))
    stream.sort(key=lambda x: x[0])
    if args.max_events:
        stream = stream[: args.max_events]

    sim = SimEngine(bot, venues, args.cluster)

    print(f"replaying {len(stream):,} events...")
    t_start = time.monotonic()

    # Group by timestamp so strategize fires once per tick.
    for time_ms, group_iter in groupby(stream, key=lambda x: x[0]):
        group = list(group_iter)
        touched_venues: set[str] = set()
        for _t, venue, kind, payload in group:
            if kind == "ob":
                ob = payload  # type: ignore
                sim.update_local_book(venue, ob.ticker, ob.bids, ob.asks)
                sim.feed_md(venue, time_ms, {ob.ticker: {
                    "bids": {p: q for p, q in ob.bids.items()},
                    "asks": {p: q for p, q in ob.asks.items()},
                }})
                touched_venues.add(venue)
            elif kind == "tr":
                tr = payload  # type: ignore
                sim.fill_resting_against_trade(venue, tr["ticker"], tr["price"], tr["qty"])
                touched_venues.add(venue)
        sim.step_strategize()
        for v in touched_venues:
            sim.drain_outbound(v)

    elapsed = time.monotonic() - t_start

    # End-of-segment unwind: simulate what the bot would do if the round ended.
    # Advance server_time on all venues to the end and run a few more ticks.
    end_t = stream[-1][0] if stream else 600_000
    for v in venues:
        sim.hub.server_time[v] = end_t
    for _ in range(20):
        sim.step_strategize()
        for v in venues:
            sim.drain_outbound(v)

    report(sim, elapsed)


if __name__ == "__main__":
    main()
