"""Replay the recorded order books through fabijan_v8.Strategy and report
realized PnL, fills, and per-venue breakdown.

Latency model: when colocated at <SEAT>, every IOC takes one-way latency
LATENCY[seat][exchange] before reaching the matching engine. We assume:
  - Our IOC arrives at the matching engine at t + dt.
  - At t+dt, the order book is whatever the next CSV snapshot at that venue
    shows (advance the book to the latest tick at-or-before t+dt).
  - We sweep that *advanced* book with the IOC's limit price and quantity.
  - This makes the simulator pessimistic at high latency: the displayed L1
    you saw at t may already be gone at t+dt.
"""
import os, sys, json, glob
import pandas as pd
import numpy as np
import collections, dataclasses as dc, copy
sys.path.insert(0, "/home/pitfa/Documents/algotrade2026")
from fabijan_v8 import (
    Strategy, Book, Order, EXCHANGES, ETF_DEF, SAME_VENUE_ARB,
    POS_FLOOR, POS_CEIL, START_CASH_CENTS, MSGS_PER_SEC_CAP,
)

DATA = "/home/pitfa/Documents/algotrade2026/market_data"

# round-trip latency in ms from the official table; halved for one-way
LATENCY_RT = {
    "NYSE":     {"NYSE":0,"NASDAQ":1,"SSE":165,"JPX":152,"Euronext":84,"LSE":80,"HKEX":180,"NSE":174,"TMX":11,"ZSE":96},
    "ZSE":      {"NYSE":96,"NASDAQ":96,"SSE":145,"JPX":140,"Euronext":22,"LSE":24,"HKEX":150,"NSE":95,"TMX":98,"ZSE":0},
    "HKEX":     {"NYSE":180,"NASDAQ":180,"SSE":19,"JPX":37,"Euronext":130,"LSE":135,"HKEX":0,"NSE":53,"TMX":174,"ZSE":150},
}

def load_books(ex: str) -> pd.DataFrame:
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv")
    int_cols = [c for c in df.columns if c not in ("instrument",)]
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
    df["ticker"] = df["instrument"].str.split("-",n=1).str[1]
    return df.sort_values("time").reset_index(drop=True)

def load_orderbook_snapshots(ex: str):
    """Yield (time, {ticker: depth_dict}) per broadcast tick."""
    df = load_books(ex)
    out = []
    for t, g in df.groupby("time", sort=True):
        depths = {}
        for r in g.itertuples():
            depths[f"{ex}-{r.ticker}"] = {
                "bids": {str(r.bid1_price): r.bid1_qty,
                          str(r.bid2_price): r.bid2_qty,
                          str(r.bid3_price): r.bid3_qty},
                "asks": {str(r.ask1_price): r.ask1_qty,
                          str(r.ask2_price): r.ask2_qty,
                          str(r.ask3_price): r.ask3_qty},
            }
        out.append((int(t), depths))
    return out


@dc.dataclass
class FillSim:
    """Simulates IOC matching against a Book snapshot at order arrival.
    DECREMENTS the book in-place so subsequent matches see depleted depth."""
    @staticmethod
    def fill_ioc(book: Book, side: str, limit_price: int, qty: int):
        if side == "bid":
            rem = qty; cost = 0; filled = 0
            for i in range(len(book.ask_p)):
                if rem <= 0: break
                p, q = book.ask_p[i], book.ask_q[i]
                if p > limit_price or q <= 0: continue
                take = min(rem, q); cost += take*p; filled += take; rem -= take
                book.ask_q[i] = q - take
            return filled, cost
        else:
            rem = qty; rev = 0; filled = 0
            for i in range(len(book.bid_p)):
                if rem <= 0: break
                p, q = book.bid_p[i], book.bid_q[i]
                if p < limit_price or q <= 0: continue
                take = min(rem, q); rev += take*p; filled += take; rem -= take
                book.bid_q[i] = q - take
            return filled, rev


class Backtester:
    def __init__(self, seat: str = "ZSE"):
        self.seat = seat
        self.lat_rt = LATENCY_RT[seat]
        self.lat_oneway = {ex: self.lat_rt[ex]/2 for ex in EXCHANGES}
        self.strategy = Strategy(seat=seat)
        # Override rate budget so it works on simulated wall-clock
        # We swap msg-budget to count per-second-of-data instead of real time.
        # For backtest, we use a per-second counter keyed on data time.
        self._data_msgs = {ex: collections.deque() for ex in EXCHANGES}
        self.strategy._budget = self._budget_data
        self.strategy._spend  = self._spend_data
        self._now_ms = 0  # current data time

        # full-feed timeline: merge all venue snapshots, ordered by time
        self.snapshots = {}
        self.iters = {}
        self.next_t = {}
        for ex in EXCHANGES:
            ss = load_orderbook_snapshots(ex)
            self.snapshots[ex] = ss
            self.iters[ex] = iter(ss)
            try:
                self.next_t[ex] = next(self.iters[ex])
            except StopIteration:
                self.next_t[ex] = None

        # pending fills: list of (arrival_ms, ex, ticker, side, price, qty)
        self.pending: List = []
        # PnL bookkeeping (we track cash + mark-to-market at end)
        self.cash = {ex: START_CASH_CENTS for ex in EXCHANGES}
        self.pos  = {ex: collections.defaultdict(int) for ex in EXCHANGES}
        # keep latest book to mark-to-market at end
        self.last_book = {ex: {} for ex in EXCHANGES}
        # diagnostics
        self.fills = []
        self.sent_orders = 0
        self.rejected_orders = 0  # overshot rate-limit
        self.partial_fills = 0
        self.no_fill = 0

    # rate-limit accounting on data clock
    def _budget_data(self, ex, n):
        q = self._data_msgs[ex]
        floor = self._now_ms - 1000
        while q and q[0] <= floor:
            q.popleft()
        return len(q) + n <= MSGS_PER_SEC_CAP

    def _spend_data(self, ex, n):
        for _ in range(n):
            self._data_msgs[ex].append(self._now_ms)

    def _process_pending(self, up_to_ms: int):
        """Match all pending orders that arrive at or before `up_to_ms`."""
        idx = 0
        new_pending = []
        for (arr_ms, ex, inst, side, price, qty) in self.pending:
            if arr_ms <= up_to_ms:
                # find the orderbook for `ex`/`ticker` *as of* arr_ms
                tk = inst.split("-",1)[1]
                book = self.last_book[ex].get(tk)
                if book is None:
                    self.no_fill += 1; continue
                # advance the venue's snapshots up to arr_ms
                # — this is already done in main loop; last_book is up-to-date
                f, c = FillSim.fill_ioc(book, side, price, qty)
                # clamp f so the simulated pos cannot exceed -200/+2000
                cur = self.pos[ex][tk]
                if side == "bid":
                    cap = max(0, POS_CEIL - cur)
                    f = min(f, cap)
                else:
                    cap = max(0, cur - POS_FLOOR)
                    f = min(f, cap)
                if f == 0:
                    self.no_fill += 1; continue
                if f < qty:
                    self.partial_fills += 1
                # cost/revenue must be recomputed because we may have shrunk f
                # — assume the trimmed shares would have been taken at the
                # vwap of the original sweep (inexact but conservative).
                if qty > 0:
                    c = int(round(c * f / qty)) if c else 0
                if side == "bid":
                    self.cash[ex] -= c
                    self.pos[ex][tk] += f
                else:
                    self.cash[ex] += c
                    self.pos[ex][tk] -= f
                self.fills.append((arr_ms, ex, tk, side, price, qty, f, c))
                # Adjust internal strategy bookkeeping for ACTUAL fills
                # (strategy optimistically updated qty; we correct for partial fills)
                # The strategy assumed full fill (n*k or k). If partial, residual exists.
                # For simplicity, treat as: strategy already shifted its `pos` by `qty`;
                # adjust by (f - qty) here (negative if partial).
                miss = qty - f
                if miss > 0:
                    if side == "bid":
                        self.strategy.pos[ex][tk] -= miss
                    else:
                        self.strategy.pos[ex][tk] += miss
            else:
                new_pending.append((arr_ms, ex, inst, side, price, qty))
        self.pending = new_pending

    def run(self):
        """Replay all venue snapshots in time order, dispatching strategy and
        matching pending fills along the way."""
        venues_done = set()
        # heap-ish: pop the venue with smallest next time
        while True:
            cands = [(self.next_t[ex][0], ex) for ex in EXCHANGES if self.next_t[ex] is not None]
            if not cands: break
            t, ex = min(cands)
            self._now_ms = t
            self._process_pending(t)

            # Apply this venue's snapshot to last_book[ex] AND drive strategy
            _, depths = self.next_t[ex]
            for inst, depth in depths.items():
                tk = inst.split("-",1)[1]
                self.last_book[ex].setdefault(tk, Book()).update(depth, t)
            orders = self.strategy.on_snapshot(ex, depths, t)
            for o in orders:
                # The strategy already accounted for the message in the rate budget.
                self.sent_orders += 1
                arr_ms = t + int(self.lat_oneway[ex])
                self.pending.append((arr_ms, ex, o.instrument, o.side, o.price, o.quantity))

            try:
                self.next_t[ex] = next(self.iters[ex])
            except StopIteration:
                self.next_t[ex] = None

        # process any remaining pending
        self._process_pending(self._now_ms + 10_000_000)

        # mark-to-market: close all positions at last seen mid (no spread).
        # This *over*-states the unwind value because real unwind costs L1
        # spread; we'll print BOTH numbers below.
        m2m_optimistic = 0
        m2m_realistic = 0
        for ex in EXCHANGES:
            for tk, q in self.pos[ex].items():
                if q == 0: continue
                book = self.last_book[ex].get(tk)
                if book is None or not book.bid_p or not book.ask_p: continue
                mid = (book.bid_p[0]+book.ask_p[0])/2
                m2m_optimistic += q * mid
                if q > 0:
                    # closing a long -> hit the bid
                    m2m_realistic += q * book.bid_p[0]
                else:
                    m2m_realistic += q * book.ask_p[0]

        cash_total = sum(self.cash.values())
        starting   = START_CASH_CENTS * len(EXCHANGES)
        pnl_opt    = cash_total + m2m_optimistic - starting
        pnl_real   = cash_total + m2m_realistic - starting

        print(f"\n=== Backtest seat={self.seat} ===")
        print(f"sent orders: {self.sent_orders}, fills: {len(self.fills)}, partial: {self.partial_fills}, no-fill: {self.no_fill}")
        print(f"realized cash change ($): {(cash_total-starting)/100:>14,.2f}")
        print(f"M2M optimistic (mid)   ($): {m2m_optimistic/100:>14,.2f}")
        print(f"M2M realistic (touch)  ($): {m2m_realistic/100:>14,.2f}")
        print(f"PnL optimistic ($): {pnl_opt/100:>14,.2f}")
        print(f"PnL realistic  ($): {pnl_real/100:>14,.2f}")

        # per-venue breakdown
        print("\nPer venue end positions / cash change:")
        for ex in EXCHANGES:
            cash_chg = self.cash[ex] - START_CASH_CENTS
            pos_str = ", ".join(f"{tk}:{q:+d}" for tk, q in self.pos[ex].items() if q != 0)
            print(f"  {ex:9s} cash {cash_chg/100:+10,.2f}$  pos[{pos_str}]")
        return pnl_real, pnl_opt


if __name__ == "__main__":
    seat = sys.argv[1] if len(sys.argv) > 1 else "ZSE"
    bt = Backtester(seat=seat)
    bt.run()
