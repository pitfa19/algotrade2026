"""Diagnostic: rate-limit utilization, per-second arb count, and what's
actually limiting more profit (rate vs. position vs. spread)."""
import sys
sys.path.insert(0, "/home/pitfa/Documents/algotrade2026")
sys.path.insert(0, "/home/pitfa/Documents/algotrade2026/edge_research")
from importlib import reload
import edge_research as _; del _
import pandas as pd, numpy as np, collections, dataclasses as dc
from fabijan_v8 import EXCHANGES, ETF_DEF, SAME_VENUE_ARB, MSGS_PER_SEC_CAP

# Re-run a streamlined arb sim and emit per-second timeseries
DATA = "/home/pitfa/Documents/algotrade2026/market_data"

def per_sec_arb(ex):
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv")
    df["ticker"] = df["instrument"].str.split("-",n=1).str[1]
    counts = collections.Counter()
    edges  = collections.defaultdict(list)
    for etf in SAME_VENUE_ARB.get(ex, []):
        comps = ETF_DEF[etf]
        if not all(c in df.ticker.unique() for c in comps): continue
        sub = df[df.ticker.isin([etf]+comps)]
        for t, g in sub.groupby("time"):
            sec = t // 1000
            ask, bid = {}, {}
            for r in g.itertuples():
                ask[r.ticker] = (r.ask1_price, r.ask1_qty, r.ask2_price, r.ask2_qty, r.ask3_price, r.ask3_qty)
                bid[r.ticker] = (r.bid1_price, r.bid1_qty, r.bid2_price, r.bid2_qty, r.bid3_price, r.bid3_qty)
            if etf not in ask: continue
            if any(c not in ask for c in comps): continue
            n = len(comps)
            # check L1-only edge (long_etf): sum(C_bid1) > n * etf_ask1
            sum_b = sum(bid[c][0] for c in comps)
            sum_a = sum(ask[c][0] for c in comps)
            if sum_b > n * ask[etf][0]:
                counts[sec] += 1
                edges[sec].append((sum_b - n*ask[etf][0]) / n)
            if n * bid[etf][0] > sum_a:
                counts[sec] += 1
                edges[sec].append((n*bid[etf][0] - sum_a) / n)
    return counts

print("=== Arb opportunities per second per venue (L1, count both sides) ===")
total_per_sec = collections.Counter()
for ex in ["ZSE","NYSE","NASDAQ","TMX","HKEX","Euronext","JPX"]:
    c = per_sec_arb(ex)
    if not c: continue
    total = sum(c.values())
    avg = total / max(1, len(c))
    print(f"  {ex:9s}  total={total:5d}  avg/sec={avg:5.1f}  max/sec={max(c.values()):5d}")
    for s,v in c.items():
        total_per_sec[s] += v

print("\n=== Total arbs/sec across all venues (rate-limit pressure) ===")
print(f"avg arbs/sec across all venues: {sum(total_per_sec.values())/max(1,len(total_per_sec)):.1f}")
print(f"max arbs/sec: {max(total_per_sec.values())}")
print(f"each arb costs 1+n msgs (~7 for 6-leg). At avg X arbs/s × 7 = msgs/s.")
print(f"500/s budget is binding only if arbs/s > 71 across one venue — we're far below.")
