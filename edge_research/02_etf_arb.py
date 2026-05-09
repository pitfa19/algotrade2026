"""How often is *risk-free* same-venue ETF basket arb available?

For each ETF X with constituents C_1..C_n on a given venue, an instant arb
exists if:
  - 6*ETF_bid > sum_i(C_i_ask)  -> SELL ETF, BUY each constituent
  - 6*ETF_ask < sum_i(C_i_bid)  -> BUY ETF, SELL each constituent

We also look at depth-aware version using L1 sizes only (most conservative
available size).
"""
import pandas as pd
import numpy as np

DATA = "/home/pitfa/Documents/algotrade2026/market_data"

ETFS = {
    "ETFA":  ["NGUP","OIT","KTST","FSR","JZRO","XFR"],
    "ETFB":  ["KOTD","INA","HT","JNAF","DLKV","DDJH"],
    "ETFA3": ["NGUP","KTST","XFR"],
    "ETFB3": ["KOTD","INA","DLKV"],
    "ETFSH": ["GOLD","XAG"],
}

EXCHANGES_WITH_ETFS = {
    "ETFA":  ["NYSE","Euronext","HKEX","ZSE"],
    "ETFB":  ["NASDAQ","LSE","HKEX","ZSE"],
    "ETFA3": ["NYSE","TMX","ZSE"],
    "ETFB3": ["NASDAQ","HKEX","ZSE"],
    "ETFSH": ["Euronext","JPX","ZSE"],
}

def analyze(ex):
    """For all ETFs on `ex`, compute per-tick NAV arb opportunities."""
    p = f"{DATA}/{ex}_orderbooks.csv"
    df = pd.read_csv(p)
    df["ticker"] = df["instrument"].str.split("-",n=1).str[1]
    print(f"\n=== {ex} ===")
    for etf, comps in ETFS.items():
        if ex not in EXCHANGES_WITH_ETFS[etf]: continue
        # need ETF + all its constituents on this venue
        present = set(df["ticker"].unique())
        if etf not in present: continue
        if not all(c in present for c in comps):
            missing = [c for c in comps if c not in present]
            print(f"  {etf}: missing constituents on {ex}: {missing} -- can't arb same-venue")
            continue
        wanted = [etf] + comps
        sub = df[df.ticker.isin(wanted)][["time","ticker","bid1_price","bid1_qty","ask1_price","ask1_qty"]].copy()
        # asof-pivot: use forward-fill within tick groups
        # Reduce to one row per (time, ticker) — already true since 1 broadcast/tick
        bid = sub.pivot_table(index="time", columns="ticker", values="bid1_price").ffill()
        ask = sub.pivot_table(index="time", columns="ticker", values="ask1_price").ffill()
        bid_q = sub.pivot_table(index="time", columns="ticker", values="bid1_qty").fillna(0)
        ask_q = sub.pivot_table(index="time", columns="ticker", values="ask1_qty").fillna(0)
        n = len(comps)
        bid = bid.dropna(subset=[etf]+comps)
        ask = ask.dropna(subset=[etf]+comps)
        common = bid.index.intersection(ask.index)
        bid, ask = bid.loc[common], ask.loc[common]
        bid_q, ask_q = bid_q.loc[common], ask_q.loc[common]
        # arb 1: sell ETF, buy basket: profit per ETF-unit = ETF_bid - sum(C_ask)/n
        sum_ask = ask[comps].sum(axis=1)
        sum_bid = bid[comps].sum(axis=1)
        profit_short_etf = bid[etf] - sum_ask / n
        profit_long_etf  = sum_bid / n - ask[etf]
        # max executable size per arb (ETF-share units): min of ETF L1 size and floor(min C L1 size)
        # if we want to short etf 1 unit, we need 1/n of each constituent; but shares are ints.
        # Simplification: trade in batches of n ETF shares = 1 unit of each constituent.
        size_short = pd.concat([bid_q[etf]/n, ask_q[comps].min(axis=1)], axis=1).min(axis=1).clip(lower=0)
        size_long  = pd.concat([ask_q[etf]/n, bid_q[comps].min(axis=1)], axis=1).min(axis=1).clip(lower=0)
        # convert profit-per-ETF-share to profit-per-batch: batch is n ETF + 1 of each = n*p
        # but pnl is per ETF-share so total = profit_per_share * batch_size_in_etf_shares
        gross_short = (profit_short_etf.clip(lower=0) * size_short * n).sum()
        gross_long  = (profit_long_etf.clip(lower=0) * size_long * n).sum()
        # count of arb-positive ticks
        n_short = (profit_short_etf > 0).sum()
        n_long  = (profit_long_etf > 0).sum()
        ticks = len(profit_short_etf)
        print(f"  {etf:6s} ticks={ticks:5d}  short>0:{n_short:5d} ({100*n_short/ticks:5.1f}%)  long>0:{n_long:5d} ({100*n_long/ticks:5.1f}%)")
        print(f"           median diff_short={profit_short_etf.median():+.2f}c  long={profit_long_etf.median():+.2f}c  | gross_short_pnl=${gross_short/100:.0f}  gross_long_pnl=${gross_long/100:.0f}")

for ex in ["ZSE","NYSE","HKEX","NASDAQ","TMX","LSE","Euronext","JPX"]:
    analyze(ex)
