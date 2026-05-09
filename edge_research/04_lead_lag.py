"""Lead-lag between exchanges.
For CARD/SIMP — pick a pair of exchanges, compute cross-correlation of mid
returns at lags from -50 to +50 ticks (each ~20-100ms). The peak-correlation
lag tells us which exchange leads.

Also: when a large aggressive trade hits venue A, does venue B follow within
N ticks?
"""
import pandas as pd, numpy as np

DATA = "/home/pitfa/Documents/algotrade2026/market_data"
EXCHANGES = ["NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"]

def mid_series(ex, ticker):
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv")
    sub = df[df.instrument == f"{ex}-{ticker}"].copy()
    sub["mid"] = (sub.bid1_price + sub.ask1_price) / 2
    return sub.set_index("time")["mid"].sort_index()

# Build a 100-ms grid for cross-venue analysis
GRID_MS = 100
def aligned_grid(ticker):
    series = {ex: mid_series(ex, ticker) for ex in EXCHANGES}
    tmin = max(s.index.min() for s in series.values())
    tmax = min(s.index.max() for s in series.values())
    grid_t = np.arange(tmin, tmax, GRID_MS)
    out = pd.DataFrame(index=grid_t)
    for ex in EXCHANGES:
        s = series[ex]
        out[ex] = s.reindex(grid_t, method="ffill")
    return out.dropna()

for ticker in ["CARD","SIMP"]:
    print(f"\n=== {ticker} cross-venue lead-lag ===")
    grid = aligned_grid(ticker)
    print(f"grid ticks: {len(grid)} ({GRID_MS}ms cadence)")
    rets = grid.diff().dropna()
    leaders = ["NYSE","HKEX","ZSE","TMX","NASDAQ"]
    for L in leaders:
        for F in EXCHANGES:
            if F==L: continue
            best = (0,0)
            for lag in range(-5,6):
                if lag>=0:
                    c = rets[L].iloc[:-lag if lag else None].corr(rets[F].iloc[lag:])
                else:
                    c = rets[L].iloc[-lag:].corr(rets[F].iloc[:lag])
                if abs(c)>abs(best[1]): best=(lag,c)
            if abs(best[1])>0.05:
                pos = "leads" if best[0]>0 else ("lags" if best[0]<0 else "synced")
                print(f"  {L:>9s} vs {F:<9s}: peak lag={best[0]:+d} corr={best[1]:+.2f} ({L} {pos} {F})")
