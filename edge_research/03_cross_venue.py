"""Cross-venue dispersion and convergence analysis.
For CARD and SIMP (on all 10 exchanges), align mids on a common time
grid and study:
  - cross-venue dispersion (spread between max and min mid)
  - mean reversion: does dispersion shrink over time?
  - correlation between venues
"""
import pandas as pd, numpy as np

DATA = "/home/pitfa/Documents/algotrade2026/market_data"
EXCHANGES = ["NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"]

def mid_series(ex, ticker):
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv")
    sub = df[df.instrument == f"{ex}-{ticker}"].copy()
    sub["mid"] = (sub.bid1_price + sub.ask1_price) / 2
    return sub.set_index("time")["mid"].sort_index()

for ticker in ["CARD","SIMP"]:
    print(f"\n=== {ticker} mid mid across venues ===")
    series = {ex: mid_series(ex, ticker) for ex in EXCHANGES}
    # Build a unified time index (every 100 ms across all venues' times)
    all_times = sorted(set().union(*[set(s.index) for s in series.values()]))
    grid = pd.DataFrame(index=all_times)
    for ex in EXCHANGES:
        grid[ex] = series[ex].reindex(all_times).ffill()
    grid = grid.dropna()
    print(f"aligned ticks: {len(grid)}")
    print("\ncorr matrix:")
    print(grid.corr().round(2))
    # Per tick, dispersion = max - min
    disp = grid.max(axis=1) - grid.min(axis=1)
    print(f"\ncross-venue dispersion (max-min) cents:")
    print(disp.describe(percentiles=[0.05,0.5,0.95]).round(0))
    # mean revert: pair-wise z-score
    pair = grid["NYSE"] - grid["HKEX"]
    print(f"\nNYSE - HKEX: mean={pair.mean():.0f}c std={pair.std():.0f}c")
    # autocorr of |spread - mean|
    deviation = pair - pair.mean()
    print(f"NYSE-HKEX 1-tick autocorr (mean revert if <1): {deviation.autocorr(lag=1):.3f}")
    print(f"NYSE-HKEX 10-tick autocorr: {deviation.autocorr(lag=10):.3f}")
    print(f"NYSE-HKEX 100-tick autocorr: {deviation.autocorr(lag=100):.3f}")

# Does dispersion track time? Is it a random walk?
print("\n=== Random walk check: mid first-differences ===")
for ex in ["NYSE","ZSE","HKEX"]:
    s = mid_series(ex, "CARD")
    d = s.diff().dropna()
    print(f"{ex} CARD: dmid/tick mean={d.mean():.2f}c std={d.std():.2f}c autocorr={d.autocorr(1):+.3f}")
