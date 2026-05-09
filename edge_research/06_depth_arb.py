"""ETF arb using L1+L2+L3 depth, properly bounded."""
import pandas as pd, numpy as np

DATA = "/home/pitfa/Documents/algotrade2026/market_data"
ETF_DEF = {
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

def sweep(prices, qtys, q):
    """Cost to fill q shares walking levels in order. Returns (filled, cost). filled<q if depth ran out."""
    rem = q; cost = 0
    for p, qq in zip(prices, qtys):
        if rem <= 0: break
        take = min(rem, qq); cost += take * p; rem -= take
    return q - rem, cost  # filled=q-rem; cost only counts filled


def venue_arb_depth(ex, etf):
    comps = ETF_DEF[etf]
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv")
    df["ticker"] = df["instrument"].str.split("-",n=1).str[1]
    if etf not in df.ticker.unique() or not all(c in df.ticker.unique() for c in comps): return None
    needed = [etf]+comps
    sub = df[df.ticker.isin(needed)].copy()

    n = len(comps)
    pos = {t:0 for t in needed}
    cash = 10_000_000
    pnl = 0
    arb_count = 0
    msgs_per_round_trip = 1+n
    msgs_this_sec = 0; last_sec = None

    for t, g in sub.groupby("time"):
        sec = t // 1000
        if sec != last_sec:
            last_sec = sec; msgs_this_sec = 0
        if msgs_this_sec + msgs_per_round_trip > 500:
            continue
        # build per-instrument levels
        ask = {}; bid = {}
        for r in g.itertuples():
            tk = r.ticker
            ask[tk] = ([r.ask1_price, r.ask2_price, r.ask3_price],
                       [r.ask1_qty,   r.ask2_qty,   r.ask3_qty])
            bid[tk] = ([r.bid1_price, r.bid2_price, r.bid3_price],
                       [r.bid1_qty,   r.bid2_qty,   r.bid3_qty])
        if etf not in ask or any(c not in ask for c in comps): continue

        # try LONG-ETF and SHORT-ETF arbs
        for side in ("long","short"):
            # find best k
            best_k = 0; best_profit = 0
            # k_max bounded by position limits
            if side == "long":
                k_max_pos = min((2000 - pos[etf]) // n, *(pos[c] - (-200) for c in comps))
            else:
                k_max_pos = min((pos[etf] - (-200)) // n, *(2000 - pos[c] for c in comps))
            if k_max_pos <= 0: continue
            # binary search by profit at increasing k
            lo, hi = 1, min(k_max_pos, 200)
            ans = 0
            while lo <= hi:
                mid = (lo+hi)//2
                if side == "long":
                    f_etf, c_etf = sweep(ask[etf][0], ask[etf][1], n*mid)
                    if f_etf < n*mid: hi = mid-1; continue
                    rev = 0; ok = True
                    for c in comps:
                        f, cv = sweep(bid[c][0], bid[c][1], mid)
                        if f < mid: ok=False; break
                        rev += cv
                    if not ok: hi = mid-1; continue
                    profit = rev - c_etf
                else:
                    f_etf, c_etf = sweep(bid[etf][0], bid[etf][1], n*mid)
                    if f_etf < n*mid: hi = mid-1; continue
                    cost = 0; ok = True
                    for c in comps:
                        f, cv = sweep(ask[c][0], ask[c][1], mid)
                        if f < mid: ok=False; break
                        cost += cv
                    if not ok: hi = mid-1; continue
                    profit = c_etf - cost  # c_etf is sell rev, cost is buy cost
                if profit > 0:
                    ans = mid; lo = mid+1
                else:
                    hi = mid-1
            best_k = ans
            if best_k <= 0: continue
            # execute
            if side == "long":
                _, c_etf = sweep(ask[etf][0], ask[etf][1], n*best_k)
                rev = sum(sweep(bid[c][0], bid[c][1], best_k)[1] for c in comps)
                profit = rev - c_etf
                pos[etf] += n*best_k
                for c in comps: pos[c] -= best_k
                cash += -c_etf + rev
            else:
                _, c_etf_sell = sweep(bid[etf][0], bid[etf][1], n*best_k)
                cost = sum(sweep(ask[c][0], ask[c][1], best_k)[1] for c in comps)
                profit = c_etf_sell - cost
                pos[etf] -= n*best_k
                for c in comps: pos[c] += best_k
                cash += c_etf_sell - cost
            pnl += profit
            msgs_this_sec += msgs_per_round_trip
            arb_count += 1
    return pnl/100, arb_count, dict(pos), cash/100


print("Venue×ETF, depth-aware, position-respecting, rate-limited:\n")
total = 0
for etf in ETF_DEF:
    for ex in EXCHANGES_WITH_ETFS[etf]:
        r = venue_arb_depth(ex, etf)
        if r is None: continue
        pnl, n_arbs, pos, cash = r
        total += pnl
        print(f"{ex:9s} {etf:6s}  PnL=${pnl:>9.0f}  arbs={n_arbs:>4d}  pos={pos}")
print(f"\nTotal across venues over ~180s: ${total:,.0f}  → 30 min: ${total*30*60/180:,.0f}")
