"""Build a *realistic* ETF arb simulator on a single venue (ZSE first).

For every tick where 6 * ETFA_ask < sum(constituent_bids) (LONG side):
  - take ETF ask up to min(L1_qty, basket_constraint)
  - simultaneously sell each constituent at its bid up to its L1 qty
  - profit = sum(bids) - 6 * ETFA_ask, scaled by batch size in ETF shares
Limit per-tick batch size by min(ETF L1 qty / N, min over constituents of L1 qty).

We assume:
  - We see the L1 prints in the tick.
  - We submit IOCs against L1 instantly (colocated, <1ms latency).
  - Our IOCs hit L1 quote BEFORE the next tick's update.
  - Position cap: -200..+2000 per instrument per exchange.
  - Cash floor -50k per exchange. Start 100k cash.
  - Rate limit: 500 msgs/sec per exchange. Each arb uses 1+N=7 IOC msgs (ETFA arb).

We compute realized PnL and simulate over the entire ZSE order book stream.
"""
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

POS_FLOOR, POS_CEIL = -200, 2000
CASH_FLOOR = -5_000_000  # cents (-$50k)
START_CASH = 10_000_000  # cents
RATE_LIMIT_PER_S = 500


def venue_arb(ex: str, etf: str):
    """Simulate single-venue ETF arb on (ex, etf), returning per-tick PnL."""
    comps = ETF_DEF[etf]
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv")
    df["ticker"] = df["instrument"].str.split("-",n=1).str[1]
    if etf not in df.ticker.unique(): return 0
    if not all(c in df.ticker.unique() for c in comps): return 0
    needed = [etf]+comps
    sub = df[df.ticker.isin(needed)][["time","ticker","bid1_price","bid1_qty","ask1_price","ask1_qty"]]
    times = np.sort(sub.time.unique())
    bid_p = sub.pivot_table(index="time", columns="ticker", values="bid1_price")
    ask_p = sub.pivot_table(index="time", columns="ticker", values="ask1_price")
    bid_q = sub.pivot_table(index="time", columns="ticker", values="bid1_qty")
    ask_q = sub.pivot_table(index="time", columns="ticker", values="ask1_qty")
    bid_p, ask_p, bid_q, ask_q = (x.ffill() for x in (bid_p, ask_p, bid_q, ask_q))
    common = bid_p.dropna(subset=needed).index.intersection(ask_p.dropna(subset=needed).index)
    bid_p, ask_p, bid_q, ask_q = (x.loc[common] for x in (bid_p, ask_p, bid_q, ask_q))
    n = len(comps)
    pos = {t:0 for t in needed}
    cash = START_CASH
    pnl = 0
    msgs_sent = 0  # for rate limit accounting
    last_sec = None
    msgs_this_sec = 0
    msgs_per_round_trip = 1+n  # one IOC per leg
    arb_count = 0
    for t in common:
        sec = t // 1000
        if sec != last_sec:
            last_sec = sec
            msgs_this_sec = 0
        if msgs_this_sec + msgs_per_round_trip > RATE_LIMIT_PER_S:
            continue
        # LONG ETF side: buy ETF, sell basket
        sb = sum(bid_p.loc[t, c] for c in comps)
        edge_long  = sb / n - ask_p.loc[t, etf]   # cents per ETF share
        # SHORT ETF side: sell ETF, buy basket
        sa = sum(ask_p.loc[t, c] for c in comps)
        edge_short = bid_p.loc[t, etf] - sa / n   # cents per ETF share

        # batch_size_in_ETF_shares is multiple of n
        # constituent constraint: each comp must accept the same number of shares = batch / n
        # so max batch_n_units = min over comps of L1_qty; ETF needs n*batch_n_units shares
        if edge_long > 0:
            # need to buy n*k ETF, sell k of each constituent
            etf_avail = ask_q.loc[t, etf]
            comp_avail = min(bid_q.loc[t, c] for c in comps)
            k = int(min(etf_avail // n, comp_avail))
            # position limits: ETF buy -> +n*k (must not exceed CEIL); each comp sold -> -k (must not go below FLOOR)
            k = min(k, (POS_CEIL - pos[etf]) // n)
            for c in comps:
                k = min(k, pos[c] - POS_FLOOR)
            # cash floor: buying ETF costs n*k*ask; selling comps yields k*bid_each
            etf_cost = n * k * ask_p.loc[t, etf]
            basket_rev = k * sb
            net_cash = -etf_cost + basket_rev
            if cash + net_cash < CASH_FLOOR:
                # find max k such that cash >= floor
                # cash + net_cash >= floor
                # net_cash_per_k = -n*ask_etf + sum_bid_comps
                pkc = -n*ask_p.loc[t,etf] + sb
                if pkc < 0:
                    k = max(0, (cash - CASH_FLOOR) // (-pkc))
            if k > 0:
                pos[etf] += n*k
                for c in comps: pos[c] -= k
                cash += -n*k*ask_p.loc[t, etf] + k*sb
                profit = k * edge_long * n  # since edge_long is per-ETF-share
                # Wait: edge_long is per ETF share. We bought n*k ETF shares.
                # So profit = (sb/n - ask_etf) * n * k = (sb - n*ask_etf) * k. correct
                # Actually re-derive: net cash flow = -n*k*ask_etf + k*sb = k*(sb - n*ask_etf).
                # Since position is +n*k ETF and -k of each comp, the basket completely hedges the ETF.
                # PnL realized when we close the position. But if we close at *future* prices...
                # We're treating same-instant cancellation: pretend the position is immediately closed
                # at current bids/asks (no, that creates a loss).
                # Better: keep the position. At end, mark to mid; if hedge is perfect, M2M=0 for any
                # parallel move and the realized cash flow is the profit.
                # k*(sb - n*ask_etf) = realized cash flow. = profit if ETF is "worth" its NAV later.
                pnl += k*(sb - n*ask_p.loc[t,etf])
                msgs_this_sec += msgs_per_round_trip
                arb_count += 1
        if edge_short > 0:
            etf_avail = bid_q.loc[t, etf]
            comp_avail = min(ask_q.loc[t, c] for c in comps)
            k = int(min(etf_avail // n, comp_avail))
            # ETF -n*k must >= FLOOR; comps +k must <= CEIL
            k = min(k, (pos[etf] - POS_FLOOR) // n)
            for c in comps:
                k = min(k, POS_CEIL - pos[c])
            pkc = n*bid_p.loc[t,etf] - sa
            net_cash = k*pkc
            if cash + net_cash < CASH_FLOOR and pkc<0:
                k = max(0, (cash - CASH_FLOOR) // (-pkc))
            if k > 0:
                pos[etf] -= n*k
                for c in comps: pos[c] += k
                cash += k*pkc
                pnl += k*pkc
                msgs_this_sec += msgs_per_round_trip
                arb_count += 1
    return pnl, arb_count, dict(pos), cash

results = []
for etf in ETF_DEF:
    for ex in EXCHANGES_WITH_ETFS[etf]:
        try:
            r = venue_arb(ex, etf)
            if r==0: continue
            pnl, n_arbs, pos, cash = r
            results.append((ex, etf, pnl/100, n_arbs))
            print(f"{ex:9s} {etf:6s}: PnL=${pnl/100:>9.0f}  arbs={n_arbs:>5d}  end_pos={pos}")
        except Exception as e:
            print(f"{ex} {etf}: ERROR {e}")

print()
print("=== TOTAL across venues + ETFs ===")
total = sum(r[2] for r in results)
print(f"Total PnL: ${total:,.0f}  over ~{180:.0f}s of data")
print(f"Extrapolated to 30-min round (×{30*60/180:.1f}): ${total*30*60/180:,.0f}")
