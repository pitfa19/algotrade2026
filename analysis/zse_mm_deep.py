"""ZSE deep MM analysis.

Adds three things on top of the basic extractor:

  A. Stacked vs flat hypothesis test
     ─ Group trade rows by (time, price, active_order_id) — a single market
       order sweeping a single price level produces one row per resting
       order it consumed. The SUM of those quantities at one price tells us
       how much liquidity that price held in aggregate.
     ─ For prices that we know are *outside* the visible top-3 (i.e. MM
       L4 / L5 territory), if the per-price aggregate often exceeds 50
       shares, the MM is stacking. If it caps near 50, MM is flat.

  B. Inventory skew per instrument over time
     ─ Define skew = (ask1 - mid) - (mid - bid1)  in cents,
       where mid = (bid1 + ask1)/2.   skew > 0  ⇒ ask is further from mid
       than the bid  ⇒ MM cheaper to *sell to* than buy from
       ⇒ MM is short and wants to buy back  ⇒ price likely to mean-revert UP.
       skew < 0 is the opposite.
     ─ We then test whether skew at time t predicts the change in mid at
       t+Δ — that's the actual tradeable signal.

  C. ETF fair-value cross-check (ETFA3, ETFB3, ETFSH)
     ─ Compute the constituent-implied fair value at each timestamp from
       the ZSE order books of the constituents. Compare to the ETF's mid.
     ─ Persistent gap = arb signal.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from zse_mm_extract import OUT, load_ob, load_trades

EX = "ZSE"
PLOTS = OUT / "plots"
PLOTS.mkdir(exist_ok=True)

ETF_BASKETS = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}

ob = load_ob(EX)
tr = load_trades(EX)
ob = ob.sort_values(["instrument", "time"]).reset_index(drop=True)
tr = tr.sort_values(["instrument", "time"]).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────
# A. Stacked vs flat
# ─────────────────────────────────────────────────────────────────────────
# Sum trade quantities by (time, instrument, price, active_order_id) — i.e.
# the total liquidity a single incoming market order consumed at one price.
agg = (
    tr.groupby(["time", "instrument", "price", "active_order_id"], as_index=False)
      .agg(qty_at_price=("quantity", "sum"),
           n_passive=("passive_order_id", "nunique"))
)

# Tag each aggregated print with the prevailing top-3 *just before* it.
ob_for_merge = ob.copy()
ob_for_merge = ob_for_merge.rename(columns={"time": "ob_time"})
agg = agg.sort_values(["instrument", "time"]).reset_index(drop=True)
ob_for_merge = ob_for_merge.sort_values(["instrument", "ob_time"]).reset_index(drop=True)

merged_parts = []
for inst in agg["instrument"].unique():
    a = agg[agg["instrument"] == inst]
    o = ob_for_merge[ob_for_merge["instrument"] == inst]
    if o.empty:
        continue
    m = pd.merge_asof(a.sort_values("time"),
                      o.sort_values("ob_time"),
                      left_on="time", right_on="ob_time",
                      direction="backward")
    merged_parts.append(m)
agg_with_book = pd.concat(merged_parts, ignore_index=True).dropna(subset=["bid1_price"])

# Classify which level (if any) the print came from in the visible book.
def classify_level(row):
    p = row["price"]
    if p == row["ask1_price"]: return "ask1"
    if p == row["ask2_price"]: return "ask2"
    if p == row["ask3_price"]: return "ask3"
    if p == row["bid1_price"]: return "bid1"
    if p == row["bid2_price"]: return "bid2"
    if p == row["bid3_price"]: return "bid3"
    if p > row["ask3_price"]:  return "above_ask3"
    if p < row["bid3_price"]:  return "below_bid3"
    return "between"

agg_with_book["level"] = agg_with_book.apply(classify_level, axis=1)

# Distribution of qty_at_price by level.
level_qty = (
    agg_with_book.groupby("level")["qty_at_price"]
      .describe(percentiles=[0.5, 0.9, 0.95, 0.99])
      .round(1)
)
print("\n=== A. Aggregate qty consumed at a single price, grouped by which level ===")
print("(if MM stacks, levels deeper out should have larger qty per price)")
print(level_qty[["count", "mean", "50%", "90%", "95%", "99%", "max"]])

# Plot: qty per price, by level
fig, ax = plt.subplots(figsize=(11, 6))
order = ["below_bid3", "bid3", "bid2", "bid1", "ask1", "ask2", "ask3", "above_ask3"]
data = [agg_with_book.loc[agg_with_book["level"] == lv, "qty_at_price"].to_numpy()
        for lv in order]
data = [d for d in data if len(d)]
labels = [lv for lv, d in zip(order, [agg_with_book.loc[agg_with_book["level"] == lv, "qty_at_price"]
                                       for lv in order]) if len(d)]
bp = ax.boxplot(data, tick_labels=labels, showfliers=False, patch_artist=True)
colors = {"bid": "#1d3557", "ask": "#e63946", "bel": "#a8dadc", "abo": "#fbb4ae"}
for patch, lv in zip(bp["boxes"], labels):
    key = "bel" if "below" in lv else "abo" if "above" in lv else ("bid" if "bid" in lv else "ask")
    patch.set_facecolor(colors[key]); patch.set_alpha(0.7)
ax.axhline(50, color="grey", linestyle="--", label="50 sh (one MM lot)")
ax.axhline(100, color="grey", linestyle=":", alpha=0.6, label="100 sh (two lots)")
ax.set_ylabel("Aggregate quantity consumed at one price by one market order")
ax.set_title(f"{EX}: per-price liquidity by orderbook level "
             "(stacked-vs-flat MM disambiguation)")
ax.legend()
ax.grid(axis="y", alpha=0.3)
fig.tight_layout()
fig.savefig(PLOTS / "6_qty_per_price_by_level.png", dpi=120)
plt.close(fig)


# ─────────────────────────────────────────────────────────────────────────
# B. Inventory skew → predict next-mid move
# ─────────────────────────────────────────────────────────────────────────
sk = ob.copy()
sk["mid"] = (sk["bid1_price"] + sk["ask1_price"]) / 2.0
sk["spread"] = sk["ask1_price"] - sk["bid1_price"]
# skew >0  ⇒ ask further than bid from mid (mid sits closer to bid)
sk["skew"] = (sk["ask1_price"] - sk["mid"]) - (sk["mid"] - sk["bid1_price"])
# also: depth-weighted skew using top-3 (per-level qty, not cumulative —
# both interpretations give the same imbalance ordering since they only
# differ by a constant additive factor).
sk["bid_depth"] = sk["bid1_qty"] + sk["bid2_qty"] + sk["bid3_qty"]
sk["ask_depth"] = sk["ask1_qty"] + sk["ask2_qty"] + sk["ask3_qty"]
sk["depth_imb"] = (sk["bid_depth"] - sk["ask_depth"]) / (sk["bid_depth"] + sk["ask_depth"])

# Per-instrument: future return vs current skew
horizon = 5  # snapshots ahead (≈500ms each)
sk["mid_fwd"] = sk.groupby("instrument")["mid"].shift(-horizon)
sk["dmid"] = sk["mid_fwd"] - sk["mid"]
sk_clean = sk.dropna(subset=["dmid"]).copy()

corr_rows = []
for inst, sub in sk_clean.groupby("instrument", observed=True):
    if len(sub) < 20:
        continue
    corr_skew = sub["skew"].corr(sub["dmid"])
    corr_imb = sub["depth_imb"].corr(sub["dmid"])
    corr_rows.append({
        "instrument": inst,
        "n": len(sub),
        "corr(skew, dmid_+5)": round(corr_skew, 3),
        "corr(depth_imb, dmid_+5)": round(corr_imb, 3),
        "skew_std": round(float(sub["skew"].std()), 3),
        "imb_std": round(float(sub["depth_imb"].std()), 3),
    })
corr_df = pd.DataFrame(corr_rows).sort_values("corr(depth_imb, dmid_+5)",
                                              ascending=False)
print("\n=== B. Inventory-skew → next-mid correlation (horizon = 5 snapshots ≈ 500 ms) ===")
print(corr_df.to_string(index=False))
corr_df.to_csv(OUT / f"{EX}_skew_corr.csv", index=False)

# Plot: skew correlation per instrument
fig, ax = plt.subplots(figsize=(10, 7))
inst_sorted = corr_df.sort_values("corr(depth_imb, dmid_+5)")
y = np.arange(len(inst_sorted))
ax.barh(y - 0.2, inst_sorted["corr(depth_imb, dmid_+5)"], height=0.4,
        label="depth imbalance → +5 mid move", color="#264653")
ax.barh(y + 0.2, inst_sorted["corr(skew, dmid_+5)"], height=0.4,
        label="bid/ask skew → +5 mid move", color="#e9c46a")
ax.axvline(0, color="black", linewidth=0.6)
ax.set_yticks(y, inst_sorted["instrument"])
ax.set_xlabel("Pearson correlation")
ax.set_title(f"{EX}: do current book imbalance / skew predict the next mid move?")
ax.legend()
ax.grid(axis="x", alpha=0.3)
fig.tight_layout()
fig.savefig(PLOTS / "7_skew_predictivity.png", dpi=120)
plt.close(fig)


# ─────────────────────────────────────────────────────────────────────────
# C. ETF fair-value cross-check
# ─────────────────────────────────────────────────────────────────────────
mids = sk[["time", "instrument", "mid"]].copy()
mids_w = mids.pivot_table(index="time", columns="instrument",
                          values="mid", aggfunc="last").sort_index().ffill()

etf_rows = []
fig, axes = plt.subplots(len(ETF_BASKETS), 1, figsize=(12, 14), sharex=True)
for ax, (etf, basket) in zip(axes, ETF_BASKETS.items()):
    if etf not in mids_w.columns:
        ax.set_visible(False); continue
    have = [c for c in basket if c in mids_w.columns]
    if not have:
        ax.set_visible(False); continue
    fair = mids_w[have].mean(axis=1)
    etf_mid = mids_w[etf]
    diff = etf_mid - fair
    aligned = pd.DataFrame({"etf_mid": etf_mid, "fair": fair, "diff": diff}).dropna()
    if aligned.empty:
        ax.set_visible(False); continue
    etf_rows.append({
        "etf": etf,
        "n_basket_present": len(have),
        "diff_mean_c": round(float(aligned["diff"].mean()), 2),
        "diff_std_c":  round(float(aligned["diff"].std()),  2),
        "diff_max_abs_c": round(float(aligned["diff"].abs().max()), 2),
        "abs_diff_>10c_%": round(100 * (aligned["diff"].abs() > 10).mean(), 1),
    })
    ax.plot(aligned.index, aligned["etf_mid"], color="#1d3557", label=f"{etf} mid")
    ax.plot(aligned.index, aligned["fair"], color="#e76f51",
            label=f"basket fair  (mean of {','.join(have)})")
    ax.set_ylabel("price (¢)")
    ax.set_title(f"{etf}: ETF mid vs constituent-implied fair value  "
                 f"(mean Δ = {aligned['diff'].mean():+.1f}¢, "
                 f"std = {aligned['diff'].std():.1f}¢, "
                 f"max |Δ| = {aligned['diff'].abs().max():.0f}¢)")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(alpha=0.3)
axes[-1].set_xlabel("time (ms)")
fig.suptitle(f"{EX}: ETF arb candidates — gap between ETF mid and basket fair", y=1.0)
fig.tight_layout()
fig.savefig(PLOTS / "8_etf_arb.png", dpi=120)
plt.close(fig)

etf_df = pd.DataFrame(etf_rows)
print("\n=== C. ETF fair-value gap statistics ===")
print(etf_df.to_string(index=False))
etf_df.to_csv(OUT / f"{EX}_etf_arb.csv", index=False)

print(f"\nWrote new plots to {PLOTS}/")
for p in sorted(PLOTS.glob("*.png"))[5:]:
    print(" ", p.name)
