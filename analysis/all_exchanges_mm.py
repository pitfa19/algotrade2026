"""All-exchanges MM characterization.

For each of the 10 exchanges, runs the same MM-signature pipeline and
emits one combined summary CSV. Then performs three cross-cutting
analyses:

  D. Per-(exchange, instrument) MM signature table.
  E. ETF arb gap across all venues that list each ETF — is the
     constituent-vs-ETF gap a ZSE-only artefact, or is it everywhere?
  F. Cross-venue divergence of the same instrument's MM mid — uncovers
     systematic latency-driven mispricings (e.g., CARD on NYSE vs ZSE).

Outputs:
  analysis/out/all_mm_summary.csv
  analysis/out/etf_arb_by_venue.csv
  analysis/out/cross_venue_mid_divergence.csv
  analysis/out/plots/9_clean_pct_by_venue.png
  analysis/out/plots/10_spread_by_venue.png
  analysis/out/plots/11_etf_arb_by_venue.png
  analysis/out/plots/12_cross_venue_divergence.png
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATA = Path(__file__).resolve().parents[1] / "market_data"
OUT = Path(__file__).resolve().parent / "out"
PLOTS = OUT / "plots"
PLOTS.mkdir(parents=True, exist_ok=True)

EXCHANGES = ["NYSE", "NASDAQ", "SSE", "JPX", "Euronext", "LSE",
             "HKEX", "NSE", "TMX", "ZSE"]

ETF_BASKETS = {
    "ETFA":  ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB":  ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}


def load_ob(ex: str) -> pd.DataFrame:
    df = pd.read_csv(DATA / f"{ex}_orderbooks.csv")
    df["instrument"] = df["instrument"].str.replace(f"{ex}-", "", regex=False)
    df["exchange"] = ex
    return df


def load_trades(ex: str) -> pd.DataFrame:
    df = pd.read_csv(DATA / f"{ex}_trades.csv")
    df["instrument"] = df["instrument"].str.replace(f"{ex}-", "", regex=False)
    df["exchange"] = ex
    return df


def signature_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bid_step_12"] = df["bid1_price"] - df["bid2_price"]
    out["ask_step_12"] = df["ask2_price"] - df["ask1_price"]
    out["spread"] = df["ask1_price"] - df["bid1_price"]
    out["mid"] = (df["ask1_price"] + df["bid1_price"]) / 2.0
    bid_qs = df[["bid1_qty", "bid2_qty", "bid3_qty"]]
    ask_qs = df[["ask1_qty", "ask2_qty", "ask3_qty"]]
    out["bid_dirty"] = (bid_qs % 50 != 0).any(axis=1) | (bid_qs == 0).any(axis=1)
    out["ask_dirty"] = (ask_qs % 50 != 0).any(axis=1) | (ask_qs == 0).any(axis=1)
    out["bid_stacked"] = (
        (df["bid1_qty"] == 50) & (df["bid2_qty"] == 100) & (df["bid3_qty"] == 150))
    out["ask_stacked"] = (
        (df["ask1_qty"] == 50) & (df["ask2_qty"] == 100) & (df["ask3_qty"] == 150))
    out["bid_depth"] = bid_qs.sum(axis=1)
    out["ask_depth"] = ask_qs.sum(axis=1)
    out["depth_imb"] = (
        (out["bid_depth"] - out["ask_depth"]) /
        (out["bid_depth"] + out["ask_depth"]).replace(0, np.nan))
    return out


# ─────────────────────────────────────────────────────────────────────────
# Load every exchange.
# ─────────────────────────────────────────────────────────────────────────
print("Loading all exchanges...")
all_ob = pd.concat([load_ob(ex) for ex in EXCHANGES], ignore_index=True)
all_tr = pd.concat([load_trades(ex) for ex in EXCHANGES], ignore_index=True)
feat = signature_features(all_ob)
print(f"  {len(all_ob):,} ob rows, {len(all_tr):,} trade rows, "
      f"{feat['exchange'].nunique()} venues, "
      f"{feat['instrument'].nunique()} unique instruments")


# ─────────────────────────────────────────────────────────────────────────
# D. Per-(exchange, instrument) MM signature.
# ─────────────────────────────────────────────────────────────────────────
def horizon_corr(sub: pd.DataFrame, h: int = 5) -> float:
    s = sub.sort_values("time").copy()
    s["mid_fwd"] = s["mid"].shift(-h)
    s["dmid"] = s["mid_fwd"] - s["mid"]
    valid = s.dropna(subset=["dmid", "depth_imb"])
    if len(valid) < 20 or valid["depth_imb"].std() == 0:
        return np.nan
    return float(valid["depth_imb"].corr(valid["dmid"]))


rows = []
for (ex, inst), sub in feat.groupby(["exchange", "instrument"], observed=True):
    rows.append({
        "exchange": ex,
        "instrument": inst,
        "n_snaps": len(sub),
        "spread_med_c": int(sub["spread"].median()),
        "spread_max_c": int(sub["spread"].max()),
        "tick_step": int(sub["bid_step_12"].mode().iloc[0]),
        "bid_clean_pct": round(100 * (~sub["bid_dirty"]).mean(), 1),
        "ask_clean_pct": round(100 * (~sub["ask_dirty"]).mean(), 1),
        "bid_stacked_pct": round(100 * sub["bid_stacked"].mean(), 1),
        "ask_stacked_pct": round(100 * sub["ask_stacked"].mean(), 1),
        "mid_mean": round(float(sub["mid"].mean()), 1),
        "mid_std": round(float(sub["mid"].std()), 1),
        "imb_corr_h5": round(horizon_corr(sub, 5), 3),
    })
sig = pd.DataFrame(rows).sort_values(["exchange", "instrument"]).reset_index(drop=True)
sig.to_csv(OUT / "all_mm_summary.csv", index=False)
print("\n=== D. Per-(exchange, instrument) MM signature — first 20 rows ===")
with pd.option_context("display.width", 200):
    print(sig.head(20).to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────
# E. ETF arb gap across all venues.
# ─────────────────────────────────────────────────────────────────────────
all_mid_long = feat[["time", "exchange", "instrument", "mid"]].copy()

etf_rows = []
for etf, basket in ETF_BASKETS.items():
    for ex in EXCHANGES:
        sub_etf = all_mid_long[(all_mid_long["exchange"] == ex) &
                               (all_mid_long["instrument"] == etf)]
        if sub_etf.empty:
            continue
        # The basket fair must come from the SAME exchange's mids.
        sub_const = all_mid_long[(all_mid_long["exchange"] == ex) &
                                 (all_mid_long["instrument"].isin(basket))]
        if sub_const.empty:
            continue
        # pivot constituents to columns, ffill forward in time.
        wide = sub_const.pivot_table(index="time", columns="instrument",
                                     values="mid", aggfunc="last").sort_index().ffill()
        present = [c for c in basket if c in wide.columns]
        if len(present) < max(2, len(basket) - 1):
            # need at least the full basket (allow 1 missing for big baskets)
            continue
        fair = wide[present].mean(axis=1)
        etf_series = sub_etf.set_index("time")["mid"].sort_index()
        aligned = pd.concat([etf_series.rename("etf"), fair.rename("fair")],
                            axis=1).ffill().dropna()
        if aligned.empty:
            continue
        diff = aligned["etf"] - aligned["fair"]
        etf_rows.append({
            "etf": etf,
            "exchange": ex,
            "n_basket_present": len(present),
            "n_basket_required": len(basket),
            "n_aligned": len(aligned),
            "diff_mean_c": round(float(diff.mean()), 2),
            "diff_std_c":  round(float(diff.std()),  2),
            "diff_max_abs_c": round(float(diff.abs().max()), 2),
            "abs_gap_>10c_%": round(100 * (diff.abs() > 10).mean(), 1),
        })
etf_df = pd.DataFrame(etf_rows).sort_values(["etf", "exchange"]).reset_index(drop=True)
etf_df.to_csv(OUT / "etf_arb_by_venue.csv", index=False)
print("\n=== E. ETF gap (ETF mid – constituent mean) per venue ===")
print(etf_df.to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────
# F. Cross-venue mid divergence for the SAME instrument.
# ─────────────────────────────────────────────────────────────────────────
# For each instrument, compute pairwise mid difference between every pair
# of exchanges that list it, summarize.
mids_wide = (
    all_mid_long
    .pivot_table(index="time", columns=["instrument", "exchange"],
                 values="mid", aggfunc="last")
    .sort_index().ffill()
)

cv_rows = []
for inst in sorted(set(all_mid_long["instrument"])):
    cols = [c for c in mids_wide.columns if c[0] == inst]
    if len(cols) < 2:
        continue
    sub = mids_wide[cols].dropna(how="all")
    venues = [c[1] for c in cols]
    for i in range(len(venues)):
        for j in range(i + 1, len(venues)):
            a, b = venues[i], venues[j]
            d = (sub[(inst, a)] - sub[(inst, b)]).dropna()
            if len(d) < 20:
                continue
            cv_rows.append({
                "instrument": inst,
                "venue_a": a,
                "venue_b": b,
                "n": len(d),
                "diff_mean_c": round(float(d.mean()), 2),
                "diff_std_c":  round(float(d.std()),  2),
                "diff_max_abs_c": round(float(d.abs().max()), 2),
                "abs_diff_>spread_%": round(100 * (d.abs() > 4).mean(), 1),
            })
cv_df = pd.DataFrame(cv_rows).sort_values("diff_max_abs_c", ascending=False).reset_index(drop=True)
cv_df.to_csv(OUT / "cross_venue_mid_divergence.csv", index=False)
print(f"\n=== F. Cross-venue mid divergence — top 25 widest pairs ===")
print(cv_df.head(25).to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────
# Plots.
# ─────────────────────────────────────────────────────────────────────────

# 9. MM-clean % heatmap (instrument × venue).
clean_avg = (sig.assign(clean=lambda d: (d["bid_clean_pct"] + d["ask_clean_pct"]) / 2)
                .pivot(index="instrument", columns="exchange", values="clean"))
clean_avg = clean_avg.reindex(columns=EXCHANGES)
fig, ax = plt.subplots(figsize=(11, 9))
im = ax.imshow(clean_avg.values, aspect="auto", cmap="RdYlGn", vmin=0, vmax=100)
ax.set_xticks(range(len(EXCHANGES)), EXCHANGES, rotation=45, ha="right")
ax.set_yticks(range(len(clean_avg.index)), clean_avg.index)
for i in range(clean_avg.shape[0]):
    for j in range(clean_avg.shape[1]):
        v = clean_avg.values[i, j]
        if not np.isnan(v):
            ax.text(j, i, f"{v:.0f}", ha="center", va="center",
                    fontsize=8, color="black")
fig.colorbar(im, ax=ax, label="MM-clean % (avg of bid+ask)")
ax.set_title("MM-clean fraction by (instrument, venue) — "
             "where the visible top-3 is purely the MM ladder")
fig.tight_layout()
fig.savefig(PLOTS / "9_clean_pct_by_venue.png", dpi=120)
plt.close(fig)

# 10. Spread heatmap (median spread).
sp_med = sig.pivot(index="instrument", columns="exchange", values="spread_med_c")
sp_med = sp_med.reindex(columns=EXCHANGES)
fig, ax = plt.subplots(figsize=(11, 9))
im = ax.imshow(sp_med.values, aspect="auto", cmap="viridis", vmin=1, vmax=10)
ax.set_xticks(range(len(EXCHANGES)), EXCHANGES, rotation=45, ha="right")
ax.set_yticks(range(len(sp_med.index)), sp_med.index)
for i in range(sp_med.shape[0]):
    for j in range(sp_med.shape[1]):
        v = sp_med.values[i, j]
        if not np.isnan(v):
            ax.text(j, i, f"{int(v)}", ha="center", va="center",
                    fontsize=8, color="white")
fig.colorbar(im, ax=ax, label="median MM spread (cents)")
ax.set_title("Median MM spread by (instrument, venue)")
fig.tight_layout()
fig.savefig(PLOTS / "10_spread_by_venue.png", dpi=120)
plt.close(fig)

# 11. ETF gap by venue: bar chart per ETF.
fig, axes = plt.subplots(len(ETF_BASKETS), 1, figsize=(10, 12), sharex=False)
for ax, etf in zip(axes, ETF_BASKETS):
    sub = etf_df[etf_df["etf"] == etf]
    if sub.empty:
        ax.set_visible(False); continue
    ax.bar(sub["exchange"], sub["diff_std_c"], color="#264653",
           label="std of gap (¢)")
    ax.bar(sub["exchange"], sub["diff_mean_c"], color="#e76f51",
           alpha=0.6, label="mean gap (¢)")
    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_title(
        f"{etf}: ETF gap statistics by venue  "
        f"(max |Δ| range = {sub['diff_max_abs_c'].min():.0f}–"
        f"{sub['diff_max_abs_c'].max():.0f}¢)")
    ax.set_ylabel("cents")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
fig.suptitle("ETF mid – constituent mean: gap dispersion across all listing venues",
             y=1.0)
fig.tight_layout()
fig.savefig(PLOTS / "11_etf_arb_by_venue.png", dpi=120)
plt.close(fig)

# 12. Cross-venue divergence: top N (instrument, pair) by max |Δ|.
top = cv_df.head(20).copy()
top["label"] = top["instrument"] + "  " + top["venue_a"] + "↔" + top["venue_b"]
fig, ax = plt.subplots(figsize=(11, 9))
y = np.arange(len(top))
ax.barh(y, top["diff_max_abs_c"], color="#e63946", label="max |Δ|")
ax.barh(y, top["diff_std_c"], color="#1d3557", alpha=0.7, label="std Δ")
ax.set_yticks(y, top["label"])
ax.invert_yaxis()
ax.set_xlabel("cents")
ax.set_title("Top 20 cross-venue MM-mid divergences "
             "(same instrument, different exchange)")
ax.legend()
ax.grid(axis="x", alpha=0.3)
fig.tight_layout()
fig.savefig(PLOTS / "12_cross_venue_divergence.png", dpi=120)
plt.close(fig)

print(f"\nWrote new artefacts under {OUT}")
print(" ", (OUT / "all_mm_summary.csv").name,
      f"({len(sig)} (exchange,instrument) rows)")
print(" ", (OUT / "etf_arb_by_venue.csv").name)
print(" ", (OUT / "cross_venue_mid_divergence.csv").name)
for p in sorted(PLOTS.glob("9_*.png")) + sorted(PLOTS.glob("1[0-9]_*.png")):
    print(" ", p.relative_to(OUT))
