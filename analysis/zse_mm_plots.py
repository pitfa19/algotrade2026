"""Charts for ZSE MM ladder analysis.

Generates:
  1. mm_clean_pct.png   — per-instrument fraction of snapshots whose top-3 is
     "clean" (MM-only — qty all multiples of 50, no team residuals).
  2. spread_box.png     — per-instrument top-of-book spread distribution.
  3. book_timeseries.png — for the cleanest instrument, plot bid/ask prices
     and trade prints over time.
  4. trades_outside_hist.png — distribution (in ticks) of trade prints that
     landed outside the visible top-3, evidence of MM levels 4 and 5.
  5. stacked_signature.png  — for one snapshot, draw the 50/100/150 ladder.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from zse_mm_extract import (
    OUT, load_ob, load_trades, signature_features, trades_outside_top3,
)

EX = "ZSE"
PLOTS = OUT / "plots"
PLOTS.mkdir(exist_ok=True)

ob = load_ob(EX)
tr = load_trades(EX)
feat = signature_features(ob)


# ── 1. MM-clean fraction per instrument ───────────────────────────────────
clean = (
    feat.groupby("instrument", observed=True)
    .agg(bid_clean=("bid_dirty", lambda s: 100 * (~s).mean()),
         ask_clean=("ask_dirty", lambda s: 100 * (~s).mean()))
    .sort_values("bid_clean", ascending=True)
)
fig, ax = plt.subplots(figsize=(10, 8))
y = np.arange(len(clean))
ax.barh(y - 0.2, clean["bid_clean"], height=0.4, label="bid clean %", color="#2a9d8f")
ax.barh(y + 0.2, clean["ask_clean"], height=0.4, label="ask clean %", color="#e76f51")
ax.set_yticks(y, clean.index)
ax.set_xlabel("% of snapshots with MM-only top-3 (qty all multiples of 50)")
ax.set_title(f"{EX}: MM-clean fraction per instrument")
ax.legend()
ax.grid(axis="x", alpha=0.3)
fig.tight_layout()
fig.savefig(PLOTS / "1_mm_clean_pct.png", dpi=120)
plt.close(fig)


# ── 2. Spread distribution per instrument ─────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 6))
data = [feat[feat["instrument"] == i]["spread"].to_numpy()
        for i in sorted(feat["instrument"].unique())]
labels = sorted(feat["instrument"].unique())
ax.boxplot(data, tick_labels=labels, showfliers=True)
ax.set_ylabel("Top-of-book spread (cents)")
ax.set_title(f"{EX}: bid1↔ask1 spread distribution per instrument")
ax.grid(axis="y", alpha=0.3)
plt.setp(ax.get_xticklabels(), rotation=60, ha="right")
fig.tight_layout()
fig.savefig(PLOTS / "2_spread_box.png", dpi=120)
plt.close(fig)


# ── 3. Book timeseries for cleanest instrument ────────────────────────────
cleanest = clean.index[-1]
sub = ob[ob["instrument"] == cleanest].sort_values("time").reset_index(drop=True)
trsub = tr[tr["instrument"] == cleanest].sort_values("time").reset_index(drop=True)

fig, ax = plt.subplots(figsize=(12, 6))
for col, color, lbl in [
    ("ask3_price", "#fbb4ae", "ask3"),
    ("ask2_price", "#f4978e", "ask2"),
    ("ask1_price", "#e63946", "ask1 (best ask)"),
    ("bid1_price", "#1d3557", "bid1 (best bid)"),
    ("bid2_price", "#457b9d", "bid2"),
    ("bid3_price", "#a8dadc", "bid3"),
]:
    ax.plot(sub["time"], sub[col], color=color, label=lbl, linewidth=1.4)
ax.scatter(trsub["time"], trsub["price"], s=12, c="black",
           alpha=0.5, label="trade prints", zorder=5)
ax.set_xlabel("time (ms)")
ax.set_ylabel("price (cents)")
ax.set_title(f"{EX}-{cleanest}: top-3 book + trades over time "
             f"(cleanest MM signature: {clean.loc[cleanest, 'bid_clean']:.0f}% / "
             f"{clean.loc[cleanest, 'ask_clean']:.0f}%)")
ax.legend(loc="upper right", ncol=3, fontsize=9)
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(PLOTS / "3_book_timeseries.png", dpi=120)
plt.close(fig)


# ── 4. Trades outside top-3: how many ticks away? ─────────────────────────
outside = trades_outside_top3(ob, tr)
outside["ticks_above_ask3"] = (
    outside["price"] - outside["ask3_price"]).where(outside["above_ask3"])
outside["ticks_below_bid3"] = (
    outside["bid3_price"] - outside["price"]).where(outside["below_bid3"])
above = outside["ticks_above_ask3"].dropna().astype(int)
below = outside["ticks_below_bid3"].dropna().astype(int)

fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
bins_a = np.arange(1, max(above.max(), 1) + 2) - 0.5
axes[0].hist(above, bins=bins_a, color="#e76f51", edgecolor="black")
axes[0].set_title(f"Trades printed ABOVE ask3 (n={len(above):,})")
axes[0].set_xlabel("cents above ask3")
axes[0].set_ylabel("# trades")
axes[0].grid(axis="y", alpha=0.3)
bins_b = np.arange(1, max(below.max(), 1) + 2) - 0.5
axes[1].hist(below, bins=bins_b, color="#2a9d8f", edgecolor="black")
axes[1].set_title(f"Trades printed BELOW bid3 (n={len(below):,})")
axes[1].set_xlabel("cents below bid3")
axes[1].grid(axis="y", alpha=0.3)
fig.suptitle(f"{EX}: where do trades print when they're outside the visible top-3?")
fig.tight_layout()
fig.savefig(PLOTS / "4_trades_outside_hist.png", dpi=120)
plt.close(fig)


# ── 5. Stacked-MM signature visualisation ─────────────────────────────────
# Pick a snapshot of ZABA where bid+ask are both perfectly stacked.
clean_inst = "ZABA"
zsub = feat[(feat["instrument"] == clean_inst)
            & feat["bid_stacked"] & feat["ask_stacked"]].iloc[0]
levels = [
    (zsub["ask3_price"], zsub["ask3_qty"], "ask"),
    (zsub["ask2_price"], zsub["ask2_qty"], "ask"),
    (zsub["ask1_price"], zsub["ask1_qty"], "ask"),
    (zsub["bid1_price"], zsub["bid1_qty"], "bid"),
    (zsub["bid2_price"], zsub["bid2_qty"], "bid"),
    (zsub["bid3_price"], zsub["bid3_qty"], "bid"),
]
fig, ax = plt.subplots(figsize=(9, 6))
for price, qty, side in levels:
    color = "#e63946" if side == "ask" else "#1d3557"
    ax.barh(price, qty if side == "ask" else -qty,
            color=color, edgecolor="black", height=0.6)
    ax.text(qty + 5 if side == "ask" else -qty - 5, price,
            f"{int(qty)} sh", va="center",
            ha="left" if side == "ask" else "right", fontsize=10)
# inferred deeper levels (extrapolate stacked pattern)
step = int(zsub["ask_step_12"])
inferred_ask4 = zsub["ask3_price"] + step
inferred_ask5 = zsub["ask3_price"] + 2 * step
inferred_bid4 = zsub["bid3_price"] - step
inferred_bid5 = zsub["bid3_price"] - 2 * step
for p, q in [(inferred_ask4, 200), (inferred_ask5, 250)]:
    ax.barh(p, q, color="#e63946", alpha=0.25, height=0.6,
            edgecolor="black", linestyle="--")
    ax.text(q + 5, p, f"{q} sh (inferred L{int((q-150)/50+3)})",
            va="center", ha="left", fontsize=9, color="#888")
for p, q in [(inferred_bid4, 200), (inferred_bid5, 250)]:
    ax.barh(p, -q, color="#1d3557", alpha=0.25, height=0.6,
            edgecolor="black", linestyle="--")
    ax.text(-q - 5, p, f"{q} sh (inferred L{int((q-150)/50+3)})",
            va="center", ha="right", fontsize=9, color="#888")
ax.axhline((zsub["bid1_price"] + zsub["ask1_price"]) / 2,
           color="grey", linestyle=":", label="mid (≈ MM fair value)")
ax.axvline(0, color="black", linewidth=0.8)
ax.set_xlabel("quantity   (← bids   asks →)")
ax.set_ylabel("price (cents)")
ax.set_title(
    f"{EX}-{clean_inst}: MM ladder signature  "
    f"(visible top-3 + extrapolated L4/L5)"
)
ax.legend(loc="lower right")
ax.grid(axis="x", alpha=0.3)
fig.tight_layout()
fig.savefig(PLOTS / "5_stacked_signature.png", dpi=120)
plt.close(fig)


print(f"Wrote 5 plots to {PLOTS}/")
for p in sorted(PLOTS.glob("*.png")):
    print(" ", p.name)
