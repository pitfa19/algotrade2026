"""
ZSE market-maker ladder reconstruction.

Goal: per-instrument, characterize the MM's quote pattern from the recorded
top-3 orderbook snapshots, plus use trades that print outside the visible
top-3 to infer the deeper levels (4 and 5).

Hypotheses tested:
  H1: MM places one 50-share order at each of 5 prices on each side ("flat").
  H2: MM places 1, 2, 3, 4, 5 lots of 50 at levels 1..5 ("stacked"),
      so visible cumulative qty is 50/100/150 at the inner three.

Outputs:
  - Per-instrument summary CSV
  - Identification of "MM-only" snapshots (where the visible book is the MM
    ladder with no team activity)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA = Path(__file__).resolve().parents[1] / "market_data"
OUT = Path(__file__).resolve().parent / "out"
OUT.mkdir(exist_ok=True)


def load_ob(ex: str) -> pd.DataFrame:
    df = pd.read_csv(DATA / f"{ex}_orderbooks.csv")
    df["instrument"] = df["instrument"].str.replace(f"{ex}-", "", regex=False)
    return df


def load_trades(ex: str) -> pd.DataFrame:
    df = pd.read_csv(DATA / f"{ex}_trades.csv")
    df["instrument"] = df["instrument"].str.replace(f"{ex}-", "", regex=False)
    return df


def signature_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-snapshot features used to detect MM-only ladders."""
    out = df.copy()
    # tick step between adjacent visible levels (cents)
    out["bid_step_12"] = df["bid1_price"] - df["bid2_price"]
    out["bid_step_23"] = df["bid2_price"] - df["bid3_price"]
    out["ask_step_12"] = df["ask2_price"] - df["ask1_price"]
    out["ask_step_23"] = df["ask3_price"] - df["ask2_price"]
    out["spread"] = df["ask1_price"] - df["bid1_price"]
    out["mid"] = (df["ask1_price"] + df["bid1_price"]) / 2.0
    # quantity match for "stacked" hypothesis (50/100/150)
    out["bid_stacked"] = (
        (df["bid1_qty"] == 50) & (df["bid2_qty"] == 100) & (df["bid3_qty"] == 150)
    )
    out["ask_stacked"] = (
        (df["ask1_qty"] == 50) & (df["ask2_qty"] == 100) & (df["ask3_qty"] == 150)
    )
    # quantity match for "flat" hypothesis (50/50/50 at each level)
    out["bid_flat"] = (
        (df["bid1_qty"] == 50) & (df["bid2_qty"] == 50) & (df["bid3_qty"] == 50)
    )
    out["ask_flat"] = (
        (df["ask1_qty"] == 50) & (df["ask2_qty"] == 50) & (df["ask3_qty"] == 50)
    )
    # team activity inside top-3 (qty not multiple of 50, or qty < 50 at L1)
    bid_qs = df[["bid1_qty", "bid2_qty", "bid3_qty"]]
    ask_qs = df[["ask1_qty", "ask2_qty", "ask3_qty"]]
    out["bid_dirty"] = (bid_qs % 50 != 0).any(axis=1) | (bid_qs == 0).any(axis=1)
    out["ask_dirty"] = (ask_qs % 50 != 0).any(axis=1) | (ask_qs == 0).any(axis=1)
    return out


def per_instrument_summary(feat: pd.DataFrame) -> pd.DataFrame:
    g = feat.groupby("instrument", observed=True)
    rows = []
    for inst, sub in g:
        n = len(sub)
        rows.append({
            "instrument": inst,
            "n_snaps": n,
            "spread_median_c": int(sub["spread"].median()),
            "spread_min_c": int(sub["spread"].min()),
            "spread_max_c": int(sub["spread"].max()),
            "bid_step12_mode": int(sub["bid_step_12"].mode().iloc[0]),
            "ask_step12_mode": int(sub["ask_step_12"].mode().iloc[0]),
            "bid_stacked_pct": round(100 * sub["bid_stacked"].mean(), 1),
            "ask_stacked_pct": round(100 * sub["ask_stacked"].mean(), 1),
            "bid_flat_pct": round(100 * sub["bid_flat"].mean(), 1),
            "ask_flat_pct": round(100 * sub["ask_flat"].mean(), 1),
            "bid_clean_pct": round(100 * (~sub["bid_dirty"]).mean(), 1),
            "ask_clean_pct": round(100 * (~sub["ask_dirty"]).mean(), 1),
            "mid_mean": round(float(sub["mid"].mean()), 1),
            "mid_std": round(float(sub["mid"].std()), 1),
        })
    return pd.DataFrame(rows).sort_values("instrument").reset_index(drop=True)


def trades_outside_top3(ob: pd.DataFrame, tr: pd.DataFrame) -> pd.DataFrame:
    """For each trade, look up the orderbook snapshot at-or-before that time,
    and flag trades whose price is strictly outside the visible top-3 levels.
    These prints reveal the existence of hidden levels 4 / 5 (almost certainly MM).
    """
    rows = []
    for inst, tr_inst in tr.groupby("instrument", observed=True):
        ob_inst = ob[ob["instrument"] == inst].sort_values("time").reset_index(drop=True)
        if ob_inst.empty:
            continue
        tr_inst = tr_inst.sort_values("time").reset_index(drop=True)
        merged = pd.merge_asof(
            tr_inst, ob_inst, on="time", direction="backward",
        )
        merged = merged.dropna(subset=["bid1_price"])
        merged["above_ask3"] = merged["price"] > merged["ask3_price"]
        merged["below_bid3"] = merged["price"] < merged["bid3_price"]
        merged["outside"] = merged["above_ask3"] | merged["below_bid3"]
        if merged["outside"].any():
            sub = merged[merged["outside"]].copy()
            sub["instrument"] = inst
            rows.append(sub[[
                "time", "instrument", "price", "quantity",
                "bid1_price", "bid3_price", "ask1_price", "ask3_price",
                "above_ask3", "below_bid3",
            ]])
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def main(exchange: str = "ZSE") -> None:
    ob = load_ob(exchange)
    tr = load_trades(exchange)
    feat = signature_features(ob)
    summary = per_instrument_summary(feat)
    summary.to_csv(OUT / f"{exchange}_mm_summary.csv", index=False)
    print(f"\n=== {exchange} per-instrument MM signature ===")
    with pd.option_context("display.max_rows", None, "display.width", 200):
        print(summary.to_string(index=False))

    outside = trades_outside_top3(ob, tr)
    outside.to_csv(OUT / f"{exchange}_trades_outside_top3.csv", index=False)
    n_total = len(tr)
    n_out = len(outside)
    print(
        f"\nTrades outside visible top-3: {n_out:,} / {n_total:,} "
        f"({100 * n_out / max(n_total, 1):.2f}%)  → evidence of MM levels 4–5"
    )
    if n_out:
        per_inst = (
            outside.groupby("instrument", observed=True)
            .size()
            .sort_values(ascending=False)
        )
        print("\nPer-instrument trades-outside-top-3 (top 10):")
        print(per_inst.head(10).to_string())


if __name__ == "__main__":
    ex = sys.argv[1] if len(sys.argv) > 1 else "ZSE"
    main(ex)
