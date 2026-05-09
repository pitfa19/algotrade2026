"""Quick structural map of the market data.
Goal: understand time alignment, instrument coverage per exchange,
spread distributions, MM behavior.
"""
import os, glob
import pandas as pd
import numpy as np

DATA = "/home/pitfa/Documents/algotrade2026/market_data"

EXCHANGES = ["NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"]

def load_ob(ex):
    p = f"{DATA}/{ex}_orderbooks.csv"
    df = pd.read_csv(p)
    df["exchange"] = ex
    df["ticker"] = df["instrument"].str.split("-",n=1).str[1]
    return df

def load_trades(ex):
    p = f"{DATA}/{ex}_trades.csv"
    df = pd.read_csv(p)
    df["exchange"] = ex
    df["ticker"] = df["instrument"].str.split("-",n=1).str[1]
    return df

print("=== Time range per exchange (orderbooks) ===")
ranges = {}
for ex in EXCHANGES:
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv", usecols=["time"])
    ranges[ex] = (df.time.min(), df.time.max(), len(df))
    print(f"{ex:9s} t={df.time.min():>7d} → {df.time.max():>7d}  span={df.time.max()-df.time.min():>7d}ms  rows={len(df):>6d}")

print("\n=== Spread per ticker on ZSE (median bps) ===")
zse = load_ob("ZSE")
zse["mid"] = (zse.bid1_price + zse.ask1_price)/2
zse["spread"] = zse.ask1_price - zse.bid1_price
zse["spread_bps"] = zse.spread / zse.mid * 10000
print(zse.groupby("ticker")["spread_bps"].agg(["median","mean","max"]).round(1).sort_values("median"))

print("\n=== ETFA on ZSE: spread vs basket NAV ===")
basket_a = ["NGUP","OIT","KTST","FSR","JZRO","XFR"]
piv = zse.pivot_table(index="time", columns="ticker", values="mid")
piv["NAV_A"] = piv[basket_a].mean(axis=1)
piv["NAV_A_diff"] = piv["ETFA"] - piv["NAV_A"]
print(piv["NAV_A_diff"].describe(percentiles=[0.05,0.5,0.95]).round(2))
print("times with |ETFA - NAV| > 50c:", (piv["NAV_A_diff"].abs()>50).sum(), "/", piv["NAV_A_diff"].notna().sum())

print("\n=== Cross-venue CARD sample (every venue, t closest to 660000) ===")
TARGET = 660000
for ex in EXCHANGES:
    df = pd.read_csv(f"{DATA}/{ex}_orderbooks.csv")
    sub = df[df.instrument == f"{ex}-CARD"].copy()
    if len(sub)==0: continue
    sub["dt"] = (sub.time - TARGET).abs()
    r = sub.nsmallest(1,"dt").iloc[0]
    mid = (r.bid1_price + r.ask1_price)/2
    print(f"{ex:9s} t={int(r.time):>7d} bid={int(r.bid1_price)} ask={int(r.ask1_price)} mid={mid:.0f} spread={int(r.ask1_price-r.bid1_price)}c")
