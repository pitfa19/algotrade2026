#!/usr/bin/env python3
"""
Offline market-data analyzer for AlgoTrade 2026 captures.

Reads the CSV files produced by history_bot.py and extracts practical trading
signals: ETF dislocations, recurring cross-venue locks, cancel pressure,
venue spreads, trade activity, and CARD/SIMP moves.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional


EXCHANGES = [
    "NYSE",
    "NASDAQ",
    "SSE",
    "JPX",
    "Euronext",
    "LSE",
    "HKEX",
    "NSE",
    "TMX",
    "ZSE",
]

ETF_BASKETS: dict[str, list[str]] = {
    "ETFA": ["NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"],
    "ETFB": ["KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"],
    "ETFA3": ["NGUP", "KTST", "XFR"],
    "ETFB3": ["KOTD", "INA", "DLKV"],
    "ETFSH": ["GOLD", "XAG"],
}

SPECIAL_TICKERS = {"CARD", "SIMP"}


@dataclass(frozen=True)
class Quote:
    time: int
    exchange: str
    instrument: str
    bid: int
    ask: int
    bid_qty: int
    ask_qty: int

    @property
    def ticker(self) -> str:
        return ticker_from_instrument(self.instrument)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> int:
        return self.ask - self.bid


def analyze_market_data(
    data_dir: str | Path = "market_data",
    *,
    top_n: int = 20,
    sync_tolerance_ms: int = 75,
    bucket_ms: int = 100,
) -> dict[str, Any]:
    data_path = Path(data_dir)
    books = load_order_books(data_path)
    trade_counts, trade_summaries = load_trades(data_path)
    cancel_counts, nonexpired_cancel_counts = load_events(data_path)

    report = {
        "data_dir": str(data_path),
        "venue_summary": venue_summary(books, trade_summaries, cancel_counts),
        "top_etf_dislocations": top_etf_dislocations(
            books,
            top_n=top_n,
            sync_tolerance_ms=sync_tolerance_ms,
        ),
        "recurring_cross_routes": recurring_cross_routes(
            books,
            top_n=top_n,
            bucket_ms=bucket_ms,
        ),
        "top_cross_venue_locks": top_cross_venue_locks(
            books,
            top_n=top_n,
            bucket_ms=bucket_ms,
        ),
        "cancel_pressure": cancel_pressure(
            trade_counts,
            cancel_counts,
            nonexpired_cancel_counts,
            top_n=top_n,
        ),
        "special_moves": special_moves(data_path, top_n=top_n),
        "recommendations": recommendations(),
    }
    return report


def load_order_books(data_dir: Path) -> dict[str, dict[int, dict[str, Quote]]]:
    books: dict[str, dict[int, dict[str, Quote]]] = {}
    for path in sorted(data_dir.glob("*_orderbooks.csv")):
        exchange = path.name.removesuffix("_orderbooks.csv")
        exchange_books: dict[int, dict[str, Quote]] = defaultdict(dict)
        for row in read_rows(path):
            bid = parse_int(row.get("bid1_price"))
            ask = parse_int(row.get("ask1_price"))
            time_ms = parse_int(row.get("time"))
            instrument = row.get("instrument", "")
            if bid is None or ask is None or time_ms is None or not instrument:
                continue
            exchange_books[time_ms][instrument] = Quote(
                time=time_ms,
                exchange=exchange,
                instrument=instrument,
                bid=bid,
                ask=ask,
                bid_qty=parse_int(row.get("bid1_qty")) or 0,
                ask_qty=parse_int(row.get("ask1_qty")) or 0,
            )
        books[exchange] = dict(exchange_books)
    return books


def load_trades(data_dir: Path) -> tuple[Counter[str], dict[str, dict[str, Any]]]:
    counts: Counter[str] = Counter()
    summaries: dict[str, dict[str, Any]] = {}
    for path in sorted(data_dir.glob("*_trades.csv")):
        exchange = path.name.removesuffix("_trades.csv")
        exchange_count = 0
        quantity = 0
        notional = 0
        by_instrument: Counter[str] = Counter()
        for row in read_rows(path):
            instrument = row.get("instrument", "")
            price = parse_int(row.get("price"))
            qty = parse_int(row.get("quantity"))
            if not instrument or price is None or qty is None:
                continue
            counts[instrument] += 1
            by_instrument[instrument] += 1
            exchange_count += 1
            quantity += qty
            notional += price * qty
        summaries[exchange] = {
            "exchange": exchange,
            "trades": exchange_count,
            "quantity": quantity,
            "notional_cents": notional,
            "top_instruments": [
                {"instrument": inst, "trades": count}
                for inst, count in by_instrument.most_common(5)
            ],
        }
    return counts, summaries


def load_events(data_dir: Path) -> tuple[Counter[str], Counter[str]]:
    cancel_counts: Counter[str] = Counter()
    nonexpired_counts: Counter[str] = Counter()
    for path in sorted(data_dir.glob("*_events.csv")):
        for row in read_rows(path):
            instrument = row.get("instrument", "")
            if not instrument:
                continue
            cancel_counts[instrument] += 1
            if str(row.get("expired", "")).strip().lower() == "false":
                nonexpired_counts[instrument] += 1
    return cancel_counts, nonexpired_counts


def venue_summary(
    books: dict[str, dict[int, dict[str, Quote]]],
    trade_summaries: dict[str, dict[str, Any]],
    cancel_counts: Counter[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for exchange, by_time in sorted(books.items()):
        quotes = [quote for insts in by_time.values() for quote in insts.values()]
        instruments = {quote.instrument for quote in quotes}
        spreads = [quote.spread for quote in quotes if quote.spread >= 0]
        exchange_cancels = sum(
            count for instrument, count in cancel_counts.items() if instrument.startswith(f"{exchange}-")
        )
        trades = trade_summaries.get(exchange, {}).get("trades", 0)
        rows.append(
            {
                "exchange": exchange,
                "ticks": len(by_time),
                "instruments": len(instruments),
                "quotes": len(quotes),
                "median_spread_cents": safe_median(spreads),
                "avg_spread_cents": safe_mean(spreads),
                "trades": trades,
                "cancels": exchange_cancels,
                "cancel_to_trade": safe_ratio(exchange_cancels, trades),
            }
        )
    rows.sort(key=lambda row: row["exchange"])
    return rows


def top_etf_dislocations(
    books: dict[str, dict[int, dict[str, Quote]]],
    *,
    top_n: int,
    sync_tolerance_ms: int,
) -> list[dict[str, Any]]:
    if "ZSE" not in books:
        return []
    zse_times = sorted(books["ZSE"])
    hits: list[dict[str, Any]] = []

    for exchange, by_time in books.items():
        for time_ms, quotes in by_time.items():
            zse_time = nearest_time(zse_times, time_ms, sync_tolerance_ms)
            if zse_time is None:
                continue
            zse_quotes = books["ZSE"][zse_time]
            for etf, components in ETF_BASKETS.items():
                quote = quotes.get(f"{exchange}-{etf}")
                if quote is None:
                    continue
                component_mids = []
                for component in components:
                    component_quote = zse_quotes.get(f"ZSE-{component}")
                    if component_quote is None:
                        break
                    component_mids.append(component_quote.mid)
                if len(component_mids) != len(components):
                    continue
                fair_value = sum(component_mids) / len(component_mids)
                buy_edge = fair_value - quote.ask
                sell_edge = quote.bid - fair_value
                if buy_edge > 0:
                    hits.append(
                        etf_hit(exchange, etf, "BUY", time_ms, zse_time, quote.ask, fair_value, buy_edge, quote.ask_qty)
                    )
                if sell_edge > 0:
                    hits.append(
                        etf_hit(exchange, etf, "SELL", time_ms, zse_time, quote.bid, fair_value, sell_edge, quote.bid_qty)
                    )

    hits.sort(key=lambda hit: hit["edge_cents"], reverse=True)
    return hits[:top_n]


def etf_hit(
    exchange: str,
    ticker: str,
    side: str,
    time_ms: int,
    zse_time_ms: int,
    price: int,
    fair_value: float,
    edge: float,
    quantity: int,
) -> dict[str, Any]:
    return {
        "exchange": exchange,
        "ticker": ticker,
        "instrument": f"{exchange}-{ticker}",
        "side": side,
        "time": time_ms,
        "zse_time": zse_time_ms,
        "price": price,
        "fair_value": round(fair_value, 4),
        "edge_cents": round(edge, 4),
        "top_quantity": quantity,
    }


def top_cross_venue_locks(
    books: dict[str, dict[int, dict[str, Quote]]],
    *,
    top_n: int,
    bucket_ms: int,
) -> list[dict[str, Any]]:
    locks = all_cross_venue_locks(books, bucket_ms=bucket_ms)
    locks.sort(key=lambda row: row["edge_cents"], reverse=True)
    return locks[:top_n]


def recurring_cross_routes(
    books: dict[str, dict[int, dict[str, Quote]]],
    *,
    top_n: int,
    bucket_ms: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for lock in all_cross_venue_locks(books, bucket_ms=bucket_ms):
        key = (lock["ticker"], lock["buy_exchange"], lock["sell_exchange"])
        group = grouped.setdefault(
            key,
            {
                "ticker": lock["ticker"],
                "buy_exchange": lock["buy_exchange"],
                "sell_exchange": lock["sell_exchange"],
                "occurrences": 0,
                "sum_edge_cents": 0.0,
                "max_edge_cents": 0.0,
                "min_quantity": 0,
            },
        )
        group["occurrences"] += 1
        group["sum_edge_cents"] += lock["edge_cents"]
        group["max_edge_cents"] = max(group["max_edge_cents"], lock["edge_cents"])
        group["min_quantity"] += lock["quantity"]

    routes = []
    for group in grouped.values():
        occurrences = group.pop("occurrences")
        sum_edge = group.pop("sum_edge_cents")
        sum_quantity = group.pop("min_quantity")
        routes.append(
            {
                **group,
                "occurrences": occurrences,
                "avg_edge_cents": round(sum_edge / occurrences, 4),
                "max_edge_cents": round(group["max_edge_cents"], 4),
                "avg_top_quantity": round(sum_quantity / occurrences, 4),
            }
        )
    routes.sort(key=lambda row: (row["occurrences"], row["avg_edge_cents"]), reverse=True)
    return routes[:top_n]


def all_cross_venue_locks(
    books: dict[str, dict[int, dict[str, Quote]]],
    *,
    bucket_ms: int,
) -> list[dict[str, Any]]:
    by_bucket: dict[int, dict[str, list[Quote]]] = defaultdict(lambda: defaultdict(list))
    for by_time in books.values():
        for time_ms, quotes in by_time.items():
            bucket = round(time_ms / bucket_ms) * bucket_ms
            for quote in quotes.values():
                by_bucket[bucket][quote.ticker].append(quote)

    locks: list[dict[str, Any]] = []
    for bucket, by_ticker in by_bucket.items():
        for ticker, quotes in by_ticker.items():
            if len(quotes) < 2:
                continue
            best_ask = min(quotes, key=lambda quote: quote.ask)
            best_bid = max(quotes, key=lambda quote: quote.bid)
            if best_ask.exchange == best_bid.exchange:
                continue
            edge = best_bid.bid - best_ask.ask
            if edge <= 0:
                continue
            locks.append(
                {
                    "ticker": ticker,
                    "bucket": bucket,
                    "buy_exchange": best_ask.exchange,
                    "sell_exchange": best_bid.exchange,
                    "buy_instrument": best_ask.instrument,
                    "sell_instrument": best_bid.instrument,
                    "buy_price": best_ask.ask,
                    "sell_price": best_bid.bid,
                    "edge_cents": edge,
                    "quantity": min(best_ask.ask_qty, best_bid.bid_qty),
                    "buy_time": best_ask.time,
                    "sell_time": best_bid.time,
                }
            )
    return locks


def cancel_pressure(
    trade_counts: Counter[str],
    cancel_counts: Counter[str],
    nonexpired_cancel_counts: Counter[str],
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument, cancels in cancel_counts.items():
        trades = trade_counts[instrument]
        rows.append(
            {
                "instrument": instrument,
                "cancels": cancels,
                "nonexpired_cancels": nonexpired_cancel_counts[instrument],
                "trades": trades,
                "cancel_to_trade": round(safe_ratio(cancels, trades), 4),
                "manual_cancel_share": round(safe_ratio(nonexpired_cancel_counts[instrument], cancels), 4),
            }
        )
    rows.sort(key=lambda row: (row["cancel_to_trade"], row["cancels"]), reverse=True)
    return rows[:top_n]


def special_moves(data_dir: Path, *, top_n: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(data_dir.glob("*_candles.csv")):
        exchange = path.name.removesuffix("_candles.csv")
        by_instrument: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in read_rows(path):
            instrument = row.get("instrument", "")
            if ticker_from_instrument(instrument) in SPECIAL_TICKERS:
                by_instrument[instrument].append(row)
        for instrument, candles in by_instrument.items():
            parsed = []
            for row in candles:
                index = parse_int(row.get("index"))
                open_price = parse_int(row.get("open"))
                close_price = parse_int(row.get("close"))
                volume = parse_int(row.get("volume")) or 0
                if index is None or open_price is None or close_price is None:
                    continue
                parsed.append((index, open_price, close_price, volume))
            if not parsed:
                continue
            parsed.sort()
            first_open = parsed[0][1]
            last_close = parsed[-1][2]
            return_bps = ((last_close - first_open) / first_open * 10_000) if first_open else 0.0
            rows.append(
                {
                    "exchange": exchange,
                    "instrument": instrument,
                    "ticker": ticker_from_instrument(instrument),
                    "first_open": first_open,
                    "last_close": last_close,
                    "return_bps": round(return_bps, 4),
                    "volume": sum(row[3] for row in parsed),
                    "candles": len(parsed),
                    "first_index": parsed[0][0],
                    "last_index": parsed[-1][0],
                }
            )
    rows.sort(key=lambda row: abs(row["return_bps"]), reverse=True)
    return rows[:top_n]


def recommendations() -> list[str]:
    return [
        "Prioritize IOC ETF arbitrage where ZSE basket fair value disagrees with ETF top-of-book.",
        "Promote recurring cross-venue routes into a whitelist; trade only routes that repeat and survive latency.",
        "Treat CARD as an event/momentum instrument and SIMP as a stability/control instrument until data says otherwise.",
        "Discount book depth on instruments with extreme cancel/trade ratios before quoting passively.",
        "Use top-level 50-share liquidity as a likely market-maker fingerprint; unusual size is informative only if it persists.",
    ]


def read_rows(path: Path) -> Iterable[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        yield from csv.DictReader(handle)


def nearest_time(times: list[int], target: int, tolerance_ms: int) -> Optional[int]:
    if not times:
        return None
    low = 0
    high = len(times)
    while low < high:
        mid = (low + high) // 2
        if times[mid] < target:
            low = mid + 1
        else:
            high = mid
    candidates = []
    if low < len(times):
        candidates.append(times[low])
    if low > 0:
        candidates.append(times[low - 1])
    if not candidates:
        return None
    best = min(candidates, key=lambda value: abs(value - target))
    return best if abs(best - target) <= tolerance_ms else None


def ticker_from_instrument(instrument: str) -> str:
    return instrument.split("-", 1)[1] if "-" in instrument else instrument


def parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_mean(values: list[int | float]) -> Optional[float]:
    return round(statistics.mean(values), 4) if values else None


def safe_median(values: list[int | float]) -> Optional[float]:
    return round(statistics.median(values), 4) if values else None


def safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return float(numerator) if numerator else 0.0
    return float(numerator) / float(denominator)


def print_text_report(report: dict[str, Any], top_n: int) -> None:
    print("AnalyzerBot offline report")
    print(f"data_dir: {report['data_dir']}")

    print("\nVenue summary")
    for row in report["venue_summary"]:
        print(
            f"{row['exchange']:8s} ticks={row['ticks']:4d} "
            f"instruments={row['instruments']:2d} "
            f"spread_med={format_optional(row['median_spread_cents'])}c "
            f"trades={row['trades']:5d} cancels={row['cancels']:6d} "
            f"cancel/trade={row['cancel_to_trade']:.1f}"
        )

    print(f"\nTop {top_n} ETF dislocations")
    for row in report["top_etf_dislocations"][:top_n]:
        print(
            f"{row['edge_cents']:7.2f}c {row['side']:4s} "
            f"{row['instrument']:14s} px={row['price']:6d} "
            f"fv={row['fair_value']:8.2f} qty={row['top_quantity']:3d} "
            f"t={row['time']}"
        )

    print(f"\nTop {top_n} recurring cross-venue routes")
    for row in report["recurring_cross_routes"][:top_n]:
        print(
            f"{row['ticker']:6s} buy={row['buy_exchange']:8s} "
            f"sell={row['sell_exchange']:8s} n={row['occurrences']:4d} "
            f"avg={row['avg_edge_cents']:7.2f}c max={row['max_edge_cents']:7.2f}c"
        )

    print(f"\nTop {top_n} cancel-pressure instruments")
    for row in report["cancel_pressure"][:top_n]:
        print(
            f"{row['instrument']:14s} cancels={row['cancels']:6d} "
            f"manual={row['nonexpired_cancels']:6d} trades={row['trades']:4d} "
            f"cancel/trade={row['cancel_to_trade']:7.1f}"
        )

    print(f"\nTop {top_n} CARD/SIMP moves")
    for row in report["special_moves"][:top_n]:
        print(
            f"{row['instrument']:14s} return={row['return_bps']:8.1f}bps "
            f"{row['first_open']}->{row['last_close']} vol={row['volume']}"
        )

    print("\nRecommendations")
    for number, recommendation in enumerate(report["recommendations"], start=1):
        print(f"{number}. {recommendation}")


def format_optional(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze AlgoTrade history_bot CSV captures.")
    parser.add_argument("--data-dir", default="market_data", help="Directory containing *_orderbooks/trades/events/candles.csv")
    parser.add_argument("--top", type=int, default=20, help="Number of rows to show per section")
    parser.add_argument("--sync-tolerance-ms", type=int, default=75, help="Max time mismatch for ZSE ETF reference")
    parser.add_argument("--bucket-ms", type=int, default=100, help="Time bucket size for cross-venue locks")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of text")
    parser.add_argument("--write-json", help="Optional path to write full JSON report")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    report = analyze_market_data(
        args.data_dir,
        top_n=args.top,
        sync_tolerance_ms=args.sync_tolerance_ms,
        bucket_ms=args.bucket_ms,
    )
    if args.write_json:
        output_path = Path(args.write_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_text_report(report, args.top)


if __name__ == "__main__":
    main()
