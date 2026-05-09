#!/usr/bin/env python3
"""Certificate bot for the AlgoTrade ETF package edge.

The core proof is tiny:

    n * ETF fair value == sum(constituent fair values)

So an executable package of +n ETF and -1 of every constituent, or the
reverse package, has zero terminal fair value. If the current book pays a
positive entry credit for that full integer package, that credit is the
path-independent value of the filled package.

This bot is dry-run by default. Set LIVE_TRADING=1 to send IOC orders.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

EXCHANGE_PORT = 9001
EXCHANGE_HOSTS = {
    "NYSE": "10.0.201.2",
    "NASDAQ": "10.0.202.2",
    "SSE": "10.0.203.2",
    "JPX": "10.0.204.2",
    "Euronext": "10.0.205.2",
    "LSE": "10.0.206.2",
    "HKEX": "10.0.207.2",
    "NSE": "10.0.208.2",
    "TMX": "10.0.209.2",
    "ZSE": "10.0.210.2",
}
EXCHANGES = tuple(EXCHANGE_HOSTS)

ETF_BASKETS = {
    "ETFA": ("NGUP", "OIT", "KTST", "FSR", "JZRO", "XFR"),
    "ETFB": ("KOTD", "INA", "HT", "JNAF", "DLKV", "DDJH"),
    "ETFA3": ("NGUP", "KTST", "XFR"),
    "ETFB3": ("KOTD", "INA", "DLKV"),
    "ETFSH": ("GOLD", "XAG"),
}

STOCK_LISTINGS = {
    "CARD": EXCHANGES,
    "SIMP": EXCHANGES,
    "NGUP": ("NYSE", "NASDAQ", "Euronext", "TMX", "ZSE"),
    "OIT": ("LSE", "Euronext", "HKEX", "NSE", "ZSE"),
    "KTST": ("NYSE", "JPX", "TMX", "ZSE"),
    "FSR": ("NASDAQ", "LSE", "SSE", "HKEX", "ZSE"),
    "JZRO": ("NYSE", "LSE", "Euronext", "TMX", "ZSE"),
    "XFR": ("NYSE", "HKEX", "TMX", "ZSE"),
    "KOTD": ("NASDAQ", "LSE", "Euronext", "HKEX", "ZSE"),
    "INA": ("NYSE", "NASDAQ", "Euronext", "HKEX", "ZSE"),
    "HT": ("NASDAQ", "LSE", "JPX", "SSE", "TMX", "ZSE"),
    "JNAF": ("NYSE", "Euronext", "JPX", "HKEX", "ZSE"),
    "DLKV": ("NASDAQ", "LSE", "HKEX", "NSE", "ZSE"),
    "DDJH": ("NYSE", "LSE", "Euronext", "TMX", "ZSE"),
    "MDKA": ("NYSE", "LSE", "HKEX", "TMX", "ZSE"),
    "KRAS": ("NYSE", "Euronext", "SSE", "TMX", "ZSE"),
    "ZITO": ("NASDAQ", "LSE", "Euronext", "NSE", "ZSE"),
    "ZABA": ("NYSE", "LSE", "SSE", "NSE", "TMX", "ZSE"),
    "GOLD": ("NASDAQ", "Euronext", "JPX", "TMX", "ZSE"),
    "XAG": ("LSE", "Euronext", "JPX", "ZSE"),
}

ETF_LISTINGS = {
    "ETFA": ("NYSE", "Euronext", "HKEX", "ZSE"),
    "ETFB": ("NASDAQ", "LSE", "HKEX", "ZSE"),
    "ETFA3": ("NYSE", "TMX", "ZSE"),
    "ETFB3": ("NASDAQ", "HKEX", "ZSE"),
    "ETFSH": ("Euronext", "JPX", "ZSE"),
}


class Side(Enum):
    BUY_ETF_SELL_BASKET = "buy_etf_sell_basket"
    SELL_ETF_BUY_BASKET = "sell_etf_buy_basket"


@dataclass(frozen=True)
class Level:
    price: int
    quantity: int


@dataclass(frozen=True)
class FillPlan:
    quantity: int
    notional_cents: int
    limit_price: int


@dataclass(frozen=True)
class OrderBook:
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]

    def buy_plan(self, quantity: int) -> FillPlan | None:
        return walk_levels(self.asks, quantity)

    def sell_plan(self, quantity: int) -> FillPlan | None:
        return walk_levels(self.bids, quantity)


@dataclass(frozen=True)
class CertificateLeg:
    instrument_id: str
    api_side: str
    quantity: int
    limit_price: int
    notional_cents: int


@dataclass(frozen=True)
class Certificate:
    exchange: str
    etf: str
    side: Side
    credit_cents: int
    etf_quantity: int
    component_quantity: int
    entry_notional_cents: int
    legs: tuple[CertificateLeg, ...]

    @property
    def proof(self) -> str:
        components = ", ".join(ETF_BASKETS[self.etf])
        return (
            f"{self.exchange}-{self.etf}: credit={self.credit_cents}c, "
            f"package terminal FV = {self.etf_quantity}*ETF - ({components}) = 0"
        )


def parse_orderbook_depth(raw: dict[str, Any]) -> OrderBook:
    bids = tuple(
        Level(int(price), int(qty))
        for price, qty in sorted(
            (raw.get("bids") or {}).items(), key=lambda item: int(item[0]), reverse=True
        )
        if int(qty) > 0
    )
    asks = tuple(
        Level(int(price), int(qty))
        for price, qty in sorted((raw.get("asks") or {}).items(), key=lambda item: int(item[0]))
        if int(qty) > 0
    )
    return OrderBook(bids=bids, asks=asks)


def walk_levels(levels: tuple[Level, ...], quantity: int) -> FillPlan | None:
    remaining = quantity
    notional = 0
    limit_price = 0
    for level in levels:
        take = min(remaining, level.quantity)
        notional += take * level.price
        remaining -= take
        limit_price = level.price
        if remaining == 0:
            return FillPlan(quantity=quantity, notional_cents=notional, limit_price=limit_price)
    return None


def find_certificates(
    exchange: str,
    books: dict[str, OrderBook],
    *,
    min_credit_cents: int,
) -> list[Certificate]:
    certificates: list[Certificate] = []
    for etf, components in ETF_BASKETS.items():
        if exchange not in ETF_LISTINGS[etf]:
            continue
        if any(exchange not in STOCK_LISTINGS[component] for component in components):
            continue

        etf_instrument = f"{exchange}-{etf}"
        etf_book = books.get(etf_instrument)
        component_books = {
            component: books.get(f"{exchange}-{component}") for component in components
        }
        if etf_book is None or any(book is None for book in component_books.values()):
            continue

        n = len(components)
        buy_etf = etf_book.buy_plan(n)
        sell_etf = etf_book.sell_plan(n)
        component_sells = {
            component: book.sell_plan(1)
            for component, book in component_books.items()
            if book is not None
        }
        component_buys = {
            component: book.buy_plan(1)
            for component, book in component_books.items()
            if book is not None
        }
        if buy_etf is not None and all(plan is not None for plan in component_sells.values()):
            component_revenue = sum(plan.notional_cents for plan in component_sells.values() if plan)
            credit = component_revenue - buy_etf.notional_cents
            if credit >= min_credit_cents:
                legs = [
                    CertificateLeg(
                        instrument_id=etf_instrument,
                        api_side="bid",
                        quantity=n,
                        limit_price=buy_etf.limit_price,
                        notional_cents=-buy_etf.notional_cents,
                    )
                ]
                legs.extend(
                    CertificateLeg(
                        instrument_id=f"{exchange}-{component}",
                        api_side="ask",
                        quantity=1,
                        limit_price=plan.limit_price,
                        notional_cents=plan.notional_cents,
                    )
                    for component, plan in component_sells.items()
                    if plan is not None
                )
                certificates.append(
                    Certificate(
                        exchange=exchange,
                        etf=etf,
                        side=Side.BUY_ETF_SELL_BASKET,
                        credit_cents=credit,
                        etf_quantity=n,
                        component_quantity=1,
                        entry_notional_cents=credit,
                        legs=tuple(legs),
                    )
                )

        if sell_etf is not None and all(plan is not None for plan in component_buys.values()):
            component_cost = sum(plan.notional_cents for plan in component_buys.values() if plan)
            credit = sell_etf.notional_cents - component_cost
            if credit >= min_credit_cents:
                legs = [
                    CertificateLeg(
                        instrument_id=etf_instrument,
                        api_side="ask",
                        quantity=n,
                        limit_price=sell_etf.limit_price,
                        notional_cents=sell_etf.notional_cents,
                    )
                ]
                legs.extend(
                    CertificateLeg(
                        instrument_id=f"{exchange}-{component}",
                        api_side="bid",
                        quantity=1,
                        limit_price=plan.limit_price,
                        notional_cents=-plan.notional_cents,
                    )
                    for component, plan in component_buys.items()
                    if plan is not None
                )
                certificates.append(
                    Certificate(
                        exchange=exchange,
                        etf=etf,
                        side=Side.SELL_ETF_BUY_BASKET,
                        credit_cents=credit,
                        etf_quantity=n,
                        component_quantity=1,
                        entry_notional_cents=credit,
                        legs=tuple(legs),
                    )
                )

    certificates.sort(key=lambda certificate: certificate.credit_cents, reverse=True)
    return certificates


def build_certificate_orders(
    certificate: Certificate,
    *,
    request_prefix: str,
    expiry_ms: int,
) -> list[dict[str, Any]]:
    return [
        {
            "type": "add_order",
            "user_request_id": f"{request_prefix}-{index}",
            "instrument_id": leg.instrument_id,
            "side": leg.api_side,
            "quantity": leg.quantity,
            "price": leg.limit_price,
            "expiry": expiry_ms,
            "order_type": "ioc",
        }
        for index, leg in enumerate(certificate.legs, start=1)
    ]


class CertificateBot:
    def __init__(
        self,
        *,
        exchanges: list[str],
        min_credit_cents: int,
        live_trading: bool,
        cooldown_seconds: float,
    ) -> None:
        self.exchanges = exchanges
        self.min_credit_cents = min_credit_cents
        self.live_trading = live_trading
        self.cooldown_seconds = cooldown_seconds
        self.books: dict[str, dict[str, OrderBook]] = defaultdict(dict)
        self.last_fire_monotonic: dict[tuple[str, str, Side], float] = {}
        self.request_counter = 0

    async def run(self) -> None:
        await asyncio.gather(*(self._run_exchange(exchange) for exchange in self.exchanges))

    async def _run_exchange(self, exchange: str) -> None:
        from websockets.asyncio.client import connect as ws_connect

        url = f"ws://{EXCHANGE_HOSTS[exchange]}:{EXCHANGE_PORT}/trade"
        while True:
            try:
                async with ws_connect(url) as ws:
                    welcome = json.loads(await ws.recv())
                    logging.info("%s connected: %s", exchange, welcome.get("message", ""))
                    async for raw in ws:
                        message = json.loads(raw)
                        if message.get("type") == "market_data_update":
                            self._apply_market_data(exchange, message)
                            await self._maybe_fire(exchange, ws)
                        elif message.get("type") == "end_of_round":
                            self.books[exchange].clear()
                            logging.info("%s end_of_round; reconnecting", exchange)
                            break
            except Exception as exc:
                logging.warning("%s connection problem: %s", exchange, exc)
            await asyncio.sleep(1.0)

    def _apply_market_data(self, exchange: str, message: dict[str, Any]) -> None:
        for instrument_id, raw_book in message.get("orderbook_depths", {}).items():
            self.books[exchange][instrument_id] = parse_orderbook_depth(raw_book)

    async def _maybe_fire(self, exchange: str, ws: Any) -> None:
        certificates = find_certificates(
            exchange,
            self.books[exchange],
            min_credit_cents=self.min_credit_cents,
        )
        if not certificates:
            return

        certificate = certificates[0]
        key = (certificate.exchange, certificate.etf, certificate.side)
        now = time.monotonic()
        if now - self.last_fire_monotonic.get(key, 0.0) < self.cooldown_seconds:
            return
        self.last_fire_monotonic[key] = now

        logging.info("CERTIFICATE %s", certificate.proof)
        for leg in certificate.legs:
            logging.info(
                "  leg %-12s %-3s qty=%s limit=%s notional=%+s",
                leg.instrument_id,
                leg.api_side,
                leg.quantity,
                leg.limit_price,
                leg.notional_cents,
            )

        if not self.live_trading:
            return

        self.request_counter += 1
        expiry_ms = int(time.time() * 1000) + 2_000
        orders = build_certificate_orders(
            certificate,
            request_prefix=f"cert-{self.request_counter}",
            expiry_ms=expiry_ms,
        )
        await asyncio.gather(*(ws.send(json.dumps(order)) for order in orders))


def scan_market_data(data_dir: Path, min_credit_cents: int, top: int) -> list[Certificate]:
    by_exchange_time: dict[str, dict[int, dict[str, OrderBook]]] = defaultdict(lambda: defaultdict(dict))
    for path in sorted(data_dir.glob("*_orderbooks.csv")):
        exchange = path.name.removesuffix("_orderbooks.csv")
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                try:
                    book = parse_csv_book(row)
                    rounded_time = round(int(row["time"]) / 100) * 100
                except (KeyError, TypeError, ValueError):
                    continue
                by_exchange_time[exchange][rounded_time][row["instrument"]] = book

    found: list[Certificate] = []
    for exchange, by_time in by_exchange_time.items():
        for books in by_time.values():
            found.extend(
                find_certificates(exchange, books, min_credit_cents=min_credit_cents)
            )
    found.sort(key=lambda certificate: certificate.credit_cents, reverse=True)
    return found[:top]


def parse_csv_book(row: dict[str, str]) -> OrderBook:
    raw = {"bids": {}, "asks": {}}
    for index in (1, 2, 3):
        bid_price = row.get(f"bid{index}_price", "")
        bid_qty = row.get(f"bid{index}_qty", "")
        ask_price = row.get(f"ask{index}_price", "")
        ask_qty = row.get(f"ask{index}_qty", "")
        if bid_price and bid_qty:
            raw["bids"][bid_price] = int(bid_qty)
        if ask_price and ask_qty:
            raw["asks"][ask_price] = int(ask_qty)
    return parse_orderbook_depth(raw)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prove and optionally trade ETF package certificates.")
    parser.add_argument("--scan-market-data", action="store_true", help="Scan local CSV data instead of connecting live.")
    parser.add_argument("--data-dir", default="market_data", help="CSV directory for --scan-market-data.")
    parser.add_argument("--top", type=int, default=20, help="Rows to print in scan mode.")
    parser.add_argument(
        "--min-credit-cents",
        type=int,
        default=int(os.environ.get("MIN_CREDIT_CENTS", "100")),
        help="Minimum positive package credit before logging/trading.",
    )
    parser.add_argument(
        "--exchanges",
        default=os.environ.get("EXCHANGES", ",".join(EXCHANGES)),
        help="Comma-separated live exchanges to connect to.",
    )
    parser.add_argument(
        "--live-trading",
        action="store_true",
        default=os.environ.get("LIVE_TRADING", "0") == "1",
        help="Actually send IOC orders. Default only logs certificates.",
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=float,
        default=float(os.environ.get("COOLDOWN_SECONDS", "1.0")),
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = parse_args()
    if args.scan_market_data:
        certificates = scan_market_data(Path(args.data_dir), args.min_credit_cents, args.top)
        for certificate in certificates:
            print(certificate.proof)
            for leg in certificate.legs:
                print(
                    f"  {leg.instrument_id:14s} {leg.api_side:3s} "
                    f"qty={leg.quantity:<2d} limit={leg.limit_price:<7d} "
                    f"notional={leg.notional_cents:+d}"
                )
        return

    exchanges = [exchange.strip() for exchange in args.exchanges.split(",") if exchange.strip()]
    unknown = sorted(set(exchanges) - set(EXCHANGES))
    if unknown:
        raise SystemExit(f"unknown exchange(s): {', '.join(unknown)}")
    asyncio.run(
        CertificateBot(
            exchanges=exchanges,
            min_credit_cents=args.min_credit_cents,
            live_trading=args.live_trading,
            cooldown_seconds=args.cooldown_seconds,
        ).run()
    )


if __name__ == "__main__":
    main()
