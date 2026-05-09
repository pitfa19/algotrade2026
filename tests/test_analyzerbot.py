import csv
import tempfile
import unittest
from pathlib import Path

from analyzerbot import analyze_market_data


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


class AnalyzerBotTests(unittest.TestCase):
    def test_finds_etf_dislocation_from_zse_components(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_csv(
                data_dir / "ZSE_orderbooks.csv",
                [
                    quote_row(1000, "ZSE-KOTD", 10000, 10004),
                    quote_row(1000, "ZSE-INA", 10000, 10004),
                    quote_row(1000, "ZSE-DLKV", 10000, 10004),
                ],
            )
            write_csv(
                data_dir / "NASDAQ_orderbooks.csv",
                [quote_row(1001, "NASDAQ-ETFB3", 10090, 10100)],
            )

            report = analyze_market_data(data_dir, top_n=5, sync_tolerance_ms=25)

            best = report["top_etf_dislocations"][0]
            self.assertEqual(best["exchange"], "NASDAQ")
            self.assertEqual(best["ticker"], "ETFB3")
            self.assertEqual(best["side"], "SELL")
            self.assertAlmostEqual(best["edge_cents"], 88.0)

    def test_finds_recurring_cross_venue_route(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_csv(
                data_dir / "HKEX_orderbooks.csv",
                [
                    quote_row(1000, "HKEX-INA", 9000, 9010),
                    quote_row(1100, "HKEX-INA", 9005, 9015),
                ],
            )
            write_csv(
                data_dir / "NASDAQ_orderbooks.csv",
                [
                    quote_row(1001, "NASDAQ-INA", 9100, 9110),
                    quote_row(1101, "NASDAQ-INA", 9120, 9130),
                ],
            )

            report = analyze_market_data(data_dir, top_n=5, sync_tolerance_ms=25)

            route = report["recurring_cross_routes"][0]
            self.assertEqual(route["ticker"], "INA")
            self.assertEqual(route["buy_exchange"], "HKEX")
            self.assertEqual(route["sell_exchange"], "NASDAQ")
            self.assertEqual(route["occurrences"], 2)
            self.assertGreater(route["avg_edge_cents"], 90)

    def test_computes_cancel_pressure_and_special_moves(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            write_csv(
                data_dir / "NYSE_events.csv",
                [
                    {"time": 1000, "instrument": "NYSE-CARD", "order_id": 1, "expired": "False"},
                    {"time": 1001, "instrument": "NYSE-CARD", "order_id": 2, "expired": "False"},
                    {"time": 1002, "instrument": "NYSE-CARD", "order_id": 3, "expired": "True"},
                ],
            )
            write_csv(
                data_dir / "NYSE_trades.csv",
                [
                    {
                        "time": 1000,
                        "instrument": "NYSE-CARD",
                        "price": 10000,
                        "quantity": 10,
                        "passive_order_id": 10,
                        "active_order_id": 11,
                    }
                ],
            )
            write_csv(
                data_dir / "NYSE_candles.csv",
                [
                    candle_row(1, "NYSE-CARD", 10000, 10000),
                    candle_row(2, "NYSE-CARD", 10000, 11000),
                ],
            )

            report = analyze_market_data(data_dir, top_n=5)

            pressure = report["cancel_pressure"][0]
            self.assertEqual(pressure["instrument"], "NYSE-CARD")
            self.assertEqual(pressure["cancels"], 3)
            self.assertEqual(pressure["nonexpired_cancels"], 2)
            self.assertEqual(pressure["trades"], 1)
            move = report["special_moves"][0]
            self.assertEqual(move["instrument"], "NYSE-CARD")
            self.assertEqual(move["return_bps"], 1000.0)


def quote_row(time: int, instrument: str, bid: int, ask: int) -> dict[str, object]:
    return {
        "time": time,
        "instrument": instrument,
        "bid1_price": bid,
        "bid1_qty": 50,
        "bid2_price": bid - 1,
        "bid2_qty": 50,
        "bid3_price": bid - 2,
        "bid3_qty": 50,
        "ask1_price": ask,
        "ask1_qty": 50,
        "ask2_price": ask + 1,
        "ask2_qty": 50,
        "ask3_price": ask + 2,
        "ask3_qty": 50,
    }


def candle_row(index: int, instrument: str, open_price: int, close_price: int) -> dict[str, object]:
    return {
        "index": index,
        "instrument": instrument,
        "open": open_price,
        "high": max(open_price, close_price),
        "low": min(open_price, close_price),
        "close": close_price,
        "volume": 10,
    }


if __name__ == "__main__":
    unittest.main()
