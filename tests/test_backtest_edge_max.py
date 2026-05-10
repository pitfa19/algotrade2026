import unittest

from tools.backtest_edge_max import BacktestBook, SimAccount


class EdgeMaxBacktestTests(unittest.TestCase):
    def test_ioc_buy_fills_through_visible_asks_and_updates_cash(self):
        account = SimAccount()
        book = BacktestBook(
            time_ms=1000,
            instrument="ZSE-CARD",
            bids=((10000, 50),),
            asks=((10010, 40), (10020, 50), (10030, 60)),
        )

        filled = account.execute_ioc(book, "bid", 10020, 100)

        self.assertEqual(filled, 90)
        self.assertEqual(account.positions["ZSE-CARD"], 90)
        self.assertEqual(account.cash["ZSE"], 10_000_000 - 40 * 10010 - 50 * 10020)

    def test_passive_bid_fill_then_ioc_unwind_realizes_profit(self):
        account = SimAccount()
        order_id = account.place_limit("ZSE-CARD", "bid", 9000, 100)
        account.passive_fill(order_id, 40, 9000)
        book = BacktestBook(
            time_ms=1100,
            instrument="ZSE-CARD",
            bids=((9100, 50),),
            asks=((9110, 50),),
        )

        filled = account.execute_ioc(book, "ask", 9001, 40)

        self.assertEqual(filled, 40)
        self.assertEqual(account.realized_cents, 40 * 100)
        self.assertEqual(account.positions["ZSE-CARD"], 0)


if __name__ == "__main__":
    unittest.main()
