import unittest

from god_bot import (
    ExchangeState,
    LatencyModel,
    OrderBook,
    OrderIntent,
    RiskConfig,
    RiskManager,
    equal_weight_fair_cents,
)


class GodBotCoreTests(unittest.TestCase):
    def test_equal_weight_fair_uses_integer_cents(self):
        self.assertEqual(equal_weight_fair_cents([10000, 10003, 10004]), 10002)
        self.assertEqual(equal_weight_fair_cents([10001, 10002]), 10002)

    def test_microprice_moves_toward_ask_when_bid_depth_dominates(self):
        book = OrderBook.from_depth(
            {
                "bids": {"10000": 80, "9999": 10},
                "asks": {"10010": 20, "10011": 10},
            },
            now_ms=123,
        )

        self.assertEqual(book.best_bid, 10000)
        self.assertEqual(book.best_ask, 10010)
        self.assertEqual(book.microprice(), 10008)

    def test_risk_manager_blocks_short_floor_and_cash_floor(self):
        state = ExchangeState(exchange="ZSE")
        state.cash_total = -4_990_000
        state.positions["ZSE-CARD"] = -199
        risk = RiskManager(RiskConfig(max_order_qty=50))

        sell = OrderIntent(
            exchange="ZSE",
            instrument_id="ZSE-CARD",
            side="ask",
            quantity=5,
            price=10_000,
            order_type="ioc",
            strategy="test",
            reason="would breach short floor",
        )
        buy = OrderIntent(
            exchange="ZSE",
            instrument_id="ZSE-CARD",
            side="bid",
            quantity=2,
            price=10_000,
            order_type="ioc",
            strategy="test",
            reason="would breach cash floor",
        )

        self.assertFalse(risk.approve(state, sell).ok)
        self.assertFalse(risk.approve(state, buy).ok)

    def test_latency_model_ranks_venues_for_current_location(self):
        model = LatencyModel()

        self.assertEqual(
            model.rank("NYSE", ["HKEX", "ZSE", "NASDAQ", "NYSE"]),
            ["NYSE", "NASDAQ", "ZSE", "HKEX"],
        )
        self.assertEqual(
            model.rank("HKEX", ["NYSE", "SSE", "JPX", "HKEX"]),
            ["HKEX", "SSE", "JPX", "NYSE"],
        )


if __name__ == "__main__":
    unittest.main()
