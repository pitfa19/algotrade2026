import unittest

import simple_mm_bot as bot


class SimpleMMBotTests(unittest.TestCase):
    def synced_state(self) -> bot.ExchangeState:
        state = bot.ExchangeState("NYSE")
        state.inventory_synced = True
        state.pending_orders_synced = True
        return state

    def test_default_configs_trade_card_everywhere_and_simp_only_when_profitable(self):
        configs = bot.default_configs()

        self.assertEqual({cfg.symbol for cfg in configs["NYSE"]}, {"CARD", "SIMP"})
        self.assertEqual([cfg.symbol for cfg in configs["JPX"]], ["CARD"])
        self.assertTrue(all(configs[exchange][0].symbol == "CARD" for exchange in configs))

    def test_accepting_resting_bid_immediately_reserves_cash(self):
        state = self.synced_state()
        order = bot.LiveOrder("x", 42, "NYSE-CARD", "bid", 9200, 100, "bid")

        state.track_order(order, reserve=True)

        self.assertEqual(state.reserved_cash, 920_000)
        self.assertEqual(state.free_cash(), bot.INITIAL_CASH - 920_000 - bot.CASH_FLOOR)

    def test_strategy_prioritizes_card_when_cash_is_tight(self):
        state = self.synced_state()
        state.cash = -4_000_000
        strategy = bot.SimpleMarketMaker(
            [
                bot.InstrumentConfig("NYSE", "CARD", 9200, 10550, 500, 20_000, 3_000),
                bot.InstrumentConfig("NYSE", "SIMP", 9950, 10000, 100, 10_000, 8_000),
            ]
        )

        orders = strategy.plan(
            state,
            {
                "NYSE-CARD": {"bids": {"10550": 100}, "asks": {"10560": 100}},
                "NYSE-SIMP": {"bids": {"10000": 100}, "asks": {"10010": 100}},
            },
            now_ms=1_000,
        )

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["instrument_id"], "NYSE-CARD")
        self.assertLessEqual(orders[0]["quantity"] * orders[0]["price"], 1_000_000)


if __name__ == "__main__":
    unittest.main()
