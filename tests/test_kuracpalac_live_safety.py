import unittest

import kuracpalac as bot


def synced_state(exchange: str = "NASDAQ") -> bot.ExchangeState:
    state = bot.ExchangeState(exchange=exchange)
    state.inventory_synced = True
    state.pending_orders_synced = True
    return state


class KuracpalacLiveSafetyTests(unittest.TestCase):
    def test_resting_bid_ack_reserves_cash_immediately(self):
        state = synced_state()

        state.track_order(
            local_id="rail-1",
            order_id=1,
            instrument="NASDAQ-CARD",
            side="bid",
            price=9_700,
            quantity=200,
            role="rail",
            reserve=True,
        )

        self.assertEqual(state.reserved_cash, 1_940_000)
        self.assertEqual(state.free_cash(), 10_000_000 - 1_940_000 - bot.CASH_FLOOR)

    def test_passive_fill_releases_the_filled_bid_reservation(self):
        config = bot.default_rail_configs()["NASDAQ"]
        state = synced_state()
        state.track_order(
            local_id="rail-1",
            order_id=1,
            instrument=config.instrument,
            side="bid",
            price=config.low_bid_price,
            quantity=200,
            role="rail",
            reserve=True,
        )

        bot.RailEdgeStrategy(config).apply_trade_event(
            state,
            {"passiveOrderID": 1, "quantity": 75, "price": config.low_bid_price},
        )

        self.assertEqual(state.reserved_cash, config.low_bid_price * 125)
        self.assertEqual(state.position(config.instrument), 75)

    def test_cancel_response_releases_remaining_reservation(self):
        state = synced_state()
        state.track_order(
            local_id="rail-1",
            order_id=22,
            instrument="NASDAQ-CARD",
            side="bid",
            price=9_700,
            quantity=200,
            role="rail",
            reserve=True,
        )

        state.drop_order(22, release_reserved=True)

        self.assertEqual(state.reserved_cash, 0)
        self.assertNotIn(22, state.live_orders)

    def test_reprice_cancel_waits_for_minimum_age(self):
        config = bot.RailConfig(
            "ZSE",
            "CARD",
            9_500,
            11_000,
            10_600,
            10_100,
            lot_size=200,
            cross_median_mode="floor_bid_static_close",
            median_max_age_ms=5_000,
            rail_reprice_threshold=100,
            rail_reprice_min_age_ms=2_000,
        )
        oracle = bot.CrossMedianOracle()
        for exchange in ("NYSE", "NASDAQ", "ZSE"):
            oracle.update(
                f"{exchange}-CARD",
                {"bids": {"10995": 100}, "asks": {"11005": 100}},
                now_ms=10_000,
            )
        state = synced_state("ZSE")
        state.track_order(
            local_id="old",
            order_id=7,
            instrument=config.instrument,
            side="bid",
            price=9_500,
            quantity=200,
            role="rail",
            created_ms=10_000,
        )

        strategy = bot.RailEdgeStrategy(config, oracle)
        early = strategy.plan_orders(state, {"bids": {"10995": 100}, "asks": {"11005": 100}}, 11_000)
        late = strategy.plan_orders(state, {"bids": {"10995": 100}, "asks": {"11005": 100}}, 12_001)

        self.assertFalse(any(order.get("action") == "cancel" for order in early))
        self.assertTrue(any(order.get("role") == "reprice_rail" for order in late))


if __name__ == "__main__":
    unittest.main()
