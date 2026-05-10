import unittest

from parallax_smart import (
    CrossMedianOracle,
    ExchangeState,
    RailConfig,
    RailEdgeStrategy,
    default_rail_configs,
)


def synced_state(exchange: str = "NYSE") -> ExchangeState:
    state = ExchangeState(exchange=exchange)
    state.inventory_synced = True
    state.pending_orders_synced = True
    return state


def depth(bid: int = 9990, ask: int = 10010, qty: int = 100) -> dict[str, dict[str, int]]:
    return {"bids": {str(bid): qty}, "asks": {str(ask): qty}}


class CrossMedianOracleTests(unittest.TestCase):
    def test_returns_fresh_median_for_symbol_across_venues(self):
        oracle = CrossMedianOracle(min_venues=3, max_age_ms=1_000)
        oracle.update("NYSE-CARD", depth(9980, 10000), now_ms=10_000)
        oracle.update("NASDAQ-CARD", depth(9990, 10010), now_ms=10_000)
        oracle.update("ZSE-CARD", depth(10020, 10040), now_ms=10_000)

        self.assertEqual(oracle.median("CARD", now_ms=10_500), 10_000)

    def test_ignores_stale_venues_when_computing_median(self):
        oracle = CrossMedianOracle(min_venues=2, max_age_ms=200)
        oracle.update("NYSE-CARD", depth(9980, 10000), now_ms=1_000)
        oracle.update("NASDAQ-CARD", depth(9990, 10010), now_ms=1_000)
        oracle.update("ZSE-CARD", depth(10100, 10120), now_ms=2_000)

        self.assertIsNone(oracle.median("CARD", now_ms=2_000))


class SmartRailStrategyTests(unittest.TestCase):
    def test_flat_account_places_multiple_bid_rungs_below_cross_median(self):
        config = default_rail_configs()["NYSE"][0]
        oracle = CrossMedianOracle(min_venues=3, max_age_ms=1_000)
        for exchange in ("NYSE", "NASDAQ", "ZSE"):
            oracle.update(f"{exchange}-CARD", depth(9990, 10010), now_ms=5_000)

        state = synced_state("NYSE")
        strategy = RailEdgeStrategy(config, oracle)
        orders = strategy.plan_orders(state, depth(9990, 10010), now_ms=5_000)

        bids = [order for order in orders if order.get("side") == "bid"]
        self.assertEqual(
            [(order["price"], order["quantity"], order["role"]) for order in bids],
            [(9_970, 60, "rail"), (9_940, 80, "rail"), (9_900, 100, "rail")],
        )

    def test_long_inventory_closes_near_cross_median(self):
        config = default_rail_configs()["NYSE"][0]
        oracle = CrossMedianOracle(min_venues=3, max_age_ms=1_000)
        for exchange in ("NYSE", "NASDAQ", "ZSE"):
            oracle.update(f"{exchange}-CARD", depth(9990, 10010), now_ms=5_000)

        state = synced_state("NYSE")
        state.positions[config.instrument] = 50
        strategy = RailEdgeStrategy(config, oracle)
        orders = strategy.plan_orders(state, depth(9995, 10010, qty=50), now_ms=5_000)

        close = [order for order in orders if order.get("role") == "close"]
        self.assertEqual(len(close), 1)
        self.assertEqual(
            (close[0]["side"], close[0]["order_type"], close[0]["price"], close[0]["quantity"]),
            ("ask", "ioc", 9_980, 50),
        )

    def test_default_force_close_waits_for_sustained_stuck_inventory(self):
        config = default_rail_configs()["NYSE"][0]
        state = synced_state("NYSE")
        state.positions[config.instrument] = 50
        state.last_flat_ms[config.instrument] = 0
        strategy = RailEdgeStrategy(config)

        early = strategy.plan_orders(state, depth(9_500, 9_510), now_ms=10_000)
        late = strategy.plan_orders(state, depth(9_500, 9_510), now_ms=30_001)

        self.assertFalse(any(order.get("role") == "force_close" for order in early))
        self.assertTrue(any(order.get("role") == "force_close" for order in late))

    def test_live_rung_reprices_when_cross_median_moves(self):
        config = RailConfig(
            "NYSE",
            "CARD",
            bid_rungs=((30, 60),),
            rail_reprice_threshold=10,
            rail_reprice_min_age_ms=1_000,
        )
        oracle = CrossMedianOracle(min_venues=3, max_age_ms=1_000)
        for exchange in ("NYSE", "NASDAQ", "ZSE"):
            oracle.update(f"{exchange}-CARD", depth(10_090, 10_110), now_ms=5_000)

        state = synced_state("NYSE")
        state.track_order(
            local_id="old",
            order_id=7,
            instrument=config.instrument,
            side="bid",
            price=9_970,
            quantity=60,
            role="rail",
            created_ms=3_500,
        )
        strategy = RailEdgeStrategy(config, oracle)
        orders = strategy.plan_orders(state, depth(10_090, 10_110), now_ms=5_000)

        self.assertTrue(any(order.get("role") == "reprice_rail" for order in orders))


if __name__ == "__main__":
    unittest.main()
