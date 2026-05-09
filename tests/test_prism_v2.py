import unittest

from prism_v2 import ARB_EDGE, Hub, VolTracker


def depth(bids: dict[int, int], asks: dict[int, int]) -> dict[str, dict[str, int]]:
    return {
        "bids": {str(price): qty for price, qty in bids.items()},
        "asks": {str(price): qty for price, qty in asks.items()},
    }


class PrismV2StrategyTests(unittest.TestCase):
    def test_etf_basket_arb_walks_depth_when_larger_trade_still_has_edge(self):
        hub = Hub(["NYSE", "ZSE", "TMX"])

        hub.books[("NYSE", "ETFA3")].update(
            depth(
                bids={9950: 20},
                asks={9970: 3, 9971: 3, 9972: 3},
            )
        )
        for ticker in ("NGUP", "KTST", "XFR"):
            hub.books[("ZSE", ticker)].update(
                depth(
                    bids={10000: 3, 9999: 3, 9998: 3},
                    asks={10020: 9},
                )
            )

        plans = [
            plan
            for plan in hub.etf_basket_arbs()
            if plan.strategy == "ETF-NAV ETFA3@NYSE long"
        ]

        self.assertTrue(plans)
        best = plans[0]
        self.assertEqual(best.legs[0].ticker, "ETFA3")
        self.assertEqual(best.legs[0].qty, 9)
        self.assertEqual(best.legs[0].price, 9972)
        self.assertEqual({leg.qty for leg in best.legs[1:]}, {3})

    def test_adaptive_edge_floor_widens_when_ticker_is_volatile(self):
        tracker = VolTracker()
        for mid in (10000, 10040, 9960, 10080):
            tracker.update(mid)

        hub = Hub(["ZSE"])
        hub.vol["CARD"] = tracker

        self.assertGreater(hub.edge_for("CARD", floor=ARB_EDGE), ARB_EDGE)


if __name__ == "__main__":
    unittest.main()
