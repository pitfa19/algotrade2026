import unittest

from edgehammer import (
    Book,
    EdgeHammerEngine,
    ExchangeClient,
    Opportunity,
    OrderLeg,
    SimAccount,
    StrategyConfig,
    clip_opportunity_for_live,
    find_cross_venue_arbs,
    find_local_basket_arbs,
    special_target,
)


class EdgeHammerCoreTests(unittest.TestCase):
    def test_local_basket_arb_uses_n_etf_shares_per_component_basket(self):
        books = {
            "ZSE-ETFA3": Book(bids=[(89, 9)], asks=[(90, 9)]),
            "ZSE-NGUP": Book(bids=[(100, 5)], asks=[(101, 5)]),
            "ZSE-KTST": Book(bids=[(100, 5)], asks=[(101, 5)]),
            "ZSE-XFR": Book(bids=[(100, 5)], asks=[(101, 5)]),
        }
        accounts = {"ZSE": SimAccount()}

        opps = find_local_basket_arbs("ZSE", books, accounts, min_edge=0)

        self.assertEqual(len(opps), 1)
        opp = opps[0]
        self.assertEqual(opp.label, "cheap:ETFA3")
        self.assertEqual(opp.edge_cents, 90)
        self.assertEqual([(leg.side, leg.instrument_id, leg.quantity, leg.price) for leg in opp.legs], [
            ("bid", "ZSE-ETFA3", 9, 90),
            ("ask", "ZSE-NGUP", 3, 100),
            ("ask", "ZSE-KTST", 3, 100),
            ("ask", "ZSE-XFR", 3, 100),
        ])

    def test_cross_venue_arb_pairs_same_ticker_iocs(self):
        states = {
            "NYSE": {"NYSE-CARD": Book(bids=[(99, 50)], asks=[(100, 40)])},
            "NASDAQ": {"NASDAQ-CARD": Book(bids=[(106, 30)], asks=[(107, 50)])},
        }
        accounts = {"NYSE": SimAccount(), "NASDAQ": SimAccount()}

        opps = find_cross_venue_arbs(states, accounts, venues=["NYSE", "NASDAQ"], min_edge=3)

        self.assertEqual(len(opps), 1)
        opp = opps[0]
        self.assertEqual(opp.label, "cross:CARD:NYSE->NASDAQ")
        self.assertEqual(opp.edge_cents, 180)
        self.assertEqual([(leg.side, leg.instrument_id, leg.quantity, leg.price) for leg in opp.legs], [
            ("bid", "NYSE-CARD", 30, 100),
            ("ask", "NASDAQ-CARD", 30, 106),
        ])

    def test_special_target_mean_reverts_around_anchor(self):
        self.assertEqual(special_target(mid=9400, current_position=0, anchor=10000, enter=500, exit=100), 2000)
        self.assertEqual(special_target(mid=10600, current_position=0, anchor=10000, enter=500, exit=100), -200)
        self.assertEqual(special_target(mid=10050, current_position=1700, anchor=10000, enter=500, exit=100), 0)
        self.assertEqual(special_target(mid=10250, current_position=1700, anchor=10000, enter=500, exit=100), 1700)

    def test_sim_account_respects_cash_and_position_limits(self):
        account = SimAccount(cash=10_000)
        filled = account.apply_ioc("NYSE-CARD", "bid", Book(bids=[], asks=[(100, 200)]), 200)
        self.assertEqual(filled, 200)
        self.assertEqual(account.cash, -10_000)
        self.assertEqual(account.positions["NYSE-CARD"], 200)

        filled = account.apply_ioc("NYSE-CARD", "ask", Book(bids=[(100, 500)], asks=[]), 5000)
        self.assertEqual(filled, 400)
        self.assertEqual(account.positions["NYSE-CARD"], -200)

    def test_live_clip_skips_stale_sell_when_short_capacity_is_gone(self):
        states = {"NASDAQ": {"NASDAQ-CARD": Book(bids=[(106, 200)], asks=[(107, 200)])}}
        account = SimAccount()
        account.positions["NASDAQ-CARD"] = -200
        accounts = {"NASDAQ": account}
        opp = Opportunity(
            "cross:CARD:NYSE->NASDAQ",
            1000,
            [OrderLeg("NASDAQ", "NASDAQ-CARD", "ask", 200, 106)],
        )

        self.assertIsNone(clip_opportunity_for_live(opp, states, accounts))

    def test_live_clip_resizes_paired_cross_to_current_sell_capacity(self):
        states = {
            "NYSE": {"NYSE-CARD": Book(bids=[(99, 200)], asks=[(100, 200)])},
            "NASDAQ": {"NASDAQ-CARD": Book(bids=[(106, 200)], asks=[(107, 200)])},
        }
        accounts = {"NYSE": SimAccount(), "NASDAQ": SimAccount()}
        accounts["NASDAQ"].positions["NASDAQ-CARD"] = -150
        opp = Opportunity(
            "cross:CARD:NYSE->NASDAQ",
            1200,
            [
                OrderLeg("NYSE", "NYSE-CARD", "bid", 200, 100),
                OrderLeg("NASDAQ", "NASDAQ-CARD", "ask", 200, 106),
            ],
        )

        clipped = clip_opportunity_for_live(opp, states, accounts)

        self.assertIsNotNone(clipped)
        self.assertEqual([leg.quantity for leg in clipped.legs], [50, 50])

    def test_inventory_sync_clears_stale_positions_for_exchange(self):
        engine = EdgeHammerEngine(StrategyConfig(venues=("NYSE",)))
        client = ExchangeClient("NYSE", engine)
        account = engine.accounts["NYSE"]
        account.cash = 1
        account.positions["NYSE-CARD"] = 100

        client.apply_inventory({"$": [0, 10_000_000]})

        self.assertTrue(client.inventory_ready)
        self.assertEqual(account.cash, 10_000_000)
        self.assertEqual(account.positions["NYSE-CARD"], 0)


if __name__ == "__main__":
    unittest.main()
