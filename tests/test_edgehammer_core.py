import unittest

from edgehammer import (
    Book,
    SimAccount,
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


if __name__ == "__main__":
    unittest.main()
