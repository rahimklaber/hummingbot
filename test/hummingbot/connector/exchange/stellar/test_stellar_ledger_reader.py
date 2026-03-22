import unittest

from stellar_sdk import Asset

from hummingbot.connector.exchange.stellar.stellar_ledger_reader import (
    InternalStellarOrderBook,
    StellarOrder,
    StellarOrderCreated,
    StellarOrderRemoved,
    StellarOrderUpdated,
)

USDC = Asset("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
XLM = Asset.native()


def _make_order(
    order_id: int,
    selling: Asset = USDC,
    buying: Asset = XLM,
    amount: float = 100.0,
    price: float = 0.5,
    seller: str = "GABC",
    tx_hash: str = None,
    op_index: int = None,
) -> StellarOrder:
    return StellarOrder(
        id=order_id,
        selling_asset=selling,
        buying_asset=buying,
        amount=amount,
        price=price,
        seller_id=seller,
        created_in_tx=tx_hash,
        created_in_op_index=op_index,
    )


class TestInternalStellarOrderBook(unittest.TestCase):
    def setUp(self):
        self.book = InternalStellarOrderBook(selling_asset=USDC, buying_asset=XLM)

    # 1
    def test_order_creation(self):
        creation = StellarOrderCreated(order=_make_order(1))
        self.book.apply_order_creation(creation)
        self.assertEqual(self.book.get_order_count(), 1)
        self.assertIn(1, self.book.orders)

    # 2
    def test_order_creation_wrong_pair(self):
        wrong_asset = Asset("BTC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN")
        creation = StellarOrderCreated(order=_make_order(1, selling=wrong_asset, buying=wrong_asset))
        self.book.apply_order_creation(creation)
        self.assertEqual(self.book.get_order_count(), 0)

    # 3
    def test_order_update(self):
        self.book.apply_order_creation(StellarOrderCreated(order=_make_order(1, amount=100.0)))
        updated_order = _make_order(1, amount=50.0)
        self.book.apply_order_update(StellarOrderUpdated(order=updated_order, switched=False))
        self.assertEqual(self.book.orders[1].amount, 50.0)

    # 4
    def test_order_update_preserves_creation_metadata(self):
        original = _make_order(1, tx_hash="abc123", op_index=2)
        self.book.apply_order_creation(StellarOrderCreated(order=original))
        updated = _make_order(1, amount=50.0)
        self.book.apply_order_update(StellarOrderUpdated(order=updated, switched=False))
        self.assertEqual(self.book.orders[1].created_in_tx, "abc123")
        self.assertEqual(self.book.orders[1].created_in_op_index, 2)

    # 5
    def test_order_removal(self):
        self.book.apply_order_creation(StellarOrderCreated(order=_make_order(1)))
        self.book.apply_order_removal(StellarOrderRemoved(id=1))
        self.assertEqual(self.book.get_order_count(), 0)

    # 6
    def test_order_removal_nonexistent(self):
        self.book.apply_order_removal(StellarOrderRemoved(id=999))
        self.assertEqual(self.book.get_order_count(), 0)

    # 7
    def test_apply_order_change_creation(self):
        result = self.book.apply_order_change(StellarOrderCreated(order=_make_order(1)))
        self.assertTrue(result)
        self.assertEqual(self.book.get_order_count(), 1)

    # 8
    def test_apply_order_change_removal(self):
        self.book.apply_order_creation(StellarOrderCreated(order=_make_order(1)))
        result = self.book.apply_order_change(StellarOrderRemoved(id=1))
        self.assertTrue(result)
        self.assertEqual(self.book.get_order_count(), 0)

    # 9 – buy orders: buying_asset == selling_asset of book (USDC)
    def test_get_buy_orders_sorted(self):
        for i, price in enumerate([0.3, 0.5, 0.1], start=1):
            order = _make_order(i, selling=XLM, buying=USDC, price=price)
            self.book.apply_order_creation(StellarOrderCreated(order=order))
        buys = self.book.get_buy_orders()
        prices = [o.price for o in buys]
        self.assertEqual(prices, sorted(prices, reverse=True))

    # 10 – sell orders: selling_asset == selling_asset of book (USDC)
    def test_get_sell_orders_sorted(self):
        for i, price in enumerate([0.5, 0.1, 0.3], start=1):
            order = _make_order(i, selling=USDC, buying=XLM, price=price)
            self.book.apply_order_creation(StellarOrderCreated(order=order))
        sells = self.book.get_sell_orders()
        prices = [o.price for o in sells]
        self.assertEqual(prices, sorted(prices))

    # 11
    def test_get_best_bid_ask(self):
        # sell orders (asks)
        self.book.apply_order_creation(
            StellarOrderCreated(order=_make_order(1, selling=USDC, buying=XLM, price=0.5)))
        self.book.apply_order_creation(
            StellarOrderCreated(order=_make_order(2, selling=USDC, buying=XLM, price=0.3)))
        # buy orders (bids)
        self.book.apply_order_creation(
            StellarOrderCreated(order=_make_order(3, selling=XLM, buying=USDC, price=0.2)))
        self.book.apply_order_creation(
            StellarOrderCreated(order=_make_order(4, selling=XLM, buying=USDC, price=0.1)))

        best_ask = self.book.get_best_ask()
        best_bid = self.book.get_best_bid()
        self.assertEqual(best_ask.price, 0.3)
        # buy orders get price inverted (1/price) because selling_asset != book selling_asset
        self.assertEqual(best_bid.price, 1 / 0.1)

    # 12
    def test_get_spread(self):
        self.book.apply_order_creation(
            StellarOrderCreated(order=_make_order(1, selling=USDC, buying=XLM, price=0.5)))
        self.book.apply_order_creation(
            StellarOrderCreated(order=_make_order(2, selling=XLM, buying=USDC, price=0.3)))
        spread = self.book.get_spread()
        # ask price = 0.5, bid price = 1/0.3
        self.assertIsNotNone(spread)
        self.assertAlmostEqual(spread, 0.5 - (1 / 0.3), places=10)

    # 13
    def test_empty_order_book(self):
        self.assertIsNone(self.book.get_best_bid())
        self.assertIsNone(self.book.get_best_ask())
        self.assertIsNone(self.book.get_spread())


if __name__ == "__main__":
    unittest.main()
