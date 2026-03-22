import unittest
from unittest.async_case import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from stellar_sdk import Asset

from hummingbot.connector.exchange.stellar.stellar_api_order_book_data_source import StellarAPIOrderBookDataSource
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import (
    InternalStellarOrderBook,
    StellarOrder,
    StellarOrderCreated,
)
from hummingbot.connector.exchange.stellar.stellar_utils import StellarMarket
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

USDC_ISSUER = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"


def _make_mock_connector():
    connector = MagicMock()
    connector._rpc_url = "https://soroban-testnet.stellar.org"
    connector._custom_markets = {
        "USDC-XLM": StellarMarket(
            base="USDC",
            quote="XLM",
            base_issuer=USDC_ISSUER,
            quote_issuer="",
        ),
    }
    return connector


def _make_data_source(trading_pairs=None):
    if trading_pairs is None:
        trading_pairs = ["USDC-XLM"]
    connector = _make_mock_connector()
    api_factory = MagicMock()
    return StellarAPIOrderBookDataSource(trading_pairs, connector, api_factory)


class TestStellarAPIOrderBookDataSource(IsolatedAsyncioTestCase):

    def test_initialize_order_books(self):
        ds = _make_data_source(["USDC-XLM"])
        self.assertEqual(ds._internal_order_books, {})

        ds._initialize_order_books()

        self.assertIn("USDC-XLM", ds._internal_order_books)
        ob = ds._internal_order_books["USDC-XLM"]
        self.assertIsInstance(ob, InternalStellarOrderBook)
        self.assertEqual(ob.selling_asset, Asset("USDC", USDC_ISSUER))
        self.assertEqual(ob.buying_asset, Asset.native())

    async def test_request_order_book_snapshot_empty(self):
        ds = _make_data_source()
        ds._initialize_order_books()

        snapshot = await ds._request_order_book_snapshot("USDC-XLM")

        self.assertEqual(snapshot["asks"], [])
        self.assertEqual(snapshot["bids"], [])

    async def test_request_order_book_snapshot_missing_pair(self):
        ds = _make_data_source()
        # Don't initialize order books — pair won't exist
        snapshot = await ds._request_order_book_snapshot("USDC-XLM")

        self.assertEqual(snapshot, {"asks": [], "bids": []})

    async def test_request_order_book_snapshot_with_orders(self):
        ds = _make_data_source()
        ds._initialize_order_books()

        ob = ds._internal_order_books["USDC-XLM"]
        base_asset = Asset("USDC", USDC_ISSUER)
        quote_asset = Asset.native()

        # A sell order: selling base (USDC) for quote (XLM)
        sell_order = StellarOrder(
            id=100,
            selling_asset=base_asset,
            buying_asset=quote_asset,
            amount=500.0,
            price=1.5,
            seller_id="GABC",
        )
        ob.apply_order_change(StellarOrderCreated(order=sell_order))

        # A buy order: selling quote (XLM) for base (USDC)
        buy_order = StellarOrder(
            id=101,
            selling_asset=quote_asset,
            buying_asset=base_asset,
            amount=300.0,
            price=0.7,
            seller_id="GDEF",
        )
        ob.apply_order_change(StellarOrderCreated(order=buy_order))

        snapshot = await ds._request_order_book_snapshot("USDC-XLM")

        self.assertEqual(len(snapshot["asks"]), 1)
        ask = snapshot["asks"][0]
        self.assertEqual(ask["price"], 1.5)
        self.assertEqual(ask["amount"], 500.0)
        self.assertEqual(ask["id"], 100)

        self.assertEqual(len(snapshot["bids"]), 1)
        bid = snapshot["bids"][0]
        self.assertEqual(bid["id"], 101)

    async def test_order_book_snapshot_message(self):
        ds = _make_data_source()
        ds._initialize_order_books()

        ob = ds._internal_order_books["USDC-XLM"]
        base_asset = Asset("USDC", USDC_ISSUER)
        quote_asset = Asset.native()

        sell_order = StellarOrder(
            id=200,
            selling_asset=base_asset,
            buying_asset=quote_asset,
            amount=250.0,
            price=2.0,
            seller_id="GABC",
        )
        ob.apply_order_change(StellarOrderCreated(order=sell_order))

        msg = await ds._order_book_snapshot("USDC-XLM")

        self.assertEqual(msg.type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(msg.content["trading_pair"], "USDC-XLM")
        self.assertEqual(len(msg.content["asks"]), 1)
        self.assertEqual(msg.content["asks"][0].price, 2.0)
        self.assertEqual(msg.content["asks"][0].amount, 250.0)
        self.assertIsNotNone(msg.timestamp)


if __name__ == "__main__":
    unittest.main()
