import unittest

from hummingbot.connector.exchange.stellar.stellar_order_book import StellarOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.data_type.order_book_row import OrderBookRow


class TestStellarOrderBook(unittest.TestCase):

    def test_snapshot_message_from_exchange(self):
        msg = {
            "trading_pair": "USDC-XLM",
            "asks": [
                {"price": 1.5, "amount": 100.0, "id": 1},
                {"price": 1.6, "amount": 200.0, "id": 2},
            ],
            "bids": [
                {"price": 1.4, "amount": 50.0, "id": 3},
                {"price": 1.3, "amount": 75.0, "id": 4},
            ],
        }
        timestamp = 1700000000.0

        result = StellarOrderBook.snapshot_message_from_exchange(msg, timestamp)

        self.assertEqual(result.type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(result.timestamp, timestamp)
        self.assertEqual(result.content["trading_pair"], "USDC-XLM")
        self.assertEqual(result.content["update_id"], timestamp)

        asks = result.content["asks"]
        self.assertEqual(len(asks), 2)
        self.assertIsInstance(asks[0], OrderBookRow)
        self.assertEqual(asks[0].price, 1.5)
        self.assertEqual(asks[0].amount, 100.0)
        self.assertEqual(asks[0].update_id, 1)
        self.assertEqual(asks[1].price, 1.6)
        self.assertEqual(asks[1].amount, 200.0)
        self.assertEqual(asks[1].update_id, 2)

        bids = result.content["bids"]
        self.assertEqual(len(bids), 2)
        self.assertIsInstance(bids[0], OrderBookRow)
        self.assertEqual(bids[0].price, 1.4)
        self.assertEqual(bids[0].amount, 50.0)
        self.assertEqual(bids[0].update_id, 3)
        self.assertEqual(bids[1].price, 1.3)
        self.assertEqual(bids[1].amount, 75.0)
        self.assertEqual(bids[1].update_id, 4)

    def test_snapshot_message_empty(self):
        msg = {
            "trading_pair": "USDC-XLM",
            "asks": [],
            "bids": [],
        }
        timestamp = 1700000000.0

        result = StellarOrderBook.snapshot_message_from_exchange(msg, timestamp)

        self.assertEqual(result.type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(result.content["trading_pair"], "USDC-XLM")
        self.assertEqual(result.content["asks"], [])
        self.assertEqual(result.content["bids"], [])

    def test_snapshot_with_metadata(self):
        msg = {
            "asks": [{"price": 2.0, "amount": 10.0, "id": 5}],
            "bids": [],
        }
        metadata = {"trading_pair": "USDC-XLM"}
        timestamp = 1700000000.0

        result = StellarOrderBook.snapshot_message_from_exchange(msg, timestamp, metadata=metadata)

        self.assertEqual(result.type, OrderBookMessageType.SNAPSHOT)
        self.assertEqual(result.content["trading_pair"], "USDC-XLM")
        self.assertEqual(len(result.content["asks"]), 1)
        self.assertEqual(result.content["asks"][0].price, 2.0)

    def test_trade_message_from_exchange(self):
        msg = {
            "trading_pair": "USDC-XLM",
            "trade_type": 1.0,
            "trade_id": 12345,
            "update_id": 67890,
            "price": 1.5,
            "amount": 100.0,
            "timestamp": 1700000000.0,
        }

        result = StellarOrderBook.trade_message_from_exchange(msg)

        self.assertEqual(result.type, OrderBookMessageType.TRADE)
        self.assertEqual(result.timestamp, 1700000000.0)
        self.assertEqual(result.content["trading_pair"], "USDC-XLM")
        self.assertEqual(result.content["price"], 1.5)
        self.assertEqual(result.content["amount"], 100.0)
        self.assertEqual(result.content["trade_id"], 12345)
        self.assertEqual(result.content["trade_type"], 1.0)
        self.assertEqual(result.content["update_id"], 67890)


if __name__ == "__main__":
    unittest.main()
