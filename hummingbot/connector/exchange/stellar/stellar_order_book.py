from typing import Dict, Optional

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_row import OrderBookRow


class StellarOrderBook(OrderBook):
    @classmethod
    def snapshot_message_from_exchange(
        cls, msg: Dict[str, any], timestamp: float, metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        processed_asks = [
            OrderBookRow(ask["price"], ask["amount"], ask["id"])
            for ask in msg.get("asks", [])
        ]
        processed_bids = [
            OrderBookRow(bid["price"], bid["amount"], bid["id"])
            for bid in msg.get("bids", [])
        ]

        content = {
            "trading_pair": msg["trading_pair"],
            "update_id": timestamp,
            "bids": processed_bids,
            "asks": processed_asks,
        }

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(
        cls, msg: Dict[str, any], timestamp: Optional[float] = None, metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        pass

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            {
                "trading_pair": msg["trading_pair"],
                "trade_type": msg["trade_type"],
                "trade_id": msg["trade_id"],
                "update_id": msg["update_id"],
                "price": msg["price"],
                "amount": msg["amount"],
            },
            timestamp=msg["timestamp"],
        )
