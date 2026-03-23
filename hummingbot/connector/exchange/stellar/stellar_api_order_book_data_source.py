import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.stellar import stellar_constants as CONSTANTS
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import InternalStellarOrderBook
from hummingbot.connector.exchange.stellar.stellar_ledger_stream import StellarLedgerEvent
from hummingbot.connector.exchange.stellar.stellar_order_book import StellarOrderBook
from hummingbot.connector.exchange.stellar.stellar_utils import trading_pair_to_assets
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.stellar.stellar_exchange import StellarExchange


class StellarAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Order book data source for the Stellar DEX.

    Instead of WebSocket subscriptions, this consumes events from the connector's
    shared Stellar ledger stream, updates local order book state, and emits
    snapshot / trade messages.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: List[str], connector: "StellarExchange", api_factory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._snapshot_messages_queue_key = CONSTANTS.SNAPSHOT_EVENT_TYPE
        self._internal_order_books: Dict[str, InternalStellarOrderBook] = {}
        self._subscription_queue: Optional[asyncio.Queue] = None

    async def get_last_traded_prices(self, trading_pairs: List[str], domain=None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    def _initialize_order_books(self):
        for trading_pair in self._trading_pairs:
            base_asset, quote_asset = trading_pair_to_assets(
                trading_pair, self._connector._all_markets
            )
            self._internal_order_books[trading_pair] = InternalStellarOrderBook(
                selling_asset=base_asset, buying_asset=quote_asset
            )

    def _process_ledger_event(self, event: StellarLedgerEvent):
        """Process a single ledger event for order-book changes and trades."""
        try:
            for trading_pair, order_book in self._internal_order_books.items():
                for change in event.order_changes:
                    order_book.apply_order_change(change)

            for trade in event.trades:
                trade_data = {
                    "trading_pair": trade["trading_pair"],
                    "trade": {
                        "price": trade["price"],
                        "amount": trade["amount"],
                        "trade_id": trade["trade_id"],
                        "trade_type": trade["trade_type"],
                        "update_id": trade["update_id"],
                        "timestamp": trade["timestamp"],
                    },
                }
                for tp in self._trading_pairs:
                    if tp == trade["trading_pair"]:
                        trade_data["trading_pair"] = tp
                        self._message_queue[CONSTANTS.TRADE_EVENT_TYPE].put_nowait(trade_data)
                        break
        except Exception as e:
            self.logger().error(f"Error processing ledger {event.ledger_sequence}: {e}")

    # -- subscription loop (overrides base WebSocket approach) ----------------

    async def listen_for_subscriptions(self):
        """Consume shared ledger events instead of polling RPC directly."""
        if not self._internal_order_books:
            self._initialize_order_books()
        self._subscription_queue = await self._connector._ledger_stream.subscribe()
        try:
            while True:
                event = await self._subscription_queue.get()
                self._process_ledger_event(event)
        except asyncio.CancelledError:
            raise
        finally:
            if self._subscription_queue is not None:
                await self._connector._ledger_stream.unsubscribe(self._subscription_queue)
                self._subscription_queue = None

    # -- snapshots ------------------------------------------------------------

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """Build an order book snapshot from internal state."""
        order_book = self._internal_order_books.get(trading_pair)
        if order_book is None:
            return {"asks": [], "bids": []}

        asks = [
            {"price": order.price, "amount": order.amount, "id": order.id}
            for order in order_book.get_sell_orders()[:CONSTANTS.ORDER_BOOK_DEPTH]
        ]
        bids = [
            {"price": order.price, "amount": order.amount, "id": order.id}
            for order in order_book.get_buy_orders()[:CONSTANTS.ORDER_BOOK_DEPTH]
        ]
        return {"asks": asks, "bids": bids}

    async def listen_for_order_book_snapshots(self, ev_loop, output: asyncio.Queue):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    snapshot_data = await self._request_order_book_snapshot(trading_pair)
                    snapshot_timestamp = time.time()
                    snapshot_msg = StellarOrderBook.snapshot_message_from_exchange(
                        msg=snapshot_data,
                        timestamp=snapshot_timestamp,
                        metadata={"trading_pair": trading_pair},
                    )
                    output.put_nowait(snapshot_msg)
                await self._sleep(CONSTANTS.REQUEST_ORDERBOOK_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error when processing order book snapshots")
                await self._sleep(CONSTANTS.REQUEST_ORDERBOOK_INTERVAL)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = time.time()
        return StellarOrderBook.snapshot_message_from_exchange(
            msg=snapshot,
            timestamp=snapshot_timestamp,
            metadata={"trading_pair": trading_pair},
        )

    # -- message parsers ------------------------------------------------------

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = raw_message["trading_pair"]
        trade = raw_message["trade"]
        msg = {
            "trading_pair": trading_pair,
            "price": trade["price"],
            "amount": trade["amount"],
            "update_id": trade.get("update_id", trade.get("timestamp", 0)),
            "trade_id": trade["trade_id"],
            "trade_type": trade["trade_type"],
            "timestamp": trade["timestamp"],
        }
        trade_message = StellarOrderBook.trade_message_from_exchange(msg)
        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message, message_queue):
        # Full snapshots are used instead of diffs for the Stellar DEX.
        pass

    # -- unused WebSocket stubs (required by base class) ----------------------

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("Stellar DEX uses ledger polling, not WebSocket")

    async def _subscribe_channels(self, ws):
        raise NotImplementedError("Stellar DEX uses ledger polling, not WebSocket")

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        raise NotImplementedError("Stellar DEX uses ledger polling, not WebSocket")

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """Dynamic subscription not supported for this connector."""
        self.logger().warning(
            f"Dynamic subscription not supported for {self.__class__.__name__}"
        )
        return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """Dynamic unsubscription not supported for this connector."""
        self.logger().warning(
            f"Dynamic unsubscription not supported for {self.__class__.__name__}"
        )
        return False
