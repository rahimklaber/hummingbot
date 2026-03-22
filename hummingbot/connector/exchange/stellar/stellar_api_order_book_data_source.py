import asyncio
import base64
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from stellar_sdk import AiohttpClient, SorobanServerAsync

from hummingbot.connector.exchange.stellar import stellar_constants as CONSTANTS
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import (
    InternalStellarOrderBook,
    get_ledger_entry_changes_for_ledger,
    get_order_changes_from_ledger_entry_changes,
    get_trades_from_ledger_entry_changes,
)
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

    Instead of WebSocket subscriptions, this polls Soroban RPC ``get_ledgers()``
    for new ledgers, parses ``LedgerCloseMeta`` to extract order book changes
    and trades, and emits snapshot / trade messages.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: List[str], connector: "StellarExchange", api_factory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._snapshot_messages_queue_key = CONSTANTS.SNAPSHOT_EVENT_TYPE
        self._last_processed_ledger: int = 0
        self._internal_order_books: Dict[str, InternalStellarOrderBook] = {}

    # -- helpers --------------------------------------------------------------

    async def get_last_traded_prices(self, trading_pairs: List[str], domain=None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    def _get_soroban_server(self) -> SorobanServerAsync:
        return SorobanServerAsync(
            server_url=self._connector._rpc_url,
            client=AiohttpClient(),
        )

    def _initialize_order_books(self):
        for trading_pair in self._trading_pairs:
            base_asset, quote_asset = trading_pair_to_assets(
                trading_pair, self._connector._custom_markets
            )
            self._internal_order_books[trading_pair] = InternalStellarOrderBook(
                selling_asset=base_asset, buying_asset=quote_asset
            )

    # -- ledger polling -------------------------------------------------------

    async def _poll_ledgers(self):
        """Poll for new ledgers and process them."""
        if not self._internal_order_books:
            self._initialize_order_books()

        server = self._get_soroban_server()
        try:
            if self._last_processed_ledger == 0:
                latest = await server.get_latest_ledger()
                self._last_processed_ledger = latest.sequence - 1
                self.logger().info(
                    f"Starting ledger polling from sequence {self._last_processed_ledger + 1}"
                )

            response = await server.get_ledgers(
                start_ledger=self._last_processed_ledger + 1,
                limit=CONSTANTS.GET_LEDGERS_BATCH_SIZE,
            )

            if not response.ledgers:
                return

            for ledger_info in response.ledgers:
                try:
                    from stellar_sdk.xdr import LedgerCloseMeta

                    meta = LedgerCloseMeta.from_xdr_bytes(
                        base64.b64decode(ledger_info.metadata_xdr)
                    )
                    self._process_ledger(meta, ledger_info.sequence)
                    self._last_processed_ledger = ledger_info.sequence
                except Exception as e:
                    self.logger().error(
                        f"Error processing ledger {ledger_info.sequence}: {e}"
                    )
                    self._last_processed_ledger = ledger_info.sequence
        finally:
            await server.close()

    def _process_ledger(self, meta, ledger_sequence: int):
        """Process a single ledger's close meta for order-book changes and trades."""
        try:
            entry_changes = get_ledger_entry_changes_for_ledger(meta)
            order_changes = get_order_changes_from_ledger_entry_changes(entry_changes)
            trades = get_trades_from_ledger_entry_changes(entry_changes, meta)

            for trading_pair, order_book in self._internal_order_books.items():
                for change in order_changes:
                    order_book.apply_order_change(change)

            for trade in trades:
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
            self.logger().error(f"Error processing ledger {ledger_sequence}: {e}")

    # -- subscription loop (overrides base WebSocket approach) ----------------

    async def listen_for_subscriptions(self):
        """Main loop that polls for new ledgers instead of using WebSocket."""
        while True:
            try:
                await self._poll_ledgers()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in ledger polling: {e}", exc_info=True)
            await self._sleep(CONSTANTS.LEDGER_POLL_INTERVAL)

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
            "transact_time": trade["update_id"],
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
