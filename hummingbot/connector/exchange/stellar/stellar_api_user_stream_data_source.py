import asyncio
import logging
import time
from typing import TYPE_CHECKING, Optional, Set

from hummingbot.connector.exchange.stellar.stellar_auth import StellarAuth
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import StellarOrderCreated, StellarOrderUpdated
from hummingbot.connector.exchange.stellar.stellar_ledger_stream import StellarLedgerEvent
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.stellar.stellar_exchange import StellarExchange

_logger: Optional[HummingbotLogger] = None


class StellarAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """User stream data source for the Stellar DEX connector.

    Consumes shared ledger events, filters them for the user's main account
    and channel accounts, and emits balance updates, order state changes,
    and trade events to a queue consumed by the exchange connector.
    """

    def __init__(self, auth: StellarAuth, connector: "StellarExchange"):
        super().__init__()
        self._connector = connector
        self._auth = auth
        self._last_recv_time: float = 0
        self._monitored_accounts: Set[str] = set()
        self._subscription_queue: Optional[asyncio.Queue] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _logger
        if _logger is None:
            _logger = logging.getLogger(__name__)
        return _logger

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    def _get_monitored_accounts(self) -> Set[str]:
        """Get set of account IDs to monitor (main account + channel accounts)."""
        accounts = {self._auth.get_account_id()}
        if hasattr(self._connector, '_channel_pool') and self._connector._channel_pool is not None:
            for channel in self._connector._channel_pool._channels:
                accounts.add(channel.keypair.public_key)
        return accounts

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Consume shared ledger events and emit events relevant to the user's accounts."""
        self._subscription_queue = await self._connector._ledger_stream.subscribe()
        try:
            while True:
                self._monitored_accounts = self._get_monitored_accounts()
                event = await self._subscription_queue.get()
                await self._process_ledger_for_user(event, output)
                self._last_recv_time = time.time()
        except asyncio.CancelledError:
            raise
        finally:
            if self._subscription_queue is not None:
                await self._connector._ledger_stream.unsubscribe(self._subscription_queue)
                self._subscription_queue = None

    async def _process_ledger_for_user(self, ledger_event: StellarLedgerEvent, output: asyncio.Queue):
        """Process a shared ledger event and emit events relevant to the user."""
        for trade in ledger_event.trades:
            if trade.get("seller_id") not in self._monitored_accounts:
                continue
            trade_event = {
                "type": "trade",
                "trade": trade,
                "ledger_sequence": ledger_event.ledger_sequence,
                "ledger_close_time": ledger_event.ledger_close_time,
                "timestamp": time.time(),
            }
            output.put_nowait(trade_event)

        for change in ledger_event.order_changes:
            seller_id = None
            if isinstance(change, StellarOrderCreated):
                seller_id = change.order.seller_id
            elif isinstance(change, StellarOrderUpdated):
                seller_id = change.order.seller_id
            # StellarOrderRemoved doesn't have seller_id, so we emit all removals.
            # The exchange connector will filter by tracked offer IDs.

            if seller_id is not None and seller_id not in self._monitored_accounts:
                continue

            order_event = {
                "type": "order_change",
                "change": change,
                "ledger_sequence": ledger_event.ledger_sequence,
                "ledger_close_time": ledger_event.ledger_close_time,
                "timestamp": time.time(),
            }
            output.put_nowait(order_event)

        balance_event = {
            "type": "balance_update",
            "ledger_sequence": ledger_event.ledger_sequence,
            "ledger_close_time": ledger_event.ledger_close_time,
            "timestamp": time.time(),
        }
        output.put_nowait(balance_event)
