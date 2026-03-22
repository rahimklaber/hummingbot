import asyncio
import base64
import logging
import time
from typing import TYPE_CHECKING, Optional, Set

from stellar_sdk import AiohttpClient, SorobanServerAsync
from stellar_sdk.xdr import LedgerCloseMeta

from hummingbot.connector.exchange.stellar import stellar_constants as CONSTANTS
from hummingbot.connector.exchange.stellar.stellar_auth import StellarAuth
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import (
    StellarOrderCreated,
    StellarOrderUpdated,
    get_ledger_close_time,
    get_ledger_entry_changes_for_ledger,
    get_order_changes_from_ledger_entry_changes,
    get_trades_from_ledger_entry_changes,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.stellar.stellar_exchange import StellarExchange

_logger: Optional[HummingbotLogger] = None


class StellarAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """User stream data source for the Stellar DEX connector.

    Polls Soroban RPC for new ledgers, filters ledger entry changes for the
    user's main account and channel accounts, and emits balance updates,
    order state changes, and trade events to a queue consumed by the exchange
    connector.
    """

    def __init__(self, auth: StellarAuth, connector: "StellarExchange"):
        super().__init__()
        self._connector = connector
        self._auth = auth
        self._last_recv_time: float = 0
        self._last_processed_ledger: int = 0
        self._monitored_accounts: Set[str] = set()

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
        """Poll ledgers and extract events relevant to the user's accounts."""
        while True:
            try:
                self._monitored_accounts = self._get_monitored_accounts()
                await self._poll_user_events(output)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in user stream polling: {e}", exc_info=True)
            await self._sleep(CONSTANTS.LEDGER_POLL_INTERVAL)

    async def _poll_user_events(self, output: asyncio.Queue):
        """Fetch new ledgers from Soroban RPC and extract user-relevant events."""
        server = SorobanServerAsync(
            server_url=self._connector._rpc_url,
            client=AiohttpClient(),
        )
        try:
            if self._last_processed_ledger == 0:
                latest = await server.get_latest_ledger()
                self._last_processed_ledger = latest.sequence - 1

            response = await server.get_ledgers(
                start_ledger=self._last_processed_ledger + 1,
                limit=CONSTANTS.GET_LEDGERS_BATCH_SIZE,
            )

            if not response.ledgers:
                return

            for ledger_info in response.ledgers:
                try:
                    meta = LedgerCloseMeta.from_xdr_bytes(base64.b64decode(ledger_info.metadata_xdr))
                    await self._process_ledger_for_user(meta, ledger_info.sequence, output)
                    self._last_processed_ledger = ledger_info.sequence
                    self._last_recv_time = time.time()
                except Exception as e:
                    self.logger().error(f"Error processing ledger {ledger_info.sequence} for user stream: {e}")
                    self._last_processed_ledger = ledger_info.sequence
        finally:
            await server.close()

    async def _process_ledger_for_user(self, meta: LedgerCloseMeta, ledger_sequence: int, output: asyncio.Queue):
        """Process ledger and emit events relevant to the user."""
        entry_changes = get_ledger_entry_changes_for_ledger(meta)
        order_changes = get_order_changes_from_ledger_entry_changes(entry_changes)
        trades = get_trades_from_ledger_entry_changes(entry_changes, meta)

        ledger_close_time = get_ledger_close_time(meta)

        # Filter order changes for our accounts
        for change in order_changes:
            seller_id = None
            if isinstance(change, StellarOrderCreated):
                seller_id = change.order.seller_id
            elif isinstance(change, StellarOrderUpdated):
                seller_id = change.order.seller_id
            # StellarOrderRemoved doesn't have seller_id, so we emit all removals.
            # The exchange connector will filter by tracked offer IDs.

            if seller_id is not None and seller_id not in self._monitored_accounts:
                continue

            event = {
                "type": "order_change",
                "change": change,
                "ledger_sequence": ledger_sequence,
                "ledger_close_time": ledger_close_time,
                "timestamp": time.time(),
            }
            output.put_nowait(event)

        # Signal that a new ledger was processed so the exchange can query balances
        balance_event = {
            "type": "balance_update",
            "ledger_sequence": ledger_sequence,
            "ledger_close_time": ledger_close_time,
            "timestamp": time.time(),
        }
        output.put_nowait(balance_event)

        # Emit trade events for the user's accounts
        for trade in trades:
            trade_event = {
                "type": "trade",
                "trade": trade,
                "ledger_sequence": ledger_sequence,
                "ledger_close_time": ledger_close_time,
                "timestamp": time.time(),
            }
            output.put_nowait(trade_event)

    async def _sleep(self, seconds: float):
        await asyncio.sleep(seconds)
