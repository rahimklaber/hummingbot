import asyncio
import base64
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from stellar_sdk import AiohttpClient, SorobanServerAsync
from stellar_sdk.xdr import LedgerCloseMeta

from hummingbot.connector.exchange.stellar import stellar_constants as CONSTANTS
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import (
    DomainLedgerEntryChange,
    OrderChange,
    get_ledger_close_time,
    get_ledger_entry_changes_for_ledger,
    get_order_changes_from_ledger_entry_changes,
    get_trades_from_ledger_entry_changes,
)
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger


@dataclass
class StellarLedgerEvent:
    ledger_sequence: int
    ledger_close_time: int
    meta: LedgerCloseMeta
    entry_changes: List[DomainLedgerEntryChange]
    order_changes: List[OrderChange]
    trades: List[Dict[str, Any]]


class StellarLedgerStream:
    _logger: Optional[HummingbotLogger] = None

    def __init__(self, rpc_url: str):
        self._rpc_url = rpc_url
        self._subscribers: Set[asyncio.Queue] = set()
        self._subscribers_lock = asyncio.Lock()
        self._polling_task: Optional[asyncio.Task] = None
        self._last_processed_ledger: int = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    async def start(self):
        if self._polling_task is None or self._polling_task.done():
            self._polling_task = safe_ensure_future(self._poll_ledgers_loop())

    async def stop(self):
        if self._polling_task is not None:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
            self._polling_task = None
        async with self._subscribers_lock:
            self._subscribers.clear()

    async def subscribe(self) -> asyncio.Queue:
        queue = asyncio.Queue()
        async with self._subscribers_lock:
            self._subscribers.add(queue)
        await self.start()
        return queue

    async def unsubscribe(self, queue: asyncio.Queue):
        async with self._subscribers_lock:
            self._subscribers.discard(queue)

    async def _poll_ledgers_loop(self):
        server = SorobanServerAsync(
            server_url=self._rpc_url,
            client=AiohttpClient(),
        )
        try:
            while True:
                try:
                    if not await self._has_subscribers():
                        await self._sleep(CONSTANTS.LEDGER_POLL_INTERVAL)
                        continue

                    if self._last_processed_ledger == 0:
                        latest = await server.get_latest_ledger()
                        self._last_processed_ledger = latest.sequence - 1
                        self.logger().info(
                            f"Starting shared ledger polling from sequence {self._last_processed_ledger + 1}"
                        )

                    response = await server.get_ledgers(
                        start_ledger=self._last_processed_ledger + 1,
                        limit=CONSTANTS.GET_LEDGERS_BATCH_SIZE,
                    )

                    if response.ledgers:
                        for ledger_info in response.ledgers:
                            try:
                                event = self._build_ledger_event(ledger_info=ledger_info)
                                await self._broadcast(event)
                            except Exception:
                                self.logger().error(
                                    f"Error processing shared ledger {ledger_info.sequence}",
                                    exc_info=True,
                                )
                            finally:
                                self._last_processed_ledger = ledger_info.sequence
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().error("Error in shared Stellar ledger polling loop", exc_info=True)

                await self._sleep(CONSTANTS.LEDGER_POLL_INTERVAL)
        finally:
            await server.close()

    def _build_ledger_event(self, ledger_info) -> StellarLedgerEvent:
        meta = LedgerCloseMeta.from_xdr_bytes(base64.b64decode(ledger_info.metadata_xdr))
        entry_changes = get_ledger_entry_changes_for_ledger(meta)
        order_changes = get_order_changes_from_ledger_entry_changes(entry_changes)
        trades = get_trades_from_ledger_entry_changes(entry_changes, meta)
        ledger_close_time = get_ledger_close_time(meta)
        return StellarLedgerEvent(
            ledger_sequence=ledger_info.sequence,
            ledger_close_time=ledger_close_time,
            meta=meta,
            entry_changes=entry_changes,
            order_changes=order_changes,
            trades=trades,
        )

    async def _broadcast(self, event: StellarLedgerEvent):
        async with self._subscribers_lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.put_nowait(event)

    async def _has_subscribers(self) -> bool:
        async with self._subscribers_lock:
            return len(self._subscribers) > 0

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)
