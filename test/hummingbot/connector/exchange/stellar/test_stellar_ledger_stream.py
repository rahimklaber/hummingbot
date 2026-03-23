import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.stellar.stellar_ledger_stream import StellarLedgerEvent, StellarLedgerStream


class TestStellarLedgerStream(unittest.IsolatedAsyncioTestCase):

    async def test_subscribe_and_unsubscribe_manage_subscribers(self):
        stream = StellarLedgerStream(rpc_url="https://soroban-testnet.stellar.org")
        stream.start = AsyncMock()

        subscriber = await stream.subscribe()

        self.assertIn(subscriber, stream._subscribers)
        stream.start.assert_awaited_once()

        await stream.unsubscribe(subscriber)

        self.assertNotIn(subscriber, stream._subscribers)

    async def test_broadcast_fans_out_to_all_subscribers(self):
        stream = StellarLedgerStream(rpc_url="https://soroban-testnet.stellar.org")
        subscriber_one = asyncio.Queue()
        subscriber_two = asyncio.Queue()
        stream._subscribers = {subscriber_one, subscriber_two}
        event = StellarLedgerEvent(
            ledger_sequence=1,
            ledger_close_time=2,
            meta=MagicMock(),
            entry_changes=[],
            order_changes=[],
            trades=[],
        )

        await stream._broadcast(event)

        self.assertIs(await subscriber_one.get(), event)
        self.assertIs(await subscriber_two.get(), event)


if __name__ == "__main__":
    unittest.main()
