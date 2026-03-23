import asyncio
import unittest
from decimal import Decimal
from typing import Set
from unittest.mock import AsyncMock, MagicMock

from stellar_sdk import Asset, Keypair

from hummingbot.connector.exchange.stellar.stellar_api_user_stream_data_source import StellarAPIUserStreamDataSource
from hummingbot.connector.exchange.stellar.stellar_auth import StellarAuth
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import StellarOrder, StellarOrderCreated
from hummingbot.connector.exchange.stellar.stellar_ledger_stream import StellarLedgerEvent
from hummingbot.connector.exchange.stellar.stellar_utils import ChannelAccount


class TestStellarAPIUserStreamDataSource(unittest.IsolatedAsyncioTestCase):

    def _make_mock_auth(self, keypair: Keypair) -> MagicMock:
        mock_auth = MagicMock(spec=StellarAuth)
        mock_auth.get_account_id.return_value = keypair.public_key
        return mock_auth

    def _make_mock_connector(self, channel_pool=None) -> MagicMock:
        mock_connector = MagicMock()
        mock_connector._rpc_url = "https://soroban-testnet.stellar.org"
        mock_connector._channel_pool = channel_pool
        mock_connector._ledger_stream = MagicMock()
        mock_connector._ledger_stream.subscribe = AsyncMock(return_value=asyncio.Queue())
        mock_connector._ledger_stream.unsubscribe = AsyncMock()
        return mock_connector

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_get_monitored_accounts_main_only(self):
        """Without a channel pool only the main account should be monitored."""
        main_kp = Keypair.random()
        auth = self._make_mock_auth(main_kp)
        connector = self._make_mock_connector(channel_pool=None)

        ds = StellarAPIUserStreamDataSource(auth=auth, connector=connector)
        accounts: Set[str] = ds._get_monitored_accounts()

        self.assertEqual(accounts, {main_kp.public_key})

    def test_get_monitored_accounts_with_channels(self):
        """Main account plus channel accounts should all be monitored."""
        main_kp = Keypair.random()
        channel_kps = [Keypair.random() for _ in range(3)]

        auth = self._make_mock_auth(main_kp)

        mock_pool = MagicMock()
        mock_pool._channels = [
            ChannelAccount(keypair=kp, lock=asyncio.Lock()) for kp in channel_kps
        ]
        connector = self._make_mock_connector(channel_pool=mock_pool)

        ds = StellarAPIUserStreamDataSource(auth=auth, connector=connector)
        accounts: Set[str] = ds._get_monitored_accounts()

        expected = {main_kp.public_key} | {kp.public_key for kp in channel_kps}
        self.assertEqual(accounts, expected)

    def test_last_recv_time_initialized_to_zero(self):
        """last_recv_time should start at 0."""
        main_kp = Keypair.random()
        auth = self._make_mock_auth(main_kp)
        connector = self._make_mock_connector()

        ds = StellarAPIUserStreamDataSource(auth=auth, connector=connector)

        self.assertEqual(ds.last_recv_time, 0)

    async def test_process_ledger_for_user_emits_user_events(self):
        main_kp = Keypair.random()
        auth = self._make_mock_auth(main_kp)
        connector = self._make_mock_connector()
        ds = StellarAPIUserStreamDataSource(auth=auth, connector=connector)
        ds._monitored_accounts = {main_kp.public_key}
        output = asyncio.Queue()

        event = StellarLedgerEvent(
            ledger_sequence=55,
            ledger_close_time=99,
            meta=MagicMock(),
            entry_changes=[],
            order_changes=[
                StellarOrderCreated(order=StellarOrder(
                    id=1,
                    selling_asset=Asset.native(),
                    buying_asset=Asset("USDC", "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"),
                    amount=1.0,
                    price=Decimal("2"),
                    seller_id=main_kp.public_key,
                ))
            ],
            trades=[{"trade_id": 1, "seller_id": main_kp.public_key, "offer_id": 1}],
        )

        await ds._process_ledger_for_user(event, output)

        self.assertEqual(output.qsize(), 3)
        self.assertEqual((await output.get())["type"], "trade")
        self.assertEqual((await output.get())["type"], "order_change")
        self.assertEqual((await output.get())["type"], "balance_update")

    async def test_process_ledger_for_user_filters_trades_for_other_accounts(self):
        main_kp = Keypair.random()
        auth = self._make_mock_auth(main_kp)
        connector = self._make_mock_connector()
        ds = StellarAPIUserStreamDataSource(auth=auth, connector=connector)
        ds._monitored_accounts = {main_kp.public_key}
        output = asyncio.Queue()

        event = StellarLedgerEvent(
            ledger_sequence=55,
            ledger_close_time=99,
            meta=MagicMock(),
            entry_changes=[],
            order_changes=[],
            trades=[{"trade_id": 1, "seller_id": Keypair.random().public_key, "offer_id": 1}],
        )

        await ds._process_ledger_for_user(event, output)

        self.assertEqual(output.qsize(), 1)
        self.assertEqual((await output.get())["type"], "balance_update")


if __name__ == "__main__":
    unittest.main()
