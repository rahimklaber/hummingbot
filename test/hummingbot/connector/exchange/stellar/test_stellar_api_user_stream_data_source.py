import asyncio
import unittest
from typing import Set
from unittest.mock import MagicMock

from stellar_sdk import Keypair

from hummingbot.connector.exchange.stellar.stellar_api_user_stream_data_source import StellarAPIUserStreamDataSource
from hummingbot.connector.exchange.stellar.stellar_auth import StellarAuth
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


if __name__ == "__main__":
    unittest.main()
