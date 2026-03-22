import unittest
from unittest.async_case import IsolatedAsyncioTestCase

from stellar_sdk import Keypair

from hummingbot.connector.exchange.stellar.stellar_auth import StellarAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class TestStellarAuth(IsolatedAsyncioTestCase):
    def setUp(self):
        self._keypair = Keypair.random()
        self._auth = StellarAuth(self._keypair.secret)

    def test_valid_secret_key(self):
        self.assertEqual(self._auth.get_account_id(), self._keypair.public_key)

    def test_empty_secret_key(self):
        with self.assertRaises(ValueError):
            StellarAuth("")

    def test_invalid_secret_key(self):
        with self.assertRaises(ValueError):
            StellarAuth("invalid_key_here")

    def test_get_keypair(self):
        kp = self._auth.get_keypair()
        self.assertIsInstance(kp, Keypair)
        self.assertEqual(kp.public_key, self._keypair.public_key)

    async def test_rest_authenticate(self):
        request = RESTRequest(method=RESTMethod.GET, url="https://example.com")
        result = await self._auth.rest_authenticate(request)
        self.assertIs(result, request)

    async def test_ws_authenticate(self):
        request = WSJSONRequest(payload={"op": "subscribe"})
        result = await self._auth.ws_authenticate(request)
        self.assertIs(result, request)


if __name__ == "__main__":
    unittest.main()
