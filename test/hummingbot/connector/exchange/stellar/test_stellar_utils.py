import unittest
from unittest.async_case import IsolatedAsyncioTestCase

from stellar_sdk import Asset, Keypair

from hummingbot.connector.exchange.stellar.stellar_utils import (
    ChannelAccountPool,
    StellarMarket,
    assets_to_trading_pair,
    stellar_asset_to_str,
    trading_pair_to_assets,
)

FAKE_ISSUER = "GBZXN7PIRZGNMHGA7MUUUF4GWDXNPHMCUKNLIFPMSBDNNCNO7M7EEZM"


class TestStellarUtils(IsolatedAsyncioTestCase):
    # --- StellarMarket ---

    def test_stellar_market_to_stellar_assets_native(self):
        market = StellarMarket(base="XLM", quote="XLM", base_issuer="", quote_issuer="")
        base_asset, quote_asset = market.to_stellar_assets()
        self.assertTrue(base_asset.is_native())
        self.assertTrue(quote_asset.is_native())

    def test_stellar_market_to_stellar_assets_issued(self):
        market = StellarMarket(
            base="USDC", quote="XLM", base_issuer=FAKE_ISSUER, quote_issuer=""
        )
        base_asset, quote_asset = market.to_stellar_assets()
        self.assertFalse(base_asset.is_native())
        self.assertEqual(base_asset.code, "USDC")
        self.assertEqual(base_asset.issuer, FAKE_ISSUER)
        self.assertTrue(quote_asset.is_native())

    # --- ChannelAccountPool ---

    async def test_channel_account_pool_acquire_release(self):
        kp1 = Keypair.random()
        kp2 = Keypair.random()
        pool = ChannelAccountPool([kp1.secret, kp2.secret])
        self.assertEqual(pool.pool_size, 2)

        channel = await pool.acquire()
        self.assertTrue(channel.lock.locked())

        pool.release(channel)
        self.assertFalse(channel.lock.locked())

    async def test_channel_account_pool_empty(self):
        pool = ChannelAccountPool([])
        self.assertEqual(pool.pool_size, 0)

    # --- stellar_asset_to_str ---

    def test_stellar_asset_to_str_native(self):
        self.assertEqual(stellar_asset_to_str(Asset.native()), "XLM")

    def test_stellar_asset_to_str_issued(self):
        asset = Asset("USDC", FAKE_ISSUER)
        self.assertEqual(stellar_asset_to_str(asset), f"USDC:{FAKE_ISSUER}")

    # --- trading_pair_to_assets / assets_to_trading_pair ---

    def _make_custom_markets(self):
        return {
            "USDC-XLM": StellarMarket(
                base="USDC",
                quote="XLM",
                base_issuer=FAKE_ISSUER,
                quote_issuer="",
            )
        }

    def test_trading_pair_to_assets(self):
        markets = self._make_custom_markets()
        base, quote = trading_pair_to_assets("USDC-XLM", markets)
        self.assertEqual(base.code, "USDC")
        self.assertEqual(base.issuer, FAKE_ISSUER)
        self.assertTrue(quote.is_native())

    def test_assets_to_trading_pair(self):
        markets = self._make_custom_markets()
        base = Asset("USDC", FAKE_ISSUER)
        quote = Asset.native()
        result = assets_to_trading_pair(base, quote, markets)
        self.assertEqual(result, "USDC-XLM")

    def test_assets_to_trading_pair_not_found(self):
        markets = self._make_custom_markets()
        result = assets_to_trading_pair(
            Asset("BTC", FAKE_ISSUER), Asset.native(), markets
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
