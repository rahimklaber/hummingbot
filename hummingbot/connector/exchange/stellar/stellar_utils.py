import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from stellar_sdk import Asset, Keypair

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.exchange.stellar import stellar_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = False
EXAMPLE_PAIR = "USDC-XLM"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
    buy_percent_fee_deducted_from_returns=False,
)


class StellarMarket(BaseModel):
    base: str
    quote: str
    base_issuer: str
    quote_issuer: str
    trading_pair_symbol: Optional[str] = None

    def to_stellar_assets(self) -> tuple:
        base_asset = Asset.native() if self.base_issuer == "" else Asset(self.base, self.base_issuer)
        quote_asset = Asset.native() if self.quote_issuer == "" else Asset(self.quote, self.quote_issuer)
        return base_asset, quote_asset


@dataclass
class ChannelAccount:
    keypair: Keypair
    lock: asyncio.Lock


class ChannelAccountPool:
    """
    Manages a pool of channel accounts for parallel transaction submission.
    Stellar uses per-account sequence numbers, so a single account can only submit
    one transaction at a time. Channel accounts provide additional sequence number
    sources for parallel submission.
    """

    def __init__(self, channel_secret_keys: List[str]):
        self._channels: List[ChannelAccount] = []
        for key in channel_secret_keys:
            kp = Keypair.from_secret(key)
            self._channels.append(ChannelAccount(keypair=kp, lock=asyncio.Lock()))
        self._semaphore = asyncio.Semaphore(len(self._channels))

    async def acquire(self) -> ChannelAccount:
        """Acquire an available channel account. Blocks if all are in use."""
        await self._semaphore.acquire()
        for channel in self._channels:
            if channel.lock.locked():
                continue
            await channel.lock.acquire()
            return channel
        # Shouldn't reach here due to semaphore, but safety fallback
        raise RuntimeError("No channel accounts available")

    def release(self, channel: ChannelAccount):
        """Release a channel account back to the pool."""
        channel.lock.release()
        self._semaphore.release()

    @property
    def pool_size(self) -> int:
        return len(self._channels)


class StellarConfigMap(BaseConnectorConfigMap):
    connector: str = "stellar"
    stellar_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Stellar wallet secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    rpc_url: str = Field(
        default=CONSTANTS.DEFAULT_RPC_URL,
        json_schema_extra={
            "prompt": "Enter the Soroban RPC URL",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    channel_account_secret_keys: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter channel account secret keys (comma separated, for parallel order submission)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    custom_markets: Dict[str, StellarMarket] = Field(
        default={},
    )

    model_config = ConfigDict(title="stellar")

    @field_validator("channel_account_secret_keys", mode="before")
    @classmethod
    def validate_channel_keys(cls, v):
        if isinstance(v, list):
            v = ",".join(v)
        if isinstance(v, SecretStr):
            return v
        return v


KEYS = StellarConfigMap.model_construct()


def stellar_asset_to_str(asset: Asset) -> str:
    """Convert a stellar Asset to a string like 'XLM' or 'USDC:GA5ZS...'"""
    if asset.is_native():
        return "XLM"
    return f"{asset.code}:{asset.issuer}"


def trading_pair_to_assets(
    trading_pair: str, custom_markets: Dict[str, StellarMarket]
) -> tuple:
    """Convert hummingbot trading pair like 'USDC-XLM' to (base_asset, quote_asset) stellar Assets.
    Uses custom_markets for issuer resolution."""
    if trading_pair in custom_markets:
        return custom_markets[trading_pair].to_stellar_assets()

    raise ValueError(f"Trading pair {trading_pair} not found in custom markets for issuer resolution.")


def assets_to_trading_pair(
    base_asset: Asset,
    quote_asset: Asset,
    custom_markets: Dict[str, StellarMarket],
) -> Optional[str]:
    """Reverse lookup: find trading pair string from assets."""
    for pair, market in custom_markets.items():
        m_base, m_quote = market.to_stellar_assets()
        if m_base == base_asset and m_quote == quote_asset:
            return pair
        if m_base == quote_asset and m_quote == base_asset:
            return pair
    return None
