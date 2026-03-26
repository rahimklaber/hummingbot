import sys
from decimal import Decimal

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "stellar"
DOMAIN = "stellar"

HBOT_ORDER_ID_PREFIX = "hbot"
MAX_ORDER_ID_LEN = 40

# Soroban RPC URL
DEFAULT_RPC_URL = "https://rpc.lightsail.network"

# Websocket channels
TRADE_EVENT_TYPE = "trades"
DIFF_EVENT_TYPE = "diffs"
SNAPSHOT_EVENT_TYPE = "order_book_snapshots"

# Stellar precision (7 decimal places, smallest unit is 1 stroop)
STELLAR_DECIMAL_PLACES = 7
ONE_STROOP = Decimal("0.0000001")

# Transaction base fee in stroops (0.00001 XLM)
BASE_FEE = 100

# Ledger reserves
ACCOUNT_BASE_RESERVE = Decimal("1")  # XLM required to keep an account active
LEDGER_ENTRY_RESERVE = Decimal("0.5")  # XLM per subentry (e.g. offers, trustlines)

# Order States
ORDER_STATE = {
    "open": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "partial_filled": OrderState.PARTIALLY_FILLED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

# Market Order Max Slippage
MARKET_ORDER_MAX_SLIPPAGE = Decimal("0.01")

# Orderbook settings
ORDER_BOOK_DEPTH = 100

# Timeout for pending order status check
PENDING_ORDER_STATUS_CHECK_TIMEOUT = 120

# Request Timeout
REQUEST_TIMEOUT = 60

# Rate Limits
RAW_REQUESTS = "RAW_REQUESTS"
NO_LIMIT = sys.maxsize
RATE_LIMITS = [
    RateLimit(limit_id=RAW_REQUESTS, limit=NO_LIMIT, time_interval=1),
]

# Place order retry parameters
PLACE_ORDER_MAX_RETRY = 3
PLACE_ORDER_RETRY_INTERVAL = 5

# Cancel All Timeout
CANCEL_ALL_TIMEOUT = 600

# Cancel retry parameters
CANCEL_MAX_RETRY = 3
CANCEL_RETRY_INTERVAL = 5

# Verify transaction retry parameters
VERIFY_TRANSACTION_MAX_RETRY = 3
VERIFY_TRANSACTION_RETRY_INTERVAL = 5

# Request retry interval
REQUEST_RETRY_INTERVAL = 5

# Request Orderbook Interval
REQUEST_ORDERBOOK_INTERVAL = 10

# Ledger polling interval (matches ~5s Stellar ledger close time)
LEDGER_POLL_INTERVAL = 5

# Number of ledgers to fetch per poll
GET_LEDGERS_BATCH_SIZE = 5

# Default trading pairs
# Fill in the issuer addresses for each asset
MARKETS = {
    "XLM-USDC": {
        "base": "XLM",
        "quote": "USDC",
        "base_issuer": "",
        "quote_issuer": "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
    },
}
