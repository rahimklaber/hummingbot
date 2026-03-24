import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from stellar_sdk import Keypair

from hummingbot.connector.exchange.stellar.stellar_api_order_book_data_source import StellarAPIOrderBookDataSource
from hummingbot.connector.exchange.stellar.stellar_api_user_stream_data_source import StellarAPIUserStreamDataSource
from hummingbot.connector.exchange.stellar.stellar_auth import StellarAuth
from hummingbot.connector.exchange.stellar.stellar_exchange import StellarExchange
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import (
    StellarOrder,
    StellarOrderCreated,
    StellarOrderRemoved,
)
from hummingbot.connector.exchange.stellar.stellar_utils import StellarMarket
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee

_TEST_TRADING_PAIR = "USDC-XLM"
_CUSTOM_MARKETS = {
    _TEST_TRADING_PAIR: StellarMarket(
        base="USDC",
        quote="XLM",
        base_issuer="GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
        quote_issuer="",
    )
}


def _create_exchange(kp: Keypair | None = None) -> StellarExchange:
    """Create a StellarExchange instance with trading_required=False to skip network calls."""
    if kp is None:
        kp = Keypair.random()

    # Clear abstract methods so StellarExchange can be instantiated in tests.
    StellarExchange.__abstractmethods__ = frozenset()

    return StellarExchange(
        stellar_secret_key=kp.secret,
        rpc_url="https://soroban-testnet.stellar.org",
        trading_pairs=[_TEST_TRADING_PAIR],
        trading_required=False,
        custom_markets=_CUSTOM_MARKETS,
    )


class TestStellarExchangeProperties(unittest.IsolatedAsyncioTestCase):
    """Tests that do not require a running event loop for the exchange."""

    def setUp(self):
        self.exchange = _create_exchange()

    def test_connector_properties(self):
        self.assertEqual(self.exchange.name, "stellar")
        self.assertEqual(self.exchange.domain, "stellar")
        self.assertIn(_TEST_TRADING_PAIR, self.exchange._trading_pairs)
        self.assertIn(OrderType.LIMIT, self.exchange.supported_order_types())
        self.assertIn(OrderType.LIMIT_MAKER, self.exchange.supported_order_types())

    def test_authenticator(self):
        auth = self.exchange.authenticator
        self.assertIsInstance(auth, StellarAuth)

    def test_supported_order_types(self):
        order_types = self.exchange.supported_order_types()
        self.assertEqual(order_types, [OrderType.LIMIT, OrderType.LIMIT_MAKER])

    def test_get_fee(self):
        fee: AddedToCostTradeFee = self.exchange._get_fee(
            base_currency="USDC",
            quote_currency="XLM",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("100"),
            price=Decimal("0.5"),
        )
        self.assertIsInstance(fee, AddedToCostTradeFee)
        self.assertEqual(fee.percent, Decimal("0"))
        self.assertEqual(len(fee.flat_fees), 1)
        self.assertEqual(fee.flat_fees[0].token, "XLM")
        self.assertEqual(fee.flat_fees[0].amount, Decimal("0.00001"))

    def test_format_trading_rules(self):
        rules = self.exchange._format_trading_rules({})
        self.assertEqual(len(rules), 1)
        rule: TradingRule = rules[0]
        self.assertEqual(rule.trading_pair, _TEST_TRADING_PAIR)
        self.assertEqual(rule.min_order_size, Decimal("0.0000001"))
        self.assertEqual(rule.min_price_increment, Decimal("0.0000001"))
        self.assertEqual(rule.min_base_amount_increment, Decimal("0.0000001"))

    def test_create_order_book_data_source(self):
        ds = self.exchange._create_order_book_data_source()
        self.assertIsInstance(ds, StellarAPIOrderBookDataSource)

    def test_create_user_stream_data_source(self):
        ds = self.exchange._create_user_stream_data_source()
        self.assertIsInstance(ds, StellarAPIUserStreamDataSource)


class TestStellarExchangeExtractOfferId(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.exchange = _create_exchange()

    def test_extract_offer_id_from_result_returns_none_for_mock(self):
        """A plain mock result without valid XDR should return None."""
        mock_result = MagicMock()
        mock_result.result_xdr = None
        result = self.exchange._extract_offer_id_from_result(mock_result)
        self.assertIsNone(result)


class TestStellarExchangeProcessOrderChangeEvent(unittest.IsolatedAsyncioTestCase):
    """Test _process_order_change_event for Created, Removed-cancelled, Removed-filled."""

    def setUp(self):
        self.kp = Keypair.random()
        self.exchange = _create_exchange(self.kp)
        # Set up a mock order tracker
        self.mock_tracker = MagicMock()
        self.exchange._order_tracker = self.mock_tracker

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_tracked_order(
        self, client_order_id: str, state: OrderState, amount: Decimal = Decimal("100"),
        price: Decimal = Decimal("0.5"), executed: Decimal = Decimal("0"),
    ) -> MagicMock:
        order = MagicMock(spec=InFlightOrder)
        order.client_order_id = client_order_id
        order.trading_pair = _TEST_TRADING_PAIR
        order.current_state = state
        order.amount = amount
        order.price = price
        order.executed_amount_base = executed
        return order

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    async def test_process_order_change_event_created(self):
        """StellarOrderCreated should transition a PENDING_CREATE order to OPEN."""
        offer_id = 12345
        client_oid = "hbot-test-001"
        self.exchange._offer_id_to_order_id[offer_id] = client_oid

        tracked = self._make_tracked_order(client_oid, OrderState.PENDING_CREATE)
        self.mock_tracker.active_orders = {client_oid: tracked}

        stellar_order = StellarOrder(
            id=offer_id,
            selling_asset=MagicMock(),
            buying_asset=MagicMock(),
            amount=100.0,
            price=0.5,
            seller_id=self.kp.public_key,
        )
        event = {
            "change": StellarOrderCreated(order=stellar_order),
            "ledger_sequence": 100,
            "timestamp": 1700000000.0,
        }

        await self.exchange._process_order_change_event(event)

        self.mock_tracker.process_order_update.assert_called_once()
        order_update: OrderUpdate = self.mock_tracker.process_order_update.call_args[0][0]
        self.assertEqual(order_update.client_order_id, client_oid)
        self.assertEqual(order_update.new_state, OrderState.OPEN)

    async def test_process_order_change_event_removed_cancelled(self):
        """StellarOrderRemoved for PENDING_CANCEL should result in CANCELED."""
        offer_id = 54321
        client_oid = "hbot-test-002"
        self.exchange._offer_id_to_order_id[offer_id] = client_oid

        tracked = self._make_tracked_order(client_oid, OrderState.PENDING_CANCEL)
        self.mock_tracker.active_orders = {client_oid: tracked}

        event = {
            "change": StellarOrderRemoved(id=offer_id),
            "ledger_sequence": 101,
            "timestamp": 1700000001.0,
        }

        await self.exchange._process_order_change_event(event)

        self.mock_tracker.process_order_update.assert_called_once()
        order_update: OrderUpdate = self.mock_tracker.process_order_update.call_args[0][0]
        self.assertEqual(order_update.new_state, OrderState.CANCELED)
        # Offer mapping should be cleaned up
        self.assertNotIn(offer_id, self.exchange._offer_id_to_order_id)

    async def test_process_order_change_event_removed_cancel_requested_is_cancelled(self):
        """A local cancel request should classify removal as CANCELED even if the order state is still OPEN."""
        offer_id = 65432
        client_oid = "hbot-test-002b"
        self.exchange._offer_id_to_order_id[offer_id] = client_oid
        self.exchange._cancel_requested_order_ids.add(client_oid)
        self.exchange._cancel_requested_offer_ids.add(offer_id)

        tracked = self._make_tracked_order(client_oid, OrderState.OPEN)
        self.mock_tracker.active_orders = {client_oid: tracked}

        event = {
            "change": StellarOrderRemoved(id=offer_id),
            "ledger_sequence": 101,
            "timestamp": 1700000001.5,
        }

        await self.exchange._process_order_change_event(event)

        self.mock_tracker.process_order_update.assert_called_once()
        order_update: OrderUpdate = self.mock_tracker.process_order_update.call_args[0][0]
        self.assertEqual(order_update.new_state, OrderState.CANCELED)
        self.assertNotIn(client_oid, self.exchange._cancel_requested_order_ids)
        self.assertNotIn(offer_id, self.exchange._cancel_requested_offer_ids)

    async def test_process_order_change_event_removed_filled(self):
        """StellarOrderRemoved for an OPEN order should result in FILLED."""
        offer_id = 99999
        client_oid = "hbot-test-003"
        self.exchange._offer_id_to_order_id[offer_id] = client_oid

        tracked = self._make_tracked_order(
            client_oid, OrderState.OPEN,
            amount=Decimal("100"), price=Decimal("0.5"), executed=Decimal("0"),
        )
        self.mock_tracker.active_orders = {client_oid: tracked}

        event = {
            "change": StellarOrderRemoved(id=offer_id),
            "ledger_sequence": 102,
            "timestamp": 1700000002.0,
        }

        await self.exchange._process_order_change_event(event)

        self.mock_tracker.process_order_update.assert_called_once()
        order_update: OrderUpdate = self.mock_tracker.process_order_update.call_args[0][0]
        self.assertEqual(order_update.new_state, OrderState.FILLED)
        self.assertNotIn(offer_id, self.exchange._offer_id_to_order_id)


if __name__ == "__main__":
    unittest.main()
