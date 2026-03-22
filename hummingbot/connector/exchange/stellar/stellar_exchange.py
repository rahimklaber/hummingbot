import asyncio
import time
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from bidict import bidict
from stellar_sdk import AiohttpClient, Keypair, Network, SorobanServerAsync, TransactionBuilder

from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.stellar import stellar_constants as CONSTANTS, stellar_web_utils
from hummingbot.connector.exchange.stellar.stellar_api_order_book_data_source import StellarAPIOrderBookDataSource
from hummingbot.connector.exchange.stellar.stellar_api_user_stream_data_source import StellarAPIUserStreamDataSource
from hummingbot.connector.exchange.stellar.stellar_auth import StellarAuth
from hummingbot.connector.exchange.stellar.stellar_ledger_reader import (
    StellarOrderCreated,
    StellarOrderRemoved,
    StellarOrderUpdated,
)
from hummingbot.connector.exchange.stellar.stellar_utils import (
    ChannelAccountPool,
    StellarMarket,
    trading_pair_to_assets,
)
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class StellarOrderTracker(ClientOrderTracker):
    TRADE_FILLS_WAIT_TIMEOUT = 20


class StellarExchange(ExchangePyBase):
    web_utils = stellar_web_utils

    def __init__(
        self,
        stellar_secret_key: str,
        rpc_url: str,
        channel_account_secret_keys: list = None,
        custom_markets: Optional[Dict[str, StellarMarket]] = None,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
    ):
        self._stellar_secret_key = stellar_secret_key
        self._rpc_url = rpc_url
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._custom_markets = custom_markets or {}
        self._stellar_auth: StellarAuth = self.authenticator
        self._nonce_creator = NonceCreator.for_milliseconds()
        self._network_passphrase = Network.PUBLIC_NETWORK_PASSPHRASE

        # Channel account pool for parallel tx submission
        channel_keys = channel_account_secret_keys or []
        self._channel_pool: Optional[ChannelAccountPool] = (
            ChannelAccountPool(channel_keys) if channel_keys else None
        )

        # Offer ID to client_order_id mapping
        self._offer_id_to_order_id: Dict[int, str] = {}

        # Order locking
        self._place_order_lock = asyncio.Lock()
        self._order_status_locks: Dict[str, asyncio.Lock] = {}
        self._order_status_lock_manager = asyncio.Lock()

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---- Order tracker ----

    def _create_order_tracker(self) -> ClientOrderTracker:
        return StellarOrderTracker(connector=self)

    # ---- Properties ----

    @property
    def authenticator(self) -> StellarAuth:
        return StellarAuth(stellar_secret_key=self._stellar_secret_key)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return CONSTANTS.DOMAIN

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return ""

    @property
    def trading_pairs_request_path(self):
        return ""

    @property
    def check_network_request_path(self):
        return ""

    @property
    def trading_pairs(self):
        return self._trading_pairs or []

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception) -> bool:
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        pass  # Not used for Soroban RPC

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return StellarAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs or [],
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return StellarAPIUserStreamDataSource(
            auth=self._stellar_auth,
            connector=self,
        )

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, StellarMarket]):
        mapping_symbol = bidict()
        for market in exchange_info:
            mapping_symbol[market.upper()] = market.upper()
        self._set_trading_pair_symbol_map(mapping_symbol)

    # ---- Soroban server ----

    def _get_soroban_server(self) -> SorobanServerAsync:
        return SorobanServerAsync(
            server_url=self._rpc_url,
            client=AiohttpClient(),
        )

    # ---- Last traded prices ----

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """Get last traded price from the order book data source's internal state."""
        results = {}
        for trading_pair in trading_pairs:
            ob_ds = self.order_book_data_source
            if hasattr(ob_ds, '_internal_order_books') and trading_pair in ob_ds._internal_order_books:
                internal_ob = ob_ds._internal_order_books[trading_pair]
                best_bid = internal_ob.get_best_bid()
                best_ask = internal_ob.get_best_ask()
                if best_bid and best_ask:
                    results[trading_pair] = (best_bid.price + best_ask.price) / 2.0
                elif best_bid:
                    results[trading_pair] = best_bid.price
                elif best_ask:
                    results[trading_pair] = best_ask.price
                else:
                    results[trading_pair] = 0.0
            else:
                results[trading_pair] = 0.0
        return results

    # ---- Place order ----

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        **kwargs,
    ) -> Tuple[str, float]:
        """
        Place an order on the Stellar DEX.
        Returns (exchange_order_id, transact_time).

        exchange_order_id format: "{offer_id}" (extracted from tx result)
        """
        base_asset, quote_asset = trading_pair_to_assets(trading_pair, self._custom_markets)

        exchange_order_id = "UNKNOWN"
        transact_time = time.time()

        server = self._get_soroban_server()
        channel = None

        try:
            # Determine which account to use for the transaction source
            if self._channel_pool is not None and self._channel_pool.pool_size > 0:
                channel = await self._channel_pool.acquire()
                source_keypair = channel.keypair
            else:
                source_keypair = self._stellar_auth.get_keypair()

            # Load account for sequence number
            account = await server.load_account(source_keypair.public_key)

            # Build the transaction
            builder = TransactionBuilder(
                source_account=account,
                network_passphrase=self._network_passphrase,
                base_fee=CONSTANTS.BASE_FEE,
            )
            builder.set_timeout(30)

            # The operation source is the main account (which holds the funds)
            main_account_id = self._stellar_auth.get_keypair().public_key

            if trade_type == TradeType.BUY:
                # Buying base with quote: ManageBuyOffer
                builder.append_manage_buy_offer_op(
                    selling=quote_asset,
                    buying=base_asset,
                    amount=str(amount),
                    price=str(price),
                    offer_id=0,  # 0 = new offer
                    source=main_account_id,
                )
            else:
                # Selling base for quote: ManageSellOffer
                builder.append_manage_sell_offer_op(
                    selling=base_asset,
                    buying=quote_asset,
                    amount=str(amount),
                    price=str(price),
                    offer_id=0,  # 0 = new offer
                    source=main_account_id,
                )

            tx = builder.build()

            # Sign with main account (always needed as operation source)
            tx.sign(self._stellar_auth.get_keypair())
            # Sign with channel account if different from main
            if channel is not None:
                tx.sign(channel.keypair)

            # Submit
            response = await server.send_transaction(tx)
            transact_time = time.time()

            self.logger().info(
                f"Submitted order {order_id}: status={response.status}, hash={response.hash}"
            )

            if response.status == "ERROR":
                raise Exception(f"Transaction failed: {response.status}")

            # Poll for transaction result to get the offer ID
            tx_result = await self._poll_transaction_result(server, response.hash)

            # Extract offer ID from the transaction result
            offer_id = self._extract_offer_id_from_result(tx_result)

            if offer_id is not None:
                exchange_order_id = str(offer_id)
                self._offer_id_to_order_id[offer_id] = order_id
            else:
                # If we couldn't extract the offer ID, use the tx hash
                exchange_order_id = response.hash

            # Update order state to OPEN
            order_update = OrderUpdate(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                update_timestamp=transact_time,
                new_state=OrderState.OPEN,
            )
            self._order_tracker.process_order_update(order_update)

        except Exception as e:
            order_update = OrderUpdate(
                trading_pair=trading_pair,
                update_timestamp=time.time(),
                new_state=OrderState.FAILED,
                client_order_id=order_id,
            )
            self._order_tracker.process_order_update(order_update)
            self.logger().error(f"Order {order_id} creation failed: {e}")
            raise
        finally:
            if channel is not None and self._channel_pool is not None:
                self._channel_pool.release(channel)
            await server.close()

        return exchange_order_id, transact_time

    # ---- Transaction polling ----

    async def _poll_transaction_result(self, server: SorobanServerAsync, tx_hash: str, max_retries: int = 30) -> dict:
        """Poll Soroban RPC for transaction result until it's confirmed."""
        for i in range(max_retries):
            try:
                result = await server.get_transaction(tx_hash)
                if result.status == "SUCCESS":
                    return result
                elif result.status == "FAILED":
                    raise Exception(f"Transaction {tx_hash} failed: {result}")
                elif result.status == "NOT_FOUND":
                    await asyncio.sleep(1)
                    continue
            except Exception as e:
                if "NOT_FOUND" in str(e) or i < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                raise
        raise Exception(f"Transaction {tx_hash} not found after {max_retries} retries")

    # ---- Offer ID extraction ----

    def _extract_offer_id_from_result(self, tx_result) -> Optional[int]:
        """Extract the offer ID from a transaction result."""
        try:
            if hasattr(tx_result, 'result_xdr') and tx_result.result_xdr:
                from stellar_sdk.xdr import TransactionResult
                result = TransactionResult.from_xdr(tx_result.result_xdr)
                for op_result in result.result.results:
                    tr = op_result.tr
                    # Check ManageSellOffer result
                    if tr.manage_sell_offer_result is not None:
                        offer_result = tr.manage_sell_offer_result.success
                        if offer_result and offer_result.offer and offer_result.offer.offer:
                            return offer_result.offer.offer.offer_id.int64
                    # Check ManageBuyOffer result
                    if tr.manage_buy_offer_result is not None:
                        offer_result = tr.manage_buy_offer_result.success
                        if offer_result and offer_result.offer and offer_result.offer.offer:
                            return offer_result.offer.offer.offer_id.int64
        except Exception as e:
            self.logger().debug(f"Could not extract offer ID from result: {e}")
        return None

    # ---- Cancel order ----

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """Cancel an order on the Stellar DEX."""
        exchange_order_id = tracked_order.exchange_order_id
        if exchange_order_id is None:
            self.logger().error(f"Cannot cancel order {order_id}: no exchange_order_id")
            return False

        server = self._get_soroban_server()
        channel = None

        try:
            offer_id = int(exchange_order_id)
            base_asset, quote_asset = trading_pair_to_assets(
                tracked_order.trading_pair, self._custom_markets
            )

            # Use channel account or main account for tx source
            if self._channel_pool is not None and self._channel_pool.pool_size > 0:
                channel = await self._channel_pool.acquire()
                source_keypair = channel.keypair
            else:
                source_keypair = self._stellar_auth.get_keypair()

            account = await server.load_account(source_keypair.public_key)
            main_account_id = self._stellar_auth.get_keypair().public_key

            builder = TransactionBuilder(
                source_account=account,
                network_passphrase=self._network_passphrase,
                base_fee=CONSTANTS.BASE_FEE,
            )
            builder.set_timeout(30)

            # Cancel = ManageSellOffer/ManageBuyOffer with amount=0 and the existing offer_id
            if tracked_order.trade_type == TradeType.SELL:
                builder.append_manage_sell_offer_op(
                    selling=base_asset,
                    buying=quote_asset,
                    amount="0",
                    price="1",  # Price doesn't matter for cancel
                    offer_id=offer_id,
                    source=main_account_id,
                )
            else:
                builder.append_manage_buy_offer_op(
                    selling=quote_asset,
                    buying=base_asset,
                    amount="0",
                    price="1",
                    offer_id=offer_id,
                    source=main_account_id,
                )

            tx = builder.build()
            tx.sign(self._stellar_auth.get_keypair())
            if channel is not None:
                tx.sign(channel.keypair)

            response = await server.send_transaction(tx)

            self.logger().info(
                f"Submitted cancel for order {order_id} (offer {offer_id}): status={response.status}"
            )

            if response.status == "ERROR":
                raise Exception(f"Cancel transaction failed: {response.status}")

            # Poll for confirmation
            await self._poll_transaction_result(server, response.hash)
            return True

        except Exception as e:
            self.logger().error(f"Order cancellation failed for {order_id}: {e}")
            return False
        finally:
            if channel is not None and self._channel_pool is not None:
                self._channel_pool.release(channel)
            await server.close()

    # ---- Cancel and process update ----

    async def _execute_order_cancel_and_process_update(self, order: InFlightOrder) -> bool:
        """Cancel an order and process the state update."""
        if order.current_state in [OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED]:
            return order.current_state == OrderState.CANCELED

        # Mark as pending cancel
        order_update = OrderUpdate(
            client_order_id=order.client_order_id,
            trading_pair=order.trading_pair,
            update_timestamp=time.time(),
            new_state=OrderState.PENDING_CANCEL,
        )
        self._order_tracker.process_order_update(order_update)

        cancelled = await self._place_cancel(order.client_order_id, order)

        if cancelled:
            order_update = OrderUpdate(
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                trading_pair=order.trading_pair,
                update_timestamp=time.time(),
                new_state=OrderState.CANCELED,
            )
            self._order_tracker.process_order_update(order_update)
            return True

        return False

    # ---- Place order and process update ----

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        exchange_order_id, _ = await self._place_order(
            order_id=order.client_order_id,
            trading_pair=order.trading_pair,
            amount=order.amount,
            trade_type=order.trade_type,
            order_type=order.order_type,
            price=order.price,
            **kwargs,
        )
        return exchange_order_id

    # ---- Trading rules ----

    async def _update_trading_rules(self):
        """Set trading rules for Stellar DEX pairs."""
        trading_rules = []
        for trading_pair in self._trading_pairs:
            trading_rules.append(TradingRule(
                trading_pair=trading_pair,
                min_order_size=Decimal("0.0000001"),  # 1 stroop
                min_price_increment=Decimal("0.0000001"),
                min_base_amount_increment=Decimal("0.0000001"),
                min_quote_amount_increment=Decimal("0.0000001"),
                min_notional_size=Decimal("0.0000001"),
            ))
        self._trading_rules.clear()
        for rule in trading_rules:
            self._trading_rules[rule.trading_pair] = rule

    def _format_trading_rules(self, trading_rules_info) -> List[TradingRule]:
        trading_rules = []
        for trading_pair in self._trading_pairs:
            trading_rules.append(TradingRule(
                trading_pair=trading_pair,
                min_order_size=Decimal("0.0000001"),
                min_price_increment=Decimal("0.0000001"),
                min_base_amount_increment=Decimal("0.0000001"),
                min_quote_amount_increment=Decimal("0.0000001"),
                min_notional_size=Decimal("0.0000001"),
            ))
        return trading_rules

    # ---- Balances ----

    async def _update_balances(self):
        """Update account balances from the Stellar network."""
        server = self._get_soroban_server()
        try:
            account_id = self._stellar_auth.get_keypair().public_key

            from stellar_sdk.xdr import (
                AccountID,
                LedgerEntryData,
                LedgerKey,
                LedgerKeyAccount,
                LedgerKeyTrustLine,
                PublicKey,
                PublicKeyType,
                Uint256,
            )

            account_pubkey = PublicKey(
                type=PublicKeyType.PUBLIC_KEY_TYPE_ED25519,
                ed25519=Uint256(Keypair.from_public_key(account_id).raw_public_key()),
            )
            ledger_key = LedgerKey.from_account(LedgerKeyAccount(account_id=AccountID(account_pubkey)))

            response = await server.get_ledger_entries([ledger_key])

            local_asset_names = set()
            for trading_pair in self._trading_pairs:
                base, quote = trading_pair.split("-")
                local_asset_names.add(base)
                local_asset_names.add(quote)

            # Reset balances
            for asset_name in local_asset_names:
                self._account_balances[asset_name] = Decimal("0")
                self._account_available_balances[asset_name] = Decimal("0")

            if response.entries:
                for entry in response.entries:
                    data = LedgerEntryData.from_xdr(entry.xdr)
                    if data.account:
                        # Native XLM balance
                        xlm_balance = Decimal(str(data.account.balance.int64)) / Decimal("10000000")
                        reserve = CONSTANTS.ACCOUNT_BASE_RESERVE + (
                            Decimal(str(data.account.num_sub_entries.uint32)) * CONSTANTS.LEDGER_ENTRY_RESERVE
                        )
                        if "XLM" in local_asset_names:
                            self._account_balances["XLM"] = xlm_balance
                            self._account_available_balances["XLM"] = max(
                                Decimal("0"), xlm_balance - reserve
                            )

            # For non-native assets, query trustline entries
            for trading_pair in self._trading_pairs:
                base_asset, quote_asset = trading_pair_to_assets(trading_pair, self._custom_markets)
                for asset, name in [
                    (base_asset, trading_pair.split("-")[0]),
                    (quote_asset, trading_pair.split("-")[1]),
                ]:
                    if not asset.is_native() and name in local_asset_names:
                        try:
                            tl_asset = asset.to_trust_line_asset_xdr_object()
                            tl_key = LedgerKey.from_trust_line(LedgerKeyTrustLine(
                                account_id=AccountID(account_pubkey),
                                asset=tl_asset,
                            ))
                            tl_response = await server.get_ledger_entries([tl_key])
                            if tl_response.entries:
                                for tl_entry in tl_response.entries:
                                    tl_data = LedgerEntryData.from_xdr(tl_entry.xdr)
                                    if tl_data.trust_line:
                                        balance = Decimal(str(tl_data.trust_line.balance.int64)) / Decimal("10000000")
                                        self._account_balances[name] = balance
                                        self._account_available_balances[name] = balance
                        except Exception as e:
                            self.logger().debug(f"Error fetching trustline for {name}: {e}")
        except Exception as e:
            self.logger().error(f"Error updating balances: {e}", exc_info=True)
        finally:
            await server.close()

    # ---- Fees ----

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> AddedToCostTradeFee:
        # Stellar DEX has no maker/taker fees, only network base fee
        fee = AddedToCostTradeFee(
            percent=Decimal("0"),
            flat_fees=[TokenAmount(token="XLM", amount=Decimal("0.00001"))],
        )
        return fee

    async def _update_trading_fees(self):
        pass  # Stellar DEX has no variable fees

    # ---- User stream event listener ----

    async def _user_stream_event_listener(self):
        """Process events from the user stream."""
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")

                if event_type == "order_change":
                    await self._process_order_change_event(event_message)
                elif event_type == "balance_update":
                    pass  # Balances are updated periodically via _update_balances
                elif event_type == "trade":
                    pass  # Trades are processed via order changes

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in user stream listener: {e}", exc_info=True)

    # ---- Process order change events ----

    async def _process_order_change_event(self, event: dict):
        """Process an order change event from the user stream."""
        change = event["change"]
        ledger_sequence = event["ledger_sequence"]
        timestamp = event["timestamp"]

        if isinstance(change, StellarOrderCreated):
            offer_id = change.order.id
            client_order_id = self._offer_id_to_order_id.get(offer_id)
            if client_order_id is None:
                return  # Not our order
            tracked_order = self._order_tracker.active_orders.get(client_order_id)
            if tracked_order is None:
                return
            # Order confirmed on ledger
            if tracked_order.current_state == OrderState.PENDING_CREATE:
                order_update = OrderUpdate(
                    client_order_id=client_order_id,
                    exchange_order_id=str(offer_id),
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=timestamp,
                    new_state=OrderState.OPEN,
                )
                self._order_tracker.process_order_update(order_update)

        elif isinstance(change, StellarOrderUpdated):
            offer_id = change.order.id
            client_order_id = self._offer_id_to_order_id.get(offer_id)
            if client_order_id is None:
                return
            tracked_order = self._order_tracker.active_orders.get(client_order_id)
            if tracked_order is None:
                return
            # Offer was updated (partial fill)
            if tracked_order.current_state in [OrderState.OPEN, OrderState.PARTIALLY_FILLED]:
                # Calculate fill amount from the difference
                original_amount = tracked_order.amount
                remaining_amount = Decimal(str(change.order.amount))
                filled_amount = original_amount - remaining_amount

                if filled_amount > Decimal("0"):
                    fee = AddedToCostTradeFee(
                        percent=Decimal("0"),
                        flat_fees=[TokenAmount(token="XLM", amount=Decimal("0.00001"))],
                    )
                    trade_update = TradeUpdate(
                        trade_id=f"{offer_id}_{ledger_sequence}",
                        client_order_id=client_order_id,
                        exchange_order_id=str(offer_id),
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_base_amount=filled_amount,
                        fill_quote_amount=filled_amount * Decimal(str(change.order.price)),
                        fill_price=Decimal(str(change.order.price)),
                        fill_timestamp=timestamp,
                    )
                    self._order_tracker.process_trade_update(trade_update)

                    order_update = OrderUpdate(
                        client_order_id=client_order_id,
                        exchange_order_id=str(offer_id),
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=timestamp,
                        new_state=OrderState.PARTIALLY_FILLED,
                    )
                    self._order_tracker.process_order_update(order_update)

        elif isinstance(change, StellarOrderRemoved):
            offer_id = change.id
            client_order_id = self._offer_id_to_order_id.get(offer_id)
            if client_order_id is None:
                return
            tracked_order = self._order_tracker.active_orders.get(client_order_id)
            if tracked_order is None:
                return

            if tracked_order.current_state == OrderState.PENDING_CANCEL:
                # This was a cancellation
                order_update = OrderUpdate(
                    client_order_id=client_order_id,
                    exchange_order_id=str(offer_id),
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=timestamp,
                    new_state=OrderState.CANCELED,
                )
                self._order_tracker.process_order_update(order_update)
            else:
                # Order was fully filled (removed from book)
                fee = AddedToCostTradeFee(
                    percent=Decimal("0"),
                    flat_fees=[TokenAmount(token="XLM", amount=Decimal("0.00001"))],
                )
                # Remaining amount was filled
                remaining = tracked_order.amount - tracked_order.executed_amount_base
                if remaining > Decimal("0") and tracked_order.price:
                    trade_update = TradeUpdate(
                        trade_id=f"{offer_id}_{timestamp}",
                        client_order_id=client_order_id,
                        exchange_order_id=str(offer_id),
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_base_amount=remaining,
                        fill_quote_amount=remaining * tracked_order.price,
                        fill_price=tracked_order.price,
                        fill_timestamp=timestamp,
                    )
                    self._order_tracker.process_trade_update(trade_update)

                order_update = OrderUpdate(
                    client_order_id=client_order_id,
                    exchange_order_id=str(offer_id),
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=timestamp,
                    new_state=OrderState.FILLED,
                )
                self._order_tracker.process_order_update(order_update)

            # Cleanup
            self._offer_id_to_order_id.pop(offer_id, None)

    # ---- Cancel all ----

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        return await super().cancel_all(CONSTANTS.CANCEL_ALL_TIMEOUT)

    # ---- Trade updates ----

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        return []  # Trade updates come from the user stream

    # ---- Order status ----

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """Check order status by looking up the offer on the ledger."""
        if tracked_order.exchange_order_id is None:
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=tracked_order.current_state,
            )

        server = self._get_soroban_server()
        try:
            offer_id = int(tracked_order.exchange_order_id)

            from stellar_sdk.xdr import AccountID, Int64, LedgerKey, LedgerKeyOffer, PublicKey, PublicKeyType, Uint256

            account_pubkey = PublicKey(
                type=PublicKeyType.PUBLIC_KEY_TYPE_ED25519,
                ed25519=Uint256(self._stellar_auth.get_keypair().raw_public_key()),
            )
            offer_key = LedgerKey.from_offer(LedgerKeyOffer(
                seller_id=AccountID(account_pubkey),
                offer_id=Int64(offer_id),
            ))

            response = await server.get_ledger_entries([offer_key])

            if response.entries:
                # Offer still exists — it's still open
                return OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=time.time(),
                    new_state=OrderState.OPEN,
                )
            else:
                # Offer not found — it was either filled or cancelled
                if tracked_order.current_state == OrderState.PENDING_CANCEL:
                    new_state = OrderState.CANCELED
                else:
                    new_state = OrderState.FILLED
                return OrderUpdate(
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=time.time(),
                    new_state=new_state,
                )
        except Exception as e:
            self.logger().error(f"Error checking order status for {tracked_order.client_order_id}: {e}")
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=time.time(),
                new_state=tracked_order.current_state,
            )
        finally:
            await server.close()
