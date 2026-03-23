import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from bidict import bidict
from stellar_sdk import AiohttpClient, Keypair, Network, SorobanServerAsync, TransactionBuilder
from stellar_sdk.soroban_rpc import GetTransactionStatus, SendTransactionStatus

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
    ChannelAccount,
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
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

# Timeout for pending transactions before marking them as failed
PENDING_TX_TIMEOUT = 10  # seconds
PENDING_TX_POLL_INTERVAL = 2  # seconds
TX_BATCH_WAIT_MS = 100  # milliseconds to wait for batching operations
TX_MAX_OPERATIONS = 100  # max operations per Stellar transaction


@dataclass
class BatchedOperation:
    """A single operation queued for batching into a transaction."""
    client_order_id: str
    trading_pair: str
    future: asyncio.Future
    # For new orders
    is_cancel: bool = False
    trade_type: Optional[TradeType] = None
    amount: Optional[Decimal] = None
    price: Optional[Decimal] = None
    # For cancels
    cancel_offer_id: Optional[int] = None
    cancel_trade_type: Optional[TradeType] = None


@dataclass
class PendingTransaction:
    """Tracks a submitted but unconfirmed transaction."""
    tx_hash: str
    order_ids: List[str]  # client_order_ids in operation order
    trading_pairs: List[str]  # trading pairs in operation order
    submit_time: float
    channel: Optional[ChannelAccount] = None
    cancel_offer_ids: Optional[Dict[str, int]] = None  # client_order_id -> offer_id for cancels
    is_cancel_flags: Optional[Dict[str, bool]] = None  # client_order_id -> is_cancel


class StellarOrderTracker(ClientOrderTracker):
    TRADE_FILLS_WAIT_TIMEOUT = 20


class StellarExchange(ExchangePyBase):
    web_utils = stellar_web_utils

    def __init__(
        self,
        stellar_secret_key: str,
        rpc_url: str,
        channel_account_secret_keys: str = None,
        custom_markets: Optional[Dict[str, StellarMarket]] = None,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
    ):
        self._stellar_secret_key = stellar_secret_key
        self._rpc_url = rpc_url
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._custom_markets = custom_markets or {}
        # Merge default markets with custom markets (custom overrides defaults)
        self._all_markets: Dict[str, StellarMarket] = self._load_markets()

        self._stellar_auth: StellarAuth = self.authenticator
        self._nonce_creator = NonceCreator.for_milliseconds()
        self._network_passphrase = Network.PUBLIC_NETWORK_PASSPHRASE

        # Channel account pool for parallel tx submission
        channel_keys = []
        if channel_account_secret_keys:
            channel_keys = [k.strip() for k in channel_account_secret_keys.split(",") if k.strip()]
        self._channel_pool: Optional[ChannelAccountPool] = (
            ChannelAccountPool(channel_keys) if channel_keys else None
        )

        # Offer ID to client_order_id mapping
        self._offer_id_to_order_id: Dict[int, str] = {}
        # Pending transactions awaiting confirmation
        self._pending_transactions: Dict[str, PendingTransaction] = {}

        # Transaction batching queue
        self._batch_queue: asyncio.Queue = asyncio.Queue()

        # Order locking
        self._order_status_locks: Dict[str, asyncio.Lock] = {}
        self._order_status_lock_manager = asyncio.Lock()

        # Background tasks
        self._pending_order_resolver_task: Optional[asyncio.Task] = None
        self._tx_batcher_task: Optional[asyncio.Task] = None

        super().__init__(balance_asset_limit, rate_limits_share_pct)

        # Must be called AFTER super().__init__() so _trading_pair_symbol_map exists
        self._initialize_trading_pair_symbols_from_exchange_info(self._all_markets)

    # ---- Order tracker ----

    def _create_order_tracker(self) -> ClientOrderTracker:
        return StellarOrderTracker(connector=self)

    # ---- Network lifecycle ----

    async def start_network(self):
        await super().start_network()
        self._pending_order_resolver_task = safe_ensure_future(self._pending_order_resolver_loop())
        self._tx_batcher_task = safe_ensure_future(self._tx_batcher_loop())

    async def stop_network(self):
        if self._pending_order_resolver_task is not None:
            self._pending_order_resolver_task.cancel()
            self._pending_order_resolver_task = None
        if self._tx_batcher_task is not None:
            self._tx_batcher_task.cancel()
            self._tx_batcher_task = None
        # Release any held channel accounts
        for pending_tx in list(self._pending_transactions.values()):
            if pending_tx.channel is not None and self._channel_pool is not None:
                self._channel_pool.release(pending_tx.channel)
        self._pending_transactions.clear()
        await super().stop_network()

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

    def _load_markets(self) -> Dict[str, StellarMarket]:
        """Load default markets from constants and merge with custom markets."""
        loaded_markets: Dict[str, StellarMarket] = {}
        for k, v in CONSTANTS.MARKETS.items():
            loaded_markets[k] = StellarMarket(
                base=v["base"],
                quote=v["quote"],
                base_issuer=v["base_issuer"],
                quote_issuer=v["quote_issuer"],
                trading_pair_symbol=k,
            )
        loaded_markets.update(self._custom_markets)
        return loaded_markets

    async def _initialize_trading_pair_symbol_map(self):
        pass

    async def _make_network_check_request(self):
        server = self._get_soroban_server()
        try:
            await server.get_health()
        finally:
            await server.close()

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

    # ---- Place order (batched) ----

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
        Queue an order for batched submission.
        Returns (temporary_id, submit_time). The background batcher will submit the tx
        and the resolver will confirm it with the real offer_id.
        """
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        op = BatchedOperation(
            client_order_id=order_id,
            trading_pair=trading_pair,
            future=future,
            trade_type=trade_type,
            amount=amount,
            price=price,
        )
        await self._batch_queue.put(op)

        # Wait for the batcher to submit and return the tx_hash
        tx_hash = await future
        return tx_hash, time.time()

    # ---- Transaction batcher ----

    async def _tx_batcher_loop(self):
        """
        Background loop that collects operations for TX_BATCH_WAIT_MS, then
        builds and submits a single transaction with up to TX_MAX_OPERATIONS.
        """
        while True:
            try:
                # Wait for the first operation
                first_op = await self._batch_queue.get()
                batch: List[BatchedOperation] = [first_op]

                # Wait TX_BATCH_WAIT_MS to collect more operations
                deadline = time.time() + TX_BATCH_WAIT_MS / 1000.0
                while len(batch) < TX_MAX_OPERATIONS:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    try:
                        op = await asyncio.wait_for(self._batch_queue.get(), timeout=remaining)
                        batch.append(op)
                    except asyncio.TimeoutError:
                        break

                self.logger().info(f"Batching {len(batch)} operations into one transaction")
                await self._submit_batch(batch)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in tx batcher: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _submit_batch(self, batch: List[BatchedOperation]):
        """Build and submit a single transaction for a batch of operations."""
        server = self._get_soroban_server()
        channel = None

        try:
            # Acquire a channel account (or use main)
            if self._channel_pool is not None and self._channel_pool.pool_size > 0:
                channel = await self._channel_pool.acquire()
                source_keypair = channel.keypair
            else:
                source_keypair = self._stellar_auth.get_keypair()

            account = await server.load_account(source_keypair.public_key)

            builder = TransactionBuilder(
                source_account=account,
                network_passphrase=self._network_passphrase,
                base_fee=CONSTANTS.BASE_FEE * len(batch),
            )
            builder.set_timeout(30)

            main_account_id = self._stellar_auth.get_keypair().public_key

            # Add each operation to the transaction
            for op in batch:
                base_asset, quote_asset = trading_pair_to_assets(op.trading_pair, self._all_markets)

                if op.is_cancel:
                    # Cancel operation
                    if op.cancel_trade_type == TradeType.SELL:
                        builder.append_manage_sell_offer_op(
                            selling=base_asset,
                            buying=quote_asset,
                            amount="0",
                            price="1",
                            offer_id=op.cancel_offer_id,
                            source=main_account_id,
                        )
                    else:
                        builder.append_manage_buy_offer_op(
                            selling=quote_asset,
                            buying=base_asset,
                            amount="0",
                            price="1",
                            offer_id=op.cancel_offer_id,
                            source=main_account_id,
                        )
                else:
                    # New order operation
                    if op.trade_type == TradeType.BUY:
                        builder.append_manage_buy_offer_op(
                            selling=quote_asset,
                            buying=base_asset,
                            amount=str(op.amount),
                            price=str(op.price),
                            offer_id=0,
                            source=main_account_id,
                        )
                    else:
                        builder.append_manage_sell_offer_op(
                            selling=base_asset,
                            buying=quote_asset,
                            amount=str(op.amount),
                            price=str(op.price),
                            offer_id=0,
                            source=main_account_id,
                        )

            tx = builder.build()
            tx.sign(self._stellar_auth.get_keypair())
            if channel is not None:
                tx.sign(channel.keypair)

            response = await server.send_transaction(tx)
            submit_time = time.time()

            self.logger().info(
                f"Submitted batch tx ({len(batch)} ops): "
                f"status={response.status}, hash={response.hash}"
            )

            if response.status == SendTransactionStatus.ERROR:
                if channel is not None and self._channel_pool is not None:
                    self._channel_pool.release(channel)
                    channel = None
                raise Exception(f"Batch transaction submission failed: {response.status}")

            # Mark all orders as PENDING_CREATE and resolve futures with tx_hash
            order_ids = []
            trading_pairs = []
            cancel_offer_ids = {}
            is_cancel_flags = {}

            for op in batch:
                order_ids.append(op.client_order_id)
                trading_pairs.append(op.trading_pair)
                is_cancel_flags[op.client_order_id] = op.is_cancel

                if op.is_cancel:
                    cancel_offer_ids[op.client_order_id] = op.cancel_offer_id
                else:
                    # Mark as PENDING_CREATE
                    order_update = OrderUpdate(
                        client_order_id=op.client_order_id,
                        exchange_order_id=response.hash,
                        trading_pair=op.trading_pair,
                        update_timestamp=submit_time,
                        new_state=OrderState.PENDING_CREATE,
                    )
                    self._order_tracker.process_order_update(order_update)

                # Resolve the future so _place_order / _place_cancel can return
                if not op.future.done():
                    op.future.set_result(response.hash)

            # Track the batch as a single pending transaction
            pending = PendingTransaction(
                tx_hash=response.hash,
                order_ids=order_ids,
                trading_pairs=trading_pairs,
                submit_time=submit_time,
                channel=channel,
                cancel_offer_ids=cancel_offer_ids if cancel_offer_ids else None,
                is_cancel_flags=is_cancel_flags,
            )
            self._pending_transactions[response.hash] = pending

        except Exception as e:
            # Release channel on failure
            if channel is not None and self._channel_pool is not None:
                self._channel_pool.release(channel)
            # Fail all operations in the batch
            for op in batch:
                if not op.future.done():
                    op.future.set_exception(e)
                if not op.is_cancel:
                    order_update = OrderUpdate(
                        trading_pair=op.trading_pair,
                        update_timestamp=time.time(),
                        new_state=OrderState.FAILED,
                        client_order_id=op.client_order_id,
                    )
                    self._order_tracker.process_order_update(order_update)
            self.logger().error(f"Batch submission failed: {e}", exc_info=True)
        finally:
            await server.close()

    # ---- Background pending order resolver ----

    async def _pending_order_resolver_loop(self):
        """
        Background loop that polls pending transactions for confirmation.
        On success: extracts offer IDs, maps them, and transitions orders to OPEN.
        On failure or timeout: transitions orders to FAILED.
        """
        while True:
            try:
                await asyncio.sleep(PENDING_TX_POLL_INTERVAL)
                if not self._pending_transactions:
                    continue

                server = self._get_soroban_server()
                try:
                    resolved_hashes = []
                    for tx_hash, pending in list(self._pending_transactions.items()):
                        try:
                            result = await server.get_transaction(tx_hash)

                            if result.status == GetTransactionStatus.SUCCESS:
                                self._resolve_pending_batch(pending, result)
                                resolved_hashes.append(tx_hash)

                            elif result.status == GetTransactionStatus.FAILED:
                                self.logger().error(
                                    f"Batch tx {tx_hash} ({len(pending.order_ids)} ops) failed on-chain"
                                )
                                self._fail_pending_batch(pending)
                                resolved_hashes.append(tx_hash)

                            elif result.status == GetTransactionStatus.NOT_FOUND:
                                elapsed = time.time() - pending.submit_time
                                if elapsed > PENDING_TX_TIMEOUT:
                                    self.logger().error(
                                        f"Batch tx {tx_hash} ({len(pending.order_ids)} ops) "
                                        f"timed out after {elapsed:.0f}s"
                                    )
                                    self._fail_pending_batch(pending)
                                    resolved_hashes.append(tx_hash)

                        except Exception as e:
                            elapsed = time.time() - pending.submit_time
                            if elapsed > PENDING_TX_TIMEOUT:
                                self.logger().error(
                                    f"Batch tx {tx_hash} timed out with error: {e}"
                                )
                                self._fail_pending_batch(pending)
                                resolved_hashes.append(tx_hash)
                            else:
                                self.logger().debug(f"Polling tx {tx_hash}: {e}")

                    for tx_hash in resolved_hashes:
                        self._cleanup_pending_tx(tx_hash)

                finally:
                    await server.close()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in pending order resolver: {e}", exc_info=True)
                await asyncio.sleep(PENDING_TX_POLL_INTERVAL)

    def _resolve_pending_batch(self, pending: PendingTransaction, tx_result):
        """Process a successfully confirmed batch transaction."""
        offer_ids = self._extract_offer_ids_from_result(tx_result)
        is_cancel_flags = pending.is_cancel_flags or {}
        cancel_offer_ids = pending.cancel_offer_ids or {}
        now = time.time()

        for i, client_order_id in enumerate(pending.order_ids):
            is_cancel = is_cancel_flags.get(client_order_id, False)
            trading_pair = pending.trading_pairs[i] if i < len(pending.trading_pairs) else ""

            if is_cancel:
                cancel_oid = cancel_offer_ids.get(client_order_id)
                order_update = OrderUpdate(
                    client_order_id=client_order_id,
                    exchange_order_id=str(cancel_oid) if cancel_oid else None,
                    trading_pair=trading_pair,
                    update_timestamp=now,
                    new_state=OrderState.CANCELED,
                )
                self._order_tracker.process_order_update(order_update)
                if cancel_oid:
                    self._offer_id_to_order_id.pop(cancel_oid, None)
                self.logger().info(f"Cancel confirmed for order {client_order_id}")
            else:
                # Extract the offer_id for this operation (by index)
                offer_id = offer_ids[i] if i < len(offer_ids) else None

                if offer_id is not None:
                    exchange_order_id = str(offer_id)
                    self._offer_id_to_order_id[offer_id] = client_order_id
                else:
                    exchange_order_id = pending.tx_hash

                # Explicitly update exchange_order_id on the tracked order
                tracked_order = self._order_tracker.active_orders.get(client_order_id)
                if tracked_order is not None:
                    tracked_order.update_exchange_order_id(exchange_order_id)

                order_update = OrderUpdate(
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=trading_pair,
                    update_timestamp=now,
                    new_state=OrderState.OPEN,
                )
                self._order_tracker.process_order_update(order_update)
                self.logger().info(
                    f"Order {client_order_id} confirmed: offer_id={offer_id}, hash={pending.tx_hash}"
                )

    def _fail_pending_batch(self, pending: PendingTransaction):
        """Mark all orders in a pending batch as failed."""
        is_cancel_flags = pending.is_cancel_flags or {}
        for i, client_order_id in enumerate(pending.order_ids):
            is_cancel = is_cancel_flags.get(client_order_id, False)
            if is_cancel:
                self.logger().error(f"Cancel failed for order {client_order_id}")
                continue
            trading_pair = pending.trading_pairs[i] if i < len(pending.trading_pairs) else ""
            order_update = OrderUpdate(
                client_order_id=client_order_id,
                trading_pair=trading_pair,
                update_timestamp=time.time(),
                new_state=OrderState.FAILED,
            )
            self._order_tracker.process_order_update(order_update)

    def _cleanup_pending_tx(self, tx_hash: str):
        """Release channel account and clean up pending tx tracking."""
        pending = self._pending_transactions.pop(tx_hash, None)
        if pending and pending.channel is not None and self._channel_pool is not None:
            self._channel_pool.release(pending.channel)

    # ---- Offer ID extraction ----

    def _extract_offer_ids_from_result(self, tx_result) -> List[Optional[int]]:
        """Extract offer IDs from a transaction result, one per operation in order."""
        offer_ids = []
        try:
            if hasattr(tx_result, 'result_xdr') and tx_result.result_xdr:
                from stellar_sdk.xdr import TransactionResult
                result = TransactionResult.from_xdr(tx_result.result_xdr)
                for op_result in result.result.results:
                    tr = op_result.tr
                    offer_id = None
                    # Check ManageSellOffer result
                    if tr.manage_sell_offer_result is not None:
                        offer_result = tr.manage_sell_offer_result.success
                        if offer_result and offer_result.offer and offer_result.offer.offer:
                            offer_id = offer_result.offer.offer.offer_id.int64
                    # Check ManageBuyOffer result
                    if tr.manage_buy_offer_result is not None:
                        offer_result = tr.manage_buy_offer_result.success
                        if offer_result and offer_result.offer and offer_result.offer.offer:
                            offer_id = offer_result.offer.offer.offer_id.int64
                    offer_ids.append(offer_id)
        except Exception as e:
            self.logger().debug(f"Could not extract offer IDs from result: {e}")
        return offer_ids

    # ---- Cancel order (batched) ----

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """Queue a cancel for batched submission."""
        exchange_order_id = tracked_order.exchange_order_id

        # If the order is still pending confirmation (no offer_id yet),
        # wait for the resolver to confirm it first.
        if exchange_order_id is None or not exchange_order_id.isdigit():
            pending_hash = None
            for tx_hash, pending in self._pending_transactions.items():
                if order_id in pending.order_ids:
                    is_cancel = (pending.is_cancel_flags or {}).get(order_id, False)
                    if not is_cancel:
                        pending_hash = tx_hash
                        break

            if pending_hash is not None:
                for _ in range(20):
                    if pending_hash not in self._pending_transactions:
                        break
                    await asyncio.sleep(0.5)

                tracked_order = self._order_tracker.active_orders.get(order_id)
                if tracked_order is None:
                    self.logger().info(f"Order {order_id} already resolved, skip cancel")
                    return True
                exchange_order_id = tracked_order.exchange_order_id

            if exchange_order_id is None or not exchange_order_id.isdigit():
                self.logger().error(
                    f"Cannot cancel order {order_id}: no valid offer_id "
                    f"(exchange_order_id={exchange_order_id})"
                )
                return False

        offer_id = int(exchange_order_id)
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        op = BatchedOperation(
            client_order_id=order_id,
            trading_pair=tracked_order.trading_pair,
            future=future,
            is_cancel=True,
            cancel_offer_id=offer_id,
            cancel_trade_type=tracked_order.trade_type,
        )
        await self._batch_queue.put(op)

        try:
            await future
            return True
        except Exception as e:
            self.logger().error(f"Order cancellation failed for {order_id}: {e}")
            return False

    # ---- Cancel and process update ----

    async def _execute_order_cancel_and_process_update(self, order: InFlightOrder) -> bool:
        """Cancel an order — the background resolver handles state transitions."""
        if order.current_state in [OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED]:
            return order.current_state == OrderState.CANCELED

        order_update = OrderUpdate(
            client_order_id=order.client_order_id,
            trading_pair=order.trading_pair,
            update_timestamp=time.time(),
            new_state=OrderState.PENDING_CANCEL,
        )
        self._order_tracker.process_order_update(order_update)

        return await self._place_cancel(order.client_order_id, order)

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
                LedgerEntryType,
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
            ledger_key = LedgerKey(
                type=LedgerEntryType.ACCOUNT,
                account=LedgerKeyAccount(account_id=AccountID(account_pubkey)),
            )

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
                base_asset, quote_asset = trading_pair_to_assets(trading_pair, self._all_markets)
                for asset, name in [
                    (base_asset, trading_pair.split("-")[0]),
                    (quote_asset, trading_pair.split("-")[1]),
                ]:
                    if not asset.is_native() and name in local_asset_names:
                        try:
                            tl_asset = asset.to_trust_line_asset_xdr_object()
                            tl_key = LedgerKey(
                                type=LedgerEntryType.TRUSTLINE,
                                trust_line=LedgerKeyTrustLine(
                                    account_id=AccountID(account_pubkey),
                                    asset=tl_asset,
                                ),
                            )
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
            flat_fees=[TokenAmount(token="XLM", amount=Decimal("0.0001"))],
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

        # If exchange_order_id is still a tx_hash (not a numeric offer_id),
        # the order is still pending confirmation — let the resolver handle it
        if not tracked_order.exchange_order_id.isdigit():
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

            from stellar_sdk.xdr import (
                AccountID,
                Int64,
                LedgerEntryType,
                LedgerKey,
                LedgerKeyOffer,
                PublicKey,
                PublicKeyType,
                Uint256,
            )

            account_pubkey = PublicKey(
                type=PublicKeyType.PUBLIC_KEY_TYPE_ED25519,
                ed25519=Uint256(self._stellar_auth.get_keypair().raw_public_key()),
            )
            offer_key = LedgerKey(
                type=LedgerEntryType.OFFER,
                offer=LedgerKeyOffer(
                    seller_id=AccountID(account_pubkey),
                    offer_id=Int64(offer_id),
                ),
            )

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
