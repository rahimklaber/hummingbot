"""
Stellar DEX ledger reader for the Hummingbot connector.

This module processes Stellar ledger close metadata to extract order book changes
and trades from the Stellar decentralized exchange. It provides utilities for
parsing ledger entry changes, maintaining an internal order book representation,
and detecting trades from offer state transitions.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from stellar_sdk import Asset, StrKey
from stellar_sdk.xdr import LedgerCloseMeta, LedgerEntry, LedgerEntryChange, OfferEntry

from hummingbot.core.data_type.common import TradeType


@dataclass
class DomainLedgerEntryChange:
    changes: LedgerEntryChange
    transaction_hash: str
    operation_index: int


def get_ledger_sequence(meta: LedgerCloseMeta) -> int:
    """Return the ledger sequence number from the ledger close metadata."""
    return meta.v2.ledger_header.header.ledger_seq.uint32


def get_ledger_close_time(meta: LedgerCloseMeta) -> int:
    """Return the close time (unix timestamp) from the ledger close metadata."""
    return meta.v2.ledger_header.header.scp_value.close_time.time_point.uint64


def get_ledger_entry_changes_for_ledger(meta: LedgerCloseMeta) -> List[DomainLedgerEntryChange]:
    txs = filter(lambda x: int(x.result.result.result.code) >= 0, meta.v2.tx_processing)

    changes = [
        (change.changes.ledger_entry_changes, tx.result.transaction_hash.hash.hex(), idx)
        for tx in txs
        for idx, change in enumerate(tx.tx_apply_processing.v4.operations)
    ]

    flattened = [
        DomainLedgerEntryChange(changes=change, transaction_hash=tx, operation_index=op_idx)
        for change_list, tx, op_idx in changes
        for change in change_list
    ]

    return flattened


@dataclass
class StellarOrder:
    id: int
    selling_asset: Asset
    buying_asset: Asset
    amount: float
    price: float
    seller_id: str
    created_in_tx: Optional[str] = None
    created_in_op_index: Optional[int] = None


@dataclass
class StellarOrderCreated:
    order: StellarOrder


@dataclass
class StellarOrderUpdated:
    order: StellarOrder
    switched: bool


@dataclass
class StellarOrderRemoved:
    id: int


@dataclass
class StellarTrade:
    selling_asset: Asset
    buying_asset: Asset
    amount: float
    price: float
    buyer_id: str
    seller_id: str
    traded_in_tx: str
    traded_in_op_index: int


OrderChange = Union[StellarOrderCreated, StellarOrderUpdated, StellarOrderRemoved]


class InternalStellarOrderBook:
    def __init__(self, selling_asset: Asset, buying_asset: Asset):
        self.selling_asset = selling_asset
        self.buying_asset = buying_asset
        self.orders: Dict[int, StellarOrder] = {}

    def apply_order_creation(self, creation: StellarOrderCreated):
        order = creation.order
        assets = [self.selling_asset, self.buying_asset]
        if order.selling_asset not in assets or order.buying_asset not in assets:
            return
        if order.selling_asset != self.selling_asset:
            order.price = 1 / order.price
        self.orders[order.id] = order

    def apply_order_update(self, update: StellarOrderUpdated):
        order = update.order
        assets = [self.selling_asset, self.buying_asset]
        if order.selling_asset not in assets or order.buying_asset not in assets:
            return
        if order.selling_asset != self.selling_asset:
            order.price = 1 / order.price
        if order.id in self.orders:
            existing_order = self.orders[order.id]
            order.created_in_tx = existing_order.created_in_tx
            order.created_in_op_index = existing_order.created_in_op_index
        self.orders[order.id] = order

    def apply_order_removal(self, removal: StellarOrderRemoved):
        if removal.id in self.orders:
            del self.orders[removal.id]

    def apply_order_change(self, change: OrderChange) -> bool:
        if isinstance(change, StellarOrderCreated):
            prev_count = len(self.orders)
            self.apply_order_creation(change)
            return len(self.orders) > prev_count
        elif isinstance(change, StellarOrderUpdated):
            order_id = change.order.id
            self.apply_order_update(change)
            return order_id in self.orders
        elif isinstance(change, StellarOrderRemoved):
            order_id = change.id
            if order_id in self.orders:
                self.apply_order_removal(change)
                return True
            return False
        return False

    def get_buy_orders(self) -> List[StellarOrder]:
        buy_orders = [order for order in self.orders.values()
                      if order.buying_asset == self.selling_asset]
        return sorted(buy_orders, key=lambda x: x.price, reverse=True)

    def get_sell_orders(self) -> List[StellarOrder]:
        sell_orders = [order for order in self.orders.values()
                       if order.selling_asset == self.selling_asset]
        return sorted(sell_orders, key=lambda x: x.price)

    def get_best_bid(self) -> Optional[StellarOrder]:
        buy_orders = self.get_buy_orders()
        return buy_orders[0] if buy_orders else None

    def get_best_ask(self) -> Optional[StellarOrder]:
        sell_orders = self.get_sell_orders()
        return sell_orders[0] if sell_orders else None

    def get_spread(self) -> Optional[float]:
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return best_ask.price - best_bid.price
        return None

    def get_order_count(self) -> int:
        return len(self.orders)


def get_entry_or_none(change: LedgerEntryChange) -> Optional[LedgerEntry]:
    if change.created:
        return change.created
    elif change.updated:
        return change.updated
    else:
        return None


def create_stellar_order_from_offer_entry(
    entry: OfferEntry,
    tx_hash: Optional[str] = None,
    op_index: Optional[int] = None,
) -> StellarOrder:
    return StellarOrder(
        id=entry.offer_id.int64,
        selling_asset=Asset.from_xdr_object(entry.selling),
        buying_asset=Asset.from_xdr_object(entry.buying),
        amount=float(entry.amount.int64) / 1e7,
        price=float(entry.price.n.int32) / float(entry.price.d.int32),
        seller_id=StrKey.encode_ed25519_public_key(entry.seller_id.account_id.ed25519.uint256),
        created_in_tx=tx_hash,
        created_in_op_index=op_index
    )


def get_trades_from_ledger_entry_changes(
    ledger_entry_changes: List[DomainLedgerEntryChange],
    ledger_close_meta: LedgerCloseMeta
) -> List[Dict]:
    trades = []

    for idx in range(len(ledger_entry_changes)):
        domain_change = ledger_entry_changes[idx]
        domain_prev_change = ledger_entry_changes[idx - 1] if idx > 0 else None

        change = domain_change.changes
        prev_change = domain_prev_change.changes if domain_prev_change else None

        if change.removed and change.removed.offer:
            if prev_change and prev_change.state and prev_change.state.data.offer:
                prev_offer = prev_change.state.data.offer
                prev_amount = float(prev_offer.amount.int64) / 1e7

                if prev_amount > 0:
                    selling_asset = Asset.from_xdr_object(prev_offer.selling)
                    buying_asset = Asset.from_xdr_object(prev_offer.buying)
                    price = float(prev_offer.price.n.int32) / float(prev_offer.price.d.int32)

                    trade = {
                        "trading_pair": f"{selling_asset.code or 'XLM'}-{buying_asset.code or 'XLM'}",
                        "trade_type": TradeType.BUY.value,
                        "trade_id": abs(hash(f"{domain_change.transaction_hash}_{domain_change.operation_index}")),
                        "offer_id": prev_offer.offer_id.int64,
                        "seller_id": StrKey.encode_ed25519_public_key(prev_offer.seller_id.account_id.ed25519.uint256),
                        "update_id": ledger_close_meta.v2.ledger_header.header.ledger_seq.uint32,
                        "price": price,
                        "amount": prev_amount,
                        "timestamp": ledger_close_meta.v2.ledger_header.header.scp_value.close_time.time_point.uint64}
                    trades.append(trade)
            continue

        if change.updated and change.updated.data.offer:
            if prev_change and prev_change.state and prev_change.state.data.offer:
                current_offer = change.updated.data.offer
                prev_offer = prev_change.state.data.offer

                current_amount = float(current_offer.amount.int64) / 1e7
                prev_amount = float(prev_offer.amount.int64) / 1e7

                if prev_amount > current_amount:
                    filled_amount = prev_amount - current_amount
                    selling_asset = Asset.from_xdr_object(current_offer.selling)
                    buying_asset = Asset.from_xdr_object(current_offer.buying)
                    price = float(current_offer.price.n.int32) / float(current_offer.price.d.int32)

                    trade = {
                        "trading_pair": f"{selling_asset.code or 'XLM'}-{buying_asset.code or 'XLM'}",
                        "trade_type": TradeType.SELL.value,
                        "trade_id": abs(hash(f"{domain_change.transaction_hash}_{domain_change.operation_index}")),
                        "offer_id": current_offer.offer_id.int64,
                        "seller_id": StrKey.encode_ed25519_public_key(current_offer.seller_id.account_id.ed25519.uint256),
                        "update_id": ledger_close_meta.v2.ledger_header.header.ledger_seq.uint32,
                        "price": price,
                        "amount": filled_amount,
                        "timestamp": ledger_close_meta.v2.ledger_header.header.scp_value.close_time.time_point.uint64
                    }
                    trades.append(trade)

    return trades


def get_order_changes_from_ledger_entry_changes(
    ledger_entry_changes: List[DomainLedgerEntryChange]
) -> List[OrderChange]:
    orders = []

    for idx in range(len(ledger_entry_changes)):
        domain_change = ledger_entry_changes[idx]
        domain_prev_change = ledger_entry_changes[idx - 1] if idx > 0 else None

        change = domain_change.changes
        prev_change = domain_prev_change.changes if domain_prev_change else None

        if change.removed and change.removed.offer:
            order_id = change.removed.offer.offer_id.int64
            orders.append(StellarOrderRemoved(id=order_id))
            continue

        entry = get_entry_or_none(change)
        if not entry or not entry.data or not entry.data.offer:
            continue

        switched = False

        if change.created:
            stellar_order = create_stellar_order_from_offer_entry(
                entry.data.offer,
                domain_change.transaction_hash,
                domain_change.operation_index
            )
            orders.append(StellarOrderCreated(order=stellar_order))
            continue

        stellar_order = create_stellar_order_from_offer_entry(entry.data.offer)

        if prev_change and prev_change.state:
            prev_order_state = create_stellar_order_from_offer_entry(prev_change.state.data.offer)
            if prev_order_state.selling_asset != stellar_order.selling_asset:
                switched = True

        orders.append(StellarOrderUpdated(order=stellar_order, switched=switched))

    return orders
