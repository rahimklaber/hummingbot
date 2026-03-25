# Stellar Connector Setup Guide

This guide explains how to configure and run the Stellar spot connector in this folder.

## Important Notes

- This connector uses **Soroban RPC only**. **Horizon is not used**.
- The connector is currently configured for the **Stellar public network** because `stellar_exchange.py` uses `Network.PUBLIC_NETWORK_PASSPHRASE`.
- The default RPC URL is:

  ```text
  https://mainnet.sorobanrpc.com
  ```

- Order submission supports:
  - background transaction confirmation
  - transaction batching
  - channel accounts for parallel submission
  - shared ledger polling for public and private events

## Accounts You Need

You need:

1. A **main Stellar account**
   - holds the balances you want to trade
   - owns trustlines
   - owns the offers on the Stellar DEX

2. One or more **channel accounts**
   - used as transaction sources for sequence numbers
   - let the connector submit multiple transactions in parallel
   - should be funded with enough XLM to remain active

## Funding and Reserves

The connector assumes normal Stellar reserve rules:

- base account reserve: `1 XLM`
- per-subentry reserve: `0.5 XLM`

Practical guidance:

- Fund the **main account** with enough XLM for:
  - account reserve
  - trustline reserves
  - open offer reserves
  - transaction fees

- Fund each **channel account** with enough XLM to stay active and pay fees.

## Asset Trustlines

Before trading non-native assets, the **main account** must have trustlines for those assets.

Examples:

- To trade `XLM-USDC`, the main account must trust the configured `USDC` issuer.
- If you add other assets, add trustlines for those issuers before starting Hummingbot.

Channel accounts do not need asset balances for normal order submission, but they do need enough XLM to stay active and submit transactions.

## Markets and Issuers

The connector resolves Stellar assets from the market definitions in:

```text
hummingbot/connector/exchange/stellar/stellar_constants.py
```

Current default market definitions live in `MARKETS`.

Example:

```python
MARKETS = {
    "XLM-USDC": {
        "base": "XLM",
        "quote": "USDC",
        "base_issuer": "",
        "quote_issuer": "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
    },
}
```

Rules:

- native XLM uses an empty issuer string: `""`
- issued assets must use the correct issuer account
- if the issuer is wrong, the connector may read the wrong market or fail to resolve balances/orders correctly

If you want additional default markets, add them to `MARKETS` in `stellar_constants.py`.

## Connector Config Fields

The connector config is defined in:

```text
hummingbot/connector/exchange/stellar/stellar_utils.py
```

The required fields are:

### `stellar_secret_key`

Secret key for the main trading account.

This account:

- owns offers
- holds balances
- should have trustlines for non-XLM assets

### `rpc_url`

Soroban RPC endpoint.

Default:

```text
https://mainnet.sorobanrpc.com
```

Use a reliable RPC provider with good ledger availability.

### `channel_account_secret_keys`

Comma-separated secret keys for the channel accounts.

Example format:

```text
SCXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX,SCYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY
```

Do not include spaces unless you want them trimmed by the parser.

### `custom_markets`

Optional market override map used to resolve asset issuers.

This is mainly relevant if:

- you want to trade pairs not present in `stellar_constants.py`
- you want to override default issuer definitions

The model shape is:

```python
{
    "BASE-QUOTE": {
        "base": "BASE",
        "quote": "QUOTE",
        "base_issuer": "G...",
        "quote_issuer": "G...",
    }
}
```

For XLM, use an empty issuer string.

## Recommended Setup Flow

1. Create and fund your main Stellar account.
2. Create and fund one or more channel accounts.
3. Add trustlines on the main account for every issued asset you want to trade.
4. Verify issuer addresses for every configured market.
5. Confirm your RPC endpoint is a **public-network Soroban RPC** endpoint.
6. Run Hummingbot and connect the Stellar connector.
7. Start with a small order size and verify:
   - balances load correctly
   - orderbook data appears
   - orders move from pending to open
   - cancels are classified correctly

## Example Hummingbot Inputs

When Hummingbot prompts for connector values, provide:

- `stellar_secret_key`: your main account secret
- `rpc_url`: your Soroban RPC URL
- `channel_account_secret_keys`: comma-separated channel account secrets

## Operational Behavior

A few behaviors are useful to know while testing:

- Orders may be submitted in **batched transactions**.
- Newly submitted orders are first tracked in a pending state and resolved after ledger confirmation.
- Public order book data and private user events share a **single ledger polling path**.
- Cancellation is tracked explicitly so canceled offers are not mistaken for fills.

## Troubleshooting Checklist

If the connector is not behaving correctly, check:

1. Is the RPC endpoint reachable and serving the public network?
2. Does the main account exist and have enough XLM?
3. Do required trustlines exist on the main account?
4. Are the configured issuer addresses correct?
5. Are channel accounts funded and valid?
6. Are you using markets that exist in `MARKETS` or `custom_markets`?
7. Are balances and order books loading before strategy start?

## Security

- Never commit Stellar secret keys.
- Rotate any key that was ever exposed in logs, commits, or screenshots.
- Prefer dedicated trading and channel accounts instead of using a primary wallet.

## File Reference

- Connector implementation: `stellar_exchange.py`
- Config definitions: `stellar_utils.py`
- Market defaults: `stellar_constants.py`
- Ledger parsing: `stellar_ledger_reader.py`
- Shared ledger polling: `stellar_ledger_stream.py`
- Technical architecture doc: `TECHNICAL.md`
