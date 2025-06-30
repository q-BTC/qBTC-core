#!/usr/bin/env python3
"""broadcast_tx_test_harness.py

Interactively crafts and submits a **`broadcast_tx`** request against your
Q‑Safe node (the FastAPI server with the `/worker` endpoint).

▪Loads an existing wallet (or creates one on first run) **using the same
  `wallet.py` ML‑DSA‑87 logic** your node expects.
▪Builds the canonical message `sender:receiver:amount:timestamp:chain_id`.
▪Signs it with `wallet.sign_transaction()`.
▪Base‑64–encodes the _message_, _signature_, and _compressed public key_.
▪POSTs the JSON payload to your node and prints the JSON response.

Usage examples
--------------
```bash
# simple invocation (prompts for wallet passphrase)
python broadcast_tx_test_harness.py --receiver bqs1Hpm... --amount 100

# specify custom node URL, wallet file and passphrase inline
python broadcast_tx_test_harness.py \
  --node http://localhost:8000 \
  --wallet mywallet.json \
  --password "hunter2" \
  --receiver bqs1GPSETB9Kz... \
  --amount 42.5
```"""
from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from decimal import Decimal
import requests
from wallet.wallet import get_or_create_wallet,sign_transaction

def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def build_and_sign_message(sender: str, receiver: str, amount: Decimal, timestamp: str, chain_id: str, privkey_hex: str) -> tuple[str, str]:
    """Return `(message_str, signature_hex)`.

    `message` format: `sender:receiver:amount:timestamp:chain_id` (all plain text).
    """
    message_str = f"{sender}:{receiver}:{amount.normalize()}:{timestamp}:{chain_id}"
    signature_hex = sign_transaction(message_str, privkey_hex)
    return message_str, signature_hex

# -------------------------------------------------------------------------------
# CLI entry‑point
# -------------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--node", default="http://127.0.0.1:8000", help="Base URL of the FastAPI node")
    parser.add_argument("--receiver", required=True, help="Destination BQS address")
    parser.add_argument("--amount", required=True, type=Decimal, help="Amount to send (in whole coins, decimals allowed)")
    parser.add_argument("--timestamp", help="Optional timestamp; defaults to unix‑ms timestamp")
    parser.add_argument("--chain_id", default="1", help="Chain ID for the network (default: 1)")
    parser.add_argument("--wallet", default="wallet.json", help="Wallet JSON file")
    parser.add_argument("--password", help="Wallet passphrase (if omitted you'll be prompted)")
    args = parser.parse_args()

    # -----------------------------------------------------------------------
    # Load or create wallet
    # -----------------------------------------------------------------------
    w = get_or_create_wallet(fname=args.wallet, password=args.password)
    sender_addr: str = w["address"]
    priv_hex: str = w["privateKey"]
    pub_hex: str = w["publicKey"]

    timestamp: str = args.timestamp or str(int(time.time() * 1000))
    chain_id: str = args.chain_id

    # -----------------------------------------------------------------------
    # Build & sign message
    # -----------------------------------------------------------------------
    message_str, signature_hex = build_and_sign_message(
        sender_addr, args.receiver, args.amount, timestamp, chain_id, priv_hex
    )

    # -----------------------------------------------------------------------
    # Build request payload
    # -----------------------------------------------------------------------
    payload = {
        "request_type": "broadcast_tx",
        "message": b64(message_str.encode()),
        "signature": b64(bytes.fromhex(signature_hex)),
        "pubkey": b64(bytes.fromhex(pub_hex)),
    }

    print("⤵  POSTing to", args.node + "/worker")
    print(json.dumps(payload, indent=2)[:400] + "…\n")  # preview (truncated)

    try:
        r = requests.post(args.node + "/worker", json=payload, timeout=10)
    except requests.RequestException as e:
        sys.exit("Network error: " + str(e))

    print("Status:", r.status_code)
    try:
        print(json.dumps(r.json(), indent=2))
    except ValueError:
        print("Response parsing failed – raw body:\n", r.text)


if __name__ == "__main__":
    main()

