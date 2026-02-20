#!/bin/bash
set -e

# Require WALLET_PASSWORD to be set — no default
WALLET_FILE="${WALLET_FILE:-wallet.json}"
if [ -z "$WALLET_PASSWORD" ]; then
    echo "ERROR: WALLET_PASSWORD environment variable is not set. Cannot start node."
    exit 1
fi

echo "Wallet file: $WALLET_FILE"
echo "Password: (hidden)"

# Generate or unlock wallet
python3 -c "
import os
from wallet.wallet import get_or_create_wallet
import json

password = os.getenv('WALLET_PASSWORD')

# Force wallet generation or loading
wallet = get_or_create_wallet(fname='/app/${WALLET_FILE}', password=password)

print(f'Wallet ready: Address {wallet[\"address\"]}')
"

# Run main.py with passed arguments
exec python3 main.py "$@"
