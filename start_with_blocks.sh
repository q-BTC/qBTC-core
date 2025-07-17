#!/bin/bash
# Startup script that blocks IPs before starting the node

echo "Pre-blocking malicious IPs..."
python3 scripts/block_ips.py

if [ $? -eq 0 ]; then
    echo "Starting qBTC node..."
    python3 main.py
else
    echo "Failed to block IPs. Aborting startup."
    exit 1
fi