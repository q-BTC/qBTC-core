#!/usr/bin/env python3
"""Test RPC directly without going through Docker networking"""

import requests
import json

# Test from inside the container
url = "http://localhost:8332/"
payload = {
    "jsonrpc": "2.0",
    "method": "getblockchaininfo",
    "params": [],
    "id": 1
}

print(f"Testing RPC at {url}")
try:
    response = requests.post(url, json=payload, timeout=5)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")
except requests.exceptions.Timeout:
    print("Request timed out after 5 seconds")
except Exception as e:
    print(f"Error: {e}")