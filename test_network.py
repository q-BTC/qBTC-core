#!/usr/bin/env python3
"""Test script to check if nodes are networking properly"""

import requests
import json
import time

def check_node(name, port):
    """Check a node's network status"""
    print(f"\n=== Checking {name} (port {port}) ===")
    
    try:
        # Try the debug endpoint
        response = requests.get(f"http://localhost:{port}/debug/network", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"Validator ID: {data.get('validator_id', 'N/A')}")
            
            # Check DHT
            dht = data.get('dht', {})
            print(f"DHT Running: {dht.get('running', False)}")
            if 'error' in dht:
                print(f"DHT Error: {dht['error']}")
            
            # Check Gossip
            gossip = data.get('gossip', {})
            print(f"Gossip Running: {gossip.get('running', False)}")
            if gossip.get('running'):
                print(f"  - Node ID: {gossip.get('node_id')}")
                print(f"  - Port: {gossip.get('port')}")
                print(f"  - Is Bootstrap: {gossip.get('is_bootstrap')}")
                print(f"  - Peers: {gossip.get('dht_peers')} connected")
                if gossip.get('peer_list'):
                    print(f"  - Peer List: {gossip['peer_list']}")
        else:
            print(f"Debug endpoint not available (status: {response.status_code})")
            
        # Try basic health check
        response = requests.get(f"http://localhost:{port}/health", timeout=5)
        if response.status_code == 200:
            print("Health check: OK")
        else:
            print(f"Health check failed: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Cannot connect to {name} - is it running?")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

def main():
    print("Testing qBTC network connectivity...")
    print("=" * 50)
    
    # Check all nodes
    check_node("Bootstrap", 8080)
    check_node("Validator1", 8081)
    check_node("Validator2", 8082)
    
    print("\n" + "=" * 50)
    print("\nIf nodes are not networking:")
    print("1. Check docker logs: docker-compose logs")
    print("2. Ensure ports are not blocked")
    print("3. Check if bootstrap node is reachable")
    print("4. Verify DHT/Gossip ports match configuration")

if __name__ == "__main__":
    main()