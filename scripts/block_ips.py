#!/usr/bin/env python3
"""
Script to pre-block IP addresses before starting the node
"""

import redis
import json
import time
import sys

# List of IPs to block
BLOCKED_IPS = [
    "190.6.51.73"
]

def block_ips():
    """Block IPs in Redis before node startup"""
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        # Test connection
        r.ping()
        print("Connected to Redis")
        
        for ip in BLOCKED_IPS:
            # Add to permanently blocked list
            r.sadd("permanently_blocked_ips", ip)
            
            # Create blocked client entry
            client_data = {
                'ip': ip,
                'user_agent': 'Pre-blocked',
                'first_seen': time.time(),
                'last_seen': time.time(),
                'request_count': 0,
                'failed_requests': 0,
                'threat_level': 'critical',
                'blocked_until': float('inf'),
                'warnings': 999,
                'block_reason': 'Pre-blocked at startup'
            }
            
            # Store for 1 year
            r.setex(f"client:{ip}", 31536000, json.dumps(client_data))
            print(f"Blocked IP: {ip}")
            
        print(f"Successfully blocked {len(BLOCKED_IPS)} IP addresses")
        
    except redis.ConnectionError:
        print("Error: Could not connect to Redis. Make sure Redis is running.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    block_ips()