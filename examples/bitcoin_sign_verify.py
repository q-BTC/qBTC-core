#!/usr/bin/env python3
"""
Example of Bitcoin message signing and verification using python-bitcoinlib
This shows how BlueWallet would sign messages and how qBTC-core verifies them
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from bitcoin.wallet import CBitcoinSecret, P2PKHBitcoinAddress
    from bitcoin.signmessage import BitcoinMessage, SignMessage, VerifyMessage
    from bitcoin.core import x
    import bitcoin
    bitcoin.SelectParams('mainnet')  # or 'testnet'
    BITCOIN_LIB_AVAILABLE = True
except ImportError:
    print("ERROR: python-bitcoinlib not installed")
    print("Install with: pip install python-bitcoinlib")
    BITCOIN_LIB_AVAILABLE = False
    sys.exit(1)

def example_sign_and_verify():
    """Example of signing and verifying a Bitcoin message"""
    
    print("=== Bitcoin Message Signing Example ===\n")
    
    # Example 1: Using a known private key (DO NOT USE IN PRODUCTION)
    # This is the private key for address 1CC3X2gu58d6wXUWMffpuzN9JAfTUWu4Kj
    privkey_wif = "5KJvsngHeMpm884wtkJNzQGaCErckhHJBGFsvd3VyK5qMZXj3hS"
    
    try:
        # Create private key object
        privkey = CBitcoinSecret(privkey_wif)
        
        # Get the corresponding Bitcoin address
        address = P2PKHBitcoinAddress.from_pubkey(privkey.pub)
        print(f"Bitcoin Address: {address}")
        
        # Create commitment message
        qbtc_address = "bqs1234567890abcdefghijklmnopqrstuvwxyz"
        message = f"I confirm that my Bitcoin address is: {address} and my qBTC address is: {qbtc_address}. Timestamp: 2024-01-01T00:00:00Z"
        print(f"\nMessage to sign:\n{message}")
        
        # Sign the message
        signature = SignMessage(privkey, BitcoinMessage(message))
        print(f"\nSignature (base64):\n{signature}")
        print(f"Signature length: {len(signature)} characters")
        
        # Verify the signature
        print("\n=== Verifying Signature ===")
        
        is_valid = VerifyMessage(address, BitcoinMessage(message), signature)
        print(f"Signature is valid: {is_valid}")
        
        # Test with wrong address
        wrong_address = P2PKHBitcoinAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
        is_valid_wrong = VerifyMessage(wrong_address, BitcoinMessage(message), signature)
        print(f"Signature with wrong address: {is_valid_wrong}")
        
        # Test with modified message
        modified_message = message.replace("2024", "2025")
        is_valid_modified = VerifyMessage(address, BitcoinMessage(modified_message), signature)
        print(f"Signature with modified message: {is_valid_modified}")
        
        print("\n=== Example API Request ===")
        print("This is what BlueWallet would send to qBTC-core:\n")
        
        import json
        api_payload = {
            "btcAddress": str(address),
            "qbtcAddress": qbtc_address,
            "message": message,
            "signature": signature
        }
        print(json.dumps(api_payload, indent=2))
        
    except Exception as e:
        print(f"Error: {e}")


def example_verify_from_bluewallet():
    """Example of verifying a signature from BlueWallet"""
    
    print("\n\n=== Verifying BlueWallet Signature ===\n")
    
    # Example data that would come from BlueWallet
    # You would get these values from the actual BlueWallet commitment screen
    btc_address = "1CC3X2gu58d6wXUWMffpuzN9JAfTUWu4Kj"
    qbtc_address = "bqs1234567890abcdefghijklmnopqrstuvwxyz"
    message = f"I confirm that my Bitcoin address is: {btc_address} and my qBTC address is: {qbtc_address}. Timestamp: 2024-01-01T00:00:00Z"
    
    # This signature was created using the private key above
    signature = "IOs7mX1K1v3h8bNQ6HsiJuJmJHwtcLDKW5Zn0StrQCqHG5mAs/Jl5S3u8qOYNPMPVx8V5yR3kAdxYZpRq8n1CTs="
    
    try:
        # Verify the signature
        address_obj = P2PKHBitcoinAddress(btc_address)
        is_valid = VerifyMessage(address_obj, BitcoinMessage(message), signature)
        
        print(f"Bitcoin Address: {btc_address}")
        print(f"qBTC Address: {qbtc_address}")
        print(f"Message: {message[:80]}...")
        print(f"Signature: {signature[:40]}...")
        print(f"\nSignature verification result: {is_valid}")
        
        if is_valid:
            print("✓ This commitment is valid and can be stored in qBTC-core")
        else:
            print("✗ Invalid signature - commitment should be rejected")
            
    except Exception as e:
        print(f"Verification error: {e}")


if __name__ == "__main__":
    if BITCOIN_LIB_AVAILABLE:
        example_sign_and_verify()
        example_verify_from_bluewallet()
    
    print("\n\n=== Integration with qBTC-core ===")
    print("qBTC-core uses python-bitcoinlib to verify signatures from BlueWallet")
    print("The verification happens in utils/bitcoin_utils.py")
    print("BlueWallet signs the commitment message with the Bitcoin private key")
    print("qBTC-core verifies the signature matches the Bitcoin address")