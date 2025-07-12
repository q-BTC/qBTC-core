"""
Bitcoin signature verification utilities using python-bitcoinlib
"""

import logging
from typing import Optional

import bitcoin
from bitcoin.wallet import CBitcoinAddress
from bitcoin.signmessage import BitcoinMessage, VerifyMessage

# Set Bitcoin network parameters (mainnet by default)
bitcoin.SelectParams('mainnet')

logger = logging.getLogger(__name__)


def verify_bitcoin_signature(message: str, signature: str, address: str) -> bool:
    """
    Verify a Bitcoin message signature using python-bitcoinlib.
    
    Args:
        message: The message that was signed
        signature: The base64-encoded signature
        address: The Bitcoin address that signed the message
        
    Returns:
        bool: True if signature is valid, False otherwise
    """
    try:
        # Validate inputs
        if not message or not signature or not address:
            logger.error("Missing required parameters")
            return False
        
        # Create Bitcoin address object
        try:
            btc_address = CBitcoinAddress(address)
        except Exception as e:
            logger.error(f"Invalid Bitcoin address '{address}': {e}")
            return False
        
        # Create Bitcoin message object
        btc_message = BitcoinMessage(message)
        
        # Verify the signature
        try:
            # VerifyMessage returns True if valid, False otherwise
            is_valid = VerifyMessage(btc_address, btc_message, signature)
            
            if is_valid:
                logger.info(f"Signature verified successfully for address: {address}")
            else:
                logger.error(f"Signature verification failed for address: {address}")
                
            return is_valid
            
        except Exception as e:
            logger.error(f"Signature verification error: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Bitcoin signature verification error: {e}")
        return False


def validate_bitcoin_address(address: str) -> bool:
    """
    Validate a Bitcoin address using python-bitcoinlib.
    
    Args:
        address: The Bitcoin address to validate
        
    Returns:
        bool: True if address is valid, False otherwise
    """
    try:
        # This will raise an exception if the address is invalid
        CBitcoinAddress(address)
        return True
    except Exception:
        return False


def get_address_type(address: str) -> Optional[str]:
    """
    Get the type of a Bitcoin address.
    
    Args:
        address: The Bitcoin address
        
    Returns:
        Optional[str]: The address type ('P2PKH', 'P2SH', 'Bech32') or None if invalid
    """
    if not validate_bitcoin_address(address):
        return None
        
    if address.startswith('1'):
        return 'P2PKH'
    elif address.startswith('3'):
        return 'P2SH'
    elif address.startswith('bc1'):
        return 'Bech32'
    elif address.startswith(('m', 'n', '2')):
        return 'Testnet-Legacy'
    elif address.startswith('tb1'):
        return 'Testnet-Bech32'
    else:
        return None