"""
Pydantic models for input validation
"""

from pydantic import BaseModel, validator, Field
from typing import Optional, List, Union
from decimal import Decimal
import re
import base64

class TransactionRequest(BaseModel):
    message: str = Field(..., description="Base64 encoded transaction message")
    signature: str = Field(..., description="Base64 encoded signature")
    pubkey: str = Field(..., description="Base64 encoded public key")
    
    @validator('message', 'signature', 'pubkey')
    def validate_base64(cls, v):
        try:
            base64.b64decode(v)
            return v
        except Exception:
            raise ValueError('Must be valid base64 encoded string')
    
    @validator('message')
    def validate_message_format(cls, v):
        try:
            decoded = base64.b64decode(v).decode('utf-8')
            parts = decoded.split(':')
            if len(parts) < 3:
                raise ValueError('Message must have format: sender:receiver:amount')
            
            sender, receiver, amount_str = parts[0], parts[1], parts[2]
            
            # Validate addresses
            if not (sender.startswith('bqs') and len(sender) >= 20):
                raise ValueError('Invalid sender address format')
            if not (receiver.startswith('bqs') and len(receiver) >= 20):
                raise ValueError('Invalid receiver address format')
            
            # Validate amount
            try:
                amount = Decimal(amount_str)
                if amount <= 0:
                    raise ValueError('Amount must be positive')
                if amount > Decimal('21000000'):  # Max supply
                    raise ValueError('Amount exceeds maximum supply')
            except (ValueError, TypeError):
                raise ValueError('Invalid amount format')
            
            return v
        except Exception as e:
            raise ValueError(f'Invalid message format: {str(e)}')

class WalletAddressRequest(BaseModel):
    wallet_address: str = Field(..., min_length=20, max_length=100)
    
    @validator('wallet_address')
    def validate_address_format(cls, v):
        if not v.startswith('bqs'):
            raise ValueError('Address must start with "bqs"')
        if not re.match(r'^bqs[A-Za-z0-9]+$', v):
            raise ValueError('Address contains invalid characters')
        return v

class TransactionHistoryRequest(WalletAddressRequest):
    limit: int = Field(50, ge=1, le=1000, description="Number of transactions to return")

class RPCRequest(BaseModel):
    method: str = Field(..., min_length=1, max_length=50)
    params: Optional[List] = Field(default_factory=list)
    id: Optional[Union[str, int]] = Field(None)
    
    @validator('method')
    def validate_method(cls, v):
        allowed_methods = [
            'getblocktemplate', 'submitblock', 'getblock', 'getbalance',
            'getblockchaininfo', 'getmininginfo', 'getnetworkinfo', 'getpeerinfo',
            'getwork', 'createwallet', 'listwallets', 'getwalletinfo',
            'listunspent', 'walletpassphrase', 'walletlock', 'walletpassphrasechange',
            'encryptwallet'
        ]
        if v not in allowed_methods:
            raise ValueError(f'Method must be one of: {", ".join(allowed_methods)}')
        return v

class BlockSubmissionRequest(BaseModel):
    block_hex: str = Field(..., min_length=160)  # Minimum block header size
    
    @validator('block_hex')
    def validate_hex_format(cls, v):
        try:
            bytes.fromhex(v)
            return v
        except ValueError:
            raise ValueError('Block data must be valid hexadecimal')

class WebSocketSubscription(BaseModel):
    update_type: str = Field(..., description="Type of updates to subscribe to")
    wallet_address: Optional[str] = Field(None, description="Wallet address for targeted updates")
    network: Optional[str] = Field('testnet', description="Network type (mainnet/testnet)")
    
    @validator('update_type')
    def validate_update_type(cls, v):
        allowed_types = ['all_transactions', 'combined_update', 'l1_proofs_testnet', 'bridge']
        if v not in allowed_types:
            raise ValueError(f'Update type must be one of: {", ".join(allowed_types)}')
        return v
    
    @validator('wallet_address')
    def validate_wallet_address(cls, v):
        if v is not None:
            if not v.startswith('bqs') or len(v) < 20:
                raise ValueError('Invalid wallet address format')
        return v
    
    @validator('network')
    def validate_network(cls, v):
        if v is not None:
            allowed_networks = ['mainnet', 'testnet']
            if v not in allowed_networks:
                raise ValueError(f'Network must be one of: {", ".join(allowed_networks)}')
        return v

class CommitRequest(BaseModel):
    btcAddress: str = Field(..., description="Bitcoin address")
    qbtcAddress: str = Field(..., description="qBTC address")
    message: str = Field(..., description="Signed message with timestamp")
    signature: str = Field(..., description="Bitcoin signature")
    
    @validator('btcAddress')
    def validate_btc_address(cls, v):
        # Basic Bitcoin address validation (P2PKH, P2SH, Bech32)
        if not re.match(r'^(1|3|bc1)[a-zA-HJ-NP-Z0-9]{25,62}$', v):
            raise ValueError('Invalid Bitcoin address format')
        return v
    
    @validator('qbtcAddress')
    def validate_qbtc_address(cls, v):
        if not v.startswith('bqs') or len(v) < 20:
            raise ValueError('Invalid qBTC address format')
        if not re.match(r'^bqs[A-Za-z0-9]+$', v):
            raise ValueError('qBTC address contains invalid characters')
        return v
    
    @validator('message')
    def validate_message_format(cls, v):
        # Check if message contains expected format
        if 'Bitcoin address is:' not in v or 'qBTC address is:' not in v or 'Timestamp:' not in v:
            raise ValueError('Message must contain Bitcoin address, qBTC address, and timestamp')
        return v
    
    @validator('signature')
    def validate_signature_format(cls, v):
        # Bitcoin signatures are base64 encoded and typically 88-90 chars
        if len(v) < 80 or len(v) > 100:
            raise ValueError('Invalid signature length')
        try:
            base64.b64decode(v)
            return v
        except Exception:
            raise ValueError('Signature must be valid base64')