import json
import struct
import hashlib
from typing import Union
import base58

def derive_qsafe_address(pubkey: Union[bytes, str]) -> str:
    # Convert pubkey to bytes if it's a string
    if isinstance(pubkey, str):
        pubkey = bytes.fromhex(pubkey)  # Assuming pubkey is hex-encoded
    elif not isinstance(pubkey, bytes):
        raise ValueError("pubkey must be bytes or hex string")

    # Step 1: SHA3-256 hash of the public key
    sha3_hash = hashlib.sha3_256(pubkey).digest()

    # Step 2: Version prefix (0x00) + first 20 bytes of hash
    versioned_hash = b'\x00' + sha3_hash[:20]

    # Step 3: Checksum: first 4 bytes of SHA3-256(versioned_hash)
    checksum = hashlib.sha3_256(versioned_hash).digest()[:4]

    # Step 4: Concatenate versioned_hash + checksum
    address_bytes = versioned_hash + checksum

    # Step 5: Base58 encode and prefix with "bqs"
    return "bqs" + base58.b58encode(address_bytes).decode()


def sha256d(b: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()

def calculate_merkle_root(txids: list[str]) -> str:
    if not txids:
        return sha256d(b"").hex()
    
    # Special case: single transaction, merkle root is just the txid
    if len(txids) == 1:
        return txids[0]
    
    hashes = [bytes.fromhex(txid)[::-1] for txid in txids]  # little-endian

    while len(hashes) > 1:
        if len(hashes) % 2 == 1:
            hashes.append(hashes[-1])  # duplicate last if odd

        new_hashes = []
        for i in range(0, len(hashes), 2):
            combined = hashes[i] + hashes[i + 1]
            new_hashes.append(sha256d(combined))
        hashes = new_hashes

    return hashes[0][::-1].hex()  # return as hex, big-endian

def bits_to_target(bits: int) -> int:
    exponent = bits >> 24
    coefficient = bits & 0xffffff
    if exponent < 3:
        return coefficient >> (8 * (3 - exponent))
    if exponent > 32:
        raise ValueError(f"Invalid compact bits: exponent {exponent} exceeds maximum (32)")
    return coefficient * (1 << (8 * (exponent - 3)))

def validate_pow(block: "Block") -> bool:
    target = bits_to_target(block.bits)
    return int(block.hash(), 16) < target

class Block:
    def __init__(self, version, prev_block_hash, merkle_root, timestamp, bits, nonce):
        self.version = version
        self.prev_block_hash = prev_block_hash
        self.merkle_root = merkle_root
        self.timestamp = timestamp
        self.bits = bits
        self.nonce = nonce

    def header(self):
        return (
            struct.pack("<L", self.version) +
            bytes.fromhex(self.prev_block_hash)[::-1] +
            bytes.fromhex(self.merkle_root)[::-1] +
            struct.pack("<L", self.timestamp) +
            struct.pack("<L", self.bits) +
            struct.pack("<L", self.nonce)
        )

    def hash(self):
        return sha256d(self.header())[::-1].hex()







def serialize_transaction(tx: dict) -> str:
    # Create a clean copy without txid field for consistent serialization
    import copy
    tx_clean = copy.deepcopy(tx)
    if "txid" in tx_clean:
        del tx_clean["txid"]
    # Also remove txid from outputs if present
    for output in tx_clean.get("outputs", []):
        output.pop("txid", None)
    
    tx_data = json.dumps(tx_clean, sort_keys=True)
    return tx_data.encode().hex()

def deserialize_transaction(raw_tx: str) -> dict:
    try:
        tx_bytes = bytes.fromhex(raw_tx)
        tx_json = tx_bytes.decode()
        tx_dict = json.loads(tx_json)
        return tx_dict
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"Failed to deserialize transaction: {e}")



def read_varint(raw: bytes, offset: int) -> (int, int):
    i = raw[offset]
    if i < 0xfd:
        return i, 1
    elif i == 0xfd:
        return struct.unpack_from('<H', raw, offset + 1)[0], 3
    elif i == 0xfe:
        return struct.unpack_from('<I', raw, offset + 1)[0], 5
    else:
        return struct.unpack_from('<Q', raw, offset + 1)[0], 9


def parse_tx(raw: bytes, offset: int) -> (dict, int):
    start = offset
    tx = {}
    tx['version'] = struct.unpack_from('<I', raw, offset)[0]
    offset += 4
    vin_cnt, sz = read_varint(raw, offset)
    offset += sz
    inputs = []
    for _ in range(vin_cnt):
        prev_txid = raw[offset:offset+32][::-1].hex()
        offset += 32
        prev_index = struct.unpack_from('<I', raw, offset)[0]
        offset += 4
        script_len, sz = read_varint(raw, offset)
        offset += sz
        script_sig = raw[offset:offset+script_len].hex()
        offset += script_len
        sequence = struct.unpack_from('<I', raw, offset)[0]
        offset += 4
        inputs.append({
            'prev_txid': prev_txid,
            'prev_index': prev_index,
            'script_sig': script_sig,
            'sequence': sequence
        })
    tx['inputs'] = inputs
    vout_cnt, sz = read_varint(raw, offset)
    offset += sz
    outputs = []
    for _ in range(vout_cnt):
        value = struct.unpack_from('<Q', raw, offset)[0]
        offset += 8
        pk_len, sz = read_varint(raw, offset)
        offset += sz
        script_pubkey = raw[offset:offset+pk_len].hex()
        offset += pk_len
        outputs.append({
            'value': value,
            'script_pubkey': script_pubkey
        })
    tx['outputs'] = outputs
    tx['locktime'] = struct.unpack_from('<I', raw, offset)[0]
    offset += 4
    total_size = offset - start
    return tx, total_size

def scriptpubkey_to_address(script_pubkey_hex):
    # Support P2PKH format (76a914...88ac)
    if script_pubkey_hex.startswith("76a914") and script_pubkey_hex.endswith("88ac"):
        pubkey_hash = script_pubkey_hex[6:-4]  # extract 20-byte pubkey hash
        versioned = b'\x00' + bytes.fromhex(pubkey_hash)
        checksum = hashlib.sha256(hashlib.sha256(versioned).digest()).digest()[:4]
        address_bytes = versioned + checksum
        return base58.b58encode(address_bytes).decode()

    # Support P2SH format (a914...87)
    elif script_pubkey_hex.startswith("a914") and script_pubkey_hex.endswith("87"):
        script_hash = script_pubkey_hex[4:-2]  # extract 20-byte script hash
        versioned = b'\x05' + bytes.fromhex(script_hash)  # 0x05 prefix for P2SH
        checksum = hashlib.sha256(hashlib.sha256(versioned).digest()).digest()[:4]
        address_bytes = versioned + checksum
        return base58.b58encode(address_bytes).decode()

    else:
        raise ValueError(f"Unsupported scriptPubKey format: {script_pubkey_hex[:10]}...")
