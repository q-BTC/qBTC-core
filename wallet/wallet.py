import os
import sys
import json
import getpass
import logging
import base64
import hashlib
from typing import Optional
import oqs    
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base58        

WALLET_FILENAME   = "wallet.json"
_PQ_ALG           = "ML-DSA-87"
_PBKDF2_ROUNDS    = 100_000
_AES_KEYLEN       = 32
_SALT_LEN         = 16
_IV_LEN           = 12
_CHECKSUM_LEN     = 4


def _as_bytes(buf) -> bytes:
    """Convert any liboqs buffer to real bytes."""
    if isinstance(buf, (bytes, bytearray)):
        return bytes(buf)
    if isinstance(buf, memoryview):
        return buf.tobytes()
    if hasattr(buf, "tobytes"):
        return buf.tobytes()
    return bytes(buf)

def _hex(b: bytes) -> str: return b.hex()

def _derive_address(pub: bytes) -> str:
    h = hashlib.sha3_256(pub).digest()
    versioned = bytes([0x00]) + h[:20]
    chk = hashlib.sha3_256(versioned).digest()[:_CHECKSUM_LEN]
    return "bqs" + base58.b58encode(versioned + chk).decode()

def _pbkdf2_key(password: str, salt: bytes) -> bytes:
    return PBKDF2HMAC(
        algorithm=hashes.SHA256(), length=_AES_KEYLEN,
        salt=salt, iterations=_PBKDF2_ROUNDS
    ).derive(password.encode())

def _encrypt_privkey(priv_hex: str, password: str):
    salt, iv = os.urandom(_SALT_LEN), os.urandom(_IV_LEN)
    ct_tag = AESGCM(_pbkdf2_key(password, salt)).encrypt(iv, priv_hex.encode(), None)
    return (
        base64.b64encode(ct_tag).decode(),
        base64.b64encode(salt).decode(),
        base64.b64encode(iv).decode(),
    )

def _decrypt_privkey(enc_b64, password, salt_b64, iv_b64) -> str:
    ct_tag, salt, iv = map(base64.b64decode, (enc_b64, salt_b64, iv_b64))
    return AESGCM(_pbkdf2_key(password, salt)).decrypt(iv, ct_tag, None).decode()


def load_wallet_file(fname=WALLET_FILENAME) -> Optional[dict]:
    return json.load(open(fname)) if os.path.exists(fname) else None

def save_wallet_file(wallet: dict, fname=WALLET_FILENAME):
    json.dump(wallet, open(fname, "w"), indent=2)


def generate_wallet(password: str) -> dict:
    """Generate ML-DSA-87 key-pair, derive address, encrypt private key."""
    try:
        with oqs.Signature(_PQ_ALG) as signer:
            public_key = _as_bytes(signer.generate_keypair())      # bytes
            secret_key = _as_bytes(signer.export_secret_key())     # bytes

        enc_priv, salt, iv = _encrypt_privkey(_hex(secret_key), password)

        return {
            "address":              _derive_address(public_key),
            "encryptedPrivateKey":  enc_priv,
            "PrivateKeySalt":       salt,
            "PrivateKeyIV":         iv,
            "publicKey":            _hex(public_key),
        }
    except Exception as e:
        logging.error(f"Error generating wallet: {e}")
        sys.exit(1)

def unlock_wallet(wallet: dict, password: str) -> dict:
    """Decrypt private key and return plaintext key + pubkey + address."""
    try:
        priv_hex = _decrypt_privkey(
            wallet["encryptedPrivateKey"], password,
            wallet["PrivateKeySalt"], wallet["PrivateKeyIV"])
        return {
            "privateKey": priv_hex,
            "publicKey":  wallet["publicKey"],
            "address":    wallet["address"],
        }
    except Exception as e:
        logging.error(f"Error unlocking wallet: {e}")
        sys.exit(1)

def sign_transaction(message: str, priv_hex: str) -> str:
    """Sign a UTF-8 message string; return hex signature."""
    try:
        with oqs.Signature(_PQ_ALG, secret_key=bytes.fromhex(priv_hex)) as signer:
            signature = _as_bytes(signer.sign(message.encode()))
        return _hex(signature)
    except Exception as e:
        logging.error(f"Failed to sign: {e}")
        raise RuntimeError("Failed to sign transaction") from e

def verify_transaction(message: str, sig_hex: str, pub_hex: str) -> bool:
    """Verify signature with public key (old-order verify)."""
    try:
        with oqs.Signature(_PQ_ALG) as verifier:
            sig_verify = verifier.verify(
                message.encode(),
                bytes.fromhex(sig_hex),
                bytes.fromhex(pub_hex)
            )
            #print(f"*** VERIFING MESSAGE: {message} Result: {sig_verify}")
            return sig_verify

    except Exception as e:
        logging.error(f"Failed to verify: {e}")


def get_or_create_wallet(fname=WALLET_FILENAME, password: str | None = None):
    wallet = load_wallet_file(fname)
    if wallet:
        if password is None:
            password = getpass.getpass(f"Enter password to unlock {fname}: ")
        return unlock_wallet(wallet, password)

    if password is None:
        password = getpass.getpass("Enter a new password: ")
        if password != getpass.getpass("Confirm password: "):
            logging.error("Passwords do not match"); sys.exit(1)

    wallet_json = generate_wallet(password)
    save_wallet_file(wallet_json, fname)
    logging.info(f"Wallet generated → {fname}")
    return unlock_wallet(wallet_json, password)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    w = get_or_create_wallet()
    print("Address :", w["address"])

    msg = "hello world"
    sig = sign_transaction(msg, w["privateKey"])
    print("Signature:", sig)
    print("Verified :", verify_transaction(msg, sig, w["publicKey"]))
