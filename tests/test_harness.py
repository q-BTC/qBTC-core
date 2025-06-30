"""
Unit-tests for harness.py

• b64() round-trip
• build_and_sign_message() content
• main() end-to-end with monkey-patched wallet, signer and requests
"""
from importlib import import_module, reload
import base64
import sys
from decimal import Decimal

import pytest

MODULE_PATH = "harness"                # adjust if your filename differs


# ─────────────────────────── shared fixture ───────────────────────────
@pytest.fixture
def mod(monkeypatch):
    """
    Import harness fresh each test and stub external dependencies so no real
    disk, crypto or network I/O occurs.
    """
    m = import_module(MODULE_PATH)
    reload(m)

    # ---- stub wallet layer --------------------------------------------
    def fake_get_wallet(fname="wallet.json", password=None):
        return {
            "address":    "bqs1sender",
            "privateKey": "deadbeef",
            "publicKey":  "cafebabe",
        }

    def fake_sign_transaction(msg: str, priv_hex: str):
        assert priv_hex == "deadbeef"
        # Return *hex* so harness bytes.fromhex() works
        return msg.encode().hex()

    monkeypatch.setattr(m, "get_or_create_wallet", fake_get_wallet)
    monkeypatch.setattr(m, "sign_transaction",      fake_sign_transaction)

    # ---- stub requests.post -------------------------------------------
    sent = {}
    class _Resp:
        status_code = 200
        @staticmethod
        def json(): return {"ok": True}

    def fake_post(url, *, json=None, timeout=10):
        sent.update(url=url, json=json, timeout=timeout)
        return _Resp()

    monkeypatch.setattr(m, "requests",
                        type("RqStub", (), {"post": staticmethod(fake_post)}))
    m.__dict__["_sent"] = sent   # expose for assertions

    # deterministic timestamp
    monkeypatch.setattr(m.time, "time", lambda: 1_700_000_000.000)

    return m


# ───────────────────────────── helper tests ───────────────────────────
@pytest.mark.parametrize("raw", [b"hello", b"", b"123"])
def test_b64_roundtrip(mod, raw):
    assert base64.b64decode(mod.b64(raw)) == raw


def test_build_and_sign_message(mod):
    # Use the exact private key expected by the stub
    test_privkey = "deadbeef"
    msg, sig = mod.build_and_sign_message(
        "bqs1sender", "bqs1dest", Decimal("42.5"), "123", "1", test_privkey
    )
    assert msg == "bqs1sender:bqs1dest:42.5:123:1"
    # With the stub, signature should be hex-encoded message
    assert sig == msg.encode().hex()


# ───────────────────────── main() end-to-end ──────────────────────────
@pytest.mark.asyncio
async def test_main_sends_correct_payload(mod):
    argv_backup = sys.argv[:]
    sys.argv = [
        "harness.py",
        "--receiver", "bqs1dest",
        "--amount",   "10",
        "--password", "pwd",
    ]
    try:
        mod.main()          # should finish without error
    finally:
        sys.argv[:] = argv_backup

    payload = mod._sent["json"]
    assert mod._sent["url"].endswith("/worker")
    assert payload["request_type"] == "broadcast_tx"

    # ---- decode and validate message & signature ----------------------
    decoded_msg = base64.b64decode(payload["message"]).decode()
    decoded_sig = base64.b64decode(payload["signature"]).decode()

    expected_amount = Decimal("10").normalize()      # '1E+1'
    expected_nonce  = "1700000000000"                # fixed by monkey-patch
    expected_chain_id = "1"                          # default chain ID
    expected_msg    = f"bqs1sender:bqs1dest:{expected_amount}:{expected_nonce}:{expected_chain_id}"

    assert decoded_msg == expected_msg
    assert decoded_sig == expected_msg              # signer echoes msg in hex
    assert base64.b64decode(payload["pubkey"]).hex() == "cafebabe"
