import asyncio
import uuid
import os

VALIDATOR_ID = str(uuid.uuid4())[:8]
ROCKSDB_PATH = os.environ.get("ROCKSDB_PATH", "ledger.rocksdb")  
DEFAULT_GOSSIP_PORT = 7002
DHT_PORT = 8001
HEARTBEAT_INTERVAL = 30
VALIDATOR_TIMEOUT = 90 * 3
BOOTSTRAP_NODES = [("api.bitcoinqs.org", 8001)]
VALIDATORS_LIST_KEY = "validators_list"

# CRITICAL: Known validators that should NEVER be removed
KNOWN_VALIDATORS = {
    "18.156.69.79": {"port": 8002, "name": "v1.qb.tc"},
    "3.120.176.166": {"port": 8002, "name": "v2.qb.tc"}
}
shutdown_event = asyncio.Event()
MAX_CHECKPOINTS = 1000
MAX_TX_HISTORY = 10000
FEE_PERCENTAGE = float(os.environ.get("FEE_PERCENTAGE", "0.001"))
DIFFICULTY_ADJUSTMENT_INTERVAL = int(os.environ.get("DIFFICULTY_ADJUSTMENT_INTERVAL", "2000"))
BLOCK_TIME_TARGET = int(os.environ.get("BLOCK_TIME_TARGET", "10"))
GENESIS_ADDRESS = "bqs1genesis00000000000000000000000000000000"
ADMIN_ADDRESS = os.getenv("ADMIN_ADDRESS", "bqs1HpmbeSd8nhRpq5zX5df91D3Xy8pSUovmV")
# Chain ID for replay protection (default: 1 for mainnet, can be overridden for testnets)
CHAIN_ID = int(os.getenv("CHAIN_ID", "1"))
# Transaction expiration time in seconds (default: 1 hour)
TX_EXPIRATION_TIME = int(os.getenv("TX_EXPIRATION_TIME", "3600"))
