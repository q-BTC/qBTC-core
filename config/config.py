import asyncio
import uuid
import os

VALIDATOR_ID = str(uuid.uuid4())[:8]
ROCKSDB_PATH = os.environ.get("ROCKSDB_PATH", "/app/db")  
DEFAULT_GOSSIP_PORT = 7002
DHT_PORT = 8001
HEARTBEAT_INTERVAL = 30
VALIDATOR_TIMEOUT = 90  # 90 seconds = 3 heartbeat intervals (was 270 seconds)
MAX_HEARTBEAT_FAILURES = 3  # Max consecutive failures before removal (was hardcoded as 10)
BOOTSTRAP_NODES = [("api.bitcoinqs.org", 8001)]
VALIDATORS_LIST_KEY = "validators_list"

# CRITICAL: Known validators that should NEVER be removed
KNOWN_VALIDATORS = {
}
shutdown_event = asyncio.Event()
MAX_CHECKPOINTS = 1000
MAX_TX_HISTORY = 10000
FEE_PERCENTAGE = float(os.environ.get("FEE_PERCENTAGE", "0.001"))
# Consensus parameters - Bitcoin-proportional for security
# 120,960 blocks = 2 weeks at 10-second blocks (same as Bitcoin's 2016 blocks at 10-minute blocks)
DIFFICULTY_ADJUSTMENT_INTERVAL = int(os.environ.get("DIFFICULTY_ADJUSTMENT_INTERVAL", "120960"))
BLOCK_TIME_TARGET = int(os.environ.get("BLOCK_TIME_TARGET", "10"))

# Halving parameters - Bitcoin-proportional emission schedule
# 12,600,000 blocks = 4 years at 10-second blocks (same as Bitcoin's 210,000 blocks at 10-minute blocks)
HALVING_INTERVAL = int(os.environ.get("HALVING_INTERVAL", "12600000"))
# Initial reward scaled for 50% mining supply: 0.4167 qBTC per block
# This maintains Bitcoin's emission rate (50 BTC/10min = 0.0833 BTC/sec) scaled to 50%
INITIAL_BLOCK_REWARD = float(os.environ.get("INITIAL_BLOCK_REWARD", "0.4167"))

# Supply parameters
TOTAL_SUPPLY = 21_000_000  # Total qBTC supply (same as Bitcoin)
MINING_SUPPLY = 10_500_000  # 50% allocated to mining
PRESALE_TREASURY_SUPPLY = 10_500_000  # 50% for presale/treasury/ecosystem
GENESIS_ADDRESS = "bqs1genesis00000000000000000000000000000000"
ADMIN_ADDRESS = os.getenv("ADMIN_ADDRESS", "bqs1HpmbeSd8nhRpq5zX5df91D3Xy8pSUovmV")
# Chain ID for replay protection (default: 1 for mainnet, can be overridden for testnets)
CHAIN_ID = int(os.getenv("CHAIN_ID", "1"))
# Transaction expiration time in seconds (default: 1 hour)
TX_EXPIRATION_TIME = int(os.getenv("TX_EXPIRATION_TIME", "3600"))
# Maximum number of blocks to request from a peer at once
MAX_BLOCKS_PER_SYNC_REQUEST = int(os.getenv("MAX_BLOCKS_PER_SYNC_REQUEST", "500"))
# Maximum number of transactions allowed per block (excluding coinbase)
MAX_TRANSACTIONS_PER_BLOCK = int(os.getenv("MAX_TRANSACTIONS_PER_BLOCK", "4000"))
# Mempool limits
MAX_MEMPOOL_TRANSACTIONS = int(os.getenv("MAX_MEMPOOL_TRANSACTIONS", "10000"))  # Max number of transactions
MAX_MEMPOOL_SIZE_MB = int(os.getenv("MAX_MEMPOOL_SIZE_MB", "300"))  # Max mempool size in MB
MIN_RELAY_FEE = float(os.getenv("MIN_RELAY_FEE", "0.00001"))  # Minimum fee to relay transaction
