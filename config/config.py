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
# Bootstrap nodes for DHT discovery — first entry is primary, rest are fallbacks
# Override via env: comma-separated "host:port" pairs
_bootstrap_env = os.environ.get("BOOTSTRAP_NODES", "")
if _bootstrap_env:
    BOOTSTRAP_NODES = [
        (h.strip(), int(p.strip()))
        for h, p in (entry.split(":") for entry in _bootstrap_env.split(",") if ":" in entry)
    ]
else:
    BOOTSTRAP_NODES = [("api.bitcoinqs.org", 8001)]
VALIDATORS_LIST_KEY = "validators_list"

# CRITICAL: Known validators that should NEVER be removed
# Bootstrap node is always a known validator; additional ones can be added via env
KNOWN_VALIDATORS = {
    "api.bitcoinqs.org": {"port": 8002, "name": "bootstrap-primary"},
}
# Allow adding extra known validators via env: comma-separated "ip:port:name" entries
_extra_validators = os.environ.get("EXTRA_KNOWN_VALIDATORS", "")
if _extra_validators:
    for entry in _extra_validators.split(","):
        parts = entry.strip().split(":")
        if len(parts) >= 2:
            _ip, _port = parts[0], int(parts[1])
            _name = parts[2] if len(parts) > 2 else "extra"
            KNOWN_VALIDATORS[_ip] = {"port": _port, "name": _name}
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
# Maximum reorg depth - reject reorganizations deeper than this (matches Bitcoin's practical limit)
MAX_REORG_DEPTH = 100

# Gossip network hardening constants
MAX_INBOUND_CONNECTIONS = int(os.getenv("MAX_INBOUND_CONNECTIONS", "117"))
MAX_OUTBOUND_CONNECTIONS = int(os.getenv("MAX_OUTBOUND_CONNECTIONS", "10"))
GOSSIP_RATE_LIMIT_WINDOW = int(os.getenv("GOSSIP_RATE_LIMIT_WINDOW", "60"))  # seconds
GOSSIP_RATE_LIMIT_TX = int(os.getenv("GOSSIP_RATE_LIMIT_TX", "200"))  # per peer per window
GOSSIP_RATE_LIMIT_BLOCK = int(os.getenv("GOSSIP_RATE_LIMIT_BLOCK", "30"))
GOSSIP_RATE_LIMIT_QUERY = int(os.getenv("GOSSIP_RATE_LIMIT_QUERY", "60"))
GOSSIP_RATE_LIMIT_DEFAULT = int(os.getenv("GOSSIP_RATE_LIMIT_DEFAULT", "120"))
MAX_PEERS_PER_SUBNET = int(os.getenv("MAX_PEERS_PER_SUBNET", "4"))  # /16 netgroup limit
NUM_ANCHOR_PEERS = int(os.getenv("NUM_ANCHOR_PEERS", "2"))

# Security hardening constants
MAX_WS_CONNECTIONS = int(os.getenv("MAX_WS_CONNECTIONS", "100"))
MAX_WS_PER_IP = int(os.getenv("MAX_WS_PER_IP", "5"))
TRUSTED_PROXIES = set(
    p.strip() for p in os.getenv("TRUSTED_PROXIES", "127.0.0.1,172.17.0.1,192.168.65.1").split(",") if p.strip()
)
RPC_ALLOW_REMOTE = os.getenv("RPC_ALLOW_REMOTE", "false").lower() == "true"
BANNED_PEER_DURATION = int(os.getenv("BANNED_PEER_DURATION", "300"))  # 5 minutes
MAX_VALIDATORS_PER_IP = int(os.getenv("MAX_VALIDATORS_PER_IP", "3"))
MIN_PASSWORD_LENGTH = int(os.getenv("MIN_PASSWORD_LENGTH", "12"))
MAX_CONNECTIONS_PER_IP = int(os.getenv("MAX_CONNECTIONS_PER_IP", "2"))
