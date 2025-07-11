# qBTC-core DEV NET

**qBTC-core** is a modern blockchain implementation inspired by Satoshi Nakamoto's original design for Bitcoin. It stays true to foundational concepts such as:

- **Proof-of-Work (PoW)**
- **UTXO-based accounting**
- **Bitcoin-style block headers**
- **Mining compatibility**
- **Standard RPC methods** like `getblocktemplate` and `submitblock`

Built from the ground up in Python to demonstrate a proof-of-concept, qBTC introduces key innovations for the future of Bitcoin:

- **Post-Quantum Security** using the ML-DSA signature scheme  
- **Decentralized validator discovery** via a Kademlia DHT  
- **Fast, scalable propagation** through an asynchronous gossip network  

The cryptographic layer is modular, allowing ML-DSA to be replaced with other post-quantum algorithms as standards evolve.

## 🌐 Key Features

- 🛡 **Post-Quantum Cryptography** (ML-DSA-87 signatures)
- 🔁 **UTXO-Based Ledger** with Merkle root verification
- 🌱 **Validator Discovery** via Kademlia DHT
- 📡 **Gossip Protocol** for fast block and transaction propagation
- 🧠 **Protobuf-encoded Transactions and Blocks**
- 📊 **Prometheus Metrics & Grafana Dashboards**
- 🔄 **Automatic Genesis Block Creation** (21M coins)
- 🌐 **Full P2P Networking** with NAT traversal support
- 🚀 Built with **Python**, **FastAPI**, and **asyncio**

---

## 📦 Architecture Overview

```text
+-------------------+       +-----------------------+
|   Kademlia DHT    |<----->| Validator Peer Nodes  |
+-------------------+       +-----------------------+
         |                               |
         v                               v
+-------------------+         +----------------------+
| Gossip Network    | <-----> |  GossipNode Class    |
| (async TCP/JSON)  |         +----------------------+
         |                               |
         v                               v
+-------------------+         +----------------------+
| Blockchain Logic  | <-----> | Protobuf Structures  |
| - Merkle Root     |         | - Blocks, Txns       |
| - UTXO State      |         +----------------------+
| - Chain Manager   |
+-------------------+
         |
         v
+----------------------+       +----------------------+
| Local DB (RocksDB)   | <---> | Event Bus System     |
+----------------------+       +----------------------+
         |
         v
+----------------------+       +----------------------+
| Web API (FastAPI)    | <---> | RPC Server (Mining)  |
| - /debug endpoints   |       | - getblocktemplate   |
| - /worker (broadcast)|       | - submitblock        |
| - /health (metrics)  |       +----------------------+
+----------------------+
```

---

## 🛠 Getting Started

### 1. Clone the Repository & Install Dependencies

```bash
git clone https://github.com/q-btc/qBTC-core.git
cd qBTC-core
pip install -r requirements.txt
```

Follow the instructions here to install liboqs-python:
https://github.com/open-quantum-safe/liboqs-python

### 2. Generate a Wallet

Before starting a node, you must generate a wallet file:

```bash
python3 wallet/wallet.py
```

This will create a `wallet.json` file containing your ML-DSA public/private keypair encrypted with a passphrase.

Keep it safe — this is your validator's identity and signing authority.

### 3. Start a Node via CLI

You can start qBTC-core either as a **bootstrap server** or connect to an existing bootstrap peer. Networking (DHT and Gossip) is always enabled and required for node operation.

#### CLI Usage

```bash
usage: main.py [-h] [--bootstrap] [--bootstrap_server BOOTSTRAP_SERVER]
               [--bootstrap_port BOOTSTRAP_PORT] [--dht-port DHT_PORT]
               [--gossip-port GOSSIP_PORT] [--external-ip EXTERNAL_IP]
```

Optional arguments:
- `--bootstrap`: Run as bootstrap server
- `--bootstrap_server`: Bootstrap server host (default: api.bitcoinqs.org)
- `--bootstrap_port`: Bootstrap server port (default: 8001)
- `--dht-port`: DHT port (default: 8001)
- `--gossip-port`: Gossip port (default: 8002)
- `--external-ip`: External IP address for NAT traversal

#### a) Start as a Bootstrap Server

```bash
python3 main.py --bootstrap
```

This initializes a bootstrap node that other nodes can connect to.

#### b) Connect to Default Bootstrap Server (api.bitcoinqs.org)

```bash
python3 main.py
```

This connects to the default bootstrap server at api.bitcoinqs.org:8001.

#### c) Connect to Custom Bootstrap Server

```bash
python3 main.py --bootstrap_server 192.168.1.10 --bootstrap_port 9001
```

Replace `192.168.1.10` and `9001` with your custom bootstrap server details.

#### d) Use Custom Ports

```bash
python3 main.py --dht-port 8009 --gossip-port 8010
```

This starts a node with custom DHT and gossip ports while connecting to the default bootstrap server.

---

## 🐳 Docker Usage

The project includes several Docker Compose configurations for different deployment scenarios:

### Development/Testing Environment

```bash
# Start a test network with 1 bootstrap node and 2 validators
docker compose up -d

# View logs
docker compose logs -f

# Stop the network
docker compose down

# Stop and remove all data
docker compose down --volumes
```

### Key Docker Features:
- **Automatic wallet generation** with secure passwords
- **Redis** for caching and rate limiting
- **Prometheus** metrics collection (http://localhost:9090)
- **Grafana** dashboards for monitoring (http://localhost:3000)
- **Persistent storage** using Docker volumes
- **Automatic peer discovery** between containers

### Production Bootstrap Server

```bash
# Start a production bootstrap server
docker compose -f docker-compose.bootstrap.yml up -d
```

### Production Validator

```bash
# Start a production validator node
docker compose -f docker-compose.validator.yml up -d
```

---

## 🧪 Testing Multi-Node

You can simulate multiple validators by launching separate containers or Python processes with unique ports and wallet keys.

### Docker Multi-Node Network

The default `docker-compose.yml` creates:
- 1 Bootstrap node (port 8080)
- 2 Validator nodes (ports 8081, 8082)
- Prometheus monitoring (port 9090)
- Grafana dashboards (port 3000)
- Redis cache

All nodes automatically discover each other and maintain peer connections.

---

## 📜 Core Components

| Component            | Description                                      |
|---------------------|--------------------------------------------------|
| `main.py`           | Entry point - starts web/RPC servers             |
| `blockchain/`       | Block, transaction, UTXO, Merkle logic           |
| `chain_manager.py`  | Manages blockchain state and fork resolution     |
| `dht/`              | Kademlia-based peer discovery                    |
| `gossip/`           | Gossip protocol for block/tx propagation         |
| `web/`              | FastAPI web server with API endpoints            |
| `rpc/`              | Bitcoin-compatible RPC for mining                |
| `wallet/`           | Post-quantum key management (ML-DSA)             |
| `database/`         | RocksDB storage layer                            |
| `monitoring/`       | Health checks and Prometheus metrics             |
| `events/`           | Event bus for internal communication             |
| `security/`         | Rate limiting and DDoS protection                |

---

## ⛏️ Submitting & Mining Transactions

### Submitting a Transaction to the Mempool

You can broadcast a signed transaction using the test harness:

```bash
python3 broadcast_tx_test_harness.py \
  --node http://localhost:8080 \
  --receiver bqs1Bo4quBsE6f5aitv42X5n1S9kASsphn9At \
  --amount 500 \
  --wallet ~/Desktop/ledger.json
```

This sends 500 qBTC to the specified address using your signed wallet. The transaction includes:
- **Chain ID** for replay protection
- **Timestamp** for transaction expiration
- **ML-DSA signature** for post-quantum security

### Mining Transactions in the Mempool

To mine blocks (including mempool transactions), use `cpuminer-opt` connected to any node's RPC endpoint:

```bash
docker run --rm -it cpuminer-opt \
  -a sha256d \
  -o http://localhost:8332 \
  -u someuser -p x \
  --coinbase-addr=bqs1YourAddressHere
```

The RPC server automatically:
- Includes pending transactions from the mempool
- Creates proper coinbase transactions with fees
- Broadcasts mined blocks to all peers via gossip

---

## 📊 Monitoring & Debugging

### Health & Metrics Endpoints

- **Health Check**: `http://localhost:8080/health` - Prometheus metrics
- **Network Status**: `http://localhost:8080/debug/network` - Peer connections
- **Peer Details**: `http://localhost:8080/debug/peers` - Detailed peer info
- **Mempool**: `http://localhost:8080/debug/mempool` - Pending transactions
- **UTXOs**: `http://localhost:8080/debug/utxos` - Available UTXOs
- **Genesis Debug**: `http://localhost:8080/debug/genesis` - Genesis block info

### Prometheus Metrics (http://localhost:9090)

Key metrics include:
- `qbtc_connected_peers_total` - Number of connected peers
- `qbtc_blockchain_height` - Current blockchain height
- `qbtc_pending_transactions` - Mempool size
- `qbtc_uptime_seconds` - Node uptime
- `qbtc_health_check_status` - Component health status

### Grafana Dashboards (http://localhost:3000)

Pre-configured dashboards show:
- Network topology and peer connections
- Blockchain growth and sync status
- Transaction throughput
- System performance metrics

---

## 🔐 Security Features

- **Post-Quantum Signatures**: All transactions use ML-DSA-87 for quantum resistance
- **Chain ID**: Prevents replay attacks across different networks
- **Transaction Expiration**: Transactions expire after 1 hour by default
- **Rate Limiting**: Redis-based rate limiting on all API endpoints
- **DDoS Protection**: Integrated security middleware with IP blocking
- **Peer Reputation**: Automatic tracking and scoring of peer reliability
- **Secure WebSockets**: Authenticated WebSocket connections for real-time updates

### Security Audits

Internal and external audits can be found in the `audits/` folder. We are actively addressing issues in order of criticality.

---

## 🌐 Network Architecture

### Peer Discovery

Nodes use Kademlia DHT for decentralized peer discovery:
1. Bootstrap nodes maintain the DHT network
2. New nodes query the DHT for active validators
3. Validators announce their presence with gossip endpoints
4. NAT traversal support for nodes behind firewalls

### Block & Transaction Propagation

The gossip protocol ensures fast network-wide propagation:
1. Transactions are broadcast to all connected peers
2. Blocks are propagated immediately upon mining
3. Nodes sync missing blocks automatically
4. Failed peers are tracked and retried with exponential backoff

### Consensus & Fork Resolution

- Longest chain rule with proper difficulty validation
- Chain manager tracks multiple chain tips
- Automatic reorganization when longer chains are found
- Full validation of all blocks and transactions

---

## 📈 Roadmap

### Completed ✅
- Merkle Root validation
- Gossip protocol implementation
- Kademlia DHT integration
- UTXO state management
- Genesis block with 21M coin distribution
- Prometheus metrics & monitoring
- Docker containerization
- NAT traversal support
- Chain reorganization
- Transaction mempool
- RPC mining interface
- Event-driven architecture

### In Progress 🚧
- TLS encryption for all connections
- Peer authentication with ML-DSA
- Advanced fork choice rules
- State pruning optimizations

### Planned 📋
- Fee market implementation
- Smart contract support
- Light client protocol
- Mobile wallet SDK
- Hardware wallet integration

---

## 🧠 License

MIT License. See [LICENSE](./LICENSE) for more information.

---

## 🤝 Contributing

PRs and issues welcome! To contribute:

1. Fork the repo  
2. Create your feature branch (`git checkout -b feature/foo`)  
3. Commit your changes  
4. Push to the branch  
5. Open a PR  

### Development Tips

- Run tests: `pytest tests/`
- Check logs: `docker compose logs -f`
- Format code: `black .`
- Type checking: `mypy .`

---

## 🚀 Authors

- Christian Papathanasiou / Quantum Safe Technologies Corp

---

## 📚 Additional Resources

- [Bitcoin Whitepaper](https://bitcoin.org/bitcoin.pdf)
- [ML-DSA Specification](https://csrc.nist.gov/pubs/fips/204/final)
- [Kademlia Paper](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf)
- [qBTC Website](https://qb.tc)
