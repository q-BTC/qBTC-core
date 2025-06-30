
# qBTC-core DEV NET

**qBTC-core** is a modern blockchain implementation inspired by Satoshi Nakamoto’s original design for Bitcoin. It stays true to foundational concepts such as:

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
- 📡 **Gossip Protocol** for fast block propagation
- 🧠 **Protobuf-encoded Transactions and Blocks**
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
+-------------------+
         |
         v
+----------------------+
| Local DB (RocksDB)   |
+----------------------+
```

---

## 🛠 Getting Started

### 1. Clone the Repository & Install Dependencies

```bash
git clone https://github.com/bitcoinqs/qBTC-core.git
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

You can start qBTC-core either as a **bootstrap server** or connect to an existing bootstrap peer.

#### CLI Usage

```bash
usage: main.py [-h] [--Bootstrap_ip BOOTSTRAP_IP]
               [--Bootstrap_port BOOTSTRAP_PORT] --wallet WALLET [--local]
               validator_port [gossip_port]
```

#### a) Start as a Bootstrap Server

```bash
python main.py 9001 9002 --wallet mywallet.json --local
```

This initializes a validator node and makes it discoverable by others.

#### b) Connect to an Existing Bootstrap Server

```bash
python main.py 9003 9004 --wallet mywallet.json --Bootstrap_ip 192.168.1.10 --Bootstrap_port 9002
```

Replace `192.168.1.10` and `9002` with the IP and port of your chosen bootstrap peer.

If you want to connect to the existing qBTC network then use the following parameters 

```bash
python3 main.py --Bootstrap_ip api.bitcoinqs.org --Bootstrap_port 8001 --wallet admin.json 8009 8010
```

Where 8009 8010 are example DHT and gossip ports on your local server. 
---

## 🧪 Testing Multi-Node

You can simulate multiple validators by launching separate containers or Python processes with unique ports and wallet keys.

---

## 📜 File Structure

| File/Folder          | Description                             |
|----------------------|-----------------------------------------|
| `main.py`            | Entry point for validator logic         |
| `blockchain/`        | Block, transaction, UTXO, Merkle logic  |
| `dht/`               | Kademlia-based peer discovery           |
| `gossip/`            | Gossip protocol for block syncing       |
| `protobuf.proto`     | Message format for blocks and txns      |
| `database/`          | Local RocksDB-like storage abstraction  |
| `wallet/`            | Post-quantum key management (ML-DSA)    |

---

## ⛏️ Submitting & Mining Transactions

### Submitting a Transaction to the Mempool

You can broadcast a signed transaction to the mempool using the following command:

```bash
python3 harness.py --node http://localhost:8080 --receiver bqs1Bo4quBsE6f5aitv42X5n1S9kASsphn9At --amount 500 --wallet ~/Desktop/ledger.json
```

This sends 500 qBTC to the specified address using your signed wallet.

---

### Mining Transactions in the Mempool

To mine blocks (including mempool transactions), use `cpuminer-opt` connected to any node's RPC endpoint:

```bash
docker run --rm -it cpuminer-opt   -a sha256d   -o http://api.bitcoinqs.org:8332   -u someuser -p x   --coinbase-addr=1BoatSLRHtKNngkdXEeobR76b53LETtpyT
```

#### Example Output:

```
[2025-05-28 11:47:16] 14 of 14 miner threads started using 'sha256d' algorithm
[2025-05-28 11:47:17] CPU temp: curr 0 C max 0, Freq: 0.000/0.000 GHz
[2025-05-28 11:47:17] New Block 1101, Tx 0, Net Diff 1.5259e-05, Ntime 6836f7c5
                      Miner TTF @ 280.00 h/s 3m54s, Net TTF @ 0.00 h/s NA
[2025-05-28 11:47:17] 1 Submitted Diff 8.5386e-05, Block 1101, Ntime c5f73668
[2025-05-28 11:47:17] 1 A1 S0 R0 BLOCK SOLVED 1, 0.497 sec (207ms)
[2025-05-28 11:47:17] New Block 1102, Tx 0, Net Diff 1.5259e-05, Ntime 6836f7c5
                      Miner TTF @ 41.47 Mh/s 0m00s, Net TTF @ 0.00 h/s NA
```

---

## 🔐 Security Notes

- Transactions use **ML-DSA** for post-quantum-safe signing.
- Each validator announces itself via DHT and syncs using gossip.
- Merkle roots ensure transaction integrity in each block.
- Future work includes replay protection, rate limiting, TLS, and full PoW consensus validation.
- Internal/external audits can be found in the audits folder we are working our way through these issues in order of criticality

---

## 📈 Roadmap

- ✅ Merkle Root, Gossip, Kademlia, UTXO
- 🔒 TLS + Peer Authentication
- ⚠️ Fork Choice Rule & Difficulty Enforcement
- 🧮 Fee Market & Miner Incentives
- 🧹 UTXO Pruning & State Compression

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

---

## 🚀 Authors

- Christian Papathanasiou / Quantum Safe Technologies Corp  
