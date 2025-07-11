# qBTC Deployment Guide

This guide covers different deployment scenarios for qBTC nodes.

## Overview

qBTC-core provides three Docker Compose configurations for different deployment scenarios:

1. **docker-compose.test.yml** - Local development/testing with 3 nodes
2. **docker-compose.bootstrap.yml** - Production bootstrap server with monitoring
3. **docker-compose.validator.yml** - Production validator node that connects to mainnet

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for wallet generation)
- Git

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/q-btc/qBTC-core.git
cd qBTC-core
```

### 2. Generate a Wallet

Before starting any node, you need a wallet:

```bash
python3 wallet/wallet.py
```

This creates a `wallet.json` file with your ML-DSA keypair. Keep it safe!

## Deployment Scenarios

### Scenario 1: Local Test Network (3 Nodes)

Perfect for development and testing. Creates a complete network with:
- 1 Bootstrap node
- 2 Validator nodes
- Full monitoring stack (Prometheus + Grafana)
- Redis for caching
- Nginx load balancer

```bash
# Start the test network
docker compose -f docker-compose.test.yml up -d

# View logs
docker compose -f docker-compose.test.yml logs -f

# Access services:
# - Bootstrap API: http://localhost:8080 (via nginx)
# - Validator1 API: http://localhost:8081
# - Validator2 API: http://localhost:8082
# - Grafana: http://localhost:3000 (admin/admin123)
# - Prometheus: http://localhost:9090
# - RPC ports: 8332, 8333, 8334

# Stop the network
docker compose -f docker-compose.test.yml down

# Clean up all data
docker compose -f docker-compose.test.yml down --volumes
```

### Scenario 2: Production Bootstrap Server

Run a bootstrap server that other nodes can connect to:

```bash
# Set required environment variables
export BOOTSTRAP_WALLET_PASSWORD=your-secure-password
export ADMIN_ADDRESS=your-admin-address
export GRAFANA_ADMIN_USER=admin
export GRAFANA_ADMIN_PASSWORD=secure-password
export GRAFANA_DOMAIN=your-domain.com

# Generate SSL certificates (or provide your own)
mkdir -p ./monitoring/nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout ./monitoring/nginx/ssl/server.key \
  -out ./monitoring/nginx/ssl/server.crt \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=your-domain.com"

# Start bootstrap server
docker compose -f docker-compose.bootstrap.yml up -d

# View logs
docker compose -f docker-compose.bootstrap.yml logs -f bootstrap

# Access services:
# - API: https://localhost:8080 (SSL)
# - Grafana: https://localhost:443 (public read-only)
# - RPC: localhost:8332
# - DHT: localhost:8001/udp
# - Gossip: localhost:8002/tcp
```

### Scenario 3: Production Validator Node

Connect to the mainnet as a validator:

```bash
# Set required environment variables
export VALIDATOR_WALLET_PASSWORD=your-secure-password
export ADMIN_ADDRESS=your-admin-address
export GRAFANA_ADMIN_USER=admin
export GRAFANA_ADMIN_PASSWORD=secure-password
export GRAFANA_DOMAIN=your-domain.com

# Generate SSL certificates (same as bootstrap)
mkdir -p ./monitoring/nginx/ssl
# ... (same SSL generation command as above)

# Connect to default mainnet (api.bitcoinqs.org)
docker compose -f docker-compose.validator.yml up -d

# OR connect to custom bootstrap server
BOOTSTRAP_SERVER=your.bootstrap.server BOOTSTRAP_PORT=8001 \
  docker compose -f docker-compose.validator.yml up -d

# View logs
docker compose -f docker-compose.validator.yml logs -f validator

# Access services (same as bootstrap):
# - API: https://localhost:8080 (SSL)
# - Grafana: https://localhost:443 (public read-only)
# - RPC: localhost:8332
```

## Configuration Options

### Environment Variables

#### Common Variables
- `WALLET_PASSWORD` - Password for the wallet file (required)
- `WALLET_FILE` - Wallet filename (default: varies by node type)
- `ADMIN_ADDRESS` - Admin address for security features
- `USE_REDIS` - Enable Redis caching (default: true)

#### Security Configuration
- `RATE_LIMIT_ENABLED` - Enable rate limiting (default: false)
- `DDOS_PROTECTION_ENABLED` - Enable DDoS protection (default: true for production)
- `ATTACK_PATTERN_DETECTION` - Detect attack patterns (default: true for production)
- `BOT_DETECTION_ENABLED` - Detect bots (default: true for production)
- `PEER_REPUTATION_ENABLED` - Track peer reputation (default: true)
- `SECURITY_LOGGING_ENABLED` - Enable security logging (default: true)

#### Monitoring Configuration
- `GRAFANA_ADMIN_USER` - Grafana admin username (default: admin)
- `GRAFANA_ADMIN_PASSWORD` - Grafana admin password (required for production)
- `GRAFANA_DOMAIN` - Domain for Grafana (required for production)

### CLI Arguments

When running without Docker, use these arguments:

```bash
# Bootstrap server
python3 main.py --bootstrap --dht-port 8001 --gossip-port 8002

# Validator connecting to mainnet
python3 main.py --bootstrap_server api.bitcoinqs.org --bootstrap_port 8001

# Validator with custom ports
python3 main.py --bootstrap_server api.bitcoinqs.org --bootstrap_port 8001 \
  --dht-port 8003 --gossip-port 8004

# With external IP for NAT traversal
python3 main.py --external-ip YOUR_PUBLIC_IP
```

## Monitoring

### Grafana Dashboards

All deployments include pre-configured Grafana dashboards:
- **Network Overview** - Peer connections, blockchain height, sync status
- **Performance Metrics** - CPU, memory, disk usage
- **Transaction Flow** - Mempool size, transaction throughput
- **Security Events** - Attack detection, rate limiting

### Prometheus Metrics

Key metrics exposed:
- `qbtc_connected_peers_total` - Number of connected peers
- `qbtc_blockchain_height` - Current blockchain height
- `qbtc_pending_transactions` - Mempool size
- `qbtc_health_check_status` - Component health status

## Mining

To mine blocks to your node:

```bash
# Connect to any node's RPC port
docker run --rm -it cpuminer-opt \
  -a sha256d \
  -o http://localhost:8332 \
  -u test -p x \
  --coinbase-addr=YOUR_QBTC_ADDRESS
```

## Maintenance

### Backup

```bash
# Backup blockchain data
docker run --rm -v VOLUME_NAME:/data -v $(pwd):/backup alpine \
  tar czf /backup/blockchain-backup.tar.gz -C /data .

# Example for test network bootstrap node
docker run --rm -v qbtc-core_bootstrap-data:/data -v $(pwd):/backup alpine \
  tar czf /backup/bootstrap-backup.tar.gz -C /data .
```

### Update

```bash
# Pull latest changes
git pull

# Rebuild and restart
docker compose -f docker-compose.SCENARIO.yml up -d --build
```

### View Logs

```bash
# All services
docker compose -f docker-compose.SCENARIO.yml logs -f

# Specific service
docker compose -f docker-compose.SCENARIO.yml logs -f SERVICE_NAME
```

## Troubleshooting

### Connection Issues

1. Check if bootstrap is reachable:
```bash
curl http://api.bitcoinqs.org:8080/health
```

2. Verify ports are open:
```bash
netstat -tuln | grep -E "8001|8002|8080|8332"
```

3. Check Docker network:
```bash
docker network ls
docker network inspect qbtc-core_qbtc-network
```

### Wallet Issues

1. Verify wallet file exists:
```bash
ls -la wallet.json
```

2. Check wallet password in environment:
```bash
docker compose -f docker-compose.SCENARIO.yml config | grep WALLET
```

### Performance Issues

1. Check resource usage:
```bash
docker stats
```

2. View metrics in Grafana:
- http://localhost:3000 (test network)
- https://your-domain.com/grafana/ (production)

## Security Best Practices

1. **Wallet Security**
   - Use strong passwords
   - Backup wallet files securely
   - Never commit wallets to git

2. **Network Security**
   - Use firewalls to limit port access
   - Enable SSL for production
   - Regularly update Docker images

3. **Monitoring Security**
   - Change default Grafana passwords
   - Use read-only access for public dashboards
   - Limit Prometheus access

## Advanced Configuration

### Custom Network Configuration

Create a `.env` file:
```bash
# Network settings
BOOTSTRAP_SERVER=api.bitcoinqs.org
BOOTSTRAP_PORT=8001
DHT_PORT=8001
GOSSIP_PORT=8002

# Security settings
RATE_LIMIT_ENABLED=true
DDOS_PROTECTION_ENABLED=true

# Resource limits
CPU_LIMIT=2.0
MEMORY_LIMIT=2G
```

### Multi-Region Deployment

For geo-distributed networks:
1. Deploy bootstrap servers in multiple regions
2. Use external IPs for NAT traversal
3. Configure monitoring aggregation

## Support

- GitHub Issues: https://github.com/q-btc/qBTC-core/issues
- Documentation: https://qb.tc/docs
- Community: Discord/Telegram links