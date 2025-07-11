# qBTC Monitoring Setup

This directory contains the monitoring configuration for qBTC nodes using Prometheus and Grafana.

## Architecture

Each qBTC node exposes a `/health` endpoint that provides Prometheus-compatible metrics including:
- Node health status (database, blockchain, network, mempool)
- Blockchain height
- Connected and synced peer counts
- Pending transactions in mempool
- Database response times
- Node uptime

## Docker Compose Configurations

### 1. Test Environment (docker-compose.test.yml)
- 3-node test network (1 bootstrap + 2 validators)
- Prometheus and Grafana included
- Nginx load balancer for API access
- Redis for caching (3 separate databases)
- Full access to Grafana (admin/admin123)
- Suitable for development and testing

**Start with:**
```bash
docker compose -f docker-compose.test.yml up -d
```

**Access:**
- Bootstrap API: http://localhost:8080 (via nginx)
- Validator 1 API: http://localhost:8081
- Validator 2 API: http://localhost:8082
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin123)
- RPC: localhost:8332, 8333, 8334

### 2. Production Bootstrap (docker-compose.bootstrap.yml)
- Single bootstrap node with secure monitoring
- Nginx reverse proxy with SSL/TLS
- Public read-only Grafana dashboards
- Rate limiting and security headers
- DDoS protection enabled

**Requirements:**
- SSL certificates in `monitoring/nginx/ssl/`
- Environment variables set

**Start with:**
```bash
# Set required environment variables
export BOOTSTRAP_WALLET_PASSWORD="your-secure-password"
export ADMIN_ADDRESS="your-admin-address"
export GRAFANA_ADMIN_USER="admin"
export GRAFANA_ADMIN_PASSWORD="secure-password"
export GRAFANA_DOMAIN="your-domain.com"

# Generate SSL certificates
mkdir -p monitoring/nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout monitoring/nginx/ssl/server.key \
  -out monitoring/nginx/ssl/server.crt \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=your-domain.com"

# Start bootstrap server
docker compose -f docker-compose.bootstrap.yml up -d
```

**Access:**
- API: https://localhost:8080 (SSL)
- Grafana: https://localhost:443 (public read-only)
- Admin Grafana: https://localhost:443 (login as admin)
- RPC: localhost:8332
- DHT: localhost:8001/udp
- Gossip: localhost:8002/tcp

### 3. Production Validator (docker-compose.validator.yml)
- Connects to mainnet via api.bitcoinqs.org:8001
- Local monitoring stack
- SSL/TLS enabled
- Public read-only Grafana dashboards
- DDoS protection enabled

**Start with:**
```bash
# Set required environment variables
export VALIDATOR_WALLET_PASSWORD="your-secure-password"
export ADMIN_ADDRESS="your-admin-address"
export GRAFANA_ADMIN_USER="admin"
export GRAFANA_ADMIN_PASSWORD="secure-password"
export GRAFANA_DOMAIN="your-domain.com"

# Generate SSL certificates (same as bootstrap)
mkdir -p monitoring/nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout monitoring/nginx/ssl/server.key \
  -out monitoring/nginx/ssl/server.crt \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=your-domain.com"

# Connect to default mainnet
docker compose -f docker-compose.validator.yml up -d

# OR connect to custom bootstrap server
BOOTSTRAP_SERVER=your.bootstrap.server BOOTSTRAP_PORT=8001 \
  docker compose -f docker-compose.validator.yml up -d
```

**Access:**
- API: https://localhost:8080 (SSL)
- Grafana: https://localhost:443 (public read-only)
- Admin Grafana: https://localhost:443 (login as admin)
- RPC: localhost:8332

## Grafana Dashboard

A pre-configured dashboard (`qbtc-overview.json`) is automatically loaded showing:
- Node health status
- Blockchain height over time
- Network peer statistics
- Mempool transaction counts
- Database performance metrics
- Node uptime

## Production Security Features

Both production configurations (bootstrap and validator) include:
- **Anonymous Access**: Public read-only dashboards
- **Admin Access**: Secured admin login for configuration
- **SSL/TLS**: All traffic encrypted
- **Security Headers**: HSTS, CSP, XSS protection
- **Rate Limiting**: Via Redis (when enabled)
- **DDoS Protection**: Built-in protection mechanisms

## Prometheus Metrics

Available metrics include:
- `qbtc_node_info` - Node information
- `qbtc_uptime_seconds` - Node uptime
- `qbtc_blockchain_height` - Current blockchain height
- `qbtc_blockchain_sync_status` - Sync status (1=synced, 0=not synced)
- `qbtc_last_block_time_seconds` - Timestamp of last block
- `qbtc_pending_transactions` - Mempool transaction count
- `qbtc_connected_peers_total` - Total connected peers
- `qbtc_synced_peers` - Number of synced peers
- `qbtc_failed_peers` - Number of failed peers
- `qbtc_database_response_seconds` - Database response time histogram
- `qbtc_health_check_status{component}` - Component health status

## Nginx Configuration

Production deployments use nginx for:
- SSL/TLS termination
- Reverse proxy to API and Grafana
- Load balancing (test environment)
- Security headers
- Rate limiting (when configured)

Configuration files:
- `nginx-test.conf` - Simple load balancer for test environment
- `nginx-prod-split.conf` - Production config with SSL and split ports

## Troubleshooting

### Grafana not showing data
1. Check Prometheus targets at http://localhost:9090/targets
2. Verify nodes are running and `/health` endpoint is accessible
3. Check container logs: `docker compose logs prometheus grafana`

### SSL Certificate Issues
1. Ensure `server.crt` and `server.key` are in `monitoring/nginx/ssl/`
2. Verify certificate validity and domain match
3. Check nginx logs: `docker compose -f docker-compose.bootstrap.yml logs nginx`

### Authentication Issues
1. Verify GRAFANA_ADMIN_PASSWORD is set correctly
2. For public dashboards, ensure anonymous access is working
3. Admin login at /login with configured credentials

### Connection Issues
1. Check if bootstrap is reachable: `curl http://api.bitcoinqs.org:8080/health`
2. Verify Docker network: `docker network inspect qbtc-core_qbtc-network`
3. Check firewall rules for required ports

## Monitoring Best Practices

1. **Resource Monitoring**: Keep Grafana dashboards open to monitor resource usage
2. **Alert Configuration**: Set up alerts in Grafana for critical metrics
3. **Log Aggregation**: Consider adding log aggregation for production
4. **Backup**: Regularly backup Prometheus data and Grafana configurations
5. **Updates**: Keep Prometheus and Grafana images updated for security