"""
Health monitoring system for qBTC-core
"""

import time
import asyncio
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

from prometheus_client import Gauge, Histogram, Info, generate_latest, CONTENT_TYPE_LATEST

from database.database import get_db, get_current_height
from state.state import mempool_manager
from log_utils import get_logger

logger = get_logger(__name__)

# Prometheus metrics
node_info = Info('qbtc_node', 'qBTC node information')
uptime_seconds = Gauge('qbtc_uptime_seconds', 'Node uptime in seconds')
blockchain_height = Gauge('qbtc_blockchain_height', 'Current blockchain height')
blockchain_sync_status = Gauge('qbtc_blockchain_sync_status', 'Blockchain sync status (1=synced, 0=not synced)')
last_block_time = Gauge('qbtc_last_block_time_seconds', 'Timestamp of the last block')
pending_transactions_count = Gauge('qbtc_pending_transactions', 'Number of pending transactions in mempool')
connected_peers_total = Gauge('qbtc_connected_peers_total', 'Total number of connected peers')
synced_peers_count = Gauge('qbtc_synced_peers', 'Number of synced peers')
failed_peers_count = Gauge('qbtc_failed_peers', 'Number of failed peers')
database_response_time = Histogram('qbtc_database_response_seconds', 'Database response time in seconds')
health_check_status = Gauge('qbtc_health_check_status', 'Health check status by component', ['component'])

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

@dataclass
class ComponentHealth:
    status: HealthStatus
    message: str
    last_check: float
    details: Optional[Dict[str, Any]] = None

class HealthMonitor:
    """System health monitoring"""
    
    def __init__(self):
        self.components: Dict[str, ComponentHealth] = {}
        self.start_time = time.time()
        # Set node info on initialization
        import os
        node_info.info({
            'version': '1.0.0',
            'node_id': os.environ.get('HOSTNAME', 'unknown')
        })
    
    async def check_database_health(self) -> ComponentHealth:
        """Check database connectivity and performance"""
        try:
            start_time = time.time()
            db = get_db()
            
            # Test read operation
            height, tip = get_current_height(db)
            
            # Test performance
            check_duration = time.time() - start_time
            
            # Update Prometheus metrics
            database_response_time.observe(check_duration)
            blockchain_height.set(height)
            
            if check_duration > 1.0:  # Slow response
                health_check_status.labels(component='database').set(0.5)  # Degraded
                return ComponentHealth(
                    status=HealthStatus.DEGRADED,
                    message=f"Database slow: {check_duration:.2f}s",
                    last_check=time.time(),
                    details={"response_time": check_duration, "height": height}
                )
            
            health_check_status.labels(component='database').set(1.0)  # Healthy
            return ComponentHealth(
                status=HealthStatus.HEALTHY,
                message="Database operational",
                last_check=time.time(),
                details={"response_time": check_duration, "height": height}
            )
            
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}")
            health_check_status.labels(component='database').set(0.0)  # Unhealthy
            return ComponentHealth(
                status=HealthStatus.UNHEALTHY,
                message=f"Database error: {str(e)}",
                last_check=time.time()
            )
    
    async def check_blockchain_health(self) -> ComponentHealth:
        """Check blockchain sync status"""
        try:
            db = get_db()
            height, tip = get_current_height(db)
            
            # Check if we're receiving new blocks
            current_time = time.time()
            
            # Get latest block timestamp
            if tip and tip != "0" * 64:
                block_key = f"block:{tip}".encode()
                block_data = db.get(block_key)
                if block_data:
                    block = json.loads(block_data.decode())
                    block_time = block.get("timestamp", 0) / 1000  # Convert to seconds
                    time_since_block = current_time - block_time
                    
                    # Update Prometheus metrics
                    last_block_time.set(block_time)
                    
                    if time_since_block > 3600:  # No blocks for 1 hour
                        blockchain_sync_status.set(0)
                        health_check_status.labels(component='blockchain').set(0.5)  # Degraded
                        return ComponentHealth(
                            status=HealthStatus.DEGRADED,
                            message=f"No new blocks for {time_since_block/60:.1f} minutes",
                            last_check=current_time,
                            details={"height": height, "last_block_time": block_time}
                        )
            
            blockchain_sync_status.set(1)
            health_check_status.labels(component='blockchain').set(1.0)  # Healthy
            return ComponentHealth(
                status=HealthStatus.HEALTHY,
                message="Blockchain synchronized",
                last_check=current_time,
                details={"height": height, "tip": tip}
            )
            
        except Exception as e:
            logger.error(f"Blockchain health check failed: {str(e)}")
            blockchain_sync_status.set(0)
            health_check_status.labels(component='blockchain').set(0.0)  # Unhealthy
            return ComponentHealth(
                status=HealthStatus.UNHEALTHY,
                message=f"Blockchain error: {str(e)}",
                last_check=time.time()
            )
    
    async def check_network_health(self, gossip_client) -> ComponentHealth:
        """Check network connectivity and peer count"""
        try:
            if not gossip_client:
                connected_peers_total.set(0)
                synced_peers_count.set(0)
                failed_peers_count.set(0)
                health_check_status.labels(component='network').set(0.0)  # Unhealthy
                return ComponentHealth(
                    status=HealthStatus.UNHEALTHY,
                    message="Gossip client not available",
                    last_check=time.time()
                )
            
            total_peers = len(gossip_client.client_peers) + len(gossip_client.dht_peers)
            synced_peers = len(gossip_client.synced_peers)
            failed_peers = len(gossip_client.failed_peers)
            
            # Update Prometheus metrics
            connected_peers_total.set(total_peers)
            synced_peers_count.set(synced_peers)
            failed_peers_count.set(failed_peers)
            
            if total_peers == 0:
                health_check_status.labels(component='network').set(0.0)  # Unhealthy
                return ComponentHealth(
                    status=HealthStatus.UNHEALTHY,
                    message="No network peers connected",
                    last_check=time.time(),
                    details={"total_peers": 0, "synced_peers": 0}
                )
            
            if total_peers < 3:
                health_check_status.labels(component='network').set(0.5)  # Degraded
                return ComponentHealth(
                    status=HealthStatus.DEGRADED,
                    message=f"Low peer count: {total_peers}",
                    last_check=time.time(),
                    details={
                        "total_peers": total_peers,
                        "synced_peers": synced_peers,
                        "failed_peers": failed_peers
                    }
                )
            
            health_check_status.labels(component='network').set(1.0)  # Healthy
            return ComponentHealth(
                status=HealthStatus.HEALTHY,
                message="Network connected",
                last_check=time.time(),
                details={
                    "total_peers": total_peers,
                    "synced_peers": synced_peers,
                    "failed_peers": failed_peers
                }
            )
            
        except Exception as e:
            logger.error(f"Network health check failed: {str(e)}")
            health_check_status.labels(component='network').set(0.0)  # Unhealthy
            return ComponentHealth(
                status=HealthStatus.UNHEALTHY,
                message=f"Network error: {str(e)}",
                last_check=time.time()
            )
    
    async def check_mempool_health(self) -> ComponentHealth:
        """Check transaction mempool status"""
        try:
            pending_count = mempool_manager.size()
            stats = mempool_manager.get_stats()
            
            # Update Prometheus metric
            pending_transactions_count.set(pending_count)
            
            if pending_count > 10000:  # Large mempool
                health_check_status.labels(component='mempool').set(0.5)  # Degraded
                return ComponentHealth(
                    status=HealthStatus.DEGRADED,
                    message=f"Large mempool: {pending_count} transactions",
                    last_check=time.time(),
                    details={
                        "pending_transactions": pending_count,
                        "memory_usage_mb": stats["memory_usage_mb"],
                        "in_use_utxos": stats["in_use_utxos"]
                    }
                )
            
            health_check_status.labels(component='mempool').set(1.0)  # Healthy
            return ComponentHealth(
                status=HealthStatus.HEALTHY,
                message="Mempool normal",
                last_check=time.time(),
                details={
                    "pending_transactions": pending_count,
                    "memory_usage_mb": stats["memory_usage_mb"],
                    "in_use_utxos": stats["in_use_utxos"]
                }
            )
            
        except Exception as e:
            logger.error(f"Mempool health check failed: {str(e)}")
            health_check_status.labels(component='mempool').set(0.0)  # Unhealthy
            return ComponentHealth(
                status=HealthStatus.UNHEALTHY,
                message=f"Mempool error: {str(e)}",
                last_check=time.time()
            )
    
    async def run_health_checks(self, gossip_client=None) -> Dict[str, ComponentHealth]:
        """Run all health checks"""
        # Update uptime metric
        uptime_seconds.set(time.time() - self.start_time)
        
        checks = {
            "database": self.check_database_health(),
            "blockchain": self.check_blockchain_health(),
            "network": self.check_network_health(gossip_client),
            "mempool": self.check_mempool_health()
        }
        
        # Run checks concurrently
        results = await asyncio.gather(*checks.values(), return_exceptions=True)
        
        # Process results
        health_status = {}
        for component, result in zip(checks.keys(), results):
            if isinstance(result, Exception):
                health_status[component] = ComponentHealth(
                    status=HealthStatus.UNHEALTHY,
                    message=f"Health check failed: {str(result)}",
                    last_check=time.time()
                )
                health_check_status.labels(component=component).set(0.0)
            else:
                health_status[component] = result
        
        self.components = health_status
        return health_status
    
    def get_overall_health(self) -> HealthStatus:
        """Get overall system health status"""
        if not self.components:
            return HealthStatus.UNHEALTHY
        
        statuses = [comp.status for comp in self.components.values()]
        
        if any(status == HealthStatus.UNHEALTHY for status in statuses):
            return HealthStatus.UNHEALTHY
        elif any(status == HealthStatus.DEGRADED for status in statuses):
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive health summary"""
        overall_status = self.get_overall_health()
        uptime = time.time() - self.start_time
        
        return {
            "status": overall_status.value,
            "uptime": uptime,
            "timestamp": time.time(),
            "components": {
                name: {
                    "status": comp.status.value,
                    "message": comp.message,
                    "last_check": comp.last_check,
                    "details": comp.details
                }
                for name, comp in self.components.items()
            }
        }
    
    def generate_metrics(self) -> tuple[bytes, str]:
        """Generate Prometheus metrics"""
        return generate_latest(), CONTENT_TYPE_LATEST

# Global health monitor instance
health_monitor = HealthMonitor()