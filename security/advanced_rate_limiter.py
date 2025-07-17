"""
Advanced rate limiting system with Redis backend support
"""

import time
import json
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
from enum import Enum

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from fastapi import Request
from errors.exceptions import RateLimitError

logger = logging.getLogger(__name__)

class ThreatLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class RateLimitRule:
    endpoint: str
    max_requests: int
    window_seconds: int
    burst_limit: Optional[int] = None  # Allow short bursts
    threat_multiplier: float = 1.0  # Adjust based on threat level

@dataclass
class ClientInfo:
    ip: str
    user_agent: str
    first_seen: float
    last_seen: float
    request_count: int = 0
    failed_requests: int = 0
    threat_level: ThreatLevel = ThreatLevel.LOW
    blocked_until: float = 0
    warnings: int = 0
    
    def is_blocked(self) -> bool:
        return time.time() < self.blocked_until
    
    def calculate_threat_score(self) -> float:
        """Calculate threat score based on behavior"""
        age = time.time() - self.first_seen
        if age == 0:
            age = 1
        
        # Base score from failure rate
        failure_rate = self.failed_requests / max(self.request_count, 1)
        threat_score = failure_rate * 100
        
        # Increase score for new clients
        if age < 300:  # Less than 5 minutes old
            threat_score += 20
        
        # Increase score for high request rate
        request_rate = self.request_count / age
        if request_rate > 10:  # More than 10 requests per second average
            threat_score += 30
        
        return min(threat_score, 100)
    
    def update_threat_level(self):
        """Update threat level based on current score"""
        score = self.calculate_threat_score()
        
        if score >= 80:
            self.threat_level = ThreatLevel.CRITICAL
        elif score >= 60:
            self.threat_level = ThreatLevel.HIGH
        elif score >= 30:
            self.threat_level = ThreatLevel.MEDIUM
        else:
            self.threat_level = ThreatLevel.LOW

class AdvancedRateLimiter:
    """Advanced rate limiter with threat detection and Redis support"""
    
    def __init__(self, redis_url: Optional[str] = None, enable_redis: bool = True):
        self.redis_client = None
        self.use_redis = enable_redis and REDIS_AVAILABLE
        
        # In-memory fallback
        self.client_info: Dict[str, ClientInfo] = {}
        self.request_windows: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(deque))
        
        # Rate limit rules
        self.rules = {
            "/worker": RateLimitRule("/worker", 10, 60, burst_limit=3),
            "/balance": RateLimitRule("/balance", 100, 60, burst_limit=20),
            "/transactions": RateLimitRule("/transactions", 50, 60, burst_limit=10),
            "/health": RateLimitRule("/health", 30, 60, burst_limit=10),
            "default": RateLimitRule("default", 60, 60, burst_limit=15)
        }
        
        # Threat-based adjustments
        self.threat_multipliers = {
            ThreatLevel.LOW: 1.0,
            ThreatLevel.MEDIUM: 0.7,
            ThreatLevel.HIGH: 0.4,
            ThreatLevel.CRITICAL: 0.1
        }
        
        # Initialize Redis if available
        if self.use_redis and redis_url:
            self._init_redis(redis_url)
    
    def _init_redis(self, redis_url: str):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
            logger.info("Redis rate limiter backend initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize Redis: {e}, falling back to in-memory")
            self.use_redis = False
    
    def _get_client_key(self, request: Request) -> str:
        """Get unique client identifier"""
        # Check for real IP behind proxy
        forwarded_for = request.headers.get('x-forwarded-for')
        if forwarded_for:
            client_ip = forwarded_for.split(',')[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"
        
        return client_ip
    
    def _get_endpoint_key(self, path: str) -> str:
        """Map request path to rate limit rule"""
        if path.startswith('/worker'):
            return '/worker'
        elif path.startswith('/balance'):
            return '/balance'
        elif path.startswith('/transactions'):
            return '/transactions'
        elif path.startswith('/health'):
            return '/health'
        else:
            return 'default'
    
    async def _get_client_info(self, client_key: str, user_agent: str) -> ClientInfo:
        """Get or create client info"""
        if self.use_redis and self.redis_client:
            try:
                # Check if IP is in permanently blocked list
                is_permanently_blocked = await self.redis_client.sismember("permanently_blocked_ips", client_key)
                if is_permanently_blocked:
                    # Return a permanently blocked client info
                    return ClientInfo(
                        ip=client_key,
                        user_agent=user_agent,
                        first_seen=time.time(),
                        last_seen=time.time(),
                        threat_level=ThreatLevel.CRITICAL,
                        blocked_until=float('inf'),  # Blocked forever
                        warnings=999
                    )
                
                data = await self.redis_client.get(f"client:{client_key}")
                if data:
                    client_data = json.loads(data)
                    client_info = ClientInfo(**client_data)
                    client_info.last_seen = time.time()
                    return client_info
            except Exception as e:
                logger.warning(f"Redis get failed: {e}")
        
        # Fallback to in-memory
        if client_key not in self.client_info:
            self.client_info[client_key] = ClientInfo(
                ip=client_key,
                user_agent=user_agent,
                first_seen=time.time(),
                last_seen=time.time()
            )
        
        self.client_info[client_key].last_seen = time.time()
        return self.client_info[client_key]
    
    async def _save_client_info(self, client_key: str, client_info: ClientInfo):
        """Save client info to storage"""
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.setex(
                    f"client:{client_key}",
                    3600,  # 1 hour TTL
                    json.dumps(asdict(client_info))
                )
            except Exception as e:
                logger.warning(f"Redis save failed: {e}")
    
    async def _check_request_window(self, client_key: str, endpoint_key: str, rule: RateLimitRule) -> Tuple[int, bool]:
        """Check request count in time window"""
        now = time.time()
        window_start = now - rule.window_seconds
        
        if self.use_redis and self.redis_client:
            try:
                # Use Redis sorted set for time-based windows
                pipe = self.redis_client.pipeline()
                redis_key = f"requests:{client_key}:{endpoint_key}"
                
                # Remove old entries
                pipe.zremrangebyscore(redis_key, 0, window_start)
                # Count current requests
                pipe.zcard(redis_key)
                # Add current request
                pipe.zadd(redis_key, {str(now): now})
                # Set expiry
                pipe.expire(redis_key, rule.window_seconds + 60)
                
                results = await pipe.execute()
                current_count = results[1]
                
                return current_count, current_count < rule.max_requests
                
            except Exception as e:
                logger.warning(f"Redis window check failed: {e}")
        
        # Fallback to in-memory
        requests = self.request_windows[client_key][endpoint_key]
        
        # Clean old requests
        while requests and requests[0] < window_start:
            requests.popleft()
        
        # Add current request
        requests.append(now)
        
        return len(requests), len(requests) <= rule.max_requests
    
    async def check_rate_limit(self, request: Request) -> bool:
        """Main rate limiting check"""
        client_key = self._get_client_key(request)
        endpoint_key = self._get_endpoint_key(request.url.path)
        user_agent = request.headers.get('user-agent', 'unknown')
        
        # Get client info
        client_info = await self._get_client_info(client_key, user_agent)
        
        # Check if client is blocked
        if client_info.is_blocked():
            logger.warning(f"Blocked client {client_key} attempted request")
            raise RateLimitError(f"Client blocked until {client_info.blocked_until}")
        
        # Get rate limit rule
        rule = self.rules.get(endpoint_key, self.rules['default'])
        
        # Adjust limits based on threat level
        client_info.update_threat_level()
        threat_multiplier = self.threat_multipliers[client_info.threat_level]
        effective_limit = int(rule.max_requests * threat_multiplier)
        
        # Check request window
        request_count, within_limit = await self._check_request_window(
            client_key, endpoint_key, 
            RateLimitRule(rule.endpoint, effective_limit, rule.window_seconds)
        )
        
        # Update client stats
        client_info.request_count += 1
        
        if not within_limit:
            # Rate limit exceeded
            client_info.warnings += 1
            await self._handle_rate_limit_violation(client_key, client_info, request_count, effective_limit)
            
            logger.warning(
                f"Rate limit exceeded for {client_key} on {endpoint_key}: "
                f"{request_count}/{effective_limit} (threat: {client_info.threat_level.value})"
            )
            
            raise RateLimitError(
                f"Rate limit exceeded: {request_count}/{effective_limit} requests "
                f"in {rule.window_seconds} seconds"
            )
        
        # Save updated client info
        await self._save_client_info(client_key, client_info)
        
        # Log suspicious activity
        if client_info.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
            logger.warning(
                f"High threat client {client_key}: level={client_info.threat_level.value}, "
                f"score={client_info.calculate_threat_score():.1f}"
            )
        
        return True
    
    async def _handle_rate_limit_violation(self, client_key: str, client_info: ClientInfo, request_count: int, limit: int):
        """Handle rate limit violations with progressive penalties"""
        client_info.failed_requests += 1
        
        # Progressive blocking based on warnings
        if client_info.warnings == 1:
            block_duration = 300  # 5 minutes
        elif client_info.warnings == 2:
            block_duration = 900  # 15 minutes
        elif client_info.warnings <= 5:
            block_duration = 3600  # 1 hour
        else:
            block_duration = 86400  # 24 hours
        
        # Increase block duration for high threat clients
        if client_info.threat_level == ThreatLevel.CRITICAL:
            block_duration *= 3
        elif client_info.threat_level == ThreatLevel.HIGH:
            block_duration *= 2
        
        client_info.blocked_until = time.time() + block_duration
        
        logger.error(
            f"Client {client_key} blocked for {block_duration}s "
            f"(warning #{client_info.warnings}, threat: {client_info.threat_level.value})"
        )
    
    async def record_failed_request(self, request: Request, error_type: str):
        """Record a failed request for threat analysis"""
        client_key = self._get_client_key(request)
        user_agent = request.headers.get('user-agent', 'unknown')
        
        client_info = await self._get_client_info(client_key, user_agent)
        client_info.failed_requests += 1
        
        # Auto-block for too many failures
        failure_rate = client_info.failed_requests / max(client_info.request_count, 1)
        if failure_rate > 0.5 and client_info.failed_requests > 10:
            client_info.blocked_until = time.time() + 3600  # 1 hour
            logger.error(f"Auto-blocked {client_key} for high failure rate: {failure_rate:.2f}")
        
        await self._save_client_info(client_key, client_info)
    
    async def get_client_stats(self, client_key: str) -> Optional[Dict]:
        """Get client statistics"""
        if self.use_redis and self.redis_client:
            try:
                data = await self.redis_client.get(f"client:{client_key}")
                if data:
                    return json.loads(data)
            except Exception:
                pass
        
        if client_key in self.client_info:
            return asdict(self.client_info[client_key])
        
        return None
    
    async def get_all_blocked_clients(self) -> List[Dict]:
        """Get all currently blocked clients"""
        blocked_clients = []
        now = time.time()
        
        if self.use_redis and self.redis_client:
            try:
                keys = await self.redis_client.keys("client:*")
                for key in keys:
                    data = await self.redis_client.get(key)
                    if data:
                        client_data = json.loads(data)
                        if client_data.get('blocked_until', 0) > now:
                            blocked_clients.append(client_data)
            except Exception:
                pass
        else:
            for client_info in self.client_info.values():
                if client_info.blocked_until > now:
                    blocked_clients.append(asdict(client_info))
        
        return blocked_clients
    
    async def block_client(self, client_key: str, duration_hours: int = 24, reason: str = "Manual block") -> bool:
        """Manually block a client"""
        try:
            block_duration = duration_hours * 3600  # Convert hours to seconds
            blocked_until = time.time() + block_duration
            
            if self.use_redis and self.redis_client:
                # Check if client exists, if not create new entry
                data = await self.redis_client.get(f"client:{client_key}")
                if data:
                    client_data = json.loads(data)
                else:
                    # Create new client entry
                    client_data = {
                        'ip': client_key,
                        'user_agent': 'Unknown',
                        'first_seen': time.time(),
                        'last_seen': time.time(),
                        'request_count': 0,
                        'failed_requests': 0,
                        'threat_level': ThreatLevel.CRITICAL.value,
                        'warnings': 999  # High warning count for manual blocks
                    }
                
                client_data['blocked_until'] = blocked_until
                client_data['threat_level'] = ThreatLevel.CRITICAL.value
                client_data['block_reason'] = reason
                
                # Store with expiration longer than block duration
                await self.redis_client.setex(f"client:{client_key}", block_duration + 3600, json.dumps(client_data))
                
                # Add to permanently blocked list for persistent blocking
                await self.redis_client.sadd("permanently_blocked_ips", client_key)
                
                logger.info(f"Blocked client {client_key} for {duration_hours} hours. Reason: {reason}")
                return True
            else:
                # In-memory fallback
                if client_key in self.client_info:
                    client = self.client_info[client_key]
                else:
                    # Create new client entry
                    client = ClientInfo(
                        ip=client_key,
                        user_agent='Unknown',
                        first_seen=time.time(),
                        last_seen=time.time(),
                        threat_level=ThreatLevel.CRITICAL
                    )
                    self.client_info[client_key] = client
                
                client.blocked_until = blocked_until
                client.threat_level = ThreatLevel.CRITICAL
                client.warnings = 999  # High warning count for manual blocks
                
                logger.info(f"Blocked client {client_key} for {duration_hours} hours. Reason: {reason}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to block client {client_key}: {e}")
        
        return False

    async def unblock_client(self, client_key: str) -> bool:
        """Manually unblock a client"""
        try:
            if self.use_redis and self.redis_client:
                data = await self.redis_client.get(f"client:{client_key}")
                if data:
                    client_data = json.loads(data)
                    client_data['blocked_until'] = 0
                    client_data['warnings'] = 0
                    await self.redis_client.setex(f"client:{client_key}", 3600, json.dumps(client_data))
                    
                    # Remove from permanently blocked list
                    await self.redis_client.srem("permanently_blocked_ips", client_key)
                    
                    logger.info(f"Unblocked client {client_key}")
                    return True
            else:
                if client_key in self.client_info:
                    self.client_info[client_key].blocked_until = 0
                    self.client_info[client_key].warnings = 0
                    logger.info(f"Unblocked client {client_key}")
                    return True
        except Exception as e:
            logger.error(f"Failed to unblock client {client_key}: {e}")
        
        return False

# Global instance
advanced_rate_limiter = AdvancedRateLimiter()