"""
Rate limiting and DDoS protection for qBTC-core
"""

import time
from collections import defaultdict, deque
from typing import Dict, Deque
import logging
from fastapi import Request
from errors.exceptions import RateLimitError

logger = logging.getLogger(__name__)

class RateLimiter:
    """Advanced rate limiter with multiple tiers"""
    
    def __init__(self):
        # Track requests per IP
        self.request_counts: Dict[str, Deque[float]] = defaultdict(deque)
        self.blocked_ips: Dict[str, float] = {}
        
        # Different limits for different endpoints
        self.endpoint_limits = {
            '/worker': {'requests': 10, 'window': 60},  # 10 tx/min
            '/balance': {'requests': 100, 'window': 60}, # 100 queries/min
            '/transactions': {'requests': 50, 'window': 60}, # 50 queries/min
            'default': {'requests': 60, 'window': 60}  # 60 requests/min default
        }
        
        # Track suspicious patterns
        self.suspicious_ips: Dict[str, Dict] = defaultdict(lambda: {
            'failed_requests': 0,
            'last_failed': 0,
            'warnings': 0
        })
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract real client IP considering proxies.

        H5: Only trust X-Forwarded-For/X-Real-IP if the direct connection
        comes from a trusted proxy. Otherwise, use the direct connection IP
        to prevent header spoofing.
        """
        from config.config import TRUSTED_PROXIES
        direct_ip = request.client.host if request.client else "unknown"

        # Only trust forwarded headers from known proxies
        if direct_ip in TRUSTED_PROXIES:
            forwarded_for = request.headers.get('x-forwarded-for')
            if forwarded_for:
                return forwarded_for.split(',')[0].strip()

            real_ip = request.headers.get('x-real-ip')
            if real_ip:
                return real_ip

        return direct_ip
    
    def _get_endpoint_key(self, path: str) -> str:
        """Get endpoint key for rate limiting rules"""
        # Map specific paths to rate limit categories
        if path.startswith('/worker'):
            return '/worker'
        elif path.startswith('/balance'):
            return '/balance'
        elif path.startswith('/transactions'):
            return '/transactions'
        else:
            return 'default'
    
    def is_blocked(self, ip: str) -> bool:
        """Check if IP is currently blocked"""
        if ip in self.blocked_ips:
            if time.time() < self.blocked_ips[ip]:
                return True
            else:
                # Unblock expired IPs
                del self.blocked_ips[ip]
        return False
    
    def _clean_old_requests(self, ip: str, window: int):
        """Remove requests older than the time window"""
        now = time.time()
        requests = self.request_counts[ip]
        
        while requests and requests[0] < now - window:
            requests.popleft()
    
    def check_rate_limit(self, request: Request) -> bool:
        """Check if request should be rate limited"""
        ip = self._get_client_ip(request)
        endpoint_key = self._get_endpoint_key(request.url.path)
        
        # Check if IP is blocked
        if self.is_blocked(ip):
            logger.warning(f"Blocked IP {ip} attempted request to {request.url.path}")
            raise RateLimitError(f"IP {ip} is temporarily blocked")
        
        # Get rate limit settings for this endpoint
        limits = self.endpoint_limits.get(endpoint_key, self.endpoint_limits['default'])
        max_requests = limits['requests']
        window = limits['window']
        
        # Clean old requests
        self._clean_old_requests(ip, window)
        
        # Check current request count
        current_requests = len(self.request_counts[ip])
        
        if current_requests >= max_requests:
            # Rate limit exceeded
            self._handle_rate_limit_violation(ip, endpoint_key)
            logger.warning(f"Rate limit exceeded for IP {ip} on {endpoint_key}: {current_requests}/{max_requests}")
            raise RateLimitError(f"Rate limit exceeded: {max_requests} requests per {window} seconds")
        
        # Record this request
        self.request_counts[ip].append(time.time())
        return True
    
    def _handle_rate_limit_violation(self, ip: str, endpoint: str):
        """Handle rate limit violation with progressive penalties"""
        suspicious = self.suspicious_ips[ip]
        suspicious['warnings'] += 1
        
        # Progressive blocking: longer blocks for repeat offenders
        if suspicious['warnings'] == 1:
            block_duration = 300  # 5 minutes
        elif suspicious['warnings'] == 2:
            block_duration = 900  # 15 minutes
        elif suspicious['warnings'] <= 5:
            block_duration = 3600  # 1 hour
        else:
            block_duration = 86400  # 24 hours for persistent offenders
        
        self.blocked_ips[ip] = time.time() + block_duration
        logger.error(f"IP {ip} blocked for {block_duration} seconds (warning #{suspicious['warnings']})")
    
    def record_failed_request(self, ip: str, error_type: str):
        """Record failed request for suspicious pattern detection"""
        suspicious = self.suspicious_ips[ip]
        suspicious['failed_requests'] += 1
        suspicious['last_failed'] = time.time()
        
        # Block IPs with too many failures
        if suspicious['failed_requests'] > 20:
            self.blocked_ips[ip] = time.time() + 3600  # 1 hour block
            logger.error(f"IP {ip} blocked due to {suspicious['failed_requests']} failed requests")

class DDoSProtection:
    """Advanced DDoS protection with pattern analysis"""
    
    def __init__(self):
        self.connection_counts: Dict[str, int] = defaultdict(int)
        self.request_patterns: Dict[str, Dict] = defaultdict(lambda: {
            'rapid_requests': deque(),
            'identical_requests': defaultdict(int),
            'user_agents': set(),
            'last_request_time': 0
        })
        
        # Thresholds
        self.max_connections_per_ip = 10
        self.rapid_request_threshold = 50  # requests in 10 seconds
        self.identical_request_threshold = 20
    
    def check_connection_limit(self, ip: str) -> bool:
        """Check if IP has too many concurrent connections"""
        if self.connection_counts[ip] > self.max_connections_per_ip:
            logger.warning(f"Connection limit exceeded for IP {ip}: {self.connection_counts[ip]}")
            return False
        return True
    
    def track_connection(self, ip: str):
        """Track new connection"""
        self.connection_counts[ip] += 1
    
    def release_connection(self, ip: str):
        """Release connection"""
        if self.connection_counts[ip] > 0:
            self.connection_counts[ip] -= 1
    
    def analyze_request_pattern(self, request: Request) -> bool:
        """Analyze request patterns for DDoS detection"""
        ip = request.client.host if request.client else "unknown"
        now = time.time()
        
        pattern = self.request_patterns[ip]
        
        # Track rapid requests
        pattern['rapid_requests'].append(now)
        while pattern['rapid_requests'] and pattern['rapid_requests'][0] < now - 10:
            pattern['rapid_requests'].popleft()
        
        if len(pattern['rapid_requests']) > self.rapid_request_threshold:
            logger.warning(f"Rapid request pattern detected from IP {ip}")
            return False
        
        # Track identical requests
        request_signature = f"{request.method}:{request.url.path}"
        pattern['identical_requests'][request_signature] += 1
        
        if pattern['identical_requests'][request_signature] > self.identical_request_threshold:
            logger.warning(f"Identical request spam detected from IP {ip}: {request_signature}")
            return False
        
        # Track user agents (basic bot detection)
        user_agent = request.headers.get('user-agent', 'unknown')
        pattern['user_agents'].add(user_agent)
        
        # Suspicious if too many different user agents from same IP
        if len(pattern['user_agents']) > 10:
            logger.warning(f"Multiple user agents from IP {ip}: possible bot activity")
        
        pattern['last_request_time'] = now
        return True

# Global instances
rate_limiter = RateLimiter()
ddos_protection = DDoSProtection()