"""
Integrated security middleware combining rate limiting, DDoS protection, and monitoring
"""

import time
import logging
import os
from typing import Dict, Any, Optional
from fastapi import Request
from fastapi.responses import JSONResponse

from .advanced_rate_limiter import advanced_rate_limiter, ThreatLevel
from .rate_limiter import ddos_protection
from network.peer_reputation import peer_reputation_manager
from errors.exceptions import RateLimitError

logger = logging.getLogger(__name__)

class SecurityMetrics:
    """Track security metrics for monitoring"""
    
    def __init__(self):
        self.reset_time = time.time()
        self.total_requests = 0
        self.blocked_requests = 0
        self.rate_limited_requests = 0
        self.suspicious_requests = 0
        self.attack_attempts = 0
        
        # Attack pattern detection
        self.request_patterns: Dict[str, int] = {}
        self.suspicious_ips: set = set()
    
    def record_request(self, client_ip: str, endpoint: str, status: str):
        """Record request for metrics"""
        self.total_requests += 1
        
        if status == "blocked":
            self.blocked_requests += 1
        elif status == "rate_limited":
            self.rate_limited_requests += 1
        elif status == "suspicious":
            self.suspicious_requests += 1
            self.suspicious_ips.add(client_ip)
        elif status == "attack":
            self.attack_attempts += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current security metrics"""
        uptime = time.time() - self.reset_time
        
        return {
            "uptime": uptime,
            "total_requests": self.total_requests,
            "blocked_requests": self.blocked_requests,
            "rate_limited_requests": self.rate_limited_requests,
            "suspicious_requests": self.suspicious_requests,
            "attack_attempts": self.attack_attempts,
            "requests_per_second": self.total_requests / max(uptime, 1),
            "block_rate": self.blocked_requests / max(self.total_requests, 1),
            "suspicious_ips_count": len(self.suspicious_ips)
        }
    
    def reset_metrics(self):
        """Reset metrics counters"""
        self.__init__()

# Global metrics instance
security_metrics = SecurityMetrics()

class IntegratedSecurityMiddleware:
    """Comprehensive security middleware"""
    
    def __init__(self):
        self.attack_patterns = {
            # Common attack signatures
            'sql_injection': [
                'union select', 'drop table', 'insert into',
                '1=1', '1\'=\'1', 'or 1=1'
            ],
            'xss': [
                '<script', 'javascript:', 'onerror=',
                'onload=', 'alert(', 'document.cookie'
            ],
            'directory_traversal': [
                '../', '..\\', '/etc/passwd', '/etc/shadow',
                'c:\\windows', 'boot.ini'
            ],
            'command_injection': [
                '; cat', '| cat', '&& cat', '`cat',
                '; ls', '| ls', '&& ls', '`ls'
            ]
        }
        
        # Mining-related whitelisted user agents
        self.mining_user_agents = [
            'cpuminer',
            'cgminer',
            'bfgminer',
            'sgminer',
            'ccminer',
            'xmrig',
            'mining'
        ]
        
        # Mining-related whitelisted endpoints
        self.mining_endpoints = [
            '/',  # RPC endpoint
            '/rpc',
            '/api/rpc'
        ]
        
        # Wallet-related whitelisted endpoints (should have relaxed rate limiting)
        self.wallet_endpoints = [
            '/balance/',
            '/transactions/',
            '/utxos/',
            '/health',  # Also whitelist health endpoint
            '/ws'  # WebSocket endpoint
        ]
    
    def _get_client_info(self, request: Request) -> Dict[str, str]:
        """Extract client information from request"""
        # Get real IP considering proxies
        forwarded_for = request.headers.get('x-forwarded-for')
        if forwarded_for:
            client_ip = forwarded_for.split(',')[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"
        
        return {
            'ip': client_ip,
            'user_agent': request.headers.get('user-agent', 'unknown'),
            'referer': request.headers.get('referer', ''),
            'method': request.method,
            'path': request.url.path,
            'query': str(request.query_params)
        }
    
    def _detect_attack_patterns(self, request: Request) -> Optional[str]:
        """Detect common attack patterns in request"""
        # Check URL path and query parameters
        full_url = str(request.url).lower()
        
        for attack_type, patterns in self.attack_patterns.items():
            for pattern in patterns:
                if pattern in full_url:
                    return f"{attack_type}: {pattern}"
        
        # Check for suspicious headers
        user_agent = request.headers.get('user-agent', '').lower()
        suspicious_agents = [
            'sqlmap', 'nikto', 'nmap', 'masscan',
            'gobuster', 'dirb', 'dirbuster', 'burp'
        ]
        
        for agent in suspicious_agents:
            if agent in user_agent:
                return f"suspicious_tool: {agent}"
        
        return None
    
    def _is_mining_request(self, request: Request) -> bool:
        """Check if request is from a mining client"""
        user_agent = request.headers.get('user-agent', '').lower()
        path = request.url.path
        
        # Check if it's a mining user agent
        for mining_agent in self.mining_user_agents:
            if mining_agent in user_agent:
                return True
        
        # Check if it's a mining endpoint with mining-related RPC methods
        if path in self.mining_endpoints:
            # For POST requests, check the method
            return True  # We'll check the actual method in the main handler
        
        return False
    
    def _is_wallet_request(self, request: Request) -> bool:
        """Check if request is for wallet-related endpoints that should have relaxed rate limiting"""
        path = request.url.path
        
        # Check if path starts with any whitelisted wallet endpoint
        for endpoint in self.wallet_endpoints:
            if path.startswith(endpoint):
                return True
        
        # Also check for specific patterns (e.g., /balance/{address})
        wallet_patterns = ['/balance/', '/transactions/', '/utxos/']
        for pattern in wallet_patterns:
            if pattern in path:
                return True
                
        return False
    
    def _is_automated_request(self, request: Request) -> bool:
        """Detect if request is from automated tool/bot"""
        user_agent = request.headers.get('user-agent', '').lower()
        
        # Common bot indicators
        bot_indicators = [
            'bot', 'crawler', 'spider', 'scraper',
            'curl', 'wget', 'python-requests', 'axios',
            'postman', 'insomnia'
        ]
        
        # Check for missing common headers
        missing_headers = 0
        expected_headers = ['accept', 'accept-language', 'accept-encoding']
        for header in expected_headers:
            if header not in request.headers:
                missing_headers += 1
        
        return (any(indicator in user_agent for indicator in bot_indicators) or
                missing_headers >= 2 or
                user_agent == 'unknown')
    
    async def __call__(self, request: Request, call_next):
        """Main security middleware function"""
        start_time = time.time()
        client_info = self._get_client_info(request)
        client_ip = client_info['ip']
        
        # Check if security features are disabled via environment variables
        rate_limit_enabled = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
        ddos_protection_enabled = os.getenv("DDOS_PROTECTION_ENABLED", "true").lower() == "true"
        attack_pattern_detection = os.getenv("ATTACK_PATTERN_DETECTION", "true").lower() == "true"
        bot_detection_enabled = os.getenv("BOT_DETECTION_ENABLED", "true").lower() == "true"
        
        # If all security features are disabled, bypass all checks (for testing)
        if not (rate_limit_enabled or ddos_protection_enabled or attack_pattern_detection or bot_detection_enabled):
            logger.info(f"TESTING MODE: All security checks bypassed for {client_ip}")
            response = await call_next(request)
            return response
        
        # Security response headers
        security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY", 
            "X-XSS-Protection": "1; mode=block",
            "X-Robots-Tag": "noindex, nofollow",
            "Referrer-Policy": "strict-origin-when-cross-origin"
        }
        
        try:
            # Check if this is a mining request
            is_mining = self._is_mining_request(request)
            
            if is_mining:
                logger.debug(f"Mining request detected from {client_ip}")
                # For mining requests, skip most security checks but still apply basic rate limiting
                # This allows miners to make frequent requests without being blocked
                
                # Only check basic rate limit for mining (much higher threshold)
                # Skip attack pattern detection, bot detection, and strict rate limiting
                response = await call_next(request)
                process_time = time.time() - start_time
                
                # Add security headers
                for header, value in security_headers.items():
                    response.headers[header] = value
                
                response.headers["X-Process-Time"] = str(process_time)
                security_metrics.record_request(client_ip, request.url.path, "allowed")
                
                return response
            
            # Check if this is a wallet-related request
            is_wallet = self._is_wallet_request(request)
            
            if is_wallet:
                logger.debug(f"Wallet request detected from {client_ip} to {request.url.path}")
                # For wallet requests, skip aggressive security checks
                # These are legitimate API endpoints that wallets need to access
                
                # Skip attack pattern detection and bot detection for wallet endpoints
                # Still apply basic DDoS protection but with more lenient limits
                response = await call_next(request)
                process_time = time.time() - start_time
                
                # Add security headers
                for header, value in security_headers.items():
                    response.headers[header] = value
                
                response.headers["X-Process-Time"] = str(process_time)
                security_metrics.record_request(client_ip, request.url.path, "allowed")
                
                return response
            
            # 1. Attack pattern detection (non-mining requests)
            attack_pattern = self._detect_attack_patterns(request)
            if attack_pattern:
                logger.error(f"Attack pattern detected from {client_ip}: {attack_pattern}")
                security_metrics.record_request(client_ip, request.url.path, "attack")
                
                # Immediate ban for attack attempts
                await advanced_rate_limiter.record_failed_request(request, "attack_pattern")
                
                return JSONResponse(
                    status_code=403,
                    content={"error": "Forbidden", "message": "Request blocked"},
                    headers=security_headers
                )
            
            # 2. Check DDoS protection
            ddos_protection.track_connection(client_ip)
            
            if not ddos_protection.check_connection_limit(client_ip):
                logger.warning(f"Connection limit exceeded for {client_ip}")
                security_metrics.record_request(client_ip, request.url.path, "blocked")
                
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Connections", "message": "Connection limit exceeded"},
                    headers=security_headers
                )
            
            if not ddos_protection.analyze_request_pattern(request):
                logger.warning(f"Suspicious request pattern from {client_ip}")
                security_metrics.record_request(client_ip, request.url.path, "suspicious")
                
                await advanced_rate_limiter.record_failed_request(request, "suspicious_pattern")
                
                return JSONResponse(
                    status_code=429,
                    content={"error": "Suspicious Pattern", "message": "Request pattern blocked"},
                    headers=security_headers
                )
            
            # 3. Advanced rate limiting
            try:
                await advanced_rate_limiter.check_rate_limit(request)
            except RateLimitError as e:
                logger.warning(f"Rate limit exceeded for {client_ip}: {str(e)}")
                security_metrics.record_request(client_ip, request.url.path, "rate_limited")
                
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": "Rate Limit Exceeded",
                        "message": str(e),
                        "retry_after": 60
                    },
                    headers={**security_headers, "Retry-After": "60"}
                )
            
            # 4. Bot detection and handling
            if self._is_automated_request(request):
                logger.info(f"Automated request detected from {client_ip}")
                
                # Apply stricter rate limiting for bots
                # (This is handled internally by the advanced rate limiter)
            
            # 5. Process request
            response = await call_next(request)
            
            # 6. Post-request processing
            process_time = time.time() - start_time
            
            # Log successful request
            logger.debug(
                f"Request processed: {client_info['method']} {client_info['path']} "
                f"from {client_ip} in {process_time:.3f}s"
            )
            
            security_metrics.record_request(client_ip, request.url.path, "success")
            
            # Add security headers to response
            for header, value in security_headers.items():
                response.headers[header] = value
            
            # Add performance headers
            response.headers["X-Response-Time"] = f"{process_time:.3f}s"
            
            return response
            
        except RateLimitError:
            # Already handled above
            raise
            
        except Exception as e:
            # Record failed request
            logger.error(f"Security middleware error for {client_ip}: {str(e)}")
            security_metrics.record_request(client_ip, request.url.path, "error")
            
            await advanced_rate_limiter.record_failed_request(request, "middleware_error")
            
            return JSONResponse(
                status_code=500,
                content={"error": "Internal Error", "message": "Request processing failed"},
                headers=security_headers
            )
        
        finally:
            # Always release connection
            ddos_protection.release_connection(client_ip)

# Create middleware instance
integrated_security_middleware = IntegratedSecurityMiddleware()

# Security management API functions
async def get_security_status() -> Dict[str, Any]:
    """Get comprehensive security status"""
    metrics = security_metrics.get_metrics()
    
    # Get blocked clients
    blocked_clients = await advanced_rate_limiter.get_all_blocked_clients()
    
    # Get peer reputation summary
    peer_summary = peer_reputation_manager.get_reputation_summary()
    
    return {
        "status": "active",
        "metrics": metrics,
        "blocked_clients": len(blocked_clients),
        "peer_reputation": peer_summary,
        "threat_levels": {
            "low": len([c for c in blocked_clients if c.get('threat_level') == ThreatLevel.LOW.value]),
            "medium": len([c for c in blocked_clients if c.get('threat_level') == ThreatLevel.MEDIUM.value]),
            "high": len([c for c in blocked_clients if c.get('threat_level') == ThreatLevel.HIGH.value]),
            "critical": len([c for c in blocked_clients if c.get('threat_level') == ThreatLevel.CRITICAL.value])
        }
    }

async def unblock_client(client_ip: str) -> bool:
    """Unblock a specific client"""
    return await advanced_rate_limiter.unblock_client(client_ip)

async def get_client_info(client_ip: str) -> Optional[Dict]:
    """Get information about a specific client"""
    return await advanced_rate_limiter.get_client_stats(client_ip)

def reset_security_metrics():
    """Reset security metrics"""
    security_metrics.reset_metrics()
    logger.info("Security metrics reset")