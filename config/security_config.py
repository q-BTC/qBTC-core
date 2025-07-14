"""
Security configuration for qBTC-core
"""

import os
from typing import Dict, Any

class SecurityConfig:
    """Security system configuration"""
    
    def __init__(self):
        # Rate limiting configuration
        self.RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
        self.REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.USE_REDIS = os.getenv("USE_REDIS", "true").lower() == "true"
        
        # Rate limit rules (requests per minute)
        self.RATE_LIMITS = {
            "/worker": {
                "max_requests": int(os.getenv("RATE_LIMIT_WORKER", "10")),
                "window_seconds": 60,
                "burst_limit": 3
            },
            "/balance": {
                "max_requests": int(os.getenv("RATE_LIMIT_BALANCE", "100")),
                "window_seconds": 60,
                "burst_limit": 20
            },
            "/transactions": {
                "max_requests": int(os.getenv("RATE_LIMIT_TRANSACTIONS", "100")),
                "window_seconds": 60,
                "burst_limit": 20
            },
            "/health": {
                "max_requests": int(os.getenv("RATE_LIMIT_HEALTH", "100")),
                "window_seconds": 60,
                "burst_limit": 20
            },
            "/utxos": {
                "max_requests": int(os.getenv("RATE_LIMIT_UTXOS", "100")),
                "window_seconds": 60,
                "burst_limit": 20
            },
            "default": {
                "max_requests": int(os.getenv("RATE_LIMIT_DEFAULT", "60")),
                "window_seconds": 60,
                "burst_limit": 15
            }
        }
        
        # DDoS protection
        self.DDOS_PROTECTION_ENABLED = os.getenv("DDOS_PROTECTION_ENABLED", "true").lower() == "true"
        self.MAX_CONNECTIONS_PER_IP = int(os.getenv("MAX_CONNECTIONS_PER_IP", "10"))
        self.RAPID_REQUEST_THRESHOLD = int(os.getenv("RAPID_REQUEST_THRESHOLD", "50"))
        self.IDENTICAL_REQUEST_THRESHOLD = int(os.getenv("IDENTICAL_REQUEST_THRESHOLD", "20"))
        
        # Threat level adjustments
        self.THREAT_MULTIPLIERS = {
            "low": float(os.getenv("THREAT_MULTIPLIER_LOW", "1.0")),
            "medium": float(os.getenv("THREAT_MULTIPLIER_MEDIUM", "0.7")),
            "high": float(os.getenv("THREAT_MULTIPLIER_HIGH", "0.4")),
            "critical": float(os.getenv("THREAT_MULTIPLIER_CRITICAL", "0.1"))
        }
        
        # Blocking durations (seconds)
        self.BLOCK_DURATIONS = {
            "first_warning": int(os.getenv("BLOCK_DURATION_FIRST", "300")),    # 5 minutes
            "second_warning": int(os.getenv("BLOCK_DURATION_SECOND", "900")),  # 15 minutes
            "repeated_warnings": int(os.getenv("BLOCK_DURATION_REPEATED", "3600")),  # 1 hour
            "persistent_offender": int(os.getenv("BLOCK_DURATION_PERSISTENT", "86400"))  # 24 hours
        }
        
        # Peer reputation configuration
        self.PEER_REPUTATION_ENABLED = os.getenv("PEER_REPUTATION_ENABLED", "true").lower() == "true"
        self.PEER_REPUTATION_THRESHOLDS = {
            "good": float(os.getenv("PEER_THRESHOLD_GOOD", "70.0")),
            "suspicious": float(os.getenv("PEER_THRESHOLD_SUSPICIOUS", "40.0")),
            "malicious": float(os.getenv("PEER_THRESHOLD_MALICIOUS", "20.0")),
            "banned": float(os.getenv("PEER_THRESHOLD_BANNED", "0.0"))
        }
        
        # Security monitoring
        self.SECURITY_LOGGING_ENABLED = os.getenv("SECURITY_LOGGING_ENABLED", "true").lower() == "true"
        self.LOG_FAILED_REQUESTS = os.getenv("LOG_FAILED_REQUESTS", "true").lower() == "true"
        self.LOG_SUSPICIOUS_PATTERNS = os.getenv("LOG_SUSPICIOUS_PATTERNS", "true").lower() == "true"
        
        # Admin endpoints
        self.ADMIN_ENDPOINTS_ENABLED = os.getenv("ADMIN_ENDPOINTS_ENABLED", "true").lower() == "true"
        self.ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")  # Should be set in production
        
        # Security headers
        self.SECURITY_HEADERS = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "X-Robots-Tag": "noindex, nofollow",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains"
        }
        
        # Attack pattern detection
        self.ATTACK_PATTERN_DETECTION = os.getenv("ATTACK_PATTERN_DETECTION", "true").lower() == "true"
        self.ATTACK_PATTERNS = {
            'sql_injection': [
                'union select', 'drop table', 'insert into',
                '1=1', "1'='1", 'or 1=1', '; select', '/*', '*/', '--'
            ],
            'xss': [
                '<script', 'javascript:', 'onerror=',
                'onload=', 'alert(', 'document.cookie', '<img src=x'
            ],
            'directory_traversal': [
                '../', '..\\', '/etc/passwd', '/etc/shadow',
                'c:\\windows', 'boot.ini', '/proc/version'
            ],
            'command_injection': [
                '; cat', '| cat', '&& cat', '`cat',
                '; ls', '| ls', '&& ls', '`ls', '$(', '${', '|&'
            ],
            'nosql_injection': [
                '$where', '$ne', '$gt', '$lt', '$regex',
                '{"$where"', '{"$ne"', 'db.eval'
            ]
        }
        
        # Bot detection
        self.BOT_DETECTION_ENABLED = os.getenv("BOT_DETECTION_ENABLED", "true").lower() == "true"
        self.BOT_USER_AGENTS = [
            'bot', 'crawler', 'spider', 'scraper',
            'curl', 'wget', 'python-requests', 'axios',
            'postman', 'insomnia', 'httpie'
        ]
        self.SUSPICIOUS_TOOLS = [
            'sqlmap', 'nikto', 'nmap', 'masscan',
            'gobuster', 'dirb', 'dirbuster', 'burp',
            'owasp-zap', 'w3af', 'skipfish'
        ]
    
    def get_rate_limit_for_endpoint(self, endpoint: str) -> Dict[str, Any]:
        """Get rate limit configuration for specific endpoint"""
        for pattern, config in self.RATE_LIMITS.items():
            if endpoint.startswith(pattern.rstrip('*')):
                return config
        return self.RATE_LIMITS["default"]
    
    def is_admin_enabled(self) -> bool:
        """Check if admin endpoints are enabled and configured"""
        return self.ADMIN_ENDPOINTS_ENABLED and bool(self.ADMIN_API_KEY)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "rate_limiting": {
                "enabled": self.RATE_LIMIT_ENABLED,
                "use_redis": self.USE_REDIS,
                "rules": self.RATE_LIMITS,
                "threat_multipliers": self.THREAT_MULTIPLIERS,
                "block_durations": self.BLOCK_DURATIONS
            },
            "ddos_protection": {
                "enabled": self.DDOS_PROTECTION_ENABLED,
                "max_connections_per_ip": self.MAX_CONNECTIONS_PER_IP,
                "rapid_request_threshold": self.RAPID_REQUEST_THRESHOLD,
                "identical_request_threshold": self.IDENTICAL_REQUEST_THRESHOLD
            },
            "peer_reputation": {
                "enabled": self.PEER_REPUTATION_ENABLED,
                "thresholds": self.PEER_REPUTATION_THRESHOLDS
            },
            "monitoring": {
                "security_logging": self.SECURITY_LOGGING_ENABLED,
                "log_failed_requests": self.LOG_FAILED_REQUESTS,
                "log_suspicious_patterns": self.LOG_SUSPICIOUS_PATTERNS
            },
            "attack_detection": {
                "enabled": self.ATTACK_PATTERN_DETECTION,
                "bot_detection": self.BOT_DETECTION_ENABLED
            },
            "admin": {
                "endpoints_enabled": self.ADMIN_ENDPOINTS_ENABLED,
                "api_key_configured": bool(self.ADMIN_API_KEY)
            }
        }

# Global configuration instance
security_config = SecurityConfig()