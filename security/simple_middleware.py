"""
Simple middleware for testing - minimal security checks without blocking
"""
import logging
from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

async def simple_security_middleware(request: Request, call_next):
    """Simple middleware that logs requests but doesn't block"""
    logger.info(f"Request received: {request.method} {request.url.path}")
    
    # Basic security headers
    security_headers = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block"
    }
    
    try:
        # Process the request
        response = await call_next(request)
        
        # Add security headers to response
        for header, value in security_headers.items():
            response.headers[header] = value
            
        return response
        
    except Exception as e:
        logger.error(f"Middleware error: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error"},
            headers=security_headers
        )