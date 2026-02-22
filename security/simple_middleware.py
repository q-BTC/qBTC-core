"""
Simple middleware for testing - minimal security checks without blocking
"""
import logging
from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

async def simple_security_middleware(request: Request, call_next):
    """Simple middleware that logs requests with security context"""
    client_ip = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    logger.info(f"Request: {request.method} {request.url.path} from {client_ip} UA={user_agent}")

    # Basic security headers
    security_headers = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block"
    }

    try:
        # Process the request
        response = await call_next(request)

        # M8: Log response status for security auditing
        if response.status_code >= 400:
            logger.warning(f"Response {response.status_code} for {request.method} {request.url.path} from {client_ip}")

        # Add security headers to response
        for header, value in security_headers.items():
            response.headers[header] = value

        return response

    except Exception as e:
        logger.error(f"Middleware error: {e} — client={client_ip} path={request.url.path}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error"},
            headers=security_headers
        )