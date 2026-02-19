"""
Centralized error handling middleware for qBTC-core
"""

import logging
import traceback
import uuid
import time
from typing import Union
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from errors.exceptions import (
    BlockchainError, RateLimitError, ValidationError as CustomValidationError,
    InsufficientFundsError, InvalidSignatureError
)

logger = logging.getLogger(__name__)

async def add_correlation_id_middleware(request: Request, call_next):
    """Add correlation ID to all requests for tracing"""
    correlation_id = str(uuid.uuid4())
    request.state.correlation_id = correlation_id
    
    # Add correlation ID and process request
    try:
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response
    except Exception as e:
        logger.error(f"Unhandled exception in request {correlation_id}: {str(e)}", exc_info=True)
        raise

def create_error_response(
    error_code: str,
    message: str,
    status_code: int = 400,
    correlation_id: str = None,
    details: dict = None
) -> JSONResponse:
    """Create standardized error response"""
    
    error_response = {
        "error": {
            "code": error_code,
            "message": message,
            "correlation_id": correlation_id,
            "timestamp": int(time.time() * 1000)
        }
    }
    
    if details:
        error_response["error"]["details"] = details
    
    return JSONResponse(
        status_code=status_code,
        content=error_response
    )

async def blockchain_error_handler(request: Request, exc: BlockchainError) -> JSONResponse:
    """Handle blockchain-specific errors"""
    correlation_id = getattr(request.state, 'correlation_id', None)
    
    logger.error(
        f"Blockchain error: {exc.message}",
        extra={
            "error_code": exc.code,
            "correlation_id": correlation_id,
            "endpoint": request.url.path
        }
    )
    
    # Map blockchain errors to HTTP status codes
    status_code_map = {
        "VALIDATION_ERROR": 400,
        "AUTH_ERROR": 401,
        "RATE_LIMIT_ERROR": 429,
        "NETWORK_ERROR": 503,
        "DATABASE_ERROR": 500,
        "BLOCKCHAIN_ERROR": 500
    }
    
    status_code = status_code_map.get(exc.code, 500)
    
    return create_error_response(
        error_code=exc.code,
        message=exc.message,
        status_code=status_code,
        correlation_id=correlation_id
    )

async def validation_error_handler(request: Request, exc: Union[RequestValidationError, ValidationError]) -> JSONResponse:
    """Handle Pydantic validation errors"""
    correlation_id = getattr(request.state, 'correlation_id', None)
    
    # Extract validation error details
    if isinstance(exc, RequestValidationError):
        errors = exc.errors()
        message = "Request validation failed"
    else:
        errors = exc.errors() if hasattr(exc, 'errors') else [{"msg": str(exc)}]
        message = "Data validation failed"
    
    logger.warning(
        f"Validation error: {message}",
        extra={
            "errors": errors,
            "correlation_id": correlation_id,
            "endpoint": request.url.path
        }
    )
    
    return create_error_response(
        error_code="VALIDATION_ERROR",
        message=message,
        status_code=422,
        correlation_id=correlation_id,
        details={"validation_errors": errors}
    )

async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle FastAPI HTTP exceptions"""
    correlation_id = getattr(request.state, 'correlation_id', None)
    
    logger.warning(
        f"HTTP exception: {exc.detail}",
        extra={
            "status_code": exc.status_code,
            "correlation_id": correlation_id,
            "endpoint": request.url.path
        }
    )
    
    return create_error_response(
        error_code="HTTP_ERROR",
        message=exc.detail,
        status_code=exc.status_code,
        correlation_id=correlation_id
    )

async def rate_limit_error_handler(request: Request, exc: RateLimitError) -> JSONResponse:
    """Handle rate limiting errors"""
    correlation_id = getattr(request.state, 'correlation_id', None)
    client_ip = request.client.host if request.client else "unknown"
    
    logger.warning(
        f"Rate limit exceeded for IP {client_ip}: {exc.message}",
        extra={
            "correlation_id": correlation_id,
            "client_ip": client_ip,
            "endpoint": request.url.path
        }
    )
    
    return create_error_response(
        error_code="RATE_LIMIT_EXCEEDED",
        message=exc.message,
        status_code=429,
        correlation_id=correlation_id,
        details={"retry_after": 60}  # Suggest retry after 60 seconds
    )

async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected exceptions"""
    correlation_id = getattr(request.state, 'correlation_id', None)
    
    logger.error(
        f"Unhandled exception: {str(exc)}",
        extra={
            "exception_type": type(exc).__name__,
            "correlation_id": correlation_id,
            "endpoint": request.url.path,
            "traceback": traceback.format_exc()
        }
    )
    
    # Don't expose internal errors in production
    message = "Internal server error"
    if logger.level == logging.DEBUG:
        message = f"Internal error: {str(exc)}"
    
    return create_error_response(
        error_code="INTERNAL_ERROR",
        message=message,
        status_code=500,
        correlation_id=correlation_id
    )

def setup_error_handlers(app):
    """Setup all error handlers for FastAPI app"""
    
    # REMOVED correlation ID middleware to prevent header conflicts
    # The correlation ID middleware was potentially interfering with CORS
    # app.middleware("http")(add_correlation_id_middleware)
    
    # Add exception handlers
    # Handle our custom validation errors with blockchain handler
    app.add_exception_handler(CustomValidationError, blockchain_error_handler)
    app.add_exception_handler(InsufficientFundsError, blockchain_error_handler)
    app.add_exception_handler(InvalidSignatureError, blockchain_error_handler)
    app.add_exception_handler(BlockchainError, blockchain_error_handler)
    
    # Handle Pydantic validation errors
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(ValidationError, validation_error_handler)
    
    # Handle other errors
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RateLimitError, rate_limit_error_handler)
    app.add_exception_handler(Exception, generic_exception_handler)