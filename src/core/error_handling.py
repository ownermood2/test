from typing import Optional, Any, Callable
import logging
import functools
import asyncio
from datetime import datetime
from telegram.error import TelegramError, NetworkError, RetryAfter

logger = logging.getLogger(__name__)

class BotError(Exception):
    """Base exception for bot errors"""
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error
        self.timestamp = datetime.utcnow()

def handle_telegram_errors(max_retries: int = 3, initial_delay: float = 1.0):
    """Decorator for handling Telegram API errors with exponential backoff"""
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except RetryAfter as e:
                    delay = float(e.retry_after)
                    logger.warning(f"Rate limit hit, waiting {delay} seconds")
                    await asyncio.sleep(delay)
                except NetworkError as e:
                    delay = initial_delay * (2 ** attempt)
                    logger.error(f"Network error (attempt {attempt + 1}/{max_retries}): {e}")
                    last_error = e
                    await asyncio.sleep(delay)
                except TelegramError as e:
                    logger.error(f"Telegram API error: {e}")
                    raise BotError(f"Telegram API error: {str(e)}", original_error=e)
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    raise BotError(f"Unexpected error: {str(e)}", original_error=e)
            
            if last_error:
                raise BotError(f"Failed after {max_retries} retries", original_error=last_error)
        return wrapper
    return decorator

def setup_error_handlers(app):
    """Setup global error handlers for the Flask app"""
    @app.errorhandler(Exception)
    def handle_exception(e):
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return {"error": "Internal server error"}, 500

    @app.errorhandler(404)
    def handle_not_found(e):
        return {"error": "Resource not found"}, 404

    @app.errorhandler(400)
    def handle_bad_request(e):
        return {"error": str(e)}, 400

class CircuitBreaker:
    """Circuit breaker for external service calls"""
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = None
        self._state = "closed"  # closed, open, half-open

    @property
    def state(self) -> str:
        if self._state == "open" and self.should_reset():
            self._state = "half-open"
        return self._state

    def should_reset(self) -> bool:
        if not self.last_failure_time:
            return False
        return (datetime.utcnow() - self.last_failure_time).total_seconds() >= self.reset_timeout

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = datetime.utcnow()
        if self.failures >= self.failure_threshold:
            self._state = "open"

    def record_success(self):
        self.failures = 0
        self._state = "closed"

    def can_execute(self) -> bool:
        return self.state in ["closed", "half-open"]