"""Performance monitoring utilities for the Telegram Quiz Bot.

This module provides performance monitoring and optimization tools to track
response times, identify bottlenecks, and ensure optimal bot performance.
"""

import time
import logging
import asyncio
from functools import wraps
from typing import Callable, Any
from collections import defaultdict, deque
from threading import Lock

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """Monitor and track performance metrics for the bot."""
    
    def __init__(self):
        self.metrics = defaultdict(list)
        self.lock = Lock()
        self.max_metrics = 1000  # Keep last 1000 measurements per metric
    
    def record_metric(self, metric_name: str, value: float, unit: str = "ms"):
        """Record a performance metric.
        
        Args:
            metric_name: Name of the metric (e.g., 'quiz_response_time')
            value: Metric value
            unit: Unit of measurement
        """
        with self.lock:
            self.metrics[metric_name].append({
                'value': value,
                'unit': unit,
                'timestamp': time.time()
            })
            
            # Keep only the most recent metrics
            if len(self.metrics[metric_name]) > self.max_metrics:
                self.metrics[metric_name] = self.metrics[metric_name][-self.max_metrics:]
    
    def get_average_metric(self, metric_name: str, last_n: int = 100) -> float:
        """Get average value for a metric over the last N measurements.
        
        Args:
            metric_name: Name of the metric
            last_n: Number of recent measurements to average
            
        Returns:
            Average value or 0 if no data
        """
        with self.lock:
            if metric_name not in self.metrics or not self.metrics[metric_name]:
                return 0.0
            
            recent_metrics = self.metrics[metric_name][-last_n:]
            return sum(m['value'] for m in recent_metrics) / len(recent_metrics)
    
    def get_metric_stats(self, metric_name: str, last_n: int = 100) -> dict:
        """Get comprehensive statistics for a metric.
        
        Args:
            metric_name: Name of the metric
            last_n: Number of recent measurements to analyze
            
        Returns:
            Dictionary with min, max, avg, and count
        """
        with self.lock:
            if metric_name not in self.metrics or not self.metrics[metric_name]:
                return {'min': 0, 'max': 0, 'avg': 0, 'count': 0}
            
            recent_metrics = self.metrics[metric_name][-last_n:]
            values = [m['value'] for m in recent_metrics]
            
            return {
                'min': min(values),
                'max': max(values),
                'avg': sum(values) / len(values),
                'count': len(values)
            }

# Global performance monitor instance
performance_monitor = PerformanceMonitor()

def measure_performance(metric_name: str, unit: str = "ms"):
    """Decorator to measure function execution time.
    
    Args:
        metric_name: Name for the performance metric
        unit: Unit of measurement
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
                performance_monitor.record_metric(metric_name, execution_time, unit)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
                performance_monitor.record_metric(metric_name, execution_time, unit)
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

def log_performance_summary():
    """Log a summary of performance metrics."""
    logger.info("=== PERFORMANCE SUMMARY ===")
    
    for metric_name in performance_monitor.metrics:
        stats = performance_monitor.get_metric_stats(metric_name)
        if stats['count'] > 0:
            logger.info(
                f"{metric_name}: "
                f"avg={stats['avg']:.2f}ms, "
                f"min={stats['min']:.2f}ms, "
                f"max={stats['max']:.2f}ms, "
                f"count={stats['count']}"
            )
