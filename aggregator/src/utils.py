"""
Utility Functions
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional


def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def format_timestamp(dt: datetime = None) -> str:
    """Format datetime to ISO8601 string"""
    if dt is None:
        dt = datetime.utcnow()
    return dt.isoformat() + 'Z'


class EventMetrics:
    """Track event metrics with in-memory counters (atomic via asyncio)"""

    def __init__(self):
        self.start_time = datetime.utcnow()

    def get_uptime(self) -> int:
        """Get uptime in seconds"""
        return int((datetime.utcnow() - self.start_time).total_seconds())


def calculate_throughput(event_count: int, duration_seconds: float) -> float:
    """Calculate events per second"""
    if duration_seconds <= 0:
        return 0
    return event_count / duration_seconds
