"""
Timezone and datetime utilities.

Provides utilities for handling timezone-aware datetime operations.
"""

from datetime import datetime

import pytz
from dateutil import parser


def make_timezone_aware(
    dt: datetime, timezone_str: str = "America/Santiago", assume_local: bool = False
) -> datetime:
    """
    Make a datetime object timezone-aware.

    Args:
        dt: Datetime object (may be naive or aware).
        timezone_str: Timezone string (e.g., "America/Santiago").
        assume_local: If True and dt is naive, assume it's in timezone_str.

    Returns:
        Timezone-aware datetime object.
    """
    tz = pytz.timezone(timezone_str)

    if dt.tzinfo is None:
        if assume_local:
            return tz.localize(dt)
        else:
            return pytz.utc.localize(dt).astimezone(tz)
    else:
        return dt.astimezone(tz)


def parse_datetime(
    date_str: str, time_str: str | None = None, timezone_str: str = "America/Santiago"
) -> datetime:
    """
    Parse date and optional time strings into timezone-aware datetime.

    Args:
        date_str: Date string (various formats supported).
        time_str: Optional time string.
        timezone_str: Timezone to assign to the parsed datetime.

    Returns:
        Timezone-aware datetime object.
    """
    if time_str:
        combined = f"{date_str} {time_str}"
    else:
        combined = date_str

    dt = parser.parse(combined)

    return make_timezone_aware(dt, timezone_str, assume_local=True)


def timestamps_match(
    ts1: datetime, ts2: datetime, tolerance_seconds: int = 60
) -> bool:
    """
    Check if two timestamps match within a tolerance.

    Args:
        ts1: First timestamp.
        ts2: Second timestamp.
        tolerance_seconds: Tolerance in seconds.

    Returns:
        True if timestamps are within tolerance, False otherwise.
    """
    diff = abs((ts1 - ts2).total_seconds())
    return diff <= tolerance_seconds
