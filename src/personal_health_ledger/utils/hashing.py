"""
Hashing and record ID generation utilities.

Provides deterministic record ID generation based on configurable fields.
"""

import hashlib
from datetime import datetime

from personal_health_ledger.domain.weight import SourceType
from personal_health_ledger.utils.parameters import RecordIDConfig


def round_timestamp(timestamp: datetime, rounding_seconds: int) -> datetime:
    """
    Round timestamp to nearest N seconds.

    Args:
        timestamp: Timestamp to round.
        rounding_seconds: Rounding interval in seconds.

    Returns:
        Rounded timestamp.
    """
    total_seconds = int(timestamp.timestamp())
    rounded_seconds = (total_seconds // rounding_seconds) * rounding_seconds
    return datetime.fromtimestamp(rounded_seconds, tz=timestamp.tzinfo)


def generate_record_id(
    timestamp: datetime,
    weight_kg: float,
    source_types: set[SourceType],
    config: RecordIDConfig,
) -> str:
    """
    Generate deterministic record ID based on configuration.

    Args:
        timestamp: Measurement timestamp.
        weight_kg: Weight in kilograms.
        source_types: Set of source types.
        config: Record ID generation configuration.

    Returns:
        Deterministic record ID (hex string).
    """
    rounded_ts = round_timestamp(timestamp, config.timestamp_rounding_seconds)

    hash_data: list[str] = []

    if "timestamp" in config.include_fields:
        hash_data.append(rounded_ts.isoformat())

    if "weight_kg" in config.include_fields:
        hash_data.append(f"{weight_kg:.3f}")

    if "source_types" in config.include_fields:
        # Handle both enum objects and string values (due to use_enum_values=True)
        sorted_types = sorted([st.value if hasattr(st, 'value') else st for st in source_types])
        hash_data.append(",".join(sorted_types))

    hash_string = "|".join(hash_data)

    hash_func = hashlib.new(config.algorithm)
    hash_func.update(hash_string.encode("utf-8"))

    return hash_func.hexdigest()


def compute_file_hash(file_path: str, algorithm: str = "md5") -> str:
    """
    Compute hash of a file.

    Args:
        file_path: Path to the file.
        algorithm: Hash algorithm to use.

    Returns:
        Hex string of the file hash.
    """
    hash_func = hashlib.new(algorithm)

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)

    return hash_func.hexdigest()
