"""Unit tests for CSV parser."""


import pandas as pd

from personal_health_ledger.infrastructure.parsers.csv_parser import CSVParser
from personal_health_ledger.utils.parameters import (
    ConflictResolutionConfig,
    CSVConfig,
    ProcessingConfig,
    RecordIDConfig,
)


def test_normalize_spanish_columns() -> None:
    """Test normalization of Spanish column names."""
    csv_config = CSVConfig(
        encodings=["utf-8"],
        delimiters=[","],
        column_mappings={
            "Fecha": "date",
            "Hora": "time",
            "Peso": "weight_kg",
            "Porcentaje de grasa corporal": "body_fat_pct",
        },
    )
    processing_config = ProcessingConfig(
        timezone="America/Santiago",
        timestamp_tolerance_seconds=60,
        numeric_tolerance=0.001,
        record_id=RecordIDConfig(
            algorithm="sha256",
            timestamp_rounding_seconds=60,
            include_fields=["timestamp", "weight_kg", "source_types"],
        ),
        conflict_resolution=ConflictResolutionConfig(
            default_preference=None, field_preferences={}
        ),
    )

    parser = CSVParser(csv_config, processing_config)

    df = pd.DataFrame(
        {
            "Fecha": ["2024-01-15"],
            "Hora": ["10:30:00"],
            "Peso": [75.5],
            "Porcentaje de grasa corporal": [18.2],
        }
    )

    normalized_df = parser._normalize_column_names(df)

    if "date" not in normalized_df.columns:
        raise AssertionError("Expected 'date' column after normalization")
    if "time" not in normalized_df.columns:
        raise AssertionError("Expected 'time' column after normalization")
    if "weight_kg" not in normalized_df.columns:
        raise AssertionError("Expected 'weight_kg' column after normalization")
    if "body_fat_pct" not in normalized_df.columns:
        raise AssertionError("Expected 'body_fat_pct' column after normalization")


def test_safe_float_conversion_comma_decimal() -> None:
    """Test safe float conversion with comma as decimal separator."""
    csv_config = CSVConfig(
        encodings=["utf-8"], delimiters=[","], column_mappings={}
    )
    processing_config = ProcessingConfig(
        timezone="America/Santiago",
        timestamp_tolerance_seconds=60,
        numeric_tolerance=0.001,
        record_id=RecordIDConfig(
            algorithm="sha256",
            timestamp_rounding_seconds=60,
            include_fields=["timestamp", "weight_kg", "source_types"],
        ),
        conflict_resolution=ConflictResolutionConfig(
            default_preference=None, field_preferences={}
        ),
    )

    parser = CSVParser(csv_config, processing_config)

    result = parser._safe_float_conversion("75,5")
    if result != 75.5:
        raise AssertionError(f"Expected 75.5, got {result}")

    result = parser._safe_float_conversion("18,23")
    if result != 18.23:
        raise AssertionError(f"Expected 18.23, got {result}")

    result = parser._safe_float_conversion("invalid")
    if result is not None:
        raise AssertionError(f"Expected None for invalid input, got {result}")


def test_safe_float_conversion_existing_float() -> None:
    """Test safe float conversion with existing float values."""
    csv_config = CSVConfig(
        encodings=["utf-8"], delimiters=[","], column_mappings={}
    )
    processing_config = ProcessingConfig(
        timezone="America/Santiago",
        timestamp_tolerance_seconds=60,
        numeric_tolerance=0.001,
        record_id=RecordIDConfig(
            algorithm="sha256",
            timestamp_rounding_seconds=60,
            include_fields=["timestamp", "weight_kg", "source_types"],
        ),
        conflict_resolution=ConflictResolutionConfig(
            default_preference=None, field_preferences={}
        ),
    )

    parser = CSVParser(csv_config, processing_config)

    result = parser._safe_float_conversion(75.5)
    if result != 75.5:
        raise AssertionError(f"Expected 75.5, got {result}")

    result = parser._safe_float_conversion(None)
    if result is not None:
        raise AssertionError(f"Expected None, got {result}")

    result = parser._safe_float_conversion(pd.NA)
    if result is not None:
        raise AssertionError(f"Expected None for pd.NA, got {result}")
