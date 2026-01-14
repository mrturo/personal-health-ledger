"""Unit tests for comparison service."""

from datetime import datetime

import pytz

from personal_health_ledger.domain.weight import RawWeightRecord, SourceType
from personal_health_ledger.services.comparison import ComparisonService
from personal_health_ledger.utils.parameters import (
    ConflictResolutionConfig,
    ProcessingConfig,
    RecordIDConfig,
)


def test_comparison_basic() -> None:
    """Test basic comparison of CSV and FIT records."""
    config = ProcessingConfig(
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

    service = ComparisonService(config)

    tz = pytz.UTC
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=tz)

    csv_records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            source_file_name="Peso 1-2024 Huawei Health.csv",
            source_file_id="csv1",
            source_type=SourceType.CSV,
        )
    ]

    fit_records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            source_file_name="Peso 1-2024 Huawei Health.fit",
            source_file_id="fit1",
            source_type=SourceType.FIT,
        )
    ]

    results = service.compare(csv_records, fit_records)

    if len(results) != 1:
        raise AssertionError(f"Expected 1 comparison result, got {len(results)}")

    result = results[0]

    if result.both_count != 1:
        raise AssertionError(f"Expected both_count=1, got {result.both_count}")

    if result.csv_only_count != 0:
        raise AssertionError(f"Expected csv_only_count=0, got {result.csv_only_count}")

    if result.fit_only_count != 0:
        raise AssertionError(f"Expected fit_only_count=0, got {result.fit_only_count}")


def test_comparison_csv_only() -> None:
    """Test comparison with CSV-only records."""
    config = ProcessingConfig(
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

    service = ComparisonService(config)

    tz = pytz.UTC
    ts1 = datetime(2024, 1, 15, 10, 30, 0, tzinfo=tz)
    ts2 = datetime(2024, 1, 15, 11, 30, 0, tzinfo=tz)

    csv_records = [
        RawWeightRecord(
            timestamp=ts1,
            weight_kg=75.5,
            source_file_name="Peso 1-2024 Huawei Health.csv",
            source_file_id="csv1",
            source_type=SourceType.CSV,
        ),
        RawWeightRecord(
            timestamp=ts2,
            weight_kg=75.6,
            source_file_name="Peso 1-2024 Huawei Health.csv",
            source_file_id="csv1",
            source_type=SourceType.CSV,
        ),
    ]

    fit_records = [
        RawWeightRecord(
            timestamp=ts1,
            weight_kg=75.5,
            source_file_name="Peso 1-2024 Huawei Health.fit",
            source_file_id="fit1",
            source_type=SourceType.FIT,
        )
    ]

    results = service.compare(csv_records, fit_records)

    if len(results) != 1:
        raise AssertionError(f"Expected 1 comparison result, got {len(results)}")

    result = results[0]

    if result.both_count != 1:
        raise AssertionError(f"Expected both_count=1, got {result.both_count}")

    if result.csv_only_count != 1:
        raise AssertionError(f"Expected csv_only_count=1, got {result.csv_only_count}")

    if result.fit_only_count != 0:
        raise AssertionError(f"Expected fit_only_count=0, got {result.fit_only_count}")


def test_comparison_with_mismatch() -> None:
    """Test comparison with value mismatches."""
    config = ProcessingConfig(
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

    service = ComparisonService(config)

    tz = pytz.UTC
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=tz)

    csv_records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            body_fat_pct=18.2,
            source_file_name="Peso 1-2024 Huawei Health.csv",
            source_file_id="csv1",
            source_type=SourceType.CSV,
        )
    ]

    fit_records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=76.0,
            body_fat_pct=18.5,
            source_file_name="Peso 1-2024 Huawei Health.fit",
            source_file_id="fit1",
            source_type=SourceType.FIT,
        )
    ]

    results = service.compare(csv_records, fit_records)

    if len(results) != 1:
        raise AssertionError(f"Expected 1 comparison result, got {len(results)}")

    result = results[0]

    if result.both_count != 1:
        raise AssertionError(f"Expected both_count=1, got {result.both_count}")

    if "weight_kg" not in result.mismatches:
        raise AssertionError("Expected 'weight_kg' in mismatches")

    if result.mismatches["weight_kg"] != 1:
        raise AssertionError(
            f"Expected 1 weight_kg mismatch, got {result.mismatches['weight_kg']}"
        )

    if result.weight_mae is None:
        raise AssertionError("Expected weight_mae to be calculated")

    if abs(result.weight_mae - 0.5) > 0.001:
        raise AssertionError(f"Expected weight_mae≈0.5, got {result.weight_mae}")
