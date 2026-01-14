"""Unit tests for consolidation service."""

from datetime import datetime

import pytz

from personal_health_ledger.domain.weight import FieldSource, RawWeightRecord, SourceType
from personal_health_ledger.services.consolidation import ConsolidationService
from personal_health_ledger.utils.parameters import (
    ConflictResolutionConfig,
    ProcessingConfig,
    RecordIDConfig,
)


def test_consolidation_csv_only() -> None:
    """Test consolidation with CSV records only."""
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

    service = ConsolidationService(config)

    tz = pytz.UTC
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=tz)

    records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            body_fat_pct=18.2,
            source_file_name="test.csv",
            source_file_id="file1",
            source_type=SourceType.CSV,
        )
    ]

    consolidated = service.consolidate(records)

    if len(consolidated) != 1:
        raise AssertionError(f"Expected 1 consolidated record, got {len(consolidated)}")

    measurement = consolidated[0]

    if measurement.weight_kg != 75.5:
        raise AssertionError(f"Expected weight_kg=75.5, got {measurement.weight_kg}")

    if measurement.body_fat_pct != 18.2:
        raise AssertionError(f"Expected body_fat_pct=18.2, got {measurement.body_fat_pct}")

    if SourceType.CSV not in measurement.source_types:
        raise AssertionError("Expected CSV in source_types")

    if "test.csv" not in measurement.source_files:
        raise AssertionError("Expected 'test.csv' in source_files")


def test_consolidation_merge_no_conflict() -> None:
    """Test merging CSV and FIT with no conflicts."""
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

    service = ConsolidationService(config)

    tz = pytz.UTC
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=tz)

    records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            body_fat_pct=18.2,
            source_file_name="test.csv",
            source_file_id="file1",
            source_type=SourceType.CSV,
        ),
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            body_fat_pct=18.2,
            source_file_name="test.fit",
            source_file_id="file2",
            source_type=SourceType.FIT,
        ),
    ]

    consolidated = service.consolidate(records)

    if len(consolidated) != 1:
        raise AssertionError(f"Expected 1 consolidated record, got {len(consolidated)}")

    measurement = consolidated[0]

    if measurement.weight_kg != 75.5:
        raise AssertionError(f"Expected weight_kg=75.5, got {measurement.weight_kg}")

    if len(measurement.conflicting_fields) != 0:
        raise AssertionError(
            f"Expected no conflicts, got {len(measurement.conflicting_fields)}"
        )

    if measurement.field_sources.get("weight_kg") != FieldSource.MERGED:
        raise AssertionError(
            f"Expected MERGED source for weight_kg, got {measurement.field_sources.get('weight_kg')}"
        )

    if len(measurement.source_files) != 2:
        raise AssertionError(f"Expected 2 source files, got {len(measurement.source_files)}")


def test_consolidation_with_conflict() -> None:
    """Test consolidation with conflicting values."""
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

    service = ConsolidationService(config)

    tz = pytz.UTC
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=tz)

    records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            body_fat_pct=18.2,
            source_file_name="test.csv",
            source_file_id="file1",
            source_type=SourceType.CSV,
        ),
        RawWeightRecord(
            timestamp=ts,
            weight_kg=76.0,
            body_fat_pct=18.5,
            source_file_name="test.fit",
            source_file_id="file2",
            source_type=SourceType.FIT,
        ),
    ]

    consolidated = service.consolidate(records)

    if len(consolidated) != 1:
        raise AssertionError(f"Expected 1 consolidated record, got {len(consolidated)}")

    measurement = consolidated[0]

    if len(measurement.conflicting_fields) == 0:
        raise AssertionError("Expected conflicts but got none")

    if "weight_kg" not in measurement.conflicting_fields:
        raise AssertionError("Expected 'weight_kg' in conflicting_fields")

    if measurement.weight_kg_csv != 75.5:
        raise AssertionError(f"Expected weight_kg_csv=75.5, got {measurement.weight_kg_csv}")

    if measurement.weight_kg_fit != 76.0:
        raise AssertionError(f"Expected weight_kg_fit=76.0, got {measurement.weight_kg_fit}")

    if measurement.field_sources.get("weight_kg") != FieldSource.CONFLICT:
        raise AssertionError("Expected CONFLICT source for weight_kg")


def test_lineage_preservation() -> None:
    """Test that lineage is properly preserved in consolidated records."""
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

    service = ConsolidationService(config)

    tz = pytz.UTC
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=tz)

    records = [
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            source_file_name="test1.csv",
            source_file_id="drive_id_1",
            source_type=SourceType.CSV,
        ),
        RawWeightRecord(
            timestamp=ts,
            weight_kg=75.5,
            source_file_name="test1.fit",
            source_file_id="drive_id_2",
            source_type=SourceType.FIT,
        ),
    ]

    consolidated = service.consolidate(records)
    measurement = consolidated[0]

    if len(measurement.source_files) == 0:
        raise AssertionError("source_files should not be empty")

    if "test1.csv" not in measurement.source_files:
        raise AssertionError("Expected 'test1.csv' in source_files")

    if "test1.fit" not in measurement.source_files:
        raise AssertionError("Expected 'test1.fit' in source_files")

    if len(measurement.drive_file_ids) == 0:
        raise AssertionError("drive_file_ids should not be empty")

    if "drive_id_1" not in measurement.drive_file_ids:
        raise AssertionError("Expected 'drive_id_1' in drive_file_ids")

    if "drive_id_2" not in measurement.drive_file_ids:
        raise AssertionError("Expected 'drive_id_2' in drive_file_ids")

    if SourceType.CSV not in measurement.source_types:
        raise AssertionError("Expected CSV in source_types")

    if SourceType.FIT not in measurement.source_types:
        raise AssertionError("Expected FIT in source_types")

    if measurement.record_id == "":
        raise AssertionError("record_id should not be empty")

    if len(measurement.field_sources) == 0:
        raise AssertionError("field_sources should not be empty")
