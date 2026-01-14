"""
Weight domain models and canonical schema.

This module defines the canonical schema for weight measurements,
including data lineage and metadata tracking.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SourceType(str, Enum):
    """Enumeration of data source types."""

    CSV = "csv"
    FIT = "fit"


class FieldSource(str, Enum):
    """Enumeration of field source resolution types."""

    CSV = "csv"
    FIT = "fit"
    MERGED = "merged"
    CONFLICT = "conflict"


class WeightMeasurement(BaseModel):
    """
    Canonical weight measurement model with full data lineage.

    All numeric measurements use float32-compatible types for efficiency.
    Timestamps are timezone-aware.
    """

    record_id: str = Field(description="Deterministic record identifier (hash)")
    timestamp: datetime = Field(description="Measurement timestamp (timezone-aware)")

    weight_kg: float | None = Field(None, description="Weight in kilograms")
    body_fat_pct: float | None = Field(None, description="Body fat percentage")
    fat_mass_kg: float | None = Field(None, description="Fat mass in kilograms")
    fat_free_pct: float | None = Field(None, description="Fat-free percentage")
    fat_free_mass_kg: float | None = Field(None, description="Fat-free mass in kilograms")
    skeletal_muscle_pct: float | None = Field(None, description="Skeletal muscle percentage")
    skeletal_muscle_mass_kg: float | None = Field(
        None, description="Skeletal muscle mass in kilograms"
    )
    muscle_pct: float | None = Field(None, description="Muscle percentage")
    muscle_mass_kg: float | None = Field(None, description="Muscle mass in kilograms")
    bone_mass_kg: float | None = Field(None, description="Bone mass in kilograms")
    body_water: float | None = Field(None, description="Body water (percentage or absolute)")
    bmr_kcal: float | None = Field(None, description="Basal metabolic rate in kcal")
    metabolic_age: float | None = Field(None, description="Metabolic age in years")
    visceral_fat_rating: float | None = Field(None, description="Visceral fat rating")

    source_files: list[str] = Field(
        default_factory=list, description="List of source file names"
    )
    source_types: set[SourceType] = Field(
        default_factory=set, description="Set of source types contributing to this record"
    )
    drive_file_ids: list[str] = Field(
        default_factory=list, description="List of Google Drive file IDs"
    )
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="Processing timestamp (UTC)"
    )
    field_sources: dict[str, FieldSource] = Field(
        default_factory=dict,
        description="Mapping of field names to their source resolution type",
    )
    conflicting_fields: list[str] = Field(
        default_factory=list, description="List of fields with CSV vs FIT conflicts"
    )
    chosen_source: SourceType | None = Field(
        None, description="Chosen source for conflict resolution (if policy applied)"
    )

    weight_kg_csv: float | None = Field(None, description="Weight from CSV (if conflict)")
    weight_kg_fit: float | None = Field(None, description="Weight from FIT (if conflict)")
    body_fat_pct_csv: float | None = Field(
        None, description="Body fat percentage from CSV (if conflict)"
    )
    body_fat_pct_fit: float | None = Field(
        None, description="Body fat percentage from FIT (if conflict)"
    )

    model_config = ConfigDict(
        use_enum_values=True,
        # Note: json_encoders is handled in to_dict() method for Pydantic v2
    )

    def to_dict(self, for_csv: bool = False) -> dict[str, Any]:
        """
        Convert measurement to dictionary representation.

        Args:
            for_csv: If True, serialize complex types (lists, sets, dicts) to JSON strings.

        Returns:
            Dictionary representation of the measurement.
        """
        import json

        data = self.model_dump()

        if for_csv:
            data["source_files"] = json.dumps(data["source_files"])
            data["source_types"] = json.dumps(list(data["source_types"]))
            data["drive_file_ids"] = json.dumps(data["drive_file_ids"])
            data["field_sources"] = json.dumps(data["field_sources"])
            data["conflicting_fields"] = json.dumps(data["conflicting_fields"])

        data["timestamp"] = (
            data["timestamp"].isoformat() if isinstance(data["timestamp"], datetime) else data["timestamp"]
        )
        data["ingestion_timestamp"] = (
            data["ingestion_timestamp"].isoformat()
            if isinstance(data["ingestion_timestamp"], datetime)
            else data["ingestion_timestamp"]
        )

        return data


class RawWeightRecord(BaseModel):
    """
    Raw weight record from a single source (CSV or FIT).

    Used during ingestion before consolidation.
    """

    timestamp: datetime = Field(description="Measurement timestamp (timezone-aware)")
    weight_kg: float | None = None
    body_fat_pct: float | None = None
    fat_mass_kg: float | None = None
    fat_free_pct: float | None = None
    fat_free_mass_kg: float | None = None
    skeletal_muscle_pct: float | None = None
    skeletal_muscle_mass_kg: float | None = None
    muscle_pct: float | None = None
    muscle_mass_kg: float | None = None
    bone_mass_kg: float | None = None
    body_water: float | None = None
    bmr_kcal: float | None = None
    metabolic_age: float | None = None
    visceral_fat_rating: float | None = None

    source_file_name: str = Field(description="Name of the source file")
    source_file_id: str = Field(description="Drive file ID or local file identifier")
    source_type: SourceType = Field(description="Type of source (CSV or FIT)")

    model_config = ConfigDict(use_enum_values=True)
