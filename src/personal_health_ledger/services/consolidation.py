"""
Consolidation service for merging and deduplicating weight data.

Handles merging of CSV and FIT records, conflict detection and resolution,
and generation of consolidated datasets with full lineage tracking.
"""

import logging
from collections import defaultdict
from datetime import datetime, timezone

from personal_health_ledger.domain.weight import (
    FieldSource,
    RawWeightRecord,
    SourceType,
    WeightMeasurement,
)
from personal_health_ledger.utils.exceptions import ConsolidationError
from personal_health_ledger.utils.hashing import generate_record_id
from personal_health_ledger.utils.parameters import ProcessingConfig
from personal_health_ledger.utils.timezone_utils import timestamps_match

logger = logging.getLogger(__name__)


class ConsolidationService:
    """
    Service for consolidating weight measurements from multiple sources.

    Merges CSV and FIT data, detects conflicts, and maintains full data lineage.
    """

    def __init__(self, config: ProcessingConfig) -> None:
        """
        Initialize consolidation service.

        Args:
            config: Processing configuration.
        """
        self.config = config

    def _find_matching_records(
        self,
        records: list[RawWeightRecord],
        tolerance_seconds: int,
    ) -> dict[tuple[datetime, SourceType], list[RawWeightRecord]]:
        """
        Group records by timestamp and source type.

        Args:
            records: List of raw weight records.
            tolerance_seconds: Timestamp matching tolerance.

        Returns:
            Dictionary mapping (timestamp, source_type) to list of records.
        """
        grouped: dict[tuple[datetime, SourceType], list[RawWeightRecord]] = defaultdict(list)

        for record in records:
            grouped[(record.timestamp, record.source_type)].append(record)

        return grouped

    def _merge_field(
        self,
        csv_value: float | None,
        fit_value: float | None,
        field_name: str,
    ) -> tuple[float | None, FieldSource, bool]:
        """
        Merge a single field from CSV and FIT sources.

        Args:
            csv_value: Value from CSV.
            fit_value: Value from FIT.
            field_name: Name of the field.

        Returns:
            Tuple of (merged_value, field_source, is_conflict).
        """
        if csv_value is None and fit_value is None:
            return None, FieldSource.MERGED, False

        if csv_value is None:
            return fit_value, FieldSource.FIT, False

        if fit_value is None:
            return csv_value, FieldSource.CSV, False

        if abs(csv_value - fit_value) <= self.config.numeric_tolerance:
            return csv_value, FieldSource.MERGED, False

        preference = self.config.conflict_resolution.field_preferences.get(
            field_name, self.config.conflict_resolution.default_preference
        )

        if preference == "csv":
            return csv_value, FieldSource.CONFLICT, True
        elif preference == "fit":
            return fit_value, FieldSource.CONFLICT, True
        else:
            return csv_value, FieldSource.CONFLICT, True

    def _merge_records(
        self,
        csv_records: list[RawWeightRecord],
        fit_records: list[RawWeightRecord],
    ) -> WeightMeasurement:
        """
        Merge CSV and FIT records into a single consolidated measurement.

        Args:
            csv_records: Records from CSV source.
            fit_records: Records from FIT source.

        Returns:
            Consolidated weight measurement with lineage.
        """
        all_records = csv_records + fit_records

        timestamp = all_records[0].timestamp
        source_files = [r.source_file_name for r in all_records]
        drive_file_ids = [r.source_file_id for r in all_records]
        source_types: set[SourceType] = {r.source_type for r in all_records}

        csv_data = csv_records[0] if csv_records else None
        fit_data = fit_records[0] if fit_records else None

        field_sources: dict[str, FieldSource] = {}
        conflicting_fields: list[str] = []

        weight_kg, field_sources["weight_kg"], is_conflict = self._merge_field(
            csv_data.weight_kg if csv_data else None,
            fit_data.weight_kg if fit_data else None,
            "weight_kg",
        )
        if is_conflict:
            conflicting_fields.append("weight_kg")

        body_fat_pct, field_sources["body_fat_pct"], is_conflict = self._merge_field(
            csv_data.body_fat_pct if csv_data else None,
            fit_data.body_fat_pct if fit_data else None,
            "body_fat_pct",
        )
        if is_conflict:
            conflicting_fields.append("body_fat_pct")

        fat_mass_kg, field_sources["fat_mass_kg"], _ = self._merge_field(
            csv_data.fat_mass_kg if csv_data else None,
            fit_data.fat_mass_kg if fit_data else None,
            "fat_mass_kg",
        )

        fat_free_pct, field_sources["fat_free_pct"], _ = self._merge_field(
            csv_data.fat_free_pct if csv_data else None,
            fit_data.fat_free_pct if fit_data else None,
            "fat_free_pct",
        )

        fat_free_mass_kg, field_sources["fat_free_mass_kg"], _ = self._merge_field(
            csv_data.fat_free_mass_kg if csv_data else None,
            fit_data.fat_free_mass_kg if fit_data else None,
            "fat_free_mass_kg",
        )

        skeletal_muscle_pct, field_sources["skeletal_muscle_pct"], _ = self._merge_field(
            csv_data.skeletal_muscle_pct if csv_data else None,
            fit_data.skeletal_muscle_pct if fit_data else None,
            "skeletal_muscle_pct",
        )

        skeletal_muscle_mass_kg, field_sources["skeletal_muscle_mass_kg"], _ = self._merge_field(
            csv_data.skeletal_muscle_mass_kg if csv_data else None,
            fit_data.skeletal_muscle_mass_kg if fit_data else None,
            "skeletal_muscle_mass_kg",
        )

        muscle_pct, field_sources["muscle_pct"], _ = self._merge_field(
            csv_data.muscle_pct if csv_data else None,
            fit_data.muscle_pct if fit_data else None,
            "muscle_pct",
        )

        muscle_mass_kg, field_sources["muscle_mass_kg"], _ = self._merge_field(
            csv_data.muscle_mass_kg if csv_data else None,
            fit_data.muscle_mass_kg if fit_data else None,
            "muscle_mass_kg",
        )

        bone_mass_kg, field_sources["bone_mass_kg"], _ = self._merge_field(
            csv_data.bone_mass_kg if csv_data else None,
            fit_data.bone_mass_kg if fit_data else None,
            "bone_mass_kg",
        )

        body_water, field_sources["body_water"], _ = self._merge_field(
            csv_data.body_water if csv_data else None,
            fit_data.body_water if fit_data else None,
            "body_water",
        )

        bmr_kcal, field_sources["bmr_kcal"], _ = self._merge_field(
            csv_data.bmr_kcal if csv_data else None,
            fit_data.bmr_kcal if fit_data else None,
            "bmr_kcal",
        )

        metabolic_age, field_sources["metabolic_age"], _ = self._merge_field(
            csv_data.metabolic_age if csv_data else None,
            fit_data.metabolic_age if fit_data else None,
            "metabolic_age",
        )

        visceral_fat_rating, field_sources["visceral_fat_rating"], _ = self._merge_field(
            csv_data.visceral_fat_rating if csv_data else None,
            fit_data.visceral_fat_rating if fit_data else None,
            "visceral_fat_rating",
        )

        if weight_kg is None:
            raise ConsolidationError("Cannot consolidate record without weight_kg")

        record_id = generate_record_id(
            timestamp, weight_kg, source_types, self.config.record_id
        )

        measurement = WeightMeasurement(
            record_id=record_id,
            timestamp=timestamp,
            weight_kg=weight_kg,
            body_fat_pct=body_fat_pct,
            fat_mass_kg=fat_mass_kg,
            fat_free_pct=fat_free_pct,
            fat_free_mass_kg=fat_free_mass_kg,
            skeletal_muscle_pct=skeletal_muscle_pct,
            skeletal_muscle_mass_kg=skeletal_muscle_mass_kg,
            muscle_pct=muscle_pct,
            muscle_mass_kg=muscle_mass_kg,
            bone_mass_kg=bone_mass_kg,
            body_water=body_water,
            bmr_kcal=bmr_kcal,
            metabolic_age=metabolic_age,
            visceral_fat_rating=visceral_fat_rating,
            source_files=source_files,
            source_types=source_types,
            drive_file_ids=drive_file_ids,
            ingestion_timestamp=datetime.now(timezone.utc),
            field_sources=field_sources,
            conflicting_fields=conflicting_fields,
            chosen_source=None,
            weight_kg_csv=None,
            weight_kg_fit=None,
            body_fat_pct_csv=None,
            body_fat_pct_fit=None,
        )

        if conflicting_fields:
            if "weight_kg" in conflicting_fields:
                measurement.weight_kg_csv = csv_data.weight_kg if csv_data else None
                measurement.weight_kg_fit = fit_data.weight_kg if fit_data else None
            if "body_fat_pct" in conflicting_fields:
                measurement.body_fat_pct_csv = csv_data.body_fat_pct if csv_data else None
                measurement.body_fat_pct_fit = fit_data.body_fat_pct if fit_data else None

        return measurement

    def consolidate(self, raw_records: list[RawWeightRecord]) -> list[WeightMeasurement]:
        """
        Consolidate raw records into canonical measurements.

        Args:
            raw_records: List of raw weight records from all sources.

        Returns:
            List of consolidated weight measurements with lineage.

        Raises:
            ConsolidationError: If consolidation fails.
        """
        try:
            csv_records = [r for r in raw_records if r.source_type == SourceType.CSV]
            fit_records = [r for r in raw_records if r.source_type == SourceType.FIT]

            logger.info(f"Consolidating {len(csv_records)} CSV and {len(fit_records)} FIT records")

            csv_by_timestamp: dict[datetime, list[RawWeightRecord]] = defaultdict(list)
            for record in csv_records:
                csv_by_timestamp[record.timestamp].append(record)

            fit_by_timestamp: dict[datetime, list[RawWeightRecord]] = defaultdict(list)
            for record in fit_records:
                fit_by_timestamp[record.timestamp].append(record)

            consolidated: list[WeightMeasurement] = []
            matched_fit_timestamps: set[datetime] = set()

            for csv_ts, csv_recs in csv_by_timestamp.items():
                matched_fit_recs: list[RawWeightRecord] = []

                for fit_ts, fit_recs in fit_by_timestamp.items():
                    if timestamps_match(
                        csv_ts, fit_ts, self.config.timestamp_tolerance_seconds
                    ):
                        matched_fit_recs.extend(fit_recs)
                        matched_fit_timestamps.add(fit_ts)

                measurement = self._merge_records(csv_recs, matched_fit_recs)
                consolidated.append(measurement)

            for fit_ts, fit_recs in fit_by_timestamp.items():
                if fit_ts not in matched_fit_timestamps:
                    measurement = self._merge_records([], fit_recs)
                    consolidated.append(measurement)

            consolidated.sort(key=lambda m: m.timestamp)

            logger.info(f"Consolidated to {len(consolidated)} measurements")
            return consolidated

        except Exception as e:
            raise ConsolidationError(f"Consolidation failed: {e}") from e
