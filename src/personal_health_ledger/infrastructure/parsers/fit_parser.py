"""
FIT parser for weight data.

Provides FIT file parsing using fitparse library.
"""

import logging
from pathlib import Path
from typing import Any

from fitparse import FitFile

from personal_health_ledger.domain.weight import RawWeightRecord, SourceType
from personal_health_ledger.utils.exceptions import ParsingError
from personal_health_ledger.utils.parameters import FITConfig, ProcessingConfig
from personal_health_ledger.utils.timezone_utils import make_timezone_aware

logger = logging.getLogger(__name__)


class FITParser:
    """
    Parser for FIT weight data files.

    Extracts weight scale records and converts to canonical schema.
    """

    def __init__(self, fit_config: FITConfig, processing_config: ProcessingConfig) -> None:
        """
        Initialize FIT parser.

        Args:
            fit_config: FIT parsing configuration.
            processing_config: Processing configuration (for timezone).
        """
        self.fit_config = fit_config
        self.processing_config = processing_config

    def parse(self, file_path: Path, drive_file_id: str) -> list[RawWeightRecord]:
        """
        Parse FIT file into raw weight records.

        Args:
            file_path: Path to FIT file.
            drive_file_id: Google Drive file ID.

        Returns:
            List of raw weight records.

        Raises:
            ParsingError: If parsing fails.
        """
        try:
            fitfile = FitFile(str(file_path))
            records: list[RawWeightRecord] = []

            for message_type in self.fit_config.message_types:
                for record_data in fitfile.get_messages(message_type):
                    try:
                        data_dict = {}
                        for field in record_data:
                            data_dict[field.name] = field.value

                        timestamp = data_dict.get("timestamp")
                        if not timestamp:
                            logger.warning("No timestamp in FIT record, skipping")
                            continue

                        if not timestamp.tzinfo:
                            timestamp = make_timezone_aware(
                                timestamp, self.processing_config.timezone, assume_local=True
                            )

                        # Map FIT fields to canonical fields using field_mappings
                        mapped_data = {}
                        for fit_field, canonical_field in self.fit_config.field_mappings.items():
                            if fit_field in data_dict and data_dict[fit_field] is not None:
                                mapped_data[canonical_field] = data_dict[fit_field]

                        # Weight is required
                        if "weight_kg" not in mapped_data:
                            logger.warning("No weight in FIT record, skipping")
                            continue

                        def safe_float(val: Any) -> float | None:
                            return float(val) if val is not None else None

                        record = RawWeightRecord(
                            timestamp=timestamp,
                            weight_kg=safe_float(mapped_data.get("weight_kg")),
                            body_fat_pct=safe_float(mapped_data.get("body_fat_pct")),
                            fat_mass_kg=safe_float(mapped_data.get("fat_mass_kg")),
                            fat_free_pct=safe_float(mapped_data.get("fat_free_pct")),
                            fat_free_mass_kg=safe_float(mapped_data.get("fat_free_mass_kg")),
                            skeletal_muscle_pct=safe_float(mapped_data.get("skeletal_muscle_pct")),
                            skeletal_muscle_mass_kg=safe_float(mapped_data.get("skeletal_muscle_mass_kg")),
                            muscle_pct=safe_float(mapped_data.get("muscle_pct")),
                            muscle_mass_kg=safe_float(mapped_data.get("muscle_mass_kg")),
                            bone_mass_kg=safe_float(mapped_data.get("bone_mass_kg")),
                            body_water=safe_float(mapped_data.get("body_water")),
                            bmr_kcal=safe_float(mapped_data.get("bmr_kcal")),
                            metabolic_age=safe_float(mapped_data.get("metabolic_age")),
                            visceral_fat_rating=safe_float(mapped_data.get("visceral_fat_rating")),
                            source_file_name=file_path.name,
                            source_file_id=drive_file_id,
                            source_type=SourceType.FIT,
                        )

                        records.append(record)

                    except Exception as e:
                        logger.warning(f"Failed to parse FIT record: {e}")
                        continue

            logger.info(f"Parsed {len(records)} records from {file_path.name}")
            return records

        except Exception as e:
            raise ParsingError(f"Failed to parse FIT file {file_path}: {e}") from e
