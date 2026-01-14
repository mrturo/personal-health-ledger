"""
CSV parser for weight data.

Provides robust CSV parsing with encoding detection, delimiter detection,
column name normalization, and safe numeric conversion.
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from personal_health_ledger.domain.weight import RawWeightRecord, SourceType
from personal_health_ledger.utils.exceptions import ParsingError
from personal_health_ledger.utils.parameters import CSVConfig, ProcessingConfig
from personal_health_ledger.utils.timezone_utils import make_timezone_aware, parse_datetime

logger = logging.getLogger(__name__)


class CSVParser:
    """
    Parser for CSV weight data files.

    Handles encoding detection, delimiter detection, column normalization,
    and conversion to canonical schema.
    """

    def __init__(self, csv_config: CSVConfig, processing_config: ProcessingConfig) -> None:
        """
        Initialize CSV parser.

        Args:
            csv_config: CSV parsing configuration.
            processing_config: Processing configuration (for timezone).
        """
        self.csv_config = csv_config
        self.processing_config = processing_config
        self.column_mappings = csv_config.column_mappings

    def _detect_encoding(self, file_path: Path) -> str:
        """
        Detect file encoding.

        Args:
            file_path: Path to CSV file.

        Returns:
            Detected encoding.
        """
        for encoding in self.csv_config.encodings:
            try:
                with open(file_path, encoding=encoding) as f:
                    f.read()
                logger.debug(f"Detected encoding: {encoding}")
                return encoding
            except (UnicodeDecodeError, LookupError):
                continue

        logger.warning("Encoding detection failed, using utf-8")
        return "utf-8"

    def _detect_delimiter(self, file_path: Path, encoding: str) -> str:
        """
        Detect CSV delimiter.

        Args:
            file_path: Path to CSV file.
            encoding: File encoding.

        Returns:
            Detected delimiter.
        """
        with open(file_path, encoding=encoding) as f:
            first_line = f.readline()

        for delimiter in self.csv_config.delimiters:
            if delimiter in first_line:
                logger.debug(f"Detected delimiter: {repr(delimiter)}")
                return delimiter

        logger.warning("Delimiter detection failed, using comma")
        return ","

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names to canonical schema.

        Args:
            df: DataFrame with original column names.

        Returns:
            DataFrame with normalized column names.
        """
        rename_map = {}

        for col in df.columns:
            col_stripped = col.strip()
            if col_stripped in self.column_mappings:
                rename_map[col] = self.column_mappings[col_stripped]

        if rename_map:
            df = df.rename(columns=rename_map)
            logger.debug(f"Normalized columns: {list(rename_map.values())}")

        return df

    def _safe_float_conversion(self, value: Any) -> float | None:
        """
        Safely convert value to float, handling comma decimal separator.

        Args:
            value: Value to convert.

        Returns:
            Float value or None if conversion fails.
        """
        if pd.isna(value):
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            value = value.strip().replace(",", ".")
            try:
                return float(value)
            except ValueError:
                return None

        return None

    def parse(self, file_path: Path, drive_file_id: str) -> list[RawWeightRecord]:
        """
        Parse CSV file into raw weight records.

        Args:
            file_path: Path to CSV file.
            drive_file_id: Google Drive file ID.

        Returns:
            List of raw weight records.

        Raises:
            ParsingError: If parsing fails.
        """
        try:
            encoding = self._detect_encoding(file_path)
            delimiter = self._detect_delimiter(file_path, encoding)

            df = pd.read_csv(file_path, encoding=encoding, sep=delimiter)

            df = self._normalize_column_names(df)

            records: list[RawWeightRecord] = []

            for idx, row in df.iterrows():
                try:
                    if "date" in df.columns:
                        date_str = str(row.get("date", ""))
                        time_str = str(row.get("time", "")) if "time" in df.columns else None
                        timestamp = parse_datetime(
                            date_str, time_str, self.processing_config.timezone
                        )
                    elif "timestamp" in df.columns:
                        timestamp = parse_datetime(
                            str(row["timestamp"]), None, self.processing_config.timezone
                        )
                    else:
                        logger.warning(f"Row {idx}: No date/timestamp column found, skipping")
                        continue

                    if not timestamp.tzinfo:
                        timestamp = make_timezone_aware(
                            timestamp, self.processing_config.timezone, assume_local=True
                        )

                    record = RawWeightRecord(
                        timestamp=timestamp,
                        weight_kg=self._safe_float_conversion(row.get("weight_kg")),
                        body_fat_pct=self._safe_float_conversion(row.get("body_fat_pct")),
                        fat_mass_kg=self._safe_float_conversion(row.get("fat_mass_kg")),
                        fat_free_pct=self._safe_float_conversion(row.get("fat_free_pct")),
                        fat_free_mass_kg=self._safe_float_conversion(row.get("fat_free_mass_kg")),
                        skeletal_muscle_pct=self._safe_float_conversion(
                            row.get("skeletal_muscle_pct")
                        ),
                        skeletal_muscle_mass_kg=self._safe_float_conversion(
                            row.get("skeletal_muscle_mass_kg")
                        ),
                        muscle_pct=self._safe_float_conversion(row.get("muscle_pct")),
                        muscle_mass_kg=self._safe_float_conversion(row.get("muscle_mass_kg")),
                        bone_mass_kg=self._safe_float_conversion(row.get("bone_mass_kg")),
                        body_water=self._safe_float_conversion(row.get("body_water")),
                        bmr_kcal=self._safe_float_conversion(row.get("bmr_kcal")),
                        source_file_name=file_path.name,
                        source_file_id=drive_file_id,
                        source_type=SourceType.CSV,
                    )

                    records.append(record)

                except Exception as e:
                    logger.warning(f"Failed to parse row {idx}: {e}")
                    continue

            logger.info(f"Parsed {len(records)} records from {file_path.name}")
            return records

        except Exception as e:
            raise ParsingError(f"Failed to parse CSV file {file_path}: {e}") from e
