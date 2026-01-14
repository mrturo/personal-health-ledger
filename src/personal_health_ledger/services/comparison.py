"""
Comparison service for analyzing CSV vs FIT data quality.

Provides detailed comparison reports for CSV and FIT file pairs.
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

from personal_health_ledger.domain.weight import RawWeightRecord
from personal_health_ledger.utils.parameters import ProcessingConfig
from personal_health_ledger.utils.timezone_utils import timestamps_match

logger = logging.getLogger(__name__)


class ComparisonResult:
    """Results of comparing CSV and FIT file pair."""

    def __init__(
        self,
        csv_file_name: str,
        fit_file_name: str,
        csv_drive_id: str,
        fit_drive_id: str,
    ) -> None:
        """
        Initialize comparison result.

        Args:
            csv_file_name: Name of CSV file.
            fit_file_name: Name of FIT file.
            csv_drive_id: Drive ID of CSV file.
            fit_drive_id: Drive ID of FIT file.
        """
        self.csv_file_name = csv_file_name
        self.fit_file_name = fit_file_name
        self.csv_drive_id = csv_drive_id
        self.fit_drive_id = fit_drive_id

        self.csv_only_count = 0
        self.fit_only_count = 0
        self.both_count = 0
        self.mismatches: dict[str, int] = defaultdict(int)

        self.csv_min_timestamp: datetime | None = None
        self.csv_max_timestamp: datetime | None = None
        self.fit_min_timestamp: datetime | None = None
        self.fit_max_timestamp: datetime | None = None

        self.weight_mae: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "csv_file_name": self.csv_file_name,
            "fit_file_name": self.fit_file_name,
            "csv_drive_id": self.csv_drive_id,
            "fit_drive_id": self.fit_drive_id,
            "csv_only_count": self.csv_only_count,
            "fit_only_count": self.fit_only_count,
            "both_count": self.both_count,
            "mismatches": dict(self.mismatches),
            "csv_timestamp_range": {
                "min": self.csv_min_timestamp.isoformat() if self.csv_min_timestamp else None,
                "max": self.csv_max_timestamp.isoformat() if self.csv_max_timestamp else None,
            },
            "fit_timestamp_range": {
                "min": self.fit_min_timestamp.isoformat() if self.fit_min_timestamp else None,
                "max": self.fit_max_timestamp.isoformat() if self.fit_max_timestamp else None,
            },
            "weight_mae": self.weight_mae,
        }


class ComparisonService:
    """
    Service for comparing CSV and FIT weight data.

    Provides detailed metrics on data quality and discrepancies.
    """

    def __init__(self, config: ProcessingConfig) -> None:
        """
        Initialize comparison service.

        Args:
            config: Processing configuration.
        """
        self.config = config

    def _extract_month_year(self, filename: str) -> tuple[int, int] | None:
        """
        Extract month and year from filename.

        Expected pattern: "Peso <m>-<yyyy> Huawei Health.(csv|fit)"

        Args:
            filename: File name to parse.

        Returns:
            Tuple of (month, year) or None if parsing fails.
        """
        try:
            parts = filename.split()
            if len(parts) < 2:
                return None

            date_part = parts[1]
            month_str, year_str = date_part.split("-")
            month = int(month_str)
            year = int(year_str)

            return (month, year)

        except Exception as e:
            logger.debug(f"Failed to extract month/year from {filename}: {e}")
            return None

    def _find_file_pairs(
        self, csv_records: list[RawWeightRecord], fit_records: list[RawWeightRecord]
    ) -> list[tuple[list[RawWeightRecord], list[RawWeightRecord]]]:
        """
        Find matching CSV and FIT file pairs.

        Args:
            csv_records: All CSV records.
            fit_records: All FIT records.

        Returns:
            List of (csv_records, fit_records) pairs.
        """
        csv_by_file: dict[str, list[RawWeightRecord]] = defaultdict(list)
        for record in csv_records:
            csv_by_file[record.source_file_name].append(record)

        fit_by_file: dict[str, list[RawWeightRecord]] = defaultdict(list)
        for record in fit_records:
            fit_by_file[record.source_file_name].append(record)

        csv_by_month_year: dict[tuple[int, int], str] = {}
        for csv_file in csv_by_file.keys():
            month_year = self._extract_month_year(csv_file)
            if month_year:
                csv_by_month_year[month_year] = csv_file

        fit_by_month_year: dict[tuple[int, int], str] = {}
        for fit_file in fit_by_file.keys():
            month_year = self._extract_month_year(fit_file)
            if month_year:
                fit_by_month_year[month_year] = fit_file

        pairs: list[tuple[list[RawWeightRecord], list[RawWeightRecord]]] = []

        for month_year, csv_file in csv_by_month_year.items():
            if month_year in fit_by_month_year:
                fit_file = fit_by_month_year[month_year]
                pairs.append((csv_by_file[csv_file], fit_by_file[fit_file]))

        # Track which files have been paired
        paired_csv_files = {csv_file for month_year, csv_file in csv_by_month_year.items()
                           if month_year in fit_by_month_year}
        paired_fit_files = {fit_by_month_year[month_year] for month_year, csv_file in csv_by_month_year.items()
                           if month_year in fit_by_month_year}

        # Add unpaired FIT files
        for fit_file in fit_by_file.keys():
            if fit_file not in paired_fit_files:
                pairs.append(([], fit_by_file[fit_file]))

        # Add unpaired CSV files
        for csv_file in csv_by_file.keys():
            if csv_file not in paired_csv_files:
                pairs.append((csv_by_file[csv_file], []))

        return pairs

    def _compare_pair(
        self, csv_records: list[RawWeightRecord], fit_records: list[RawWeightRecord]
    ) -> ComparisonResult:
        """
        Compare a pair of CSV and FIT records.

        Args:
            csv_records: Records from CSV file.
            fit_records: Records from FIT file.

        Returns:
            Comparison result.
        """
        csv_file_name = csv_records[0].source_file_name if csv_records else "N/A"
        fit_file_name = fit_records[0].source_file_name if fit_records else "N/A"
        csv_drive_id = csv_records[0].source_file_id if csv_records else "N/A"
        fit_drive_id = fit_records[0].source_file_id if fit_records else "N/A"

        result = ComparisonResult(csv_file_name, fit_file_name, csv_drive_id, fit_drive_id)

        if csv_records:
            csv_timestamps = [r.timestamp for r in csv_records]
            result.csv_min_timestamp = min(csv_timestamps)
            result.csv_max_timestamp = max(csv_timestamps)

        if fit_records:
            fit_timestamps = [r.timestamp for r in fit_records]
            result.fit_min_timestamp = min(fit_timestamps)
            result.fit_max_timestamp = max(fit_timestamps)

        matched_fit_indices: set[int] = set()

        weight_differences: list[float] = []

        for csv_record in csv_records:
            matched = False

            for fit_idx, fit_record in enumerate(fit_records):
                if timestamps_match(
                    csv_record.timestamp,
                    fit_record.timestamp,
                    self.config.timestamp_tolerance_seconds,
                ):
                    matched = True
                    matched_fit_indices.add(fit_idx)
                    result.both_count += 1

                    if csv_record.weight_kg and fit_record.weight_kg:
                        diff = abs(csv_record.weight_kg - fit_record.weight_kg)
                        weight_differences.append(diff)

                        if diff > self.config.numeric_tolerance:
                            result.mismatches["weight_kg"] += 1

                    for field in [
                        "body_fat_pct",
                        "fat_mass_kg",
                        "fat_free_pct",
                        "fat_free_mass_kg",
                    ]:
                        csv_val = getattr(csv_record, field)
                        fit_val = getattr(fit_record, field)

                        if csv_val is not None and fit_val is not None:
                            if abs(csv_val - fit_val) > self.config.numeric_tolerance:
                                result.mismatches[field] += 1

                    break

            if not matched:
                result.csv_only_count += 1

        for fit_idx in range(len(fit_records)):
            if fit_idx not in matched_fit_indices:
                result.fit_only_count += 1

        if weight_differences:
            result.weight_mae = sum(weight_differences) / len(weight_differences)

        return result

    def compare(
        self, csv_records: list[RawWeightRecord], fit_records: list[RawWeightRecord]
    ) -> list[ComparisonResult]:
        """
        Compare CSV and FIT records across all file pairs.

        Args:
            csv_records: All CSV records.
            fit_records: All FIT records.

        Returns:
            List of comparison results, one per file pair.
        """
        logger.info(f"Comparing {len(csv_records)} CSV and {len(fit_records)} FIT records")

        pairs = self._find_file_pairs(csv_records, fit_records)
        results: list[ComparisonResult] = []

        for csv_recs, fit_recs in pairs:
            result = self._compare_pair(csv_recs, fit_recs)
            results.append(result)

        logger.info(f"Compared {len(results)} file pairs")
        return results
