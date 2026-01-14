"""
Output service for writing consolidated data to various formats.

Handles CSV, Parquet, and JSON output with proper lineage serialization.
"""

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from personal_health_ledger.domain.weight import WeightMeasurement
from personal_health_ledger.services.comparison import ComparisonResult
from personal_health_ledger.utils.parameters import OutputConfig

logger = logging.getLogger(__name__)


class OutputService:
    """
    Service for writing data to output files.

    Handles multiple output formats with proper serialization of complex types.
    """

    def __init__(self, config: OutputConfig) -> None:
        """
        Initialize output service.

        Args:
            config: Output configuration.
        """
        self.config = config
        self.output_dir = Path(config.dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_consolidated_data(self, measurements: list[WeightMeasurement]) -> None:
        """
        Write consolidated measurements to CSV and/or Parquet.

        Args:
            measurements: List of consolidated weight measurements.
        """
        if not measurements:
            logger.warning("No measurements to write")
            return

        if "csv" in self.config.formats:
            self._write_csv(measurements)

        if "parquet" in self.config.formats:
            self._write_parquet(measurements)

        logger.info(f"Wrote {len(measurements)} measurements to output")

    def _write_csv(self, measurements: list[WeightMeasurement]) -> None:
        """
        Write measurements to CSV file.

        Args:
            measurements: List of measurements.
        """
        csv_path = self.output_dir / self.config.files.consolidated_csv

        data = [m.to_dict(for_csv=True) for m in measurements]
        df = pd.DataFrame(data)

        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"Wrote CSV to {csv_path}")

    def _write_parquet(self, measurements: list[WeightMeasurement]) -> None:
        """
        Write measurements to Parquet file.

        Args:
            measurements: List of measurements.
        """
        parquet_path = self.output_dir / self.config.files.consolidated_parquet

        data = [m.to_dict(for_csv=False) for m in measurements]
        df = pd.DataFrame(data)

        df["source_types"] = df["source_types"].apply(lambda x: list(x) if x else [])

        df.to_parquet(  # type: ignore[call-overload]
            parquet_path,
            engine=self.config.parquet.engine,
            compression=self.config.parquet.compression,
            index=False,
        )
        logger.info(f"Wrote Parquet to {parquet_path}")

    def write_conflicts(self, measurements: list[WeightMeasurement]) -> None:
        """
        Write records with conflicts to separate CSV file.

        Args:
            measurements: List of all measurements.
        """
        conflicts = [m for m in measurements if m.conflicting_fields]

        if not conflicts:
            logger.info("No conflicts to write")
            return

        conflicts_path = self.output_dir / self.config.files.conflicts

        data = [m.to_dict(for_csv=True) for m in conflicts]
        df = pd.DataFrame(data)

        df.to_csv(conflicts_path, index=False, encoding="utf-8")
        logger.info(f"Wrote {len(conflicts)} conflicts to {conflicts_path}")

    def write_comparison_summary(self, results: list[ComparisonResult]) -> None:
        """
        Write comparison results to JSON file.

        Args:
            results: List of comparison results.
        """
        summary_path = self.output_dir / self.config.files.comparison_summary

        summary: dict[str, Any] = {
            "total_pairs": len(results),
            "pairs": [r.to_dict() for r in results],
        }

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)

        logger.info(f"Wrote comparison summary to {summary_path}")

    def write_ingestion_log(self, events: list[dict[str, Any]]) -> None:
        """
        Write ingestion events to JSONL file.

        Args:
            events: List of event dictionaries.
        """
        log_path = self.output_dir / self.config.files.ingestion_log

        with open(log_path, "w", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event, default=str) + "\n")

        logger.info(f"Wrote {len(events)} events to {log_path}")
