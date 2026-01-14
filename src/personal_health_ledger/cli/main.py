"""
Command-line interface for Personal Health Ledger.

Provides commands for syncing, building, comparing, and processing health data.
"""

from datetime import datetime
from datetime import timezone as dt_timezone
from pathlib import Path
from typing import Any

import typer

from personal_health_ledger.domain.weight import RawWeightRecord
from personal_health_ledger.infrastructure.drive_client.client import DriveClient
from personal_health_ledger.infrastructure.parsers.csv_parser import CSVParser
from personal_health_ledger.infrastructure.parsers.fit_parser import FITParser
from personal_health_ledger.services.comparison import ComparisonService
from personal_health_ledger.services.consolidation import ConsolidationService
from personal_health_ledger.services.daily_consolidation import DailyConsolidationService
from personal_health_ledger.services.output import OutputService
from personal_health_ledger.utils.exceptions import PersonalHealthLedgerError
from personal_health_ledger.utils.logging_config import get_logger, setup_logging
from personal_health_ledger.utils.parameters import ParameterLoader

app = typer.Typer(help="Personal Health Ledger - Health data consolidation and auditing")

logger = get_logger(__name__)


def init_config(config_path: str = "config/config.yaml") -> ParameterLoader:
    """
    Initialize configuration and logging.

    Args:
        config_path: Path to configuration file.

    Returns:
        Parameter loader instance.
    """
    param_loader = ParameterLoader(config_path)
    setup_logging(param_loader.get_logging_config(), "personal_health_ledger")
    return param_loader


@app.command()
def sync(
    config_path: str = typer.Option("config/config.yaml", help="Path to configuration file"),
    folder_id: str | None = typer.Option(None, help="Override folder ID from config"),
    folder_name: str | None = typer.Option(None, help="Override folder name from config"),
    force: bool = typer.Option(False, help="Force re-download of all files"),
) -> None:
    """
    Sync files from Google Drive.

    Downloads weight data files from configured Google Drive folder
    with checksum-based optimization.
    """
    try:
        param_loader = init_config(config_path)
        drive_config = param_loader.get_drive_config()

        if folder_id:
            drive_config.folder_id = folder_id
        if folder_name:
            drive_config.folder_name = folder_name

        logger.info("Starting Google Drive sync")

        drive_client = DriveClient(drive_config)
        local_paths = drive_client.sync_folder(force=force)

        typer.echo(f"Successfully synced {len(local_paths)} files")
        for path in local_paths:
            typer.echo(f"  - {path.name}")

    except PersonalHealthLedgerError as e:
        logger.error(f"Sync failed: {e}")
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def build(
    config_path: str = typer.Option("config/config.yaml", help="Path to configuration file"),
    timezone: str | None = typer.Option(None, help="Override timezone from config"),
    tolerance_seconds: int | None = typer.Option(
        None, help="Override timestamp tolerance from config"
    ),
    output_format: str | None = typer.Option(
        None, help="Output format: csv, parquet, or both"
    ),
) -> None:
    """
    Build consolidated dataset from raw files.

    Parses CSV and FIT files, consolidates data, detects conflicts,
    and writes output with full lineage tracking.
    """
    try:
        param_loader = init_config(config_path)
        csv_config = param_loader.get_csv_config()
        fit_config = param_loader.get_fit_config()
        processing_config = param_loader.get_processing_config()
        output_config = param_loader.get_output_config()

        if timezone:
            processing_config.timezone = timezone
        if tolerance_seconds:
            processing_config.timestamp_tolerance_seconds = tolerance_seconds
        if output_format:
            if output_format == "both":
                output_config.formats = ["csv", "parquet"]
            else:
                output_config.formats = [output_format]

        logger.info("Starting build process")

        raw_dir = Path(param_loader.get_drive_config().cache_dir)
        if not raw_dir.exists():
            raise PersonalHealthLedgerError(f"Raw data directory not found: {raw_dir}")

        csv_parser = CSVParser(csv_config, processing_config)
        fit_parser = FITParser(fit_config, processing_config)

        all_records: list[RawWeightRecord] = []
        ingestion_events = []

        for file_path in raw_dir.iterdir():
            if file_path.is_file():
                event: dict[str, Any] = {
                    "timestamp": datetime.now(dt_timezone.utc).isoformat(),
                    "file": file_path.name,
                    "action": "parse",
                }

                try:
                    if file_path.suffix.lower() == ".csv":
                        records = csv_parser.parse(file_path, file_path.name)
                        all_records.extend(records)
                        event["status"] = "success"
                        event["records"] = len(records)
                        typer.echo(f"Parsed {len(records)} records from {file_path.name}")

                    elif file_path.suffix.lower() == ".fit":
                        records = fit_parser.parse(file_path, file_path.name)
                        all_records.extend(records)
                        event["status"] = "success"
                        event["records"] = len(records)
                        typer.echo(f"Parsed {len(records)} records from {file_path.name}")

                    else:
                        event["status"] = "skipped"
                        event["reason"] = "unsupported_format"

                except Exception as e:
                    event["status"] = "error"
                    event["error"] = str(e)
                    logger.error(f"Failed to parse {file_path.name}: {e}")

                ingestion_events.append(event)

        if not all_records:
            raise PersonalHealthLedgerError("No records parsed from raw files")

        logger.info(f"Parsed total of {len(all_records)} records")

        consolidation_service = ConsolidationService(processing_config)
        consolidated = consolidation_service.consolidate(all_records)

        typer.echo(f"Consolidated to {len(consolidated)} measurements")

        output_service = OutputService(output_config)
        output_service.write_consolidated_data(consolidated)
        output_service.write_conflicts(consolidated)
        output_service.write_ingestion_log(ingestion_events)

        conflicts_count = len([m for m in consolidated if m.conflicting_fields])
        typer.echo(f"Found {conflicts_count} measurements with conflicts")
        typer.echo(f"Output written to {output_config.dir}/")

    except PersonalHealthLedgerError as e:
        logger.error(f"Build failed: {e}")
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def compare(
    config_path: str = typer.Option("config/config.yaml", help="Path to configuration file"),
    tolerance_seconds: int | None = typer.Option(
        None, help="Override timestamp tolerance from config"
    ),
) -> None:
    """
    Compare CSV vs FIT data without consolidation.

    Generates detailed comparison report showing discrepancies
    between CSV and FIT file pairs.
    """
    try:
        param_loader = init_config(config_path)
        csv_config = param_loader.get_csv_config()
        fit_config = param_loader.get_fit_config()
        processing_config = param_loader.get_processing_config()
        output_config = param_loader.get_output_config()

        if tolerance_seconds:
            processing_config.timestamp_tolerance_seconds = tolerance_seconds

        logger.info("Starting comparison")

        raw_dir = Path(param_loader.get_drive_config().cache_dir)

        csv_parser = CSVParser(csv_config, processing_config)
        fit_parser = FITParser(fit_config, processing_config)

        csv_records: list[RawWeightRecord] = []
        fit_records: list[RawWeightRecord] = []

        for file_path in raw_dir.iterdir():
            if file_path.is_file():
                try:
                    if file_path.suffix.lower() == ".csv":
                        records = csv_parser.parse(file_path, file_path.name)
                        csv_records.extend(records)

                    elif file_path.suffix.lower() == ".fit":
                        records = fit_parser.parse(file_path, file_path.name)
                        fit_records.extend(records)

                except Exception as e:
                    logger.error(f"Failed to parse {file_path.name}: {e}")

        comparison_service = ComparisonService(processing_config)
        results = comparison_service.compare(csv_records, fit_records)

        output_service = OutputService(output_config)
        output_service.write_comparison_summary(results)

        typer.echo(f"Compared {len(results)} file pairs")
        for result in results:
            typer.echo(f"\n{result.csv_file_name} vs {result.fit_file_name}:")
            typer.echo(f"  CSV only: {result.csv_only_count}")
            typer.echo(f"  FIT only: {result.fit_only_count}")
            typer.echo(f"  Both: {result.both_count}")
            if result.mismatches:
                typer.echo(f"  Mismatches: {dict(result.mismatches)}")
            if result.weight_mae is not None:
                typer.echo(f"  Weight MAE: {result.weight_mae:.3f} kg")

        typer.echo(f"\nComparison summary written to {output_config.dir}/")

    except PersonalHealthLedgerError as e:
        logger.error(f"Comparison failed: {e}")
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def daily(
    config_path: str = typer.Option("config/config.yaml", help="Path to configuration file"),
    input_file: str = typer.Option("output/weight_consolidated.csv", help="Input consolidated file"),
    output_file: str = typer.Option("output/weight_daily.csv", help="Output daily consolidated file"),
) -> None:
    """
    Generate daily consolidated data with averages.

    For each day, computes the average of all non-null numeric values
    across multiple measurements.
    """
    try:
        param_loader = init_config(config_path)
        
        logger.info("Starting daily consolidation")
        
        service = DailyConsolidationService()
        service.consolidate_by_day(Path(input_file), Path(output_file))
        
        typer.echo(f"\n✅ Daily consolidated file created: {output_file}")

    except PersonalHealthLedgerError as e:
        logger.error(f"Daily consolidation failed: {e}")
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def all(
    config_path: str = typer.Option("config/config.yaml", help="Path to configuration file"),
    force_sync: bool = typer.Option(False, help="Force re-download of all files"),
) -> None:
    """
    Run complete pipeline: sync, build, compare, and daily consolidation.

    Executes the full workflow from Drive sync to daily consolidated output.
    """
    try:
        typer.echo("=== Step 1: Syncing from Google Drive ===")
        sync(config_path=config_path, folder_id=None, folder_name=None, force=force_sync)

        typer.echo("\n=== Step 2: Building consolidated dataset ===")
        build(config_path=config_path, timezone=None, tolerance_seconds=None, output_format=None)

        typer.echo("\n=== Step 3: Comparing CSV vs FIT ===")
        compare(config_path=config_path, tolerance_seconds=None)

        typer.echo("\n=== Step 4: Generating daily consolidation ===")
        daily(config_path=config_path, input_file="output/weight_consolidated.csv", output_file="output/weight_daily.csv")

        typer.echo("\n=== Pipeline completed successfully ===")

    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
