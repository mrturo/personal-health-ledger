"""
Configuration and parameter loading using Pydantic.

This module provides centralized configuration management for the entire application.
All parameters are loaded from YAML and validated using Pydantic models.
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from personal_health_ledger.utils.exceptions import ConfigurationError


class OAuth2Config(BaseModel):
    """OAuth2 authentication configuration."""

    credentials_path: str
    token_path: str
    scopes: list[str]


class ServiceAccountConfig(BaseModel):
    """Service account authentication configuration."""

    credentials_path: str
    scopes: list[str]


class DriveConfig(BaseModel):
    """Google Drive configuration."""

    auth_method: str = Field(pattern="^(oauth2|service_account)$")
    oauth2: OAuth2Config
    service_account: ServiceAccountConfig
    folder_name: str | None = None
    folder_id: str | None = None
    recursive: bool = False
    cache_dir: str
    index_file: str


class RecordIDConfig(BaseModel):
    """Record ID generation configuration."""

    algorithm: str = "sha256"
    timestamp_rounding_seconds: int = 60
    include_fields: list[str]


class ConflictResolutionConfig(BaseModel):
    """Conflict resolution policy configuration."""

    default_preference: str | None = None
    field_preferences: dict[str, str] = Field(default_factory=dict)


class ProcessingConfig(BaseModel):
    """Data processing configuration."""

    timezone: str
    timestamp_tolerance_seconds: int
    numeric_tolerance: float
    record_id: RecordIDConfig
    conflict_resolution: ConflictResolutionConfig


class CSVConfig(BaseModel):
    """CSV parsing configuration."""

    encodings: list[str]
    delimiters: list[str]
    column_mappings: dict[str, str]


class FITConfig(BaseModel):
    """FIT parsing configuration."""

    message_types: list[str]
    field_mappings: dict[str, str]


class OutputFilesConfig(BaseModel):
    """Output file names configuration."""

    consolidated_csv: str
    consolidated_parquet: str
    conflicts: str
    comparison_summary: str
    ingestion_log: str


class ParquetConfig(BaseModel):
    """Parquet output configuration."""

    compression: str = "snappy"
    engine: str = "pyarrow"


class OutputConfig(BaseModel):
    """Output configuration."""

    dir: str
    files: OutputFilesConfig
    formats: list[str]
    csv_complex_serialization: str = "json"
    parquet: ParquetConfig


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    format: str
    file: str
    console: bool = True


class AppConfig(BaseSettings):
    """Main application configuration."""

    drive: DriveConfig
    processing: ProcessingConfig
    csv: CSVConfig
    fit: FITConfig
    output: OutputConfig
    logging: LoggingConfig

    model_config = SettingsConfigDict(env_prefix="PHL_", case_sensitive=False)


class ParameterLoader:
    """
    Centralized parameter loader for the application.

    Loads and validates configuration from YAML files using Pydantic models.
    Provides type-safe access to all configuration parameters.
    """

    def __init__(self, config_path: str = "config/config.yaml") -> None:
        """
        Initialize parameter loader.

        Args:
            config_path: Path to the YAML configuration file.

        Raises:
            ConfigurationError: If configuration file cannot be loaded or is invalid.
        """
        self.config_path = Path(config_path)
        self.config: AppConfig
        self._load_config()

    def _load_config(self) -> None:
        """
        Load configuration from YAML file.

        Raises:
            ConfigurationError: If configuration file cannot be loaded or is invalid.
        """
        if not self.config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {self.config_path}")

        try:
            with open(self.config_path, encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)

            self.config = AppConfig(**config_dict)

        except yaml.YAMLError as e:
            raise ConfigurationError(f"Failed to parse YAML configuration: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {e}") from e

    def get_drive_config(self) -> DriveConfig:
        """Get Google Drive configuration."""
        return self.config.drive

    def get_processing_config(self) -> ProcessingConfig:
        """Get data processing configuration."""
        return self.config.processing

    def get_csv_config(self) -> CSVConfig:
        """Get CSV parsing configuration."""
        return self.config.csv

    def get_fit_config(self) -> FITConfig:
        """Get FIT parsing configuration."""
        return self.config.fit

    def get_output_config(self) -> OutputConfig:
        """Get output configuration."""
        return self.config.output

    def get_logging_config(self) -> LoggingConfig:
        """Get logging configuration."""
        return self.config.logging

    def get_raw_config(self) -> dict[str, Any]:
        """
        Get raw configuration dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        return self.config.model_dump()
