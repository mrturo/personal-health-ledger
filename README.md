# Personal Health Ledger

A comprehensive Python system for consolidating, auditing, and comparing personal health data from multiple sources. The MVP focuses on **weight measurements** exported from **Health Sync** app to **Google Drive**, with a modular, extensible architecture (DDD-inspired) designed to support future expansion to activities, sleep, and other data sources.

## Key Features

✅ **Multi-source integration**: Google Drive (OAuth2 & Service Account)  
✅ **Robust parsing**: CSV (Spanish/English, encoding/delimiter detection) and FIT files  
✅ **Intelligent consolidation**: Timestamp-based merge with conflict detection  
✅ **Full data lineage**: Every record tracks source files, Drive IDs, field sources, conflicts  
✅ **Multiple output formats**: CSV, Parquet, JSON reports  
✅ **Type-safe & tested**: Mypy/Pyright strict mode, pytest with custom assertions  
✅ **Production-ready**: Modular architecture, centralized config, comprehensive logging  
✅ **GitHub Actions CI**: Automated linting, type checking, and testing

## Architecture

```
personal-health-ledger/
├── src/personal_health_ledger/
│   ├── domain/              # Canonical models (WeightMeasurement, RawWeightRecord)
│   ├── infrastructure/      # External systems
│   │   ├── drive_client/    # Google Drive authentication & sync
│   │   └── parsers/         # CSV & FIT parsers
│   ├── services/            # Business logic
│   │   ├── consolidation.py # Merge & dedupe with conflict detection
│   │   ├── comparison.py    # CSV vs FIT quality analysis
│   │   └── output.py        # Multi-format output writer
│   ├── cli/                 # Typer-based CLI
│   └── utils/               # Config, logging, hashing, timezone
├── config/
│   ├── config.yaml          # Centralized configuration
│   ├── credentials.json     # OAuth2 credentials (not in repo)
│   └── token.json           # OAuth2 token (auto-generated)
├── data/
│   ├── raw/                 # Downloaded files from Drive
│   └── processed/           # (Reserved for future use)
├── output/                  # Consolidated datasets & reports
├── tests/                   # Pytest unit tests
└── .github/workflows/       # CI/CD pipeline
```

## Installation

### Prerequisites

- **Python 3.10+**
- **Google Cloud Platform account** (for Drive API credentials)

### Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/personal-health-ledger.git
   cd personal-health-ledger
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   # For development:
   pip install -r requirements-dev.txt
   pip install -e .
   ```

3. **Configure Google Drive credentials** (see next section)

4. **Edit configuration**:
   ```bash
   cp config/config.yaml config/config.yaml.local  # Optional: keep local overrides
   # Edit config/config.yaml with your preferences
   ```

## Google Drive Setup

### Option 1: OAuth2 (Recommended for personal use)

1. **Create Google Cloud Project**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project (e.g., "Personal Health Ledger")

2. **Enable Google Drive API**:
   - Navigate to "APIs & Services" → "Library"
   - Search for "Google Drive API" and click "Enable"

3. **Create OAuth2 Credentials**:
   - Go to "APIs & Services" → "Credentials"
   - Click "Create Credentials" → "OAuth client ID"
   - Application type: **Desktop app**
   - Name: "Personal Health Ledger CLI"
   - Download the JSON file

4. **Save credentials**:
   ```bash
   mv ~/Downloads/client_secret_*.json config/credentials.json
   ```

5. **Configure `config.yaml`**:
   ```yaml
   drive:
     auth_method: "oauth2"
     oauth2:
       credentials_path: "config/credentials.json"
       token_path: "config/token.json"
       scopes:
         - "https://www.googleapis.com/auth/drive.readonly"
   ```

6. **First run** will open a browser for authorization. Token is saved for future use.

### Option 2: Service Account (For automation/headless servers)

1. **Create Service Account**:
   - Go to "APIs & Services" → "Credentials"
   - Click "Create Credentials" → "Service Account"
   - Name: "phl-service-account"
   - Grant role: (none required for Drive)
   - Download JSON key

2. **Save credentials**:
   ```bash
   mv ~/Downloads/service-account-key.json config/service_account.json
   ```

3. **Share Drive folder with service account**:
   - Open Google Drive folder "Health Sync Weight"
   - Share with service account email (found in JSON: `client_email`)
   - Grant "Viewer" access

4. **Configure `config.yaml`**:
   ```yaml
   drive:
     auth_method: "service_account"
     service_account:
       credentials_path: "config/service_account.json"
       scopes:
         - "https://www.googleapis.com/auth/drive.readonly"
   ```

## Configuration

All settings are in `config/config.yaml`. Key parameters:

```yaml
drive:
  folder_name: "Health Sync Weight"  # Or use folder_id: "1A2B3C..."
  cache_dir: "data/raw"

processing:
  timezone: "America/Santiago"
  timestamp_tolerance_seconds: 60  # For CSV-FIT matching
  numeric_tolerance: 0.001         # For conflict detection
  
  conflict_resolution:
    default_preference: null  # null = keep both, "csv"/"fit" = prefer one
    field_preferences:
      weight_kg: "csv"  # Override per field

csv:
  column_mappings:  # Spanish → English canonical names
    "Fecha": "date"
    "Peso": "weight_kg"
    # ... (full list in config file)

output:
  dir: "output"
  formats: ["csv", "parquet"]
```

## Usage

### CLI Commands

**Full pipeline** (sync → build → compare):
```bash
phl all
```

**Individual commands**:

1. **Sync from Google Drive**:
   ```bash
   phl sync
   # Options:
   #   --folder-id TEXT        Override folder ID
   #   --folder-name TEXT      Override folder name
   #   --force                 Re-download all files
   ```

2. **Build consolidated dataset**:
   ```bash
   phl build
   # Options:
   #   --timezone TEXT              Override timezone
   #   --tolerance-seconds INT      Override timestamp tolerance
   #   --output-format TEXT         csv, parquet, or both
   ```

3. **Compare CSV vs FIT**:
   ```bash
   phl compare
   # Options:
   #   --tolerance-seconds INT      Override timestamp tolerance
   ```

### Output Files

All files written to `output/` (configurable):

- **`weight_consolidated.csv`**: Complete dataset with lineage (JSON-serialized lists/dicts)
- **`weight_consolidated.parquet`**: Same data, native types (lists/maps preserved)
- **`conflicts.csv`**: Records with CSV vs FIT discrepancies
- **`comparison_summary.json`**: Detailed quality metrics per file pair
- **`ingestion_log.jsonl`**: Processing events (success/errors per file)

### Data Lineage Fields

Every consolidated record includes:

| Field                  | Type           | Description                                      |
|------------------------|----------------|--------------------------------------------------|
| `record_id`            | string         | Deterministic hash (timestamp + weight + sources)|
| `timestamp`            | datetime       | Measurement time (timezone-aware)                |
| `weight_kg`            | float          | Consolidated weight value                        |
| `source_files`         | list[string]   | Original file names                              |
| `source_types`         | set[enum]      | {"csv", "fit"}                                   |
| `drive_file_ids`       | list[string]   | Google Drive file IDs                            |
| `ingestion_timestamp`  | datetime       | Processing timestamp (UTC)                       |
| `field_sources`        | dict           | Field → "csv"/"fit"/"merged"/"conflict"          |
| `conflicting_fields`   | list[string]   | Fields with CSV≠FIT                              |
| `weight_kg_csv`        | float          | CSV value (if conflict)                          |
| `weight_kg_fit`        | float          | FIT value (if conflict)                          |

## Development

### Running Tests

```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src/personal_health_ledger --cov-report=term

# Specific test file
pytest tests/test_consolidation.py -v
```

**Note**: Tests use `raise AssertionError(...)` instead of `assert` statements (project requirement).

### Type Checking

```bash
# Mypy (strict mode)
mypy src/

# Pyright (strict mode)
pyright src/
```

### Linting

```bash
# Ruff (configured in pyproject.toml)
ruff check src/ tests/

# Auto-fix
ruff check src/ tests/ --fix
```

### Code Style

- **Line length**: 100 characters max
- **Docstrings**: Required for all modules, classes, functions
- **Type hints**: Mandatory (strict mode enforced)
- **Naming**: PEP 8 compliant
- **No hardcoded values**: Everything in `config.yaml`

## Testing Strategy

Tests cover:

✅ CSV normalization (Spanish column names, comma decimals)  
✅ FIT parsing (timestamp extraction, field mapping)  
✅ Consolidation (CSV-only, FIT-only, merged, conflicts)  
✅ Lineage validation (source files, Drive IDs, field sources preserved)  
✅ Comparison (matching, CSV-only, FIT-only, mismatches, MAE)

## Extensibility

The architecture is designed for future growth:

### Adding New Domains (e.g., Sleep, Activities)

1. Create `domain/sleep.py` with canonical models
2. Add parsers in `infrastructure/parsers/`
3. Implement consolidation in `services/`
4. Add CLI commands in `cli/main.py`

### Adding New Sources (e.g., Apple Health, Fitbit)

1. Create client in `infrastructure/<source>_client/`
2. Add parsers with same interface
3. Configuration in `config/config.yaml`

### Design Principles

- **Domain-driven**: Business logic in `domain/` and `services/`
- **Infrastructure isolation**: External systems in `infrastructure/`
- **Configuration over code**: No hardcoded values
- **Type safety**: Pydantic models + strict type checking
- **Observability**: Structured logging + event tracking

## Troubleshooting

### Common Issues

**"Configuration file not found"**
```bash
# Ensure you're in the project root
cd personal-health-ledger
phl all
```

**"Folder 'Health Sync Weight' not found"**
```bash
# Use folder ID directly
phl sync --folder-id "1A2B3C4D5E..."
```

**"Authentication failed"**
```bash
# For OAuth2: Delete token and re-authenticate
rm config/token.json
phl sync  # Will open browser

# For Service Account: Check email is shared with folder
```

**"No records parsed from raw files"**
```bash
# Check file formats in data/raw/
ls -la data/raw/
# Expected: *.csv and *.fit files

# Check logs
tail -f output/app.log
```

## License

[Include your license here]

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Run tests and type checks
4. Submit a pull request

Ensure all CI checks pass (lint + typecheck + tests).

## Future Roadmap

- [ ] Activities domain (steps, distance, calories)
- [ ] Sleep domain (duration, quality, stages)
- [ ] Apple Health integration
- [ ] Web dashboard (FastAPI + React)
- [ ] Anomaly detection (statistical outliers)
- [ ] Data export to standard formats (FHIR, HL7)
- [ ] Multi-user support with authentication

---

**Built with**: Python 3.10+ • Pydantic • Pandas • Typer • Google Drive API • fitparse
A modular, auditable data ledger for consolidating personal health data from multiple sources into a canonical, reproducible dataset.
