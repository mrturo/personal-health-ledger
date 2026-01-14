"""
Daily consolidation service.

Consolidates measurements by day, computing averages for numeric fields.
"""

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from personal_health_ledger.utils.exceptions import ConsolidationError

logger = logging.getLogger(__name__)


class DailyConsolidationService:
    """
    Service for consolidating measurements by day.
    
    For each day, computes the average of all non-null numeric values.
    """

    def consolidate_by_day(self, input_file: Path, output_file: Path) -> None:
        """
        Consolidate measurements by day.
        
        Args:
            input_file: Path to input consolidated CSV file.
            output_file: Path to output daily consolidated CSV file.
            
        Raises:
            ConsolidationError: If consolidation fails.
        """
        try:
            logger.info(f"Reading consolidated data from {input_file}")
            df = pd.read_csv(input_file)
            
            if df.empty:
                logger.warning("Input file is empty")
                return
            
            # Parse timestamp and extract date
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date
            
            # Define numeric columns to average
            numeric_cols = [
                'weight_kg',
                'body_fat_pct',
                'fat_mass_kg',
                'fat_free_pct',
                'fat_free_mass_kg',
                'skeletal_muscle_pct',
                'skeletal_muscle_mass_kg',
                'muscle_pct',
                'muscle_mass_kg',
                'bone_mass_kg',
                'body_water',
                'bmr_kcal',
                'metabolic_age',
                'visceral_fat_rating',
            ]
            
            # Filter only columns that exist in the dataframe
            numeric_cols = [col for col in numeric_cols if col in df.columns]
            
            # Group by date and compute averages for numeric columns
            logger.info(f"Consolidating {len(df)} records into daily averages")
            
            daily_agg = df.groupby('date')[numeric_cols].mean().reset_index()
            
            # Add metadata: count of records per day and source types
            record_counts = df.groupby('date').size().reset_index(name='record_count')
            daily_agg = daily_agg.merge(record_counts, on='date')
            
            # Collect unique source types per day
            def collect_source_types(series: pd.Series) -> list[str]:
                """Collect all unique source types from a series of JSON strings."""
                import json
                types: set[str] = set()
                for val in series:
                    if pd.notna(val):
                        try:
                            types.update(json.loads(val))
                        except (json.JSONDecodeError, TypeError):
                            pass
                return sorted(list(types))
            
            if 'source_types' in df.columns:
                source_types_per_day = df.groupby('date')['source_types'].apply(
                    collect_source_types
                ).reset_index()
                source_types_per_day['source_types'] = source_types_per_day['source_types'].apply(
                    lambda x: ','.join(x) if x else ''
                )
                daily_agg = daily_agg.merge(source_types_per_day, on='date')
            
            # Sort by date
            daily_agg = daily_agg.sort_values('date')
            
            # Round numeric values to reasonable precision
            for col in numeric_cols:
                if col in daily_agg.columns:
                    daily_agg[col] = daily_agg[col].round(2)
            
            # Save to output file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            daily_agg.to_csv(output_file, index=False)
            
            logger.info(
                f"Consolidated {len(df)} records into {len(daily_agg)} daily averages"
            )
            logger.info(f"Wrote daily consolidated data to {output_file}")
            
        except Exception as e:
            raise ConsolidationError(
                f"Failed to consolidate by day: {e}"
            ) from e
