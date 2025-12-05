"""
Gold Layer - Business aggregations and metrics
"""
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from configs.db_config import get_connection, save_to_db
from utils.logger import setup_logger
import warnings

# Suppress pandas SQLAlchemy warning for psycopg2 connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

logger = setup_logger(__name__)


class GoldLayer:
    """Gold Layer: Creates business-ready aggregations"""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Gold Layer"""
        self.config = config
        self.gold_config = config.get('gold', {})
        self.source_schema = self.gold_config.get('source_schema', 'silver')
        self.target_schema = self.gold_config.get('target_schema', 'gold')
        self.aggregations = self.gold_config.get('aggregations', {})

        logger.info("Gold Layer initialized")

    def run(self) -> Dict[str, Any]:
        """Execute Gold Layer processing"""
        logger.info("=" * 70)
        logger.info("GOLD LAYER - Starting")
        logger.info("=" * 70)

        results = {
            'status': 'success',
            'aggregations_processed': 0,
            'total_rows': 0,
            'errors': []
        }

        for agg_name, agg_config in self.aggregations.items():
            try:
                logger.info(f"\nProcessing aggregation: {agg_name}")
                rows_created = self._process_aggregation(agg_name, agg_config)

                results['aggregations_processed'] += 1
                results['total_rows'] += rows_created

                logger.info(f"Successfully created {rows_created} rows for {agg_name}")

            except Exception as e:
                error_msg = f"Error processing {agg_name}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Full error details:", exc_info=True)
                results['errors'].append(error_msg)
                results['status'] = 'partial_success' if results['aggregations_processed'] > 0 else 'failed'

        logger.info("\n" + "=" * 70)
        logger.info("GOLD LAYER - Complete")
        logger.info(f"Aggregations Processed: {results['aggregations_processed']}")
        logger.info(f"Total Rows: {results['total_rows']}")
        logger.info("=" * 70)

        return results

    def _process_aggregation(self, agg_name: str, agg_config: Dict) -> int:
        """Process a single aggregation"""
        source_table = agg_config.get('source')
        group_by = agg_config.get('group_by', [])
        metrics = agg_config.get('metrics', [])
        select_columns = agg_config.get('select_columns', [])
        filters = agg_config.get('filters', {})

        # Read from silver
        source_full = f"{self.source_schema}.{source_table}"
        df = self._read_silver_table(source_full)

        if df.empty:
            logger.warning(f"No data found in {source_full}")
            return 0

        logger.info(f"Read {len(df)} rows from {source_full}")

        # Apply filters before aggregation
        if filters:
            df = self._apply_filters(df, filters, agg_name)

        # Build aggregation
        agg_df = self._build_aggregation(df, group_by, metrics)

        # Select only specific columns for Gold (if specified)
        if select_columns:
            agg_df = self._select_gold_columns(agg_df, select_columns)

        # Remove unwanted columns
        agg_df = self._clean_for_gold(agg_df)

        # Add Gold metadata
        agg_df['created_at'] = datetime.now()
        agg_df['updated_at'] = datetime.now()

        logger.info(f"Final DataFrame columns: {list(agg_df.columns)}")

        # Write to gold
        self._write_to_gold(agg_df, agg_name)

        return len(agg_df)

    def _apply_filters(self, df: pd.DataFrame, filters: Dict, agg_name: str) -> pd.DataFrame:
        """Apply filters to DataFrame before aggregation"""
        initial_count = len(df)

        # Filter out extras
        if 'exclude_extras' in filters and filters['exclude_extras']:
            df = self._exclude_extras(df)
            # Changed arrow to -> for Windows compatibility
            logger.info(f"  Filtered extras: {initial_count} -> {len(df)} rows ({initial_count - len(df)} extras removed)")

        # Add more filter types as needed
        if 'exclude_nulls' in filters:
            columns = filters['exclude_nulls']
            df = df.dropna(subset=columns)
            logger.info(f"  Removed null values in {columns}")

        return df

    def _exclude_extras(self, df: pd.DataFrame) -> pd.DataFrame:
        """Exclude extras (wides, no-balls, leg-byes, byes) from DataFrame"""
        if 'score' not in df.columns and 'event' not in df.columns:
            logger.warning("  Cannot filter extras - 'score' and 'event' columns not found")
            return df

        # Define extra keywords (case-insensitive)
        extras_keywords = [
            'wide',
            'no ball',
            'noball',
            'leg bye',
            'legbye',
            'leg-bye',
            'bye',
            'byes',
            'penalty'
        ]

        # Create filter mask
        mask = pd.Series([True] * len(df), index=df.index)

        # Check 'score' column if it exists
        if 'score' in df.columns:
            score_mask = ~df['score'].fillna('').str.lower().str.contains(
                '|'.join(extras_keywords),
                case=False,
                na=False,
                regex=True
            )
            mask = mask & score_mask

        # Check 'event' column if it exists
        if 'event' in df.columns:
            event_mask = ~df['event'].fillna('').str.lower().str.contains(
                '|'.join(extras_keywords),
                case=False,
                na=False,
                regex=True
            )
            mask = mask & event_mask

        # Apply filter
        filtered_df = df[mask].copy()

        extras_count = len(df) - len(filtered_df)
        if extras_count > 0:
            logger.info(f"  Excluded {extras_count} extras from aggregation")

        return filtered_df

    def _select_gold_columns(self, df: pd.DataFrame, select_columns: list) -> pd.DataFrame:
        """Select only specified columns for Gold layer"""
        columns_to_keep = [col for col in select_columns if col in df.columns]

        if columns_to_keep:
            df = df[columns_to_keep]
            logger.info(f"  Selected {len(columns_to_keep)} columns for Gold: {columns_to_keep}")

        return df

    def _clean_for_gold(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove Silver-specific columns before writing to Gold"""
        columns_to_remove = [
            'id',
            'source_id',
            'processed_at',
            'is_valid',
            'validation_errors'
        ]

        columns_to_drop = [col for col in columns_to_remove if col in df.columns]

        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            logger.info(f"  Removed Silver audit columns: {columns_to_drop}")

        return df

    def _read_silver_table(self, table_name: str) -> pd.DataFrame:
        """Read data from silver table"""
        query = f"SELECT * FROM {table_name} WHERE is_valid = TRUE"

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn)
            return df
        finally:
            conn.close()

    def _build_aggregation(self, df: pd.DataFrame, group_by: list, metrics: list) -> pd.DataFrame:
        """Build aggregation based on configuration"""

        # Check if DataFrame is empty after filtering
        if df.empty:
            logger.warning("DataFrame is empty after filtering - returning empty result")
            if group_by and metrics:
                columns = group_by + [m.get('name') for m in metrics]
                return pd.DataFrame(columns=columns)
            return pd.DataFrame()

        # Special case: No metrics means just select distinct rows
        if not metrics or len(metrics) == 0:
            if group_by:
                result = df[group_by].drop_duplicates().reset_index(drop=True)
                logger.info(f"  Selected {len(result)} distinct rows (no aggregation)")
            else:
                result = df.copy()
                logger.info(f"  Returned {len(result)} rows (no aggregation)")
            return result

        # Normal case: Build aggregation with metrics
        agg_dict = {}

        for metric in metrics:
            metric_name = metric.get('name')
            metric_type = metric.get('type')
            column = metric.get('column')

            # Validate that the column exists
            if column and column not in df.columns:
                logger.error(f"Column '{column}' not found in DataFrame")
                logger.error(f"Available columns: {list(df.columns)}")
                raise KeyError(f"Column '{column}' not found for metric '{metric_name}'")

            if metric_type == 'count':
                if column:
                    agg_dict[metric_name] = (column, 'count')
                else:
                    agg_dict[metric_name] = (df.columns[0], 'count')
            elif metric_type == 'count_distinct':
                agg_dict[metric_name] = (column, 'nunique')
            elif metric_type == 'sum':
                agg_dict[metric_name] = (column, 'sum')
            elif metric_type == 'avg':
                agg_dict[metric_name] = (column, 'mean')
            elif metric_type == 'min':
                agg_dict[metric_name] = (column, 'min')
            elif metric_type == 'max':
                agg_dict[metric_name] = (column, 'max')

        # Group and aggregate
        try:
            if group_by:
                # Validate group_by columns exist
                missing_cols = [col for col in group_by if col not in df.columns]
                if missing_cols:
                    logger.error(f"Group by columns not found: {missing_cols}")
                    logger.error(f"Available columns: {list(df.columns)}")
                    raise KeyError(f"Group by columns not found: {missing_cols}")

                result = df.groupby(group_by, dropna=False).agg(**agg_dict).reset_index()
            else:
                result = df.agg(**agg_dict).to_frame().T

            logger.info(f"  Aggregated to {len(result)} rows")
            return result

        except Exception as e:
            logger.error(f"Error during aggregation: {e}")
            logger.error(f"DataFrame shape: {df.shape}")
            logger.error(f"DataFrame columns: {list(df.columns)}")
            logger.error(f"Group by: {group_by}")
            logger.error(f"Agg dict: {agg_dict}")
            raise

    def _write_to_gold(self, df: pd.DataFrame, table_name: str):
        """Write DataFrame to gold table"""
        schema = self.target_schema
        table = table_name

        try:
            save_to_db(schema, table, df)
            logger.info(f"Successfully wrote {len(df)} rows to {schema}.{table}")

        except Exception as e:
            logger.error(f"Error writing to {schema}.{table}: {e}")
            logger.error(f"DataFrame columns: {list(df.columns)}")
            logger.error(f"DataFrame shape: {df.shape}")
            raise