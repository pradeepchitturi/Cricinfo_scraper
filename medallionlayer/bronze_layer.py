"""
Bronze Layer - Raw data ingestion with data quality checks
"""
import pandas as pd
from typing import List, Dict, Any, Tuple
import uuid
from datetime import datetime
from configs.db_config import save_to_db,get_connection
from utils.logger import setup_logger
import os
import warnings

# Suppress pandas SQLAlchemy warning for psycopg2 connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

logger = setup_logger(__name__)


class BronzeLayer:
    """Bronze Layer: Ingests raw data with quality checks and validation"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Bronze Layer

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.bronze_config = config.get('bronze', {})
        self.source_schema = self.bronze_config.get('source_schema', 'raw')
        self.target_schema = self.bronze_config.get('target_schema', 'bronze')
        self.source_tables = self.bronze_config.get('source_tables', [])
        self.settings = self.bronze_config.get('settings', {})
        self.pipeline_run_id = str(uuid.uuid4())

        # Data quality tracking
        self.data_quality_report = {
            'duplicates_removed': {},
            'nulls_removed': {},
            'missing_columns': {},
            'innings_validation': {}
        }

        logger.info(f"Bronze Layer initialized - Run ID: {self.pipeline_run_id}")

    def run(self) -> Dict[str, Any]:
        """
        Execute Bronze Layer processing with data quality checks

        Returns:
            Dictionary with processing results
        """
        logger.info("=" * 70)
        logger.info("BRONZE LAYER - Starting")
        logger.info("=" * 70)

        results = {
            'status': 'success',
            'tables_processed': 0,
            'total_rows': 0,
            'errors': [],
            'data_quality': self.data_quality_report
        }

        for table_config in self.source_tables:
            table_name = table_config.get('name')

            try:
                logger.info(f"\nProcessing table: {table_name}")
                rows_ingested = self._process_table(table_name, table_config)

                results['tables_processed'] += 1
                results['total_rows'] += rows_ingested

                logger.info(f"{table_name}: {rows_ingested} rows ingested")

            except Exception as e:
                error_msg = f"Error processing {table_name}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['status'] = 'partial_success' if results['tables_processed'] > 0 else 'failed'

        # Validate innings data if enabled
        if self.settings.get('validate_innings', False):
            self._validate_innings_data()

        # Print data quality report
        if self.settings.get('report_data_quality', False):
            self._print_data_quality_report()

        logger.info("\n" + "=" * 70)
        logger.info("BRONZE LAYER - Complete")
        logger.info(f"Tables Processed: {results['tables_processed']}/{len(self.source_tables)}")
        logger.info(f"Total Rows: {results['total_rows']}")
        logger.info("=" * 70)

        return results

    def _process_table(self, table_name: str, table_config: Dict[str, Any]) -> int:
        """
        Process a single table from raw to bronze with quality checks

        Args:
            table_name: Name of the table
            table_config: Table configuration

        Returns:
            Number of rows ingested
        """
        # Read from source
        source_table = f"{self.source_schema}.{table_name}"
        df = self._read_source_table(source_table)

        if df.empty:
            logger.warning(f"No data found in {source_table}")
            return 0

        logger.info(f"Read {len(df)} rows from {source_table}")
        initial_count = len(df)

        # Check for duplicates
        if table_config.get('check_duplicates', False):
            df, duplicates_removed = self._check_duplicates(df, table_config, table_name)
            self.data_quality_report['duplicates_removed'][table_name] = duplicates_removed

        # Check for nulls in required columns
        required_columns = table_config.get('required_columns', [])
        if self.settings.get('remove_nulls', False) and required_columns:
            df, nulls_removed = self._remove_nulls(df, required_columns, table_name)
            self.data_quality_report['nulls_removed'][table_name] = nulls_removed

        # Report missing columns
        missing_cols = self._check_missing_columns(df, required_columns)
        if missing_cols:
            self.data_quality_report['missing_columns'][table_name] = missing_cols

        logger.info(f"After quality checks: {len(df)} rows (removed {initial_count - len(df)})")

        # Add audit columns
        if self.settings.get('add_audit_columns', True):
            df = self._add_audit_columns(df)

        # Write to bronze

        # Save commentary to DB
        save_to_db(self.target_schema, table_name, df)
        #target_table = f"{self.target_schema}.{table_name}"
        #self._write_to_bronze(df, target_table)

        return len(df)

    def _check_duplicates(self, df: pd.DataFrame, table_config: Dict, table_name: str) -> Tuple[pd.DataFrame, int]:
        """
        Check for and remove duplicates based on business key

        Args:
            df: DataFrame to check
            table_config: Table configuration
            table_name: Name of the table

        Returns:
            Tuple of (cleaned DataFrame, number of duplicates removed)
        """
        business_key = table_config.get('business_key')
        if not business_key:
            return df, 0

        # Ensure business_key is a list
        if not isinstance(business_key, list):
            business_key = [business_key]

        initial_count = len(df)

        # Find duplicates
        duplicates = df.duplicated(subset=business_key, keep='last')
        num_duplicates = duplicates.sum()

        if num_duplicates > 0:
            logger.warning(f"  Found {num_duplicates} duplicate rows based on {business_key}")
            # Keep last occurrence
            df = df.drop_duplicates(subset=business_key, keep='last')
        else:
            logger.info(f"  No duplicates found based on {business_key}")

        return df, num_duplicates

    def _remove_nulls(self, df: pd.DataFrame, required_columns: List[str], table_name: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Remove rows with nulls in required columns and report counts

        Args:
            df: DataFrame to check
            required_columns: List of columns that shouldn't have nulls
            table_name: Name of the table

        Returns:
            Tuple of (cleaned DataFrame, dict of null counts per column)
        """
        null_counts = {}
        initial_count = len(df)

        for column in required_columns:
            if column not in df.columns:
                continue

            # Count nulls before removal
            null_count = df[column].isna().sum()
            if null_count > 0:
                null_counts[column] = int(null_count)
                logger.warning(f"  Found {null_count} null values in column '{column}'")

        # Remove rows with any null in required columns
        existing_required_cols = [col for col in required_columns if col in df.columns]
        df = df.dropna(subset=existing_required_cols)

        removed_count = initial_count - len(df)
        if removed_count > 0:
            logger.info(f"  Removed {removed_count} rows with null values in required columns")

        return df, null_counts

    def _check_missing_columns(self, df: pd.DataFrame, required_columns: List[str]) -> List[str]:
        """
        Check if any required columns are missing from the DataFrame

        Args:
            df: DataFrame to check
            required_columns: List of required columns

        Returns:
            List of missing column names
        """
        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            logger.warning(f"  Missing required columns: {missing}")

        return missing

    def _validate_innings_data(self):
        """
        Validate that both innings data is present for each match
        """
        logger.info("\nValidating innings data...")

        query = """
        WITH innings_count AS (
            SELECT 
                matchid,
                COUNT(DISTINCT innings) as num_innings,
                STRING_AGG(DISTINCT innings, ', ' ORDER BY innings) as innings_list
            FROM bronze.match_events
            WHERE is_active = TRUE
            GROUP BY matchid
        )
        SELECT 
            matchid,
            num_innings,
            innings_list
        FROM innings_count
        WHERE num_innings < 2
        ORDER BY matchid
        """

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn)

            if not df.empty:
                incomplete_matches = len(df)
                logger.warning(f"Found {incomplete_matches} matches with incomplete innings data")

                # Store validation results
                self.data_quality_report['innings_validation'] = {
                    'incomplete_matches': int(incomplete_matches),
                    'match_ids': df['matchid'].tolist()
                }

                # Log first few incomplete matches
                for _, row in df.head(5).iterrows():
                    logger.warning(f"    Match {row['matchid']}: Has {row['num_innings']} innings ({row['innings_list']})")

                if incomplete_matches > 5:
                    logger.warning(f"    ... and {incomplete_matches - 5} more")
            else:
                logger.info("All matches have complete innings data")
                self.data_quality_report['innings_validation'] = {
                    'incomplete_matches': 0,
                    'match_ids': []
                }
        finally:
            conn.close()

    def _print_data_quality_report(self):
        """Print comprehensive data quality report"""
        logger.info("\n" + "=" * 70)
        logger.info("DATA QUALITY REPORT")
        logger.info("=" * 70)

        # Duplicates Report
        if self.data_quality_report['duplicates_removed']:
            logger.info("\nDUPLICATES REMOVED:")
            for table, count in self.data_quality_report['duplicates_removed'].items():
                if count > 0:
                    logger.info(f"  {table}: {count} duplicate rows removed")
                else:
                    logger.info(f"  {table}: No duplicates found")

        # Nulls Report
        if self.data_quality_report['nulls_removed']:
            logger.info("\nNULL VALUES FOUND:")
            for table, null_counts in self.data_quality_report['nulls_removed'].items():
                if null_counts:
                    logger.info(f"  {table}:")
                    for column, count in null_counts.items():
                        logger.info(f"    - {column}: {count} null values")
                else:
                    logger.info(f"  {table}: No null values in required columns")

        # Missing Columns Report
        if self.data_quality_report['missing_columns']:
            logger.info("\nMISSING COLUMNS:")
            for table, columns in self.data_quality_report['missing_columns'].items():
                if columns:
                    logger.warning(f"  {table}: Missing columns - {', '.join(columns)}")

        # Innings Validation Report
        if self.data_quality_report['innings_validation']:
            logger.info("\nINNINGS VALIDATION:")
            incomplete = self.data_quality_report['innings_validation'].get('incomplete_matches', 0)
            if incomplete > 0:
                logger.warning(f"{incomplete} matches with incomplete innings data")
            else:
                logger.info(f"All matches have complete innings data")

        logger.info("=" * 70 + "\n")

    def _read_source_table(self, table_name: str) -> pd.DataFrame:
        """Read data from source table"""
        query = f"SELECT * FROM {table_name}"

        conn = get_connection()
        try:
            # Use Python's warnings filter to ignore that specific warning message
            df = pd.read_sql(query, conn)
            return df
        finally:
            conn.close()

    def _add_audit_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add audit columns to DataFrame"""
        df['ingestion_timestamp'] = datetime.now()
        df['source_system'] = 'cricinfo'
        df['pipeline_run_id'] = self.pipeline_run_id
        df['is_active'] = True

        return df

