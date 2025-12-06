"""
Silver Layer - Data cleansing, validation, and transformation
"""
import pandas as pd
import re
from typing import List, Dict, Any
from datetime import datetime
from configs.db_config import save_to_db, get_connection
from utils.logger import setup_logger
import os
import warnings

# Suppress pandas SQLAlchemy warning for psycopg2 connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

logger = setup_logger(__name__)


class SilverLayer:
    """Silver Layer: Cleanses, validates, and transforms data"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Silver Layer

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.silver_config = config.get('silver', {})
        self.source_schema = self.silver_config.get('source_schema', 'bronze')
        self.target_schema = self.silver_config.get('target_schema', 'silver')
        self.transformations = self.silver_config.get('transformations', {})

        logger.info("Silver Layer initialized")

    def run(self) -> Dict[str, Any]:
        """
        Execute Silver Layer processing

        Returns:
            Dictionary with processing results
        """
        logger.info("=" * 70)
        logger.info("SILVER LAYER - Starting")
        logger.info("=" * 70)

        results = {
            'status': 'success',
            'tables_processed': 0,
            'total_rows': 0,
            'errors': []
        }

        for table_name, transforms in self.transformations.items():
            try:
                logger.info(f"\nProcessing table: {table_name}")
                rows_processed = self._process_table(table_name, transforms)

                results['tables_processed'] += 1
                results['total_rows'] += rows_processed

                logger.info(f"{table_name}: {rows_processed} rows processed")

            except Exception as e:
                error_msg = f"Error processing {table_name}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['status'] = 'partial_success' if results['tables_processed'] > 0 else 'failed'

        logger.info("\n" + "=" * 70)
        logger.info("SILVER LAYER - Complete")
        logger.info(f"Tables Processed: {results['tables_processed']}")
        logger.info(f"Total Rows: {results['total_rows']}")
        logger.info("=" * 70)

        return results

    def _process_table(self, table_name: str, transforms: List[Dict]) -> int:
        """
        Process a single table through transformations

        Args:
            table_name: Name of the table
            transforms: List of transformation configurations

        Returns:
            Number of rows processed
        """
        # Read from bronze
        source_table = f"{self.source_schema}.{table_name}"
        df = self._read_bronze_table(source_table)

        if df.empty:
            logger.warning(f"No data found in {source_table}")
            return 0

        logger.info(f"Read {len(df)} rows from {source_table}")
        initial_count = len(df)

        # Apply transformations
        for transform in transforms:
            transform_type = transform.get('type')

            if transform_type == 'clean':
                df = self._clean(df, transform)
            elif transform_type == 'validate':
                df = self._validate(df, transform)
            elif transform_type == 'convert_score':
                df = self._convert_score(df, transform)
            elif transform_type == 'extract_dismissal':  # NEW
                df = self._extract_dismissal_info(df, transform)

        logger.info(f"After transformations: {len(df)} rows (removed {initial_count - len(df)})")

        # Remove Bronze audit columns (they don't belong in Silver)
        df = self._remove_bronze_audit_columns(df)

        # Add Silver processing metadata
        df['processed_at'] = datetime.now()
        df['is_valid'] = True
        df['validation_errors'] = None

        # Add Silver audit columns (different from Bronze)
        df['source_id'] = df.get('id', None)  # Keep reference to Bronze ID
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()

        print(f"Processed {len(df)} rows")

        # Write to silver
        save_to_db(self.target_schema, table_name, df)

        return len(df)

    def _remove_bronze_audit_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove Bronze layer audit columns that don't belong in Silver schema

        Args:
            df: DataFrame with Bronze columns

        Returns:
            DataFrame with Bronze audit columns removed
        """
        # Columns to remove (Bronze-specific audit columns)
        bronze_audit_columns = [
            'ingestion_timestamp',
            'source_system',
            'pipeline_run_id',
            'is_active'
        ]

        # Remove columns that exist in the DataFrame
        columns_to_drop = [col for col in bronze_audit_columns if col in df.columns]

        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            logger.info(f"  Removed Bronze audit columns: {columns_to_drop}")

        return df

    def _extract_dismissal_info(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Extract dismissal method and fielder from commentary

        Parses dismissal information from commentary text after #**# pattern
        Example: "#**#Angkrish Raghuvanshi c †Sharma b Yash Dayal 30..."

        Args:
            df: DataFrame to process
            config: Configuration (optional)

        Returns:
            DataFrame with new columns: dismissal_method, fielder_name
        """
        score_column = config.get('score_column', 'score')
        commentary_column = config.get('commentary_column', 'commentary')

        if score_column not in df.columns or commentary_column not in df.columns:
            logger.warning(f"  Required columns not found for dismissal extraction")
            return df

        logger.info(f"  Extracting dismissal information...")

        # Initialize new columns
        df['dismissal_method'] = None
        df['fielder_name'] = None

        extracted_count = 0

        for idx, row in df.iterrows():
            score = str(row[score_column]).strip().upper() if pd.notna(row[score_column]) else ''
            commentary = str(row[commentary_column]) if pd.notna(row[commentary_column]) else ''

            # Only process OUT entries
            if 'OUT' not in score:
                continue

            # Extract dismissal details from commentary after #**#
            if '#**#' in commentary:
                dismissal_text = commentary.split('#**#')[1].strip()

                # Parse dismissal information
                method, fielder = self._parse_dismissal_text(dismissal_text)

                df.at[idx, 'dismissal_method'] = method
                df.at[idx, 'fielder_name'] = fielder

                if method:
                    extracted_count += 1

        logger.info(f"  Extracted dismissal info for {extracted_count} wickets")

        return df

    def _parse_dismissal_text(self, dismissal_text: str) -> tuple:
        """
        Parse dismissal text to extract method and fielder

        Examples:
        - "c †Sharma b Yash Dayal" -> ("caught", "Sharma")
        - "b Yash Dayal" -> ("bowled", None)
        - "lbw b Bumrah" -> ("lbw", None)
        - "st †Dhoni b Chahal" -> ("stumped", "Dhoni")
        - "run out (Jadeja)" -> ("run out", "Jadeja")

        Args:
            dismissal_text: Text containing dismissal information

        Returns:
            Tuple of (dismissal_method, fielder_name)
        """
        method = None
        fielder = None

        try:
            # Remove extra whitespace
            text = ' '.join(dismissal_text.split())

            # Caught (c or caught)
            if re.search(r'\bc\s+', text) or 'caught' in text.lower():
                method = 'caught'
                # Extract fielder name after 'c' or 'caught'
                # Pattern: c †FielderName or c FielderName
                match = re.search(r'c\s+†?([A-Za-z\s]+?)\s+b\s+', text)
                if match:
                    fielder = match.group(1).strip()

            # Stumped (st)
            elif re.search(r'\bst\s+', text) or 'stumped' in text.lower():
                method = 'stumped'
                # Extract wicketkeeper name
                match = re.search(r'st\s+†?([A-Za-z\s]+?)\s+b\s+', text)
                if match:
                    fielder = match.group(1).strip()

            # Run out
            elif 'run out' in text.lower():
                method = 'run out'
                # Extract fielder name in parentheses
                match = re.search(r'run out\s*\(([^)]+)\)', text, re.IGNORECASE)
                if match:
                    fielder = match.group(1).strip()

            # Bowled
            elif re.search(r'\bb\s+', text) and not method:
                method = 'bowled'

            # LBW
            elif 'lbw' in text.lower():
                method = 'lbw'

            # Hit wicket
            elif 'hit wicket' in text.lower():
                method = 'hit wicket'

            # Obstructing the field
            elif 'obstructing' in text.lower():
                method = 'obstructing the field'

            # Handled the ball
            elif 'handled' in text.lower():
                method = 'handled the ball'

            # Hit the ball twice
            elif 'hit the ball twice' in text.lower():
                method = 'hit the ball twice'

            # Timed out
            elif 'timed out' in text.lower():
                method = 'timed out'

            # Retired hurt (not a dismissal but tracked)
            elif 'retired' in text.lower():
                method = 'retired hurt'

        except Exception as e:
            logger.debug(f"  Error parsing dismissal text '{dismissal_text}': {e}")

        return method, fielder

    def _convert_score(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Convert score column from string to integer

        Args:
            df: DataFrame to process
            config: Configuration with mappings

        Returns:
            DataFrame with converted score column
        """
        column = config.get('column', 'score')
        mappings = config.get('mappings', {})

        if column not in df.columns:
            logger.warning(f"  Column '{column}' not found, skipping score conversion")
            return df

        logger.info(f"  Converting '{column}' column to numeric...")

        # Create a new column for numeric runs
        df['runs_scored'] = 0

        # Track conversion stats
        converted = 0
        not_converted = 0

        for idx, value in df[column].items():
            if pd.isna(value):
                df.at[idx, 'runs_scored'] = 0
                continue

            value_str = str(value).strip().lower()

            # Check direct mappings first
            matched = False
            for pattern, runs in mappings.items():
                if pattern.lower() in value_str or value_str == pattern.lower():
                    df.at[idx, 'runs_scored'] = runs
                    converted += 1
                    matched = True
                    break

            if matched:
                continue

            # Try to extract number using regex
            number_match = re.search(r'(\d+)', value_str)
            if number_match:
                df.at[idx, 'runs_scored'] = int(number_match.group(1))
                converted += 1
            elif 'four' in value_str:
                df.at[idx, 'runs_scored'] = 4
                converted += 1
            elif 'six' in value_str:
                df.at[idx, 'runs_scored'] = 6
                converted += 1
            elif 'no run' in value_str or value_str == '':
                df.at[idx, 'runs_scored'] = 0
                converted += 1
            else:
                df.at[idx, 'runs_scored'] = 0
                not_converted += 1

        logger.info(f"  Score conversion: {converted} converted, {not_converted} not converted (set to 0)")

        return df

    def _read_bronze_table(self, table_name: str) -> pd.DataFrame:
        """Read data from bronze table"""
        query = f"SELECT * FROM {table_name} WHERE is_active = TRUE"

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn)
            return df
        finally:
            conn.close()

    def _clean(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Clean column values"""
        columns = config.get('columns', {})

        for column, operations in columns.items():
            if column not in df.columns:
                continue

            if not isinstance(operations, list):
                operations = [operations]

            for operation in operations:
                if operation == 'trim':
                    df[column] = df[column].str.strip()
                elif operation == 'title_case':
                    df[column] = df[column].str.title()
                elif operation == 'lower':
                    df[column] = df[column].str.lower()
                elif operation == 'upper':
                    df[column] = df[column].str.upper()

        logger.info(f"  Cleaned columns: {list(columns.keys())}")
        return df

    def _validate(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Validate data quality"""
        rules = config.get('rules', {})

        for column, rule in rules.items():
            if column not in df.columns:
                continue

            if rule == 'not_null':
                initial_count = len(df)
                df = df[df[column].notna()]
                removed = initial_count - len(df)
                if removed > 0:
                    logger.info(f"  Validation: Removed {removed} rows with null {column}")

        return df