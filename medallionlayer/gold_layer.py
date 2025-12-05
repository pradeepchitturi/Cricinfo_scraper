"""
Gold Layer - Business aggregations and metrics
"""
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from configs.db_config import save_to_db,get_connection
from utils.logger import setup_logger
import os
import warnings

# Suppress pandas SQLAlchemy warning for psycopg2 connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')


logger = setup_logger(__name__)


class GoldLayer:
    """Gold Layer: Creates business-ready aggregations"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Gold Layer

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.gold_config = config.get('gold', {})
        self.source_schema = self.gold_config.get('source_schema', 'silver')
        self.target_schema = self.gold_config.get('target_schema', 'gold')
        self.aggregations = self.gold_config.get('aggregations', {})

        logger.info("Gold Layer initialized")

    def run(self) -> Dict[str, Any]:
        """
        Execute Gold Layer processing

        Returns:
            Dictionary with processing results
        """
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

                logger.info(f"{agg_name}: {rows_created} rows created")

            except Exception as e:
                error_msg = f"Error processing {agg_name}: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['status'] = 'partial_success' if results['aggregations_processed'] > 0 else 'failed'

        logger.info("\n" + "=" * 70)
        logger.info("GOLD LAYER - Complete")
        logger.info(f"Aggregations Processed: {results['aggregations_processed']}")
        logger.info(f"Total Rows: {results['total_rows']}")
        logger.info("=" * 70)

        return results

    def _process_aggregation(self, agg_name: str, agg_config: Dict) -> int:
        """
        Process a single aggregation

        Args:
            agg_name: Name of the aggregation
            agg_config: Aggregation configuration

        Returns:
            Number of rows created
        """
        source_table = agg_config.get('source')
        group_by = agg_config.get('group_by', [])
        metrics = agg_config.get('metrics', [])

        # Read from silver
        source_full = f"{self.source_schema}.{source_table}"
        df = self._read_silver_table(source_full)

        if df.empty:
            logger.warning(f"No data found in {source_full}")
            return 0

        logger.info(f"Read {len(df)} rows from {source_full}")

        # Build aggregation
        agg_df = self._build_aggregation(df, group_by, metrics)

        # Add metadata
        agg_df['created_at'] = datetime.now()
        agg_df['updated_at'] = datetime.now()

        # Write to gold
        save_to_db(self.target_schema, agg_name, df)
        #target_table = f"{self.target_schema}.{agg_name}"
        #self._write_to_gold(agg_df, target_table)

        return len(agg_df)

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
        agg_dict = {}

        for metric in metrics:
            metric_name = metric.get('name')
            metric_type = metric.get('type')
            column = metric.get('column')

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
        if group_by:
            result = df.groupby(group_by).agg(**agg_dict).reset_index()
        else:
            result = df.agg(**agg_dict).to_frame().T

        logger.info(f"  Aggregated to {len(result)} rows")
        return result
