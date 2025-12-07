"""
Gold Layer - Business aggregations and metrics with SCD Type 2 support using SQL MERGE/UPSERT
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
    """Gold Layer: Creates business-ready aggregations and dimensions"""

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

        # Check if this is SCD Type 2 aggregation
        if agg_config.get('type') == 'scd_type2':
            return self._process_scd_type2_sql(agg_name, agg_config)

        # Normal aggregation processing
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

    # ========================================================================
    # SCD TYPE 2 METHODS (SQL-BASED)
    # ========================================================================

    def _process_scd_type2_sql(self, agg_name: str, agg_config: Dict) -> int:
        """
        Process SCD Type 2 using SQL MERGE/UPSERT approach

        Steps:
        1. Create temporary staging table
        2. Load new data into staging
        3. Use SQL to perform SCD Type 2 logic:
           - Close old records (set effective_to, is_current=FALSE)
           - Insert new records
           - Update unchanged records
        """
        logger.info(f"Processing SCD Type 2 (SQL MERGE) for {agg_name}")

        source_table = agg_config.get('source')

        # Read source data from Silver
        source_full = f"{self.source_schema}.{source_table}"
        df_source = self._read_silver_table(source_full)

        if df_source.empty:
            logger.warning(f"No data found in {source_full}")
            return 0

        logger.info(f"Read {len(df_source)} rows from {source_full}")

        # Build the dimension data
        df_new = self._build_player_team_dimension(df_source)

        if df_new.empty:
            logger.warning("No dimension data to process")
            return 0

        logger.info(f"Built {len(df_new)} dimension records")

        # Execute SQL-based SCD Type 2 merge
        records_processed = self._execute_scd_merge(df_new, agg_name)

        return records_processed

    def _execute_scd_merge(self, df_new: pd.DataFrame, table_name: str) -> int:
        """
        Execute SCD Type 2 merge using SQL

        Logic:
        1. Create temporary staging table
        2. Load new data into staging
        3. Close old records that changed or disappeared
        4. Insert new records
        5. Update unchanged records (increment counters)
        """
        conn = get_connection()
        cursor = conn.cursor()

        staging_table = f"{table_name}_staging"
        target_table = f"{self.target_schema}.{table_name}"

        try:
            # Step 1: Create staging table (TEMPORARY TABLE)
            logger.info("Creating staging table...")
            cursor.execute(f"""
                DROP TABLE IF EXISTS {staging_table};
                CREATE TEMP TABLE {staging_table} (
                    player_name VARCHAR(100),
                    team VARCHAR(100),
                    season INT,
                    series VARCHAR(255),
                    first_match_date DATE,
                    last_match_date DATE,
                    matches_played INT,
                    is_impact_player BOOLEAN,
                    effective_from DATE,
                    effective_to DATE,
                    is_current BOOLEAN,
                    version INT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
            """)
            conn.commit()

            # Step 2: Load data into staging
            logger.info("Loading data into staging table...")
            df_staging = df_new.copy()

            # Remove id column if present
            if 'id' in df_staging.columns:
                df_staging = df_staging.drop(columns=['id'])

            # Insert data into staging table
            for _, row in df_staging.iterrows():
                cursor.execute(f"""
                    INSERT INTO {staging_table} (
                        player_name, team, season, series,
                        first_match_date, last_match_date, matches_played, is_impact_player,
                        effective_from, effective_to, is_current, version,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['player_name'],
                    row['team'],
                    row['season'],
                    row.get('series'),
                    row['first_match_date'],
                    row['last_match_date'],
                    row['matches_played'],
                    row['is_impact_player'],
                    row['effective_from'],
                    row.get('effective_to'),
                    row['is_current'],
                    row['version'],
                    row['created_at'],
                    row['updated_at']
                ))

            conn.commit()
            logger.info(f"Loaded {len(df_staging)} records into staging")

            # Step 3: Close old records that changed or are no longer present
            logger.info("Closing old records...")
            close_query = f"""
                UPDATE {target_table} tgt
                SET 
                    is_current = FALSE,
                    effective_to = CURRENT_DATE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE tgt.is_current = TRUE
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM {staging_table} stg
                      WHERE stg.player_name = tgt.player_name
                        AND stg.team = tgt.team
                        AND stg.season = tgt.season
                  );
            """
            cursor.execute(close_query)
            closed_count = cursor.rowcount
            logger.info(f"Closed {closed_count} old records")

            # Step 4: Insert new records (player-team-season combinations that don't exist)
            logger.info("Inserting new records...")
            insert_query = f"""
                INSERT INTO {target_table} (
                    player_name, team, season, series,
                    first_match_date, last_match_date, matches_played, is_impact_player,
                    effective_from, effective_to, is_current, version,
                    created_at, updated_at
                )
                SELECT 
                    stg.player_name,
                    stg.team,
                    stg.season,
                    stg.series,
                    stg.first_match_date,
                    stg.last_match_date,
                    stg.matches_played,
                    stg.is_impact_player,
                    stg.effective_from,
                    stg.effective_to,
                    stg.is_current,
                    stg.version,
                    stg.created_at,
                    stg.updated_at
                FROM {staging_table} stg
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {target_table} tgt
                    WHERE tgt.player_name = stg.player_name
                      AND tgt.team = stg.team
                      AND tgt.season = stg.season
                      AND tgt.is_current = TRUE
                );
            """
            cursor.execute(insert_query)
            inserted_count = cursor.rowcount
            logger.info(f"Inserted {inserted_count} new records")

            # Step 5: Update existing current records (increment matches, update dates)
            logger.info("Updating existing records...")
            update_query = f"""
                UPDATE {target_table} tgt
                SET 
                    matches_played = tgt.matches_played + stg.matches_played,
                    last_match_date = stg.last_match_date,
                    is_impact_player = CASE 
                        WHEN stg.is_impact_player = TRUE THEN TRUE 
                        ELSE tgt.is_impact_player 
                    END,
                    updated_at = CURRENT_TIMESTAMP
                FROM {staging_table} stg
                WHERE tgt.player_name = stg.player_name
                  AND tgt.team = stg.team
                  AND tgt.season = stg.season
                  AND tgt.is_current = TRUE;
            """
            cursor.execute(update_query)
            updated_count = cursor.rowcount
            logger.info(f"Updated {updated_count} existing records")

            # Commit all changes
            conn.commit()

            total_processed = closed_count + inserted_count + updated_count
            logger.info(f"SCD Type 2 merge complete: {closed_count} closed, {inserted_count} inserted, {updated_count} updated")

            return total_processed

        except Exception as e:
            conn.rollback()
            logger.error(f"Error during SCD merge: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def _build_player_team_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build player-team dimension from match_players data

        Aggregates:
        - player_name, team, season
        - matches_played (count)
        - is_impact_player (any TRUE = TRUE)
        - first_match_date, last_match_date
        """

        # Check required columns
        required_cols = ['player_name', 'team']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return pd.DataFrame()

        # Get season from match_metadata if not in players table
        if 'season' not in df.columns:
            logger.info("Season not in match_players, fetching from match_metadata")
            df = self._enrich_with_season(df)

        # Group by player, team, season
        group_cols = ['player_name', 'team', 'season']

        # Build aggregation dictionary
        agg_dict = {
            'matchid': 'count'  # This will become matches_played
        }

        # Add is_impact_player if column exists
        if 'is_impact_player' in df.columns:
            agg_dict['is_impact_player'] = 'max'  # True if ever impact player

        # Aggregate
        df_agg = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()

        # Rename columns
        df_agg.rename(columns={'matchid': 'matches_played'}, inplace=True)

        # Add default is_impact_player if not present
        if 'is_impact_player' not in df_agg.columns:
            df_agg['is_impact_player'] = False

        # Convert is_impact_player to boolean
        df_agg['is_impact_player'] = df_agg['is_impact_player'].astype(bool)

        # Add SCD Type 2 metadata
        df_agg['effective_from'] = datetime.now().date()
        df_agg['effective_to'] = None
        df_agg['is_current'] = True
        df_agg['version'] = 1
        df_agg['first_match_date'] = datetime.now().date()
        df_agg['last_match_date'] = datetime.now().date()
        df_agg['series'] = None
        df_agg['created_at'] = datetime.now()
        df_agg['updated_at'] = datetime.now()

        logger.info(f"Built dimension with {len(df_agg)} player-team-season combinations")

        return df_agg

    def _enrich_with_season(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich player data with season from match_metadata"""
        try:
            query = f"""
                SELECT matchid, season 
                FROM {self.source_schema}.match_metadata
            """

            conn = get_connection()
            df_metadata = pd.read_sql(query, conn)
            conn.close()

            # Merge to get season
            df = df.merge(df_metadata, on='matchid', how='left')

            # Fill missing seasons with current year
            df['season'] = df['season'].fillna(datetime.now().year)

            logger.info("Enriched with season data")
            return df

        except Exception as e:
            logger.warning(f"Could not enrich with season: {e}")
            # Use current year as default
            df['season'] = datetime.now().year
            return df

    # ========================================================================
    # STANDARD AGGREGATION METHODS
    # ========================================================================

    def _apply_filters(self, df: pd.DataFrame, filters: Dict, agg_name: str) -> pd.DataFrame:
        """Apply filters to DataFrame before aggregation"""
        initial_count = len(df)

        # Filter out extras
        if 'exclude_extras' in filters and filters['exclude_extras']:
            df = self._exclude_extras(df)
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