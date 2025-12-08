"""
Gold Layer - Enhanced Cricket Analytics with Advanced Aggregations
"""
import pandas as pd
import re
from typing import Dict, Any
from datetime import datetime
from configs.db_config import get_connection, save_to_db
from utils.logger import setup_logger
import warnings

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
        """Process aggregation with custom handlers for enhanced analytics"""

        # SCD Type 2
        if agg_config.get('type') == 'scd_type2':
            return self._process_scd_type2_sql(agg_name, agg_config)

        # Custom enhanced aggregations
        if agg_name == 'match_summary':
            return self._process_match_summary()
        elif agg_name == 'series_summary':
            return self._process_series_summary()
        elif agg_name == 'match_ball_statistics':
            return self._process_match_ball_statistics()
        elif agg_name == 'match_batsman_statistics':
            return self._process_match_batsman_statistics()
        elif agg_name == 'match_bowler_statistics':
            return self._process_match_bowler_statistics()
        elif agg_name == 'batsman_statistics':
            return self._process_batsman_career_statistics()
        elif agg_name == 'bowler_statistics':
            return self._process_bowler_career_statistics()

        # Standard aggregation
        return self._process_standard_aggregation(agg_name, agg_config)

    # ========================================================================
    # ENHANCED AGGREGATION METHODS
    # ========================================================================

    def _process_match_summary(self) -> int:
        """
        Enhanced match summary with:
        - Series ID from matchid URL pattern
        - First innings score
        - Second innings score
        - Winner from points
        """
        logger.info("Processing enhanced match summary...")

        query = f"""
            SELECT 
                matchid,
                venue,
                series,
                season,
                player_of_the_match,
                first_innings,
                second_innings,
                points
            FROM {self.source_schema}.match_metadata
        """

        conn = get_connection()
        df = pd.read_sql(query, conn)

        # Get innings scores
        scores_query = f"""
            SELECT 
                matchid,
                innings,
                SUM(runs_scored) as total_score
            FROM {self.source_schema}.match_events
            GROUP BY matchid, innings
        """
        df_scores = pd.read_sql(scores_query, conn)
        conn.close()

        # Pivot scores to get first_innings_score and second_innings_score
        df_scores_pivot = df_scores.pivot(
            index='matchid',
            columns='innings',
            values='total_score'
        ).reset_index()

        df_scores_pivot.columns = ['matchid', 'first_innings_score', 'second_innings_score']

        # Merge scores
        df = df.merge(df_scores_pivot, on='matchid', how='left')

        # Fill NaN scores with 0
        df['first_innings_score'] = df['first_innings_score'].fillna(0).astype(int)
        df['second_innings_score'] = df['second_innings_score'].fillna(0).astype(int)

        # Extract series_id from matchid (placeholder - will be extracted from URL in scraper)
        # For now, we'll extract it if available in a pattern
        df['series_id'] = None

        # Determine winner from points
        # Points format: "Team1: 2, Team2: 0" or "Team1: 1, Team2: 1"
        def extract_winner(row):
            points_str = row.get('points', '')
            if not points_str or pd.isna(points_str):
                return None

            # Parse points like "Kolkata Knight Riders: 2, Royal Challengers Bengaluru: 0"
            try:
                teams_points = {}
                for item in points_str.split(','):
                    if ':' in item:
                        team, points = item.rsplit(':', 1)
                        team = team.strip()
                        points = int(points.strip())
                        teams_points[team] = points

                # Find team with 2 points (winner)
                for team, points in teams_points.items():
                    if points == 2:
                        return team

                # If both have 1 point, it's a tie or no result
                if len(teams_points) == 2 and all(p == 1 for p in teams_points.values()):
                    return "Tie/No Result"

                return None
            except:
                return None

        df['winner'] = df.apply(extract_winner, axis=1)

        # Select final columns
        df_result = df[[
            'matchid', 'venue', 'series', 'series_id', 'season',
            'player_of_the_match', 'first_innings', 'second_innings',
            'first_innings_score', 'second_innings_score', 'winner'
        ]].copy()

        # Add metadata
        df_result['created_at'] = datetime.now()
        df_result['updated_at'] = datetime.now()

        # Write to gold
        save_to_db(self.target_schema, 'match_summary', df_result)

        return len(df_result)

    def _process_series_summary(self) -> int:
        """
        Enhanced series summary with series_id extraction
        Series ID pattern: /series/ipl-2025-1449924/ -> 1449924
        """
        logger.info("Processing enhanced series summary...")

        query = f"""
            SELECT 
                series,
                season,
                COUNT(DISTINCT matchid) as total_matches
            FROM {self.source_schema}.match_metadata
            GROUP BY series, season
        """

        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()

        # Extract series_id (placeholder for now, will be populated by scraper)
        df['series_id'] = None

        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()

        save_to_db(self.target_schema, 'series_summary', df)
        return len(df)

    def _process_match_ball_statistics(self) -> int:
        """
        Enhanced match ball statistics with wickets per innings
        """
        logger.info("Processing match ball statistics with wickets...")

        conn = get_connection()

        # Ball and run statistics (excluding extras)
        query = f"""
            SELECT 
                matchid,
                innings,
                COUNT(*) as total_balls,
                SUM(runs_scored) as total_runs
            FROM {self.source_schema}.match_events
            WHERE score NOT ILIKE '%wide%'
              AND score NOT ILIKE '%no ball%'
              AND score NOT ILIKE '%bye%'
              AND score NOT ILIKE '%leg bye%'
            GROUP BY matchid, innings
        """
        df_balls = pd.read_sql(query, conn)

        # Wickets per innings
        wickets_query = f"""
            SELECT 
                matchid,
                innings,
                COUNT(*) as total_wickets
            FROM {self.source_schema}.match_events
            WHERE dismissal_method IS NOT NULL
               OR score ILIKE '%OUT%'
               OR score ILIKE '%wicket%'
            GROUP BY matchid, innings
        """
        df_wickets = pd.read_sql(wickets_query, conn)
        conn.close()

        # Merge
        df_result = df_balls.merge(df_wickets, on=['matchid', 'innings'], how='left')
        df_result['total_wickets'] = df_result['total_wickets'].fillna(0).astype(int)

        df_result['created_at'] = datetime.now()
        df_result['updated_at'] = datetime.now()

        save_to_db(self.target_schema, 'match_ball_statistics', df_result)
        return len(df_result)

    def _process_match_batsman_statistics(self) -> int:
        """
        Enhanced match batsman statistics with run distribution
        Includes: 1s, 2s, 3s, 4s, 6s
        """
        logger.info("Processing match batsman statistics with run distribution...")

        query = f"""
            SELECT 
                matchid,
                batsman,
                innings,
                SUM(runs_scored) as total_runs,
                COUNT(*) as balls_faced,
                SUM(CASE WHEN runs_scored = 1 THEN 1 ELSE 0 END) as ones,
                SUM(CASE WHEN runs_scored = 2 THEN 1 ELSE 0 END) as twos,
                SUM(CASE WHEN runs_scored = 3 THEN 1 ELSE 0 END) as threes,
                SUM(CASE WHEN runs_scored = 4 THEN 1 ELSE 0 END) as fours,
                SUM(CASE WHEN runs_scored = 6 THEN 1 ELSE 0 END) as sixes
            FROM {self.source_schema}.match_events
            WHERE batsman IS NOT NULL
              AND batsman != ''
              AND score NOT ILIKE '%wide%'
              AND score NOT ILIKE '%no ball%'
              AND score NOT ILIKE '%bye%'
              AND score NOT ILIKE '%leg bye%'
            GROUP BY matchid, batsman, innings
        """

        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()

        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()

        save_to_db(self.target_schema, 'match_batsman_statistics', df)
        return len(df)

    def _process_match_bowler_statistics(self) -> int:
        """
        Enhanced match bowler statistics with:
        - Wickets taken
        - Extras (wides, no balls)
        - Run distribution (1s, 2s, 3s, 4s, 6s)
        """
        logger.info("Processing match bowler statistics with wickets and extras...")

        conn = get_connection()

        # Main bowling stats with run distribution
        query = f"""
            SELECT 
                matchid,
                bowler,
                innings,
                COUNT(*) as balls_bowled,
                SUM(runs_scored) as runs_conceded,
                SUM(CASE WHEN runs_scored = 1 THEN 1 ELSE 0 END) as ones_conceded,
                SUM(CASE WHEN runs_scored = 2 THEN 1 ELSE 0 END) as twos_conceded,
                SUM(CASE WHEN runs_scored = 3 THEN 1 ELSE 0 END) as threes_conceded,
                SUM(CASE WHEN runs_scored = 4 THEN 1 ELSE 0 END) as fours_conceded,
                SUM(CASE WHEN runs_scored = 6 THEN 1 ELSE 0 END) as sixes_conceded
            FROM {self.source_schema}.match_events
            WHERE bowler IS NOT NULL
              AND bowler != ''
            GROUP BY matchid, bowler, innings
        """
        df_bowling = pd.read_sql(query, conn)

        # Wickets taken
        wickets_query = f"""
            SELECT 
                matchid,
                bowler,
                innings,
                COUNT(*) as wickets_taken
            FROM {self.source_schema}.match_events
            WHERE bowler IS NOT NULL
              AND bowler != ''
              AND (dismissal_method IS NOT NULL 
                   OR score ILIKE '%OUT%' 
                   OR score ILIKE '%wicket%')
            GROUP BY matchid, bowler, innings
        """
        df_wickets = pd.read_sql(wickets_query, conn)

        # Extras (wides and no balls)
        extras_query = f"""
            SELECT 
                matchid,
                bowler,
                innings,
                SUM(CASE WHEN score ILIKE '%wide%' THEN 1 ELSE 0 END) as wides,
                SUM(CASE WHEN score ILIKE '%no ball%' THEN 1 ELSE 0 END) as no_balls
            FROM {self.source_schema}.match_events
            WHERE bowler IS NOT NULL
              AND bowler != ''
            GROUP BY matchid, bowler, innings
        """
        df_extras = pd.read_sql(extras_query, conn)
        conn.close()

        # Merge all dataframes
        df_result = df_bowling.merge(df_wickets, on=['matchid', 'bowler', 'innings'], how='left')
        df_result = df_result.merge(df_extras, on=['matchid', 'bowler', 'innings'], how='left')

        # Fill NaN values
        df_result['wickets_taken'] = df_result['wickets_taken'].fillna(0).astype(int)
        df_result['wides'] = df_result['wides'].fillna(0).astype(int)
        df_result['no_balls'] = df_result['no_balls'].fillna(0).astype(int)

        df_result['created_at'] = datetime.now()
        df_result['updated_at'] = datetime.now()

        save_to_db(self.target_schema, 'match_bowler_statistics', df_result)
        return len(df_result)

    def _process_batsman_career_statistics(self) -> int:
        """
        Career batting statistics aggregated across all matches:
        - Total balls faced
        - Total runs scored
        - Not outs
        - Run distribution (1s, 2s, 3s, 4s, 6s)
        - Fifties and hundreds
        """
        logger.info("Processing career batting statistics...")

        # First, ensure match_batsman_statistics exists
        check_query = f"""
            SELECT COUNT(*) as cnt 
            FROM {self.target_schema}.match_batsman_statistics
        """

        conn = get_connection()
        check_df = pd.read_sql(check_query, conn)

        if check_df['cnt'].iloc[0] == 0:
            logger.warning("match_batsman_statistics is empty, skipping career stats")
            conn.close()
            return 0

        # Aggregate career stats
        query = f"""
            SELECT 
                batsman,
                SUM(total_runs) as total_runs_scored,
                SUM(balls_faced) as total_balls_faced,
                SUM(ones) as ones,
                SUM(twos) as twos,
                SUM(threes) as threes,
                SUM(fours) as fours,
                SUM(sixes) as sixes,
                COUNT(DISTINCT matchid) as matches_played
            FROM {self.target_schema}.match_batsman_statistics
            GROUP BY batsman
        """
        df = pd.read_sql(query, conn)

        # Count fifties and hundreds
        milestones_query = f"""
            SELECT 
                batsman,
                SUM(CASE WHEN total_runs >= 50 AND total_runs < 100 THEN 1 ELSE 0 END) as fifties,
                SUM(CASE WHEN total_runs >= 100 THEN 1 ELSE 0 END) as hundreds
            FROM {self.target_schema}.match_batsman_statistics
            GROUP BY batsman
        """
        df_milestones = pd.read_sql(milestones_query, conn)

        # Count not outs (innings where batsman didn't get out)
        # This is an approximation - assuming not out if no dismissal recorded
        notouts_query = f"""
            SELECT 
                batsman,
                COUNT(DISTINCT matchid || innings) as not_outs
            FROM {self.target_schema}.match_batsman_statistics mbs
            WHERE NOT EXISTS (
                SELECT 1 
                FROM {self.source_schema}.match_events me
                WHERE me.matchid = mbs.matchid
                  AND me.innings = mbs.innings
                  AND me.batsman = mbs.batsman
                  AND (me.dismissal_method IS NOT NULL 
                       OR me.score ILIKE '%OUT%')
            )
            GROUP BY batsman
        """
        df_notouts = pd.read_sql(notouts_query, conn)
        conn.close()

        # Merge all
        df = df.merge(df_milestones, on='batsman', how='left')
        df = df.merge(df_notouts, on='batsman', how='left')

        # Fill NaN
        df['fifties'] = df['fifties'].fillna(0).astype(int)
        df['hundreds'] = df['hundreds'].fillna(0).astype(int)
        df['not_outs'] = df['not_outs'].fillna(0).astype(int)

        # Calculate batting average and strike rate
        df['batting_average'] = (
            df['total_runs_scored'] / (df['matches_played'] - df['not_outs']).replace(0, 1)
        ).round(2)

        df['strike_rate'] = (
            (df['total_runs_scored'] / df['total_balls_faced']) * 100
        ).round(2)

        # Replace inf with 0
        df['batting_average'] = df['batting_average'].replace([float('inf'), -float('inf')], 0)
        df['strike_rate'] = df['strike_rate'].replace([float('inf'), -float('inf')], 0)

        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()

        save_to_db(self.target_schema, 'batsman_statistics', df)
        return len(df)

    def _process_bowler_career_statistics(self) -> int:
        """
        Career bowling statistics aggregated across all matches:
        - Total balls bowled
        - Total runs conceded
        - Total wickets taken
        - Extras (wides, no balls)
        - Run distribution (1s, 2s, 3s, 4s, 6s)
        """
        logger.info("Processing career bowling statistics...")

        # First, ensure match_bowler_statistics exists
        check_query = f"""
            SELECT COUNT(*) as cnt 
            FROM {self.target_schema}.match_bowler_statistics
        """

        conn = get_connection()
        check_df = pd.read_sql(check_query, conn)

        if check_df['cnt'].iloc[0] == 0:
            logger.warning("match_bowler_statistics is empty, skipping career stats")
            conn.close()
            return 0

        # Aggregate career stats
        query = f"""
            SELECT 
                bowler,
                SUM(balls_bowled) as total_balls_bowled,
                SUM(runs_conceded) as total_runs_conceded,
                SUM(wickets_taken) as total_wickets_taken,
                SUM(wides) as total_wides,
                SUM(no_balls) as total_no_balls,
                SUM(ones_conceded) as ones_conceded,
                SUM(twos_conceded) as twos_conceded,
                SUM(threes_conceded) as threes_conceded,
                SUM(fours_conceded) as fours_conceded,
                SUM(sixes_conceded) as sixes_conceded,
                COUNT(DISTINCT matchid) as matches_bowled
            FROM {self.target_schema}.match_bowler_statistics
            GROUP BY bowler
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # Calculate bowling metrics
        df['economy_rate'] = (
            df['total_runs_conceded'] / (df['total_balls_bowled'] / 6.0)
        ).round(2)

        df['bowling_average'] = (
            df['total_runs_conceded'] / df['total_wickets_taken'].replace(0, 1)
        ).round(2)

        df['strike_rate'] = (
            df['total_balls_bowled'] / df['total_wickets_taken'].replace(0, 1)
        ).round(2)

        # Replace inf with 0
        df['economy_rate'] = df['economy_rate'].replace([float('inf'), -float('inf')], 0)
        df['bowling_average'] = df['bowling_average'].replace([float('inf'), -float('inf')], 0)
        df['strike_rate'] = df['strike_rate'].replace([float('inf'), -float('inf')], 0)

        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()

        save_to_db(self.target_schema, 'bowler_statistics', df)
        return len(df)

    # ========================================================================
    # SCD TYPE 2 METHODS (SQL-BASED)
    # ========================================================================

    def _process_scd_type2_sql(self, agg_name: str, agg_config: Dict) -> int:
        """Process SCD Type 2 using SQL MERGE/UPSERT approach"""
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

        Correct Order:
        1. Expire old records (player changed teams)
        2. Update existing records (same player-team-season, accumulate stats)
        3. Insert new records (new player-team-season combinations)
        """
        conn = get_connection()
        cursor = conn.cursor()

        staging_table = f"{table_name}_staging"
        target_table = f"{self.target_schema}.{table_name}"

        try:
            # Step 1: Create staging table
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
            logger.info("Staging table created successfully")

            # Step 2: Load data into staging
            logger.info("Loading data into staging table...")
            df_staging = df_new.copy()

            if 'id' in df_staging.columns:
                df_staging = df_staging.drop(columns=['id'])

            for _, row in df_staging.iterrows():
                cursor.execute(f"""
                    INSERT INTO {staging_table} (
                        player_name, team, season, series,
                        first_match_date, last_match_date, matches_played, is_impact_player,
                        effective_from, effective_to, is_current, version,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['player_name'], row['team'], row['season'], row.get('series'),
                    row['first_match_date'], row['last_match_date'], row['matches_played'],
                    row['is_impact_player'], row['effective_from'], row.get('effective_to'),
                    row['is_current'], row['version'], row['created_at'], row['updated_at']
                ))

            conn.commit()
            logger.info(f"Loaded {len(df_staging)} records into staging")

            # Step 3: Expire old records (player changed teams or left)
            logger.info("Step 1/3: Expiring old records where player changed teams...")
            expire_query = f"""
                UPDATE {target_table} tgt
                SET 
                    is_current = FALSE, 
                    effective_to = CURRENT_DATE, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE tgt.is_current = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM {staging_table} stg
                      WHERE stg.player_name = tgt.player_name
                        AND stg.team = tgt.team
                        AND stg.season = tgt.season
                  );
            """
            cursor.execute(expire_query)
            expired_count = cursor.rowcount
            conn.commit()
            logger.info(f"✓ Expired {expired_count} old records")

            # Step 4: Update existing current records (same player-team-season)
            logger.info("Step 2/3: Updating existing current records...")
            update_query = f"""
                UPDATE {target_table} tgt
                SET 
                    matches_played = tgt.matches_played + stg.matches_played,
                    last_match_date = GREATEST(tgt.last_match_date, stg.last_match_date),
                    first_match_date = LEAST(tgt.first_match_date, stg.first_match_date),
                    series = CASE 
                        WHEN tgt.series IS NULL THEN stg.series
                        WHEN stg.series IS NULL THEN tgt.series
                        WHEN tgt.series = stg.series THEN tgt.series
                        ELSE tgt.series || ', ' || stg.series
                    END,
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
            conn.commit()
            logger.info(f"✓ Updated {updated_count} existing records")

            # Step 5: Insert new records (only if not exists)
            logger.info("Step 3/3: Inserting new records...")
            insert_query = f"""
                INSERT INTO {target_table} (
                    player_name, team, season, series, first_match_date, last_match_date,
                    matches_played, is_impact_player, effective_from, effective_to,
                    is_current, version, created_at, updated_at
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
                    SELECT 1 FROM {target_table} tgt
                    WHERE tgt.player_name = stg.player_name
                      AND tgt.team = stg.team
                      AND tgt.season = stg.season
                      AND tgt.is_current = TRUE
                );
            """
            cursor.execute(insert_query)
            inserted_count = cursor.rowcount
            conn.commit()
            logger.info(f"✓ Inserted {inserted_count} new records")

            # Summary
            total_processed = expired_count + updated_count + inserted_count
            logger.info("=" * 70)
            logger.info("SCD TYPE 2 MERGE COMPLETE")
            logger.info(f"  Expired: {expired_count} records (player changed teams)")
            logger.info(f"  Updated: {updated_count} records (accumulated stats)")
            logger.info(f"  Inserted: {inserted_count} records (new combinations)")
            logger.info(f"  Total: {total_processed} records processed")
            logger.info("=" * 70)

            return total_processed

        except Exception as e:
            conn.rollback()
            logger.error(f"Error during SCD merge: {e}")
            logger.error(f"Full error details:", exc_info=True)
            raise
        finally:
            cursor.close()
            conn.close()

    def _build_player_team_dimension(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build player-team dimension from match_players data"""

        required_cols = ['player_name', 'team']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return pd.DataFrame()

        # Enrich with season if not present
        if 'season' not in df.columns:
            logger.info("Season not in match_players, fetching from match_metadata")
            df = self._enrich_with_season(df)

        # Enrich with series if not present
        if 'series' not in df.columns:
            logger.info("Series not in match_players, fetching from match_metadata")
            df = self._enrich_with_series(df)

        group_cols = ['player_name', 'team', 'season']

        # Base aggregation
        agg_dict = {
            'matchid': 'count'
        }

        # Add is_impact_player if exists
        if 'is_impact_player' in df.columns:
            agg_dict['is_impact_player'] = 'max'

        # Add series aggregation - take first non-null value or concatenate unique values
        if 'series' in df.columns:
            agg_dict['series'] = lambda x: ', '.join(x.dropna().unique()) if len(x.dropna()) > 0 else None

        # Perform aggregation
        df_agg = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
        df_agg.rename(columns={'matchid': 'matches_played'}, inplace=True)

        # Set defaults for missing columns
        if 'is_impact_player' not in df_agg.columns:
            df_agg['is_impact_player'] = False

        if 'series' not in df_agg.columns:
            df_agg['series'] = None

        # Type conversions and metadata
        df_agg['is_impact_player'] = df_agg['is_impact_player'].astype(bool)
        df_agg['effective_from'] = datetime.now().date()
        df_agg['effective_to'] = None
        df_agg['is_current'] = True
        df_agg['version'] = 1
        df_agg['first_match_date'] = datetime.now().date()
        df_agg['last_match_date'] = datetime.now().date()
        df_agg['created_at'] = datetime.now()
        df_agg['updated_at'] = datetime.now()

        logger.info(f"Built dimension with {len(df_agg)} player-team-season combinations")

        # Log series info
        series_with_data = df_agg['series'].notna().sum()
        logger.info(f"Series column populated for {series_with_data}/{len(df_agg)} records")

        return df_agg

    def _enrich_with_season(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich player data with season from match_metadata"""
        try:
            query = f"SELECT matchid, season FROM {self.source_schema}.match_metadata"
            conn = get_connection()
            df_metadata = pd.read_sql(query, conn)
            conn.close()

            # Merge with match_metadata to get season
            df = df.merge(df_metadata, on='matchid', how='left')
            df['season'] = df['season'].fillna(datetime.now().year)

            logger.info(f"Enriched with season data: {len(df)} records")
            return df

        except Exception as e:
            logger.warning(f"Could not enrich with season: {e}")
            df['season'] = datetime.now().year
            return df

    def _enrich_with_series(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich player data with series from match_metadata"""
        try:
            query = f"SELECT matchid, series FROM {self.source_schema}.match_metadata"
            conn = get_connection()
            df_metadata = pd.read_sql(query, conn)
            conn.close()

            # Merge with match_metadata to get series
            df = df.merge(df_metadata, on='matchid', how='left')

            # Count how many records got series data
            series_count = df['series'].notna().sum()
            logger.info(f"Enriched with series data: {series_count}/{len(df)} records have series")

            return df

        except Exception as e:
            logger.warning(f"Could not enrich with series: {e}")
            df['series'] = None
            return df
    # ========================================================================
    # STANDARD AGGREGATION METHODS
    # ========================================================================

    def _process_standard_aggregation(self, agg_name: str, agg_config: Dict) -> int:
        """Standard aggregation processing"""
        source_table = agg_config.get('source')
        group_by = agg_config.get('group_by', [])
        metrics = agg_config.get('metrics', [])
        select_columns = agg_config.get('select_columns', [])
        filters = agg_config.get('filters', {})

        source_full = f"{self.source_schema}.{source_table}"
        df = self._read_silver_table(source_full)

        if df.empty:
            logger.warning(f"No data found in {source_full}")
            return 0

        logger.info(f"Read {len(df)} rows from {source_full}")

        if filters:
            df = self._apply_filters(df, filters, agg_name)

        agg_df = self._build_aggregation(df, group_by, metrics)

        if select_columns:
            agg_df = self._select_gold_columns(agg_df, select_columns)

        agg_df = self._clean_for_gold(agg_df)
        agg_df['created_at'] = datetime.now()
        agg_df['updated_at'] = datetime.now()

        logger.info(f"Final DataFrame columns: {list(agg_df.columns)}")

        self._write_to_gold(agg_df, agg_name)

        return len(agg_df)

    def _apply_filters(self, df: pd.DataFrame, filters: Dict, agg_name: str) -> pd.DataFrame:
        """Apply filters to DataFrame before aggregation"""
        initial_count = len(df)

        if 'exclude_extras' in filters and filters['exclude_extras']:
            df = self._exclude_extras(df)
            logger.info(f"  Filtered extras: {initial_count} -> {len(df)} rows ({initial_count - len(df)} extras removed)")

        if 'exclude_nulls' in filters:
            columns = filters['exclude_nulls']
            df = df.dropna(subset=columns)
            logger.info(f"  Removed null values in {columns}")

        return df

    def _exclude_extras(self, df: pd.DataFrame) -> pd.DataFrame:
        """Exclude extras from DataFrame"""
        if 'score' not in df.columns and 'event' not in df.columns:
            logger.warning("  Cannot filter extras - 'score' and 'event' columns not found")
            return df

        extras_keywords = ['wide', 'no ball', 'noball', 'leg bye', 'legbye', 'leg-bye', 'bye', 'byes', 'penalty']
        mask = pd.Series([True] * len(df), index=df.index)

        if 'score' in df.columns:
            score_mask = ~df['score'].fillna('').str.lower().str.contains('|'.join(extras_keywords), case=False, na=False, regex=True)
            mask = mask & score_mask

        if 'event' in df.columns:
            event_mask = ~df['event'].fillna('').str.lower().str.contains('|'.join(extras_keywords), case=False, na=False, regex=True)
            mask = mask & event_mask

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
        columns_to_remove = ['id', 'source_id', 'processed_at', 'is_valid', 'validation_errors']
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

        if df.empty:
            logger.warning("DataFrame is empty after filtering - returning empty result")
            if group_by and metrics:
                columns = group_by + [m.get('name') for m in metrics]
                return pd.DataFrame(columns=columns)
            return pd.DataFrame()

        if not metrics or len(metrics) == 0:
            if group_by:
                result = df[group_by].drop_duplicates().reset_index(drop=True)
                logger.info(f"  Selected {len(result)} distinct rows (no aggregation)")
            else:
                result = df.copy()
                logger.info(f"  Returned {len(result)} rows (no aggregation)")
            return result

        agg_dict = {}

        for metric in metrics:
            metric_name = metric.get('name')
            metric_type = metric.get('type')
            column = metric.get('column')

            if column and column not in df.columns:
                logger.error(f"Column '{column}' not found in DataFrame")
                logger.error(f"Available columns: {list(df.columns)}")
                raise KeyError(f"Column '{column}' not found for metric '{metric_name}'")

            if metric_type == 'count':
                agg_dict[metric_name] = (column if column else df.columns[0], 'count')
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

        try:
            if group_by:
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