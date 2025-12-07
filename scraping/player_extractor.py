"""
Player Extractor - Extracts and stores player rosters for each match
"""
from core.metadata_extractor import MetadataExtractor
from configs.db_config import save_to_db, get_connection
from utils.logger import setup_logger
import pandas as pd

logger = setup_logger(__name__)


class PlayerExtractor:
    """
    Extracts player names from match scorecard and stores in database
    """

    def __init__(self, schema: str = 'raw'):
        """
        Initialize Player Extractor

        Args:
            schema: Target schema for storing player data (default: 'raw')
        """
        self.schema = schema
        self.table_name = 'match_players'
        logger.info(f"PlayerExtractor initialized - Target: {schema}.{self.table_name}")

    def extract_and_store(self, html_content: str, match_id: int) -> dict:
        """
        Extract player names from HTML and store in database

        Args:
            html_content: HTML content from match scorecard page
            match_id: Match identifier

        Returns:
            Dictionary with extraction results
        """
        try:
            logger.info(f"Extracting players for match {match_id}")

            # Extract player names using MetadataExtractor
            players_df = MetadataExtractor.extract_player_names(html_content, match_id)

            if players_df.empty:
                logger.warning(f"No players extracted for match {match_id}")
                return {
                    'status': 'failed',
                    'total_players': 0,
                    'batted': 0,
                    'did_not_bat': 0,
                    'teams': []
                }

            # Save to database
            save_to_db(self.schema, self.table_name, players_df)

            # Calculate statistics
            total_players = len(players_df)
            batted_count = int(players_df['batted'].sum())
            did_not_bat_count = total_players - batted_count
            teams = players_df['team'].unique().tolist()

            logger.info(f"Stored {total_players} players for match {match_id}")
            logger.info(f"  Teams: {', '.join(teams)}")
            logger.info(f"  Batted: {batted_count}, Did not bat: {did_not_bat_count}")

            return {
                'status': 'success',
                'total_players': total_players,
                'batted': batted_count,
                'did_not_bat': did_not_bat_count,
                'teams': teams
            }

        except Exception as e:
            logger.error(f"Error extracting players for match {match_id}: {e}", exc_info=True)
            return {
                'status': 'failed',
                'total_players': 0,
                'batted': 0,
                'did_not_bat': 0,
                'teams': [],
                'error': str(e)
            }

    def get_match_players(self, match_id: int) -> pd.DataFrame:
        """
        Retrieve players for a specific match from database

        Args:
            match_id: Match identifier

        Returns:
            DataFrame with player information
        """
        query = f"""
            SELECT *
            FROM {self.schema}.{self.table_name}
            WHERE matchid = %s
            ORDER BY 
                CASE WHEN batted = TRUE THEN batting_position ELSE 999 END,
                player_name
        """

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn, params=(match_id,))
            logger.info(f"Retrieved {len(df)} players for match {match_id}")
            return df
        finally:
            conn.close()

    def get_team_roster(self, team_name: str, match_id: int = None) -> pd.DataFrame:
        """
        Get roster for a specific team

        Args:
            team_name: Team name
            match_id: Optional match ID to filter by

        Returns:
            DataFrame with team players
        """
        if match_id:
            query = f"""
                SELECT *
                FROM {self.schema}.{self.table_name}
                WHERE team = %s AND matchid = %s
                ORDER BY batting_position NULLS LAST, player_name
            """
            params = (team_name, match_id)
        else:
            query = f"""
                SELECT DISTINCT player_name, team
                FROM {self.schema}.{self.table_name}
                WHERE team = %s
                ORDER BY player_name
            """
            params = (team_name,)

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn, params=params)
            logger.info(f"Retrieved {len(df)} players for team {team_name}")
            return df
        finally:
            conn.close()