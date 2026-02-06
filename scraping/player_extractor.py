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

    def __init__(self, schema: str = 'bronze'):
        """
        Initialize Player Extractor

        Args:
            schema: Target schema for storing player data (default: 'bronze')
        """
        self.schema = schema
        self.table_name = 'match_players'
        logger.info(f"PlayerExtractor initialized - Target: {schema}.{self.table_name}")

    def extract_and_store(self, html_content: str, match_id: int) -> tuple:
        """
        Extract player names from HTML and store in database

        Args:
            html_content: HTML content from match scorecard page
            match_id: Match identifier

        Returns:
            tuple: (players_df: pd.DataFrame, results_dict: dict)
                - players_df: DataFrame containing player data (empty DataFrame on failure)
                - results_dict: Dictionary with extraction statistics
        """
        try:
            logger.info(f"Extracting players for match {match_id}")

            # Extract player names using MetadataExtractor
            players_df = MetadataExtractor.extract_player_names(html_content, match_id)

            if players_df.empty:
                logger.warning(f"No players extracted for match {match_id}")
                return players_df, {
                    'status': 'failed',
                    'total_players': 0,
                    'batted': 0,
                    'did_not_bat': 0,
                    'regular_players': 0,
                    'impact_players': 0,
                    'teams': []
                }

            # Calculate statistics
            total_players = len(players_df)
            batted_count = int(players_df['batted'].sum())
            did_not_bat_count = total_players - batted_count
            teams = players_df['team'].unique().tolist()

            # NEW: Count impact vs regular players
            regular_count = int((players_df['player_type'] == 'regular').sum())
            impact_count = int((players_df['player_type'] == 'impact').sum())

            logger.info(f"Extracted {total_players} players for match {match_id}")
            logger.info(f"  Teams: {', '.join(teams)}")
            logger.info(f"  Batted: {batted_count}, Did not bat: {did_not_bat_count}")
            logger.info(f"  Regular: {regular_count}, Impact: {impact_count}")

            # Log impact players specifically
            if impact_count > 0:
                impact_players = players_df[players_df['player_type'] == 'impact']['player_name'].tolist()
                logger.info(f"  Impact players: {', '.join(impact_players)}")

            results_dict = {
                'status': 'success',
                'total_players': total_players,
                'batted': batted_count,
                'did_not_bat': did_not_bat_count,
                'regular_players': regular_count,
                'impact_players': impact_count,
                'teams': teams
            }

            return players_df, results_dict

        except Exception as e:
            logger.error(f"Error extracting players for match {match_id}: {e}", exc_info=True)

            # Return empty DataFrame and error dict
            empty_df = pd.DataFrame()

            error_dict = {
                'status': 'failed',
                'total_players': 0,
                'batted': 0,
                'did_not_bat': 0,
                'regular_players': 0,
                'impact_players': 0,
                'teams': [],
                'error': str(e)
            }

            return empty_df, error_dict

    def get_match_players(self, match_id: int) -> pd.DataFrame:
        """
        Retrieve players for a specific match from database

        Args:
            match_id: Match identifier

        Returns:
            DataFrame with player information
        """
        query = f"""
            SELECT 
                matchid,
                innings,
                team,
                player_name,
                batted,
                batting_position,
                player_type
            FROM {self.schema}.{self.table_name}
            WHERE matchid = %s
              AND is_active = TRUE
            ORDER BY 
                player_type ASC,  -- Impact players first
                CASE WHEN batted = TRUE THEN batting_position ELSE 999 END,
                player_name
        """

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn, params=(match_id,))
            logger.info(f"Retrieved {len(df)} players for match {match_id}")

            # Log breakdown
            if not df.empty:
                impact_count = int((df['player_type'] == 'impact').sum())
                regular_count = int((df['player_type'] == 'regular').sum())
                logger.info(f"  Regular: {regular_count}, Impact: {impact_count}")

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
                SELECT 
                    matchid,
                    innings,
                    team,
                    player_name,
                    batted,
                    batting_position,
                    player_type
                FROM {self.schema}.{self.table_name}
                WHERE team = %s 
                  AND matchid = %s
                  AND is_active = TRUE
                ORDER BY 
                    player_type ASC,  -- Impact players first
                    batting_position NULLS LAST, 
                    player_name
            """
            params = (team_name, match_id)
        else:
            query = f"""
                SELECT DISTINCT 
                    player_name, 
                    team,
                    player_type
                FROM {self.schema}.{self.table_name}
                WHERE team = %s
                  AND is_active = TRUE
                ORDER BY player_name
            """
            params = (team_name,)

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn, params=params)
            logger.info(f"Retrieved {len(df)} players for team {team_name}")

            if not df.empty and 'player_type' in df.columns:
                impact_count = int((df['player_type'] == 'impact').sum())
                if impact_count > 0:
                    logger.info(f"  Including {impact_count} impact player(s)")

            return df
        finally:
            conn.close()

    def get_impact_players(self, match_id: int = None) -> pd.DataFrame:
        """
        Get all impact players, optionally filtered by match

        Args:
            match_id: Optional match ID to filter by

        Returns:
            DataFrame with impact players only
        """
        if match_id:
            query = f"""
                SELECT 
                    matchid,
                    innings,
                    team,
                    player_name,
                    batted,
                    batting_position
                FROM {self.schema}.{self.table_name}
                WHERE player_type = 'impact'
                  AND matchid = %s
                  AND is_active = TRUE
                ORDER BY team, player_name
            """
            params = (match_id,)
        else:
            query = f"""
                SELECT 
                    matchid,
                    innings,
                    team,
                    player_name,
                    batted,
                    batting_position
                FROM {self.schema}.{self.table_name}
                WHERE player_type = 'impact'
                  AND is_active = TRUE
                ORDER BY matchid, team, player_name
            """
            params = None

        conn = get_connection()
        try:
            if params:
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)

            logger.info(f"Retrieved {len(df)} impact player(s)")
            return df
        finally:
            conn.close()

    def get_player_matches(self, player_name: str) -> pd.DataFrame:
        """
        Get all matches for a specific player

        Args:
            player_name: Player name

        Returns:
            DataFrame with match information for the player
        """
        query = f"""
            SELECT 
                matchid,
                innings,
                team,
                player_name,
                batted,
                batting_position,
                player_type
            FROM {self.schema}.{self.table_name}
            WHERE player_name = %s
              AND is_active = TRUE
            ORDER BY matchid DESC
        """

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn, params=(player_name,))
            logger.info(f"Retrieved {len(df)} match(es) for player {player_name}")

            if not df.empty:
                impact_matches = int((df['player_type'] == 'impact').sum())
                regular_matches = int((df['player_type'] == 'regular').sum())
                logger.info(f"  Regular: {regular_matches}, As impact player: {impact_matches}")

            return df
        finally:
            conn.close()

    def get_player_statistics(self, match_id: int = None) -> dict:
        """
        Get player statistics summary

        Args:
            match_id: Optional match ID to filter by

        Returns:
            Dictionary with player statistics
        """
        if match_id:
            query = f"""
                SELECT 
                    COUNT(*) as total_players,
                    COUNT(*) FILTER (WHERE batted = TRUE) as batted,
                    COUNT(*) FILTER (WHERE batted = FALSE) as did_not_bat,
                    COUNT(*) FILTER (WHERE player_type = 'regular') as regular_players,
                    COUNT(*) FILTER (WHERE player_type = 'impact') as impact_players,
                    COUNT(DISTINCT team) as teams
                FROM {self.schema}.{self.table_name}
                WHERE matchid = %s
                  AND is_active = TRUE
            """
            params = (match_id,)
        else:
            query = f"""
                SELECT 
                    COUNT(*) as total_players,
                    COUNT(*) FILTER (WHERE batted = TRUE) as batted,
                    COUNT(*) FILTER (WHERE batted = FALSE) as did_not_bat,
                    COUNT(*) FILTER (WHERE player_type = 'regular') as regular_players,
                    COUNT(*) FILTER (WHERE player_type = 'impact') as impact_players,
                    COUNT(DISTINCT matchid) as matches,
                    COUNT(DISTINCT team) as teams
                FROM {self.schema}.{self.table_name}
                WHERE is_active = TRUE
            """
            params = None

        conn = get_connection()
        try:
            if params:
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)

            stats = df.to_dict('records')[0] if not df.empty else {}

            # Convert to int for cleaner output
            for key in stats:
                if stats[key] is not None:
                    stats[key] = int(stats[key])

            return stats
        finally:
            conn.close()

    def verify_player_extraction(self, match_id: int) -> dict:
        """
        Verify player extraction for a match

        Args:
            match_id: Match identifier

        Returns:
            Dictionary with verification results
        """
        try:
            df = self.get_match_players(match_id)

            if df.empty:
                return {
                    'status': 'no_data',
                    'message': f'No players found for match {match_id}'
                }

            # Get statistics
            stats = self.get_player_statistics(match_id)

            # Check for potential issues
            issues = []

            # Check if we have players from 2 teams
            if stats.get('teams', 0) != 2:
                issues.append(f"Expected 2 teams, found {stats.get('teams', 0)}")

            # Check if we have reasonable number of players
            if stats.get('total_players', 0) < 22:
                issues.append(f"Only {stats.get('total_players', 0)} players (expected ~22)")

            # Check if impact players are reasonable (0-4 expected)
            impact_count = stats.get('impact_players', 0)
            if impact_count > 4:
                issues.append(f"Unusual number of impact players: {impact_count}")

            verification_result = {
                'status': 'success' if not issues else 'warning',
                'match_id': match_id,
                'statistics': stats,
                'issues': issues
            }

            if issues:
                logger.warning(f"Player extraction issues for match {match_id}: {', '.join(issues)}")
            else:
                logger.info(f"Player extraction verified for match {match_id}")

            return verification_result

        except Exception as e:
            logger.error(f"Error verifying player extraction for match {match_id}: {e}", exc_info=True)
            return {
                'status': 'error',
                'match_id': match_id,
                'error': str(e)
            }