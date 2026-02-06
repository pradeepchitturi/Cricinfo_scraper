"""
Silver Layer - Data cleansing, validation, and transformation
"""
import pandas as pd
import re
from typing import List, Dict, Any
from datetime import datetime
from configs.db_config import save_to_db, get_connection
from utils.logger import setup_logger
import numpy as np
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
                logger.error(error_msg, exc_info=True)
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
            elif transform_type == 'extract_dismissal':
                df = self._extract_dismissal_info(df, transform)
            elif transform_type == 'enrich_player_names':
                df = self._enrich_player_names(df, transform)
            elif transform_type == 'enrich_team_names':
                df = self._enrich_team_names(df, table_name)

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

        df = df.replace(np.nan, None)
        print(f"Processed {len(df)} rows")

        # Write to silver
        save_to_db(self.target_schema, table_name, df)

        return len(df)

    def _enrich_player_names(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Enrich player names by joining with match_players table
        Replaces abbreviated/inconsistent names with full official names

        Args:
            df: DataFrame from match_events
            config: Configuration with columns to enrich

        Returns:
            DataFrame with enriched player names
        """
        columns_to_enrich = config.get('columns', ['batsman', 'bowler', 'fielder_name'])

        logger.info(f"Enriching player names for columns: {columns_to_enrich}")

        try:
            # Load match_players lookup table
            players_df = self._load_match_players_lookup()

            if players_df.empty:
                logger.warning("No match_players data available for enrichment")
                return df

            logger.info(f"Loaded {len(players_df)} player records for enrichment")

            # Enrich each configured column
            for column in columns_to_enrich:
                if column not in df.columns:
                    logger.warning(f"  Column '{column}' not found in DataFrame, skipping")
                    continue

                df, stats = self._enrich_column(df, column, players_df)

            # Per-match deduplication (using match_players reference)
            df = self._deduplicate_player_names(df, players_df)

            # Global deduplication (across all data)
            df = self._global_player_deduplication(df)

            return df

        except Exception as e:
            logger.error(f"Error during player name enrichment: {e}", exc_info=True)
            logger.warning("Continuing without enrichment")
            return df

    def _load_match_players_lookup(self) -> pd.DataFrame:
        """
        Load match_players table for name lookup
        Creates a mapping of abbreviated names to full names

        Returns:
            DataFrame with player name mappings
        """
        query = f"""
            SELECT DISTINCT
                matchid,
                team,
                player_name,
                innings
            FROM {self.source_schema}.match_players
            WHERE is_active = TRUE and player_type = 'regular'
        """

        conn = get_connection()
        try:
            df = pd.read_sql(query, conn)
            logger.debug(f"Loaded {len(df)} player records from {self.source_schema}.match_players")
            return df
        except Exception as e:
            logger.error(f"Error loading match_players: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def _enrich_column(self, df: pd.DataFrame, column: str, players_df: pd.DataFrame) -> tuple:
        """
        Enrich a single column with full player names (with improved caching)
        """
        logger.info(f"Enriching column: {column}")

        stats = {'enriched': 0, 'not_found': 0, 'null': 0}


        # Process each match separately
        for matchid in df['matchid'].unique():

            match_mask = df['matchid'] == matchid
            match_players = players_df[players_df['matchid'] == matchid].copy()

            if match_players.empty:
                continue

            # Build abbreviation cache for this match
            abbrev_cache = self._build_player_abbreviation_cache(match_players)

            for idx in df[match_mask].index:
                original_name = df.at[idx, column]

                if pd.isna(original_name) or original_name == '' or original_name is None:
                    stats['null'] += 1
                    continue

                original_name_str = str(original_name).strip()
                name_lower = original_name_str.lower()

                # Try multiple cache lookups
                full_name = None

                # 1. Direct match (e.g., "kohli", "a raghuvanshi")
                if name_lower in abbrev_cache and abbrev_cache[name_lower]:
                    full_name = abbrev_cache[name_lower]

                # 2. First name with prefix (e.g., "prabhsimran" → "first_prabhsimran")
                elif f"first_{name_lower}" in abbrev_cache and abbrev_cache[f"first_{name_lower}"]:
                    full_name = abbrev_cache[f"first_{name_lower}"]

                # 3. Fallback to full matching logic
                if not full_name:
                    full_name = self._find_full_player_name(
                        original_name_str,
                        match_players,
                        matchid
                    )

                if full_name and full_name != original_name_str:
                    df.at[idx, column] = full_name
                    stats['enriched'] += 1
                elif not full_name or full_name == original_name_str:
                    # Check if already enriched (full name)
                    if any(original_name_str.lower() == p.lower() for p in match_players['player_name']):
                        pass  # Already full name
                    else:
                        stats['not_found'] += 1

        logger.info(f"  {column}: {stats['enriched']} enriched, "
                    f"{stats['not_found']} not found, {stats['null']} null")

        return df, stats

    def _build_player_abbreviation_cache(self, match_players: pd.DataFrame) -> dict:
        """
        Build a comprehensive cache of player abbreviations for fast lookup

        Cache structure:
        - "kohli" → "Virat Kohli"
        - "first_virat" → "Virat Kohli"
        - "a raghuvanshi" → "Angkrish Raghuvanshi"
        - "tm head" → "Travis Head"
        - "virat kohli" → "Virat Kohli"

        Args:
            match_players: DataFrame with player names

        Returns:
            dict: Mapping of abbreviations to full names
        """
        abbrev_cache = {}

        for _, player in match_players.iterrows():
            full_name = player['player_name']
            name_parts = full_name.split()


            # Full name (lowercase)
            abbrev_cache[full_name.lower()] = full_name

            if len(name_parts) >= 2:
                # Surname only
                surname = name_parts[-1].lower()
                if surname not in abbrev_cache:
                    abbrev_cache[surname] = full_name
                elif abbrev_cache[surname] != full_name:
                    # Conflict - keep first, mark as ambiguous
                    abbrev_cache[surname] = None

                # First name only (with prefix to avoid conflicts)
                firstname = name_parts[0].lower()
                key = f"first_{firstname}"
                if key not in abbrev_cache:
                    abbrev_cache[key] = full_name

                # First initial + surname (e.g., "a raghuvanshi")
                initial = name_parts[0][0].lower()
                surname = name_parts[-1].lower()
                key = f"{initial} {surname}"
                abbrev_cache[key] = full_name

                # Double initial + surname (e.g., "tm head", "rm patidar")
                if len(name_parts) >= 2:
                    first_initial = name_parts[0][0].lower()

                    # If 3+ parts, use first two initials
                    if len(name_parts) >= 3:
                        second_initial = name_parts[1][0].lower()
                        double_initial = f"{first_initial}{second_initial}"
                        key = f"{double_initial} {surname}"
                        abbrev_cache[key] = full_name

                # First initial + full surname words (e.g., "b sai sudharsan")
                if len(name_parts) >= 3:
                    surname_part = ' '.join(name_parts[1:]).lower()
                    key = f"{initial} {surname_part}"
                    abbrev_cache[key] = full_name

            # Single name players (e.g., "Azmatullah")
            if len(name_parts) == 1:
                single_name = name_parts[0].lower()
                abbrev_cache[single_name] = full_name
                abbrev_cache[f"first_{single_name}"] = full_name

        # Remove None values (ambiguous surnames)
        abbrev_cache = {k: v for k, v in abbrev_cache.items() if v is not None}

        return abbrev_cache

    def _find_full_player_name(self, short_name: str, match_players: pd.DataFrame, matchid: str) -> str:
        """
        Find full player name from match_players DataFrame
        Handles regex metacharacters safely and various name formats

        Supported formats:
        - Full names: "Virat Kohli" → "Virat Kohli"
        - Surnames only: "Kohli" → "Virat Kohli"
        - Abbreviated first names: "A Raghuvanshi" → "Angkrish Raghuvanshi"
        - Single first names: "Prabhsimran" → "Prabhsimran Singh"
        - Double initials: "TM Head" → "Travis Head"

        Args:
            short_name: Abbreviated name from event data
            match_players: DataFrame with full player names for this match
            matchid: Match identifier (for logging)

        Returns:
            str: Full player name or original short name if not found
        """
        try:
            # Handle null/empty
            if pd.isna(short_name) or short_name == '':
                return short_name

            # Normalize
            short_name = str(short_name).strip()

            if not short_name:
                return short_name

            short_name_lower = short_name.lower()

            # Strategy 1: Exact match (case-insensitive)
            exact_match = match_players[
                match_players['player_name'].str.lower() == short_name_lower
            ]
            if not exact_match.empty:
                return exact_match.iloc[0]['player_name']

            # Strategy 2: Handle abbreviated names with initials
            parts = short_name.split()

            # Pattern A: Single initial + surname (e.g., "A Raghuvanshi")
            if len(parts) == 2 and len(parts[0]) == 1:
                first_initial = parts[0].lower()
                surname = parts[1].lower()

                # Find players where first name starts with initial and surname matches
                matches = match_players[
                    (match_players['player_name'].str.lower().str.startswith(first_initial)) &
                    (match_players['player_name'].str.lower().str.contains(
                        surname, na=False, regex=False
                    ))
                ]

                if len(matches) == 1:
                    full_name = matches.iloc[0]['player_name']
                    logger.debug(f"Single initial match: '{short_name}' → '{full_name}'")
                    return full_name
                elif len(matches) > 1:
                    # Multiple matches - try to find best one where surname is last word
                    for _, player in matches.iterrows():
                        player_parts = player['player_name'].lower().split()
                        if player_parts[-1] == surname:
                            logger.debug(f"Best single initial match: '{short_name}' → '{player['player_name']}'")
                            return player['player_name']
                    # Return first match
                    return matches.iloc[0]['player_name']

            # Pattern B: Double/multiple initials + surname (e.g., "TM Head", "RM Patidar")
            if len(parts) == 2 and len(parts[0]) >= 2 and parts[0].isupper():
                initials = parts[0].lower()
                surname = parts[1].lower()

                # Find players where surname matches
                surname_matches = match_players[
                    match_players['player_name'].str.lower().str.split().str[-1] == surname
                ]

                if not surname_matches.empty:
                    # Check if initials match
                    for _, player in surname_matches.iterrows():
                        player_parts = player['player_name'].split()

                        # Build initials from full name
                        if len(player_parts) >= len(initials):
                            player_initials = ''.join([p[0].lower() for p in player_parts[:len(initials)]])

                            if player_initials == initials:
                                logger.debug(f"Double initial match: '{short_name}' → '{player['player_name']}'")
                                return player['player_name']

                    # If no exact initial match, try first initial only
                    first_initial = initials[0]
                    for _, player in surname_matches.iterrows():
                        if player['player_name'][0].lower() == first_initial:
                            logger.debug(f"Partial initial match: '{short_name}' → '{player['player_name']}'")
                            return player['player_name']

            # Strategy 3: Single word name - could be surname or first name
            if len(short_name.split()) == 1:
                single_word = short_name.lower()

                # Try surname match (last word of full name)
                surname_matches = []
                for _, player in match_players.iterrows():
                    player_parts = player['player_name'].lower().split()
                    if player_parts[-1] == single_word:
                        surname_matches.append(player['player_name'])

                if len(surname_matches) == 1:
                    logger.debug(f"Surname match: '{short_name}' → '{surname_matches[0]}'")
                    return surname_matches[0]
                elif len(surname_matches) > 1:
                    logger.debug(f"Multiple surname matches for '{short_name}': {surname_matches}")
                    return surname_matches[0]  # Return first match

                # Try first name match (first word of full name)
                firstname_matches = []
                for _, player in match_players.iterrows():
                    player_parts = player['player_name'].lower().split()
                    if player_parts[0] == single_word:
                        firstname_matches.append(player['player_name'])

                if len(firstname_matches) == 1:
                    logger.debug(f"First name match: '{short_name}' → '{firstname_matches[0]}'")
                    return firstname_matches[0]
                elif len(firstname_matches) > 1:
                    logger.debug(f"Multiple first name matches for '{short_name}': {firstname_matches}")
                    return firstname_matches[0]

            # Strategy 4: Partial word matching (existing logic)
            parts = [p for p in short_name.split() if len(p) >= 2]

            for part in parts:
                try:
                    # Use regex=False to avoid metacharacter issues
                    matches = match_players[
                        match_players['player_name'].str.lower().str.contains(
                            part.lower(),
                            na=False,
                            regex=False
                        )
                    ]

                    if len(matches) == 0:
                        continue

                    if len(matches) == 1:
                        full_name = matches.iloc[0]['player_name']
                        logger.debug(f"Part match: '{short_name}' → '{full_name}' via '{part}'")
                        return full_name

                    # Multiple matches - find best
                    part_lower = part.lower()
                    for _, player in matches.iterrows():
                        player_parts = player['player_name'].lower().split()
                        if part_lower in player_parts:
                            logger.debug(f"Best part match: '{short_name}' → '{player['player_name']}'")
                            return player['player_name']

                    # Return first match
                    return matches.iloc[0]['player_name']

                except Exception as e:
                    logger.warning(f"Error matching part '{part}': {e}")
                    continue

            # No match found
            logger.debug(f"No enrichment for '{short_name}' in match {matchid}")
            return short_name

        except Exception as e:
            logger.error(f"Error in _find_full_player_name for '{short_name}': {e}")
            return short_name

    def _deduplicate_player_names(self, df: pd.DataFrame, players_df: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplicate player names within the same team (per-match)

        Handles cases where both abbreviated and full names exist for the same player:
        - "Bhuvneshwar" + "Bhuvneshwar Kumar" → "Bhuvneshwar Kumar"
        - "TM Head" + "Travis Head" → "Travis Head"
        - "Kohli" + "Virat Kohli" → "Virat Kohli"

        Args:
            df: Match events DataFrame
            players_df: Match players DataFrame with team info

        Returns:
            DataFrame with deduplicated player names
        """
        logger.info("Deduplicating player names within teams (per-match)...")

        # Columns to deduplicate
        player_columns = ['batsman', 'bowler', 'fielder_name']

        total_deduped = 0

        for matchid in df['matchid'].unique():
            # Get all teams in this match from the innings column
            match_mask = df['matchid'] == matchid
            teams_in_match = df.loc[match_mask, 'innings'].unique()

            for team in teams_in_match:
                if pd.isna(team) or team == '' or 'super over' in str(team).lower():
                    continue

                # Get all player names from match_players for this team
                team_players = players_df[
                    (players_df['matchid'] == matchid) &
                    (players_df['team'] == team)
                ]['player_name'].unique()

                if len(team_players) == 0:
                    continue

                # Build deduplication mapping for this team
                dedup_map = self._build_team_deduplication_map(team_players)

                if not dedup_map:
                    continue

                # Apply deduplication to each player column
                team_mask = match_mask & (df['innings'] == team)

                for column in player_columns:
                    if column not in df.columns:
                        continue

                    for idx in df[team_mask].index:
                        player_name = df.at[idx, column]

                        if pd.isna(player_name) or player_name == '':
                            continue

                        player_name_str = str(player_name).strip()

                        # Check if this name should be replaced
                        if player_name_str in dedup_map:
                            full_name = dedup_map[player_name_str]
                            if full_name != player_name_str:
                                df.at[idx, column] = full_name
                                total_deduped += 1
                                logger.debug(f"Deduped: '{player_name_str}' → '{full_name}' ({team})")

        logger.info(f"Per-match deduplication: {total_deduped} entries deduplicated")

        return df

    def _build_team_deduplication_map(self, team_players: list) -> dict:
        """
        Build mapping of abbreviated names to full names for a single team

        Examples:
        - ["Virat Kohli", "Kohli"] → {"Kohli": "Virat Kohli"}
        - ["Travis Head", "TM Head"] → {"TM Head": "Travis Head", "tm head": "Travis Head"}
        - ["Bhuvneshwar Kumar", "Bhuvneshwar"] → {"Bhuvneshwar": "Bhuvneshwar Kumar"}

        Args:
            team_players: List of full player names from match_players

        Returns:
            dict: Mapping of abbreviated names to full names
        """
        dedup_map = {}

        # Sort by length (longest first) to prioritize full names
        sorted_players = sorted(team_players, key=len, reverse=True)

        for i, full_name in enumerate(sorted_players):
            name_parts = full_name.split()
            full_name_lower = full_name.lower()

            # Add full name to itself (identity mapping)
            dedup_map[full_name] = full_name
            dedup_map[full_name_lower] = full_name

            # Check all other names to see if they're abbreviations of this one
            for short_name in sorted_players[i+1:]:
                short_name_lower = short_name.lower()

                # Skip if already mapped
                if short_name in dedup_map and dedup_map[short_name] != short_name:
                    continue

                # Pattern 1: Short name is substring of full name
                # "Bhuvneshwar" is substring of "Bhuvneshwar Kumar"
                if short_name_lower in full_name_lower and short_name_lower != full_name_lower:
                    dedup_map[short_name] = full_name
                    logger.debug(f"Substring match: '{short_name}' → '{full_name}'")
                    continue

                # Pattern 2: Surname only match
                # "Kohli" → "Virat Kohli"
                if len(name_parts) >= 2:
                    surname = name_parts[-1].lower()
                    if short_name_lower == surname:
                        dedup_map[short_name] = full_name
                        logger.debug(f"Surname match: '{short_name}' → '{full_name}'")
                        continue

                # Pattern 3: First name only match
                # "Prabhsimran" → "Prabhsimran Singh"
                if len(name_parts) >= 2:
                    firstname = name_parts[0].lower()
                    if short_name_lower == firstname:
                        dedup_map[short_name] = full_name
                        logger.debug(f"First name match: '{short_name}' → '{full_name}'")
                        continue

                # Pattern 4: Initials + surname match
                # "TM Head" → "Travis Head"
                # "RM Patidar" → "Rajat Patidar"
                short_parts = short_name.split()
                if len(short_parts) == 2 and len(name_parts) >= 2:
                    short_initial = short_parts[0].lower()
                    short_surname = short_parts[1].lower()
                    full_surname = name_parts[-1].lower()

                    # Check if:
                    # 1. First part is 1-2 letters (initials)
                    # 2. Second part matches surname
                    # 3. Initials match first letter(s) of full name
                    if (len(short_initial) <= 2 and
                        short_surname == full_surname):

                        # Check if initials match
                        if len(short_initial) == 1:
                            # Single initial (e.g., "T" in "TM Head")
                            if name_parts[0][0].lower() == short_initial[0]:
                                dedup_map[short_name] = full_name
                                logger.debug(f"Initial+surname match: '{short_name}' → '{full_name}'")
                                continue
                        elif len(short_initial) == 2:
                            # Double initial (e.g., "TM" in "TM Head")
                            if len(name_parts) >= 2:
                                initials = name_parts[0][0].lower() + name_parts[1][0].lower()
                                if initials == short_initial.lower():
                                    dedup_map[short_name] = full_name
                                    logger.debug(f"Double initial+surname match: '{short_name}' → '{full_name}'")
                                    continue

                # Pattern 5: First initial + rest of name
                # "B Sai Sudharsan" → "Baba Sai Sudharsan" (if exists)
                if len(short_parts) >= 2 and len(name_parts) >= 2:
                    if (len(short_parts[0]) == 1 and
                        short_parts[0].lower() == name_parts[0][0].lower()):
                        # Check if rest matches
                        short_rest = ' '.join(short_parts[1:]).lower()
                        full_rest = ' '.join(name_parts[1:]).lower()
                        if short_rest == full_rest:
                            dedup_map[short_name] = full_name
                            logger.debug(f"Initial+rest match: '{short_name}' → '{full_name}'")
                            continue

        return dedup_map

    def _global_player_deduplication(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final global deduplication pass across ALL data

        Handles cases where abbreviated names persist across multiple matches:
        - "Kohli" + "Virat Kohli" → "Virat Kohli"
        - "TM Head" + "Travis Head" → "Travis Head"
        - "Agarwal" + "Mayank Agarwal" → "Mayank Agarwal"
        - "Salt" + "Phil Salt" → "Phil Salt"  (case-insensitive)

        Args:
            df: Match events DataFrame

        Returns:
            DataFrame with globally deduplicated player names
        """
        logger.info("Running global player name deduplication...")

        player_columns = ['batsman', 'bowler', 'fielder_name']
        total_deduped = 0

        # Process each team separately
        for team in df['innings'].unique():
            if pd.isna(team) or team == '':
                continue

            team_mask = df['innings'] == team

            # For each player column, build a global deduplication map for this team
            for column in player_columns:
                if column not in df.columns:
                    continue

                # Get all unique player names for this team in this column
                team_players = df.loc[team_mask, column].dropna().unique()

                if len(team_players) == 0:
                    continue

                # Build global deduplication map
                dedup_map = self._build_global_dedup_map(team_players)

                if not dedup_map:
                    continue

                # Apply deduplication (CASE-INSENSITIVE LOOKUP)
                for idx in df[team_mask].index:
                    player_name = df.at[idx, column]

                    if pd.isna(player_name) or player_name == '':
                        continue

                    player_name_str = str(player_name).strip()

                    # Try exact match first
                    if player_name_str in dedup_map:
                        full_name = dedup_map[player_name_str]
                        if full_name != player_name_str:
                            df.at[idx, column] = full_name
                            total_deduped += 1
                            logger.debug(f"Global dedup: '{player_name_str}' → '{full_name}' ({team}, {column})")
                    # Try case-insensitive match
                    elif player_name_str.lower() in dedup_map:
                        full_name = dedup_map[player_name_str.lower()]
                        if full_name != player_name_str:
                            df.at[idx, column] = full_name
                            total_deduped += 1
                            logger.debug(f"Global dedup (case-insensitive): '{player_name_str}' → '{full_name}' ({team}, {column})")

        logger.info(f"Global deduplication: {total_deduped} entries deduplicated")

        return df

    def _build_global_dedup_map(self, player_names: list) -> dict:
        """
        Build global deduplication mapping for a list of player names

        Prioritizes longer, more complete names over shorter versions
        Creates BOTH exact case and lowercase keys for case-insensitive matching

        Args:
            player_names: List of all player names for a team/column

        Returns:
            dict: Mapping of short names to full names (with both case variations)
        """
        dedup_map = {}

        # Convert to list and sort by length (longest first)
        sorted_names = sorted(set(player_names), key=len, reverse=True)

        for i, long_name in enumerate(sorted_names):
            long_name_lower = long_name.lower()
            long_parts = long_name.split()

            # Add identity mapping (BOTH cases)
            dedup_map[long_name] = long_name  # Exact case
            dedup_map[long_name_lower] = long_name  # Lowercase

            # Check all shorter names
            for short_name in sorted_names[i+1:]:
                short_name_lower = short_name.lower()
                short_parts = short_name.split()

                # Skip if already mapped to a different name
                if short_name in dedup_map and dedup_map[short_name] != short_name:
                    continue
                if short_name_lower in dedup_map and dedup_map[short_name_lower] != short_name and dedup_map[short_name_lower] != long_name:
                    continue

                matched = False

                # Pattern 1: Substring match
                # "Bhuvneshwar" in "Bhuvneshwar Kumar"
                if short_name_lower in long_name_lower and short_name_lower != long_name_lower:
                    dedup_map[short_name] = long_name  # Exact case
                    dedup_map[short_name_lower] = long_name  # Lowercase
                    logger.debug(f"  Global substring: '{short_name}' → '{long_name}'")
                    matched = True

                # Pattern 2: Surname match
                # "Kohli" → "Virat Kohli", "Salt" → "Phil Salt"
                if not matched and len(long_parts) >= 2 and len(short_parts) == 1:
                    if short_name_lower == long_parts[-1].lower():
                        dedup_map[short_name] = long_name
                        dedup_map[short_name_lower] = long_name
                        logger.debug(f"  Global surname: '{short_name}' → '{long_name}'")
                        matched = True

                # Pattern 3: First name match
                # "Prabhsimran" → "Prabhsimran Singh"
                if not matched and len(long_parts) >= 2 and len(short_parts) == 1:
                    if short_name_lower == long_parts[0].lower():
                        dedup_map[short_name] = long_name
                        dedup_map[short_name_lower] = long_name
                        logger.debug(f"  Global first name: '{short_name}' → '{long_name}'")
                        matched = True

                # Pattern 4: Initial + surname match
                # "TM Head" → "Travis Head"
                # "D Padikkal" → "Devdutt Padikkal"
                # "RM Patidar" → "Rajat Patidar"
                if not matched and len(short_parts) == 2 and len(long_parts) >= 2:
                    short_initial = short_parts[0].lower()
                    short_surname = short_parts[1].lower()
                    long_surname = long_parts[-1].lower()

                    if len(short_initial) <= 2 and short_surname == long_surname:
                        # Check initials
                        if len(short_initial) == 1:
                            if long_parts[0][0].lower() == short_initial[0]:
                                dedup_map[short_name] = long_name
                                dedup_map[short_name_lower] = long_name
                                logger.debug(f"  Global initial+surname: '{short_name}' → '{long_name}'")
                                matched = True
                        elif len(short_initial) == 2 and len(long_parts) >= 2:
                            initials = long_parts[0][0].lower() + long_parts[1][0].lower()
                            if initials == short_initial.lower():
                                dedup_map[short_name] = long_name
                                dedup_map[short_name_lower] = long_name
                                logger.debug(f"  Global double initial: '{short_name}' → '{long_name}'")
                                matched = True

                # Pattern 5: Partial name match with initial
                # "B Sai Sudharsan" → "Baba Sai Sudharsan" (if first char matches)
                if not matched and len(short_parts) >= 2 and len(long_parts) >= 2:
                    if len(short_parts[0]) == 1 and short_parts[0].lower() == long_parts[0][0].lower():
                        short_rest = ' '.join(short_parts[1:]).lower()
                        long_rest = ' '.join(long_parts[1:]).lower()
                        if short_rest == long_rest:
                            dedup_map[short_name] = long_name
                            dedup_map[short_name_lower] = long_name
                            logger.debug(f"  Global initial+rest: '{short_name}' → '{long_name}'")
                            matched = True

                # Pattern 6: Nickname variations
                # "Mitch" → "Mitchell", handles "Mitch Marsh" → "Mitchell Marsh"
                if not matched and len(short_parts) == len(long_parts):
                    # Check if all parts match when normalized
                    all_match = True
                    for sp, lp in zip(short_parts, long_parts):
                        sp_lower = sp.lower()
                        lp_lower = lp.lower()
                        # Check if short part is substring of long part OR they match
                        if sp_lower != lp_lower and sp_lower not in lp_lower:
                            all_match = False
                            break

                    if all_match:
                        dedup_map[short_name] = long_name
                        dedup_map[short_name_lower] = long_name
                        logger.debug(f"  Global nickname: '{short_name}' → '{long_name}'")
                        matched = True

        return dedup_map

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
            logger.info(f"Removed Bronze audit columns: {columns_to_drop}")

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
            logger.warning(f"Required columns not found for dismissal extraction")
            return df

        logger.info(f"Extracting dismissal information...")

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

        logger.info(f"Extracted dismissal info for {extracted_count} wickets")

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
            logger.debug(f"Error parsing dismissal text '{dismissal_text}': {e}")

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
            logger.warning(f"Column '{column}' not found, skipping score conversion")
            return df

        logger.info(f"Converting '{column}' column to numeric...")

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

        logger.info(f"Score conversion: {converted} converted, {not_converted} not converted (set to 0)")

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

        logger.info(f"Cleaned columns: {list(columns.keys())}")
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
                    logger.info(f"Validation: Removed {removed} rows with null {column}")

        return df

    def _enrich_team_names(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Enrich innings column with actual team names from bronze.match_players

        Handles:
        - Team abbreviations (RCB, SRH, PBKS, etc.)
        - Innings formats (innings_1, 1st innings, etc.)
        - Super Over innings (resolves to actual team names)
        - Full team names (no change)
        """
        if table_name != 'match_events':
            return df

        if 'innings' not in df.columns or 'matchid' not in df.columns:
            logger.warning("Cannot enrich team names - required columns missing")
            return df

        logger.info("Enriching innings with team names from bronze.match_players...")

        # Predefined IPL team abbreviation mapping (EXPANDED)
        IPL_TEAM_ABBREV = {
            'rcb': 'Royal Challengers Bengaluru',
            'mi': 'Mumbai Indians',
            'csk': 'Chennai Super Kings',
            'dc': 'Delhi Capitals',
            'kkr': 'Kolkata Knight Riders',
            'rr': 'Rajasthan Royals',
            'srh': 'Sunrisers Hyderabad',
            'pbks': 'Punjab Kings',
            'gt': 'Gujarat Titans',
            'lsg': 'Lucknow Super Giants',
            # Add more variations
            'punjab kings': 'Punjab Kings',
            'sunrisers hyderabad': 'Sunrisers Hyderabad',
            'royal challengers bangalore': 'Royal Challengers Bengaluru',
            'royal challengers bengaluru': 'Royal Challengers Bengaluru',
            # Legacy names
            'kings xi punjab': 'Punjab Kings',
            'kxip': 'Punjab Kings',
            'delhi daredevils': 'Delhi Capitals',
            'dd': 'Delhi Capitals',
            'rising pune supergiants': 'Rising Pune Supergiant',
            'rising pune supergiant': 'Rising Pune Supergiant',
            'rps': 'Rising Pune Supergiant',
            'pune warriors': 'Pune Warriors India',
            'pw': 'Pune Warriors India',
            'deccan chargers': 'Deccan Chargers',
            'kochi tuskers kerala': 'Kochi Tuskers Kerala'
        }

        # Get team-innings mappings from bronze.match_players
        query = f"""
            SELECT DISTINCT
                matchid,
                innings,
                team,
                player_name
            FROM {self.source_schema}.match_players
            WHERE innings IS NOT NULL
              AND team IS NOT NULL
              AND innings != ''
              AND team != ''
              AND is_active = TRUE
            ORDER BY matchid, innings
        """

        conn = get_connection()
        try:
            df_team_innings = pd.read_sql(query, conn)
            logger.info(f"Loaded {len(df_team_innings)} team-innings mappings from bronze.match_players")
        except Exception as e:
            logger.warning(f"Could not load team-innings mappings: {e}")
            return df
        finally:
            conn.close()

        if df_team_innings.empty:
            logger.warning("No team-innings mappings found in bronze.match_players")
            logger.info("Will use IPL abbreviation mapping only")

        # Strategy 1: Direct mapping (matchid, innings) -> team
        direct_mapping = {}
        if not df_team_innings.empty:
            for _, row in df_team_innings.iterrows():
                key = (row['matchid'], row['innings'])
                direct_mapping[key] = row['team']
                # Also add lowercase version
                key_lower = (row['matchid'], row['innings'].lower())
                direct_mapping[key_lower] = row['team']

        # Strategy 2: Match-based team mapping (matchid, order) -> team
        match_teams = {}
        if not df_team_innings.empty:
            for matchid, group in df_team_innings.groupby('matchid'):
                teams = group.sort_values('innings')['team'].unique().tolist()
                if len(teams) >= 1:
                    match_teams[(matchid, 1)] = teams[0]
                if len(teams) >= 2:
                    match_teams[(matchid, 2)] = teams[1]

        # Strategy 3: Create reverse abbreviation mapping (abbrev -> full names by match)
        match_abbrev_mapping = {}
        if not df_team_innings.empty:
            for matchid, group in df_team_innings.groupby('matchid'):
                teams = group['team'].unique()
                for team in teams:
                    team_lower = team.lower()
                    # Add full name mapping
                    match_abbrev_mapping[(matchid, team_lower)] = team

                    # Create initials (e.g., "Royal Challengers Bengaluru" -> "RCB")
                    team_parts = team.split()
                    if len(team_parts) >= 2:
                        initials = ''.join([p[0] for p in team_parts]).lower()
                        match_abbrev_mapping[(matchid, initials)] = team

                    # Add first word
                    if team_parts:
                        match_abbrev_mapping[(matchid, team_parts[0].lower())] = team

        logger.info(f"Created {len(direct_mapping)} direct mappings")
        logger.info(f"Created {len(match_teams)} match-team mappings")
        logger.info(f"Created {len(match_abbrev_mapping)} abbreviation mappings")

        # Get unique matchids in match_events
        events_matchids = set(df['matchid'].unique())
        players_matchids = set(df_team_innings['matchid'].unique()) if not df_team_innings.empty else set()
        missing_matchids = events_matchids - players_matchids

        if missing_matchids:
            logger.warning(f"Found {len(missing_matchids)} matches in events without player data")
            logger.warning(f"Will use IPL abbreviation mapping for these matches: {list(missing_matchids)[:5]}")

        # Map innings to team names
        def map_innings_to_team(row):
            if pd.isna(row['innings']) or row['innings'] == '':
                return row['innings']

            matchid = row['matchid']
            innings_str = str(row['innings']).strip()
            innings_lower = innings_str.lower()

            # SKIP Super Over innings temporarily (will be resolved later)
            if 'super over' in innings_lower:
                return innings_str

            # Strategy 1: Check if already a full team name (exact match)
            if not df_team_innings.empty:
                for team in df_team_innings['team'].unique():
                    if team.lower() == innings_lower:
                        return team  # Already enriched

            # Strategy 2: Direct match from match_players (case-insensitive)
            key = (matchid, innings_str)
            if key in direct_mapping:
                return direct_mapping[key]

            key_lower = (matchid, innings_lower)
            if key_lower in direct_mapping:
                return direct_mapping[key_lower]

            # Strategy 3: Match-specific abbreviation mapping
            key = (matchid, innings_lower)
            if key in match_abbrev_mapping:
                return match_abbrev_mapping[key]

            # Strategy 4: IPL abbreviation lookup (USE AGGRESSIVELY)
            if innings_lower in IPL_TEAM_ABBREV:
                full_name = IPL_TEAM_ABBREV[innings_lower]

                # If we have player data for this match, verify the team exists
                if matchid in players_matchids:
                    match_teams_list = df_team_innings[df_team_innings['matchid'] == matchid]['team'].tolist()
                    if full_name in match_teams_list:
                        return full_name
                else:
                    # No player data for this match - use abbreviation mapping directly
                    return full_name

            # Strategy 5: Handle innings_1, innings_2, 1st innings formats
            if any(x in innings_lower for x in ['innings_1', '1st innings', 'first innings']) or innings_lower == '1':
                if (matchid, 1) in match_teams:
                    return match_teams[(matchid, 1)]
                # Try direct keys
                for suffix in ['innings_1', '1st innings', 'first innings']:
                    key = (matchid, suffix)
                    if key in direct_mapping:
                        return direct_mapping[key]

            if any(x in innings_lower for x in ['innings_2', '2nd innings', 'second innings']) or innings_lower == '2':
                if (matchid, 2) in match_teams:
                    return match_teams[(matchid, 2)]
                # Try direct keys
                for suffix in ['innings_2', '2nd innings', 'second innings']:
                    key = (matchid, suffix)
                    if key in direct_mapping:
                        return direct_mapping[key]

            # Strategy 6: Fuzzy match - check if innings contains team name
            if not df_team_innings.empty:
                match_teams_data = df_team_innings[df_team_innings['matchid'] == matchid]['team'].unique()
                for team in match_teams_data:
                    team_words = set(team.lower().split())
                    innings_words = set(innings_lower.split())
                    if team_words & innings_words:  # Intersection
                        return team

            # No match found
            return innings_str

        # Store original for comparison
        original_innings_values = df['innings'].copy()

        # Apply mapping
        df['innings'] = df.apply(map_innings_to_team, axis=1)

        # Count enriched records
        enriched_count = (df['innings'] != original_innings_values).sum()
        logger.info(f"Enriched {enriched_count} records with team names")

        # Show enrichment statistics
        if enriched_count > 0:
            try:
                comparison_df = pd.DataFrame({
                    'original': original_innings_values,
                    'enriched': df['innings']
                })

                comparison_df = comparison_df[comparison_df['original'] != comparison_df['enriched']]

                if not comparison_df.empty:
                    innings_mapping = comparison_df.groupby(['original', 'enriched']).size().reset_index(name='count')

                    logger.info("Innings enrichment summary:")
                    for _, row in innings_mapping.head(20).iterrows():
                        logger.info(f"  '{row['original']}' -> '{row['enriched']}' ({row['count']} rows)")
                else:
                    logger.info("No innings values changed (all already full names)")

            except Exception as e:
                logger.warning(f"Could not generate enrichment summary: {e}")
                logger.info(f"Total enriched: {enriched_count} records")

        # Resolve Super Over innings to team names
        df = self._resolve_super_over_teams(df, df_team_innings)

        # Final validation - check if any abbreviations remain
        final_innings = df['innings'].unique()
        remaining_abbrevs = [v for v in final_innings if
                             v.lower() in IPL_TEAM_ABBREV and v.lower() in ['rcb', 'srh', 'pbks', 'mi', 'csk', 'dc',
                                                                             'kkr', 'rr', 'gt', 'lsg']]

        if remaining_abbrevs:
            logger.error(f"WARNING: {len(remaining_abbrevs)} abbreviations still remain: {remaining_abbrevs}")
        else:
            logger.info("All team abbreviations successfully enriched")

        # Check for remaining Super Over references
        remaining_super_overs = [v for v in final_innings if 'super over' in v.lower()]
        if remaining_super_overs:
            logger.warning(f"Note: {len(remaining_super_overs)} Super Over innings remain unresolved")
        else:
            logger.info("All Super Over innings resolved to team names")

        return df

    def _resolve_super_over_teams(self, df: pd.DataFrame, df_team_innings: pd.DataFrame) -> pd.DataFrame:
        """
        Replace 'Super Over 1', 'Super Over 2' etc. with actual team names
        by looking up the player's team from match_players

        Args:
            df: Match events DataFrame
            df_team_innings: Team-innings mappings from match_players

        Returns:
            DataFrame with Super Over innings replaced with team names
        """
        if df_team_innings.empty:
            logger.warning("Cannot resolve Super Over teams - no match_players data")
            return df

        # Find all Super Over rows
        super_over_mask = df['innings'].str.contains('Super Over', case=False, na=False)
        super_over_count = super_over_mask.sum()

        if super_over_count == 0:
            logger.info("No Super Over innings found")
            return df

        logger.info(f"Resolving {super_over_count} Super Over innings to team names...")

        # Create player-team mapping from match_players
        player_team_map = {}
        for _, row in df_team_innings.iterrows():
            key = (row['matchid'], row['player_name'])
            player_team_map[key] = row['team']

        resolved_count = 0
        unresolved_count = 0

        # Resolve each Super Over row
        for idx in df[super_over_mask].index:
            matchid = df.at[idx, 'matchid']
            batsman = df.at[idx, 'batsman']
            bowler = df.at[idx, 'bowler']
            original_innings = df.at[idx, 'innings']

            # Try to find team from batsman first
            team = None
            if pd.notna(batsman) and batsman != '':
                key = (matchid, batsman)
                team = player_team_map.get(key)

            # If not found, try bowler
            if not team and pd.notna(bowler) and bowler != '':
                key = (matchid, bowler)
                team = player_team_map.get(key)

            # If found, replace innings
            if team:
                df.at[idx, 'innings'] = team
                resolved_count += 1
                logger.debug(f"Resolved: '{original_innings}' -> '{team}' for {batsman or bowler}")
            else:
                unresolved_count += 1
                logger.debug(f"Could not resolve Super Over team for match {matchid}, batsman={batsman}, bowler={bowler}")

        logger.info(f"Super Over resolution: {resolved_count} resolved, {unresolved_count} unresolved")

        return df