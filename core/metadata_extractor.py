"""
Metadata Extractor - Extracts match metadata and player names from HTML
"""
from bs4 import BeautifulSoup
from utils.logger import setup_logger
import pandas as pd
import re
import numpy as np

logger = setup_logger(__name__)


class MetadataExtractor:
    @staticmethod
    def extract_metadata(html_content, match_id):
        """
        Extract match metadata from HTML content

        Args:
            html_content: HTML string from page source
            match_id: Match identifier

        Returns:
            Dictionary containing match metadata
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            metadata = {}

            # Find the match details table
            match_details_rows = soup.find_all("div", class_="ds-border-color-border-secondary ds-flex ds-border-t")

            if not match_details_rows:
                logger.warning(f"Match details div not found for match {match_id}")
                metadata["MatchID"] = match_id
                return metadata

            # Parse each row
            for row in match_details_rows:
                # Find all span elements in the row
                spans = row.find_all("span")

                if len(spans) >= 2:
                    # First span is typically the key, subsequent spans contain the value
                    key = spans[0].get_text(strip=True)

                    # Combine all remaining spans for the value (handles multi-span values)
                    value_parts = [span.get_text(strip=True) for span in spans[1:]]
                    value = " ".join(value_parts).strip()

                    if key and value:
                        metadata[key] = value
                        logger.debug(f"Extracted: {key} = {value}")

                elif len(spans) == 1:
                    # Single span might be a standalone value (like venue)
                    text = spans[0].get_text(strip=True)
                    if text and "Venue" not in metadata:
                        metadata["Venue"] = text

            # Add match ID
            metadata["MatchID"] = match_id

            logger.info(f"Extracted {len(metadata)} metadata fields for match {match_id}")
            logger.debug(f"Metadata keys: {list(metadata.keys())}")

            return metadata

        except Exception as e:
            logger.error(f"Error extracting metadata for match {match_id}: {e}", exc_info=True)
            return {"MatchID": match_id}

    @staticmethod
    def extract_player_names(html_content, match_id):
        """
        Extract full names of all players from scorecard tables
        Includes regular players and impact players (identified by icon)

        Returns:
            pandas DataFrame with columns:
            - matchid, innings, team, player_name, batted, batting_position, player_type
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            all_players = []

            logger.info(f"Extracting player names for match {match_id}")

            # Find all batting scorecard table
            batting_tables = soup.find_all("table", class_=lambda x: x and "ci-scorecard-table" in x)

            if not batting_tables:
                logger.warning(f"No batting scorecard tables found for match {match_id}")
                return pd.DataFrame(
                    columns=['matchid', 'innings', 'team', 'player_name', 'batted', 'batting_position', 'player_type'])

            logger.info(f"Found {len(batting_tables)} batting scorecard table(s)")

            # STEP 1: Extract all batting teams first
            batting_teams = []
            for idx, table in enumerate(batting_tables, start=1):
                team_name = MetadataExtractor._extract_team_name_from_table(table, idx)
                batting_teams.append(team_name)
                logger.info(f"  Innings {idx}: {team_name} (batting)")

            # STEP 2: Process each batting table
            for idx, table in enumerate(batting_tables, start=1):
                innings_key = f"innings_{idx}"
                team_name = batting_teams[idx - 1]

                # Extract players who batted and did not bat
                batting_players = MetadataExtractor._extract_batting_players(table, match_id, innings_key, team_name)
                all_players.extend(batting_players)

                batted_count = sum(1 for p in batting_players if p['batted'])
                did_not_bat_count = len(batting_players) - batted_count

                logger.info(f"  {innings_key} ({team_name}): {batted_count} batted, {did_not_bat_count} did not bat")

            # STEP 3: Extract bowlers from bowling tables
            bowling_tables = soup.find_all("table", class_="ds-w-full ds-v2-table ds-v2-table-md ds-table-auto")

            # Filter out batting tables (already processed)
            bowling_only_tables = [t for t in bowling_tables if "ci-scorecard-table" not in t.get("class", [])]

            logger.info(f"Found {len(bowling_only_tables)} bowling table(s)")

            for idx, table in enumerate(bowling_only_tables, start=1):
                innings_key = f"innings_{idx}"

                # FIXED: Get OPPOSITE team (bowling team, not batting team)
                batting_team = batting_teams[idx - 1] if idx <= len(batting_teams) else None
                bowling_team = MetadataExtractor._get_opposite_team(batting_team, batting_teams)

                logger.info(f"  {innings_key} bowling: {bowling_team} (bowling against {batting_team})")

                # Extract bowlers with correct team
                bowlers = MetadataExtractor._extract_bowlers(table, match_id, innings_key, bowling_team)
                all_players.extend(bowlers)

                logger.info(f"  {innings_key} bowling ({bowling_team}): {len(bowlers)} bowlers")

            # Create DataFrame
            df = pd.DataFrame(all_players)
            df = df.replace(np.nan, None)

            # Define priority order (impact > regular)
            type_priority = {'impact': 1, 'regular': 2}
            df['priority'] = df['player_type'].map(type_priority)

            # Sort by priority (lower number = higher priority)
            df = df.sort_values('priority')

            # Keep first occurrence (which is highest priority due to sorting)
            df_unique = df.drop_duplicates(subset=['matchid', 'player_name'], keep='first')

            # Remove temporary priority column
            df_unique = df_unique.drop('priority', axis=1)

            logger.info(f"Created DataFrame with {len(df)} player records, {len(df_unique)} unique players")

            return df_unique

        except Exception as e:
            logger.error(f"Error extracting player names for match {match_id}: {e}", exc_info=True)
            return pd.DataFrame(
                columns=['matchid', 'innings', 'team', 'player_name', 'batted', 'batting_position', 'player_type'])

    @staticmethod
    def _get_opposite_team(current_team, all_teams):
        """
        Get the opposite team name

        Args:
            current_team: Current team (batting team)
            all_teams: List of all teams in the match

        Returns:
            Name of the opposite team (bowling team)
        """
        if not current_team or not all_teams or len(all_teams) < 2:
            logger.warning(f"Cannot determine opposite team: current={current_team}, all={all_teams}")
            return "Unknown Team"

        # Return the other team
        for team in all_teams:
            if team != current_team:
                return team

        # Fallback
        logger.warning(f"Could not find opposite team for {current_team}")
        return "Unknown Team"

    @staticmethod
    def _extract_batting_players(table, match_id, innings_key, team_name):
        """
        Extract all batting players from a scorecard table

        Players who batted: Found in td cells with player links
        Players not out: Found in td with class containing "ci-v2-scorecard-player-notout"
        Did not bat: Found in special colspan td with "Did not bat" section

        Args:
            table: BeautifulSoup table element
            match_id: Match identifier
            innings_key: Innings identifier (e.g., "innings_1")
            team_name: Team name

        Returns:
            List of player dictionaries
        """
        players = []
        batting_position = 1

        try:
            # Extract players who batted (both out and not out)
            # Look for td cells with class "ds-w-0 ds-whitespace-nowrap ds-min-w-max"
            batsman_cells = table.find_all("td", class_=lambda
                x: x and "ds-w-0" in x and "ds-whitespace-nowrap" in x and "ds-min-w-max" in x)

            logger.debug(f"Found {len(batsman_cells)} batsman cells in {innings_key}")

            for cell in batsman_cells:
                # Skip if this is a "did not bat" section (has colspan)
                if cell.get("colspan"):
                    continue

                # Look for player link (a tag with /cricketers/ in href)
                player_link = cell.find("a", href=lambda x: x and "/cricketers/" in x)

                if player_link:
                    # Get player name from title attribute (cleanest method)
                    player_name = player_link.get("title", "").strip()

                    # Fallback: extract from span if title not available
                    if not player_name:
                        player_span = player_link.find("span",
                                                       class_=lambda x: x and "ds-text-table-link" in x)
                        if player_span:
                            inner_span = player_span.find("span")
                            if inner_span:
                                player_name = inner_span.get_text(strip=True)

                    # Clean the player name
                    player_name = MetadataExtractor._clean_player_name(player_name)

                    # Check if player is not out
                    cell_classes = cell.get("class", [])
                    is_not_out = "ci-v2-scorecard-player-notout" in " ".join(cell_classes)

                    # Check for impact player icon
                    is_impact = MetadataExtractor._is_impact_player(cell)

                    # Check for retired/injured icon
                    is_retired = bool(cell.find("i", class_=lambda x: x and "icon-arrow_forward-filled" in x))

                    if player_name:
                        players.append({
                            'matchid': int(match_id),
                            'innings': str(innings_key),
                            'team': str(team_name),
                            'player_name': str(player_name),
                            'batted': True,
                            'batting_position': int(batting_position),
                            'player_type': 'impact' if is_impact else 'regular',
                            'retired': is_retired,
                            'not_out': is_not_out
                        })

                        if is_impact:
                            logger.debug(f"  Impact player (batted): {player_name}")
                        if is_retired:
                            logger.debug(f"  Retired/Injured: {player_name}")
                        if is_not_out:
                            logger.debug(f"  Not out: {player_name}")

                        batting_position += 1

            # Extract players who did not bat
            # Look for td with colspan and "Did not bat" section
            dnb_cells = table.find_all("td", class_="!ds-py-2", colspan=True)

            logger.debug(f"Found {len(dnb_cells)} potential 'did not bat' sections in {innings_key}")

            for dnb_cell in dnb_cells:
                # Check if this is actually a "Did not bat" section
                dnb_header = dnb_cell.find("span", class_=lambda x: x and "ds-text-overline-2" in x)

                if dnb_header and "did not bat" in dnb_header.get_text(strip=True).lower():
                    # Find all player links in this section
                    player_links = dnb_cell.find_all("a", href=lambda x: x and "/cricketers/" in x)

                    logger.debug(f"Found {len(player_links)} players who did not bat in {innings_key}")

                    for player_link in player_links:
                        # Get player name from title attribute
                        player_name = player_link.get("title", "").strip()

                        # Fallback: extract from span
                        if not player_name:
                            player_span = player_link.find("span",
                                                           class_=lambda x: x and "ds-text-body-3" in x)
                            if player_span:
                                inner_span = player_span.find("span")
                                if inner_span:
                                    player_name = inner_span.get_text(strip=True)

                        # Clean the player name
                        player_name = MetadataExtractor._clean_player_name(player_name)

                        # Check for impact player icon in parent structure
                        is_impact = MetadataExtractor._is_impact_player(dnb_cell)

                        if player_name:
                            players.append({
                                'matchid': int(match_id),
                                'innings': str(innings_key),
                                'team': str(team_name),
                                'player_name': str(player_name),
                                'batted': False,
                                'batting_position': None,
                                'player_type': 'impact' if is_impact else 'regular',
                                'retired': False,
                                'not_out': False
                            })

                            if is_impact:
                                logger.debug(f"  Impact player (did not bat): {player_name}")

            logger.debug(f"Extracted {len(players)} total players from {innings_key}")

            # Log summary
            batted_count = sum(1 for p in players if p['batted'])
            not_out_count = sum(1 for p in players if p.get('not_out', False))
            retired_count = sum(1 for p in players if p.get('retired', False))
            dnb_count = sum(1 for p in players if not p['batted'])
            logger.info(
                f"{innings_key} - Batted: {batted_count}, Not out: {not_out_count}, Retired: {retired_count}, Did not bat: {dnb_count}")

        except Exception as e:
            logger.warning(f"Error extracting batting players: {e}", exc_info=True)

        return players

    @staticmethod
    def _extract_bowlers(table, match_id, innings_key, team_name):
        """
        Extract bowlers from bowling table with enhanced detection

        Args:
            table: BeautifulSoup table element
            match_id: Match identifier
            innings_key: Innings identifier
            team_name: Bowling team name (OPPOSITE of batting team)

        Returns:
            List of bowler dictionaries
        """
        bowlers = []
        seen_names = set()  # Track seen names for faster duplicate checking

        try:
            # Method 1: Find by td cells
            bowler_cells = table.find_all("td", class_="ds-w-0 ds-whitespace-nowrap ds-min-w-max")

            # Method 2: Also try finding by span directly (fallback)
            if not bowler_cells:
                logger.warning(f"No bowler cells found by td, trying span method for {innings_key}")
                bowler_spans = table.find_all("span",
                                              class_=lambda
                                                  x: x and "ds-text-table-link" in x and "ds-font-semibold" in x)

                for span in bowler_spans:
                    player_name = span.get_text(strip=True)
                    player_name = MetadataExtractor._clean_player_name(player_name)

                    if player_name and player_name not in ['Bowler', 'BOWLER'] and player_name not in seen_names:
                        parent_td = span.find_parent("td")
                        is_impact = MetadataExtractor._is_impact_player(parent_td) if parent_td else False

                        bowlers.append({
                            'matchid': int(match_id),
                            'innings': str(innings_key),
                            'team': str(team_name),
                            'player_name': str(player_name),
                            'batted': False,
                            'batting_position': None,
                            'player_type': 'impact' if is_impact else 'regular',
                            'bowled': True
                        })
                        seen_names.add(player_name)
            else:
                # Process cells with player links
                logger.debug(f"Found {len(bowler_cells)} bowler cells in {innings_key}")

                for cell in bowler_cells:
                    player_link = cell.find("a", href=lambda x: x and "/cricketers/" in x)

                    if player_link:
                        # Try title first
                        player_name = player_link.get("title", "").strip()

                        # Fallback to span text
                        if not player_name:
                            player_span = player_link.find("span",
                                                           class_=lambda x: x and "ds-text-table-link" in x)
                            if player_span:
                                # Try inner span first
                                inner_span = player_span.find("span")
                                player_name = inner_span.get_text(strip=True) if inner_span else player_span.get_text(
                                    strip=True)

                        # Clean name
                        player_name = MetadataExtractor._clean_player_name(player_name)

                        # Check for impact player
                        is_impact = MetadataExtractor._is_impact_player(cell)

                        # Validate and add
                        if player_name and player_name not in ['Bowler', 'BOWLER',
                                                               'bowler'] and player_name not in seen_names:
                            bowlers.append({
                                'matchid': int(match_id),
                                'innings': str(innings_key),
                                'team': str(team_name),
                                'player_name': str(player_name),
                                'batted': False,
                                'batting_position': None,
                                'player_type': 'impact' if is_impact else 'regular',
                                'bowled': True
                            })

                            seen_names.add(player_name)

                            if is_impact:
                                logger.debug(f"  Impact player (bowler): {player_name}")

            logger.info(f"Extracted {len(bowlers)} unique bowlers from {innings_key}")

            if bowlers:
                logger.debug(f"Bowlers: {', '.join([b['player_name'] for b in bowlers])}")

        except Exception as e:
            logger.warning(f"Error extracting bowlers: {e}", exc_info=True)

        return bowlers

    @staticmethod
    def _is_impact_player(td_element):
        """
        Check if a td element contains an impact player icon

        Looks for <i> tag with classes:
        - "icon-arrow_back-filled ds-text-icon ds-text-icon-success-hover ds-ml-0.5 ds-cursor-pointer"

        Args:
            td_element: BeautifulSoup td element

        Returns:
            bool: True if impact player icon found, False otherwise
        """
        if not td_element:
            return False

        try:
            # Look for i tag with impact player icon classes
            icon = td_element.find("i", class_="icon-arrow_back-filled")

            if icon:
                # Verify it has the full class set
                icon_classes = icon.get("class", [])
                required_classes = ["icon-arrow_back-filled", "ds-text-icon", "ds-text-icon-success-hover"]

                if all(cls in icon_classes for cls in required_classes):
                    logger.debug(f"Found impact player icon in td")
                    return True

        except Exception as e:
            logger.debug(f"Error checking for impact player icon: {e}")

        return False

    @staticmethod
    def _extract_team_name_from_table(table, innings_number):
        """
        Extract team name from batting scorecard table

        Looks for:
        - Div: ds-flex ds-flex-col ds-grow ds-justify-center
        - Span: ds-text-title-xs ds-font-bold ds-capitalize
        """
        print("finding team name")
        try:
            # Strategy 1: Look in parent hierarchy
            current = table

            for level in range(10):
                parent = current.find_parent()
                if not parent:
                    break

                team_div = parent.find("div", class_="ds-bg-color-primary-bg ds-p-3")

                if team_div:
                    team_span = team_div.find("span", class_="ds-text-title-1 ds-font-semibold ds-capitalize ds-text-color-text")

                    if team_span:
                        team_name = team_span.get_text(strip=True)
                        team_name = MetadataExtractor._clean_team_name(team_name)

                        if team_name:
                            logger.debug(f"Found team name at level {level}: '{team_name}'")
                            return team_name

                current = parent

            # Strategy 2: Search for span directly
            team_span = table.find_previous("span", class_="ds-text-title-1 ds-font-semibold ds-capitalize ds-text-color-text")

            if team_span:
                team_name = team_span.get_text(strip=True)
                team_name = MetadataExtractor._clean_team_name(team_name)

                if team_name:
                    logger.debug(f"Found team name in previous span: '{team_name}'")
                    return team_name

            # Strategy 3: Search for div, then span
            team_div = table.find_previous("div", class_="ds-bg-color-primary-bg ds-p-3")

            if team_div:
                team_span = team_div.find("span", class_="ds-text-title-1 ds-font-semibold ds-capitalize ds-text-color-text")

                if team_span:
                    team_name = team_span.get_text(strip=True)
                    team_name = MetadataExtractor._clean_team_name(team_name)

                    if team_name:
                        logger.debug(f"Found team name in previous div+span: '{team_name}'")
                        return team_name

        except Exception as e:
            logger.warning(f"Error extracting team name: {e}")

        logger.warning(f"Could not find team name for innings {innings_number}, using default")
        return f"Team {innings_number}"

    @staticmethod
    def _clean_team_name(team_name):
        """Clean team name by removing innings text and extra whitespace"""
        if not team_name:
            return None

        team_name = re.sub(r'\s*Innings\s*$', '', team_name, flags=re.IGNORECASE)
        team_name = re.sub(r'^\d+(st|nd|rd|th)?\s+Innings\s*', '', team_name, flags=re.IGNORECASE)
        team_name = re.sub(r'\s+', ' ', team_name)

        cleaned = team_name.strip()
        return cleaned if cleaned and len(cleaned) > 2 else None

    @staticmethod
    def _clean_player_name(name):
        """
        Clean player name

        FIXED: Removes trailing commas and other punctuation
        """
        if not name:
            return None

        # Remove captain and wicketkeeper symbols
        name = re.sub(r'[†*]', '', name)
        name = re.sub(r'\(c\)|\(wk\)', '', name, flags=re.IGNORECASE)

        # FIXED: Remove trailing commas and other punctuation
        name = re.sub(r'[,;]+$', '', name)  # Remove trailing commas/semicolons
        name = re.sub(r'^[,;]+', '', name)  # Remove leading commas/semicolons

        # Remove extra whitespace
        name = ' '.join(name.split())

        cleaned = name.strip()

        # Filter out header/label text
        if cleaned.lower() in ['batter', 'batsman', 'batters', 'name', 'bowler']:
            return None

        return cleaned if cleaned and len(cleaned) > 1 else None