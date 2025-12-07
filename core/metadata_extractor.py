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
            match_details_table = soup.find("table", class_="ds-w-full ds-table ds-table-sm ds-table-auto")

            if not match_details_table:
                logger.warning(f"Match details table not found for match {match_id}")
                metadata["MatchID"] = match_id
                return metadata

            # Parse all rows in the table
            rows = match_details_table.find_all("tr")
            logger.info(f"Found {len(rows)} metadata rows for match {match_id}")

            for row in rows:
                columns = row.find_all("td")

                if len(columns) == 2:
                    # Key-value pair (e.g., "Toss" - "CSK won the toss")
                    key = columns[0].get_text(strip=True)
                    value = columns[1].get_text(separator=" ", strip=True)
                    metadata[key] = value

                elif len(columns) == 1:
                    # Single column (usually venue)
                    venue_text = columns[0].get_text(strip=True)
                    if venue_text:
                        metadata["Venue"] = venue_text

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
        Includes regular players, impact players, and substitutes

        Returns:
            pandas DataFrame with columns:
            - matchid, innings, team, player_name, batted, batting_position,
            - player_type (regular, impact, substitute)
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            rows = []

            logger.info(f"Extracting player names for match {match_id}")

            # Find all scorecard tables
            scorecard_tables = soup.find_all("table",
                                             class_="ds-w-full ds-table ds-table-md ds-table-auto ci-scorecard-table")

            if not scorecard_tables:
                logger.warning(f"No scorecard tables found for match {match_id}")
                return pd.DataFrame(
                    columns=['matchid', 'innings', 'team', 'player_name', 'batted', 'batting_position', 'player_type'])

            logger.info(f"Found {len(scorecard_tables)} scorecard table(s)")

            # Process each table (each table = one innings)
            for idx, table in enumerate(scorecard_tables, start=1):
                innings_key = f"innings_{idx}"

                # Extract team name
                team_name = MetadataExtractor._extract_team_name_from_table(table, idx)

                # Extract regular players from this table
                table_players = MetadataExtractor._extract_all_players_from_table(table, match_id, innings_key,
                                                                                  team_name)

                rows.extend(table_players)

                batted_count = sum(1 for p in table_players if p['batted'])
                did_not_bat_count = len(table_players) - batted_count

                logger.info(f"  {innings_key} ({team_name}): {batted_count} batted, {did_not_bat_count} did not bat")

            # Extract impact players and substitutes
            impact_players = MetadataExtractor._extract_impact_players(soup, match_id)
            rows.extend(impact_players)


            if impact_players:
                logger.info(f"  Found {len(impact_players)} impact players/substitutes")

            # Create DataFrame
            df = pd.DataFrame(rows)
            df = df.replace(np.nan, None)

            # Define priority order
            type_priority = {'impact': 1, 'substitute': 2, 'regular': 3}
            df['priority'] = df['player_type'].map(type_priority)

            # Sort by priority (lower number = higher priority)
            df = df.sort_values('priority')

            # Keep first occurrence (which is highest priority due to sorting)
            df_unique = df.drop_duplicates(subset=['matchid', 'player_name'], keep='first')

            # Remove temporary priority column
            df_unique = df_unique.drop('priority', axis=1)

            logger.info(f"Created DataFrame with {len(df)} player records from {len(scorecard_tables)} innings")

            return df_unique

        except Exception as e:
            logger.error(f"Error extracting player names for match {match_id}: {e}", exc_info=True)
            return pd.DataFrame(
                columns=['matchid', 'innings', 'team', 'player_name', 'batted', 'batting_position', 'player_type'])


        except Exception as e:
            logger.error(f"Error extracting player names for match {match_id}: {e}", exc_info=True)
            return pd.DataFrame(columns=['matchid', 'innings', 'team', 'player_name', 'batted', 'batting_position'])

    @staticmethod
    def _extract_impact_players(soup, match_id):
        """
        Extract impact players from li elements

        Two formats:
        1. "Team Impact Player Subs: Player1, Player2, Player3" (plural - list of subs)
        2. "Team Impact Player Sub: PlayerName in for..." (singular - player actually used)

        Both are marked with is_impact_player = True

        Args:
            soup: BeautifulSoup object
            match_id: Match identifier

        Returns:
            List of player dictionaries
        """
        players = []

        try:
            # Find all li elements with the specific class
            impact_li_elements = soup.find_all("li", class_="ds-text-tight-s ds-font-regular ds-text-typo ds-py-1")

            logger.debug(f"Found {len(impact_li_elements)} li elements")

            for li in impact_li_elements:
                li_text = li.get_text(strip=True)

                # Check if this contains impact player information
                if 'impact player' in li_text.lower():
                    logger.debug(f"Processing impact player text: {li_text}")

                    # Parse the text (handles both formats)
                    parsed_players = MetadataExtractor._parse_impact_player_text(li_text, match_id)
                    players.extend(parsed_players)


            logger.debug(f"Extracted {len(players)} impact players total")

        except Exception as e:
            logger.warning(f"Error extracting impact players: {e}")

        return players

    @staticmethod
    def _parse_impact_player_text(text, match_id):
        """
        Parse impact player text - handles both formats

        Format 1 (Multiple subs):
        "Kolkata Knight Riders Impact Player Subs: Manish Pandey, Luvnith Sisodia,
         Anukul Roy, Anrich Nortje and Vaibhav Arora"

        Format 2 (Single player used):
        "Royal Challengers Bengaluru Impact Player Sub: Devdutt Padikkal in for
         Suyash Sharma (Kolkata Knight Riders innings, 15.6 ov)"

        Args:
            text: Text containing impact player information
            match_id: Match identifier

        Returns:
            List of player dictionaries
        """
        players = []


        try:
            # Check if this is Format 2 (single player with "in for")
            if ' in for ' in text.lower():
                # Format 2: "Team Impact Player Sub: PlayerName in for..."
                # Team name is before "Impact Player Sub:"
                # Player name is between "Impact Player Sub:" and " in"

                team_match = re.search(r'^(.+?)\s+Impact Player Sub:', text, re.IGNORECASE)
                player_in_name  = re.search(r'Impact Player Sub:\s*(.+?)\s+in\s+for', text, re.IGNORECASE)
                player_out_match = re.search(r'in\s+for\s+(.+?)\s*\(', text, re.IGNORECASE)

                if team_match and player_in_name :
                    team_name = team_match.group(1).strip()
                    player_in_name  = player_in_name .group(1).strip()

                    # Clean names
                    team_name = MetadataExtractor._clean_team_name(team_name)
                    if not team_name:
                        team_name = "Unknown Team"

                    player_in_name  = MetadataExtractor._clean_player_name(player_in_name )


                    if player_in_name :
                        players.append({
                            'matchid': int(match_id),
                            'innings': None,
                            'team': str(team_name),
                            'player_name': str(player_in_name ),
                            'batted': False,
                            'batting_position': None,
                            'player_type': 'impact'
                        })

                        logger.debug(f"Parsed impact player (used): {player_in_name } for {team_name}")

                        # Add the player being replaced (if captured)
                        if player_out_match:
                            player_out_name = player_out_match.group(1).strip()
                            player_out_name = MetadataExtractor._clean_player_name(player_out_name)

                            if player_out_name:
                                players.append({
                                    'matchid': int(match_id),
                                    'innings': None,
                                    'team': str(team_name),
                                    'player_name': str(player_out_name),
                                    'batted': False,
                                    'batting_position': None,
                                    'player_type': 'regular'
                                })

                                logger.debug(f"Parsed replaced player (out): {player_out_name} for {team_name}")
            else:
                # Format 1: "Team Impact Player Subs: Player1, Player2, Player3..."
                match = re.search(r'(.+?)\s+Impact Player Subs?:\s*(.+)', text, re.IGNORECASE)

                if match:
                    team_name = match.group(1).strip()
                    players_text = match.group(2).strip()

                    # Clean team name
                    team_name = MetadataExtractor._clean_team_name(team_name)
                    if not team_name:
                        team_name = "Unknown Team"

                    # Split players by comma
                    player_names = [name.strip() for name in players_text.split(',')]

                    # Handle "and" in last player
                    if player_names:
                        player_names[-1] = player_names[-1].replace(' and ', ', ')
                        if ', ' in player_names[-1]:
                            last_players = [p.strip() for p in player_names[-1].split(',')]
                            player_names = player_names[:-1] + last_players

                    # Create player records
                    for player_name in player_names:
                        clean_name = MetadataExtractor._clean_player_name(player_name)

                        if clean_name:
                            players.append({
                                'matchid': int(match_id),
                                'innings': None,
                                'team': str(team_name),
                                'player_name': str(clean_name),
                                'batted': False,
                                'batting_position': None,
                                'player_type': 'substitute'
                            })

                    logger.debug(f"Parsed {len(players)} impact player subs for {team_name}")

        except Exception as e:
            logger.warning(f"Error parsing impact player text '{text}': {e}")

        return players

    @staticmethod
    def _extract_team_name_from_table(table, innings_number):
        """
        Extract team name from:
        - Div: ds-flex ds-flex-col ds-grow ds-justify-center
        - Span: ds-text-title-xs ds-font-bold ds-capitalize
        """
        try:
            # Strategy 1: Look in parent hierarchy
            current = table

            for level in range(10):
                parent = current.find_parent()
                if not parent:
                    break

                team_div = parent.find("div", class_="ds-flex ds-flex-col ds-grow ds-justify-center")

                if team_div:
                    team_span = team_div.find("span", class_="ds-text-title-xs ds-font-bold ds-capitalize")

                    if team_span:
                        team_name = team_span.get_text(strip=True)
                        team_name = MetadataExtractor._clean_team_name(team_name)

                        if team_name:
                            logger.debug(f"Found team name at level {level}: '{team_name}'")
                            return team_name

                current = parent

            # Strategy 2: Search for span directly
            team_span = table.find_previous("span", class_="ds-text-title-xs ds-font-bold ds-capitalize")

            if team_span:
                team_name = team_span.get_text(strip=True)
                team_name = MetadataExtractor._clean_team_name(team_name)

                if team_name:
                    logger.debug(f"Found team name in previous span: '{team_name}'")
                    return team_name

            # Strategy 3: Search for div, then span
            team_div = table.find_previous("div", class_="ds-flex ds-flex-col ds-grow ds-justify-center")

            if team_div:
                team_span = team_div.find("span", class_="ds-text-title-xs ds-font-bold ds-capitalize")

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
    def _extract_all_players_from_table(table, match_id, innings_key, team_name):
        """
        Extract all players from a single scorecard table

        Players who batted: td class="ds-w-0 ds-whitespace-nowrap ds-min-w-max"
        Did not bat: div class="ds-text-tight-m ds-font-regular ds-leading-4 ds-text-typo-mid1"
        """
        import numpy as np

        players = []
        batting_position = 1

        try:
            # Extract players who batted
            batsman_cells = table.find_all("td", class_="ds-w-0 ds-whitespace-nowrap ds-min-w-max")
            not_out_batsman_cells = table.find_all("td",
                                                   class_="ds-w-0 ds-whitespace-nowrap ds-min-w-max ds-border-line-primary ci-scorecard-player-notout")
            # Extend batsman_cells with the elements of not_out_batsman_cells
            batsman_cells.extend(not_out_batsman_cells)
            logger.debug(f"Found {len(batsman_cells)} batsman cells in {innings_key}")

            for cell in batsman_cells:
                player_name = MetadataExtractor._extract_batsman_from_cell(cell)

                if player_name:
                    players.append({
                        'matchid': int(match_id),
                        'innings': str(innings_key),
                        'team': str(team_name),
                        'player_name': str(player_name),
                        'batted': True,
                        'batting_position': int(batting_position),
                        'player_type': 'regular'  # NEW: Mark as regular player
                    })
                    batting_position += 1

            # Extract players who did not bat
            dnb_divs = table.find_all("div", class_="ds-text-tight-m ds-font-regular ds-leading-4 ds-text-typo-mid1")

            logger.debug(f"Found {len(dnb_divs)} did not bat divs in {innings_key}")

            for div in dnb_divs:
                div_text = div.get_text(strip=True).lower()

                if "did not bat" in div_text:
                    dnb_players = MetadataExtractor._extract_did_not_bat_from_div(div)

                    for player_name in dnb_players:
                        players.append({
                            'matchid': int(match_id),
                            'innings': str(innings_key),
                            'team': str(team_name),
                            'player_name': str(player_name),
                            'batted': False,
                            'batting_position': None,
                            'player_type': 'regular'  # NEW: Mark as regular player
                        })

            logger.debug(f"Extracted {len(players)} total players from {innings_key}")

        except Exception as e:
            logger.warning(f"Error extracting players from table: {e}")

        return players

    @staticmethod
    def _extract_batsman_from_cell(cell):
        """Extract batsman name from td with class: ds-w-0 ds-whitespace-nowrap ds-min-w-max"""
        try:
            player_link = cell.find("a")

            if player_link:
                player_name = player_link.get_text(strip=True)
            else:
                player_span = cell.find("span")
                if player_span:
                    player_name = player_span.get_text(strip=True)
                else:
                    player_name = cell.get_text(strip=True)

            player_name = MetadataExtractor._clean_player_name(player_name)

            if not player_name or player_name.lower() in ['batter', 'batsman', 'batters', 'name']:
                return None

            return player_name

        except Exception as e:
            logger.debug(f"Error extracting batsman from cell: {e}")
            return None

    @staticmethod
    def _extract_did_not_bat_from_div(div):
        """Extract player names from div: ds-text-tight-m ds-font-regular ds-leading-4 ds-text-typo-mid1"""
        players = []

        try:
            div_text = div.get_text(separator=" ", strip=True)

            match = re.search(r'Did not bat[:\s]*(.+)', div_text, re.IGNORECASE)

            if match:
                players_text = match.group(1).strip()
                player_names = [name.strip() for name in players_text.split(',')]

                for name in player_names:
                    clean_name = MetadataExtractor._clean_player_name(name)
                    if clean_name:
                        players.append(clean_name)

                logger.debug(f"Extracted {len(players)} did not bat players: {players}")

        except Exception as e:
            logger.debug(f"Error extracting did not bat players: {e}")

        return players

    @staticmethod
    def _clean_player_name(name):
        """Clean player name"""
        if not name:
            return None

        name = re.sub(r'[†*]', '', name)
        name = re.sub(r'\(c\)|\(wk\)', '', name, flags=re.IGNORECASE)
        name = ' '.join(name.split())

        cleaned = name.strip()
        return cleaned if cleaned and len(cleaned) > 1 else None