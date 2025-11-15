"""
ETL Pipeline: Bronze to Silver - Match Metadata
Transforms raw match metadata into cleaned and normalized silver layer tables.
"""

import re
import json
from datetime import datetime
import pandas as pd
from configs.db_config import get_connection


def parse_toss(toss_string):
    """
    Parse toss string to extract winner and decision.
    Example: "Mumbai Indians, who chose to field" -> ('Mumbai Indians', 'field')
    """
    if not toss_string or pd.isna(toss_string):
        return None, None

    # Pattern: "Team Name, who chose to bat/field"
    match = re.search(r"^(.*?),\s*who chose to (bat|field)", toss_string, re.IGNORECASE)
    if match:
        return match.group(1).strip(), match.group(2).lower()

    return None, None


def parse_umpires(umpires_string):
    """
    Parse umpires string to extract individual umpire names.
    Example: "Nitin Menon, Chris Gaffaney" -> ['Nitin Menon', 'Chris Gaffaney']
    """
    if not umpires_string or pd.isna(umpires_string):
        return None, None

    umpires = [u.strip() for u in umpires_string.split(',')]
    umpire_1 = umpires[0] if len(umpires) > 0 else None
    umpire_2 = umpires[1] if len(umpires) > 1 else None

    return umpire_1, umpire_2


def parse_match_date(match_days_string):
    """
    Parse match date from match_days string.
    Example: "February 14, 2025" -> date(2025, 2, 14)
    """
    if not match_days_string or pd.isna(match_days_string):
        return None

    # Try various date formats
    date_formats = [
        "%B %d, %Y",  # February 14, 2025
        "%b %d, %Y",  # Feb 14, 2025
        "%d %B %Y",   # 14 February 2025
        "%Y-%m-%d",   # 2025-02-14
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(match_days_string.strip(), fmt).date()
        except ValueError:
            continue

    return None


def parse_t20_debuts(t20_debut_string):
    """
    Parse T20 debut string into array of player names.
    Example: "Player1 (Team1), Player2 (Team2)" -> ['Player1 (Team1)', 'Player2 (Team2)']
    """
    if not t20_debut_string or pd.isna(t20_debut_string):
        return None

    # Split by comma and strip whitespace
    debuts = [d.strip() for d in t20_debut_string.split(',')]
    return debuts if debuts else None


def parse_player_replacements(replacement_string):
    """
    Parse player replacement JSON/string into structured data.
    Returns list of dictionaries with replacement info.
    """
    if not replacement_string or pd.isna(replacement_string):
        return []

    try:
        # If it's already JSON, parse it
        if replacement_string.startswith('{') or replacement_string.startswith('['):
            data = json.loads(replacement_string)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
    except json.JSONDecodeError:
        pass

    # Try to parse manually
    # Example format: "Player1 replaced by Player2 (Team)"
    replacements = []
    # This is a simplified parser - adjust based on actual data format
    # For now, return empty list if not JSON
    return replacements


def transform_metadata():
    """
    Transform raw match metadata from Bronze to Silver layer.
    """
    print("\n" + "="*80)
    print("ETL: Bronze -> Silver (Match Metadata)")
    print("="*80)

    conn = get_connection()

    # Read raw metadata from Bronze layer
    print("\n[1/4] Reading data from Bronze layer (raw_match_metadata)...")
    bronze_df = pd.read_sql(
        "SELECT * FROM raw_match_metadata",
        conn
    )

    if bronze_df.empty:
        print("⚠️  No data found in Bronze layer. Please run the scraper first.")
        conn.close()
        return

    print(f"✅ Found {len(bronze_df)} records in Bronze layer")

    # Transform data
    print("\n[2/4] Transforming and cleaning data...")

    silver_metadata = []
    silver_replacements = []

    for _, row in bronze_df.iterrows():
        match_id = row['matchid']

        # Parse toss
        toss_winner, toss_decision = parse_toss(row.get('toss'))

        # Parse umpires
        umpire_1, umpire_2 = parse_umpires(row.get('umpires'))

        # Parse date
        match_date = parse_match_date(row.get('match_days'))

        # Parse T20 debuts
        t20_debuts = parse_t20_debuts(row.get('t20_debut'))

        # Create silver metadata record
        metadata_record = {
            'match_id': match_id,
            'venue': row.get('venue'),
            'series': row.get('series'),
            'season': row.get('season'),
            'match_date': match_date,
            'toss_winner': toss_winner,
            'toss_decision': toss_decision,
            'umpire_1': umpire_1,
            'umpire_2': umpire_2,
            'tv_umpire': row.get('tv_umpire'),
            'reserve_umpire': row.get('reserve_umpire'),
            'match_referee': row.get('match_referee'),
            'player_of_the_match': row.get('player_of_the_match'),
            'first_innings_team': row.get('first_innings'),
            'second_innings_team': row.get('second_innings'),
            't20_debuts': t20_debuts,
            'hours_of_play_local_time': row.get('hours_of_play_local_time'),
            'points': row.get('points'),
        }

        silver_metadata.append(metadata_record)

        # Parse and store player replacements
        replacements = parse_player_replacements(row.get('player_replacements'))
        for repl in replacements:
            replacement_record = {
                'match_id': match_id,
                'player_out': repl.get('out'),
                'player_in': repl.get('in'),
                'team': repl.get('team'),
                'replacement_type': repl.get('type', 'unknown'),
            }
            silver_replacements.append(replacement_record)

    silver_metadata_df = pd.DataFrame(silver_metadata)
    print(f"✅ Transformed {len(silver_metadata_df)} metadata records")

    # Load data to Silver layer
    print("\n[3/4] Loading data to Silver layer...")

    cur = conn.cursor()

    # Insert metadata
    for _, row in silver_metadata_df.iterrows():
        cur.execute("""
            INSERT INTO silver_match_metadata (
                match_id, venue, series, season, match_date,
                toss_winner, toss_decision, umpire_1, umpire_2,
                tv_umpire, reserve_umpire, match_referee,
                player_of_the_match, first_innings_team, second_innings_team,
                t20_debuts, hours_of_play_local_time, points
            )
            VALUES (
                %(match_id)s, %(venue)s, %(series)s, %(season)s, %(match_date)s,
                %(toss_winner)s, %(toss_decision)s, %(umpire_1)s, %(umpire_2)s,
                %(tv_umpire)s, %(reserve_umpire)s, %(match_referee)s,
                %(player_of_the_match)s, %(first_innings_team)s, %(second_innings_team)s,
                %(t20_debuts)s, %(hours_of_play_local_time)s, %(points)s
            )
            ON CONFLICT (match_id) DO UPDATE SET
                venue = EXCLUDED.venue,
                series = EXCLUDED.series,
                season = EXCLUDED.season,
                match_date = EXCLUDED.match_date,
                toss_winner = EXCLUDED.toss_winner,
                toss_decision = EXCLUDED.toss_decision,
                umpire_1 = EXCLUDED.umpire_1,
                umpire_2 = EXCLUDED.umpire_2,
                tv_umpire = EXCLUDED.tv_umpire,
                reserve_umpire = EXCLUDED.reserve_umpire,
                match_referee = EXCLUDED.match_referee,
                player_of_the_match = EXCLUDED.player_of_the_match,
                first_innings_team = EXCLUDED.first_innings_team,
                second_innings_team = EXCLUDED.second_innings_team,
                t20_debuts = EXCLUDED.t20_debuts,
                hours_of_play_local_time = EXCLUDED.hours_of_play_local_time,
                points = EXCLUDED.points,
                updated_at = CURRENT_TIMESTAMP
        """, row.to_dict())

    conn.commit()
    print(f"✅ Loaded {len(silver_metadata_df)} records to silver_match_metadata")

    # Insert player replacements
    if silver_replacements:
        silver_replacements_df = pd.DataFrame(silver_replacements)
        for _, row in silver_replacements_df.iterrows():
            cur.execute("""
                INSERT INTO silver_player_replacements (
                    match_id, player_out, player_in, team, replacement_type
                )
                VALUES (
                    %(match_id)s, %(player_out)s, %(player_in)s, %(team)s, %(replacement_type)s
                )
                ON CONFLICT DO NOTHING
            """, row.to_dict())

        conn.commit()
        print(f"✅ Loaded {len(silver_replacements_df)} player replacements")

    # Verify results
    print("\n[4/4] Verifying transformation...")
    cur.execute("SELECT COUNT(*) FROM silver_match_metadata")
    count = cur.fetchone()[0]
    print(f"✅ Total records in silver_match_metadata: {count}")

    cur.close()
    conn.close()

    print("\n" + "="*80)
    print("ETL Complete: Bronze -> Silver (Match Metadata)")
    print("="*80 + "\n")


if __name__ == "__main__":
    transform_metadata()
