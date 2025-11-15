"""
ETL Pipeline: Bronze to Silver - Match Events
Transforms raw ball-by-ball commentary into cleaned and enriched silver layer events.
"""

import re
import pandas as pd
from configs.db_config import get_connection


def parse_ball_notation(ball_string):
    """
    Parse ball notation into over number, ball number, and ball in over.
    Example: "0.1" -> (0, 1, 1), "5.6" -> (5, 6, 6)
    """
    if not ball_string or pd.isna(ball_string):
        return None, None, None

    try:
        parts = ball_string.split('.')
        over_number = int(parts[0])
        ball_in_over = int(parts[1])
        # Calculate absolute ball number (6 balls per over)
        ball_number = (over_number * 6) + ball_in_over
        return over_number, ball_number, ball_in_over
    except (ValueError, IndexError):
        return None, None, None


def extract_runs_and_extras(event_string):
    """
    Extract runs scored and extras from event string.
    Returns: (runs_scored, extras, extra_type)

    Examples:
    - "1 run" -> (1, 0, None)
    - "FOUR" -> (4, 0, None)
    - "SIX" -> (6, 0, None)
    - "no run" -> (0, 0, None)
    - "1 wide" -> (0, 1, 'wide')
    - "2 wides" -> (0, 2, 'wide')
    - "no ball, 1 run" -> (1, 1, 'noball')
    """
    if not event_string or pd.isna(event_string):
        return 0, 0, None

    event_lower = event_string.lower()

    # Check for boundaries
    if 'four' in event_lower or event_lower.strip() == '4':
        return 4, 0, None
    if 'six' in event_lower or event_lower.strip() == '6':
        return 6, 0, None

    # Check for extras
    extras = 0
    extra_type = None

    # Wide
    wide_match = re.search(r'(\d+)?\s*wides?', event_lower)
    if wide_match:
        extras = int(wide_match.group(1)) if wide_match.group(1) else 1
        extra_type = 'wide'

    # No ball
    noball_match = re.search(r'no\s*balls?', event_lower)
    if noball_match:
        extras = 1
        extra_type = 'noball'

    # Bye
    bye_match = re.search(r'(\d+)?\s*byes?', event_lower)
    if bye_match:
        extras = int(bye_match.group(1)) if bye_match.group(1) else 1
        extra_type = 'bye'

    # Leg bye
    legbye_match = re.search(r'(\d+)?\s*leg\s*byes?', event_lower)
    if legbye_match:
        extras = int(legbye_match.group(1)) if legbye_match.group(1) else 1
        extra_type = 'legbye'

    # Extract runs scored (not including extras)
    runs = 0
    run_match = re.search(r'(\d+)\s*runs?', event_lower)
    if run_match:
        runs = int(run_match.group(1))
    elif 'no run' in event_lower or 'dot' in event_lower:
        runs = 0

    return runs, extras, extra_type


def extract_wicket_info(event_string):
    """
    Extract wicket information from event string.
    Returns: (is_wicket, wicket_type, fielder)

    Examples:
    - "OUT! Bowled" -> (True, 'bowled', None)
    - "OUT! Caught by Dhoni" -> (True, 'caught', 'Dhoni')
    - "run out (Kohli)" -> (True, 'run out', 'Kohli')
    """
    if not event_string or pd.isna(event_string):
        return False, None, None

    event_lower = event_string.lower()

    if 'out' not in event_lower and 'wicket' not in event_lower:
        return False, None, None

    is_wicket = True
    wicket_type = None
    fielder = None

    # Detect wicket type
    if 'bowled' in event_lower or 'b ' in event_lower:
        wicket_type = 'bowled'
    elif 'caught' in event_lower or 'c ' in event_lower:
        wicket_type = 'caught'
        # Extract fielder name
        fielder_match = re.search(r'caught\s+(?:by\s+)?([A-Z][a-zA-Z\s]+)', event_string, re.IGNORECASE)
        if fielder_match:
            fielder = fielder_match.group(1).strip()
    elif 'lbw' in event_lower:
        wicket_type = 'lbw'
    elif 'stumped' in event_lower or 'st ' in event_lower:
        wicket_type = 'stumped'
        fielder_match = re.search(r'stumped\s+(?:by\s+)?([A-Z][a-zA-Z\s]+)', event_string, re.IGNORECASE)
        if fielder_match:
            fielder = fielder_match.group(1).strip()
    elif 'run out' in event_lower:
        wicket_type = 'run out'
        fielder_match = re.search(r'run\s+out\s*\(([^)]+)\)', event_string, re.IGNORECASE)
        if fielder_match:
            fielder = fielder_match.group(1).strip()
    elif 'hit wicket' in event_lower:
        wicket_type = 'hit wicket'

    return is_wicket, wicket_type, fielder


def extract_cumulative_score(score_string):
    """
    Extract cumulative runs and wickets from score string.
    Example: "45/2" -> (45, 2), "120/5" -> (120, 5)
    """
    if not score_string or pd.isna(score_string):
        return None, None

    # Pattern: "runs/wickets"
    match = re.search(r'(\d+)/(\d+)', score_string)
    if match:
        return int(match.group(1)), int(match.group(2))

    return None, None


def determine_innings_number(innings_team, first_innings_team, second_innings_team):
    """
    Determine if this is innings 1 or 2 based on batting team.
    """
    if innings_team == first_innings_team:
        return 1
    elif innings_team == second_innings_team:
        return 2
    else:
        return None


def transform_events():
    """
    Transform raw match events from Bronze to Silver layer.
    """
    print("\n" + "="*80)
    print("ETL: Bronze -> Silver (Match Events)")
    print("="*80)

    conn = get_connection()

    # Read raw events from Bronze layer
    print("\n[1/5] Reading data from Bronze layer (raw_match_events)...")
    bronze_df = pd.read_sql(
        "SELECT * FROM raw_match_events ORDER BY matchid, ball",
        conn
    )

    if bronze_df.empty:
        print("⚠️  No data found in Bronze layer. Please run the scraper first.")
        conn.close()
        return

    print(f"✅ Found {len(bronze_df)} records in Bronze layer")

    # Get innings team mapping from silver metadata
    print("\n[2/5] Loading metadata for innings mapping...")
    metadata_df = pd.read_sql(
        "SELECT match_id, first_innings_team, second_innings_team FROM silver_match_metadata",
        conn
    )

    if metadata_df.empty:
        print("⚠️  No metadata found in Silver layer. Please run metadata ETL first.")
        conn.close()
        return

    # Create mapping dictionary
    innings_mapping = {}
    for _, row in metadata_df.iterrows():
        innings_mapping[row['match_id']] = {
            'first': row['first_innings_team'],
            'second': row['second_innings_team']
        }

    print(f"✅ Loaded metadata for {len(innings_mapping)} matches")

    # Transform data
    print("\n[3/5] Transforming and enriching events...")

    silver_events = []

    for _, row in bronze_df.iterrows():
        match_id = row['matchid']
        innings_team = row['innings']

        # Parse ball notation
        over_number, ball_number, ball_in_over = parse_ball_notation(row.get('ball'))

        # Extract runs and extras
        runs_scored, extras, extra_type = extract_runs_and_extras(row.get('event', ''))

        # Extract wicket info
        is_wicket, wicket_type, fielder = extract_wicket_info(row.get('event', ''))

        # Get cumulative score
        total_runs, total_wickets = extract_cumulative_score(row.get('score'))

        # Determine innings number
        innings_number = None
        if match_id in innings_mapping:
            if innings_team == innings_mapping[match_id]['first']:
                innings_number = 1
            elif innings_team == innings_mapping[match_id]['second']:
                innings_number = 2

        # Create silver event record
        event_record = {
            'match_id': match_id,
            'over_number': over_number,
            'ball_number': ball_number,
            'ball_in_over': ball_in_over,
            'ball_notation': row.get('ball'),
            'bowler': row.get('bowler'),
            'batsman': row.get('batsman'),
            'non_striker': None,  # Not available in current data
            'runs_scored': runs_scored,
            'extras': extras,
            'extra_type': extra_type,
            'is_wicket': is_wicket,
            'wicket_type': wicket_type,
            'fielder': fielder,
            'innings': innings_team,
            'innings_number': innings_number,
            'total_runs': total_runs,
            'total_wickets': total_wickets,
            'raw_event': row.get('event'),
            'commentary': row.get('commentary'),
        }

        silver_events.append(event_record)

    silver_events_df = pd.DataFrame(silver_events)
    print(f"✅ Transformed {len(silver_events_df)} event records")

    # Load data to Silver layer
    print("\n[4/5] Loading data to Silver layer...")

    # Delete existing events for these matches (for idempotency)
    match_ids = tuple(bronze_df['matchid'].unique())
    cur = conn.cursor()

    if len(match_ids) == 1:
        cur.execute(f"DELETE FROM silver_match_events WHERE match_id = {match_ids[0]}")
    else:
        cur.execute(f"DELETE FROM silver_match_events WHERE match_id IN {match_ids}")

    conn.commit()

    # Insert events
    for _, row in silver_events_df.iterrows():
        cur.execute("""
            INSERT INTO silver_match_events (
                match_id, over_number, ball_number, ball_in_over, ball_notation,
                bowler, batsman, non_striker, runs_scored, extras, extra_type,
                is_wicket, wicket_type, fielder, innings, innings_number,
                total_runs, total_wickets, raw_event, commentary
            )
            VALUES (
                %(match_id)s, %(over_number)s, %(ball_number)s, %(ball_in_over)s,
                %(ball_notation)s, %(bowler)s, %(batsman)s, %(non_striker)s,
                %(runs_scored)s, %(extras)s, %(extra_type)s, %(is_wicket)s,
                %(wicket_type)s, %(fielder)s, %(innings)s, %(innings_number)s,
                %(total_runs)s, %(total_wickets)s, %(raw_event)s, %(commentary)s
            )
        """, row.to_dict())

    conn.commit()
    print(f"✅ Loaded {len(silver_events_df)} records to silver_match_events")

    # Verify results
    print("\n[5/5] Verifying transformation...")
    cur.execute("SELECT COUNT(*) FROM silver_match_events")
    count = cur.fetchone()[0]
    print(f"✅ Total records in silver_match_events: {count}")

    # Show sample statistics
    cur.execute("""
        SELECT
            COUNT(*) as total_balls,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as total_wickets,
            SUM(runs_scored) as total_runs,
            COUNT(DISTINCT match_id) as total_matches
        FROM silver_match_events
    """)
    stats = cur.fetchone()
    print(f"📊 Statistics:")
    print(f"   - Total Balls: {stats[0]}")
    print(f"   - Total Wickets: {stats[1]}")
    print(f"   - Total Runs: {stats[2]}")
    print(f"   - Total Matches: {stats[3]}")

    cur.close()
    conn.close()

    print("\n" + "="*80)
    print("ETL Complete: Bronze -> Silver (Match Events)")
    print("="*80 + "\n")


if __name__ == "__main__":
    transform_events()
