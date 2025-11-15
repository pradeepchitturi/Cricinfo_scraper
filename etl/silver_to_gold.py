"""
ETL Pipeline: Silver to Gold - Analytics
Creates business-level aggregates and analytics-ready tables from silver layer data.
"""

import pandas as pd
from configs.db_config import get_connection


def calculate_innings_summary(conn, match_id, innings_number):
    """
    Calculate innings summary statistics.
    """
    query = """
        SELECT
            COUNT(*) as total_balls,
            SUM(runs_scored) as total_runs_from_bat,
            SUM(extras) as total_extras,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as total_wickets,
            SUM(CASE WHEN runs_scored = 4 THEN 1 ELSE 0 END) as fours,
            SUM(CASE WHEN runs_scored = 6 THEN 1 ELSE 0 END) as sixes,
            SUM(CASE WHEN runs_scored = 0 AND extras = 0 THEN 1 ELSE 0 END) as dots,
            SUM(CASE WHEN runs_scored = 1 THEN 1 ELSE 0 END) as singles,
            SUM(CASE WHEN runs_scored = 2 THEN 1 ELSE 0 END) as twos,
            SUM(CASE WHEN extra_type = 'wide' THEN extras ELSE 0 END) as wides,
            SUM(CASE WHEN extra_type = 'noball' THEN extras ELSE 0 END) as noballs,
            SUM(CASE WHEN extra_type = 'bye' THEN extras ELSE 0 END) as byes,
            SUM(CASE WHEN extra_type = 'legbye' THEN extras ELSE 0 END) as legbyes,
            MAX(total_runs) as final_score,
            MAX(total_wickets) as final_wickets,
            innings
        FROM silver_match_events
        WHERE match_id = %s AND innings_number = %s
        GROUP BY innings
    """

    df = pd.read_sql(query, conn, params=(match_id, innings_number))

    if df.empty:
        return None

    row = df.iloc[0]

    # Calculate overs (6 balls = 1 over)
    total_balls = row['total_balls']
    total_overs = total_balls // 6
    remaining_balls = total_balls % 6
    overs_decimal = round(total_overs + (remaining_balls / 10), 1)

    # Calculate run rate
    run_rate = round((row['final_score'] / total_balls) * 6, 2) if total_balls > 0 else 0

    # Get powerplay stats (first 6 overs)
    pp_query = """
        SELECT
            SUM(runs_scored + extras) as pp_runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as pp_wickets
        FROM silver_match_events
        WHERE match_id = %s AND innings_number = %s AND over_number < 6
    """
    pp_df = pd.read_sql(pp_query, conn, params=(match_id, innings_number))
    pp_runs = pp_df.iloc[0]['pp_runs'] if not pp_df.empty else 0
    pp_wickets = pp_df.iloc[0]['pp_wickets'] if not pp_df.empty else 0

    return {
        'match_id': match_id,
        'innings_number': innings_number,
        'team': row['innings'],
        'total_runs': int(row['final_score']),
        'total_wickets': int(row['final_wickets']),
        'total_overs': overs_decimal,
        'total_balls': int(total_balls),
        'boundaries': int(row['fours']),
        'sixes': int(row['sixes']),
        'dots': int(row['dots']),
        'singles': int(row['singles']),
        'twos': int(row['twos']),
        'wides': int(row['wides']),
        'noballs': int(row['noballs']),
        'byes': int(row['byes']),
        'legbyes': int(row['legbyes']),
        'total_extras': int(row['total_extras']),
        'run_rate': run_rate,
        'powerplay_runs': int(pp_runs) if pp_runs else 0,
        'powerplay_wickets': int(pp_wickets) if pp_wickets else 0,
    }


def calculate_batting_stats(conn, match_id):
    """
    Calculate batting statistics for all batsmen in a match.
    """
    query = """
        SELECT
            match_id,
            batsman as player_name,
            innings as team,
            COUNT(*) as balls_faced,
            SUM(runs_scored) as runs_scored,
            SUM(CASE WHEN runs_scored = 4 THEN 1 ELSE 0 END) as fours,
            SUM(CASE WHEN runs_scored = 6 THEN 1 ELSE 0 END) as sixes,
            MAX(CASE WHEN is_wicket THEN 1 ELSE 0 END) as is_out
        FROM silver_match_events
        WHERE match_id = %s AND batsman IS NOT NULL
        GROUP BY match_id, batsman, innings
    """

    df = pd.read_sql(query, conn, params=(match_id,))

    if df.empty:
        return []

    batting_stats = []

    for _, row in df.iterrows():
        balls = row['balls_faced']
        runs = row['runs_scored']
        strike_rate = round((runs / balls) * 100, 2) if balls > 0 else 0

        # Check for dismissal (get wicket type from events)
        dismissal_query = """
            SELECT wicket_type
            FROM silver_match_events
            WHERE match_id = %s AND batsman = %s AND is_wicket = TRUE
            LIMIT 1
        """
        dismissal_df = pd.read_sql(dismissal_query, conn, params=(match_id, row['player_name']))
        dismissal_type = dismissal_df.iloc[0]['wicket_type'] if not dismissal_df.empty else None

        batting_stats.append({
            'match_id': match_id,
            'player_name': row['player_name'],
            'team': row['team'],
            'runs_scored': int(runs),
            'balls_faced': int(balls),
            'fours': int(row['fours']),
            'sixes': int(row['sixes']),
            'strike_rate': strike_rate,
            'is_out': bool(row['is_out']),
            'dismissal_type': dismissal_type,
            'is_fifty': runs >= 50,
            'is_century': runs >= 100,
        })

    return batting_stats


def calculate_bowling_stats(conn, match_id):
    """
    Calculate bowling statistics for all bowlers in a match.
    """
    query = """
        SELECT
            match_id,
            bowler as player_name,
            COUNT(*) as balls_bowled,
            SUM(runs_scored) as runs_from_bat,
            SUM(extras) as extras_conceded,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets_taken,
            SUM(CASE WHEN extra_type = 'wide' THEN extras ELSE 0 END) as wides,
            SUM(CASE WHEN extra_type = 'noball' THEN extras ELSE 0 END) as noballs,
            SUM(CASE WHEN runs_scored = 0 AND extras = 0 AND NOT is_wicket THEN 1 ELSE 0 END) as dot_balls
        FROM silver_match_events
        WHERE match_id = %s AND bowler IS NOT NULL
        GROUP BY match_id, bowler
    """

    df = pd.read_sql(query, conn, params=(match_id,))

    if df.empty:
        return []

    bowling_stats = []

    # Get bowling team (opposite of batting team)
    team_query = """
        SELECT DISTINCT
            CASE
                WHEN innings_number = 1 THEN second_innings_team
                ELSE first_innings_team
            END as bowling_team,
            innings_number
        FROM silver_match_events e
        JOIN silver_match_metadata m ON e.match_id = m.match_id
        WHERE e.match_id = %s
    """
    team_df = pd.read_sql(team_query, conn, params=(match_id,))

    for _, row in df.iterrows():
        balls = row['balls_bowled']
        runs_conceded = row['runs_from_bat'] + row['extras_conceded']

        # Calculate overs
        overs = balls // 6
        remaining_balls = balls % 6
        overs_decimal = round(overs + (remaining_balls / 10), 1)

        # Economy rate
        economy = round((runs_conceded / balls) * 6, 2) if balls > 0 else 0

        # Maidens (overs with 0 runs)
        maiden_query = """
            SELECT over_number
            FROM silver_match_events
            WHERE match_id = %s AND bowler = %s
            GROUP BY over_number
            HAVING SUM(runs_scored + extras) = 0 AND COUNT(*) = 6
        """
        maiden_df = pd.read_sql(maiden_query, conn, params=(match_id, row['player_name']))
        maidens = len(maiden_df)

        # Determine bowling team (this is simplified - may need refinement)
        team = team_df.iloc[0]['bowling_team'] if not team_df.empty else None

        bowling_stats.append({
            'match_id': match_id,
            'player_name': row['player_name'],
            'team': team,
            'overs_bowled': overs_decimal,
            'balls_bowled': int(balls),
            'runs_conceded': int(runs_conceded),
            'wickets_taken': int(row['wickets_taken']),
            'maidens': maidens,
            'economy_rate': economy,
            'wides': int(row['wides']),
            'noballs': int(row['noballs']),
            'is_three_wicket': row['wickets_taken'] >= 3,
            'is_five_wicket': row['wickets_taken'] >= 5,
        })

    return bowling_stats


def calculate_match_summary(conn, match_id):
    """
    Calculate match summary statistics.
    """
    # Get metadata
    metadata_query = """
        SELECT
            match_id, venue, series, season, match_date,
            player_of_the_match, first_innings_team, second_innings_team
        FROM silver_match_metadata
        WHERE match_id = %s
    """
    metadata_df = pd.read_sql(metadata_query, conn, params=(match_id,))

    if metadata_df.empty:
        return None

    metadata = metadata_df.iloc[0]

    # Get innings summaries
    innings1 = calculate_innings_summary(conn, match_id, 1)
    innings2 = calculate_innings_summary(conn, match_id, 2)

    if not innings1 or not innings2:
        print(f"⚠️  Incomplete innings data for match {match_id}")
        return None

    # Determine winner (team with higher score)
    if innings1['total_runs'] > innings2['total_runs']:
        winner = innings1['team']
        margin = f"by {10 - innings2['total_wickets']} wickets" if innings1['innings_number'] == 2 else f"by {innings1['total_runs'] - innings2['total_runs']} runs"
    elif innings2['total_runs'] > innings1['total_runs']:
        winner = innings2['team']
        margin = f"by {10 - innings1['total_wickets']} wickets" if innings2['innings_number'] == 2 else f"by {innings2['total_runs'] - innings1['total_runs']} runs"
    else:
        winner = None
        margin = "Tie"

    # Calculate total statistics
    total_runs = innings1['total_runs'] + innings2['total_runs']
    total_wickets = innings1['total_wickets'] + innings2['total_wickets']
    total_boundaries = innings1['boundaries'] + innings2['boundaries']
    total_sixes = innings1['sixes'] + innings2['sixes']
    total_extras = innings1['total_extras'] + innings2['total_extras']

    return {
        'match_id': match_id,
        'venue': metadata['venue'],
        'series': metadata['series'],
        'season': metadata['season'],
        'match_date': metadata['match_date'],
        'first_innings_team': metadata['first_innings_team'],
        'first_innings_runs': innings1['total_runs'],
        'first_innings_wickets': innings1['total_wickets'],
        'first_innings_overs': innings1['total_overs'],
        'second_innings_team': metadata['second_innings_team'],
        'second_innings_runs': innings2['total_runs'],
        'second_innings_wickets': innings2['total_wickets'],
        'second_innings_overs': innings2['total_overs'],
        'winner': winner,
        'margin': margin,
        'result_type': 'normal',
        'total_runs': total_runs,
        'total_wickets': total_wickets,
        'total_boundaries': total_boundaries,
        'total_sixes': total_sixes,
        'total_extras': total_extras,
        'player_of_the_match': metadata['player_of_the_match'],
    }


def transform_to_gold():
    """
    Transform silver layer data into gold layer analytics tables.
    """
    print("\n" + "="*80)
    print("ETL: Silver -> Gold (Analytics)")
    print("="*80)

    conn = get_connection()
    cur = conn.cursor()

    # Get all matches from silver layer
    print("\n[1/5] Loading matches from Silver layer...")
    matches_df = pd.read_sql(
        "SELECT match_id FROM silver_match_metadata ORDER BY match_id",
        conn
    )

    if matches_df.empty:
        print("⚠️  No matches found in Silver layer. Please run Bronze->Silver ETL first.")
        conn.close()
        return

    match_ids = matches_df['match_id'].tolist()
    print(f"✅ Found {len(match_ids)} matches to process")

    # Process each match
    print("\n[2/5] Calculating match summaries...")
    for match_id in match_ids:
        summary = calculate_match_summary(conn, match_id)
        if summary:
            cur.execute("""
                INSERT INTO gold_match_summary (
                    match_id, venue, series, season, match_date,
                    first_innings_team, first_innings_runs, first_innings_wickets, first_innings_overs,
                    second_innings_team, second_innings_runs, second_innings_wickets, second_innings_overs,
                    winner, margin, result_type, total_runs, total_wickets,
                    total_boundaries, total_sixes, total_extras, player_of_the_match
                )
                VALUES (
                    %(match_id)s, %(venue)s, %(series)s, %(season)s, %(match_date)s,
                    %(first_innings_team)s, %(first_innings_runs)s, %(first_innings_wickets)s, %(first_innings_overs)s,
                    %(second_innings_team)s, %(second_innings_runs)s, %(second_innings_wickets)s, %(second_innings_overs)s,
                    %(winner)s, %(margin)s, %(result_type)s, %(total_runs)s, %(total_wickets)s,
                    %(total_boundaries)s, %(total_sixes)s, %(total_extras)s, %(player_of_the_match)s
                )
                ON CONFLICT (match_id) DO UPDATE SET
                    venue = EXCLUDED.venue,
                    winner = EXCLUDED.winner,
                    margin = EXCLUDED.margin,
                    total_runs = EXCLUDED.total_runs,
                    updated_at = CURRENT_TIMESTAMP
            """, summary)

    conn.commit()
    print(f"✅ Processed {len(match_ids)} match summaries")

    # Calculate innings summaries
    print("\n[3/5] Calculating innings summaries...")
    for match_id in match_ids:
        for innings_num in [1, 2]:
            innings_summary = calculate_innings_summary(conn, match_id, innings_num)
            if innings_summary:
                cur.execute("""
                    INSERT INTO gold_innings_summary (
                        match_id, innings_number, team, total_runs, total_wickets,
                        total_overs, total_balls, boundaries, sixes, dots, singles, twos,
                        wides, noballs, byes, legbyes, total_extras, run_rate,
                        powerplay_runs, powerplay_wickets
                    )
                    VALUES (
                        %(match_id)s, %(innings_number)s, %(team)s, %(total_runs)s,
                        %(total_wickets)s, %(total_overs)s, %(total_balls)s,
                        %(boundaries)s, %(sixes)s, %(dots)s, %(singles)s, %(twos)s,
                        %(wides)s, %(noballs)s, %(byes)s, %(legbyes)s, %(total_extras)s,
                        %(run_rate)s, %(powerplay_runs)s, %(powerplay_wickets)s
                    )
                    ON CONFLICT DO NOTHING
                """, innings_summary)

    conn.commit()
    print(f"✅ Processed innings summaries")

    # Calculate batting stats
    print("\n[4/5] Calculating player batting statistics...")
    for match_id in match_ids:
        batting_stats = calculate_batting_stats(conn, match_id)
        for stats in batting_stats:
            cur.execute("""
                INSERT INTO gold_player_batting_stats (
                    match_id, player_name, team, runs_scored, balls_faced,
                    fours, sixes, strike_rate, is_out, dismissal_type,
                    is_fifty, is_century
                )
                VALUES (
                    %(match_id)s, %(player_name)s, %(team)s, %(runs_scored)s,
                    %(balls_faced)s, %(fours)s, %(sixes)s, %(strike_rate)s,
                    %(is_out)s, %(dismissal_type)s, %(is_fifty)s, %(is_century)s
                )
                ON CONFLICT DO NOTHING
            """, stats)

    conn.commit()
    print(f"✅ Processed batting statistics")

    # Calculate bowling stats
    print("\n[5/5] Calculating player bowling statistics...")
    for match_id in match_ids:
        bowling_stats = calculate_bowling_stats(conn, match_id)
        for stats in bowling_stats:
            cur.execute("""
                INSERT INTO gold_player_bowling_stats (
                    match_id, player_name, team, overs_bowled, balls_bowled,
                    runs_conceded, wickets_taken, maidens, economy_rate,
                    wides, noballs, is_three_wicket, is_five_wicket
                )
                VALUES (
                    %(match_id)s, %(player_name)s, %(team)s, %(overs_bowled)s,
                    %(balls_bowled)s, %(runs_conceded)s, %(wickets_taken)s,
                    %(maidens)s, %(economy_rate)s, %(wides)s, %(noballs)s,
                    %(is_three_wicket)s, %(is_five_wicket)s
                )
                ON CONFLICT DO NOTHING
            """, stats)

    conn.commit()
    print(f"✅ Processed bowling statistics")

    # Show final statistics
    print("\n" + "="*80)
    print("Gold Layer Statistics")
    print("="*80)

    cur.execute("SELECT COUNT(*) FROM gold_match_summary")
    print(f"Match Summaries: {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM gold_innings_summary")
    print(f"Innings Summaries: {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM gold_player_batting_stats")
    print(f"Batting Records: {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM gold_player_bowling_stats")
    print(f"Bowling Records: {cur.fetchone()[0]}")

    cur.close()
    conn.close()

    print("\n" + "="*80)
    print("ETL Complete: Silver -> Gold (Analytics)")
    print("="*80 + "\n")


if __name__ == "__main__":
    transform_to_gold()
