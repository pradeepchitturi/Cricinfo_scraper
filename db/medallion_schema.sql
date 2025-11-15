-- ============================================================================
-- MEDALLION ARCHITECTURE SCHEMA
-- ============================================================================
-- This schema implements a 3-layer medallion architecture:
-- - BRONZE: Raw data (already exists as raw_match_metadata, raw_match_events)
-- - SILVER: Cleaned, validated, and enriched data
-- - GOLD: Business-level aggregates and analytics-ready data
-- ============================================================================

-- ============================================================================
-- BRONZE LAYER (Raw Data)
-- ============================================================================
-- Tables: raw_match_metadata, raw_match_events
-- These tables are already defined in schema.sql
-- They contain unvalidated, as-is data from the scraper

-- ============================================================================
-- SILVER LAYER (Cleaned & Enriched Data)
-- ============================================================================

-- Silver: Match Metadata (Cleaned and Normalized)
CREATE TABLE IF NOT EXISTS silver_match_metadata (
    id SERIAL PRIMARY KEY,
    match_id BIGINT UNIQUE NOT NULL,

    -- Match Details
    venue VARCHAR(255),
    series VARCHAR(255),
    season INT,
    match_date DATE,

    -- Toss Information
    toss_winner VARCHAR(100),
    toss_decision VARCHAR(50), -- 'bat' or 'field'

    -- Match Officials
    umpire_1 VARCHAR(100),
    umpire_2 VARCHAR(100),
    tv_umpire VARCHAR(100),
    reserve_umpire VARCHAR(100),
    match_referee VARCHAR(100),

    -- Awards
    player_of_the_match VARCHAR(255),

    -- Teams
    first_innings_team VARCHAR(100),
    second_innings_team VARCHAR(100),

    -- Additional Info
    t20_debuts TEXT[], -- Array of players making T20 debuts
    hours_of_play_local_time TEXT,
    points VARCHAR(255),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver: Player Replacements (Normalized from JSON)
CREATE TABLE IF NOT EXISTS silver_player_replacements (
    id SERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    player_out VARCHAR(100),
    player_in VARCHAR(100),
    team VARCHAR(100),
    replacement_type VARCHAR(50), -- 'concussion', 'covid', 'injury', etc.

    FOREIGN KEY (match_id) REFERENCES silver_match_metadata(match_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver: Match Events (Cleaned and Enriched)
CREATE TABLE IF NOT EXISTS silver_match_events (
    id SERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,

    -- Ball Details
    over_number INT,
    ball_number INT,
    ball_in_over INT,
    ball_notation VARCHAR(10), -- "0.1", "0.2", etc.

    -- Players
    bowler VARCHAR(100),
    batsman VARCHAR(100),
    non_striker VARCHAR(100),

    -- Event Details
    runs_scored INT DEFAULT 0,
    extras INT DEFAULT 0,
    extra_type VARCHAR(20), -- 'wide', 'noball', 'bye', 'legbye', 'penalty'
    is_wicket BOOLEAN DEFAULT FALSE,
    wicket_type VARCHAR(50), -- 'bowled', 'caught', 'lbw', 'run out', 'stumped', etc.
    fielder VARCHAR(100), -- For caught/run out

    -- Match State
    innings VARCHAR(100), -- Team batting
    innings_number INT, -- 1 or 2
    total_runs INT, -- Cumulative runs in innings
    total_wickets INT, -- Cumulative wickets in innings

    -- Raw Data (for reference)
    raw_event TEXT,
    commentary TEXT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (match_id) REFERENCES silver_match_metadata(match_id) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_silver_events_match_id ON silver_match_events(match_id);
CREATE INDEX IF NOT EXISTS idx_silver_events_bowler ON silver_match_events(bowler);
CREATE INDEX IF NOT EXISTS idx_silver_events_batsman ON silver_match_events(batsman);
CREATE INDEX IF NOT EXISTS idx_silver_events_innings ON silver_match_events(innings_number);

-- ============================================================================
-- GOLD LAYER (Analytics-Ready Data)
-- ============================================================================

-- Gold: Match Summary (Business-Level Aggregates)
CREATE TABLE IF NOT EXISTS gold_match_summary (
    id SERIAL PRIMARY KEY,
    match_id BIGINT UNIQUE NOT NULL,

    -- Match Info
    venue VARCHAR(255),
    series VARCHAR(255),
    season INT,
    match_date DATE,

    -- First Innings
    first_innings_team VARCHAR(100),
    first_innings_runs INT,
    first_innings_wickets INT,
    first_innings_overs DECIMAL(4,1),

    -- Second Innings
    second_innings_team VARCHAR(100),
    second_innings_runs INT,
    second_innings_wickets INT,
    second_innings_overs DECIMAL(4,1),

    -- Match Result
    winner VARCHAR(100),
    margin VARCHAR(100), -- "by 5 wickets", "by 20 runs", etc.
    result_type VARCHAR(50), -- 'normal', 'super over', 'no result', 'tie'

    -- Match Statistics
    total_runs INT,
    total_wickets INT,
    total_boundaries INT, -- 4s and 6s
    total_sixes INT,
    total_extras INT,

    -- Awards
    player_of_the_match VARCHAR(255),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (match_id) REFERENCES silver_match_metadata(match_id) ON DELETE CASCADE
);

-- Gold: Player Batting Performance (Per Match)
CREATE TABLE IF NOT EXISTS gold_player_batting_stats (
    id SERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    player_name VARCHAR(100),
    team VARCHAR(100),

    -- Batting Stats
    runs_scored INT DEFAULT 0,
    balls_faced INT DEFAULT 0,
    fours INT DEFAULT 0,
    sixes INT DEFAULT 0,
    strike_rate DECIMAL(6,2),
    is_out BOOLEAN DEFAULT FALSE,
    dismissal_type VARCHAR(50),

    -- Milestones
    is_fifty BOOLEAN DEFAULT FALSE,
    is_century BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (match_id) REFERENCES silver_match_metadata(match_id) ON DELETE CASCADE
);

-- Gold: Player Bowling Performance (Per Match)
CREATE TABLE IF NOT EXISTS gold_player_bowling_stats (
    id SERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    player_name VARCHAR(100),
    team VARCHAR(100),

    -- Bowling Stats
    overs_bowled DECIMAL(4,1),
    balls_bowled INT DEFAULT 0,
    runs_conceded INT DEFAULT 0,
    wickets_taken INT DEFAULT 0,
    maidens INT DEFAULT 0,
    economy_rate DECIMAL(5,2),

    -- Extras
    wides INT DEFAULT 0,
    noballs INT DEFAULT 0,

    -- Milestones
    is_three_wicket BOOLEAN DEFAULT FALSE,
    is_five_wicket BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (match_id) REFERENCES silver_match_metadata(match_id) ON DELETE CASCADE
);

-- Gold: Innings Summary
CREATE TABLE IF NOT EXISTS gold_innings_summary (
    id SERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    innings_number INT,
    team VARCHAR(100),

    -- Innings Totals
    total_runs INT,
    total_wickets INT,
    total_overs DECIMAL(4,1),
    total_balls INT,

    -- Scoring Pattern
    boundaries INT, -- 4s
    sixes INT,
    dots INT, -- Dot balls
    singles INT,
    twos INT,

    -- Extras Breakdown
    wides INT DEFAULT 0,
    noballs INT DEFAULT 0,
    byes INT DEFAULT 0,
    legbyes INT DEFAULT 0,
    penalty INT DEFAULT 0,
    total_extras INT,

    -- Run Rate
    run_rate DECIMAL(5,2),

    -- Powerplay Stats (if applicable)
    powerplay_runs INT,
    powerplay_wickets INT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (match_id) REFERENCES silver_match_metadata(match_id) ON DELETE CASCADE
);

-- Create indexes for Gold layer
CREATE INDEX IF NOT EXISTS idx_gold_batting_match ON gold_player_batting_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_gold_batting_player ON gold_player_batting_stats(player_name);
CREATE INDEX IF NOT EXISTS idx_gold_bowling_match ON gold_player_bowling_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_gold_bowling_player ON gold_player_bowling_stats(player_name);
CREATE INDEX IF NOT EXISTS idx_gold_summary_season ON gold_match_summary(season);
CREATE INDEX IF NOT EXISTS idx_gold_innings_match ON gold_innings_summary(match_id);

-- ============================================================================
-- VIEWS FOR ANALYTICS
-- ============================================================================

-- View: Season-wise Team Performance
CREATE OR REPLACE VIEW view_team_season_performance AS
SELECT
    season,
    first_innings_team as team,
    COUNT(*) as matches_played,
    SUM(CASE WHEN winner = first_innings_team THEN 1 ELSE 0 END) as wins,
    AVG(first_innings_runs) as avg_runs_scored,
    AVG(first_innings_wickets) as avg_wickets_lost
FROM gold_match_summary
GROUP BY season, first_innings_team
UNION ALL
SELECT
    season,
    second_innings_team as team,
    COUNT(*) as matches_played,
    SUM(CASE WHEN winner = second_innings_team THEN 1 ELSE 0 END) as wins,
    AVG(second_innings_runs) as avg_runs_scored,
    AVG(second_innings_wickets) as avg_wickets_lost
FROM gold_match_summary
GROUP BY season, second_innings_team;

-- View: Top Batsmen (Aggregate Stats)
CREATE OR REPLACE VIEW view_top_batsmen AS
SELECT
    player_name,
    COUNT(*) as matches,
    SUM(runs_scored) as total_runs,
    SUM(balls_faced) as total_balls,
    AVG(runs_scored) as avg_runs,
    ROUND(AVG(strike_rate), 2) as avg_strike_rate,
    SUM(fours) as total_fours,
    SUM(sixes) as total_sixes,
    SUM(CASE WHEN is_fifty THEN 1 ELSE 0 END) as fifties,
    SUM(CASE WHEN is_century THEN 1 ELSE 0 END) as centuries
FROM gold_player_batting_stats
GROUP BY player_name
HAVING COUNT(*) >= 1
ORDER BY total_runs DESC;

-- View: Top Bowlers (Aggregate Stats)
CREATE OR REPLACE VIEW view_top_bowlers AS
SELECT
    player_name,
    COUNT(*) as matches,
    SUM(wickets_taken) as total_wickets,
    SUM(balls_bowled) as total_balls,
    SUM(runs_conceded) as total_runs_conceded,
    ROUND(AVG(economy_rate), 2) as avg_economy,
    ROUND(CAST(SUM(runs_conceded) AS DECIMAL) / NULLIF(SUM(wickets_taken), 0), 2) as bowling_average,
    SUM(CASE WHEN is_five_wicket THEN 1 ELSE 0 END) as five_wicket_hauls
FROM gold_player_bowling_stats
GROUP BY player_name
HAVING COUNT(*) >= 1
ORDER BY total_wickets DESC;
