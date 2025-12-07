-- ============================================================================
-- Medallion Architecture Schema - Bronze, Silver, Gold
-- ============================================================================

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- BRONZE LAYER TABLES
-- ============================================================================

-- Bronze Match Metadata
DROP TABLE IF EXISTS bronze.match_metadata CASCADE;
CREATE TABLE bronze.match_metadata (
    id SERIAL PRIMARY KEY,
    venue VARCHAR(255),
    toss VARCHAR(255),
    series VARCHAR(255),
    season INT,
    player_of_the_match VARCHAR(255),
    hours_of_play_local_time TEXT,
    match_days VARCHAR(255),
    t20_debut VARCHAR(255),
    umpires VARCHAR(255),
    tv_umpire VARCHAR(255),
    reserve_umpire VARCHAR(255),
    match_referee VARCHAR(255),
    points VARCHAR(255),
    matchid BIGINT,
    player_replacements VARCHAR(255),
    first_innings VARCHAR(20),
    second_innings VARCHAR(20),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'cricinfo',
    pipeline_run_id VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze Match Events
DROP TABLE IF EXISTS bronze.match_events CASCADE;
CREATE TABLE bronze.match_events (
    id SERIAL PRIMARY KEY,
    ball VARCHAR(10),
    event TEXT,
    score VARCHAR(50),
    commentary TEXT,
    bowler VARCHAR(100),
    batsman VARCHAR(100),
    innings VARCHAR(50),
    matchid BIGINT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'cricinfo',
    pipeline_run_id VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze Match Players
DROP TABLE IF EXISTS bronze.match_players CASCADE;
CREATE TABLE bronze.match_players (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL,
    innings VARCHAR(20),
    team VARCHAR(100) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    batted BOOLEAN NOT NULL DEFAULT FALSE,
    batting_position INT,
    player_type VARCHAR(20) DEFAULT 'regular' CHECK (player_type IN ('regular', 'impact', 'substitute')),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'cricinfo',
    pipeline_run_id VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Unique indexes for Bronze match_players
CREATE UNIQUE INDEX idx_bronze_players_unique_regular
ON bronze.match_players(matchid, innings, player_name)
WHERE innings IS NOT NULL AND is_active = TRUE;

CREATE UNIQUE INDEX idx_bronze_players_unique_impact
ON bronze.match_players(matchid, player_name)
WHERE innings IS NULL AND is_active = TRUE;

-- Bronze Indexes
CREATE INDEX idx_bronze_metadata_matchid ON bronze.match_metadata(matchid);
CREATE INDEX idx_bronze_events_matchid ON bronze.match_events(matchid);
CREATE INDEX idx_bronze_events_match_ball ON bronze.match_events(matchid, ball, innings);
CREATE INDEX idx_bronze_players_matchid ON bronze.match_players(matchid);
CREATE INDEX idx_bronze_players_team ON bronze.match_players(team);
CREATE INDEX idx_bronze_players_player ON bronze.match_players(player_name);
CREATE INDEX idx_bronze_players_type ON bronze.match_players(player_type);

-- ============================================================================
-- SILVER LAYER TABLES
-- ============================================================================

-- Silver Match Metadata
DROP TABLE IF EXISTS silver.match_metadata CASCADE;
CREATE TABLE silver.match_metadata (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL UNIQUE,
    venue VARCHAR(255),
    toss VARCHAR(255),
    series VARCHAR(255),
    season INT,
    player_of_the_match VARCHAR(255),
    hours_of_play_local_time TEXT,
    match_days VARCHAR(255),
    t20_debut VARCHAR(255),
    umpires VARCHAR(255),
    tv_umpire VARCHAR(255),
    reserve_umpire VARCHAR(255),
    match_referee VARCHAR(255),
    points VARCHAR(255),
    player_replacements VARCHAR(255),
    first_innings VARCHAR(20),
    second_innings VARCHAR(20),
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    source_id INT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver Match Events
DROP TABLE IF EXISTS silver.match_events CASCADE;
CREATE TABLE silver.match_events (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL,
    ball VARCHAR(10) NOT NULL,
    innings VARCHAR(50),
    event TEXT,
    score VARCHAR(50),
    runs_scored INT DEFAULT 0,
    commentary TEXT,
    bowler VARCHAR(100),
    batsman VARCHAR(100),
    dismissal_method VARCHAR(50),
    fielder_name VARCHAR(100),
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    source_id BIGINT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_silver_match_ball UNIQUE (matchid, ball, innings)
);

-- Silver Match Players
DROP TABLE IF EXISTS silver.match_players CASCADE;
CREATE TABLE silver.match_players (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL,
    innings VARCHAR(20),
    team VARCHAR(100) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    batted BOOLEAN NOT NULL DEFAULT FALSE,
    batting_position INT,
    player_type VARCHAR(20) DEFAULT 'regular' CHECK (player_type IN ('regular', 'impact', 'substitute')),
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    source_id BIGINT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Unique indexes for Silver match_players
CREATE UNIQUE INDEX idx_silver_players_unique_regular
ON silver.match_players(matchid, innings, player_name)
WHERE innings IS NOT NULL;

CREATE UNIQUE INDEX idx_silver_players_unique_impact
ON silver.match_players(matchid, player_name)
WHERE innings IS NULL;

-- Silver Indexes
CREATE INDEX idx_silver_metadata_matchid ON silver.match_metadata(matchid);
CREATE INDEX idx_silver_metadata_series ON silver.match_metadata(series, season);
CREATE INDEX idx_silver_events_matchid ON silver.match_events(matchid);
CREATE INDEX idx_silver_events_batsman ON silver.match_events(batsman);
CREATE INDEX idx_silver_events_bowler ON silver.match_events(bowler);
CREATE INDEX idx_silver_players_matchid ON silver.match_players(matchid);
CREATE INDEX idx_silver_players_team ON silver.match_players(team);
CREATE INDEX idx_silver_players_player ON silver.match_players(player_name);
CREATE INDEX idx_silver_players_type ON silver.match_players(player_type);

-- ============================================================================
-- GOLD LAYER TABLES
-- ============================================================================

-- Match Summary
DROP TABLE IF EXISTS gold.match_summary CASCADE;
CREATE TABLE gold.match_summary (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL UNIQUE,
    venue VARCHAR(255),
    series VARCHAR(255),
    season INT,
    player_of_the_match VARCHAR(255),
    first_innings VARCHAR(100),
    second_innings VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Series Summary
DROP TABLE IF EXISTS gold.series_summary CASCADE;
CREATE TABLE gold.series_summary (
    id SERIAL PRIMARY KEY,
    series VARCHAR(255) NOT NULL,
    season INT NOT NULL,
    total_matches INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series, season)
);

-- Venue Statistics
DROP TABLE IF EXISTS gold.venue_statistics CASCADE;
CREATE TABLE gold.venue_statistics (
    id SERIAL PRIMARY KEY,
    venue VARCHAR(255) NOT NULL,
    season INT NOT NULL,
    total_matches INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(venue, season)
);

-- Match Ball Statistics
DROP TABLE IF EXISTS gold.match_ball_statistics CASCADE;
CREATE TABLE gold.match_ball_statistics (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL,
    innings VARCHAR(50) NOT NULL,
    total_balls INT,
    total_runs INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(matchid, innings)
);

-- Batsman Statistics
DROP TABLE IF EXISTS gold.batsman_statistics CASCADE;
CREATE TABLE gold.batsman_statistics (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL,
    batsman VARCHAR(100) NOT NULL,
    innings VARCHAR(50) NOT NULL,
    total_runs INT,
    balls_faced INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(matchid, batsman, innings)
);

-- Bowler Statistics
DROP TABLE IF EXISTS gold.bowler_statistics CASCADE;
CREATE TABLE gold.bowler_statistics (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL,
    bowler VARCHAR(100) NOT NULL,
    innings VARCHAR(50) NOT NULL,
    balls_bowled INT,
    runs_conceded INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(matchid, bowler, innings)
);

-- Player Team History (SCD Type 2)
DROP TABLE IF EXISTS gold.player_team_history CASCADE;
CREATE TABLE gold.player_team_history (
    id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    team VARCHAR(100) NOT NULL,
    season INT NOT NULL,
    series VARCHAR(255),
    first_match_date DATE,
    last_match_date DATE,
    matches_played INT DEFAULT 0,
    is_impact_player BOOLEAN DEFAULT FALSE,

    -- SCD Type 2 columns
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    version INT DEFAULT 1,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create partial unique index for current records
-- This ensures only one current record per player-team combination
CREATE UNIQUE INDEX idx_gold_player_team_unique_current
ON gold.player_team_history(player_name, team, season)
WHERE is_current = TRUE;

-- Indexes for efficient querying
CREATE INDEX idx_gold_player_team_player ON gold.player_team_history(player_name);
CREATE INDEX idx_gold_player_team_team ON gold.player_team_history(team);
CREATE INDEX idx_gold_player_team_season ON gold.player_team_history(season);
CREATE INDEX idx_gold_player_team_current ON gold.player_team_history(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_gold_player_team_effective ON gold.player_team_history(effective_from, effective_to);
CREATE INDEX idx_gold_player_team_composite ON gold.player_team_history(player_name, team, season);

-- Gold Indexes
CREATE INDEX idx_gold_series_summary ON gold.series_summary(series, season);
CREATE INDEX idx_gold_venue_stats ON gold.venue_statistics(venue, season);
CREATE INDEX idx_gold_match_ball ON gold.match_ball_statistics(matchid, innings);
CREATE INDEX idx_gold_batsman ON gold.batsman_statistics(batsman);
CREATE INDEX idx_gold_bowler ON gold.bowler_statistics(bowler);

-- Success Message
DO $$
BEGIN
    RAISE NOTICE 'Medallion Architecture schema created successfully!';
    RAISE NOTICE 'Schemas: bronze, silver, gold';
    RAISE NOTICE 'Player Team History table added with SCD Type 2 support';
END $$;