-- ============================================================================
-- Medallion Architecture Schema - Bronze, Silver, Gold
-- ============================================================================

-- Create Bronze Schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create Silver Schema
CREATE SCHEMA IF NOT EXISTS silver;

-- Create Gold Schema
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- BRONZE LAYER TABLES (Copy of raw with audit columns)
-- ============================================================================

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

-- Indexes for Bronze
CREATE INDEX idx_bronze_metadata_matchid ON bronze.match_metadata(matchid);
CREATE INDEX idx_bronze_events_matchid ON bronze.match_events(matchid);
CREATE INDEX idx_bronze_events_match_ball ON bronze.match_events(matchid, ball, innings);

-- ============================================================================
-- SILVER LAYER TABLES (Cleaned and validated)
-- ============================================================================

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
    dismissal_method VARCHAR(50),      -- NEW: Method of dismissal
    fielder_name VARCHAR(100),         -- NEW: Fielder who took catch/stumping

    -- Silver audit columns
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    source_id BIGINT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_silver_match_ball UNIQUE (matchid, ball, innings)
);

-- Indexes for Silver
CREATE INDEX idx_silver_metadata_matchid ON silver.match_metadata(matchid);
CREATE INDEX idx_silver_metadata_series ON silver.match_metadata(series, season);
CREATE INDEX idx_silver_events_matchid ON silver.match_events(matchid);
CREATE INDEX idx_silver_events_batsman ON silver.match_events(batsman);
CREATE INDEX idx_silver_events_bowler ON silver.match_events(bowler);

-- ============================================================================
-- GOLD LAYER - Business Aggregations
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

-- Series Summary (per season)
DROP TABLE IF EXISTS gold.series_summary CASCADE;
CREATE TABLE gold.series_summary (
    id SERIAL PRIMARY KEY,
    series VARCHAR(255) NOT NULL,
    season INT NOT NULL,
    total_matches INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series, season)  -- One row per series per season
);

-- Venue Statistics (per season)
DROP TABLE IF EXISTS gold.venue_statistics CASCADE;
CREATE TABLE gold.venue_statistics (
    id SERIAL PRIMARY KEY,
    venue VARCHAR(255) NOT NULL,
    season INT NOT NULL,
    total_matches INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(venue, season)  -- One row per venue per season
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
    UNIQUE(matchid, innings)  -- One row per match per innings
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
    UNIQUE(matchid, batsman, innings)  -- One row per batsman per match per innings
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
    UNIQUE(matchid, bowler, innings)  -- One row per bowler per match per innings
);
-- Indexes for Gold
CREATE INDEX idx_gold_series_summary ON gold.series_summary(series, season);
CREATE INDEX idx_gold_venue_stats ON gold.venue_statistics(venue, season);
CREATE INDEX idx_gold_match_ball ON gold.match_ball_statistics(matchid, innings);
CREATE INDEX idx_gold_batsman ON gold.batsman_statistics(batsman);
CREATE INDEX idx_gold_bowler ON gold.bowler_statistics(bowler);

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Medallion Architecture schema created successfully!';
    RAISE NOTICE 'Schemas: bronze, silver, gold';
END $$;