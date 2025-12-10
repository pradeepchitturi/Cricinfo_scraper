-- ============================================================================
-- Raw Schema - Cricinfo Data
-- ============================================================================

-- Create the raw schema
CREATE SCHEMA IF NOT EXISTS raw;

-- ============================================================================
-- Match Metadata Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.match_metadata (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Match Events Table (Ball-by-ball commentary)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.match_events (
    id SERIAL PRIMARY KEY,
    ball VARCHAR(10),
    event TEXT,
    score VARCHAR(50),
    commentary TEXT,
    bowler VARCHAR(100),
    batsman VARCHAR(100),
    innings VARCHAR(50),
    matchid BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Match Players Table (Player rosters)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.match_players (
    id SERIAL PRIMARY KEY,
    matchid BIGINT NOT NULL,
    innings VARCHAR(20),  -- NULL for impact players
    team VARCHAR(100) NOT NULL,
    player_name VARCHAR(100) NOT NULL,
    batted BOOLEAN NOT NULL DEFAULT FALSE,
    batting_position INT,
    player_type VARCHAR(20) DEFAULT 'regular' CHECK (player_type IN ('regular', 'impact', 'substitute')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Match Download Tracker Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.match_download_tracker (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL,
    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'completed' CHECK (status IN ('completed', 'failed', 'in_progress')),
    metadata_rows INT DEFAULT 0,
    events_rows INT DEFAULT 0,
    source_url TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_match_id UNIQUE (match_id),
    CONSTRAINT check_positive_rows CHECK (metadata_rows >= 0 AND events_rows >= 0)
);

-- ============================================================================
-- Altering Tables to accommodate Super Over innings
-- ============================================================================

ALTER TABLE raw.match_events
ADD COLUMN IF NOT EXISTS is_super_over BOOLEAN DEFAULT FALSE;

-- Update metadata table
ALTER TABLE raw.match_metadata
ADD COLUMN IF NOT EXISTS has_super_over BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS super_over_count INTEGER DEFAULT 0;

-- ============================================================================
-- Indexes for Performance
-- ============================================================================

-- Match Metadata Indexes
CREATE INDEX IF NOT EXISTS idx_raw_metadata_matchid ON raw.match_metadata(matchid);
CREATE INDEX IF NOT EXISTS idx_raw_metadata_series ON raw.match_metadata(series, season);

-- Match Events Indexes
CREATE INDEX IF NOT EXISTS idx_raw_events_matchid ON raw.match_events(matchid);
CREATE INDEX IF NOT EXISTS idx_raw_events_innings ON raw.match_events(innings);
CREATE INDEX IF NOT EXISTS idx_raw_events_batsman ON raw.match_events(batsman);
CREATE INDEX IF NOT EXISTS idx_raw_events_bowler ON raw.match_events(bowler);

-- Match Players Indexes
CREATE INDEX IF NOT EXISTS idx_raw_players_matchid ON raw.match_players(matchid);
CREATE INDEX IF NOT EXISTS idx_raw_players_team ON raw.match_players(team);
CREATE INDEX IF NOT EXISTS idx_raw_players_player ON raw.match_players(player_name);
CREATE INDEX IF NOT EXISTS idx_raw_players_batted ON raw.match_players(batted);
CREATE INDEX IF NOT EXISTS idx_raw_players_type ON raw.match_players(player_type);
-- Create unique index that handles NULL innings
-- For regular players: (matchid, innings, player_name) must be unique
-- For impact players: (matchid, player_name) must be unique when innings IS NULL
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_players_unique_regular
ON raw.match_players(matchid, innings, player_name,player_type)
WHERE innings IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_players_unique_impact
ON raw.match_players(matchid, player_name,player_type)
WHERE innings IS NULL;

-- Download Tracker Indexes
CREATE INDEX IF NOT EXISTS idx_tracker_match_id ON raw.match_download_tracker(match_id);
CREATE INDEX IF NOT EXISTS idx_tracker_status ON raw.match_download_tracker(status);

-- ============================================================================
-- Success Message
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Raw schema created successfully!';
    RAISE NOTICE 'Tables: match_metadata, match_events, match_players, match_download_tracker';
END $$;