# Medallion Architecture Implementation Guide

## Overview

This document describes the complete implementation of the Medallion Architecture (Bronze, Silver, Gold layers) for the Cricinfo Scraper project.

## Architecture Summary

```
┌──────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                        │
│                    Data Lakehouse Pattern                        │
└──────────────────────────────────────────────────────────────────┘

Layer 1: BRONZE (Raw/Landing Zone)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Purpose: Preserve raw data exactly as ingested from source
Tables:
  - raw_match_metadata (17 columns)
  - raw_match_events (8 columns)

Characteristics:
  ✓ No transformations
  ✓ No data quality checks
  ✓ Append-only or full refresh
  ✓ Serves as immutable source of truth


Layer 2: SILVER (Cleansed/Curated Zone)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Purpose: Cleaned, validated, and enriched data
Tables:
  - silver_match_metadata (17 columns + metadata)
  - silver_player_replacements (normalized from JSON)
  - silver_match_events (20 columns + metadata)

Transformations Applied:
  ✓ Data type conversions (dates, integers)
  ✓ Parsing complex fields (toss, umpires, events)
  ✓ Data validation and cleaning
  ✓ Normalization (1NF, 2NF, 3NF)
  ✓ Enrichment with derived fields
  ✓ Foreign key relationships


Layer 3: GOLD (Analytics/Consumption Zone)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Purpose: Business-ready aggregates for analytics
Tables:
  - gold_match_summary
  - gold_innings_summary
  - gold_player_batting_stats
  - gold_player_bowling_stats

Views:
  - view_team_season_performance
  - view_top_batsmen
  - view_top_bowlers

Features:
  ✓ Pre-aggregated metrics
  ✓ Denormalized for query performance
  ✓ Business logic applied
  ✓ Optimized for BI tools
```

## Data Flow

```
ESPN Cricinfo Website
         ↓
   [Web Scraper]
         ↓
┌────────────────────┐
│   BRONZE LAYER     │  ← Raw data ingestion
│  (raw_*)           │
└────────────────────┘
         ↓
    [ETL Process]
    - Parse fields
    - Clean data
    - Validate types
    - Enrich with calculations
         ↓
┌────────────────────┐
│   SILVER LAYER     │  ← Cleaned & enriched
│  (silver_*)        │
└────────────────────┘
         ↓
    [ETL Process]
    - Aggregate stats
    - Calculate metrics
    - Create summaries
         ↓
┌────────────────────┐
│   GOLD LAYER       │  ← Analytics-ready
│  (gold_*)          │
└────────────────────┘
         ↓
    Analytics & BI
```

## Implementation Details

### 1. Database Schema

#### Bronze Layer Tables

**raw_match_metadata**
- Direct copy of scraped match information
- Columns: venue, toss, series, season, umpires, matchid, etc.
- No constraints except primary key
- File: `db/schema.sql`

**raw_match_events**
- Ball-by-ball commentary as scraped
- Columns: ball, event, score, commentary, bowler, batsman, innings, matchid
- No data type enforcement
- File: `db/schema.sql`

#### Silver Layer Tables

**silver_match_metadata**
- Normalized match information
- Parsed fields:
  - `toss` → `toss_winner`, `toss_decision`
  - `umpires` → `umpire_1`, `umpire_2`
  - `match_days` → `match_date` (DATE type)
  - `t20_debut` → `t20_debuts` (TEXT[] array)
- Added: `created_at`, `updated_at` timestamps
- UNIQUE constraint on `match_id`
- File: `db/medallion_schema.sql`

**silver_player_replacements**
- Extracted from JSON in raw_match_metadata
- Columns: player_out, player_in, team, replacement_type
- Foreign key to silver_match_metadata
- File: `db/medallion_schema.sql`

**silver_match_events**
- Enriched ball-by-ball events
- Parsed fields:
  - `ball` → `over_number`, `ball_number`, `ball_in_over`
  - `event` → `runs_scored`, `extras`, `extra_type`, `is_wicket`, `wicket_type`
  - Added: `fielder`, `total_runs`, `total_wickets`
- Indexes on: match_id, bowler, batsman, innings_number
- File: `db/medallion_schema.sql`

#### Gold Layer Tables

**gold_match_summary**
- Match-level aggregates
- Includes: innings scores, winner, margin, total statistics
- Calculated fields: run rates, boundaries, extras
- UNIQUE constraint on match_id
- File: `db/medallion_schema.sql`

**gold_innings_summary**
- Innings-level statistics
- Scoring patterns: dots, singles, twos, boundaries, sixes
- Powerplay statistics
- Extras breakdown
- File: `db/medallion_schema.sql`

**gold_player_batting_stats**
- Per-match batting performance
- Metrics: runs, balls, strike rate, boundaries
- Milestones: fifties, centuries
- Dismissal information
- File: `db/medallion_schema.sql`

**gold_player_bowling_stats**
- Per-match bowling performance
- Metrics: overs, economy, wickets, maidens
- Extras: wides, noballs
- Milestones: 3-wicket, 5-wicket hauls
- File: `db/medallion_schema.sql`

### 2. ETL Pipelines

#### Pipeline 1: Bronze → Silver (Metadata)

**File:** `etl/bronze_to_silver_metadata.py`

**Functions:**
- `parse_toss()` - Extract toss winner and decision
- `parse_umpires()` - Split umpire names
- `parse_match_date()` - Convert to date type
- `parse_t20_debuts()` - Create array of debut players
- `parse_player_replacements()` - Extract replacement data
- `transform_metadata()` - Main ETL orchestration

**Process:**
1. Read from `raw_match_metadata`
2. Apply parsing functions to each row
3. Create silver_metadata_df with cleaned data
4. Upsert to `silver_match_metadata` (ON CONFLICT UPDATE)
5. Insert normalized replacements to `silver_player_replacements`

**Data Quality:**
- Handles NULL/missing values
- Validates date formats
- Type conversions with fallbacks

#### Pipeline 2: Bronze → Silver (Events)

**File:** `etl/bronze_to_silver_events.py`

**Functions:**
- `parse_ball_notation()` - Extract over and ball numbers
- `extract_runs_and_extras()` - Parse event for runs/extras
- `extract_wicket_info()` - Identify wickets and types
- `extract_cumulative_score()` - Parse score string
- `transform_events()` - Main ETL orchestration

**Process:**
1. Read from `raw_match_events`
2. Load innings mapping from `silver_match_metadata`
3. Parse each ball event
4. Calculate derived fields (wickets, runs, extras)
5. Delete existing events for matches (idempotency)
6. Insert enriched events to `silver_match_events`

**Enrichment:**
- Wicket detection and classification
- Boundary identification (4s, 6s)
- Extras categorization (wide, noball, bye, legbye)
- Cumulative score tracking
- Innings number mapping

#### Pipeline 3: Silver → Gold (Analytics)

**File:** `etl/silver_to_gold.py`

**Functions:**
- `calculate_innings_summary()` - Aggregate innings stats
- `calculate_batting_stats()` - Per-player batting metrics
- `calculate_bowling_stats()` - Per-player bowling metrics
- `calculate_match_summary()` - Match-level aggregates
- `transform_to_gold()` - Main ETL orchestration

**Process:**
1. Load all matches from `silver_match_metadata`
2. For each match:
   - Calculate innings summaries (1st and 2nd)
   - Calculate match summary (winner, margin, totals)
   - Aggregate batting stats for all batsmen
   - Aggregate bowling stats for all bowlers
3. Upsert to respective gold tables
4. Analytics views auto-update

**Calculations:**
- **Strike Rate:** (Runs / Balls) × 100
- **Economy Rate:** (Runs Conceded / Balls) × 6
- **Run Rate:** (Total Runs / Total Balls) × 6
- **Overs:** Balls ÷ 6 (with decimal notation)
- **Powerplay Stats:** First 6 overs aggregation

### 3. Orchestration

**File:** `run_etl_pipeline.py`

**Command-Line Interface:**
```bash
# Initialize schema
python run_etl_pipeline.py --init

# Run full pipeline
python run_etl_pipeline.py

# Run Bronze → Silver only
python run_etl_pipeline.py --bronze

# Run Silver → Gold only
python run_etl_pipeline.py --silver
```

**Features:**
- Modular execution (run individual stages)
- Progress tracking with banners
- Error handling and reporting
- Success/failure exit codes

### 4. Data Quality Checks

**Bronze Layer:**
- No quality checks (preserve raw data)
- Primary keys only

**Silver Layer:**
- Type validation (dates, integers)
- NULL handling
- Referential integrity (foreign keys)
- Unique constraints (match_id)
- Timestamp tracking (created_at, updated_at)

**Gold Layer:**
- Aggregate validation
- Derived metric calculations
- Business logic enforcement

## Usage Workflow

### Initial Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure database (.env file)
DB_NAME=cricinfo_db
DB_USER=postgres
DB_PASSWORD=yourpassword
DB_HOST=localhost
DB_PORT=5432

# 3. Initialize database schema
python main.py  # Creates Bronze tables
python run_etl_pipeline.py --init  # Creates Silver & Gold tables
```

### Regular Operations

```bash
# 1. Scrape new matches (populates Bronze)
python main.py

# 2. Run ETL pipeline (Bronze → Silver → Gold)
python run_etl_pipeline.py

# 3. Query analytics from Gold layer
psql cricinfo_db -c "SELECT * FROM view_top_batsmen LIMIT 10;"
```

### Incremental Updates

```bash
# New matches scraped
python main.py

# Update only transformed layers
python run_etl_pipeline.py --bronze  # Update Silver
python run_etl_pipeline.py --silver  # Update Gold
```

## Performance Considerations

### Indexing Strategy

**Silver Layer:**
- `idx_silver_events_match_id` - Fast match filtering
- `idx_silver_events_bowler` - Player analytics
- `idx_silver_events_batsman` - Player analytics
- `idx_silver_events_innings` - Innings filtering

**Gold Layer:**
- `idx_gold_batting_match` - Match-level lookups
- `idx_gold_batting_player` - Player aggregation
- `idx_gold_bowling_match` - Match-level lookups
- `idx_gold_bowling_player` - Player aggregation
- `idx_gold_summary_season` - Season filtering
- `idx_gold_innings_match` - Match-level lookups

### ETL Optimization

- **Batch Processing:** Process all matches in single transaction
- **Upserts:** ON CONFLICT DO UPDATE for idempotency
- **Parallel Processing:** Independent stages can run concurrently
- **Incremental Loads:** Delete and reload only changed matches

## Analytics Use Cases

### 1. Player Performance

```sql
-- Top batsmen by runs
SELECT player_name, total_runs, avg_strike_rate, fifties, centuries
FROM view_top_batsmen
ORDER BY total_runs DESC
LIMIT 10;

-- Top bowlers by wickets
SELECT player_name, total_wickets, avg_economy, five_wicket_hauls
FROM view_top_bowlers
ORDER BY total_wickets DESC
LIMIT 10;
```

### 2. Team Analysis

```sql
-- Team win/loss record
SELECT team, season, matches_played, wins,
       ROUND(100.0 * wins / matches_played, 2) as win_percentage
FROM view_team_season_performance
WHERE season = 2025
ORDER BY wins DESC;
```

### 3. Match Insights

```sql
-- High-scoring matches
SELECT match_date, venue,
       first_innings_team, first_innings_runs,
       second_innings_team, second_innings_runs,
       total_runs
FROM gold_match_summary
ORDER BY total_runs DESC
LIMIT 10;

-- Close matches (small margins)
SELECT match_date, first_innings_team, second_innings_team,
       winner, margin
FROM gold_match_summary
WHERE margin LIKE '%1 run%' OR margin LIKE '%1 wicket%'
ORDER BY match_date DESC;
```

### 4. Innings Statistics

```sql
-- Powerplay analysis
SELECT team, AVG(powerplay_runs) as avg_pp_runs,
       AVG(powerplay_wickets) as avg_pp_wickets
FROM gold_innings_summary
GROUP BY team
ORDER BY avg_pp_runs DESC;

-- Run rate comparison
SELECT team, AVG(run_rate) as avg_run_rate,
       AVG(total_runs) as avg_total
FROM gold_innings_summary
GROUP BY team
ORDER BY avg_run_rate DESC;
```

## Maintenance

### Schema Updates

When adding new fields:
1. Update `db/medallion_schema.sql`
2. Run migration: `python run_etl_pipeline.py --init`
3. Update ETL scripts to populate new fields
4. Re-run ETL: `python run_etl_pipeline.py`

### Data Refresh

**Full Refresh:**
```sql
-- Clear all transformed data
TRUNCATE silver_match_metadata CASCADE;
TRUNCATE silver_match_events CASCADE;
TRUNCATE gold_match_summary CASCADE;
-- Re-run ETL
python run_etl_pipeline.py
```

**Incremental Refresh:**
```bash
# ETL automatically handles upserts
python run_etl_pipeline.py
```

## Monitoring & Validation

### Data Counts

```sql
-- Bronze layer
SELECT 'raw_metadata' as table_name, COUNT(*) FROM raw_match_metadata
UNION ALL
SELECT 'raw_events', COUNT(*) FROM raw_match_events;

-- Silver layer
SELECT 'silver_metadata' as table_name, COUNT(*) FROM silver_match_metadata
UNION ALL
SELECT 'silver_events', COUNT(*) FROM silver_match_events;

-- Gold layer
SELECT 'gold_summary' as table_name, COUNT(*) FROM gold_match_summary
UNION ALL
SELECT 'gold_batting', COUNT(*) FROM gold_player_batting_stats
UNION ALL
SELECT 'gold_bowling', COUNT(*) FROM gold_player_bowling_stats;
```

### Data Quality Checks

```sql
-- Check for NULL critical fields
SELECT COUNT(*) as null_match_dates
FROM silver_match_metadata
WHERE match_date IS NULL;

-- Check for orphaned records
SELECT COUNT(*) as orphaned_events
FROM silver_match_events e
WHERE NOT EXISTS (
    SELECT 1 FROM silver_match_metadata m
    WHERE m.match_id = e.match_id
);

-- Validate aggregations
SELECT m.match_id,
       m.first_innings_runs,
       (SELECT MAX(total_runs) FROM silver_match_events
        WHERE match_id = m.match_id AND innings_number = 1) as calculated_runs
FROM gold_match_summary m
WHERE m.first_innings_runs !=
      (SELECT MAX(total_runs) FROM silver_match_events
       WHERE match_id = m.match_id AND innings_number = 1);
```

## Benefits of Medallion Architecture

1. **Data Quality:** Progressive refinement from raw to curated to analytics-ready
2. **Auditability:** Immutable raw data for compliance and debugging
3. **Performance:** Optimized schemas per layer (normalized vs denormalized)
4. **Flexibility:** Multiple consumption patterns (raw analysis, curated queries, dashboards)
5. **Maintainability:** Clear separation of concerns (ingestion, transformation, consumption)
6. **Scalability:** Layer-specific optimization strategies
7. **Reliability:** Idempotent ETL with upsert support

## Conclusion

This implementation provides a production-ready data lakehouse architecture for cricket analytics, following industry best practices for data engineering and enabling sophisticated analytics on IPL match data.
