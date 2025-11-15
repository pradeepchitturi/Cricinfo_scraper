# Cricinfo Scraper 🏏

This project scrapes ball-by-ball commentary and metadata for IPL 2025 matches from ESPN Cricinfo and implements a **Medallion Architecture** (Bronze, Silver, Gold layers) for data processing and analytics.

## Architecture Overview

This project implements a **3-tier Medallion Architecture** for data lakehouse:

```
┌─────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                      │
└─────────────────────────────────────────────────────────────────┘

📊 BRONZE LAYER (Raw Data)
   ├─ raw_match_metadata      → Raw match details as-is from scraper
   └─ raw_match_events        → Raw ball-by-ball commentary

                    ↓ ETL: Data Cleaning & Enrichment ↓

🔹 SILVER LAYER (Cleaned & Enriched)
   ├─ silver_match_metadata      → Normalized match information
   ├─ silver_player_replacements → Player replacement tracking
   └─ silver_match_events        → Enriched events with parsed fields

                    ↓ ETL: Aggregation & Analytics ↓

🥇 GOLD LAYER (Business Analytics)
   ├─ gold_match_summary           → Match-level aggregates
   ├─ gold_innings_summary         → Innings-level statistics
   ├─ gold_player_batting_stats    → Batting performance metrics
   └─ gold_player_bowling_stats    → Bowling performance metrics
```

## Project Structure

```
Cricinfo_scraper/
├── core/                    # Core scraping modules
│   ├── driver_manager.py
│   ├── page_navigator.py
│   ├── metadata_extractor.py
│   ├── commentary_parser.py
│   └── file_manager.py
├── scraping/               # Scrapers
│   ├── schedule_scraper.py
│   └── match_scraper.py
├── etl/                    # ETL Pipeline (NEW!)
│   ├── bronze_to_silver_metadata.py
│   ├── bronze_to_silver_events.py
│   ├── silver_to_gold.py
│   └── __init__.py
├── db/                     # Database schemas
│   ├── schema.sql          # Bronze layer tables
│   └── medallion_schema.sql # Silver & Gold layer tables
├── configs/                # Configuration
│   ├── settings.py
│   └── db_config.py
├── utils/                  # Utilities
│   ├── logger.py
│   └── tracker.py
├── main.py                 # Main scraper orchestrator
└── run_etl_pipeline.py     # ETL pipeline orchestrator (NEW!)
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup PostgreSQL Database

Create a `.env` file with your database credentials:

```env
DB_NAME=cricinfo_db
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

### 3. Initialize Database

```bash
# Initialize Bronze layer (raw tables)
python main.py  # This will auto-initialize on first run

# Initialize Medallion Architecture (Silver & Gold layers)
python run_etl_pipeline.py --init
```

### 4. Run the Scraper

```bash
# Scrape IPL 2025 matches
python main.py
```

This will:
- Fetch match schedule from ESPN Cricinfo
- Scrape ball-by-ball commentary for each match
- Save raw data to PostgreSQL (Bronze layer)
- Track processed matches to avoid duplicates

### 5. Run ETL Pipeline

```bash
# Run full ETL pipeline (Bronze → Silver → Gold)
python run_etl_pipeline.py

# Or run individual stages:
python run_etl_pipeline.py --bronze  # Bronze → Silver only
python run_etl_pipeline.py --silver  # Silver → Gold only
```

## Data Layers Explained

### 🔶 Bronze Layer (Raw Data)

**Purpose:** Store raw, unprocessed data exactly as scraped from the source.

**Tables:**
- `raw_match_metadata`: Match details (venue, toss, umpires, etc.)
- `raw_match_events`: Ball-by-ball commentary and events

**Characteristics:**
- No data validation
- No transformations
- Preserves original data format
- Serves as source of truth

### 🔹 Silver Layer (Cleaned & Enriched)

**Purpose:** Cleaned, validated, and enriched data ready for analytics.

**Tables:**
- `silver_match_metadata`: Normalized match information
  - Parsed toss (winner, decision)
  - Split umpire names
  - Converted dates to proper date type
  - Array of T20 debuts

- `silver_player_replacements`: Normalized player replacement data

- `silver_match_events`: Enriched ball-by-ball events
  - Parsed ball notation (over, ball number)
  - Extracted runs, extras, wicket info
  - Cumulative scores and wickets
  - Event classification (boundary, wicket, etc.)

**Transformations:**
- Data cleaning and validation
- Type conversions
- Parsing complex fields
- Adding derived columns

### 🥇 Gold Layer (Business Analytics)

**Purpose:** Aggregated, business-ready data optimized for analytics and reporting.

**Tables:**
- `gold_match_summary`: Match-level aggregates
  - Final scores for both innings
  - Winner and margin
  - Total runs, wickets, boundaries

- `gold_innings_summary`: Innings-level statistics
  - Run rate, powerplay stats
  - Extras breakdown
  - Scoring pattern (4s, 6s, dots)

- `gold_player_batting_stats`: Batting performance
  - Runs, balls faced, strike rate
  - Boundaries (4s, 6s)
  - Milestones (50s, 100s)

- `gold_player_bowling_stats`: Bowling performance
  - Overs, economy rate
  - Wickets, maidens
  - Extras conceded

**Analytics Views:**
- `view_team_season_performance`: Team statistics by season
- `view_top_batsmen`: Aggregate batting statistics
- `view_top_bowlers`: Aggregate bowling statistics

## ETL Pipeline Details

### Bronze → Silver Transformation

**Metadata ETL** (`bronze_to_silver_metadata.py`):
- Parses toss information (winner, decision)
- Splits umpire names into individual columns
- Converts match dates to proper date format
- Extracts player debuts into array
- Normalizes player replacement data

**Events ETL** (`bronze_to_silver_events.py`):
- Parses ball notation (over.ball → over_number, ball_number)
- Extracts runs scored and extras
- Identifies wickets and wicket types
- Parses fielder information
- Calculates cumulative scores
- Maps innings to teams

### Silver → Gold Transformation

**Analytics ETL** (`silver_to_gold.py`):
- Calculates match summaries and results
- Computes innings-level aggregates
- Generates player batting statistics
- Generates player bowling statistics
- Creates powerplay statistics
- Calculates derived metrics (strike rate, economy, etc.)

## Usage Examples

### Query Gold Layer for Analytics

```sql
-- Top run scorers
SELECT * FROM view_top_batsmen LIMIT 10;

-- Top wicket takers
SELECT * FROM view_top_bowlers LIMIT 10;

-- Match summaries
SELECT
    match_date,
    first_innings_team,
    first_innings_runs,
    second_innings_team,
    second_innings_runs,
    winner,
    margin
FROM gold_match_summary
ORDER BY match_date DESC;

-- Team performance by season
SELECT * FROM view_team_season_performance
WHERE season = 2025
ORDER BY wins DESC;
```

### Run Partial ETL

```bash
# Only transform new matches in Bronze → Silver
python run_etl_pipeline.py --bronze

# Only rebuild Gold layer analytics
python run_etl_pipeline.py --silver
```

## Features

- **Automated Web Scraping**: Selenium-based scraping of ESPN Cricinfo
- **Medallion Architecture**: Industry-standard 3-tier data lakehouse
- **Data Quality**: Cleaning, validation, and enrichment at Silver layer
- **Business Analytics**: Pre-aggregated metrics at Gold layer
- **Incremental Updates**: ETL pipeline handles upserts and updates
- **Resumable Downloads**: Tracks processed matches to avoid duplicates
- **PostgreSQL Storage**: Reliable, scalable database backend

## Requirements

- Python 3.8+
- PostgreSQL 12+
- Chrome browser (for Selenium WebDriver)
- Dependencies in `requirements.txt`:
  - selenium
  - beautifulsoup4
  - pandas
  - psycopg2-binary
  - python-dotenv

## Database Schema

See full schema documentation:
- **Bronze Layer**: `db/schema.sql`
- **Silver & Gold Layers**: `db/medallion_schema.sql`

## Troubleshooting

**Issue: ETL fails with "No data in Bronze layer"**
```bash
# Solution: Run scraper first to populate Bronze layer
python main.py
```

**Issue: "No metadata found in Silver layer"**
```bash
# Solution: Run Bronze→Silver ETL first
python run_etl_pipeline.py --bronze
```

**Issue: Database connection error**
```bash
# Solution: Check .env file has correct credentials
# Ensure PostgreSQL is running
```

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-feature`)
3. Commit changes (`git commit -m 'Add new feature'`)
4. Push to branch (`git push origin feature/new-feature`)
5. Create Pull Request

## License

MIT License - feel free to use for educational purposes.
