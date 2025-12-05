"""
Integrated Cricket Data Pipeline
- Scrapes data from Cricinfo → Raw Schema
- Processes through Medallion Architecture: Raw → Bronze → Silver → Gold
"""
from scraping.schedule_scraper import ScheduleScraper
from scraping.match_scraper import MatchScraper
from utils.match_tracker import MatchTracker
from configs.db_config import initialize_database, get_connection, initialize_medallion_schema
from pipeline.orchestrator import PipelineOrchestrator
from utils.logger import setup_logger
from selenium.common.exceptions import TimeoutException, WebDriverException
import yaml
import re
import time
from pathlib import Path

logger = setup_logger(__name__)


def load_config(config_path: str = 'configs/config.yaml') -> dict:
    """Load configuration from YAML file"""
    if not Path(config_path).exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    return config


def extract_match_id(url):
    """Extract match ID from Cricinfo URL"""
    match = re.search(r'-(\d+)/full-scorecard', url)
    return match.group(1) if match else None


def scrape_cricket_data():
    """
    Phase 1: Scrape cricket data from Cricinfo and store in Raw schema

    Returns:
        dict: Scraping results with statistics
    """
    print("\n" + "=" * 80)
    print("PHASE 1: DATA SCRAPING (Cricinfo → Raw Schema)")
    print("=" * 80)

    results = {
        'total_found': 0,
        'downloaded': 0,
        'skipped': 0,
        'failed': 0,
        'status': 'success'
    }

    # Initialize tracker
    print("\nInitializing match tracker...")
    logger.info("Initializing match tracker")

    try:
        tracker = MatchTracker()
    except Exception as e:
        print(f"Failed to initialize tracker: {e}")
        logger.error(f"Failed to initialize tracker: {e}")
        results['status'] = 'failed'
        return results

    # Show current statistics
    current_count = tracker.count()
    print(f"Current Status: {current_count} matches already downloaded")
    logger.info(f"Current Status: {current_count} matches already downloaded")

    # Schedule scraper
    schedule_url = "https://www.espncricinfo.com/series/ipl-2025-1449924/match-schedule-fixtures-and-results"

    print(f"\nFetching match schedule from Cricinfo...")
    logger.info("Fetching match schedule from Cricinfo")

    try:
        # Create schedule scraper with increased timeout
        schedule_scraper = ScheduleScraper(
            url=schedule_url,
            page_load_timeout=180,  # 3 minutes for schedule page
            max_retries=3
        )
        match_links = schedule_scraper.fetch_hrefs()
    except Exception as e:
        print(f"Failed to fetch schedule: {e}")
        logger.error(f"Failed to fetch schedule: {e}")
        results['status'] = 'failed'
        return results

    print(f"Found {len(match_links)} total links")
    logger.info(f"Found {len(match_links)} total links")

    # Filter for full scorecards (limited to 3 for testing - remove limit for production)
    scorecard_links = []
    count = 0
    for url in match_links:
        if "full-scorecard" in url and "ipl-2025" in url:
            scorecard_links.append(url)
            count += 1
            if count >= 1:  # REMOVE THIS LIMIT FOR PRODUCTION
                break

    print(f"Found {len(scorecard_links)} match scorecards")
    logger.info(f"Found {len(scorecard_links)} match scorecards")
    results['total_found'] = len(scorecard_links)

    if len(scorecard_links) == 0:
        print("No matches found to scrape")
        logger.warning("No matches found to scrape")
        return results

    # Optional: Load cache for better performance
    if len(scorecard_links) > 20:
        print("Loading match cache for faster lookups...")
        logger.info("Loading match cache")
        tracker.load_cache()

    print("\n" + "-" * 80)
    print("SCRAPING MATCHES")
    print("-" * 80 + "\n")
    logger.info("Starting match scraping process")

    # Process each match
    for idx, url in enumerate(scorecard_links, 1):
        print(f"\n[{idx}/{len(scorecard_links)}] Processing: {url}")
        logger.info(f"Processing match {idx}/{len(scorecard_links)}: {url}")

        # Extract match ID
        match_id = extract_match_id(url)
        if not match_id:
            print(f"  Could not extract match ID")
            logger.warning(f"Could not extract match ID from URL: {url}")
            results['failed'] += 1
            continue

        print(f"  Match ID: {match_id}")
        logger.debug(f"Match ID: {match_id}")

        # Check if already downloaded
        if tracker.exists(match_id):
            print(f"  Already downloaded - skipping")
            logger.info(f"Match {match_id} already downloaded - skipping")
            results['skipped'] += 1
            continue

        # Scrape the match
        try:
            print(f"  Downloading...")
            logger.info(f"Downloading match {match_id}")

            # Create match scraper with INCREASED timeout (5 minutes)
            match_scraper = MatchScraper(
                url=url,
                base_dir="data",
                page_load_timeout=300,  # 5 MINUTES timeout
                max_retries=3  # 3 retry attempts
            )
            match_scraper.scrape(match_id)

            # Track successful download
            tracker.add(
                match_id=match_id,
                source_url=url,
                status='completed'
            )

            results['downloaded'] += 1
            print(f"  ✓ Successfully downloaded")
            logger.info(f"Successfully downloaded match {match_id}")

            # Add delay between matches to avoid rate limiting
            if idx < len(scorecard_links):
                delay = 10  # 10 seconds between matches
                print(f"  ⏸  Waiting {delay}s before next match...")
                time.sleep(delay)

        except TimeoutException as e:
            error_msg = f"Timeout after multiple retries: {str(e)[:200]}"
            print(f"  ✗ {error_msg}")
            logger.error(f"Timeout for match {match_id}: {e}")

            tracker.mark_failed(
                match_id=match_id,
                error_message=error_msg,
                source_url=url
            )
            results['failed'] += 1

        except WebDriverException as e:
            error_msg = f"WebDriver error: {str(e)[:200]}"
            print(f"  ✗ {error_msg}")
            logger.error(f"WebDriver error for match {match_id}: {e}")

            tracker.mark_failed(
                match_id=match_id,
                error_message=error_msg,
                source_url=url
            )
            results['failed'] += 1

        except Exception as e:
            error_msg = f"Error: {str(e)[:200]}"
            print(f"  ✗ {error_msg}")
            logger.error(f"Error scraping match {match_id}: {e}")

            tracker.mark_failed(
                match_id=match_id,
                error_message=error_msg,
                source_url=url
            )
            results['failed'] += 1

    # Clear cache if loaded
    if len(scorecard_links) > 20:
        tracker.clear_cache()

    # Print scraping summary
    print("\n" + "=" * 80)
    print("SCRAPING SUMMARY")
    print("=" * 80)
    print(f"Total Matches Found:  {results['total_found']}")
    print(f"Downloaded (New):     {results['downloaded']}")
    print(f"Skipped (Existing):   {results['skipped']}")
    print(f"Failed:               {results['failed']}")
    print("=" * 80)

    logger.info(f"Scraping complete - Found: {results['total_found']}, "
                f"Downloaded: {results['downloaded']}, "
                f"Skipped: {results['skipped']}, "
                f"Failed: {results['failed']}")

    # Show tracker statistics
    tracker.print_statistics()

    # Show failed matches if any
    if results['failed'] > 0:
        print("\nFAILED MATCHES:")
        print("-" * 80)
        logger.warning(f"{results['failed']} matches failed to download")

        failed_matches = tracker.get_failed_matches()
        for match in failed_matches[:5]:
            print(f"  Match ID: {match['match_id']}")
            print(f"  Error: {match['error_message'][:100]}...")
            print(f"  URL: {match['source_url']}")
            print("-" * 80)
            logger.error(f"Failed match {match['match_id']}: {match['error_message']}")

        if len(failed_matches) > 5:
            print(f"  ... and {len(failed_matches) - 5} more")

    return results


def run_medallion_pipeline(config):
    """
    Phase 2: Run Medallion Architecture Pipeline
    Raw → Bronze → Silver → Gold

    Args:
        config: Configuration dictionary

    Returns:
        dict: Pipeline execution results
    """
    print("\n" + "=" * 80)
    print("PHASE 2: MEDALLION ARCHITECTURE (Raw → Bronze → Silver → Gold)")
    print("=" * 80)
    logger.info("Starting Medallion Architecture pipeline")

    # Run pipeline
    orchestrator = PipelineOrchestrator(config)
    results = orchestrator.run(layers=['bronze', 'silver', 'gold'])

    return results


def main():
    """
    Main execution function - Integrated Cricket Data Pipeline

    Workflow:
    1. Initialize database and schemas
    2. Scrape data from Cricinfo → Raw Schema
    3. Process through Medallion Architecture → Bronze → Silver → Gold
    """

    print("\n" + "=" * 80)
    print("CRICKET DATA PIPELINE - INTEGRATED SYSTEM")
    print("=" * 80)
    print("Workflow: Cricinfo → Raw → Bronze → Silver → Gold")
    print("=" * 80 + "\n")
    logger.info("Cricket Data Pipeline Started")

    try:
        # ================================================================
        # STEP 1: Initialize Database & Schemas
        # ================================================================
        print("STEP 1: Initializing database and schemas...")
        print("-" * 80)
        logger.info("STEP 1: Initializing database and schemas")

        # Initialize raw schema
        initialize_database()

        # Initialize medallion schemas
        initialize_medallion_schema()

        print("Database initialization complete\n")
        logger.info("Database initialization complete")

        # ================================================================
        # STEP 2: Load Configuration
        # ================================================================
        print("STEP 2: Loading configuration...")
        print("-" * 80)
        logger.info("STEP 2: Loading configuration")

        config = load_config('configs/config.yaml')
        print("Configuration loaded\n")
        logger.info("Configuration loaded successfully")

        # ================================================================
        # STEP 3: Scrape Data (Cricinfo → Raw Schema)
        # ================================================================
        print("STEP 3: Scraping data from Cricinfo...")
        print("-" * 80)
        logger.info("STEP 3: Scraping data from Cricinfo")

        scraping_results = scrape_cricket_data()

        if scraping_results['status'] == 'failed':
            print("\nScraping failed. Aborting pipeline.")
            logger.error("Scraping failed - aborting pipeline")
            return 1

        # Check if we have any data to process
        total_data = scraping_results['downloaded'] + scraping_results['skipped']
        if total_data == 0:
            print("\nNo data available to process. Exiting.")
            logger.warning("No data available to process")
            return 0

        # ================================================================
        # STEP 4: Run Medallion Pipeline (Raw → Bronze → Silver → Gold)
        # ================================================================
        print("\nSTEP 4: Processing through Medallion Architecture...")
        print("-" * 80)
        logger.info("STEP 4: Processing through Medallion Architecture")

        pipeline_results = run_medallion_pipeline(config)

        # ================================================================
        # STEP 5: Final Summary
        # ================================================================
        print("\n" + "=" * 80)
        print("FINAL PIPELINE SUMMARY")
        print("=" * 80)

        # Scraping Summary
        print("\nSCRAPING PHASE:")
        print(f"  Total Matches Found:  {scraping_results['total_found']}")
        print(f"  Downloaded (New):     {scraping_results['downloaded']}")
        print(f"  Skipped (Existing):   {scraping_results['skipped']}")
        print(f"  Failed:               {scraping_results['failed']}")

        # Pipeline Summary
        print("\nPROCESSING PHASE:")
        print(f"  Status: {pipeline_results['status'].upper()}")
        print(f"  Duration: {pipeline_results.get('duration_seconds', 0):.2f} seconds")

        for layer_name, layer_result in pipeline_results.get('layers', {}).items():
            print(f"\n  {layer_name.upper()} Layer:")
            print(f"    Status: {layer_result['status']}")
            if 'tables_processed' in layer_result:
                print(f"    Tables Processed: {layer_result['tables_processed']}")
            if 'aggregations_processed' in layer_result:
                print(f"    Aggregations: {layer_result['aggregations_processed']}")
            if 'total_rows' in layer_result:
                print(f"    Total Rows: {layer_result['total_rows']}")

        print("\n" + "=" * 80)
        print("DATA LOCATIONS")
        print("=" * 80)
        print("Raw Data:     raw.match_metadata, raw.match_events")
        print("Bronze Layer: bronze.match_metadata, bronze.match_events")
        print("Silver Layer: silver.match_metadata, silver.match_events")
        print("Gold Layer:   gold.series_summary, gold.venue_statistics, etc.")
        print("=" * 80)

        # Log final summary
        logger.info(f"Pipeline completed - Status: {pipeline_results['status']}, "
                   f"Duration: {pipeline_results.get('duration_seconds', 0):.2f}s")

        # Determine exit code
        if pipeline_results['status'] == 'success':
            print("\nPipeline completed successfully!")
            logger.info("Pipeline completed successfully")
            return 0
        elif pipeline_results['status'] == 'partial_success':
            print("\nPipeline completed with some errors")
            logger.warning("Pipeline completed with some errors")
            return 1
        else:
            print("\nPipeline failed")
            logger.error("Pipeline failed")
            return 1

    except FileNotFoundError as e:
        print(f"\nError: {e}")
        print("Please ensure all required files exist:")
        print("  - configs/config.yaml")
        print("  - db/schema.sql")
        print("  - db/medallion_schema.sql")
        logger.error(f"File not found: {e}")
        return 1

    except Exception as e:
        print(f"\nFatal error: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)