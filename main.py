"""
Integrated Cricket Data Pipeline
- Scrapes data from Cricinfo → Raw Schema
- Processes through Medallion Architecture: Raw → Bronze → Silver → Gold
- Features: Automatic retry on connection failures + Smart driver cleanup
"""
from scraping.schedule_scraper import ScheduleScraper
from scraping.match_scraper import MatchScraper
from utils.match_tracker import MatchTracker
from configs.db_config import initialize_database, get_connection, initialize_medallion_schema
from pipeline.orchestrator import PipelineOrchestrator
from utils.logger import setup_logger
from selenium.common.exceptions import TimeoutException, WebDriverException
from urllib3.exceptions import MaxRetryError, NewConnectionError
from requests.exceptions import ConnectionError, HTTPError, Timeout
import yaml
import re
import time
from pathlib import Path

logger = setup_logger(__name__)

# ============================================================================
# RETRY CONFIGURATION
# ============================================================================
MAX_RETRIES = 3
RETRY_DELAY_BASE = 30  # Base delay in seconds (exponential backoff)
MATCH_DELAY = 10  # Delay between successful matches


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

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


def is_connection_error(exception):
    """
    Check if exception is a connection-related error that should trigger retry

    Args:
        exception: The exception to check

    Returns:
        bool: True if this is a retryable connection error
    """
    retryable_errors = (
        ConnectionError,
        HTTPError,
        Timeout,
        MaxRetryError,
        NewConnectionError,
        TimeoutException,
        WebDriverException
    )

    if isinstance(exception, retryable_errors):
        return True

    # Check error message for connection-related keywords
    error_msg = str(exception).lower()
    connection_keywords = [
        'connection',
        'timeout',
        'timed out',
        'network',
        'http',
        'refused',
        'unreachable',
        'failed to establish',
        'cannot connect',
        'connection reset',
        'connection aborted',
        'remotedisconnected',
        'broken pipe',
        'connection pool'
    ]

    return any(keyword in error_msg for keyword in connection_keywords)


def cleanup_driver(scraper_instance):
    """
    Safely cleanup and close WebDriver
    Only attempts cleanup if driver is actually present and active
    Handles multiple scraper implementations gracefully

    Args:
        scraper_instance: Scraper instance (MatchScraper or ScheduleScraper)
    """
    if not scraper_instance:
        logger.debug("No scraper instance to cleanup")
        return

    scraper_type = type(scraper_instance).__name__

    # Strategy 1: Use built-in close/cleanup method
    for method_name in ['close', 'cleanup', 'quit', 'teardown']:
        if hasattr(scraper_instance, method_name):
            method = getattr(scraper_instance, method_name)
            if callable(method):
                try:
                    method()
                    logger.debug(f"Driver cleaned up via {scraper_type}.{method_name}()")
                    return
                except Exception as e:
                    logger.debug(f"{scraper_type}.{method_name}() failed: {e}")

    # Strategy 2: Direct driver access
    for driver_attr in ['driver', '_driver', 'webdriver', '_webdriver']:
        if hasattr(scraper_instance, driver_attr):
            try:
                driver = getattr(scraper_instance, driver_attr)
                if driver is not None:
                    # Check if driver is still active
                    try:
                        _ = driver.current_url  # Test if driver is alive
                        driver.quit()
                        logger.debug(f"Driver closed via {scraper_type}.{driver_attr}.quit()")

                        # Set to None to prevent double cleanup
                        setattr(scraper_instance, driver_attr, None)
                        return
                    except Exception:
                        # Driver already closed or inactive
                        logger.debug(f"Driver at {driver_attr} already inactive")
                        return
            except AttributeError:
                continue
            except Exception as e:
                logger.debug(f"Error accessing {driver_attr}: {e}")

    # No active driver found - this is normal if scraper cleaned up internally
    logger.debug(f"No active driver found in {scraper_type} - likely already cleaned up")


# ============================================================================
# SCRAPING FUNCTIONS
# ============================================================================

def scrape_match_with_retry(url, match_id, tracker, max_retries=MAX_RETRIES):
    """
    Scrape a single match with retry logic for connection failures
    Ensures WebDriver is properly closed before each retry

    Args:
        url: Match URL
        match_id: Match ID
        tracker: MatchTracker instance
        max_retries: Maximum number of retry attempts

    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    retry_count = 0
    match_scraper = None

    while retry_count <= max_retries:
        try:
            logger.info(f"Attempting to download match {match_id} (attempt {retry_count + 1}/{max_retries + 1})")

            # Create match scraper with timeout
            match_scraper = MatchScraper(
                url=url,
                base_dir="data",
                page_load_timeout=300,  # 5 minutes timeout
                max_retries=3  # Internal retries within MatchScraper
            )

            # Scrape the match
            match_scraper.scrape(match_id)

            # Track successful download
            tracker.add(
                match_id=match_id,
                source_url=url,
                status='completed'
            )

            logger.info(f"Successfully downloaded match {match_id}")

            # Close driver after successful scrape
            cleanup_driver(match_scraper)
            match_scraper = None

            return True, None

        except Exception as e:
            error_msg = str(e)

            # CRITICAL: Close the driver before retry
            if match_scraper:
                logger.debug(f"Cleaning up driver for match {match_id} after error")
                cleanup_driver(match_scraper)
                match_scraper = None

            # Check if this is a retryable connection error
            if is_connection_error(e) and retry_count < max_retries:
                retry_count += 1
                retry_delay = RETRY_DELAY_BASE * (2 ** (retry_count - 1))  # Exponential backoff

                print(f"  ⚠ Connection error on attempt {retry_count}/{max_retries + 1}")
                print(f"  Error: {error_msg[:150]}")
                print(f"  🔄 Driver closed, retrying in {retry_delay} seconds...")
                logger.warning(f"Connection error for match {match_id} (attempt {retry_count}): {error_msg[:200]}")
                logger.info(f"Retrying match {match_id} in {retry_delay} seconds...")

                time.sleep(retry_delay)
                continue
            else:
                # Non-retryable error or max retries exceeded
                if retry_count >= max_retries:
                    error_msg = f"Failed after {max_retries + 1} attempts: {error_msg[:200]}"
                    logger.error(f"Max retries exceeded for match {match_id}: {error_msg}")
                else:
                    logger.error(f"Non-retryable error for match {match_id}: {error_msg}")

                return False, error_msg

        finally:
            # Final cleanup - only if scraper still exists
            if match_scraper:
                logger.debug("Final cleanup in finally block")
                cleanup_driver(match_scraper)
                match_scraper = None

    return False, f"Failed after {max_retries + 1} attempts"


def scrape_cricket_data():
    """
    Phase 1: Scrape cricket data from Cricinfo and store in Raw schema
    With robust retry logic for HTTP connection failures and smart driver cleanup

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

    # ========================================================================
    # Initialize Match Tracker
    # ========================================================================
    print("\nInitializing match tracker...")
    logger.info("Initializing match tracker")

    try:
        tracker = MatchTracker()
    except Exception as e:
        print(f"Failed to initialize tracker: {e}")
        logger.error(f"Failed to initialize tracker: {e}")
        results['status'] = 'failed'
        return results

    current_count = tracker.count()
    print(f"Current Status: {current_count} matches already downloaded")
    logger.info(f"Current Status: {current_count} matches already downloaded")

    # ========================================================================
    # Fetch Match Schedule with Retry Logic
    # ========================================================================
    schedule_url = "https://www.espncricinfo.com/series/ipl-2025-1449924/match-schedule-fixtures-and-results"

    print(f"\nFetching match schedule from Cricinfo...")
    logger.info("Fetching match schedule from Cricinfo")

    schedule_retry_count = 0
    schedule_max_retries = 3
    match_links = []
    schedule_scraper = None

    while schedule_retry_count <= schedule_max_retries:
        try:
            # Create schedule scraper with increased timeout
            schedule_scraper = ScheduleScraper(
                url=schedule_url,
                page_load_timeout=180,  # 3 minutes for schedule page
                max_retries=3
            )
            match_links = schedule_scraper.fetch_hrefs()

            # Cleanup schedule scraper
            cleanup_driver(schedule_scraper)
            schedule_scraper = None

            break  # Success - exit retry loop

        except Exception as e:
            # Cleanup before retry
            cleanup_driver(schedule_scraper)
            schedule_scraper = None

            if is_connection_error(e) and schedule_retry_count < schedule_max_retries:
                schedule_retry_count += 1
                retry_delay = RETRY_DELAY_BASE * (2 ** (schedule_retry_count - 1))

                print(f"⚠ Failed to fetch schedule (attempt {schedule_retry_count}/{schedule_max_retries + 1})")
                print(f"Error: {str(e)[:150]}")
                print(f"🔄 Driver closed, retrying in {retry_delay} seconds...")
                logger.warning(f"Schedule fetch failed (attempt {schedule_retry_count}): {e}")

                time.sleep(retry_delay)
                continue
            else:
                print(f"✗ Failed to fetch schedule after {schedule_max_retries + 1} attempts: {e}")
                logger.error(f"Failed to fetch schedule: {e}")
                results['status'] = 'failed'
                return results
        finally:
            # Final cleanup
            if schedule_scraper:
                cleanup_driver(schedule_scraper)

    print(f"Found {len(match_links)} total links")
    logger.info(f"Found {len(match_links)} total links")

    # ========================================================================
    # Filter Match Links
    # ========================================================================
    scorecard_links = []
    count = 0

    for url in match_links:
        if "full-scorecard" in url and "ipl-2025" in url:
            scorecard_links.append(url)
            count += 1
            
            # REMOVE THIS LIMIT FOR PRODUCTION
            #if count >= 1:
                #break
            

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

    # ========================================================================
    # Scrape Each Match
    # ========================================================================
    print("\n" + "-" * 80)
    print("SCRAPING MATCHES (with automatic retry & driver cleanup)")
    print("-" * 80 + "\n")
    logger.info("Starting match scraping process with retry logic")

    for idx, url in enumerate(scorecard_links, 1):
        print(f"\n[{idx}/{len(scorecard_links)}] Processing: {url}")
        logger.info(f"Processing match {idx}/{len(scorecard_links)}: {url}")

        # Extract match ID
        match_id = extract_match_id(url)
        if not match_id:
            print(f"  ✗ Could not extract match ID")
            logger.warning(f"Could not extract match ID from URL: {url}")
            results['failed'] += 1
            continue

        print(f"  Match ID: {match_id}")
        logger.debug(f"Match ID: {match_id}")

        # Check if already downloaded
        if tracker.exists(match_id):
            print(f"  ℹ Already downloaded - skipping")
            logger.info(f"Match {match_id} already downloaded - skipping")
            results['skipped'] += 1
            continue

        # Scrape the match with retry logic
        print(f"  📥 Downloading...")
        success, error_msg = scrape_match_with_retry(url, match_id, tracker, max_retries=MAX_RETRIES)

        if success:
            results['downloaded'] += 1
            print(f"  ✓ Successfully downloaded")

            # Add delay between matches to avoid rate limiting
            if idx < len(scorecard_links):
                print(f"  ⏸ Waiting {MATCH_DELAY}s before next match...")
                time.sleep(MATCH_DELAY)
        else:
            # Track failed match
            tracker.mark_failed(
                match_id=match_id,
                error_message=error_msg,
                source_url=url
            )
            results['failed'] += 1
            print(f"  ✗ Failed: {error_msg[:100]}")

    # Clear cache if loaded
    if len(scorecard_links) > 20:
        tracker.clear_cache()

    # ========================================================================
    # Print Scraping Summary
    # ========================================================================
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


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function - Integrated Cricket Data Pipeline

    Workflow:
    1. Initialize database and schemas
    2. Scrape data from Cricinfo → Raw Schema (with retry logic & driver cleanup)
    3. Process through Medallion Architecture → Bronze → Silver → Gold
    """

    print("\n" + "=" * 80)
    print("CRICKET DATA PIPELINE - INTEGRATED SYSTEM")
    print("=" * 80)
    print("Workflow: Cricinfo → Raw → Bronze → Silver → Gold")
    print("Features: Automatic retry + Smart driver cleanup")
    print("=" * 80 + "\n")
    logger.info("Cricket Data Pipeline Started")

    try:
        # ====================================================================
        # STEP 1: Initialize Database & Schemas
        # ====================================================================
        print("STEP 1: Initializing database and schemas...")
        print("-" * 80)
        logger.info("STEP 1: Initializing database and schemas")

        # Initialize raw schema
        initialize_database()

        # Initialize medallion schemas
        initialize_medallion_schema()

        print("✓ Database initialization complete\n")
        logger.info("Database initialization complete")

        # ====================================================================
        # STEP 2: Load Configuration
        # ====================================================================
        print("STEP 2: Loading configuration...")
        print("-" * 80)
        logger.info("STEP 2: Loading configuration")

        config = load_config('configs/config.yaml')
        print("✓ Configuration loaded\n")
        logger.info("Configuration loaded successfully")

        # ====================================================================
        # STEP 3: Scrape Data (Cricinfo → Raw Schema)
        # ====================================================================
        print("STEP 3: Scraping data from Cricinfo...")
        print("-" * 80)
        logger.info("STEP 3: Scraping data from Cricinfo")

        scraping_results = scrape_cricket_data()

        if scraping_results['status'] == 'failed':
            print("\n✗ Scraping failed. Aborting pipeline.")
            logger.error("Scraping failed - aborting pipeline")
            return 1

        # Check if we have any data to process
        total_data = scraping_results['downloaded'] + scraping_results['skipped']
        if total_data == 0:
            print("\n⚠ No data available to process. Exiting.")
            logger.warning("No data available to process")
            return 0

        # ====================================================================
        # STEP 4: Run Medallion Pipeline (Raw → Bronze → Silver → Gold)
        # ====================================================================
        print("\nSTEP 4: Processing through Medallion Architecture...")
        print("-" * 80)
        logger.info("STEP 4: Processing through Medallion Architecture")

        pipeline_results = run_medallion_pipeline(config)

        # ====================================================================
        # STEP 5: Final Summary
        # ====================================================================
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
        print("Bronze Layer: bronze.match_metadata, bronze.match_events, bronze.match_players")
        print("Silver Layer: silver.match_metadata, silver.match_events, silver.match_players")
        print("Gold Layer:   gold.series_summary, gold.match_summary, gold.player_team_history, etc.")
        print("=" * 80)

        # Log final summary
        logger.info(f"Pipeline completed - Status: {pipeline_results['status']}, "
                   f"Duration: {pipeline_results.get('duration_seconds', 0):.2f}s")

        # Determine exit code
        if pipeline_results['status'] == 'success':
            print("\n✓ Pipeline completed successfully!")
            logger.info("Pipeline completed successfully")
            return 0
        elif pipeline_results['status'] == 'partial_success':
            print("\n⚠ Pipeline completed with some errors")
            logger.warning("Pipeline completed with some errors")
            return 1
        else:
            print("\n✗ Pipeline failed")
            logger.error("Pipeline failed")
            return 1

    except FileNotFoundError as e:
        print(f"\n✗ Error: {e}")
        print("Please ensure all required files exist:")
        print("  - configs/config.yaml")
        print("  - db/schema.sql")
        print("  - db/medallion_schema.sql")
        logger.error(f"File not found: {e}")
        return 1

    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    import sys
    exit_code = main()
    sys.exit(exit_code)