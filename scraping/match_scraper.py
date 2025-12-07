"""
Match Scraper - Uses Selenium to scrape Cricinfo match data with retry logic
"""
import os
import re
import time
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from core.driver_manager import DriverManager
from core.page_navigator import PageNavigator
from core.metadata_extractor import MetadataExtractor
from core.commentary_parser import CommentaryParser
from configs.db_config import save_to_db
import json
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from utils.logger import setup_logger
from scraping.player_extractor import PlayerExtractor

logger = setup_logger(__name__)


class MatchScraper:
    def __init__(self, url, base_dir="data", page_load_timeout=300, max_retries=3):
        """
        Initialize Match Scraper with Selenium

        Args:
            url: Match URL
            base_dir: Base directory for data storage
            page_load_timeout: Page load timeout in seconds (default: 300 = 5 minutes)
            max_retries: Maximum retry attempts (default: 3)
        """
        self.url = url
        self.base_dir = base_dir
        self.page_load_timeout = page_load_timeout
        self.max_retries = max_retries
        self.player_extractor = PlayerExtractor(schema='raw')

    def get_folder_name(self, metadata):
        """Generate folder name from metadata"""
        match_date_text = metadata.get("Match days", "")
        date_part = "UnknownDate"
        if match_date_text:
            match_date = re.search(r"(\d{1,2} \w+ \d{4})", match_date_text)
            if match_date:
                parsed_date = datetime.strptime(match_date.group(1), "%d %B %Y")
                date_part = parsed_date.strftime("%Y%m%d")

        path_parts = urlparse(self.url).path.split('/')
        if len(path_parts) > 3:
            match_slug = path_parts[3]
        else:
            match_slug = "match"

        folder_name = f"{date_part}_{match_slug}".replace(" ", "_")
        return folder_name

    def format_date(self, date_str):
        """Format date string to YYYYMMDD"""
        try:
            date_obj = datetime.strptime(date_str, '%d %B %Y')
            return date_obj.strftime('%Y%m%d')
        except Exception:
            return "unknown_date"

    def _navigate_with_retry(self, driver, url, description="page"):
        """
        Navigate to URL with retry logic

        Args:
            driver: Selenium WebDriver instance
            url: URL to navigate to
            description: Description for logging

        Raises:
            Exception: If all retries fail
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"    Loading {description} (attempt {attempt}/{self.max_retries})...")
                logger.info(f"Loading {description} - attempt {attempt}")

                driver.get(url)
                time.sleep(8)  # Wait for page to load and render

                print(f"    ✓ {description} loaded successfully")
                logger.info(f"{description} loaded successfully")
                return

            except TimeoutException as e:
                last_exception = e
                print(f"    ✗ Timeout loading {description} on attempt {attempt}")
                logger.warning(f"Timeout loading {description} - attempt {attempt}")

                if attempt < self.max_retries:
                    wait_time = attempt * 10  # Progressive backoff: 10s, 20s, 30s
                    print(f"    Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)

                    # Try to refresh if timeout
                    try:
                        driver.refresh()
                        time.sleep(5)
                    except:
                        pass

            except WebDriverException as e:
                last_exception = e
                print(f"    ✗ WebDriver error on attempt {attempt}: {str(e)[:100]}")
                logger.error(f"WebDriver error - attempt {attempt}: {e}")

                if attempt < self.max_retries:
                    wait_time = attempt * 8
                    print(f"    Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)

        # All retries failed
        error_msg = f"Failed to load {description} after {self.max_retries} attempts: {last_exception}"
        logger.error(error_msg)
        raise Exception(error_msg)

    def scrape(self, match_id):
        """
        Scrape match data with retry logic and proper error handling

        Args:
            match_id: Unique match identifier
        """
        driver = None
        driver_manager = None

        try:
            # Initialize driver with longer timeout
            print(f"    Initializing WebDriver...")
            logger.info("Initializing WebDriver")

            driver_manager = DriverManager(
                headless=False,
                page_load_timeout=self.page_load_timeout,
                implicit_wait=15
            )
            driver = driver_manager.start_driver()

            # Navigate to full scorecard with retry
            self._navigate_with_retry(driver, self.url, "full scorecard")

            # Initialize page navigator
            page_nav = PageNavigator(driver)

            # Scroll to load all content
            print(f"    Scrolling to load content...")
            logger.info("Scrolling full scorecard page")
            page_nav.scroll_full_page(5)
            time.sleep(3)

            # Extract metadata
            print(f"    Extracting metadata...")
            logger.info("Extracting metadata")
            metadata = MetadataExtractor.extract_metadata(driver.page_source, match_id)

            # Handle player replacements
            pattern = re.compile(r".*Replacement$")
            keys_to_merge = [k for k in metadata if pattern.match(k)]
            metadata["player_replacements"] = json.dumps({k: metadata.pop(k) for k in keys_to_merge})

            #Extracting player names
            logger.info("Extracting player rosters...")
            player_results = self.player_extractor.extract_and_store(
                html_content=driver.page_source,
                match_id=match_id
            )

            if player_results['status'] == 'success':
                logger.info(f"Stored {player_results['total_players']} players")
                for team in player_results['teams']:
                    logger.info(f"  - {team}")
            else:
                logger.warning("Player extraction failed")

            # Navigate to commentary page
            commentary_url = self.url.replace("/full-scorecard", "/ball-by-ball-commentary")
            self._navigate_with_retry(driver, commentary_url, "commentary page")

            # Scroll to load commentary
            print(f"    Scrolling to load commentary...")
            logger.info("Scrolling commentary page")
            page_nav.scroll_full_page()
            time.sleep(3)

            # Extract default innings team
            print(f"    Extracting innings data...")
            logger.info("Extracting first innings data")
            default_team_name = self.get_current_innings_team(driver)
            print(f"    Default batting team: {default_team_name}")
            logger.info(f"Default batting team: {default_team_name}")
            metadata["second_innings"] = default_team_name

            # Save first innings commentary
            innings1_html = driver.page_source
            innings1_data = CommentaryParser.parse_commentary(innings1_html)
            innings1_df = CommentaryParser.to_dataframe(innings1_data)
            innings1_df["Innings"] = default_team_name
            innings1_df["MatchID"] = match_id
            print(f"    Extracted {len(innings1_df)} events from first innings")
            logger.info(f"First innings: {len(innings1_df)} events")

            # Scroll to top before switch
            page_nav.scroll_to_top()
            time.sleep(5)

            # Switch innings
            print(f"    Switching to second innings...")
            logger.info("Switching to second innings")
            switched_team_name = page_nav.click_dropdown_and_switch_innings(default_team_name)
            print(f"    Switched batting team: {switched_team_name}")
            logger.info(f"Switched batting team: {switched_team_name}")
            metadata["first_innings"] = switched_team_name

            # Convert metadata to DataFrame
            metadata_df = pd.DataFrame([metadata])

            # Clean column names
            metadata_df.columns = (
                metadata_df.columns
                .str.replace(r"[ ()]", "_", regex=True)
                .str.replace(r"_+", "_", regex=True)
                .str.strip("_")
                .str.lower()
            )

            # Save metadata to DB
            print(f"Saving metadata to database...")
            logger.info("Saving metadata to database")
            save_to_db("raw", "match_metadata", metadata_df)
            print(f"Metadata saved ({len(metadata_df)} rows)")

            time.sleep(5)
            page_nav.scroll_full_page()
            time.sleep(3)

            # Save second innings commentary
            innings2_html = driver.page_source
            innings2_data = CommentaryParser.parse_commentary(innings2_html)
            innings2_df = CommentaryParser.to_dataframe(innings2_data)
            innings2_df["Innings"] = switched_team_name
            innings2_df["MatchID"] = match_id
            print(f"    Extracted {len(innings2_df)} events from second innings")
            logger.info(f"Second innings: {len(innings2_df)} events")

            # Combine both innings
            final_df = pd.concat([innings1_df, innings2_df], ignore_index=True)

            # Clean column names
            final_df.columns = (
                final_df.columns
                .str.replace(r"[ ()]", "_", regex=True)
                .str.replace(r"_+", "_", regex=True)
                .str.strip("_")
                .str.lower()
            )

            # Save commentary to DB
            print(f"Saving commentary to database...")
            logger.info("Saving commentary to database")
            save_to_db("raw", "match_events", final_df)
            print(f"Commentary saved ({len(final_df)} events)")
            logger.info(f"Commentary saved: {len(final_df)} total events")

            print(f"    ✓ Successfully scraped match {match_id}")
            logger.info(f"Successfully completed scraping match {match_id}")



        except TimeoutException as e:
            error_msg = f"Timeout error after {self.max_retries} retries: {str(e)[:200]}"
            print(f"    ✗ {error_msg}")
            logger.error(f"Timeout scraping match {match_id}: {e}")
            raise Exception(error_msg)

        except WebDriverException as e:
            error_msg = f"WebDriver error: {str(e)[:200]}"
            print(f"    ✗ {error_msg}")
            logger.error(f"WebDriver error scraping match {match_id}: {e}")
            raise Exception(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)[:200]}"
            print(f"    ✗ {error_msg}")
            logger.error(f"Error scraping match {match_id}: {e}", exc_info=True)
            raise

        finally:
            # Always close driver
            if driver_manager:
                try:
                    print(f"    Closing WebDriver...")
                    logger.info("Closing WebDriver")
                    driver_manager.stop_driver()
                except Exception as e:
                    logger.warning(f"Error closing driver: {e}")

    def get_current_innings_team(self, driver):
        """Extract current innings team name from page"""
        try:
            team_element = driver.find_element("css selector", "div.ds-cursor-pointer.ds-min-w-max")
            return team_element.text.strip()
        except NoSuchElementException as e:
            logger.error(f"Error extracting innings team: {e}")
            raise