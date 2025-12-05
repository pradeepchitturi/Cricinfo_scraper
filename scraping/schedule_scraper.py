"""
Schedule Scraper - Fetches match links from tournament schedule using Selenium
"""
from bs4 import BeautifulSoup
import time
from core.driver_manager import DriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException
from utils.logger import setup_logger

logger = setup_logger(__name__)


class ScheduleScraper:
    def __init__(self, url, page_load_timeout=180, max_retries=3):
        """
        Initialize Schedule Scraper

        Args:
            url: Schedule page URL
            page_load_timeout: Page load timeout in seconds (default: 180 = 3 minutes)
            max_retries: Maximum retry attempts (default: 3)
        """
        self.url = url
        self.page_load_timeout = page_load_timeout
        self.max_retries = max_retries
        self.driver = None
        self.hrefs = []

    def _load_page_with_retry(self, driver):
        """
        Load schedule page with retry logic

        Args:
            driver: Selenium WebDriver instance

        Raises:
            Exception: If all retries fail
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"  Loading schedule page (attempt {attempt}/{self.max_retries})...")
                logger.info(f"Loading schedule page - attempt {attempt}")

                driver.get(self.url)
                time.sleep(8)  # Wait for page to load

                print(f"  ✓ Schedule page loaded successfully")
                logger.info("Schedule page loaded successfully")
                return

            except TimeoutException as e:
                last_exception = e
                print(f"  ✗ Timeout on attempt {attempt}")
                logger.warning(f"Timeout loading schedule page - attempt {attempt}")

                if attempt < self.max_retries:
                    wait_time = attempt * 5  # Progressive backoff: 5s, 10s, 15s
                    print(f"  Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)

                    # Try to refresh
                    try:
                        driver.refresh()
                        time.sleep(3)
                    except:
                        pass

            except WebDriverException as e:
                last_exception = e
                print(f"  ✗ WebDriver error on attempt {attempt}: {str(e)[:100]}")
                logger.error(f"WebDriver error - attempt {attempt}: {e}")

                if attempt < self.max_retries:
                    wait_time = attempt * 3
                    print(f"  Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)

        # All retries failed
        error_msg = f"Failed to load schedule page after {self.max_retries} attempts: {last_exception}"
        logger.error(error_msg)
        raise Exception(error_msg)

    def fetch_hrefs(self):
        """
        Fetch all match links from the schedule page

        Returns:
            List of match URLs
        """
        driver_manager = None
        driver = None

        try:
            print("Initializing WebDriver for schedule scraping...")
            logger.info("Initializing WebDriver for schedule scraping")

            # Initialize driver with timeout
            driver_manager = DriverManager(
                headless=False,
                page_load_timeout=self.page_load_timeout,
                implicit_wait=10
            )
            driver = driver_manager.start_driver()

            # Load page with retry logic
            self._load_page_with_retry(driver)

            # Parse page with BeautifulSoup
            print("  Parsing page content...")
            logger.info("Parsing schedule page with BeautifulSoup")
            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Find all match links
            print("  Extracting match links...")
            logger.info("Extracting match links from schedule")

            links = soup.find_all("a", class_="ds-no-tap-higlight")

            if not links:
                logger.warning("No links found with 'ds-no-tap-higlight', trying alternatives...")
                links = soup.find_all("a", href=True)
                logger.info(f"Found {len(links)} total <a> tags")

            # Extract hrefs
            self.hrefs = []
            for link in links:
                href = link.get('href')

                if href and "Match yet to begin" not in href:
                    if href.startswith('/'):
                        full_link = "https://www.espncricinfo.com" + href
                    else:
                        full_link = href

                    if full_link not in self.hrefs:
                        self.hrefs.append(full_link)

            print(f"  ✓ Total match links found: {len(self.hrefs)}")
            logger.info(f"Successfully extracted {len(self.hrefs)} match links")

            return self.hrefs

        except TimeoutException as e:
            error_msg = f"Timeout error while fetching schedule: {str(e)}"
            print(f"  ✗ {error_msg}")
            logger.error(f"Timeout fetching schedule: {e}")
            return []

        except WebDriverException as e:
            error_msg = f"WebDriver error while fetching schedule: {str(e)[:200]}"
            print(f"  ✗ {error_msg}")
            logger.error(f"WebDriver error fetching schedule: {e}")
            return []

        except Exception as e:
            error_msg = f"Error fetching schedule: {str(e)[:200]}"
            print(f"  ✗ {error_msg}")
            logger.error(f"Error fetching schedule: {e}", exc_info=True)
            return []

        finally:
            # Always close driver
            if driver_manager:
                try:
                    print("  Closing WebDriver...")
                    logger.info("Closing WebDriver")
                    driver_manager.stop_driver()
                except Exception as e:
                    logger.warning(f"Error closing driver: {e}")