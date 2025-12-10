"""
Page Navigator - Handles page navigation and interactions
"""
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from utils.logger import setup_logger

logger = setup_logger(__name__)


class PageNavigator:
    def __init__(self, driver):
        """
        Initialize Page Navigator

        Args:
            driver: Selenium WebDriver instance
        """
        self.driver = driver

    def scroll_full_page(self, scroll_times=20):
        """
        Scroll down the entire page to load dynamic content

        Args:
            scroll_times: Number of times to scroll (default: 25)
        """
        try:
            logger.info(f"Scrolling page {scroll_times} times")
            for i in range(scroll_times):
                self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
                time.sleep(3)
            logger.info("Page scrolling completed")
        except Exception as e:
            logger.error(f"Error during page scroll: {e}")

    def scroll_to_top(self):
        """Scroll to the top of the page"""
        try:
            logger.info("Scrolling to top of page")
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        except Exception as e:
            logger.error(f"Error scrolling to top: {e}")

    def dismiss_popup(self):
        """Dismiss any popups or overlays that might be blocking content"""
        try:
            logger.debug("Attempting to dismiss popups")
            close_button = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".ds-modal__close, .wzrk-close"))
            )
            close_button.click()
            logger.info("Popup dismissed via close button")
            time.sleep(1)
        except (TimeoutException, NoSuchElementException):
            try:
                overlay = self.driver.find_element(By.CSS_SELECTOR, ".wzrk-overlay")
                self.driver.execute_script("arguments[0].remove();", overlay)
                logger.info("Popup dismissed by removing overlay")
            except NoSuchElementException:
                logger.debug("No popup found to dismiss")
                pass
        except Exception as e:
            logger.warning(f"Error dismissing popup: {e}")

    def get_all_innings_options(self):
        """
        Get all available innings from the dropdown

        Returns:
            List of innings names (e.g., ['DC', 'RR', 'Super Over 1'])

        Raises:
            Exception: If unable to access dropdown
        """
        #logger.info("Getting all innings options")

        try:
            # Dismiss any popups first
            self.dismiss_popup()

            # Wait for and click the dropdown
            logger.debug("Waiting for dropdown element")
            dropdown = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.ds-cursor-pointer.ds-min-w-max"))
            )

            # Scroll dropdown into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", dropdown)
            time.sleep(1)

            # Click the dropdown
            try:
                logger.debug("Opening dropdown")
                dropdown.click()
            except ElementClickInterceptedException:
                logger.debug("Regular click failed, trying JavaScript click")
                self.driver.execute_script("arguments[0].click();", dropdown)

            time.sleep(2)

            # Wait for innings items to appear
            logger.debug("Waiting for innings items")
            innings_items = WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ds-w-full.ds-flex"))
            )

            # Extract all innings names
            innings_names = []
            for item in innings_items:
                label = item.text.strip()
                if label:  # Ignore empty labels
                    innings_names.append(label)
                    logger.debug(f"Found innings option: {label}")

            logger.info(f"Found {len(innings_names)} innings options: {innings_names}")

            # Close the dropdown by clicking it again
            try:
                dropdown.click()
            except:
                self.driver.execute_script("arguments[0].click();", dropdown)

            time.sleep(1)

            return innings_names

        except Exception as e:
            logger.error(f"Error getting innings options: {e}")
            raise

    def switch_to_innings(self, target_innings):
        """
        Switch to a specific innings by name

        Args:
            target_innings: Name of innings to switch to (e.g., 'DC', 'RR', 'Super Over 1')

        Returns:
            True if successful, False otherwise

        Raises:
            Exception: If switching fails
        """
        logger.info(f"Switching to innings: {target_innings}")

        try:
            # Dismiss any popups first
            self.dismiss_popup()

            # Wait for and click the dropdown
            logger.debug("Opening innings dropdown")
            dropdown = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.ds-cursor-pointer.ds-min-w-max"))
            )

            # Scroll dropdown into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", dropdown)
            time.sleep(1)

            # Click the dropdown
            try:
                dropdown.click()
            except ElementClickInterceptedException:
                logger.debug("Regular click failed, trying JavaScript click")
                self.driver.execute_script("arguments[0].click();", dropdown)

            time.sleep(2)

            # Wait for innings items to appear
            logger.debug("Waiting for innings items")
            innings_items = WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ds-w-full.ds-flex"))
            )

            logger.debug(f"Found {len(innings_items)} innings options")

            # Find and click the target innings
            for item in innings_items:
                label = item.text.strip()
                logger.debug(f"Checking innings option: {label}")

                if label == target_innings:
                    logger.info(f"Found target innings: {label}")

                    try:
                        # Scroll item into view
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", item)
                        time.sleep(0.5)
                        item.click()
                    except ElementClickInterceptedException:
                        logger.debug("Regular click intercepted, using JavaScript")
                        self.driver.execute_script("arguments[0].click();", item)

                    time.sleep(3)  # Wait for page to reload
                    logger.info(f"Successfully switched to {label}")
                    return True

            # Target innings not found
            raise Exception(f"Target innings '{target_innings}' not found in dropdown")

        except Exception as e:
            logger.error(f"Error switching to innings '{target_innings}': {e}")
            raise

    def click_dropdown_and_switch_innings(self, default_team):
        """
        DEPRECATED: Use get_all_innings_options() and switch_to_innings() instead

        Click innings dropdown and switch to the other innings

        Args:
            default_team: Current team batting (to avoid selecting it again)

        Returns:
            Name of the switched team

        Raises:
            Exception: If switching innings fails
        """
        logger.warning("click_dropdown_and_switch_innings is deprecated, use get_all_innings_options + switch_to_innings")
        logger.info(f"Switching innings from: {default_team}")

        try:
            # Dismiss any popups first
            self.dismiss_popup()

            # Wait for and click the dropdown
            logger.debug("Waiting for dropdown element")
            dropdown = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.ds-cursor-pointer.ds-min-w-max"))
            )

            # Scroll dropdown into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", dropdown)
            time.sleep(1)

            # Click the dropdown
            try:
                logger.debug("Clicking dropdown")
                dropdown.click()
            except ElementClickInterceptedException:
                logger.debug("Regular click failed, trying JavaScript click")
                self.driver.execute_script("arguments[0].click();", dropdown)

            time.sleep(2)

            # Wait for innings items to appear
            logger.debug("Waiting for innings items")
            innings_items = WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ds-w-full.ds-flex"))
            )

            logger.debug(f"Found {len(innings_items)} innings options")

            # Find and click the other innings
            for item in innings_items:
                label = item.text.strip()
                logger.debug(f"Checking innings option: {label}")

                if label and label != default_team:
                    logger.info(f"Switching to innings: {label}")

                    try:
                        # Scroll item into view
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", item)
                        time.sleep(0.5)
                        item.click()
                    except ElementClickInterceptedException:
                        logger.debug("Regular click intercepted, using JavaScript")
                        self.driver.execute_script("arguments[0].click();", item)

                    time.sleep(2)
                    logger.info(f"Successfully switched to {label}")
                    return label

            # If we get here, other innings wasn't found
            raise Exception(f"Other innings not found (only found: {default_team})")

        except Exception as e:
            logger.error(f"Error switching innings: {e}")
            raise