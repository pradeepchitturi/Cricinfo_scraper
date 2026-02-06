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

    def scroll_full_page(self, scroll_times=30):
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
        try:
            # Dismiss any popups first
            self.dismiss_popup()

            # Wait for and click the dropdown (updated selector)
            logger.debug("Waiting for dropdown button element")

            # Use a more flexible selector
            dropdown = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "button[class*='ds-capitalize'][class*='ds-cursor-pointer']"
                ))
            )

            # Log the element found
            logger.debug(f"Found dropdown element: {dropdown.get_attribute('class')}")
            logger.debug(f"Dropdown text: {dropdown.text}")

            # Scroll dropdown into view
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", dropdown)
            time.sleep(1)

            # Click the dropdown
            click_successful = False
            try:
                logger.debug("Opening dropdown with regular click")
                dropdown.click()
                click_successful = True
            except ElementClickInterceptedException as e:
                logger.debug(f"Regular click intercepted: {e}")
                logger.debug("Trying JavaScript click")
                self.driver.execute_script("arguments[0].click();", dropdown)
                click_successful = True
            except Exception as e:
                logger.warning(f"Click failed: {e}")
                raise

            if click_successful:
                time.sleep(2)

            # Wait for innings items to appear
            logger.debug("Waiting for innings items")
            innings_items = WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ds-w-full.ds-flex"))
            )

            logger.debug(f"Found {len(innings_items)} innings items")

            # Extract all innings names
            innings_names = []
            for idx, item in enumerate(innings_items):
                label = item.text.strip()
                if label:  # Ignore empty labels
                    innings_names.append(label)
                    logger.debug(f"  {idx + 1}. {label}")

            logger.info(f"Found {len(innings_names)} innings options: {innings_names}")

            # Verify we got some innings
            if not innings_names:
                logger.warning("No innings found in dropdown")
                raise Exception("Dropdown opened but no innings found")

            # Close the dropdown by clicking it again
            try:
                logger.debug("Closing dropdown")
                dropdown.click()
            except:
                logger.debug("Using JavaScript to close dropdown")
                self.driver.execute_script("arguments[0].click();", dropdown)

            time.sleep(1)

            return innings_names

        except TimeoutException as e:
            logger.error(f"Timeout waiting for dropdown elements: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting innings options: {e}", exc_info=True)
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

            # Try multiple selectors for the dropdown
            dropdown_selectors = [
                "button.ds-capitalize.ds-h-8.ds-cursor-pointer",  # Simplified new selector
                "button.ds-flex.ds-capitalize.ds-items-center",  # Partial new selector
                "div.ds-cursor-pointer.ds-min-w-max"  # Old selector (fallback)
            ]

            dropdown = None
            for idx, selector in enumerate(dropdown_selectors):
                try:
                    logger.debug(f"Trying dropdown selector {idx + 1}: {selector}")
                    dropdown = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"Dropdown found with selector {idx + 1}")
                    break
                except TimeoutException:
                    logger.debug(f"Selector {idx + 1} failed, trying next...")
                    continue

            if not dropdown:
                raise Exception("Could not find dropdown with any selector")

            # Scroll dropdown into view
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", dropdown)
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

            logger.debug(f"Found {len(innings_items)} innings options")

            # Find and click the target innings
            for item in innings_items:
                label = item.text.strip()
                logger.debug(f"Checking innings option: {label}")

                if label == target_innings:
                    logger.info(f"Found target innings: {label}")

                    try:
                        # Scroll item into view
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item)
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
        logger.warning(
            "click_dropdown_and_switch_innings is deprecated, use get_all_innings_options + switch_to_innings")
        logger.info(f"Switching innings from: {default_team}")

        try:
            # Dismiss any popups first
            self.dismiss_popup()

            # Try multiple selectors for the dropdown
            dropdown_selectors = [
                "button.ds-capitalize.ds-h-8.ds-cursor-pointer",
                "button.ds-flex.ds-capitalize.ds-items-center",
                "div.ds-cursor-pointer.ds-min-w-max"
            ]

            dropdown = None
            for idx, selector in enumerate(dropdown_selectors):
                try:
                    logger.debug(f"Trying dropdown selector {idx + 1}")
                    dropdown = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"Dropdown found with selector {idx + 1}")
                    break
                except TimeoutException:
                    continue

            if not dropdown:
                raise Exception("Could not find dropdown with any selector")

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