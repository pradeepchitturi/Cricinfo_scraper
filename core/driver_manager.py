"""
Driver Manager - Manages Selenium WebDriver lifecycle with optimizations
"""
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


class DriverManager:
    def __init__(self, headless=False, page_load_timeout=300, implicit_wait=15):
        """
        Initialize Driver Manager

        Args:
            headless: Run browser in headless mode (default: False)
            page_load_timeout: Page load timeout in seconds (default: 300 = 5 minutes)
            implicit_wait: Implicit wait for elements in seconds (default: 15)
        """
        self.headless = headless
        self.page_load_timeout = page_load_timeout
        self.implicit_wait = implicit_wait
        self.driver = None

    def start_driver(self):
        """
        Start Chrome WebDriver with optimized options

        Returns:
            WebDriver instance
        """
        options = Options()

        # Headless mode configuration
        if self.headless:
            options.add_argument("--headless=new")
            options.add_argument("--window-size=1920,1080")
        else:
            options.add_argument('--start-maximized')

        # Performance & Stability optimizations
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-software-rasterizer')

        # Avoid detection as bot
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Set realistic user agent
        options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        )

        # Disable notifications and other popups
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_setting_values.geolocation": 2
        }
        options.add_experimental_option("prefs", prefs)

        # Initialize Chrome driver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # Set timeouts - IMPORTANT for fixing timeout issues
        self.driver.set_page_load_timeout(self.page_load_timeout)
        self.driver.set_script_timeout(self.page_load_timeout)
        self.driver.implicitly_wait(self.implicit_wait)

        # Remove webdriver property to avoid detection
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        print(f"  ✓ WebDriver started (page timeout: {self.page_load_timeout}s, wait: {self.implicit_wait}s)")

        return self.driver

    def stop_driver(self):
        """Stop and quit the WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                print(f"  Warning: Error closing driver: {e}")
                self.driver = None