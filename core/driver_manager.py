from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

class DriverManager:
    def __init__(self, headless=True):   # headless=False to open browser window
        self.headless = headless
        self.driver = None

    def start_driver(self):

        options = webdriver.ChromeOptions()
        #options.add_argument("--headless=new")
        options.add_argument('--start-maximized')
        options.add_argument('--disable-blink-features=AutomationControlled')
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        return self.driver

    def stop_driver(self):
        if self.driver:
            self.driver.quit()
