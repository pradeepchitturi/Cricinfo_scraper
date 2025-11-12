import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class PageNavigator:
    def __init__(self, driver):
        self.driver = driver

    def scroll_full_page(self, scroll_times=25):
        for _ in range(scroll_times):
            self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
            time.sleep(2)

    def scroll_to_top(self):
        self.driver.execute_script("window.scrollTo(0, 0);")

    def dismiss_popup(self):
        try:
            close_button = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".ds-modal__close, .wzrk-close"))
            )
            close_button.click()
        except (TimeoutException, NoSuchElementException):
            try:
                overlay = self.driver.find_element(By.CSS_SELECTOR, ".wzrk-overlay")
                self.driver.execute_script("arguments[0].remove();", overlay)
            except NoSuchElementException:
                pass

    def click_dropdown_and_switch_innings(self, default_team):
        print("innings switch started")
        self.dismiss_popup()
        dropdown = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "div.ds-cursor-pointer.ds-min-w-max"))
        )
        dropdown.click()
        time.sleep(2)

        innings_items = WebDriverWait(self.driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.ds-w-full.ds-flex"))
        )

        for item in innings_items:
            label = item.text.strip()
            if label != default_team:
                try:
                    item.click()
                except:
                    self.driver.execute_script("arguments[0].click();", item)
                return label
        raise Exception("Other innings not found")
