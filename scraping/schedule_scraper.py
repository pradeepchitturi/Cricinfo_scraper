from bs4 import BeautifulSoup
import time
from core.driver_manager import DriverManager

class ScheduleScraper:
    def __init__(self, url):
        self.url = url
        self.driver = None
        self.hrefs = []


    def fetch_hrefs(self):
        driver_manager = DriverManager()
        try:
            driver = driver_manager.start_driver()
            driver.get(self.url)
            time.sleep(5)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # THIS returns a list of <a> elements
            links = soup.find_all("a", class_="ds-no-tap-higlight")
            #print(links)

            # Now loop over each <a> element
            self.hrefs = []
            for link in links:
                #print(link)
                href = link.get('href')
                #print(href)
                if href and not "Match yet to begin" in href:
                    full_link = "https://www.espncricinfo.com" + href
                    self.hrefs.append(full_link)

            print(f"✅ Total match links found: {len(self.hrefs)}")
            return self.hrefs


        except Exception as e:
            print(f"❌ Error fetching hrefs: {e}")
            return []
        finally:
            driver_manager.stop_driver()
