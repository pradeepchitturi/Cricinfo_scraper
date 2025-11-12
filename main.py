from scraping.schedule_scraper import ScheduleScraper
from scraping.match_scraper import MatchScraper
from utils.tracker import MatchTracker
import re

def extract_match_id(url):
    match = re.search( r'-(\d+)/full-scorecard', url)
    return match.group(1) if match else None

def main():
    schedule_url = "https://www.espncricinfo.com/series/ipl-2025-1449924/match-schedule-fixtures-and-results"
    tracker = MatchTracker()

    schedule_scraper = ScheduleScraper(schedule_url)
    match_links = schedule_scraper.fetch_hrefs()

    for url in match_links:
        if "full-scorecard" in url and "ipl-2025" in url:
            print(url)
            match_id = extract_match_id(url)
            print(match_id)
            if not match_id:
                print(f"⚠️ Could not extract match ID from {url}")
                continue

            if tracker.exists(match_id):
                print(f"✅ Skipping already downloaded match {match_id}")
                continue

            match_scraper = MatchScraper(url)
            try:
                match_scraper.scrape(match_id)
                tracker.add(match_id)
            except Exception as e:
                print(f"❌ Error scraping {url}: {e}")

if __name__ == "__main__":
    main()
