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
from core.file_manager import FileManager

class MatchScraper:
    def __init__(self, url, base_dir="data"):
        self.url = url
        self.base_dir = base_dir

    def get_folder_name(self, metadata):
        # Match date
        match_date_text = metadata.get("Match days", "")
        date_part = "UnknownDate"
        if match_date_text:
            match_date = re.search(r"(\d{1,2} \w+ \d{4})", match_date_text)
            if match_date:
                parsed_date = datetime.strptime(match_date.group(1), "%d %B %Y")
                date_part = parsed_date.strftime("%Y%m%d")

        # Match slug from URL
        path_parts = urlparse(self.url).path.split('/')
        if len(path_parts) > 3:
            match_slug = path_parts[3]
        else:
            match_slug = "match"

        folder_name = f"{date_part}_{match_slug}".replace(" ", "_")
        return folder_name


    def format_date(self, date_str):
        # Format '23 May 2025' -> '20250523'
        try:
            from datetime import datetime
            date_obj = datetime.strptime(date_str, '%d %B %Y')
            return date_obj.strftime('%Y%m%d')
        except Exception:
            return "unknown_date"


    def scrape(self,match_id):
        driver_manager = DriverManager()
        driver = driver_manager.start_driver()
        page_nav = PageNavigator(driver)

        # Go to full scorecard first
        driver.get(self.url)
        time.sleep(5)

        # Scroll to load all commentary
        page_nav.scroll_full_page(5)

        # Extract metadata
        metadata = MetadataExtractor.extract_metadata(driver.page_source,match_id)

        # Folder setup
        folder_name = self.get_folder_name(metadata)
        folder_path = os.path.join(self.base_dir, folder_name)
        print(folder_path)
        FileManager.make_folder(folder_path)

        # Save scorecard page HTML
        FileManager.save_html(driver.page_source, os.path.join(folder_path, "scorecard.html"))

        # Save metadata
        FileManager.save_json(metadata, os.path.join(folder_path, "metadata.json"))

        commentary_url = self.url.replace("/full-scorecard","/ball-by-ball-commentary")
        driver.get(commentary_url)
        time.sleep(5)
        # Scroll to load all commentary
        page_nav.scroll_full_page()
        # Extract default innings team
        default_team_name = self.get_current_innings_team(driver)
        print(f"📝 Default batting team: {default_team_name}")

        # Save first innings commentary
        innings1_html = driver.page_source
        innings1_data = CommentaryParser.parse_commentary(innings1_html)
        innings1_df = CommentaryParser.to_dataframe(innings1_data)
        innings1_df["Innings"] = default_team_name
        innings1_df["MatchID"] = match_id

        # Save innings 1 html
        FileManager.save_html(innings1_html, os.path.join(folder_path, f"{default_team_name}_innings.html"))

        # Scroll to top before switch
        page_nav.scroll_to_top()
        time.sleep(3)

        # Switch innings
        switched_team_name = page_nav.click_dropdown_and_switch_innings(default_team_name)
        print(f"📝 Switched batting team: {switched_team_name}")

        time.sleep(5)
        page_nav.scroll_full_page()

        # Save second innings commentary
        innings2_html = driver.page_source
        innings2_data = CommentaryParser.parse_commentary(innings2_html)
        innings2_df = CommentaryParser.to_dataframe(innings2_data)
        innings2_df["Innings"] = switched_team_name
        innings2_df["MatchID"] = match_id

        FileManager.save_html(innings2_html, os.path.join(folder_path, f"{switched_team_name}_innings.html"))

        # Combine both innings
        final_df = pd.concat([innings1_df, innings2_df], ignore_index=True)

        # Add Metadata Columns
        for key, value in metadata.items():
            clean_key = key.replace(" ", "_")
            final_df[clean_key] = value

        # Save commentary CSV
        FileManager.save_csv(final_df, os.path.join(folder_path, "commentary.csv"))

        driver_manager.stop_driver()
        print(f"✅ Saved commentary and metadata for match: {match_id}")

    def get_current_innings_team(self, driver):
        team_element = driver.find_element("css selector", "div.ds-cursor-pointer.ds-min-w-max")
        return team_element.text.strip()
