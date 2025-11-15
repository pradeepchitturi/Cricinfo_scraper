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
from configs.db_config import initialize_database, save_to_db
import json


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

        # Regex pattern to identify columns to merge
        pattern = re.compile(r".*Replacement$")

        # Identify matching keys
        keys_to_merge = [k for k in metadata if pattern.match(k)]

        # Combine into a single new key with original keys and values
        metadata["player_replacements"] = json.dumps({k: metadata.pop(k) for k in keys_to_merge})

        commentary_url = self.url.replace("/full-scorecard","/ball-by-ball-commentary")
        driver.get(commentary_url)
        time.sleep(5)
        # Scroll to load all commentary
        page_nav.scroll_full_page()
        # Extract default innings team
        default_team_name = self.get_current_innings_team(driver)
        print(f"📝 Default batting team: {default_team_name}")
        metadata["second_innings"] = default_team_name

        # Save first innings commentary
        innings1_html = driver.page_source
        innings1_data = CommentaryParser.parse_commentary(innings1_html)
        innings1_df = CommentaryParser.to_dataframe(innings1_data)
        innings1_df["Innings"] = default_team_name
        innings1_df["MatchID"] = match_id

        # Scroll to top before switch
        page_nav.scroll_to_top()
        time.sleep(3)

        # Switch innings
        switched_team_name = page_nav.click_dropdown_and_switch_innings(default_team_name)
        print(f"📝 Switched batting team: {switched_team_name}")
        metadata["first_innings"] = switched_team_name

        print(metadata)

        # convert the metadata from dictionary to pandas dataframe
        metadata_df = pd.DataFrame([metadata])

        # Replace spaces with underscores in all column names
        metadata_df.columns = (
            metadata_df.columns
            .str.replace(r"[ ()]", "_", regex=True)  # replace spaces/parentheses
            .str.replace(r"_+", "_", regex=True)  # collapse multiple underscores
            .str.strip("_")  # remove leading/trailing underscores
            .str.lower()  # lowercase everything
        )

        # Save metadata to DB
        save_to_db("raw_match_metadata", metadata_df)

        time.sleep(5)
        page_nav.scroll_full_page()

        # Save second innings commentary
        innings2_html = driver.page_source
        innings2_data = CommentaryParser.parse_commentary(innings2_html)
        innings2_df = CommentaryParser.to_dataframe(innings2_data)
        innings2_df["Innings"] = switched_team_name
        innings2_df["MatchID"] = match_id

        # Combine both innings
        final_df = pd.concat([innings1_df, innings2_df], ignore_index=True)

        final_df.columns = (
            final_df.columns
            .str.replace(r"[ ()]", "_", regex=True)  # replace spaces/parentheses
            .str.replace(r"_+", "_", regex=True)  # collapse multiple underscores
            .str.strip("_")  # remove leading/trailing underscores
            .str.lower()  # lowercase everything
        )

        # Replace spaces with underscores in all column names
        final_df.columns = final_df.columns.str.replace(' ', '_')

        # Save commentary to DB
        save_to_db("raw_match_events",final_df)

        driver_manager.stop_driver()
        print(f"✅ Saved commentary and metadata for match: {match_id}")

    def get_current_innings_team(self, driver):
        team_element = driver.find_element("css selector", "div.ds-cursor-pointer.ds-min-w-max")
        return team_element.text.strip()
