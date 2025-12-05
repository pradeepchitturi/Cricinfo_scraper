"""
Metadata Extractor - Extracts match metadata from HTML
"""
from bs4 import BeautifulSoup
from utils.logger import setup_logger

logger = setup_logger(__name__)


class MetadataExtractor:
    @staticmethod
    def extract_metadata(html_content, match_id):
        """
        Extract match metadata from HTML content

        Args:
            html_content: HTML string from page source
            match_id: Match identifier

        Returns:
            Dictionary containing match metadata
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            metadata = {}

            # Find the match details table
            match_details_table = soup.find("table", class_="ds-w-full ds-table ds-table-sm ds-table-auto")

            if not match_details_table:
                logger.warning(f"Match details table not found for match {match_id}")
                metadata["MatchID"] = match_id
                return metadata

            # Parse all rows in the table
            rows = match_details_table.find_all("tr")
            logger.info(f"Found {len(rows)} metadata rows for match {match_id}")

            for row in rows:
                columns = row.find_all("td")

                if len(columns) == 2:
                    # Key-value pair (e.g., "Toss" - "CSK won the toss")
                    key = columns[0].get_text(strip=True)
                    value = columns[1].get_text(separator=" ", strip=True)
                    metadata[key] = value

                elif len(columns) == 1:
                    # Single column (usually venue)
                    venue_text = columns[0].get_text(strip=True)
                    if venue_text:
                        metadata["Venue"] = venue_text

            # Add match ID
            metadata["MatchID"] = match_id

            logger.info(f"Extracted {len(metadata)} metadata fields for match {match_id}")

            # Log extracted keys for debugging
            logger.debug(f"Metadata keys: {list(metadata.keys())}")

            return metadata

        except Exception as e:
            logger.error(f"Error extracting metadata for match {match_id}: {e}", exc_info=True)
            # Return at least the match ID even if extraction fails
            return {"MatchID": match_id}