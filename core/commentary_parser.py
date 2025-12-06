"""
Commentary Parser - Parses ball-by-ball commentary from HTML
"""
from bs4 import BeautifulSoup
import pandas as pd
from utils.logger import setup_logger

logger = setup_logger(__name__)


class CommentaryParser:
    @staticmethod
    def parse_commentary(html_content):
        """
        Parse commentary blocks from HTML content

        Args:
            html_content: HTML string from page source

        Returns:
            List of parsed commentary data
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            data = []

            # Find all commentary blocks
            blocks = soup.find_all(
                "div",
                class_="ds-text-tight-m ds-font-regular ds-flex ds-px-3 ds-py-2 "
                       "lg:ds-px-4 lg:ds-py-[10px] ds-items-start ds-select-none lg:ds-select-auto"
            )

            logger.info(f"Found {len(blocks)} commentary blocks")

            for block in blocks:
                # Extract all text from spans and paragraphs
                span_texts = [span.get_text(strip=True) for span in block.find_all("span")]
                p_texts = [p.get_text(strip=True) for p in block.find_all("p")]
                strong_texts = [strong.get_text(strip=True) for strong in block.find_all("strong")]

                # Combine p_texts and strong_texts into one element
                p_and_strong_combined = '#**#'.join(p_texts + strong_texts)

                # Add to all_text
                all_text = span_texts + [p_and_strong_combined]


                if all_text:  # Only add non-empty blocks
                    data.append(all_text)

            logger.info(f"Parsed {len(data)} commentary entries")
            return data

        except Exception as e:
            logger.error(f"Error parsing commentary: {e}", exc_info=True)
            return []

    @staticmethod
    def extract_bowler_batsman(event):
        """
        Extract bowler and batsman names from event text

        Args:
            event: Event text string (e.g., "Bumrah to Kohli, no run")

        Returns:
            pandas Series with [bowler, batsman]
        """
        try:
            if pd.isna(event) or not event:
                return pd.Series([None, None])

            # Split on ' to ' to separate bowler and batsman
            parts = event.split(' to ')

            if len(parts) < 2:
                return pd.Series([None, None])

            bowler = parts[0].strip()

            # Extract batsman (before first comma)
            batsman_part = parts[1].split(',')[0].strip() if ',' in parts[1] else parts[1].strip()

            return pd.Series([bowler, batsman_part])

        except Exception as e:
            logger.warning(f"Error extracting bowler/batsman from '{event}': {e}")
            return pd.Series([None, None])

    @staticmethod
    def to_dataframe(parsed_data):
        """
        Convert parsed commentary data to DataFrame

        Args:
            parsed_data: List of parsed commentary entries

        Returns:
            pandas DataFrame with commentary data
        """
        try:
            if not parsed_data:
                logger.warning("No parsed data to convert to DataFrame")
                return pd.DataFrame()

            df = pd.DataFrame(parsed_data)

            # Check if we have enough columns
            if df.shape[1] < 7:
                logger.warning(f"Insufficient columns in parsed data: {df.shape[1]} < 7")
                return pd.DataFrame()

            # Fill missing values in column 5 with values from column 6
            df[5] = df.apply(
                lambda row: row[6] if pd.isna(row[5]) or row[5] == "" else row[5],
                axis=1
            )

            # Drop unnecessary columns
            df = df.drop([1, 4, 6], axis=1)

            # Rename columns
            df.columns = ["Ball", "Event", "Score", "Commentary"]

            # Extract bowler and batsman from Event column
            df[['Bowler', 'Batsman']] = df['Event'].apply(
                CommentaryParser.extract_bowler_batsman
            )

            logger.info(f"Created DataFrame with {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Error converting to DataFrame: {e}", exc_info=True)
            return pd.DataFrame()