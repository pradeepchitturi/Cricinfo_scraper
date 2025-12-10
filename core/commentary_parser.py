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
            List of parsed commentary data (normalized to consistent column count)
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

            # Normalize column count before returning
            normalized_data = CommentaryParser._normalize_columns(data)

            return normalized_data

        except Exception as e:
            logger.error(f"Error parsing commentary: {e}", exc_info=True)
            return []

    @staticmethod
    def _normalize_columns(data):
        """
        Normalize all rows to have consistent column count
        Handles both 6 and 7 column formats

        Format with 6 columns: [ball, runs/wicket, description, score, empty, commentary]
        Format with 7 columns: [ball, runs/wicket, description, score, photo_indicator, empty, commentary]

        Args:
            data: List of parsed commentary rows

        Returns:
            List of normalized rows (all same length)
        """
        if not data:
            return []

        # Analyze column distribution
        column_counts = {}
        for row in data:
            count = len(row)
            column_counts[count] = column_counts.get(count, 0) + 1

        logger.debug(f"Column distribution: {column_counts}")

        # Use most common column count as target
        target_columns = max(column_counts, key=column_counts.get)
        logger.info(f"Normalizing to {target_columns} columns (most common)")

        normalized = []

        for row in data:
            current_length = len(row)

            if current_length == target_columns:
                # Already correct
                normalized.append(row)

            elif current_length < target_columns:
                # Pad with empty strings before the last column (commentary)
                padding_needed = target_columns - current_length
                # Insert padding before the last element
                padded_row = row[:-1] + [''] * padding_needed + [row[-1]]
                normalized.append(padded_row)

            else:  # current_length > target_columns
                # Truncate to target length
                truncated_row = row[:target_columns]
                normalized.append(truncated_row)

        logger.info(f"Normalized {len(normalized)} rows to {target_columns} columns")

        return normalized

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
        Handles both 6 and 7 column formats dynamically

        Args:
            parsed_data: List of parsed commentary entries (normalized)

        Returns:
            pandas DataFrame with commentary data
        """
        try:
            if not parsed_data:
                logger.warning("No parsed data to convert to DataFrame")
                return pd.DataFrame()

            # Create DataFrame
            df = pd.DataFrame(parsed_data)

            num_columns = df.shape[1]
            logger.info(f"DataFrame created with {len(df)} rows and {num_columns} columns")

            # Handle based on column count
            if num_columns == 6:
                # Format: [ball, runs/wicket, description, score, empty, commentary]
                df.columns = ["Ball", "Runs_Wicket", "Event", "Score", "Extra", "Commentary"]

                # Drop unnecessary columns
                df = df.drop(["Runs_Wicket", "Extra"], axis=1)

            elif num_columns == 7:
                # Format: [ball, runs/wicket, description, score, photo_indicator, empty, commentary]
                df.columns = ["Ball", "Runs_Wicket", "Event", "Score", "Photo", "Extra", "Commentary"]

                # Fill missing Commentary from Extra if needed
                df['Commentary'] = df.apply(
                    lambda row: row['Extra'] if pd.isna(row['Commentary']) or row['Commentary'] == "" else row['Commentary'],
                    axis=1
                )

                # Drop unnecessary columns
                df = df.drop(["Runs_Wicket", "Photo", "Extra"], axis=1)

            else:
                logger.error(f"Unexpected column count: {num_columns}. Expected 6 or 7.")
                return pd.DataFrame()

            # Extract bowler and batsman from Event column
            df[['Bowler', 'Batsman']] = df['Event'].apply(
                CommentaryParser.extract_bowler_batsman
            )

            logger.info(f"Final DataFrame: {len(df)} rows with columns: {df.columns.tolist()}")

            return df

        except Exception as e:
            logger.error(f"Error converting to DataFrame: {e}", exc_info=True)
            logger.debug(f"Data shape: {df.shape if 'df' in locals() else 'N/A'}")
            logger.debug(f"Sample data: {parsed_data[:2] if parsed_data else 'N/A'}")
            return pd.DataFrame()
