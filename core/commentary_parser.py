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
                class_=lambda x: x and "ds-text-article-body-1" in x and "ds-flex" in x and "ds-items-start" in x
            )

            logger.info(f"Found {len(blocks)} commentary blocks")

            if not blocks:
                logger.error("No commentary blocks found")
                return []

            for idx, block in enumerate(blocks):
                try:
                    # STEP 1: Extract header text (bowler to batsman)
                    # Using partial class match for flexibility
                    header_div = block.find(
                        "div",
                        class_=lambda x: x and "ds-text-overline-1" in x and "ds-font-medium" in x
                    )
                    header_text = header_div.get_text(strip=True) if header_div else ""

                    if header_text:
                        logger.debug(f"Block {idx} header: {header_text}")

                    # STEP 2: Extract span texts (ball number, event, score)
                    # Filter out spans containing unwanted text
                    unwanted_terms = ["photos", "see all", "image", "gallery"]
                    span_texts = [
                        span.get_text(strip=True)
                        for span in block.find_all("span")
                        if not any(term in span.get_text(strip=True).lower() for term in unwanted_terms)
                    ]

                    # STEP 3: Extract commentary from p and strong tags
                    p_texts = [p.get_text(strip=True) for p in block.find_all("p")]
                    strong_texts = [strong.get_text(strip=True) for strong in block.find_all("strong")]

                    # Combine p_texts and strong_texts
                    commentary_parts = p_texts + strong_texts
                    p_and_strong_combined = '#**#'.join(commentary_parts) if commentary_parts else ""

                    # STEP 4: FALLBACK - If no commentary from p/strong, extract from entire block
                    if not p_and_strong_combined.strip():
                        # Get all text from the block
                        all_block_text = block.get_text(separator=" ", strip=True)

                        # Remove header text and span texts to leave just commentary
                        remaining_text = all_block_text

                        if header_text:
                            remaining_text = remaining_text.replace(header_text, "", 1)

                        for span_text in span_texts:
                            remaining_text = remaining_text.replace(span_text, "", 1)

                        remaining_text = remaining_text.strip()

                        # If we found text after removing header and spans, use it
                        if remaining_text:
                            p_and_strong_combined = remaining_text
                            logger.debug(f"Block {idx}: Used fallback extraction")

                    # Build the row - include header text at the beginning
                    if header_text:
                        all_text = [header_text] + span_texts + [p_and_strong_combined]
                    else:
                        all_text = span_texts + [p_and_strong_combined]

                    # STEP 5: CLEAN UP "See all photos" marker
                    all_text = CommentaryParser._clean_photo_markers(all_text)

                    # Only add if we have meaningful data
                    if all_text and len([x for x in all_text if x]) >= 2:
                        data.append(all_text)

                except Exception as e:
                    logger.warning(f"Error parsing block {idx}: {e}")
                    continue

            logger.info(f"Parsed {len(data)} commentary entries")
            #print(data)
            # Normalize column count before returning
            normalized_data = CommentaryParser._normalize_columns(data)

            return normalized_data

        except Exception as e:
            logger.error(f"Error parsing commentary: {e}", exc_info=True)
            return []

    @staticmethod
    def _clean_photo_markers(all_text):
        """
        Clean up "See all photos" markers and empty elements

        Strategy:
        1. Find "See all photos" element
        2. If next element is empty, remove both
        3. If next element has content, just remove "See all photos"

        Args:
            all_text: List of text elements

        Returns:
            Cleaned list with photo markers removed
        """
        if not all_text:
            return all_text

        cleaned = []
        i = 0

        while i < len(all_text):
            current = all_text[i]

            # Check if current element is "See all photos" (case insensitive)
            if current and 'see all photo' in current.lower():
                # Check if there's a next element
                if i + 1 < len(all_text):
                    next_element = all_text[i + 1]

                    # If next element is empty or whitespace
                    if not next_element or not next_element.strip():
                        # Skip both current ("See all photos") and next (empty)
                        i += 2
                        continue
                    else:
                        # Next element has content, just skip "See all photos"
                        i += 1
                        continue
                else:
                    # "See all photos" is last element, skip it
                    i += 1
                    continue
            else:
                # Keep this element
                cleaned.append(current)
                i += 1

        return cleaned

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

        logger.info(f"Column distribution: {column_counts}")

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
                if len(row) > 0:
                    padded_row = row[:-1] + [''] * padding_needed + [row[-1]]
                    normalized.append(padded_row)
                else:
                    normalized.append([''] * target_columns)

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
            #df.to_csv('people.csv')
            num_columns = df.shape[1]
            logger.info(f"DataFrame created with {len(df)} rows and {num_columns} columns")

            # Handle based on column count
            if num_columns == 5:
                # Format: [ball, runs/wicket, description, score, empty, commentary]
                df.columns = ["Event", "Ball", "Runs_Wicket","Extra", "Commentary"]

                # Drop unnecessary columns
                df = df.drop(["Runs_Wicket", "Extra"], axis=1)

            elif num_columns == 6:
                # Format: [ball, runs/wicket, description, score, photo_indicator, empty, commentary]
                df.columns = ["Event","Ball", "Runs_Wicket", "Photo", "Extra", "Commentary"]

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

            # Log statistics about commentary extraction
            empty_commentary = df['Commentary'].isna() | (df['Commentary'] == '')
            logger.info(f"  Rows with commentary: {(~empty_commentary).sum()}")
            logger.info(f"  Rows without commentary: {empty_commentary.sum()}")

            return df

        except Exception as e:
            logger.error(f"Error converting to DataFrame: {e}", exc_info=True)
            logger.debug(f"Data shape: {df.shape if 'df' in locals() else 'N/A'}")
            logger.debug(f"Sample data: {parsed_data[:2] if parsed_data else 'N/A'}")
            return pd.DataFrame()