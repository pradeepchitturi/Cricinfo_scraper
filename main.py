"""
Main scraper script with database-backed match tracking
"""
from scraping.schedule_scraper import ScheduleScraper
from scraping.match_scraper import MatchScraper
from utils.match_tracker import MatchTracker
from configs.db_config import initialize_database
import re


def extract_match_id(url):
    """Extract match ID from Cricinfo URL"""
    match = re.search(r'-(\d+)/full-scorecard', url)
    return match.group(1) if match else None


def main():
    print("=" * 80)
    print("CRICKET DATA SCRAPER - Starting")
    print("=" * 80)

    # Initialize database and run schema
    print("\n📊 Initializing database...")
    initialize_database()

    # Initialize tracker (uses get_connection from db_config)
    print("📦 Initializing match tracker...")
    try:
        tracker = MatchTracker()
    except Exception as e:
        print(f"❌ Failed to initialize tracker: {e}")
        return

    # Show current statistics
    print(f"\n📈 Current Status: {tracker.count()} matches already downloaded")

    # Schedule scraper
    schedule_url = "https://www.espncricinfo.com/series/ipl-2025-1449924/match-schedule-fixtures-and-results"

    print(f"\n🔍 Fetching match schedule from Cricinfo...")
    schedule_scraper = ScheduleScraper(schedule_url)
    match_links = schedule_scraper.fetch_hrefs()

    print(f"✅ Found {len(match_links)} total links")

    # Filter for full scorecards
    scorecard_links = [
        url for url in match_links
        if "full-scorecard" in url and "ipl-2025" in url
    ]

    print(f"🏏 Found {len(scorecard_links)} match scorecards to process")

    # Optional: Load cache for better performance with many matches
    if len(scorecard_links) > 20:
        print("📦 Loading match cache for faster lookups...")
        tracker.load_cache()

    # Counters
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    print("\n" + "=" * 80)
    print("PROCESSING MATCHES")
    print("=" * 80 + "\n")

    # Process each match
    for idx, url in enumerate(scorecard_links, 1):
        print(f"\n[{idx}/{len(scorecard_links)}] Processing: {url}")

        # Extract match ID
        match_id = extract_match_id(url)
        if not match_id:
            print(f"⚠️  Could not extract match ID from URL")
            failed_count += 1
            continue

        print(f"🆔 Match ID: {match_id}")

        # Check if already downloaded
        if tracker.exists(match_id):
            print(f"⏭️  Skipping - already downloaded")
            skipped_count += 1
            continue

        # Scrape the match
        match_scraper = MatchScraper(url)
        try:
            print(f"⬇️  Downloading match data...")
            match_scraper.scrape(match_id)

            # Track successful download
            tracker.add(
                match_id=match_id,
                source_url=url,
                status='completed'
            )

            downloaded_count += 1
            print(f"✅ Successfully downloaded and tracked match {match_id}")

        except Exception as e:
            print(f"❌ Error scraping match {match_id}: {e}")

            # Track failed download
            tracker.mark_failed(
                match_id=match_id,
                error_message=str(e),
                source_url=url
            )

            failed_count += 1

    # Clear cache if it was loaded
    if len(scorecard_links) > 20:
        tracker.clear_cache()

    # Print summary
    print("\n" + "=" * 80)
    print("SCRAPING SUMMARY")
    print("=" * 80)
    print(f"Total Matches Found:  {len(scorecard_links)}")
    print(f"Downloaded:           {downloaded_count}")
    print(f"Skipped:              {skipped_count}")
    print(f"Failed:               {failed_count}")
    print("=" * 80)

    # Show overall statistics
    tracker.print_statistics()

    # Show failed matches if any
    if failed_count > 0:
        print("\n⚠️  FAILED MATCHES:")
        print("-" * 80)
        failed_matches = tracker.get_failed_matches()
        for match in failed_matches[:10]:  # Show first 10
            print(f"  Match ID: {match['match_id']}")
            print(f"  Error: {match['error_message']}")
            print(f"  URL: {match['source_url']}")
            print("-" * 80)

        if len(failed_matches) > 10:
            print(f"  ... and {len(failed_matches) - 10} more")

    print("\n✅ Scraping complete!")


if __name__ == "__main__":
    main()