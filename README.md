# Cricinfo Scraper 🏏

This project scrapes ball-by-ball commentary and metadata for IPL 2025 matches from ESPN Cricinfo.

## Project Structure
- `core/`: Core modules for driver, navigation, parsing
- `scraping/`: Scrapers for match and schedule
- `utils/`: Logger and download tracker
- `configs/`: Settings
- `data/`: Scraped data output
- `main.py`: Main runner script

## How to Run
1. Install dependencies:
    ```
    pip install -r requirements.txt
    ```

2. Run the scraper:
    ```
    python main.py
    ```

3. Data will be saved under `data/` folder.

## Features
- Scrapes ball-by-ball commentary
- Extracts match metadata
- Resumable downloads (no duplicate downloads)

## Requirements
- Python 3.8+
- Chrome browser installed
