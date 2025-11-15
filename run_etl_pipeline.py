#!/usr/bin/env python3
"""
Medallion Architecture ETL Pipeline Orchestrator

This script orchestrates the entire ETL pipeline:
1. Bronze Layer: Raw data (already populated by scraper)
2. Silver Layer: Cleaned and enriched data
3. Gold Layer: Analytics-ready aggregates

Usage:
    python run_etl_pipeline.py              # Run full pipeline
    python run_etl_pipeline.py --bronze     # Run only Bronze -> Silver
    python run_etl_pipeline.py --silver     # Run only Silver -> Gold
    python run_etl_pipeline.py --init       # Initialize medallion schema only
"""

import argparse
import sys
from datetime import datetime
from configs.db_config import initialize_medallion_schema
from etl.bronze_to_silver_metadata import transform_metadata
from etl.bronze_to_silver_events import transform_events
from etl.silver_to_gold import transform_to_gold


def print_banner():
    """Print pipeline banner."""
    print("\n" + "="*80)
    print(" " * 20 + "MEDALLION ARCHITECTURE ETL PIPELINE")
    print(" " * 30 + "Cricinfo Data Pipeline")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")


def print_summary():
    """Print pipeline completion summary."""
    print("\n" + "="*80)
    print(" " * 25 + "PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*80)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")


def run_bronze_to_silver():
    """Run Bronze to Silver transformations."""
    print("\n" + "━"*80)
    print("STAGE 1: BRONZE → SILVER LAYER")
    print("━"*80 + "\n")

    try:
        # Transform metadata
        print("Step 1.1: Transforming Match Metadata...")
        transform_metadata()

        # Transform events
        print("\nStep 1.2: Transforming Match Events...")
        transform_events()

        print("\n✅ Bronze → Silver transformation completed successfully!\n")
        return True
    except Exception as e:
        print(f"\n❌ Error in Bronze → Silver transformation: {e}\n")
        return False


def run_silver_to_gold():
    """Run Silver to Gold transformations."""
    print("\n" + "━"*80)
    print("STAGE 2: SILVER → GOLD LAYER")
    print("━"*80 + "\n")

    try:
        # Transform to Gold layer
        print("Step 2.1: Creating Analytics Tables...")
        transform_to_gold()

        print("\n✅ Silver → Gold transformation completed successfully!\n")
        return True
    except Exception as e:
        print(f"\n❌ Error in Silver → Gold transformation: {e}\n")
        return False


def run_full_pipeline():
    """Run the complete ETL pipeline."""
    print_banner()

    # Run Bronze -> Silver
    if not run_bronze_to_silver():
        print("❌ Pipeline failed at Bronze → Silver stage")
        return False

    # Run Silver -> Gold
    if not run_silver_to_gold():
        print("❌ Pipeline failed at Silver → Gold stage")
        return False

    print_summary()
    return True


def initialize_schema():
    """Initialize medallion schema tables."""
    print("\n" + "="*80)
    print(" " * 25 + "INITIALIZING MEDALLION SCHEMA")
    print("="*80 + "\n")

    try:
        initialize_medallion_schema()
        print("\n✅ Medallion schema initialized successfully!\n")
        return True
    except Exception as e:
        print(f"\n❌ Error initializing schema: {e}\n")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Medallion Architecture ETL Pipeline for Cricinfo Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                # Run full pipeline (Bronze -> Silver -> Gold)
  %(prog)s --init         # Initialize medallion schema only
  %(prog)s --bronze       # Run Bronze -> Silver transformation only
  %(prog)s --silver       # Run Silver -> Gold transformation only
  %(prog)s --full         # Run full pipeline (explicit)
        """
    )

    parser.add_argument(
        '--init',
        action='store_true',
        help='Initialize medallion schema tables only'
    )
    parser.add_argument(
        '--bronze',
        action='store_true',
        help='Run Bronze -> Silver transformation only'
    )
    parser.add_argument(
        '--silver',
        action='store_true',
        help='Run Silver -> Gold transformation only'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='Run full pipeline (Bronze -> Silver -> Gold)'
    )

    args = parser.parse_args()

    # Determine what to run
    if args.init:
        success = initialize_schema()
    elif args.bronze:
        print_banner()
        success = run_bronze_to_silver()
    elif args.silver:
        print_banner()
        success = run_silver_to_gold()
    elif args.full:
        success = run_full_pipeline()
    else:
        # Default: run full pipeline
        success = run_full_pipeline()

    # Exit with appropriate status code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
