"""Retry script for skipped items from the scraper."""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.checkpoint import CheckpointManager
from src.incremental_writer import IncrementalWriter
from src.logger_config import setup_logger
from src.scraper_fast import FastMankanScraper
from src.skipped_logger import SkippedLogger

logger = setup_logger()


def parse_arguments():
    """Parse command line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Retry scraping skipped items from mankan.me"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output",
        help="Output directory for Excel and CSV files (default: output)"
    )
    
    parser.add_argument(
        "--csv-filename",
        type=str,
        default="mankan_nutritional_data.csv",
        help="CSV output filename (default: mankan_nutritional_data.csv)"
    )
    
    parser.add_argument(
        "--excel-filename",
        type=str,
        default="mankan_nutritional_data.xlsx",
        help="Excel output filename (default: mankan_nutritional_data.xlsx)"
    )
    
    parser.add_argument(
        "--skip-log",
        type=str,
        default="data/logs/skipped_items.json",
        help="Path to skipped items log file (default: data/logs/skipped_items.json)"
    )
    
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help="Maximum number of items to retry (default: all)"
    )
    
    return parser.parse_args()


def analyze_skipped_items(skipped_items):
    """Analyze skipped items by error type.
    
    Args:
        skipped_items: List of skipped item dictionaries
    Returns:
        Dictionary with error type counts
    """
    error_counts = {}
    for item in skipped_items:
        error_type = item.get("error_type", "Unknown")
        reason = item.get("reason", "unknown")
        key = f"{error_type} ({reason})"
        error_counts[key] = error_counts.get(key, 0) + 1
    
    return error_counts


def main():
    """Main execution function."""
    args = parse_arguments()
    
    logger.info("=" * 60)
    logger.info("Mankan.me Skipped Items Retry Script")
    logger.info("=" * 60)
    
    # Load skipped items
    skipped_log_path = Path(args.skip_log)
    if not skipped_log_path.exists():
        logger.error(f"Skipped items log not found: {skipped_log_path}")
        logger.info("No skipped items to retry. Exiting.")
        sys.exit(0)
    
    skipped_logger = SkippedLogger(log_file=skipped_log_path)
    skipped_items = skipped_logger.get_skipped_items()
    
    if not skipped_items:
        logger.info("No skipped items found in log. Exiting.")
        sys.exit(0)
    
    # Analyze skipped items
    logger.info(f"Found {len(skipped_items)} skipped items")
    error_analysis = analyze_skipped_items(skipped_items)
    logger.info("Error type distribution:")
    for error_type, count in sorted(error_analysis.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {error_type}: {count}")
    
    # Get list of food IDs to retry
    skipped_ids = skipped_logger.get_skipped_ids()
    
    if args.max_retries:
        skipped_ids = skipped_ids[:args.max_retries]
        logger.info(f"Limiting retry to first {args.max_retries} items")
    
    if not skipped_ids:
        logger.info("No items to retry. Exiting.")
        sys.exit(0)
    
    logger.info(f"Retrying {len(skipped_ids)} items...")
    logger.info("=" * 60)
    
    # Determine ID range (min to max)
    min_id = min(skipped_ids)
    max_id = max(skipped_ids)
    
    # Initialize scraper with the ID range
    output_dir = Path(args.output_dir)
    checkpoint_manager = CheckpointManager()
    
    scraper = FastMankanScraper(
        start_id=min_id,
        end_id=max_id,
        checkpoint_manager=checkpoint_manager,
        output_dir=output_dir,
        csv_filename=args.csv_filename,
        excel_filename=args.excel_filename,
    )
    
    # Use the scraper's incremental writer (already initialized)
    incremental_writer = scraper.incremental_writer
    
    # Track retry results
    retry_successful = []
    retry_failed = []
    
    try:
        # Retry each skipped item
        for idx, food_id in enumerate(skipped_ids, 1):
            logger.info(f"[{idx}/{len(skipped_ids)}] Retrying ID {food_id}...")
            
            # Get error details for this item
            item_details = next(
                (item for item in skipped_items if item.get("food_id") == food_id),
                None
            )
            if item_details:
                logger.info(f"  Previous error: {item_details.get('error_type')} - {item_details.get('error_message', '')[:100]}")
            
            try:
                # Scrape the item
                data = scraper.scrape_item(food_id)
                
                if data:
                    # Success! Save data and remove from skipped list
                    incremental_writer.add_data(data)
                    skipped_logger.remove_skipped(food_id)
                    retry_successful.append(food_id)
                    logger.info(f"  ✓ Success: {len(data)} measurement(s) extracted")
                else:
                    # Still no data
                    retry_failed.append(food_id)
                    logger.warning(f"  ✗ Still no data extracted")
            
            except Exception as e:
                # Still failed
                retry_failed.append(food_id)
                logger.error(f"  ✗ Error: {e}")
                # Update error log with new error
                skipped_logger.log_skipped(
                    food_id=food_id,
                    error=e,
                    reason="retry_failed"
                )
        
        # Finalize incremental writer
        incremental_writer.finalize()
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Retry Complete!")
        logger.info("=" * 60)
        logger.info(f"Successfully retried: {len(retry_successful)} items")
        logger.info(f"Still failed: {len(retry_failed)} items")
        
        if retry_successful:
            logger.info(f"Successfully retried IDs: {retry_successful[:20]}{'...' if len(retry_successful) > 20 else ''}")
        
        if retry_failed:
            logger.info(f"Still failed IDs: {retry_failed[:20]}{'...' if len(retry_failed) > 20 else ''}")
        
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.warning("Retry interrupted by user.")
        incremental_writer.finalize()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error during retry: {e}", exc_info=True)
        incremental_writer.finalize()
        sys.exit(1)


if __name__ == "__main__":
    main()

