"""Main entry point for Mankan.me nutritional database scraper."""

import argparse
import sys
from pathlib import Path

from src.checkpoint import CheckpointManager
from src.logger_config import setup_logger
from src.scraper_fast import FastMankanScraper
from src.search_page_scraper import SearchPageScraper

logger = setup_logger()


def parse_arguments():
    """Parse command line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Scrape nutritional data from mankan.me website"
    )
    
    parser.add_argument(
        "--start-id",
        type=int,
        default=3,
        help="Starting food item ID (default: 3)"
    )
    
    parser.add_argument(
        "--end-id",
        type=int,
        default=1967,
        help="Ending food item ID (default: 1967)"
    )
    
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint"
    )
    
    parser.add_argument(
        "--checkpoint-frequency",
        type=int,
        default=50,
        help="Save checkpoint every N items (default: 50)"
    )
    
    parser.add_argument(
        "--delay-min",
        type=float,
        default=0.5,
        help="Minimum delay between requests in seconds (default: 0.5)"
    )
    
    parser.add_argument(
        "--delay-max",
        type=float,
        default=1.5,
        help="Maximum delay between requests in seconds (default: 1.5)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output",
        help="Output directory for Excel and CSV files (default: output)"
    )
    
    parser.add_argument(
        "--excel-filename",
        type=str,
        default="mankan_nutritional_data.xlsx",
        help="Excel output filename (default: mankan_nutritional_data.xlsx)"
    )
    
    parser.add_argument(
        "--csv-filename",
        type=str,
        default="mankan_nutritional_data.csv",
        help="CSV output filename (default: mankan_nutritional_data.csv)"
    )
    
    parser.add_argument(
        "--use-search-pages",
        action="store_true",
        help="Scrape search pages first to get all valid food IDs (recommended)"
    )
    
    return parser.parse_args()


def main():
    """Main execution function."""
    args = parse_arguments()
    
    logger.info("=" * 60)
    logger.info("Mankan.me Nutritional Database Scraper")
    logger.info("=" * 60)
    logger.info(f"ID Range: {args.start_id} - {args.end_id}")
    logger.info(f"Checkpoint frequency: {args.checkpoint_frequency}")
    logger.info(f"Request delay: {args.delay_min}-{args.delay_max} seconds")
    
    try:
        # Initialize components
        checkpoint_manager = CheckpointManager()
        
        # Load checkpoint if resuming
        if args.resume:
            checkpoint_data = checkpoint_manager.load()
            logger.info(f"Resuming from checkpoint: {len(checkpoint_data.get('completed_ids', []))} items completed")
        else:
            # Optionally clear checkpoint for fresh start
            logger.info("Starting fresh scrape (use --resume to continue from checkpoint)")
        
        # Initialize scraper with output configuration
        output_dir = Path(args.output_dir)
        
        # Get food IDs to scrape
        food_ids = None
        if args.use_search_pages:
            logger.info("=" * 60)
            logger.info("Step 1: Scraping search pages to get all valid food IDs...")
            logger.info("=" * 60)
            search_scraper = SearchPageScraper()
            food_ids = search_scraper.scrape_all_pages()
            logger.info(f"Found {len(food_ids)} valid food IDs from search pages")
            search_scraper.save_food_ids(food_ids)
            logger.info("=" * 60)
        
        scraper = FastMankanScraper(
            start_id=args.start_id,
            end_id=args.end_id,
            checkpoint_manager=checkpoint_manager,
            checkpoint_frequency=args.checkpoint_frequency,
            output_dir=output_dir,
            csv_filename=args.csv_filename,
            excel_filename=args.excel_filename,
        )
        
        # Run scraper (data is saved incrementally during scraping)
        logger.info("=" * 60)
        logger.info("Step 2: Starting scraping process...")
        logger.info("Data will be saved incrementally to CSV and Excel files.")
        logger.info("=" * 60)
        scraped_data = scraper.scrape_all(food_ids=food_ids)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Scraping Complete!")
        logger.info("=" * 60)
        logger.info(f"Total food items scraped: {len(set(row.get('food_id') for row in scraped_data))}")
        logger.info(f"Total data rows: {len(scraped_data)}")
        logger.info(f"Excel file: {output_dir / args.excel_filename}")
        logger.info(f"CSV file: {output_dir / args.csv_filename}")
        logger.info(f"Skipped items: {len(scraper.skipped_ids)}")
        if scraper.skipped_ids:
            logger.info(f"  Skipped items log: data/logs/skipped_items.json")
            logger.info(f"  Use 'python retry_skipped.py' to retry skipped items")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user. Checkpoint saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

