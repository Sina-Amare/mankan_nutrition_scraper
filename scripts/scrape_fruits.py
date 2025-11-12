"""Scrape fruits from mankan.me and append to existing nutritional data."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fruit_search_scraper import FruitSearchPageScraper
from src.fruit_scraper import FruitScraper
from src.incremental_writer import IncrementalWriter
from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("Mankan.me Fruit Scraper")
    logger.info("=" * 60)
    
    # Step 1: Scrape fruit search pages to get all fruit IDs
    logger.info("Step 1: Scraping fruit search pages to get all valid fruit IDs...")
    logger.info("=" * 60)
    
    fruit_search_scraper = FruitSearchPageScraper()
    fruit_ids = fruit_search_scraper.scrape_all_pages(start_page=1, end_page=14, resume=True)
    
    logger.info(f"Found {len(fruit_ids)} valid fruit IDs from search pages")
    fruit_search_scraper.save_fruit_ids(fruit_ids)
    logger.info("=" * 60)
    
    if not fruit_ids:
        logger.error("No fruit IDs found. Exiting.")
        return
    
    # Step 2: Scrape each fruit item
    logger.info("")
    logger.info("=" * 60)
    logger.info("Step 2: Scraping individual fruit items...")
    logger.info("=" * 60)
    
    # Initialize incremental writer to append to existing files
    output_dir = Path("output")
    incremental_writer = IncrementalWriter(
        output_dir=output_dir,
        csv_filename="mankan_nutritional_data.csv",
        excel_filename="mankan_nutritional_data.xlsx",
    )
    
    # Initialize fruit scraper
    fruit_scraper = FruitScraper()
    
    scraped_data = []
    skipped_fruits = []
    
    try:
        total = len(fruit_ids)
        logger.info(f"Scraping {total} fruits...")
        
        for idx, fruit_id in enumerate(fruit_ids, 1):
            logger.info(f"[{idx}/{total}] Scraping fruit ID {fruit_id}...")
            
            try:
                data = fruit_scraper.scrape_fruit(fruit_id)
                
                if data:
                    scraped_data.extend(data)
                    # Immediately save to CSV/Excel (incremental)
                    incremental_writer.add_data(data)
                    logger.info(f"✓ Fruit ID {fruit_id}: {len(data)} row(s) extracted (Total: {len(scraped_data)} rows)")
                else:
                    skipped_fruits.append(fruit_id)
                    logger.warning(f"⚠ Fruit ID {fruit_id}: No data extracted")
            
            except Exception as e:
                skipped_fruits.append(fruit_id)
                logger.error(f"✗ Fruit ID {fruit_id}: Error - {e}", exc_info=True)
        
        # Finalize incremental writer
        incremental_writer.finalize()
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Fruit Scraping Complete!")
        logger.info("=" * 60)
        logger.info(f"Total fruit items scraped: {len(scraped_data)}")
        logger.info(f"Skipped fruits: {len(skipped_fruits)}")
        if skipped_fruits:
            logger.info(f"Skipped fruit IDs: {skipped_fruits[:20]}{'...' if len(skipped_fruits) > 20 else ''}")
        logger.info("=" * 60)
        logger.info("Fruit data has been appended to existing CSV and Excel files.")
    
    except KeyboardInterrupt:
        logger.warning("Fruit scraping interrupted by user.")
        incremental_writer.finalize()
    except Exception as e:
        logger.error(f"Fatal error during fruit scraping: {e}", exc_info=True)
        incremental_writer.finalize()
        raise


if __name__ == "__main__":
    main()

