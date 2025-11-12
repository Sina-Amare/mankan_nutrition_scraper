"""Retry scraping skipped food items that have data but were missed."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scraper_fast import FastMankanScraper
from src.checkpoint import CheckpointManager
from src.skipped_logger import SkippedLogger
from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Retry skipped food items."""
    logger.info("=" * 60)
    logger.info("Retrying Skipped Food Items")
    logger.info("=" * 60)
    
    # Load skipped items
    skipped_logger = SkippedLogger()
    skipped_items = skipped_logger.get_skipped_items()
    
    if not skipped_items:
        logger.info("No skipped items found.")
        return
    
    logger.info(f"Found {len(skipped_items)} skipped items to retry")
    
    # Extract food IDs
    skipped_ids = [item['food_id'] for item in skipped_items]
    logger.info(f"Skipped IDs: {skipped_ids}")
    
    # Initialize scraper
    checkpoint_manager = CheckpointManager()
    output_dir = Path("output")
    
    scraper = FastMankanScraper(
        start_id=min(skipped_ids),
        end_id=max(skipped_ids),
        checkpoint_manager=checkpoint_manager,
        checkpoint_frequency=10,
        output_dir=output_dir,
    )
    
    # Remove skipped IDs from completed_ids so they get scraped again
    scraper.completed_ids = [cid for cid in scraper.completed_ids if cid not in skipped_ids]
    
    logger.info(f"Retrying {len(skipped_ids)} skipped items...")
    
    # Scrape only the skipped IDs
    try:
        scraper._init_browser()
        
        for food_id in skipped_ids:
            logger.info(f"Retrying ID {food_id}...")
            try:
                data = scraper.scrape_item(food_id)
                
                if data:
                    scraper.scraped_data.extend(data)
                    scraper.completed_ids.append(food_id)
                    scraper.incremental_writer.add_data(data)
                    logger.info(f"✓ ID {food_id}: {len(data)} row(s) extracted")
                    # Remove from skipped log
                    skipped_logger.remove_skipped_item(food_id)
                else:
                    logger.warning(f"⚠ ID {food_id}: Still no data extracted")
            
            except Exception as e:
                logger.error(f"✗ ID {food_id}: Error - {e}", exc_info=True)
        
        scraper.incremental_writer.finalize()
        scraper.checkpoint_manager.save(
            completed_ids=scraper.completed_ids,
            data=scraper.scraped_data
        )
        
        logger.info("=" * 60)
        logger.info("Retry complete!")
        logger.info(f"Successfully retried: {len([sid for sid in skipped_ids if sid in scraper.completed_ids])} items")
        logger.info("=" * 60)
    
    finally:
        scraper._close_browser()


if __name__ == "__main__":
    main()

