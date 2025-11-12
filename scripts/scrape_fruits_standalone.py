"""Scrape fruits from mankan.me to a temporary file (separate from main data)."""

import sys
import json
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fruit_search_scraper import FruitSearchPageScraper
from src.fruit_scraper import FruitScraper
from src.incremental_writer import IncrementalWriter
from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Scrape fruits separately to temporary files."""
    logger.info("=" * 60)
    logger.info("Mankan.me Fruit Scraper (Standalone)")
    logger.info("=" * 60)
    
    # Output to temporary files
    output_dir = Path("output")
    temp_csv = output_dir / "fruits_temp.csv"
    temp_excel = output_dir / "fruits_temp.xlsx"
    checkpoint_file = Path("data/fruit_scraping_checkpoint.json")
    
    # Load checkpoint if exists
    checkpoint_data = {}
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            logger.info(f"Loaded checkpoint: {len(checkpoint_data.get('completed_ids', []))} fruits already scraped")
        except Exception as e:
            logger.warning(f"Error loading checkpoint: {e}. Starting fresh.")
    
    completed_ids = set(checkpoint_data.get("completed_ids", []))
    
    # Step 1: Scrape fruit search pages to get all fruit IDs
    logger.info("")
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
    
    # Step 2: Scrape each fruit item to temporary files
    logger.info("")
    logger.info("=" * 60)
    logger.info("Step 2: Scraping individual fruit items to temporary files...")
    logger.info("=" * 60)
    
    # Initialize incremental writer for temporary files
    incremental_writer = IncrementalWriter(
        output_dir=output_dir,
        csv_filename="fruits_temp.csv",
        excel_filename="fruits_temp.xlsx",
    )
    
    # Initialize fruit scraper
    fruit_scraper = FruitScraper()
    
    scraped_data = []
    skipped_fruits = []
    fruits_to_scrape = [fid for fid in fruit_ids if fid not in completed_ids]
    
    try:
        total = len(fruit_ids)
        remaining = len(fruits_to_scrape)
        logger.info(f"Total fruits: {total}, Already scraped: {len(completed_ids)}, Remaining: {remaining}")
        
        for idx, fruit_id in enumerate(fruits_to_scrape, 1):
            logger.info(f"[{idx}/{remaining}] Scraping fruit ID {fruit_id}...")
            
            try:
                data = fruit_scraper.scrape_fruit(fruit_id)
                
                if data:
                    scraped_data.extend(data)
                    # Immediately save to temporary CSV/Excel (incremental)
                    incremental_writer.add_data(data)
                    completed_ids.add(fruit_id)
                    logger.info(f"✓ Fruit ID {fruit_id}: {len(data)} row(s) extracted (Total: {len(scraped_data)} rows)")
                    
                    # Save checkpoint every 10 fruits
                    if len(completed_ids) % 10 == 0:
                        checkpoint_data = {
                            "completed_ids": sorted(list(completed_ids)),
                            "total_scraped": len(scraped_data)
                        }
                        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(checkpoint_file, 'w', encoding='utf-8') as f:
                            json.dump(checkpoint_data, f, indent=2)
                        logger.info(f"💾 Checkpoint saved: {len(completed_ids)}/{total} fruits")
                else:
                    skipped_fruits.append(fruit_id)
                    logger.warning(f"⚠ Fruit ID {fruit_id}: No data extracted")
            
            except Exception as e:
                skipped_fruits.append(fruit_id)
                logger.error(f"✗ Fruit ID {fruit_id}: Error - {e}", exc_info=True)
        
        # Finalize incremental writer
        incremental_writer.finalize()
        
        # Final checkpoint save
        checkpoint_data = {
            "completed_ids": sorted(list(completed_ids)),
            "total_scraped": len(scraped_data)
        }
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Fruit Scraping Complete!")
        logger.info("=" * 60)
        logger.info(f"Total fruit items scraped: {len(scraped_data)}")
        logger.info(f"Completed fruit IDs: {len(completed_ids)}/{total}")
        logger.info(f"Skipped fruits: {len(skipped_fruits)}")
        if skipped_fruits:
            logger.info(f"Skipped fruit IDs: {skipped_fruits[:20]}{'...' if len(skipped_fruits) > 20 else ''}")
        logger.info("")
        logger.info(f"Temporary files created:")
        logger.info(f"  - CSV: {temp_csv}")
        logger.info(f"  - Excel: {temp_excel}")
        logger.info("")
        logger.info("Next step: Run scripts/verify_fruit_data.py to verify the data")
        logger.info("=" * 60)
    
    except KeyboardInterrupt:
        logger.warning("Fruit scraping interrupted by user.")
        incremental_writer.finalize()
        # Save checkpoint on interrupt
        checkpoint_data = {
            "completed_ids": sorted(list(completed_ids)),
            "total_scraped": len(scraped_data)
        }
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2)
        logger.info("Checkpoint saved. You can resume by running this script again.")
    except Exception as e:
        logger.error(f"Fatal error during fruit scraping: {e}", exc_info=True)
        incremental_writer.finalize()
        raise


if __name__ == "__main__":
    main()

