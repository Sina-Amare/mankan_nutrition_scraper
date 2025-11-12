"""Re-scrape all fruits (IDs 1-105) to fix incorrect data extraction."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.fruit_scraper import FruitScraper
from src.incremental_writer import IncrementalWriter
from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Re-scrape all fruits (IDs 1-105)."""
    csv_path = Path("output/mankan_nutritional_data.csv")
    
    # Read CSV
    logger.info("Reading CSV...")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # Fruit IDs are 1-105 (based on search_fruit.php pages)
    fruit_ids = list(range(1, 106))
    
    logger.info(f"Removing old fruit entries (IDs 1-105) from CSV...")
    # Remove all existing fruit entries
    df_filtered = df[~((df['food_id'].isin(fruit_ids)))]
    removed_count = len(df) - len(df_filtered)
    df_filtered.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Removed {removed_count} old fruit rows")
    
    # Re-scrape all fruits
    logger.info(f"Re-scraping {len(fruit_ids)} fruits with improved extraction...")
    fruit_scraper = FruitScraper()
    incremental_writer = IncrementalWriter(
        output_dir=Path("output"),
        csv_filename="mankan_nutritional_data.csv",
        excel_filename="mankan_nutritional_data.xlsx",
    )
    
    scraped_count = 0
    failed_count = 0
    
    for idx, fruit_id in enumerate(fruit_ids, 1):
        logger.info(f"[{idx}/{len(fruit_ids)}] Re-scraping fruit ID {fruit_id}...")
        try:
            data = fruit_scraper.scrape_fruit(fruit_id)
            if data and len(data) > 0:
                incremental_writer.add_data(data)
                scraped_count += len(data)
                row = data[0]
                logger.info(f"✓ Fruit ID {fruit_id}: {row.get('food_name')} - cal={row.get('calories')}, sugar={row.get('sugar_g')}, fiber={row.get('fiber_g')}")
            else:
                failed_count += 1
                logger.warning(f"⚠ No data extracted for fruit ID {fruit_id}")
        except Exception as e:
            failed_count += 1
            logger.error(f"✗ Error re-scraping fruit ID {fruit_id}: {e}", exc_info=True)
    
    incremental_writer.finalize()
    logger.info("=" * 60)
    logger.info(f"Re-scraping complete!")
    logger.info(f"Successfully scraped: {scraped_count} rows")
    logger.info(f"Failed: {failed_count} fruits")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

