"""Re-scrape fruits to fix incorrect data extraction."""

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
    """Re-scrape fruits that were incorrectly extracted."""
    csv_path = Path("output/mankan_nutritional_data.csv")
    
    # Read CSV to find fruit IDs (IDs 1-105 based on the data)
    logger.info("Reading CSV to find fruit entries...")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # Find fruit entries - they have sugar_g > 0 or are in the fruit ID range
    # Based on the data, fruits seem to be IDs 1-105
    # But we should identify them by checking if they have incorrect data
    fruit_ids_to_rescrape = []
    
    # Check for fruits with incorrect data patterns:
    # - calories < 20 (likely wrong - fruits should have more calories)
    # - sugar_g = 0 but fiber_g > 0 (inconsistent)
    # - Name starts with "Fruit" (extraction failed)
    
    for idx, row in df.iterrows():
        food_id = row['food_id']
        food_name = str(row['food_name'])
        calories = row.get('calories', 0)
        sugar_g = row.get('sugar_g', 0)
        fiber_g = row.get('fiber_g', 0)
        
        # Check if this looks like a fruit entry that needs re-scraping
        if (food_id <= 105 and  # Fruit IDs are typically 1-105
            (food_name.startswith("Fruit") or  # Name extraction failed
             (calories < 20 and fiber_g > 0) or  # Suspiciously low calories
             (sugar_g == 0 and fiber_g > 0 and calories < 50))):  # Missing sugar data
            if food_id not in fruit_ids_to_rescrape:
                fruit_ids_to_rescrape.append(food_id)
                logger.info(f"Found fruit to re-scrape: ID {food_id} - {food_name} (cal={calories}, sugar={sugar_g}, fiber={fiber_g})")
    
    if not fruit_ids_to_rescrape:
        logger.info("No fruits found that need re-scraping.")
        return
    
    logger.info(f"Found {len(fruit_ids_to_rescrape)} fruits to re-scrape")
    
    # Remove old fruit entries from CSV
    logger.info("Removing old fruit entries from CSV...")
    df_filtered = df[~((df['food_id'].isin(fruit_ids_to_rescrape)) & (df['food_id'] <= 105))]
    df_filtered.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Removed {len(df) - len(df_filtered)} old fruit rows")
    
    # Re-scrape fruits
    logger.info("Re-scraping fruits with improved extraction...")
    fruit_scraper = FruitScraper()
    incremental_writer = IncrementalWriter(
        output_dir=Path("output"),
        csv_filename="mankan_nutritional_data.csv",
        excel_filename="mankan_nutritional_data.xlsx",
    )
    
    scraped_count = 0
    for fruit_id in fruit_ids_to_rescrape:
        logger.info(f"Re-scraping fruit ID {fruit_id}...")
        try:
            data = fruit_scraper.scrape_fruit(fruit_id)
            if data:
                incremental_writer.add_data(data)
                scraped_count += len(data)
                logger.info(f"✓ Re-scraped fruit ID {fruit_id}: {data[0].get('food_name')} (cal={data[0].get('calories')}, sugar={data[0].get('sugar_g')}, fiber={data[0].get('fiber_g')})")
            else:
                logger.warning(f"⚠ No data extracted for fruit ID {fruit_id}")
        except Exception as e:
            logger.error(f"✗ Error re-scraping fruit ID {fruit_id}: {e}", exc_info=True)
    
    incremental_writer.finalize()
    logger.info(f"Re-scraping complete! Scraped {scraped_count} rows for {len(fruit_ids_to_rescrape)} fruits")


if __name__ == "__main__":
    main()

