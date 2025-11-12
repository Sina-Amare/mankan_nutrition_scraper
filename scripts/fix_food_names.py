"""Fix food names that show as 'Food X' instead of actual names."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.scraper_fast import FastMankanScraper
from src.checkpoint import CheckpointManager
from src.incremental_writer import IncrementalWriter
from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Fix food names in CSV that show as 'Food X'."""
    csv_path = Path("output/mankan_nutritional_data.csv")
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return
    
    logger.info("Reading CSV to find items with incorrect names...")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # Find items with "Food X" or "Fruit X" names
    incorrect_names = df[df['food_name'].str.startswith('Food ') | df['food_name'].str.startswith('Fruit ')]
    
    if incorrect_names.empty:
        logger.info("No items with incorrect names found!")
        return
    
    # Get unique food IDs that need fixing
    food_ids_to_fix = incorrect_names['food_id'].unique().tolist()
    logger.info(f"Found {len(food_ids_to_fix)} food IDs with incorrect names: {food_ids_to_fix}")
    
    # Remove old entries with incorrect names
    logger.info("Removing old entries with incorrect names...")
    df_fixed = df[~((df['food_id'].isin(food_ids_to_fix)) & (df['food_name'].str.startswith('Food ') | df['food_name'].str.startswith('Fruit ')))]
    removed_count = len(df) - len(df_fixed)
    logger.info(f"Removed {removed_count} rows with incorrect names")
    
    # Save cleaned CSV
    df_fixed.to_csv(csv_path, index=False, encoding="utf-8-sig")
    
    # Re-scrape items with improved name extraction
    logger.info("Re-scraping items with improved name extraction...")
    checkpoint_manager = CheckpointManager()
    incremental_writer = IncrementalWriter(
        output_dir=Path("output"),
        csv_filename="mankan_nutritional_data.csv",
        excel_filename="mankan_nutritional_data.xlsx",
    )
    
    scraper = FastMankanScraper(
        start_id=min(food_ids_to_fix),
        end_id=max(food_ids_to_fix),
        checkpoint_manager=checkpoint_manager,
        checkpoint_frequency=10,
        output_dir=Path("output"),
    )
    
    # Remove these IDs from completed_ids so they get scraped
    scraper.completed_ids = [cid for cid in scraper.completed_ids if cid not in food_ids_to_fix]
    
    try:
        scraper._init_browser()
        
        fixed_count = 0
        for idx, food_id in enumerate(food_ids_to_fix, 1):
            logger.info(f"[{idx}/{len(food_ids_to_fix)}] Fixing ID {food_id}...")
            try:
                data = scraper.scrape_item(food_id)
                
                if data:
                    # Check if name was extracted correctly
                    new_name = data[0].get('food_name', '')
                    if new_name and not new_name.startswith('Food ') and not new_name.startswith('Fruit '):
                        scraper.incremental_writer.add_data(data)
                        scraper.completed_ids.append(food_id)
                        fixed_count += 1
                        logger.info(f"✓ ID {food_id}: Fixed name to '{new_name}'")
                    else:
                        logger.warning(f"⚠ ID {food_id}: Still has incorrect name '{new_name}'")
                else:
                    logger.warning(f"⚠ ID {food_id}: No data extracted")
            
            except Exception as e:
                logger.error(f"✗ ID {food_id}: Error - {e}", exc_info=True)
        
        scraper.incremental_writer.finalize()
        
        logger.info("=" * 60)
        logger.info(f"Fixed {fixed_count}/{len(food_ids_to_fix)} food names")
        logger.info("=" * 60)
    
    finally:
        scraper._close_browser()


if __name__ == "__main__":
    main()

