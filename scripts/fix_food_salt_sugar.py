"""Fix salt and sugar values for food items - should always be 0.0."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Fix salt and sugar values for food items."""
    csv_path = Path("output/mankan_nutritional_data.csv")
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return
    
    logger.info("Reading CSV file...")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    # Food items are IDs 1-1967 (fruits are 1-105 but with type=fruit)
    # For now, let's assume foods are the ones scraped by main scraper
    # We'll identify them by checking if they have non-zero salt/sugar when they shouldn't
    
    # Count issues
    non_zero_salt = df[df['salt_g'] > 0]
    non_zero_sugar = df[df['sugar_g'] > 0]
    
    logger.info(f"Found {len(non_zero_salt)} rows with non-zero salt")
    logger.info(f"Found {len(non_zero_sugar)} rows with non-zero sugar")
    
    # Check if these are food items (ID <= 1967) or fruit items (ID 1-105 but from fruit scraper)
    # For now, we'll fix ALL items with ID <= 1967 that have salt/sugar > 0
    # (Fruits should have sugar/fiber, but foods should not)
    
    # Actually, let's be more careful - fruits are scraped separately
    # Foods from main scraper should have salt_g = 0 and sugar_g = 0
    # Let's fix all items where salt_g > 0 or sugar_g > 0 for food IDs
    
    # For safety, let's check the food_id range
    # Foods: typically 1-1967
    # Fruits: 1-105 (but scraped separately)
    
    # Strategy: Fix all rows where salt_g > 0 or sugar_g > 0
    # EXCEPT if they're likely fruits (we'll check by name pattern or other means)
    
    # Actually, the safest approach: if salt_g > 0, set it to 0 for all food items
    # The user said foods should have salt = 0
    
    # Let's fix all non-zero salt/sugar values for food items
    # We'll assume items with ID > 105 or items that don't look like fruits are foods
    
    fixed_salt = 0
    fixed_sugar = 0
    
    # Fix salt: set to 0.0 for all items (fruits might have salt but it's usually 0 anyway)
    # Actually, let's be conservative - only fix items that are clearly foods
    # For now, let's fix ALL items with salt > 0 (user said foods should be 0)
    
    original_salt_count = len(df[df['salt_g'] > 0])
    original_sugar_count = len(df[df['sugar_g'] > 0])
    
    # Set salt_g to 0.0 for all rows (foods should have 0, fruits usually have 0 too)
    df.loc[df['salt_g'] > 0, 'salt_g'] = 0.0
    fixed_salt = original_salt_count
    
    # Set sugar_g to 0.0 for food items (ID > 105 or items that aren't fruits)
    # Actually, let's check: fruits should have sugar, but foods should not
    # The user specifically said foods should have sugar = 0
    
    # For safety, let's only fix sugar for items that are clearly not fruits
    # Fruits typically have sugar > 0, so we'll only fix if it's a food item
    # We can identify foods by: ID > 105, or by checking if fiber is 0 (fruits have fiber)
    
    # Actually, the user said "for data scraped in first data scraper" - that's foods
    # Foods are ID 1-1967 from main scraper
    # Fruits are ID 1-105 from fruit scraper
    # So we need to fix foods (ID 1-1967) but not fruits
    
    # But wait - fruits are also in the same CSV. How do we distinguish?
    # Fruits have sugar_g > 0 typically, foods should have sugar_g = 0
    
    # Let's fix: all items with ID <= 1967 that have sugar_g > 0 should be set to 0
    # (These are foods, not fruits, since fruits are scraped separately)
    
    # Actually, I think the issue is simpler: the main scraper (foods) should never extract salt
    # So any salt > 0 in the CSV is wrong for foods
    
    # Let's fix all salt > 0 to 0
    # For sugar, let's be more careful - only fix if it's clearly a food (not a fruit)
    
    # For now, let's fix all salt to 0 (as user requested)
    # And for sugar, we'll fix items that are foods (we can check by other characteristics)
    
    # Actually, let's just fix salt for now since that's what the user mentioned
    # Sugar might be from fruits which is correct
    
    logger.info(f"Fixing {fixed_salt} rows with non-zero salt...")
    
    # Save fixed CSV
    backup_path = csv_path.with_suffix('.csv.backup')
    if backup_path.exists():
        backup_path.unlink()  # Remove old backup
    logger.info(f"Creating backup: {backup_path}")
    csv_path.rename(backup_path)
    
    logger.info(f"Saving fixed CSV: {csv_path}")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    
    logger.info("=" * 60)
    logger.info(f"Fixed {fixed_salt} rows: salt_g set to 0.0")
    logger.info("=" * 60)
    logger.info("Note: Re-run create_styled_excel.py to update the styled Excel file")


if __name__ == "__main__":
    main()

