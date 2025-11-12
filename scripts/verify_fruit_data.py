"""Verify fruit data completeness and correctness before merging."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.logger_config import setup_logger

logger = setup_logger()


def verify_fruit_data(csv_path: Path) -> bool:
    """Verify fruit data in CSV file.
    
    Args:
        csv_path: Path to fruit CSV file
    Returns:
        True if verification passes, False otherwise
    """
    logger.info("=" * 60)
    logger.info("Fruit Data Verification")
    logger.info("=" * 60)
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return False
    
    logger.info(f"Reading fruit data from: {csv_path}")
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return False
    
    logger.info(f"Total rows in CSV: {len(df)}")
    
    # Required columns
    required_columns = [
        "food_id", "food_name", "measurement_unit", "measurement_value",
        "calories", "fat_g", "protein_g", "carbs_g", "fiber_g", "sugar_g", "salt_g"
    ]
    
    # Check 1: All required columns present
    logger.info("")
    logger.info("Check 1: Required columns...")
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"❌ Missing columns: {missing_columns}")
        return False
    logger.info("✓ All required columns present")
    
    # Check 2: Measurement unit is always "100 گرم"
    logger.info("")
    logger.info("Check 2: Measurement unit...")
    invalid_units = df[df['measurement_unit'] != '100 گرم']
    if len(invalid_units) > 0:
        logger.error(f"❌ Found {len(invalid_units)} rows with invalid measurement_unit:")
        logger.error(f"   Invalid values: {invalid_units['measurement_unit'].unique().tolist()}")
        return False
    logger.info("✓ All measurement units are '100 گرم'")
    
    # Check 3: Measurement value is always 100.0
    logger.info("")
    logger.info("Check 3: Measurement value...")
    invalid_values = df[df['measurement_value'] != 100.0]
    if len(invalid_values) > 0:
        logger.error(f"❌ Found {len(invalid_values)} rows with invalid measurement_value:")
        logger.error(f"   Invalid values: {invalid_values['measurement_value'].unique().tolist()}")
        return False
    logger.info("✓ All measurement values are 100.0")
    
    # Check 4: Required fields are present and valid
    logger.info("")
    logger.info("Check 4: Required fields (name, calories, sugar, fiber)...")
    
    # Name should not be empty
    empty_names = df[df['food_name'].isna() | (df['food_name'].str.strip() == '')]
    if len(empty_names) > 0:
        logger.error(f"❌ Found {len(empty_names)} rows with empty food names")
        return False
    logger.info("✓ All food names are present")
    
    # Calories should be numeric and > 0
    invalid_calories = df[df['calories'].isna() | (df['calories'] <= 0)]
    if len(invalid_calories) > 0:
        logger.error(f"❌ Found {len(invalid_calories)} rows with invalid calories (should be > 0)")
        logger.error(f"   Sample: {invalid_calories[['food_id', 'food_name', 'calories']].head().to_dict('records')}")
        return False
    logger.info("✓ All calories are valid (> 0)")
    
    # Sugar should be numeric and >= 0
    invalid_sugar = df[df['sugar_g'].isna() | (df['sugar_g'] < 0)]
    if len(invalid_sugar) > 0:
        logger.error(f"❌ Found {len(invalid_sugar)} rows with invalid sugar (should be >= 0)")
        return False
    logger.info("✓ All sugar values are valid (>= 0)")
    
    # Fiber should be numeric and >= 0
    invalid_fiber = df[df['fiber_g'].isna() | (df['fiber_g'] < 0)]
    if len(invalid_fiber) > 0:
        logger.error(f"❌ Found {len(invalid_fiber)} rows with invalid fiber (should be >= 0)")
        return False
    logger.info("✓ All fiber values are valid (>= 0)")
    
    # Check 5: Other fields should be 0.0
    logger.info("")
    logger.info("Check 5: Other fields (fat, protein, carbs, salt) should be 0.0...")
    
    invalid_fat = df[df['fat_g'] != 0.0]
    if len(invalid_fat) > 0:
        logger.error(f"❌ Found {len(invalid_fat)} rows with non-zero fat_g")
        return False
    logger.info("✓ All fat_g values are 0.0")
    
    invalid_protein = df[df['protein_g'] != 0.0]
    if len(invalid_protein) > 0:
        logger.error(f"❌ Found {len(invalid_protein)} rows with non-zero protein_g")
        return False
    logger.info("✓ All protein_g values are 0.0")
    
    invalid_carbs = df[df['carbs_g'] != 0.0]
    if len(invalid_carbs) > 0:
        logger.error(f"❌ Found {len(invalid_carbs)} rows with non-zero carbs_g")
        return False
    logger.info("✓ All carbs_g values are 0.0")
    
    invalid_salt = df[df['salt_g'] != 0.0]
    if len(invalid_salt) > 0:
        logger.error(f"❌ Found {len(invalid_salt)} rows with non-zero salt_g")
        return False
    logger.info("✓ All salt_g values are 0.0")
    
    # Check 6: No duplicate fruit IDs
    logger.info("")
    logger.info("Check 6: Duplicate fruit IDs...")
    duplicate_ids = df[df.duplicated(subset=['food_id'], keep=False)]
    if len(duplicate_ids) > 0:
        logger.warning(f"⚠ Found {len(duplicate_ids)} rows with duplicate fruit IDs:")
        logger.warning(f"   Duplicate IDs: {duplicate_ids['food_id'].unique().tolist()}")
        # This is a warning, not an error (fruits might have multiple rows if there are multiple measurements)
        # But for fruits, we expect only one row per ID (100g measurement)
        if len(duplicate_ids) > len(duplicate_ids['food_id'].unique()):
            logger.error("❌ Multiple rows per fruit ID found (expected only one row per fruit)")
            return False
    logger.info("✓ No duplicate fruit IDs")
    
    # Check 7: Expected number of fruits
    logger.info("")
    logger.info("Check 7: Expected number of fruits...")
    unique_fruit_ids = df['food_id'].nunique()
    logger.info(f"   Unique fruit IDs: {unique_fruit_ids}")
    logger.info(f"   Total rows: {len(df)}")
    
    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("✓ VERIFICATION PASSED")
    logger.info("=" * 60)
    logger.info(f"Total fruit rows: {len(df)}")
    logger.info(f"Unique fruit IDs: {unique_fruit_ids}")
    logger.info("")
    logger.info("All checks passed! Data is ready for merging.")
    logger.info("=" * 60)
    
    return True


def main():
    """Main verification function."""
    temp_csv = Path("output/fruits_temp.csv")
    
    success = verify_fruit_data(temp_csv)
    
    if success:
        logger.info("")
        logger.info("Next step: Run scripts/merge_fruit_data.py to merge with main data")
        sys.exit(0)
    else:
        logger.error("")
        logger.error("Verification failed! Please fix the issues before merging.")
        sys.exit(1)


if __name__ == "__main__":
    main()

