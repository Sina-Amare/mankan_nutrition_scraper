"""Fix CSV column mismatch by adding sugar_g column to existing data."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Fix CSV by ensuring all rows have the correct columns."""
    csv_path = Path("output/mankan_nutritional_data.csv")
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return
    
    logger.info(f"Reading CSV file: {csv_path}")
    
    # Read CSV with error handling for inconsistent columns
    try:
        # Try reading normally first
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        logger.info(f"Successfully read CSV with {len(df)} rows")
    except pd.errors.ParserError as e:
        logger.warning(f"Parser error detected: {e}")
        logger.info("Attempting to read with error handling...")
        
        # Read with error handling
        df = pd.read_csv(
            csv_path,
            encoding='utf-8-sig',
            on_bad_lines='skip',  # Skip bad lines
            engine='python'  # Use Python engine for better error handling
        )
        logger.info(f"Read CSV with {len(df)} rows (some lines may have been skipped)")
    
    # Expected columns
    expected_columns = [
        "food_name",
        "measurement_unit",
        "measurement_value",
        "calories",
        "carbs_g",
        "protein_g",
        "fat_g",
        "fiber_g",
        "sugar_g",
        "food_id",
    ]
    
    # Check current columns
    current_columns = list(df.columns)
    logger.info(f"Current columns: {current_columns}")
    logger.info(f"Expected columns: {expected_columns}")
    
    # Add missing columns with default values
    for col in expected_columns:
        if col not in df.columns:
            logger.info(f"Adding missing column: {col}")
            if col == "sugar_g":
                df[col] = 0.0  # Default sugar to 0 for existing food data
            else:
                df[col] = None
    
    # Reorder columns to match expected order
    df = df.reindex(columns=expected_columns)
    
    # Fill NaN values appropriately
    numeric_cols = ["calories", "carbs_g", "protein_g", "fat_g", "fiber_g", "sugar_g", "measurement_value"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    # Ensure food_id is integer
    df["food_id"] = pd.to_numeric(df["food_id"], errors='coerce').fillna(0).astype(int)
    
    # Save fixed CSV
    backup_path = csv_path.with_suffix('.csv.backup')
    logger.info(f"Creating backup: {backup_path}")
    csv_path.rename(backup_path)
    
    logger.info(f"Saving fixed CSV: {csv_path}")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    
    logger.info(f"Fixed CSV saved. Total rows: {len(df)}")
    logger.info(f"Columns: {list(df.columns)}")


if __name__ == "__main__":
    main()

