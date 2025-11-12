"""Fix food names that contain questions like 'کالری موز چقدر است؟' -> 'موز'."""

import sys
import re
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.logger_config import setup_logger

logger = setup_logger()


def clean_food_name(name: str) -> str:
    """Clean food name by removing question patterns."""
    if not name or pd.isna(name):
        return name
    
    text = str(name).strip()
    original_text = text
    
    # Remove question patterns like "کالری موز چقدر است؟" -> "موز"
    # Pattern 1: "کالری X چقدر است؟" -> "X"
    text = re.sub(r'^کالری\s+(.+?)\s+چقدر\s+است\??\s*$', r'\1', text, flags=re.IGNORECASE)
    
    # Pattern 2: "کالری X چقدر؟" -> "X"
    text = re.sub(r'^کالری\s+(.+?)\s+چقدر\??\s*$', r'\1', text, flags=re.IGNORECASE)
    
    # Pattern 3: "X چقدر است؟" -> "X"
    text = re.sub(r'^(.+?)\s+چقدر\s+است\??\s*$', r'\1', text, flags=re.IGNORECASE)
    
    # Pattern 4: "X چند کالری دارد؟" -> "X"
    text = re.sub(r'^(.+?)\s+چند\s+کالری\s+دارد\??\s*$', r'\1', text, flags=re.IGNORECASE)
    
    # Pattern 5: "بانک غذایی | X" -> "X" (remove site prefix)
    text = re.sub(r'^بانک\s+غذایی\s*\|\s*(.+?)$', r'\1', text, flags=re.IGNORECASE)
    text = re.sub(r'^بانک\s+غذایی\s+(.+?)$', r'\1', text, flags=re.IGNORECASE)
    
    # Remove trailing question words
    text = re.sub(r'\s+(چقدر|است|هست|می\s*باشد|چند|دارد)\??\s*$', '', text, flags=re.IGNORECASE)
    
    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    # If cleaning didn't change anything, try more aggressive patterns
    if text == original_text:
        # Try to extract just the food name from questions
        # "کالری موز چقدر است؟" -> extract "موز"
        match = re.search(r'کالری\s+([^\s]+(?:\s+[^\s]+)?)\s+چقدر', text, re.IGNORECASE)
        if match:
            text = match.group(1).strip()
        
        # "X چند کالری دارد؟" -> extract "X" (more flexible pattern)
        match = re.search(r'^([^\s]+(?:\s+[^\s]+)?)\s+چند\s+کالری', text, re.IGNORECASE)
        if match:
            text = match.group(1).strip()
    
    # Final cleanup: remove any remaining question words
    text = re.sub(r'\s+(چقدر|است|هست|می\s*باشد|چند|دارد|کالری)\??\s*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^\s*(چقدر|است|هست|می\s*باشد|چند|دارد|کالری)\s+', '', text, flags=re.IGNORECASE)
    
    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def main():
    """Fix food names with question patterns."""
    csv_path = Path("output/mankan_nutritional_data.csv")
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return
    
    logger.info("Reading CSV file...")
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    
    logger.info(f"Total rows: {len(df)}")
    
    # Find rows with question patterns
    question_keywords = ['چقدر', 'چند', 'است؟', 'هست؟']
    rows_with_questions = df[df['food_name'].str.contains('|'.join(question_keywords), case=False, na=False)]
    
    logger.info(f"Found {len(rows_with_questions)} rows with question patterns in food names")
    
    if len(rows_with_questions) > 0:
        # Show examples
        logger.info("Examples of names to fix:")
        for idx, row in rows_with_questions.head(10).iterrows():
            logger.info(f"  '{row['food_name']}' -> '{clean_food_name(row['food_name'])}'")
    
    # Clean all food names
    logger.info("Cleaning all food names...")
    df['food_name'] = df['food_name'].apply(clean_food_name)
    
    # Count fixed names
    fixed_count = len(rows_with_questions)
    
    # Save fixed CSV
    backup_path = csv_path.with_suffix('.csv.backup2')
    if backup_path.exists():
        backup_path.unlink()
    
    # Create backup of current CSV
    import shutil
    shutil.copy2(csv_path, backup_path)
    logger.info(f"Backup created: {backup_path}")
    
    logger.info(f"Saving fixed CSV: {csv_path}")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    
    logger.info("=" * 60)
    logger.info(f"Fixed {fixed_count} food names with question patterns")
    logger.info("=" * 60)
    logger.info("Note: Re-run create_styled_excel.py to update the styled Excel file")


if __name__ == "__main__":
    main()

