"""Data validation and cleaning for scraped nutritional data."""

import re
from typing import Any, Dict, List, Optional

from src.logger_config import get_logger

logger = get_logger(__name__)


class DataProcessor:
    """Processes and validates scraped nutritional data."""
    
    REQUIRED_FIELDS = [
        "food_name",
        "measurement_unit",
        "food_id"
    ]
    
    NUMERIC_FIELDS = [
        "calories",
        "fat_g",
        "protein_g",
        "carbs_g",
        "fiber_g",
        "sugar_g",
        "salt_g",
        "measurement_value"
    ]
    
    def __init__(self):
        """Initialize data processor."""
        self.validation_errors: List[str] = []
    
    def validate_row(self, row: Dict[str, Any]) -> bool:
        """Validate a single data row.
        
        Args:
            row: Dictionary containing scraped data
        Returns:
            True if row is valid, False otherwise
        """
        self.validation_errors = []
        
        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if field not in row or row[field] is None or row[field] == "":
                self.validation_errors.append(f"Missing required field: {field}")
        
        # Validate numeric fields
        for field in self.NUMERIC_FIELDS:
            if field in row and row[field] is not None:
                value = row[field]
                # Allow empty string or None for optional numeric fields
                if value == "":
                    continue
                try:
                    # Try to convert to float
                    float_val = float(value)
                    # Check for negative values (nutritional values shouldn't be negative)
                    if field != "measurement_value" and float_val < 0:
                        self.validation_errors.append(
                            f"Negative value for {field}: {float_val}"
                        )
                except (ValueError, TypeError):
                    self.validation_errors.append(
                        f"Invalid numeric value for {field}: {value}"
                    )
        
        # Validate food_id is integer
        if "food_id" in row:
            try:
                int(row["food_id"])
            except (ValueError, TypeError):
                self.validation_errors.append(
                    f"Invalid food_id: {row['food_id']}"
                )
        
        if self.validation_errors:
            logger.warning(
                f"Validation errors for food_id {row.get('food_id', 'unknown')}: "
                f"{', '.join(self.validation_errors)}"
            )
            return False
        
        return True
    
    def clean_data(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize a data row.
        
        Args:
            row: Raw scraped data dictionary
        Returns:
            Cleaned data dictionary
        """
        cleaned = row.copy()
        
        # Clean text fields (strip whitespace)
        text_fields = ["food_name", "measurement_unit"]
        for field in text_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = str(cleaned[field]).strip()
        
        # Normalize numeric fields
        for field in self.NUMERIC_FIELDS:
            if field in cleaned:
                value = cleaned[field]
                if value is None or value == "":
                    cleaned[field] = None
                else:
                    try:
                        # Extract numeric value from string (handle "59.4g" -> 59.4)
                        if isinstance(value, str):
                            # Remove non-numeric characters except decimal point and minus
                            numeric_str = re.sub(r'[^\d.-]', '', value)
                            if numeric_str:
                                cleaned[field] = float(numeric_str)
                            else:
                                cleaned[field] = None
                        else:
                            cleaned[field] = float(value)
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Could not convert {field} to numeric: {value}"
                        )
                        cleaned[field] = None
        
        # Ensure food_id is integer
        if "food_id" in cleaned:
            try:
                cleaned["food_id"] = int(cleaned["food_id"])
            except (ValueError, TypeError):
                logger.warning(f"Invalid food_id: {cleaned['food_id']}")
        
        return cleaned
    
    def process_batch(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of data rows.
        
        Args:
            rows: List of raw data dictionaries
        Returns:
            List of cleaned and validated data dictionaries
        """
        processed = []
        skipped = 0
        
        for row in rows:
            cleaned = self.clean_data(row)
            if self.validate_row(cleaned):
                processed.append(cleaned)
            else:
                skipped += 1
                logger.debug(f"Skipped invalid row: {row.get('food_id', 'unknown')}")
        
        if skipped > 0:
            logger.warning(f"Skipped {skipped} invalid rows out of {len(rows)}")
        
        return processed
    
    def get_validation_errors(self) -> List[str]:
        """Get list of validation errors from last validation.
        
        Returns:
            List of error messages
        """
        return self.validation_errors.copy()

