"""Unit tests for data processor module."""

import pytest

from src.data_processor import DataProcessor


class TestDataProcessor:
    """Test cases for DataProcessor class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.processor = DataProcessor()
    
    def test_validate_row_valid(self):
        """Test validation of valid data row."""
        row = {
            "food_id": 3,
            "food_name": "تخم مرغ آب پز",
            "measurement_unit": "100 گرم",
            "measurement_value": 100.0,
            "calories": 155.0,
            "carbs_g": 1.1,
            "protein_g": 13.0,
            "fat_g": 10.6,
            "fiber_g": 0.0,
            "source_url": "https://www.mankan.me/mag/lib/read_one.php?id=3"
        }
        assert self.processor.validate_row(row) is True
    
    def test_validate_row_missing_required(self):
        """Test validation fails for missing required fields."""
        row = {
            "food_id": 3,
            # Missing food_name
            "measurement_unit": "100 گرم",
        }
        assert self.processor.validate_row(row) is False
        assert len(self.processor.get_validation_errors()) > 0
    
    def test_validate_row_invalid_numeric(self):
        """Test validation fails for invalid numeric values."""
        row = {
            "food_id": 3,
            "food_name": "Test Food",
            "measurement_unit": "100 گرم",
            "calories": "not a number",
            "source_url": "https://example.com"
        }
        assert self.processor.validate_row(row) is False
    
    def test_clean_data_text_stripping(self):
        """Test text fields are stripped of whitespace."""
        row = {
            "food_name": "  Test Food  ",
            "measurement_unit": "  100 گرم  ",
        }
        cleaned = self.processor.clean_data(row)
        assert cleaned["food_name"] == "Test Food"
        assert cleaned["measurement_unit"] == "100 گرم"
    
    def test_clean_data_numeric_extraction(self):
        """Test numeric values are extracted from strings."""
        row = {
            "calories": "155.5",
            "carbs_g": "10.2g",
            "protein_g": "13.0",
        }
        cleaned = self.processor.clean_data(row)
        assert cleaned["calories"] == 155.5
        assert cleaned["carbs_g"] == 10.2
        assert cleaned["protein_g"] == 13.0
    
    def test_process_batch(self):
        """Test batch processing of multiple rows."""
        rows = [
            {
                "food_id": 3,
                "food_name": "Test Food 1",
                "measurement_unit": "100 گرم",
                "source_url": "https://example.com/1"
            },
            {
                "food_id": 4,
                "food_name": "Test Food 2",
                "measurement_unit": "1 عدد",
                "source_url": "https://example.com/2"
            },
            {
                # Invalid: missing required field
                "food_id": 5,
            }
        ]
        processed = self.processor.process_batch(rows)
        assert len(processed) == 2  # Only valid rows

