"""Logger for skipped items with detailed error information."""

import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.logger_config import get_logger

logger = get_logger(__name__)


class SkippedLogger:
    """Manages logging of skipped items with error details."""
    
    def __init__(
        self,
        log_file: Path = Path("data/logs/skipped_items.json")
    ):
        """Initialize skipped logger.
        
        Args:
            log_file: Path to JSON log file for skipped items
        """
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing skipped items
        self.skipped_items: List[Dict[str, Any]] = self._load_existing()
        
        logger.info(
            f"SkippedLogger initialized: {len(self.skipped_items)} existing skipped items"
        )
    
    def _load_existing(self) -> List[Dict[str, Any]]:
        """Load existing skipped items from log file.
        
        Returns:
            List of skipped item dictionaries
        """
        if not self.log_file.exists():
            return []
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                else:
                    # Handle old format or corrupted file
                    logger.warning("Skipped log file format unexpected, starting fresh")
                    return []
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing skipped items log: {e}. Starting fresh.")
            return []
        except Exception as e:
            logger.error(f"Error loading skipped items log: {e}. Starting fresh.")
            return []
    
    def log_skipped(
        self,
        food_id: int,
        error: Optional[Exception] = None,
        error_message: Optional[str] = None,
        reason: Optional[str] = None
    ) -> None:
        """Log a skipped item with error details.
        
        Args:
            food_id: Food item ID that was skipped
            error: Exception object (if available)
            error_message: Error message string
            reason: Reason for skipping (e.g., "no_data", "timeout", "invalid_page")
        """
        # Get error details
        error_type = None
        error_msg = None
        error_traceback = None
        
        if error:
            error_type = type(error).__name__
            error_msg = str(error)
            try:
                error_traceback = ''.join(traceback.format_exception(
                    type(error), error, error.__traceback__
                ))
            except:
                error_traceback = traceback.format_exc()
        elif error_message:
            error_type = "Unknown"
            error_msg = error_message
        
        # Check if this food_id is already logged
        existing_index = None
        for idx, item in enumerate(self.skipped_items):
            if item.get("food_id") == food_id:
                existing_index = idx
                break
        
        # Create skipped item entry
        skipped_entry = {
            "food_id": food_id,
            "timestamp": datetime.now().isoformat(),
            "error_type": error_type or "Unknown",
            "error_message": error_msg or error_message or "No error details",
            "reason": reason or "unknown",
            "traceback": error_traceback or ""
        }
        
        # Update existing or add new
        if existing_index is not None:
            self.skipped_items[existing_index] = skipped_entry
            logger.debug(f"Updated skipped entry for ID {food_id}")
        else:
            self.skipped_items.append(skipped_entry)
            logger.debug(f"Added skipped entry for ID {food_id}")
        
        # Save to file
        self._save()
    
    def _save(self) -> None:
        """Save skipped items to log file."""
        try:
            # Atomic write: write to temp file first, then replace
            import tempfile
            import os
            
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                encoding='utf-8',
                delete=False,
                suffix='.json',
                dir=self.log_file.parent
            )
            temp_path = Path(temp_file.name)
            
            json.dump(
                self.skipped_items,
                temp_file,
                ensure_ascii=False,
                indent=2
            )
            temp_file.close()
            
            # Atomic replace
            os.replace(temp_path, self.log_file)
            
        except Exception as e:
            logger.error(f"Error saving skipped items log: {e}", exc_info=True)
            raise
    
    def get_skipped_ids(self) -> List[int]:
        """Get list of all skipped food IDs.
        
        Returns:
            List of food IDs that were skipped
        """
        return [item["food_id"] for item in self.skipped_items]
    
    def get_skipped_items(self) -> List[Dict[str, Any]]:
        """Get all skipped items with details.
        
        Returns:
            List of skipped item dictionaries
        """
        return self.skipped_items.copy()
    
    def remove_skipped(self, food_id: int) -> bool:
        """Remove a food ID from skipped items (e.g., after successful retry).
        
        Args:
            food_id: Food ID to remove from skipped list
        Returns:
            True if item was found and removed, False otherwise
        """
        initial_count = len(self.skipped_items)
        self.skipped_items = [
            item for item in self.skipped_items
            if item.get("food_id") != food_id
        ]
        
        if len(self.skipped_items) < initial_count:
            self._save()
            logger.debug(f"Removed food_id {food_id} from skipped items")
            return True
        
        return False
    
    def clear(self) -> None:
        """Clear all skipped items."""
        self.skipped_items = []
        self._save()
        logger.info("Cleared all skipped items")

