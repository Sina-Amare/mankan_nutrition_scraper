"""Checkpoint/resume functionality for scraper progress.

Saves progress periodically to allow resuming from last checkpoint.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from src.logger_config import get_logger

logger = get_logger(__name__)


class CheckpointManager:
    """Manages checkpoint saving and loading for scraper progress."""
    
    def __init__(
        self,
        checkpoint_dir: Path = Path("data/checkpoints"),
        checkpoint_file: str = "checkpoint.json"
    ):
        """Initialize checkpoint manager.
        
        Args:
            checkpoint_dir: Directory for checkpoint files
            checkpoint_file: Name of checkpoint file
        """
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = checkpoint_dir / checkpoint_file
        self.data: Dict[str, Any] = {}
        
    def load(self) -> Dict[str, Any]:
        """Load checkpoint data from file.
        
        Returns:
            Dictionary with checkpoint data, or empty dict if no checkpoint exists
        """
        if not self.checkpoint_path.exists():
            logger.info("No checkpoint found. Starting fresh.")
            return {
                "completed_ids": [],
                "data": [],
                "last_checkpoint": None,
                "total_scraped": 0
            }
        
        try:
            with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
            
            completed_count = len(self.data.get("completed_ids", []))
            logger.info(
                f"Loaded checkpoint: {completed_count} items completed. "
                f"Last checkpoint: {self.data.get('last_checkpoint', 'N/A')}"
            )
            return self.data
            
        except json.JSONDecodeError as e:
            logger.error(f"Checkpoint file corrupted: {e}. Attempting backup recovery.")
            return self._try_backup_recovery()
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}. Starting fresh.")
            return {
                "completed_ids": [],
                "data": [],
                "last_checkpoint": None,
                "total_scraped": 0
            }
    
    def _try_backup_recovery(self) -> Dict[str, Any]:
        """Try to recover from backup checkpoint file.
        
        Returns:
            Recovered checkpoint data or empty dict
        """
        backup_path = self.checkpoint_path.with_suffix(".json.bak")
        if backup_path.exists():
            try:
                with open(backup_path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
                logger.info("Recovered from backup checkpoint.")
                return self.data
            except Exception as e:
                logger.error(f"Backup recovery failed: {e}")
        
        return {
            "completed_ids": [],
            "data": [],
            "last_checkpoint": None,
            "total_scraped": 0
        }
    
    def save(
        self,
        completed_ids: List[int],
        data: List[Dict[str, Any]],
        force: bool = False
    ) -> bool:
        """Save checkpoint data atomically.
        
        Args:
            completed_ids: List of completed food item IDs
            data: List of scraped data dictionaries
            force: Force save even if no changes (default: False)
        Returns:
            True if save successful, False otherwise
        """
        try:
            # Create backup of existing checkpoint
            if self.checkpoint_path.exists():
                backup_path = self.checkpoint_path.with_suffix(".json.bak")
                import shutil
                shutil.copy2(self.checkpoint_path, backup_path)
            
            # Prepare checkpoint data
            checkpoint_data = {
                "completed_ids": sorted(completed_ids),
                "data": data,
                "last_checkpoint": datetime.now().isoformat(),
                "total_scraped": len(data)
            }
            
            # Atomic write: write to temp file first, then rename
            temp_file = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.checkpoint_dir,
                delete=False,
                suffix=".tmp"
            )
            
            json.dump(
                checkpoint_data,
                temp_file,
                ensure_ascii=False,
                indent=2
            )
            temp_file.close()
            
            # Atomic rename (works on Windows too)
            import os
            os.replace(temp_file.name, self.checkpoint_path)
            
            self.data = checkpoint_data
            logger.debug(
                f"Checkpoint saved: {len(completed_ids)} IDs, "
                f"{len(data)} data rows"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
            return False
    
    def is_completed(self, food_id: int) -> bool:
        """Check if a food ID has been completed.
        
        Args:
            food_id: Food item ID to check
        Returns:
            True if ID is in completed list
        """
        completed_ids = self.data.get("completed_ids", [])
        return food_id in completed_ids
    
    def get_completed_ids(self) -> Set[int]:
        """Get set of completed food IDs.
        
        Returns:
            Set of completed IDs
        """
        return set(self.data.get("completed_ids", []))
    
    def get_scraped_data(self) -> List[Dict[str, Any]]:
        """Get all scraped data from checkpoint.
        
        Returns:
            List of data dictionaries
        """
        return self.data.get("data", [])

