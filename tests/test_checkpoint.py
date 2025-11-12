"""Unit tests for checkpoint module."""

import json
import tempfile
from pathlib import Path

import pytest

from src.checkpoint import CheckpointManager


class TestCheckpointManager:
    """Test cases for CheckpointManager class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.checkpoint_manager = CheckpointManager(
            checkpoint_dir=self.temp_dir
        )
    
    def test_load_nonexistent_checkpoint(self):
        """Test loading non-existent checkpoint returns empty data."""
        data = self.checkpoint_manager.load()
        assert data["completed_ids"] == []
        assert data["data"] == []
        assert data["last_checkpoint"] is None
    
    def test_save_and_load(self):
        """Test saving and loading checkpoint."""
        completed_ids = [3, 4, 5]
        data = [
            {"food_id": 3, "food_name": "Food 3"},
            {"food_id": 4, "food_name": "Food 4"},
        ]
        
        self.checkpoint_manager.save(completed_ids, data)
        
        # Load checkpoint
        loaded_data = self.checkpoint_manager.load()
        assert set(loaded_data["completed_ids"]) == set(completed_ids)
        assert len(loaded_data["data"]) == 2
    
    def test_is_completed(self):
        """Test checking if ID is completed."""
        completed_ids = [3, 4, 5]
        data = []
        self.checkpoint_manager.save(completed_ids, data)
        self.checkpoint_manager.load()
        
        assert self.checkpoint_manager.is_completed(3) is True
        assert self.checkpoint_manager.is_completed(10) is False
    
    def test_get_completed_ids(self):
        """Test getting set of completed IDs."""
        completed_ids = [3, 4, 5]
        self.checkpoint_manager.save(completed_ids, [])
        self.checkpoint_manager.load()
        
        completed_set = self.checkpoint_manager.get_completed_ids()
        assert completed_set == {3, 4, 5}
    
    def test_corrupted_checkpoint_recovery(self):
        """Test recovery from corrupted checkpoint."""
        # Create corrupted checkpoint file
        checkpoint_file = self.checkpoint_manager.checkpoint_path
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint_file, "w") as f:
            f.write("invalid json content {")
        
        # Should handle gracefully
        data = self.checkpoint_manager.load()
        assert isinstance(data, dict)
        assert "completed_ids" in data

