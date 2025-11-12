"""Clean all data, logs, and checkpoints for fresh scraping start."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.logger_config import setup_logger

logger = setup_logger()


def main():
    """Delete all output files, checkpoints, and logs."""
    logger.info("=" * 60)
    logger.info("Cleaning all data, logs, and checkpoints...")
    logger.info("=" * 60)
    
    deleted_count = 0
    
    # Delete output files
    output_dir = Path("output")
    if output_dir.exists():
        for file in output_dir.glob("*"):
            if file.is_file():
                try:
                    file.unlink()
                    logger.info(f"Deleted: {file}")
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"Could not delete {file}: {e}")
    
    # Delete checkpoint files
    checkpoint_files = [
        Path("data/checkpoints/checkpoint.json"),
        Path("data/checkpoints/checkpoint.json.bak"),
        Path("data/search_page_checkpoint.json"),
        Path("data/fruit_search_page_checkpoint.json"),
        Path("data/food_ids.txt"),
        Path("data/fruit_ids.txt"),
    ]
    
    for checkpoint_file in checkpoint_files:
        if checkpoint_file.exists():
            try:
                checkpoint_file.unlink()
                logger.info(f"Deleted: {checkpoint_file}")
                deleted_count += 1
            except Exception as e:
                logger.warning(f"Could not delete {checkpoint_file}: {e}")
    
    # Delete log files
    log_dir = Path("data/logs")
    if log_dir.exists():
        for log_file in log_dir.glob("*.log"):
            try:
                log_file.unlink()
                logger.info(f"Deleted: {log_file}")
                deleted_count += 1
            except Exception as e:
                logger.warning(f"Could not delete {log_file}: {e}")
    
    # Delete skipped items log
    skipped_log = Path("data/logs/skipped_items.json")
    if skipped_log.exists():
        try:
            skipped_log.unlink()
            logger.info(f"Deleted: {skipped_log}")
            deleted_count += 1
        except Exception as e:
            logger.warning(f"Could not delete {skipped_log}: {e}")
    
    logger.info("=" * 60)
    logger.info(f"Cleanup complete! Deleted {deleted_count} files.")
    logger.info("=" * 60)
    logger.info("Ready for fresh scraping!")


if __name__ == "__main__":
    main()

