"""Logging configuration for Mankan scraper.

Provides dual logging: console (INFO+) and file (DEBUG+) with rotation.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str = "mankan_scraper",
    log_dir: Path = Path("data/logs"),
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """Configure and return a logger with console and file handlers.
    
    Args:
        name: Logger name
        log_dir: Directory for log files
        console_level: Logging level for console output
        file_level: Logging level for file output
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set to lowest level, handlers filter
    
    # Prevent duplicate handlers if logger already configured
    if logger.handlers:
        return logger
    
    # Create log directory if it doesn't exist
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler with immediate flush for real-time output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    # Force immediate output
    console_handler.stream = sys.stdout
    logger.addHandler(console_handler)
    
    # File handler with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"scraper_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.info(f"Logger initialized. Log file: {log_file}")
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get an existing logger or create a new one.
    
    Args:
        name: Logger name (defaults to 'mankan_scraper')
    Returns:
        Logger instance
    """
    if name is None:
        name = "mankan_scraper"
    return logging.getLogger(name)

