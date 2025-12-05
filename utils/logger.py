"""
Logging utility for the pipeline
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Setup logger with console and file handlers

    Args:
        name: Logger name
        level: Logging level

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Console handler (INFO and above)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.encoding = 'utf-8'
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    # File handler (DEBUG and above)
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_dir / log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    file_handler.encoding = 'utf-8'
    logger.addHandler(file_handler)

    # Error file handler (ERROR and above)
    error_filename = f"errors_{datetime.now().strftime('%Y%m%d')}.log"
    error_handler = logging.FileHandler(log_dir / error_filename)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    error_handler.encoding = 'utf-8'
    logger.addHandler(error_handler)

    return logger