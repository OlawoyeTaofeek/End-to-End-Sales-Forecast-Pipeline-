import logging
import sys
from pathlib import Path

def setup_logging(log_file_path: Path) -> None:
    """
    Set up a global logging configuration for the entire pipeline stage.

    Args:
        log_file_path (Path): Full path to the log file (e.g., Path("logs/data_ingestion.log")).
    """
    # Ensure the parent directory exists
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Clear existing logging handlers (avoid duplicates across multiple calls)
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Configure global logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),  # Log to file
            logging.StreamHandler(sys.stdout)    # Also log to console
        ]
    )
