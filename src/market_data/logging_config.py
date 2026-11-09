
import logging

from logging.handlers import RotatingFileHandler

from market_data.paths import DATA_PATH


LOG_PATH = DATA_PATH / "logs"

# Keep individual log files bounded in size.
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5


def _get_log_directory_parts(name: str) -> tuple[str, str]:
    """Resolve a logger name into provider and stage directories."""

    parts = name.split(".")

    # Logger names such as:
    # market_data.marketaux.silver.coordinator
    # market_data.twelvedata.bronze.downloader
    if (
        len(parts) >= 3
        and parts[0] == "market_data"
        and parts[1] not in ("main", "pipelines")
    ):
        return parts[1], parts[2]

    # Logger names such as:
    # twelvedata.bronze
    # marketaux.silver
    # market_data.main
    if len(parts) >= 2:
        return parts[0], parts[1]

    return "general", "pipeline"


def get_pipeline_logger(name: str) -> logging.Logger:
    """Create a console and rotating-file logger."""

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Remove existing handlers to avoid duplicate output and stale paths.
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Console handler.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Resolve provider and stage directories.
    provider, stage = _get_log_directory_parts(name)

    log_directory = LOG_PATH / provider / stage
    log_directory.mkdir(parents=True, exist_ok=True)

    # Rotating file handler.
    file_handler = RotatingFileHandler(
        filename=log_directory / "pipeline.log",
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
