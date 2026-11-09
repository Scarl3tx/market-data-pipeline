
import logging
from logging.handlers import RotatingFileHandler

from market_data import logging_config


def test_logger_creates_log_directory(tmp_path, monkeypatch):
    log_path = tmp_path / "logs"

    monkeypatch.setattr(logging_config, "LOG_PATH", log_path)

    logger = logging_config.get_pipeline_logger("twelvedata.bronze")

    assert (log_path / "twelvedata" / "bronze").exists()
    assert isinstance(logger, logging.Logger)


def test_logger_has_console_and_file_handlers(tmp_path, monkeypatch):
    log_path = tmp_path / "logs"

    monkeypatch.setattr(logging_config, "LOG_PATH", log_path)

    logger = logging_config.get_pipeline_logger("twelvedata.bronze")

    assert len(logger.handlers) == 2

    logger.info("Test log message")

    log_file = log_path / "twelvedata" / "bronze" / "pipeline.log"

    assert log_file.exists()
    assert "Test log message" in log_file.read_text(encoding="utf-8")


def test_logger_does_not_duplicate_handlers(tmp_path, monkeypatch):
    monkeypatch.setattr(logging_config, "LOG_PATH", tmp_path / "logs")

    logger1 = logging_config.get_pipeline_logger("twelvedata.bronze")
    logger2 = logging_config.get_pipeline_logger("twelvedata.bronze")

    assert logger1 is logger2
    assert len(logger1.handlers) == 2


def test_different_stages_use_separate_log_files(tmp_path, monkeypatch):
    log_path = tmp_path / "logs"

    monkeypatch.setattr(logging_config, "LOG_PATH", log_path)

    bronze_logger = logging_config.get_pipeline_logger("twelvedata.bronze")
    silver_logger = logging_config.get_pipeline_logger("twelvedata.silver")

    bronze_logger.info("Bronze message")
    silver_logger.info("Silver message")

    bronze_file = log_path / "twelvedata" / "bronze" / "pipeline.log"
    silver_file = log_path / "twelvedata" / "silver" / "pipeline.log"

    assert bronze_file.exists()
    assert silver_file.exists()

    assert "Bronze message" in bronze_file.read_text(encoding="utf-8")
    assert "Silver message" in silver_file.read_text(encoding="utf-8")


def test_nested_provider_logger_uses_provider_and_stage_directories(
    tmp_path,
    monkeypatch,
):
    log_path = tmp_path / "logs"

    monkeypatch.setattr(logging_config, "LOG_PATH", log_path)

    logger = logging_config.get_pipeline_logger(
        "market_data.marketaux.silver.coordinator"
    )

    logger.info("Nested logger message")

    log_file = log_path / "marketaux" / "silver" / "pipeline.log"

    assert log_file.exists()
    assert "Nested logger message" in log_file.read_text(encoding="utf-8")


def test_main_logger_uses_market_data_directory(
    tmp_path,
    monkeypatch,
):
    log_path = tmp_path / "logs"

    monkeypatch.setattr(logging_config, "LOG_PATH", log_path)

    logger = logging_config.get_pipeline_logger("market_data.main")

    logger.info("Main logger message")

    log_file = log_path / "market_data" / "main" / "pipeline.log"

    assert log_file.exists()
    assert "Main logger message" in log_file.read_text(encoding="utf-8")


def test_file_handler_uses_rotation(tmp_path, monkeypatch):
    log_path = tmp_path / "logs"

    monkeypatch.setattr(logging_config, "LOG_PATH", log_path)
    monkeypatch.setattr(logging_config, "LOG_MAX_BYTES", 100)
    monkeypatch.setattr(logging_config, "LOG_BACKUP_COUNT", 2)

    logger = logging_config.get_pipeline_logger("twelvedata.bronze")

    file_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, RotatingFileHandler)
    ]

    assert len(file_handlers) == 1
    assert file_handlers[0].maxBytes == 100
    assert file_handlers[0].backupCount == 2


def test_log_file_rotates_when_size_limit_is_exceeded(
    tmp_path,
    monkeypatch,
):
    log_path = tmp_path / "logs"

    monkeypatch.setattr(logging_config, "LOG_PATH", log_path)
    monkeypatch.setattr(logging_config, "LOG_MAX_BYTES", 100)
    monkeypatch.setattr(logging_config, "LOG_BACKUP_COUNT", 2)

    logger = logging_config.get_pipeline_logger("twelvedata.bronze")

    for index in range(10):
        logger.info("Log entry %s: %s", index, "x" * 40)

    log_directory = log_path / "twelvedata" / "bronze"

    assert (log_directory / "pipeline.log").exists()
    assert (log_directory / "pipeline.log.1").exists()


def test_logger_propagation_is_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(logging_config, "LOG_PATH", tmp_path / "logs")

    logger = logging_config.get_pipeline_logger("twelvedata.bronze")

    assert logger.propagate is False
