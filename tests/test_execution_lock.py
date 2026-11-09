
import multiprocessing
from pathlib import Path

import market_data.pipelines.execution_lock as execution_lock


def _child_try_acquire_lock(lock_path, result_queue):
    execution_lock.PIPELINE_LOCK_PATH = Path(lock_path)

    with execution_lock.pipeline_execution_lock() as acquired:
        result_queue.put(acquired)


def test_lock_can_be_acquired_and_released(
    tmp_path,
    monkeypatch,
):
    lock_path = tmp_path / "state" / "pipeline.lock"

    monkeypatch.setattr(
        execution_lock,
        "PIPELINE_LOCK_PATH",
        lock_path,
    )

    with execution_lock.pipeline_execution_lock() as acquired:
        assert acquired is True
        assert lock_path.exists()

    with execution_lock.pipeline_execution_lock() as acquired:
        assert acquired is True


def test_second_thread_execution_is_rejected(
    tmp_path,
    monkeypatch,
):
    lock_path = tmp_path / "pipeline.lock"

    monkeypatch.setattr(
        execution_lock,
        "PIPELINE_LOCK_PATH",
        lock_path,
    )

    with execution_lock.pipeline_execution_lock() as acquired:
        assert acquired is True

        with execution_lock.pipeline_execution_lock() as second:
            assert second is False


def test_second_process_execution_is_rejected(
    tmp_path,
    monkeypatch,
):
    lock_path = tmp_path / "pipeline.lock"

    monkeypatch.setattr(
        execution_lock,
        "PIPELINE_LOCK_PATH",
        lock_path,
    )

    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue()

    with execution_lock.pipeline_execution_lock() as acquired:
        assert acquired is True

        process = context.Process(
            target=_child_try_acquire_lock,
            args=(str(lock_path), result_queue),
        )

        process.start()
        process.join(timeout=10)

        assert not process.is_alive()
        assert process.exitcode == 0
        assert result_queue.get(timeout=5) is False

    # The child process was rejected while the parent held the lock.
    # A subsequent process should be able to acquire it.
    process = context.Process(
        target=_child_try_acquire_lock,
        args=(str(lock_path), result_queue),
    )

    process.start()
    process.join(timeout=10)

    assert not process.is_alive()
    assert process.exitcode == 0
    assert result_queue.get(timeout=5) is True


def test_lock_is_released_after_exception(
    tmp_path,
    monkeypatch,
):
    lock_path = tmp_path / "pipeline.lock"

    monkeypatch.setattr(
        execution_lock,
        "PIPELINE_LOCK_PATH",
        lock_path,
    )

    try:
        with execution_lock.pipeline_execution_lock() as acquired:
            assert acquired is True
            raise RuntimeError("Pipeline failed")

    except RuntimeError:
        pass

    with execution_lock.pipeline_execution_lock() as acquired:
        assert acquired is True
