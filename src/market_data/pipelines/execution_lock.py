
import errno
import os
import threading

from contextlib import contextmanager

from market_data.paths import PIPELINE_LOCK_PATH


_THREAD_LOCK = threading.Lock()


def _lock_file(handle):
    """Acquire a non-blocking OS-level file lock."""

    if os.name == "nt":
        import msvcrt

        # Windows byte-range locking requires an existing byte.
        handle.seek(0)

        if handle.read(1) == b"":
            handle.seek(0)
            handle.write(b"\0")
            handle.flush()

        handle.seek(0)
        msvcrt.locking(
            handle.fileno(),
            msvcrt.LK_NBLCK,
            1,
        )

    else:
        import fcntl

        fcntl.flock(
            handle.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )


def _unlock_file(handle):
    """Release the OS-level file lock."""

    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(
            handle.fileno(),
            msvcrt.LK_UNLCK,
            1,
        )

    else:
        import fcntl

        fcntl.flock(
            handle.fileno(),
            fcntl.LOCK_UN,
        )


def _is_lock_contention(error: OSError) -> bool:
    """Identify errors indicating another process holds the lock."""

    contention_errnos = {
        errno.EACCES,
        errno.EAGAIN,
        errno.EDEADLK,
    }

    windows_contention_errors = {
        33,  # Lock violation
        36,  # Sharing buffer exceeded / lock contention
    }

    return (
        error.errno in contention_errnos
        or getattr(error, "winerror", None)
        in windows_contention_errors
    )


@contextmanager
def pipeline_execution_lock():
    """
    Acquire the pipeline execution lock without waiting.

    Yields:
        True if acquired, otherwise False.
    """

    if not _THREAD_LOCK.acquire(blocking=False):
        yield False
        return

    handle = None
    locked = False

    try:
        PIPELINE_LOCK_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        handle = open(PIPELINE_LOCK_PATH, "a+b")

        try:
            _lock_file(handle)

        except OSError as exc:
            if _is_lock_contention(exc):
                yield False
                return

            raise

        locked = True

        yield True

    finally:
        if handle is not None:
            if locked:
                try:
                    _unlock_file(handle)
                except OSError:
                    # Closing the handle also releases the OS lock.
                    pass

            handle.close()

        _THREAD_LOCK.release()
