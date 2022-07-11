from contextlib import contextmanager


@contextmanager
def acquire_with_timeout(lock, timeout):
    result = lock.acquire(timeout=timeout)
    if not result:
        raise TimeoutError(f"Timeout acquiring lock {lock!r}")
    try:
        yield
    finally:
        lock.release()
