import threading
import functools
import asyncio

class AlreadyCalledError(RuntimeError):
    """Raised when a run-once function is invoked more than once."""

def run_once(func):
    """
    Decorator to ensure a function (sync or async) is called at most once
    per Python process. Subsequent calls raise AlreadyCalledError.
    Thread-safe.
    """
    lock = threading.Lock()
    state = {"called": False}

    if asyncio.iscoroutinefunction(func):
        @functools.wraps(func)
        async def awrapper(*args, **kwargs):
            with lock:
                if state["called"]:
                    raise AlreadyCalledError(f"{func.__name__}() was called more than once")
                state["called"] = True
            return await func(*args, **kwargs)
        wrapper = awrapper
    else:
        @functools.wraps(func)
        def swrapper(*args, **kwargs):
            with lock:
                if state["called"]:
                    raise AlreadyCalledError(f"{func.__name__}() was called more than once")
                state["called"] = True
            return func(*args, **kwargs)
        wrapper = swrapper

    # Optional: test helper to reset between unit tests
    def _reset_run_once():
        with lock:
            state["called"] = False
    wrapper.reset_run_once = _reset_run_once

    return wrapper
