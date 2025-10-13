
from __future__ import annotations
from typing import List, Callable, TypeVar
from typing_extensions import ParamSpec
import traceback
import functools

P = ParamSpec("P")
R = TypeVar("R")


def with_try_except_and_trace(if_success_then_message : str, if_failed_then_message : str, with_trace: List[str]):
    success_message = if_success_then_message
    failure_message = if_failed_then_message
    trace = with_trace
    
    def decorator(func : Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs)-> R:
            trace.append(str(func.__name__))
            try:
                result = func(*args, **kwargs)
                trace.append(success_message)
                return result
            except Exception as e:
                trace.append(failure_message)
                trace.append(traceback.format_exc())
                raise e
        return wrapper
    return decorator