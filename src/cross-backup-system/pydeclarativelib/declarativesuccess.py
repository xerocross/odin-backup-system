from typing import Tuple, List
import traceback
import functools


def with_try_except_and_trace(if_success_then_message : str, if_failed_then_message : str, with_trace: List[str]):
    success_message = if_success_then_message
    failure_message = if_failed_then_message
    trace = with_trace
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs)-> Tuple[bool, str]:
            trace.append(str(func.__name__))
            try:
                result : dict = func(*args, **kwargs)
                trace.append(success_message)
                return result
            except Exception as e:
                trace.append(failure_message)
                trace.append(traceback.format_exc())
                raise e
        return wrapper
    return decorator