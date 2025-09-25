from backuplib.audit import Tracker
from typing import Callable, Tuple
import functools
import inspect

def _audited(func):
    def wrapper(*args, **kwargs):

        print("Something happens *before* the function runs")
        result = func(*args, **kwargs)
        print("Something happens *after* the function runs")
        return result
    
    return wrapper

def audited_by(tracker : Tracker, with_step_name : str, and_run_id: str,
               transaction_recorder_name: str = "transaction_recorder"):
    step_name = with_step_name
    run_id = and_run_id
    def decorator(func):

        sig = inspect.signature(func)
        is_rec_passed = transaction_recorder_name in sig.parameters  

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            transaction_recorder = kwargs.get(transaction_recorder_name) if is_rec_passed else None

            with tracker.record_step(run_id = run_id, 
                             name = step_name
                             ) as rec:
                funcresult: dict = func(*args, **kwargs)
                success = funcresult["success"]
                message = None
                if "message" in funcresult:
                    message = funcresult["message"]

                rec["status"] = ("success" if success else "failed")

                if message is not None:
                    rec["message"] = message
                return funcresult
        return wrapper
    return decorator