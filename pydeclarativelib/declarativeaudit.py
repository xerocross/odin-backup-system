from backuplib.audit import Tracker
from typing import Callable, Tuple


def _audited(func):
    def wrapper(*args, **kwargs):

        print("Something happens *before* the function runs")
        result = func(*args, **kwargs)
        print("Something happens *after* the function runs")
        return result
    
    return wrapper

def audited_by(tracker : Tracker, with_step_name : str, and_run_id: str):
    step_name = with_step_name
    run_id = and_run_id
    def decorator(func):
        def wrapper(*args, **kwargs):
            with tracker.record_step(run_id = run_id, 
                             name = step_name
                             ) as rec:
                try:
                        result, message = func(*args, **kwargs)
                        rec["status"] = ("success" if result else "failed")
                        if message is not None:
                            rec["message"] = message
                except Exception as e:
                    rec["status"] = "failed"
                    rec["message"] = str(e)
                    raise
        return wrapper
    return decorator