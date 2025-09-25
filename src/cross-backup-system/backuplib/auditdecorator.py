
import functools, contextvars
current_tracker = contextvars.ContextVar("current_tracker", default=None)
current_run_id  = contextvars.ContextVar("current_run_id",  default=None)

def audit_this(name: str):
        """
        Decorator factory: takes name
        returns a decorator that wraps the function to record it
        in the audit trail.
        """
        def decorator(func):
            tracker = current_tracker.get()
            run_id  = current_run_id.get()
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # create the step in DB
                step = tracker.start_step(run_id, name)
                rec = {}  # the "audit handle" dict
                try:
                    # call the original function, injecting `rec`
                    result = func(tracker, rec, *args, **kwargs)
                    status = rec.get("status", "success")
                    message = rec.get("message", "")
                    tracker.finish_step(step, status, message)
                    return result
                except Exception as e:
                    tracker.finish_step(step, "failed", message=str(e))
                    raise
            return wrapper
        return decorator
        