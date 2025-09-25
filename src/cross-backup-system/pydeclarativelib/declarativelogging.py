from logging import Logger



# def audited_by(logger : Logger):
    
#     def decorator(func):
#         def wrapper(*args, **kwargs):

#             with tracker.record_step(run_id = run_id, 
#                              name = step_name
#                              ) as rec:
#                 try:
#                         result, message = func(*args, **kwargs)
#                         rec["status"] = ("success" if result else "failed")
#                         if message is not None:
#                             rec["message"] = message
#                 except Exception as e:
#                     rec["status"] = "failed"
#                     rec["message"] = str(e)
#                     raise
#         return wrapper
#     return decorator