#####################################################################
# forecast_shared_context.py
#
# Allows for shared variables during design time using a singleton approach
# At the form-instance level
# 
# INPUTS:  None
# OUTPUTS:  None
#####################################################################

import threading

lock = threading.Lock()

class ForecastSharedContext(object):
  def __new__(cls, user_id=None, flow_id=None):

   # determine which kind of shared context to create
    if(user_id is None or user_id == 'None'):
      # we can't create the shared context without at least a user id, so don't return anything
      print("ForecastSharedContext: No user_id, no flow_id do nothing")
      return
    
    elif(flow_id is None or flow_id == 'None'):
      # just create a SharedContext around the user_id
      print(f"ForecastSharedContext: user_id = {user_id}")
      singleton_instance_name = f"instance_{user_id}"

    else:
      # create an instance with both
      print(f"ForecastSharedContext: user_id = {user_id}, flow_id={flow_id}")
      singleton_instance_name = f"instance_{user_id}_{flow_id}"

    # check, lock, check pattern for multi-thread support
    if not hasattr(cls, singleton_instance_name):
      with lock:
        if not hasattr(cls, singleton_instance_name):
            setattr(cls, singleton_instance_name, super(ForecastSharedContext, cls).__new__(cls))
    return getattr(cls, singleton_instance_name)
  
  def __init__(self, **kwargs):
    self.attr = {}
    
  
  

