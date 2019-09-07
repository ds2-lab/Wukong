from serialization import protocol_pickle_dumps, protocol_pickle_loads, to_serialize
import os
import sys
import traceback 

def truncate_exception(e, n=10000):
   """ Truncate exception to be about a certain length """
   if len(str(e)) > n:
      try:
         return type(e)("Long error message", str(e)[:n])
      except Exception:
         return Exception("Long error message", type(e), str(e)[:n])
   else:
      return e
  
def get_traceback():
   exc_type, exc_value, exc_traceback = sys.exc_info()
   bad = [
      os.path.join("distributed", "worker"),
      os.path.join("distributed", "scheduler"),
      os.path.join("tornado", "gen.py"),
      os.path.join("concurrent", "futures"),
   ]
   while exc_traceback and any(
      b in exc_traceback.tb_frame.f_code.co_filename for b in bad
   ):
      exc_traceback = exc_traceback.tb_next
   return exc_traceback   
   
def error_message(e, status="error"):
   """ Produce message to send back given an exception has occurred

   This does the following:

   1.  Gets the traceback
   2.  Truncates the exception and the traceback
   3.  Serializes the exception and traceback or
   4.  If they can't be serialized send string versions
   5.  Format a message and return

   See Also
   --------
   clean_exception: deserialize and unpack message into exception/traceback
   six.reraise: raise exception/traceback
   """
   tb = get_traceback()
   e2 = truncate_exception(e, 1000)
   e2 = Exception(str(e2))
   tb = tb2 = "".join(traceback.format_tb(tb))

   if len(tb2) > 10000:
      tb_result = None
   else:
      tb_result = to_serialize(tb)

   return {"status": status, "exception": e2.__str__(), "traceback": tb_result, "text": str(e2)}  