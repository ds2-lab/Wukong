import sys
import inspect
import traceback
import tornado
from tornado import gen
from tornado.ioloop import IOLoop
from contextlib import contextmanager
import os

msgpack_opts = {
   ("max_%s_len" % x): 2 ** 31 - 1 for x in ["str", "bin", "array", "map", "ext"]
}

if sys.version_info[0] == 2:
   PY3 = False 
   PY2 = True 
if sys.version_info[0] == 3:
   PY3 = True 
   PY2 = False 

def has_keyword(func, keyword):
   if PY3:
      return keyword in inspect.signature(func).parameters
   else:
      # https://stackoverflow.com/questions/50100498/determine-keywords-of-a-tornado-coroutine
      if gen.is_coroutine_function(func):
         func = func.__wrapped__
      return keyword in inspect.getargspec(func).args
    
def nbytes(frame, _bytes_like=(bytes, bytearray)):
   """ Number of bytes of a frame or memoryview """
   if isinstance(frame, _bytes_like):
      return len(frame)
   else:
      try:
         return frame.nbytes
      except AttributeError:
         return len(frame)
        
def funcname(func):
   """Get the name of a function."""
   while hasattr(func, "func"):
      func = func.func
   try:
      return func.__name__
   except AttributeError:
      return str(func)      

@contextmanager
def ignoring(*exceptions):
   try:
      yield
   except exceptions as e:
      pass   
   
# TODO: Implement exception handling/reporting (i.e. the following commented-out methods...)   
  
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
   try:
      e3 = protocol_pickle_dumps(e2)
      protocol_pickle_loads(e3)
   except Exception:
      e2 = Exception(str(e2))
   e4 = to_serialize(e2)
   try:
      tb2 = protocol_pickle_dumps(tb)
   except Exception:
      tb = tb2 = "".join(traceback.format_tb(tb))

   if len(tb2) > 10000:
      tb_result = None
   else:
      tb_result = to_serialize(tb)

   return {"status": status, "exception": e4, "traceback": tb_result, "text": str(e2)}     