import sys
import inspect
import traceback
import tornado
from tornado import gen
from tornado.ioloop import IOLoop
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

def key_split(s):
    """
    >>> key_split('x')
    'x'
    >>> key_split('x-1')
    'x'
    >>> key_split('x-1-2-3')
    'x'
    >>> key_split(('x-2', 1))
    'x'
    >>> key_split("('x-2', 1)")
    'x'
    >>> key_split('hello-world-1')
    'hello-world'
    >>> key_split(b'hello-world-1')
    'hello-world'
    >>> key_split('ae05086432ca935f6eba409a8ecd4896')
    'data'
    >>> key_split('<module.submodule.myclass object at 0xdaf372')
    'myclass'
    >>> key_split(None)
    'Other'
    >>> key_split('x-abcdefab')  # ignores hex
    'x'
    >>> key_split('_(x)')  # strips unpleasant characters
    'x'
    """
    if type(s) is bytes:
        s = s.decode()
    if type(s) is tuple:
        s = s[0]
    try:
        words = s.split("-")
        if not words[0][0].isalpha():
            result = words[0].strip("_'()\"")
        else:
            result = words[0]
        for word in words[1:]:
            if word.isalpha() and not (
                len(word) == 8 and hex_pattern.match(word) is not None
            ):
                result += "-" + word
            else:
                break
        if len(result) == 32 and re.match(r"[a-f0-9]{32}", result):
            return "data"
        else:
            if result[0] == "<":
                result = result.strip("<>").split()[0].split(".")[-1]
            return result
    except Exception:
        return "Other"

def typename(typ):
    """ Return name of type
    Examples
    --------
    >>> from distributed import Scheduler
    >>> typename(Scheduler)
    'distributed.scheduler.Scheduler'
    """
    try:
        return typ.__module__ + "." + typ.__name__
    except AttributeError:
        return str(typ)

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