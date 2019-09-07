import dask 
import pickle 
import operator
import cloudpickle 
import msgpack
from toolz import valmap, get_in, reduce
from tornado import gen
from tornado.gen import Return
import os

from compression import maybe_compress, decompress
from utils import nbytes, has_keyword, PY3, PY2

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.async_context import AsyncContext

BIG_BYTES_SHARD_SIZE = 2 ** 26

dask_serialize = dask.utils.Dispatch("dask_serialize")
dask_deserialize = dask.utils.Dispatch("dask_deserialize")

msgpack_opts = { ("max_%s_len" % x): 2 ** 31 - 1 for x in ["str", "bin", "array", "map", "ext"] }  
   
def frame_split_size(frames, n=BIG_BYTES_SHARD_SIZE):
   """
   Split a list of frames into a list of frames of maximum size

   This helps us to avoid passing around very large bytestrings.

   Examples
   --------
   >>> frame_split_size([b'12345', b'678'], n=3)  # doctest: +SKIP
   [b'123', b'45', b'678']
   """
   if not frames:
      return frames

   if max(map(nbytes, frames)) <= n:
      return frames

   out = []
   for frame in frames:
      if nbytes(frame) > n:
         if isinstance(frame, (bytes, bytearray)):
            frame = memoryview(frame)
         try:
            itemsize = frame.itemsize
         except AttributeError:
            itemsize = 1
         for i in range(0, nbytes(frame) // itemsize, n // itemsize):
            out.append(frame[i : i + n // itemsize])
      else:
         out.append(frame)
   return out   
   
def dask_dumps(x, context=None):
   """Serialise object using the class-based registry"""
   type_name = typename(type(x))
   try:
      dumps = dask_serialize.dispatch(type(x))
   except TypeError:
      raise NotImplementedError(type_name)
   if has_keyword(dumps, "context"):
      header, frames = dumps(x, context=context)
   else:
      header, frames = dumps(x)

   header["type"] = type_name
   header["type-serialized"] = pickle.dumps(type(x))
   header["serializer"] = "dask"
   return header, frames

def dask_loads(header, frames):
   typ = pickle.loads(header["type-serialized"])
   loads = dask_deserialize.dispatch(typ)
   return loads(header, frames)

def pickle_dumps(x):
   return {"serializer": "pickle"}, [pickle.dumps(x)]

def pickle_loads(header, frames):
   return pickle.loads(b"".join(frames))

def msgpack_dumps(x):
   try:
      frame = msgpack.dumps(x, use_bin_type=True)
   except Exception:
      raise NotImplementedError()
   else:
      return {"serializer": "msgpack"}, [frame]

def msgpack_loads(header, frames):
   return msgpack.loads(b"".join(frames), use_list=False, **msgpack_opts)

def serialization_error_loads(header, frames):
   msg = "\n".join([ensure_bytes(frame).decode("utf8") for frame in frames])
   raise TypeError(msg)   
   
families = {}

def register_serialization_family(name, dumps, loads):
   families[name] = (dumps, loads, dumps and has_keyword(dumps, "context"))

register_serialization_family("dask", dask_dumps, dask_loads)
register_serialization_family("pickle", pickle_dumps, pickle_loads)
register_serialization_family("msgpack", msgpack_dumps, msgpack_loads)
register_serialization_family("error", None, serialization_error_loads)

def deserialize(header, frames, deserializers=None):
   """
   Convert serialized header and list of bytestrings back to a Python object

   Parameters
   ----------
   header: dict
   frames: list of bytes
   deserializers : Optional[Dict[str, Tuple[Callable, Callable, bool]]]
     An optional dict mapping a name to a (de)serializer.
     See `dask_serialize` and `dask_deserialize` for more.

   See Also
   --------
   serialize
   """
   name = header.get("serializer")
   if deserializers is not None and name not in deserializers:
      raise TypeError(
         "Data serialized with %s but only able to deserialize "
         "data with %s" % (name, str(list(deserializers)))
      )
   dumps, loads, wants_context = families[name]
   return loads(header, frames)

def deserialize_object_with_dict(header, frames):
   cls = pickle.loads(header["type-serialized"])
   if issubclass(cls, dict):
      dd = obj = {}
   else:
      obj = object.__new__(cls)
      dd = obj.__dict__
   dd.update(header["simple"])
   for k, d in header["complex"].items():
      h = d["header"]
      f = frames[d["start"] : d["stop"]]
      v = deserialize(h, f)
      dd[k] = v

   return obj

_deserialize = deserialize 

class Serialize(object):
   """ Mark an object that should be serialized

   Example
   -------
   >>> msg = {'op': 'update', 'data': to_serialize(123)}
   >>> msg  # doctest: +SKIP
   {'op': 'update', 'data': <Serialize: 123>}

   See also
   --------
   distributed.protocol.dumps
   """

   def __init__(self, data):
      self.data = data

   def __repr__(self):
      return "<Serialize: %s>" % str(self.data)

   def __eq__(self, other):
      return isinstance(other, Serialize) and other.data == self.data

   def __ne__(self, other):
      return not (self == other)

   def __hash__(self):
      return hash(self.data)

to_serialize = Serialize      
      
class Serialized(object):
   """
   An object that is already serialized into header and frames

   Normal serialization operations pass these objects through.  This is
   typically used within the scheduler which accepts messages that contain
   data without actually unpacking that data.
   """

   def __init__(self, header, frames):
      self.header = header
      self.frames = frames

   def deserialize(self):
      from compression import decompress

      frames = decompress(self.header, self.frames)
      return deserialize(self.header, frames)

   def __eq__(self, other):
      return (
         isinstance(other, Serialized)
         and other.header == self.header
         and other.frames == self.frames
      )

   def __ne__(self, other):
      return not (self == other) 

@xray_recorder.capture(" _extract_serialize")      
def _extract_serialize(x, ser, path=()):
   if type(x) is dict:
      for k, v in x.items():
         typ = type(v)
         if typ is list or typ is dict:
            _extract_serialize(v, ser, path + (k,))
         elif (
            typ is Serialize
            or typ is Serialized
            or typ in (bytes, bytearray)
            and len(v) > 2 ** 16
         ):
            ser[path + (k,)] = v
   elif type(x) is list:
      for k, v in enumerate(x):
         typ = type(v)
         if typ is list or typ is dict:
            _extract_serialize(v, ser, path + (k,))
         elif (
            typ is Serialize
            or typ is Serialized
            or typ in (bytes, bytearray)
            and len(v) > 2 ** 16
         ):
            ser[path + (k,)] = v   
   
@xray_recorder.capture(" extract_serialize")
def extract_serialize(x):
   """ Pull out Serialize objects from message

   This also remove large bytestrings from the message into a second
   dictionary.

   Examples
   --------
   >>> from distributed.protocol import to_serialize
   >>> msg = {'op': 'update', 'data': to_serialize(123)}
   >>> extract_serialize(msg)
   ({'op': 'update'}, {('data',): <Serialize: 123>}, set())
   """
   ser = {}
   _extract_serialize(x, ser)
   if ser:
      x = container_copy(x)
      for path in ser:
         t = get_in(path[:-1], x)
         if isinstance(t, dict):
            del t[path[-1]]
         else:
            t[path[-1]] = None

   bytestrings = set()
   for k, v in ser.items():
      if type(v) in (bytes, bytearray):
         ser[k] = to_serialize(v)
         bytestrings.add(k)
   return x, ser, bytestrings        
   
@gen.coroutine
def to_frames(msg, serializers=None, on_error="message", context=None):
   """
   Serialize a message into a list of Distributed protocol frames.
   """

   def _to_frames():
      try:
         return list(
            dumps(msg, serializers=serializers, on_error=on_error, context=context)
         )
      except Exception as e:
         #logger.info("Unserializable Message: %s", msg)
         #logger.exception(e)
         print("Unserializable Message: ", msg)
         print("Exception: ", e)
         raise

   res = _to_frames()

   raise gen.Return(res)   
   
@gen.coroutine
def from_frames(frames, deserialize=True, deserializers=None):
   """
   Unserialize a list of Distributed protocol frames.
   """
   size = sum(map(nbytes, frames))

   def _from_frames():
      try:
         return loads(
            frames, deserialize=deserialize, deserializers=deserializers
         )
      except EOFError:
         if size > 1000:
            datastr = "[too large to display]"
         else:
            datastr = frames
            # Aid diagnosing
            #logger.error("truncated data stream (%d bytes): %s", size, datastr)
            print("truncated data stream (", size, " bytes): ", datastr)
            raise

   res = _from_frames()
   #print("\nRes: ", res)
   raise gen.Return(res)   

@xray_recorder.capture(" dumps_msgpack")   
def dumps_msgpack(msg):
   """ Dump msg into header and payload, both bytestrings

   All of the message must be msgpack encodable

   See Also:
     loads_msgpack
   """
   header = {}
   payload = msgpack.dumps(msg, use_bin_type=True)

   fmt, payload = maybe_compress(payload)
   if fmt:
      header["compression"] = fmt

   if header:
      header_bytes = msgpack.dumps(header, use_bin_type=True)
   else:
      header_bytes = b""

   return [header_bytes, payload]   
   
def loads_msgpack(header, payload):
   """ Read msgpack header and payload back to Python object

   See Also:
     dumps_msgpack
   """
   if header:
      header = msgpack.loads(header, use_list=False, **msgpack_opts)
   else:
      header = {}

   if header.get("compression"):
      try:
         decompress = compressions[header["compression"]]["decompress"]
         payload = decompress(payload)
      except KeyError:
         print("ERROR: data is compressed as ", str(header["compression"]), " but we don't have this installed...")
         raise ValueError("Data is compressed as {} but we don't have this installed".format(str(header["compression"])))

   return msgpack.loads(payload, use_list=False, raw=False, **msgpack_opts)
   
@xray_recorder.capture(" dumps")
def dumps(msg, serializers=None, on_error="message", context=None):
   """ Transform Python message to bytestream suitable for communication """
   try:
      data = {}
      # Only lists and dicts can contain serialized values
      if isinstance(msg, (list, dict)):
         msg, data, bytestrings = extract_serialize(msg)
      small_header, small_payload = dumps_msgpack(msg)

      if not data:  # fast path without serialized data
         return small_header, small_payload

      pre = {
         key: (value.header, value.frames)
         for key, value in data.items()
         if type(value) is Serialized
      }

      data = {
         key: serialize(
            value.data, serializers=serializers, on_error=on_error, context=context
         )
         for key, value in data.items()
         if type(value) is Serialize
      }

      header = {"headers": {}, "keys": [], "bytestrings": list(bytestrings)}

      out_frames = []

      for key, (head, frames) in data.items():
         if "lengths" not in head:
            head["lengths"] = tuple(map(nbytes, frames))
         if "compression" not in head:
            frames = frame_split_size(frames)
            if frames:
               compression, frames = zip(*map(maybe_compress, frames))
            else:
               compression = []
            head["compression"] = compression
         head["count"] = len(frames)
         header["headers"][key] = head
         header["keys"].append(key)
         out_frames.extend(frames)

      for key, (head, frames) in pre.items():
         if "lengths" not in head:
            head["lengths"] = tuple(map(nbytes, frames))
         head["count"] = len(frames)
         header["headers"][key] = head
         header["keys"].append(key)
         out_frames.extend(frames)

      for i, frame in enumerate(out_frames):
         if type(frame) is memoryview and frame.strides != (1,):
            try:
               frame = frame.cast("b")
            except TypeError:
               frame = frame.tobytes()
            out_frames[i] = frame

      return [
         small_header,
         small_payload,
         msgpack.dumps(header, use_bin_type=True),
      ] + out_frames
   except Exception:
      #logger.critical("Failed to Serialize", exc_info=True)
      print("CRITICAL: Failed to Serialize.")
      raise   

def loads(frames, deserialize=True, deserializers=None):
   """ Transform bytestream back into Python value """
   frames = frames[::-1]  # reverse order to improve pop efficiency
   if not isinstance(frames, list):
      frames = list(frames)
   try:
      small_header = frames.pop()
      small_payload = frames.pop()
      msg = loads_msgpack(small_header, small_payload)
      if not frames:
         return msg

      header = frames.pop()
      header = msgpack.loads(header, use_list=False, raw=False, **msgpack_opts)
      #print("Header: ", header)
      keys = header["keys"]
      headers = header["headers"]
      bytestrings = set(header["bytestrings"])
      #print("Number of keys: ", len(keys))
      for key in keys:
         head = headers[key]
         count = head["count"]
         if count:
            fs = frames[-count::][::-1]
            del frames[-count:]
         else:
            fs = []

         if deserialize or key in bytestrings:
            if "compression" in head:
               fs = decompress(head, fs)
            fs = merge_frames(head, fs)
            value = _deserialize(head, fs, deserializers=deserializers)
         else:
            value = Serialized(head, fs)

         #print("Key: ", key)
         def put_in(keys, coll, val):
            """Inverse of get_in, but does type promotion in the case of lists"""
            if keys:
               holder = reduce(operator.getitem, keys[:-1], coll)
               #print("Holder: ", holder)
               if isinstance(holder, tuple):
                  holder = list(holder)
                  coll = put_in(keys[:-1], coll, holder)
               holder[keys[-1]] = val
            else:
               coll = val
            return coll

         #print("BEFORE PUT_IN (on this iteration):\nKey: ", key, ", Msg: ", msg, ", Value: ", value)
         msg = put_in(key, msg, value)
         #print("\nAFTER PUT_IN (on this iteration):\nKey: ", key, ", Msg: ", msg, ", Value: ", value)
      #print("\nMESSAGE BEING RETURNED:\n", msg, "\n")
      return msg
   except Exception:
      #logger.critical("Failed to deserialize", exc_info=True)
      print("CRITICAL: Failed to deserialize.")
      raise      
      
def container_copy(c):
   typ = type(c)
   if typ is list:
     return list(map(container_copy, c))
   if typ is dict:
     return valmap(container_copy, c)
   return c    
   
def _always_use_pickle_for(x):
   mod, _, _ = x.__class__.__module__.partition(".")
   if mod == "numpy":
      import numpy as np

      return isinstance(x, np.ndarray)
   elif mod == "pandas":
      import pandas as pd

      return isinstance(x, pd.core.generic.NDFrame)
   elif mod == "builtins":
      return isinstance(x, (str, bytes))
   else:
      return False

@xray_recorder.capture(" protocol_pickle_dumps")   
def protocol_pickle_dumps(x):
   """ Manage between cloudpickle and pickle
   
   This is the dumps file from Dask.protocol.pickle
   
   1.  Try pickle
   2.  If it is short then check if it contains __main__
   3.  If it is long, then first check type, then check __main__
   """
   try:
      result = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
      if len(result) < 1000:
         if b"__main__" in result:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
         else:
            return result
      else:
         if _always_use_pickle_for(x) or b"__main__" not in result:
            return result
         else:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
   except Exception:
      try:
         return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
      except Exception as e:
         # logger.info("Failed to serialize %s. Exception: %s", x, e)
         print("[ERROR] Failed to serialize {}. Exception: {}".format(x,e))
         raise

def protocol_pickle_loads(x):
   """This is the loads file from Dask.protocol.pickle"""
   try:
      return pickle.loads(x)
   except Exception:
      # logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
      print("[ERROR] Failed to deserialize ", x[:10000])
      raise   

def ensure_bytes(s):
   """ Turn string or bytes to bytes

   >>> ensure_bytes('123')
   b'123'
   >>> ensure_bytes(b'123')
   b'123'
   """
   if isinstance(s, bytes):
      return s
   if isinstance(s, memoryview):
      return s.tobytes()
   if isinstance(s, bytearray) or PY2 and isinstance(s, buffer):  # noqa: F821
      return bytes(s)
   if hasattr(s, "encode"):
      return s.encode()
   raise TypeError("Object %s is neither a bytes object nor has an encode method" % s)    
      
def merge_frames(header, frames):
   """ Merge frames into original lengths

   Examples
   --------
   >>> merge_frames({'lengths': [3, 3]}, [b'123456'])
   [b'123', b'456']
   >>> merge_frames({'lengths': [6]}, [b'123', b'456'])
   [b'123456']
   """
   lengths = list(header["lengths"])

   if not frames:
      return frames

   assert sum(lengths) == sum(map(nbytes, frames))

   if all(len(f) == l for f, l in zip(frames, lengths)):
      return frames

   frames = frames[::-1]
   lengths = lengths[::-1]

   out = []
   while lengths:
      l = lengths.pop()
      L = []
      while l:
         frame = frames.pop()
         if nbytes(frame) <= l:
            L.append(frame)
            l -= nbytes(frame)
         else:
            mv = memoryview(frame)
            L.append(mv[:l])
            frames.append(mv[l:])
            l = 0
      out.append(b"".join(map(ensure_bytes, L)))
   return out

def pack_frames_prelude(frames):
   lengths = [len(f) for f in frames]
   lengths = [struct.pack("Q", len(frames))] + [
      struct.pack("Q", nbytes(frame)) for frame in frames
   ]
   return b"".join(lengths)

def pack_frames(frames):
   """ Pack frames into a byte-like object

   This prepends length information to the front of the bytes-like object

   See Also
   --------
   unpack_frames
   """
   prelude = [pack_frames_prelude(frames)]

   if not isinstance(frames, list):
      frames = list(frames)

   return b"".join(prelude + frames)
   
def unpack_frames(b):
   """ Unpack bytes into a sequence of frames

   This assumes that length information is at the front of the bytestring,
   as performed by pack_frames

   See Also
   --------
   pack_frames
   """
   (n_frames,) = struct.unpack("Q", b[:8])

   frames = []
   start = 8 + n_frames * 8
   for i in range(n_frames):
      (length,) = struct.unpack("Q", b[(i + 1) * 8 : (i + 2) * 8])
      frame = b[start : start + length]
      frames.append(frame)
      start += length

   return frames        