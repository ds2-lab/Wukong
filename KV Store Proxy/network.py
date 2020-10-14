import tornado
import pickle
import cloudpickle
import sys
import json 
import socket
import six
import time 
import struct
from weakref import finalize

try:
   import ssl
except ImportError:
   ssl = None

from datetime import timedelta
from tornado import gen, netutil
from tornado.gen import Return
from tornado.iostream import StreamClosedError, IOStream
from tornado.tcpclient import TCPClient

from utils import nbytes, PY3, PY2
import serialization

def get_total_physical_memory():
    try:
        import psutil

        return psutil.virtual_memory().total / 2
    except ImportError:
        return 2e9

MAX_BUFFER_SIZE = get_total_physical_memory() 
DEFAULT_SCHEME = "tcp"
prefix = "tcp://"

class CommClosedError(IOError):
   pass

class FatalCommClosedError(CommClosedError):
   pass   

def parse_address(addr, strict=False):
   """
   Split address into its scheme and scheme-dependent location string.

   >>> parse_address('tcp://127.0.0.1')
   ('tcp', '127.0.0.1')

   If strict is set to true the address must have a scheme.
   """
   if not isinstance(addr, six.string_types):
      raise TypeError("expected str, got %r" % addr.__class__.__name__)
   scheme, sep, loc = addr.rpartition("://")
   if strict and not sep:
      msg = (
         "Invalid url scheme. "
         "Must include protocol like tcp://localhost:8000. "
         "Got %s" % addr
      )
      raise ValueError(msg)
   if not sep:
      scheme = DEFAULT_SCHEME
   return scheme, loc         
         
def parse_host_port(address, default_port=None):
    """
    Parse an endpoint address given in the form "host:port".
    """
    if isinstance(address, tuple):
        return address

    def _fail():
        raise ValueError("invalid address %r" % (address,))

    def _default():
        if default_port is None:
            raise ValueError("missing port number in address %r" % (address,))
        return default_port

    if address.startswith("["):
        # IPv6 notation: '[addr]:port' or '[addr]'.
        # The address may contain multiple colons.
        host, sep, tail = address[1:].partition("]")
        if not sep:
            _fail()
        if not tail:
            port = _default()
        else:
            if not tail.startswith(":"):
                _fail()
            port = tail[1:]
    else:
        # Generic notation: 'addr:port' or 'addr'.
        host, sep, port = address.partition(":")
        if not sep:
            port = _default()
        elif ":" in host:
            _fail()

    return host, int(port)         

@gen.coroutine
def connect_to_address(addr, timeout = None, deserialize = True, connection_args = None):         
   """
   Connect to the given address (a URI such as ``tcp://127.0.0.1:1234``)
   and yield a ``Comm`` object.  If the connection attempt fails, it is
   retried until the *timeout* is expired.
   """         
   if timeout is None:
      timeout = 11 # Seconds 
   scheme, loc = parse_address(addr)
   start = time.time()
   # print("Start: ", start, ". Timeout: ", timeout)
   deadline = start + timeout
   error = None 
   print("Attempting to connect to address ", addr)
   def _raise(error):
      error = error or "connect() didn't finish in time"
      msg = "Timed out trying to connect to %r after %s s: %s" % (
         addr,
         timeout,
         error,
      )
      raise IOError(msg)

   while True:
      try:
         future = connect(loc, deserialize = deserialize, **(connection_args or {}))
         comm = yield gen.with_timeout(timedelta(seconds = deadline - time.time()), future, quiet_exceptions = EnvironmentError,)
      except FatalCommClosedError:
         raise
      except EnvironmentError as e:
         error = str(e)
         if time.time() < deadline:
            yield gen.sleep(0.01)
            print("sleeping on connect")
         else:
            _raise(error)
      except gen.TimeoutError:
         _raise(error)
      else:
         break         
   raise gen.Return(comm)      

@gen.coroutine
def connect(address, deserialize = True, **connection_args):
   ip, port = parse_host_port(address)
   #kwargs = self._get_connect_args(**connection_args)
   kwargs = {} # The method in Dask just returns {} as far as I can tell
   client = TCPClient()
   try:
      stream = yield client.connect(ip, port, max_buffer_size = MAX_BUFFER_SIZE, **kwargs)
      
      # Under certain circumstances tornado will have a closed connnection with an error and not raise
      # a StreamClosedError.
      #
      # This occurs with tornado 5.x and openssl 1.1+      
      if stream.closed() and stream.error: 
         raise StreamClosedError(stream.error)
   except StreamClosedError as e:
      # The socket connect() call failed
      convert_stream_closed_error("Lambda", e)         
   
   local_address = prefix + get_stream_address(stream)
   raise gen.Return(TCP(stream, local_address, prefix + address, deserialize))
   
def convert_stream_closed_error(obj, exc):
   """
   Re-raise StreamClosedError as CommClosedError.
   """
   if exc.real_error is not None:
      # The stream was closed because of an underlying OS error
      exc = exc.real_error
      if ssl and isinstance(exc, ssl.SSLError):
         if "UNKNOWN_CA" in exc.reason:
            raise FatalCommClosedError(
               "in %s: %s: %s" % (obj, exc.__class__.__name__, exc)
            )
      raise CommClosedError("in %s: %s: %s" % (obj, exc.__class__.__name__, exc))
   else:
      raise CommClosedError("in %s: %s" % (obj, exc))   
   
def get_stream_address(stream):
   """
   Get a stream's local address.
   """
   if stream.closed():
      return "<closed>"

   try:
      return unparse_host_port(*stream.socket.getsockname()[:2])
   except EnvironmentError:
      # Probably EBADF
      return "<closed>"   
   
def unparse_host_port(host, port=None):
   """
   Undo parse_host_port().
   """
   if ":" in host and not host.startswith("["):
      host = "[%s]" % host
   if port:
      return "%s:%s" % (host, port)
   else:
      return host      

def set_tcp_timeout(stream):
   """
   Set kernel-level TCP timeout on the stream.
   """
   if stream.closed():
      return

   #timeout = dask.config.get("distributed.comm.timeouts.tcp")
   #timeout = int(parse_timedelta(timeout, default="seconds"))
   timeout = 11 #seconds 
   
   sock = stream.socket

   # Default (unsettable) value on Windows
   # https://msdn.microsoft.com/en-us/library/windows/desktop/dd877220(v=vs.85).aspx
   nprobes = 10
   assert timeout >= nprobes + 1, "Timeout too low"

   idle = max(2, timeout // 4)
   interval = max(1, (timeout - idle) // nprobes)
   idle = timeout - interval * nprobes
   # print("Idle: ", idle)
   assert idle > 0

   try:
      if sys.platform.startswith("win"):
         #logger.debug("Setting TCP keepalive: idle=%d, interval=%d", idle, interval)
         #print("Setting TCP keepalive: idle=", idle, ", interval=", interval)
         sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, idle * 1000, interval * 1000))
      else:
         sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
         try:
            TCP_KEEPIDLE = socket.TCP_KEEPIDLE
            TCP_KEEPINTVL = socket.TCP_KEEPINTVL
            TCP_KEEPCNT = socket.TCP_KEEPCNT
         except AttributeError:
            if sys.platform == "darwin":
               TCP_KEEPIDLE = 0x10  # (named "TCP_KEEPALIVE" in C)
               TCP_KEEPINTVL = 0x101
               TCP_KEEPCNT = 0x102
            else:
               TCP_KEEPIDLE = None

         if TCP_KEEPIDLE is not None:
            #print("Setting TCP keepalive: nprobes=", nprobes, ", idle=", idle, ", interval=", interval)
            #logger.debug(
            #   "Setting TCP keepalive: nprobes=%d, idle=%d, interval=%d",
            #   nprobes,
            #   idle,
            #   interval,
            #)
            sock.setsockopt(socket.SOL_TCP, TCP_KEEPCNT, nprobes)
            sock.setsockopt(socket.SOL_TCP, TCP_KEEPIDLE, idle)
            sock.setsockopt(socket.SOL_TCP, TCP_KEEPINTVL, interval)

      if sys.platform.startswith("linux"):
         #logger.debug("Setting TCP user timeout: %d ms", timeout * 1000)
         #print("Setting TCP user timeout: ", timeout * 1000, " ms")
         TCP_USER_TIMEOUT = 18  # since Linux 2.6.37
         sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, timeout * 1000)
   except EnvironmentError as e:
      #logger.warning("Could not set timeout on TCP stream: %s", e)      
      print("WARNING: Could not set timeout on TCP stream: ", e)
   
class TCP():
   """
   An established communication based on an underlying Tornado IOStream.
   """

   _iostream_allows_memoryview = tornado.version_info >= (4, 5)
   # IOStream.read_into() currently proposed in
   # https://github.com/tornadoweb/tornado/pull/2193
   _iostream_has_read_into = hasattr(IOStream, "read_into")

   def __init__(self, stream, local_addr, peer_addr, deserialize=True):
      #Comm.__init__(self)
      self._local_addr = local_addr
      self._peer_addr = peer_addr
      self.stream = stream
      self.deserialize = deserialize
      self._finalizer = finalize(self, self._get_finalizer())
      self._finalizer.atexit = False
      self._extra = {}

      stream.set_nodelay(True)
      set_tcp_timeout(stream)
      self._read_extra()

   def _read_extra(self):
      pass

   def _get_finalizer(self):
      def finalize(stream=self.stream, r=repr(self)):
         if not stream.closed():
            print("Closing dangling stream in ", r)
            stream.close()
      return finalize

   @property
   def local_address(self):
      """
      The local address.  For logging and debugging purposes only.
      """   
      return self._local_addr

   @property
   def peer_address(self):
      """
      The peer's address.  For logging and debugging purposes only.
      """   
      return self._peer_addr

   @gen.coroutine
   def read(self, deserializers=None):
      # print("[ {} ] Attempting to read in TCP Comm...".format(datetime.datetime.utcnow()))
      stream = self.stream
      if stream is None:
         raise CommClosedError

      try:
         n_frames = yield stream.read_bytes(8)
         n_frames = struct.unpack("Q", n_frames)[0]
         lengths = yield stream.read_bytes(8 * n_frames)
         lengths = struct.unpack("Q" * n_frames, lengths)

         frames = []
         # print("[ {} ] Reading {} lengths now...".format(datetime.datetime.utcnow(), len(lengths)))
         for length in lengths:
            if length:
               if PY3 and self._iostream_has_read_into:
                  frame = bytearray(length)
                  n = yield stream.read_into(frame)
                  assert n == length, (n, length)
               else:
                  frame = yield stream.read_bytes(length)
            else:
               frame = b""
            frames.append(frame)
      except StreamClosedError as e:
         self.stream = None
         print("StreamClosedError...")
         raise StreamClosedError("Stream closed...")
      else:
         try:
            msg = yield serialization.from_frames(
               frames, deserialize=self.deserialize, deserializers=deserializers
            )
         except EOFError:
            # Frames possibly garbled or truncated by communication error
            self.abort()
            # print("aborted stream on truncated data")
            raise CommClosedError("aborted stream on truncated data")
      raise gen.Return(msg)

   @gen.coroutine
   def write(self, msg, serializers=None, on_error="message"):
      """
      Write a message (a Python object).

      This method is a coroutine.

      Parameters
      ----------
      msg :
      on_error : Optional[str]
         The behavior when serialization fails. See
         ``distributed.protocol.core.dumps`` for valid values.
      """   
      stream = self.stream
      bytes_since_last_yield = 0
      if stream is None:
         raise CommClosedError

      frames = yield serialization.to_frames(
         msg,
         serializers=serializers,
         on_error=on_error,
         context={"sender": self._local_addr, "recipient": self._peer_addr},
      )

      try:
         lengths = [nbytes(frame) for frame in frames]
         #print("Size of entire payload: ", sum(lengths))
         length_bytes = [struct.pack("Q", len(frames))] + [
            struct.pack("Q", x) for x in lengths
         ]
         # sys.version_info[0] == 3 means Python3 
         if (sys.version_info[0] == 3) and sum(lengths) < 2 ** 17:  # 128kiB
            b = b"".join(length_bytes + frames)  # small enough, send in one go
            stream.write(b)
         else:
            stream.write(b"".join(length_bytes))  # avoid large memcpy, send in many

            for frame in frames:
               # Can't wait for the write() Future as it may be lost
               # ("If write is called again before that Future has resolved,
               #   the previous future will be orphaned and will never resolve")
               if not self._iostream_allows_memoryview:
                  frame = serialization.ensure_bytes(frame)
               future = stream.write(frame)
               bytes_since_last_yield += nbytes(frame)
               if bytes_since_last_yield > 32e6:
                  yield future
                  bytes_since_last_yield = 0
      except StreamClosedError as e:
         stream = None
         convert_stream_closed_error("Lambda", e)
      except TypeError as e:
         if stream._write_buffer is None:
            print("tried to write message {} on closed stream".format(msg))
         else:
            raise

      raise gen.Return(sum(map(nbytes, frames)))

   @gen.coroutine
   def close(self):
      """
      Close the communication cleanly.  This will attempt to flush
      outgoing buffers before actually closing the underlying transport.

      This method is a coroutine.
      """
      stream, self.stream = self.stream, None
      if stream is not None and not stream.closed():
         try:
            # Flush the stream's write buffer by waiting for a last write.
            if stream.writing():
               yield stream.write(b"")
            stream.socket.shutdown(socket.SHUT_RDWR)
         except EnvironmentError:
            pass
         finally:
            self._finalizer.detach()
            stream.close()

   def abort(self):
      """
      Close the communication immediately and abruptly.
      Useful in destructors or generators' ``finally`` blocks.
      """   
      stream, self.stream = self.stream, None
      if stream is not None and not stream.closed():
         self._finalizer.detach()
         stream.close()

   def closed(self):
      """
      Return whether the stream is closed.
      """   
      return self.stream is None or self.stream.closed()

   @property
   def extra_info(self):
      """
      Return backend-specific information about the communication,
      as a dict.  Typically, this is information which is initialized
      when the communication is established and doesn't vary afterwards.
      """   
      return self._extra 