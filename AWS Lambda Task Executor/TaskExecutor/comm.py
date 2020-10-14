from abc import ABCMeta, abstractmethod, abstractproperty
from six import with_metaclass
import weakref

class Comm(with_metaclass(ABCMeta)):
   """
   A message-oriented communication object, representing an established
   communication channel.  There should be only one reader and one
   writer at a time: to manage current communications, even with a
   single peer, you must create distinct ``Comm`` objects.

   Messages are arbitrary Python objects.  Concrete implementations
   of this class can implement different serialization mechanisms
   depending on the underlying transport's characteristics.
   """

   _instances = weakref.WeakSet()

   def __init__(self):
      self._instances.add(self)
      self.name = None

   # XXX add set_close_callback()?

   @abstractmethod
   def read(self, deserializers=None):
      """
      Read and return a message (a Python object).

      This method is a coroutine.

      Parameters
      ----------
      deserializers : Optional[Dict[str, Tuple[Callable, Callable, bool]]]
         An optional dict appropriate for distributed.protocol.deserialize.
         See :ref:`serialization` for more.
      """

   @abstractmethod
   def write(self, msg, on_error=None):
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

   @abstractmethod
   def close(self):
      """
      Close the communication cleanly.  This will attempt to flush
      outgoing buffers before actually closing the underlying transport.

      This method is a coroutine.
      """

   @abstractmethod
   def abort(self):
      """
      Close the communication immediately and abruptly.
      Useful in destructors or generators' ``finally`` blocks.
      """

   @abstractmethod
   def closed(self):
      """
      Return whether the stream is closed.
      """

   @abstractproperty
   def local_address(self):
      """
      The local address.  For logging and debugging purposes only.
      """

   @abstractproperty
   def peer_address(self):
      """
      The peer's address.  For logging and debugging purposes only.
      """

   @property
   def extra_info(self):
      """
      Return backend-specific information about the communication,
      as a dict.  Typically, this is information which is initialized
      when the communication is established and doesn't vary afterwards.
      """
      return {}

   def __repr__(self):
      clsname = self.__class__.__name__
      if self.closed():
         return "<closed %s>" % (clsname,)
      else:
         return "<%s %s local=%s remote=%s>" % (
            clsname,
            self.name or "",
            self.local_address,
            self.peer_address,
         )