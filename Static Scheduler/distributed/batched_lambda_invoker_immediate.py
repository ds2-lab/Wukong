from __future__ import print_function, division, absolute_import

from collections import deque
import logging

import dask
from tornado import gen, locks
from tornado.ioloop import IOLoop
from math import ceil

from .core import CommClosedError
from .utils import parse_timedelta

import json

logger = logging.getLogger(__name__)


class BatchedLambdaInvoker(object):
    """ Batch Lambda invocations 

    This takes an IOStream and an interval (in ms) and ensures that we invoke tasks grouped together every interval milliseconds.

    Batching several tasks at once helps performance when sending
    a myriad of tiny tasks.
    
    """

    # XXX why doesn't BatchedSend follow either the IOStream or Comm API?

    def __init__(self, interval, debug_print = False, chunk_size = 5, loop=None, serializers=None):
        # XXX is the loop arg useful?
        self.loop = loop or IOLoop.current()
        self.interval = parse_timedelta(interval, default="ms")
        self.waker = locks.Event()
        self.stopped = locks.Event()
        self.please_stop = False
        self.buffer = []
        self.message_count = 0
        self.batch_count = 0
        self.chunk_size = chunk_size
        self.byte_count = 0
        self.total_lambdas_invoked = 0
        self.num_tasks_invoked = 0
        self.next_deadline = None
        self.debug_print = debug_print 
        self.lambda_client = None 
        self.recent_message_log = deque(
            maxlen=dask.config.get("distributed.comm.recent-messages-log-length")
        )
        self.serializers = serializers

    def start(self, lambda_client, scheduler_address):
        print("Starting BatchedLambdaInvoker with interval {}...".format(self.interval))
        self.lambda_client = lambda_client
        self.loop.add_callback(self._background_send)
        self.scheduler_address = scheduler_address

    def closed(self):
        return False

    def __repr__(self):
        if self.closed():
            return "<BatchedSend: closed>"
        else:
            return "<BatchedSend: %d in buffer>" % len(self.buffer)

    __str__ = __repr__

    @gen.coroutine
    def _background_send(self):
        while not self.please_stop:
            try:
                yield self.waker.wait(self.next_deadline)
                self.waker.clear()
            except gen.TimeoutError:
                pass
            if not self.buffer:
                # Nothing to send
                self.next_deadline = None
                continue   
            if self.next_deadline is not None and self.loop.time() < self.next_deadline:
                # Send interval not expired yet
                continue
            payload, self.buffer = self.buffer, []
            self.batch_count += 1
            self.next_deadline = self.loop.time() + self.interval
            try:
                # Break these up into chunks of size `self.chunk_size`
                if self.debug_print:
                    print("Breaking payload of length {} into {} chunks of length {} or smaller...".format(len(payload), ceil(len(payload) / self.chunk_size), self.chunk_size))
                chunks = [payload[x : x + self.chunk_size] for x in range(0, len(payload), self.chunk_size)]
                self.total_lambdas_invoked = self.total_lambdas_invoked + len(chunks)
                self.num_tasks_invoked = self.num_tasks_invoked + len(payload)
                # Send each chunk to an invocation of the AWS Lambda function for evaluation.
                for chunk in chunks:
                    to_be_serialized = {"task_list": chunk, "scheduler-address": self.scheduler_address}
                    serialized_payload = json.dumps(to_be_serialized)
                    if self.debug_print:
                        print("serialized_payload: {}".format(str(serialized_payload)))
                    self.lambda_client.invoke(FunctionName='FunctionExecutor', InvocationType='Event', Payload=serialized_payload)
                print("self.total_lambdas_invoked: ", self.total_lambdas_invoked)
                print("self.num_tasks_invoked: ", self.num_tasks_invoked)
            except Exception:
                logger.exception("Error in batched write")
                print("Error in batched write.")
                break
            finally:
                payload = None  # lose ref

        self.stopped.set()
        
    def enqueue(self, payload):
        """ Schedule a task for sending to Lambda

        This completes quickly and synchronously
        """
        self.message_count += 1
        self.buffer.append(payload)
        
        if len(self.buffer) > self.chunk_size:
            self.submit_lambdas()
        
        # Avoid spurious wakeups if possible
        if self.next_deadline is None:
            self.waker.set()

    def submit_lambdas(self):
        """ Take whatever is in the buffer and send it off to AWS Lambda for processing. """
        payload, self.buffer = self.buffer, []
        self.batch_count += 1
        # self.next_deadline = self.loop.time() + self.interval
        try:
            # Break these up into chunks of size `self.chunk_size`
            if self.debug_print:
                print("Breaking payload of length {} into {} chunks of length {} or smaller...".format(len(payload), ceil(len(payload) / self.chunk_size), self.chunk_size))
            chunks = [payload[x : x + self.chunk_size] for x in range(0, len(payload), self.chunk_size)]
            self.total_lambdas_invoked = self.total_lambdas_invoked + len(chunks)
            self.num_tasks_invoked = self.num_tasks_invoked + len(payload)
            # Send each chunk to an invocation of the AWS Lambda function for evaluation.
            for chunk in chunks:
                to_be_serialized = {"task_list": chunk, "scheduler-address": self.scheduler_address}
                serialized_payload = json.dumps(to_be_serialized)
                if self.debug_print:
                    print("serialized_payload: {}".format(str(serialized_payload)))
                self.lambda_client.invoke(FunctionName='FunctionExecutor', InvocationType='Event', Payload=serialized_payload)
            print("self.total_lambdas_invoked: ", self.total_lambdas_invoked)
            print("self.num_tasks_invoked: ", self.num_tasks_invoked)
        except Exception:
            logger.exception("Error in batched write")
            print("Error in batched write.")
            break
        finally:
            payload = None  # lose ref    
            
    @gen.coroutine
    def close(self):
        """ Flush existing messages"""
        self.please_stop = True
        self.waker.set()
        # TO-DO: Gracefully send off remaining Lambdas.

    def abort(self):
        self.please_stop = True
        self.buffer = []
        self.waker.set()
