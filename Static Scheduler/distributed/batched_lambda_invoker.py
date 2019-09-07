from __future__ import print_function, division, absolute_import

from collections import deque
import logging

import dask
from tornado import gen, locks
from tornado.ioloop import IOLoop

from multiprocessing import Process, Pipe

from .core import CommClosedError
from .utils import parse_timedelta

from random import randint
import boto3
import datetime
import json
import time 
from math import ceil

logger = logging.getLogger(__name__)


class BatchedLambdaInvoker(object):
    """ Batch Lambda invocations 

    This takes an IOStream and an interval (in ms) and ensures that we invoke tasks grouped together every interval milliseconds.

    Batching several tasks at once helps performance when sending
    a myriad of tiny tasks.
    
    """

    # XXX why doesn't BatchedSend follow either the IOStream or Comm API?
    def __init__(self, interval, use_multiple_invokers = True, function_name="WukongTaskExecutor", num_invokers = 16, redis_channel_names = None, debug_print = False, 
            chunk_size = 5, loop=None, serializers=None, minimum_tasks_for_multiple_invokers = 8):
        # XXX is the loop arg useful?
        self.loop = loop or IOLoop.current()
        self.interval = parse_timedelta(interval, default="ms")
        self.waker = locks.Event()
        self.stopped = locks.Event()
        self.please_stop = False
        self.redis_channel_names = redis_channel_names
        self.current_redis_channel_index = 0
        self.buffer = []
        self.message_count = 0
        self.lambda_function_name = function_name
        self.batch_count = 0
        self.chunk_size = chunk_size
        self.byte_count = 0
        self.total_lambdas_invoked = 0
        self.minimum_tasks_for_multiple_invokers = minimum_tasks_for_multiple_invokers
        self.num_tasks_invoked = 0
        self.next_deadline = None
        self.debug_print = debug_print 
        self.lambda_client = None 
        self.time_spent_invoking = 0
        self.lambda_invokers = []
        self.lambda_pipes = []
        self.use_multiple_invokers = use_multiple_invokers 
        self.num_invokers = num_invokers
        self.recent_message_log = deque(
            maxlen=dask.config.get("distributed.comm.recent-messages-log-length")
        )
        self.serializers = serializers

    def start(self, lambda_client, scheduler_address):
        print("Starting BatchedLambdaInvoker with interval {}...".format(self.interval))
        self.lambda_client = lambda_client
        self.loop.add_callback(self._background_send)
        self.scheduler_address = scheduler_address
        
        print("[ {} ] BatchedLambdaInvoker - INFO: Launching {} ''Lambda Invoker'' processes.".format(datetime.datetime.utcnow(), self.num_invokers))
        for i in range(0, self.num_invokers):
            #(self, conn, chunk_size, scheduler_address, redis_channel_names)
            receiving_conn, sending_conn = Pipe()
            
            invoker = Process(target = self.invoker_polling_process, args = (i, receiving_conn, self.scheduler_address, self.redis_channel_names,))
            invoker.daemon = True 
            self.lambda_pipes.append(sending_conn)
            self.lambda_invokers.append(invoker)
            
        for invoker in self.lambda_invokers:
            invoker.start()
        
    def set_proxy_redis_channels(self, channels):
        self.proxy_redis_channels = channels
        self.current_redis_channel_index = 0
        self.num_proxy_channels = len(channels)

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
            self.next_deadline = self.loop.time() + self.interval            # Break the payload up into chunks -- one chunk for each invoker process AND the Scheduler process itself.
            payload_chunk_size = ceil(len(payload) / (self.num_invokers + 1))    # We divide by num_invokers + 1 since the Scheduler can also invoke Lambda functions itself.
            print("[ {} ] Size of payload (number of things that were in buffer): {}".format(datetime.datetime.utcnow(), len(payload)))
            print("[ {} ] Payload Chunk Size: {}".format(datetime.datetime.utcnow(), payload_chunk_size))
            payloads = [payload[x : x + payload_chunk_size] for x in range(0, len(payload), payload_chunk_size)]
            print("[ {} ] Number of payloads created: {}".format(datetime.datetime.utcnow(), len(payloads)))
            # The scheduler gets the first payload. This is important because,
            # in the case where there is only one payload, we want the Scheduler 
            # to invoke the Lambda function itself.
            scheduler_payload = payloads[0] 
            # Create a separate index to walk through the invoker list.
            invoker_index = 0
            # Send each invoker its respective payload.
            for i in range(1, len(payloads)):
                sent_time = time.time()
                #print("Sending payload to invoker process: {}".format(payloads[i]))
                msg = {"payload": payloads[i], "sent-time": sent_time}
                conn = self.lambda_pipes[invoker_index]
                conn.send(json.dumps(msg)) #TODO Can't we just send this without the json.dumps()?
                invoker_index += 1
            try:
                send_start_time = time.time()
                # Send each chunk to an invocation of the AWS Lambda function for evaluation.
                total_time_spent_serializing = 0
                total_time_spent_invoking = 0
                for payload in scheduler_payload:
                    time_invoke_start = time.time()
                    self.lambda_client.invoke(FunctionName=self.lambda_function_name, InvocationType='Event', Payload=payload)
                    time_invoke_end = time.time()
                    diff_invoke = time_invoke_end - time_invoke_start
                    total_time_spent_invoking += diff_invoke 
                send_done_time = time.time()
                
                self.total_lambdas_invoked = self.total_lambdas_invoked + len(scheduler_payload)
                self.num_tasks_invoked = self.num_tasks_invoked + len(scheduler_payload)                
                timestamp_now = datetime.datetime.utcnow()
                # The self.time_spent_invoking variable is the total time we've spent calling invoke() over the
                # entire lifetime of this Scheduler/batched lambda invoker object (not just the most recent set of invocations).
                self.time_spent_invoking += total_time_spent_invoking
                print("\n[ {} ] BatchedLambdaInvoker - INFO: Invoked {} Lambda functions in {} seconds.".format(timestamp_now, len(scheduler_payload), send_done_time - send_start_time))
                print("                               Serialization took {} seconds. Invocation took {} seconds.".format(total_time_spent_serializing, total_time_spent_invoking))
                print("                               Total Lambdas Invoked: {}. Total # Tasks Invoked: {}".format(self.total_lambdas_invoked, self.num_tasks_invoked))
                print("                               The Scheduler has spent {} seconds calling invoke() so far.".format(self.time_spent_invoking))
            except Exception:
                logger.exception("Error in batched write")
                print("Error in batched write.")
                break
            finally:
                payload = None  # lose ref

        self.stopped.set()

    def send(self, msg):
        """ Schedule a task for sending to Lambda

        This completes quickly and synchronously
        """
        self.message_count += 1
        self.buffer.append(msg)
        # Avoid spurious wakeups if possible
        if self.next_deadline is None:
            self.waker.set()

    @gen.coroutine
    def close(self):
        """ Flush existing messages"""
        # TO-DO: Gracefully send off remaining Lambdas.
        
        self.please_stop = True
        self.waker.set()
        
        # Terminate each of the processes.
        for process in self.lambda_invokers:
            process.terminate()
        
        # Close all of the Connection objects.
        for conn in self.lambda_pipes:
            conn.close()
            

    def abort(self):
        self.please_stop = True
        self.buffer = []
        self.waker.set()

        # Terminate each of the processes.
        for process in self.lambda_invokers:
            process.terminate()
        
        # Close all of the Connection objects.
        for conn in self.lambda_pipes:
            conn.close()        

    def invoker_polling_process(self, ID, conn, scheduler_address, redis_channel_names):
        """ This function runs as a separate process, invoking Lambda functions in parallel.
            
            It continually polls the given Connection object, checking for new payloads. If
            there are no messages in the Pipe, then the process sleeps. The sleep time increases 
            for consecutive empty Pipes."""
        print("[ {} ] - Lambda Invoker Process {} - INFO: Lambda Invoker Process began executing...".format(datetime.datetime.utcnow(), ID))
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        current_redis_channel_index = 0
        #lambda_batch_size = chunk_size                # How many tasks are sent to an individual Lambda function.
        #lambda_queue = []                            # Messages are put into this queue for processing/invocation.
        expected_messages = None                      # This is how many messages we are expecting to get.
        base_sleep_interval = 0.005                   # The starting sleep interval.
        sleep_interval_increment = 0.005              # We increment the sleep interval by this much for consecutive sleeps.
        max_sleep_interval = 0.025                    # We do not sleep longer than this (we do not want to leave messages in the pipe for too long).
        current_sleep_interval = base_sleep_interval  # This is the amount by which we will sleep during the next sleep.
        current_payload = None                        # This is the current payload we are processing and submitting to AWS Lambda.
        redis_channel_index = 0                       # Used to pick the Redis channel to assign to a given Lambda function during invocation.
        while True:
            data_available = conn.poll()
            if data_available:
                # Grab the message from the connection.
                _msg = conn.recv()
                msg  = json.loads(_msg)
                sent_time = msg["sent-time"]
                current_payload = msg["payload"]
                received_time = time.time()
                print("[ {} ] Lambda Invoker Process {} - INFO: Received message from connection. Took {} seconds to transfer through conn.".format(datetime.datetime.utcnow(), ID, received_time - sent_time))
                num_lambdas_submitted = 0
                start = time.time()
                for payload in current_payload:
                    lambda_client.invoke(FunctionName=self.lambda_function_name, InvocationType='Event', Payload=payload)
                    num_lambdas_submitted += 1
                stop = time.time()                
                time_to_submit = stop - start 
                print("[ {} ] Lambda Invoker Process {} - INFO: submitted {} Lambdas in {} seconds.".format(datetime.datetime.utcnow(), ID, num_lambdas_submitted, time_to_submit))
                # Since there was a message in the pipe, we set the sleep interval back to the base amount.
                current_sleep_interval = base_sleep_interval
            else:
                time.sleep(current_sleep_interval)
                current_sleep_interval += sleep_interval_increment
                # Constrain the sleep interval to the specified max.
                if current_sleep_interval > max_sleep_interval:
                    current_sleep_interval = base_sleep_interval 

    def invoker_process(self, payload, chunk_size, scheduler_address, redis_channel_names):
        """ Invokes Lambdas to execute the tasks contained within the given payload."""
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        num_lambdas_submitted = 0
        # Randomly pick a starting index so certain channels are not necessarily picked more often than others.
        redis_channel_index = randint(0, (len(redis_channel_names) - 1))
        start = time.time()
        # Break up the obtained payload into chunks of size 'chunk_size'
        chunks = [payload[x : x + chunk_size] for x in range(0, len(payload), chunk_size)]  
        for chunk in chunks:
            to_be_serialized = {"task_list": chunk, "scheduler-address": scheduler_address, "redis-channel": redis_channel_names[redis_channel_index]}  
            serialized_payload = json.dumps(to_be_serialized)
            lambda_client.invoke(FunctionName=self.lambda_function_name, InvocationType='Event', Payload=serialized_payload)
            redis_channel_index += 1
            if redis_channel_index >= len(redis_channel_names):
                redis_channel_index = 0            
            num_lambdas_submitted += 1
        stop = time.time()                
        time_to_submit = stop - start 
        print("\n[ {} ] Lambda Invoker Process - INFO: submitted {} Lambdas in {} seconds.".format(datetime.datetime.utcnow(), num_lambdas_submitted, time_to_submit))        