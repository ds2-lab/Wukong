from __future__ import print_function, division, absolute_import

from collections import deque
import logging

import dask
from tornado import gen, locks
from tornado.ioloop import IOLoop

from multiprocessing import Process, Pipe

from .core import CommClosedError
from .utils import parse_timedelta

import redis 
import ntplib
from random import randint
import sys
import boto3
import datetime
#import json
import ujson
import time 
from math import ceil
import random 
import string 

logger = logging.getLogger(__name__)


class BatchedLambdaInvoker(object):
    """ Batch Lambda invocations 

    This takes an IOStream and an interval (in ms) and ensures that we invoke tasks grouped together every interval milliseconds.

    Batching several tasks at once helps performance when sending
    a myriad of tiny tasks.
    
    """

    # XXX why doesn't BatchedSend follow either the IOStream or Comm API?
    # AsynchronousDaskExecutor
    # DeserializedDaskExecutor
    # UpdatedAlgorithmDaskExecutor
    # ModifiedPathDaskExecutor
    # RedisPartitioningDaskExecutor
    # ProxyPubSubDaskExecutor
    # RedisMultipleVMsExecutor
    # WukongExecutor
    def __init__(
        self, 
        interval, 
        use_multiple_invokers = True, 
        executor_function_name="WukongExecutor", 
        invoker_function_name="WukongInvoker", 
        num_invokers = 16,
        redis_channel_names = None, 
        debug_print = False, 
        chunk_size = 5, 
        loop=None, 
        serializers=None, 
        redis_address = None, 
        minimum_tasks_for_multiple_invokers = 8, 
        aws_region = 'us-east-1', 
        force_use_invoker_lambdas = False,
        use_invoker_lambdas_threshold = 10000):
        # XXX is the loop arg useful?
        self.loop = loop or IOLoop.current()
        self.interval = parse_timedelta(interval, default="ms")
        self.waker = locks.Event()
        self.stopped = locks.Event()
        self.redis_address = redis_address
        self.redis_client = redis.Redis(host = self.redis_address, port = 6379, db = 0)
        self.please_stop = False
        self.redis_channel_names = redis_channel_names
        self.current_redis_channel_index = 0
        self.buffer = []
        self.message_count = 0
        self.executor_function_name = executor_function_name
        self.invoker_function_name = invoker_function_name
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
        self.aws_region = aws_region
        self.use_multiple_invokers = use_multiple_invokers 
        self.num_invokers = num_invokers
        self.force_use_invoker_lambdas = force_use_invoker_lambdas
        self.use_invoker_lambdas_threshold = use_invoker_lambdas_threshold
        self.recent_message_log = deque(
            maxlen=dask.config.get("distributed.comm.recent-messages-log-length")
        )
        self.serializers = serializers
        #self.ntp_client = ntplib.NTPClient()

    def start(self, lambda_client, scheduler_address):
        print("Starting BatchedLambdaInvoker with interval {}...".format(self.interval))
        self.lambda_client = lambda_client
        self.loop.add_callback(self._background_send)
        self.scheduler_address = scheduler_address
        
        print("[ {} ] BatchedLambdaInvoker - INFO: Launching {} ''Lambda Invoker'' processes.".format(datetime.datetime.utcnow(), self.num_invokers))
        print("BatchedLambdaInvoker - Executor function name: \"{}\"".format(self.executor_function_name))
        print("BatchedLambdaInvoker - Invoker function name: \"{}\"".format(self.invoker_function_name))
        for i in range(0, self.num_invokers):
            #(self, conn, chunk_size, scheduler_address, redis_channel_names)
            receiving_conn, sending_conn = Pipe()
            
            invoker = Process(target = self.invoker_polling_process, args = (i, receiving_conn, self.scheduler_address, self.redis_channel_names, self.aws_region, self.redis_address))
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

            use_invoker_lambdas = False
            if len(payload) > self.use_invoker_lambdas_threshold or self.force_use_invoker_lambdas:
                use_invoker_lambdas = True
            
            # Send each invoker its respective payload.
            for i in range(1, len(payloads)):
                sent_time = time.time()
                msg = {"payload": payloads[i], "sent-time": sent_time, "use-invoker-lambdas": use_invoker_lambdas}
                conn = self.lambda_pipes[invoker_index]
                conn.send(msg)
                invoker_index += 1
            try:
                if use_invoker_lambdas:
                    # We want each Lambda invoker to invoke ~50 Lambdas.
                    payloads_for_lamba_invokers = [scheduler_payload[x : x + 50] for x in range(0, len(scheduler_payload), 50)]
                    timestamp_now = datetime.datetime.utcnow()
                    #print("[ {} ] BatchedLambdaInvoker - invoking {} Lambda invokers to parallelize Executor invocation.".format(timestamp_now, len(payloads_for_lamba_invokers)))
                    send_start_time = time.time()
                    # Send each chunk to an invocation of the AWS Lambda function for evaluation.
                    total_time_spent_serializing = 0
                    total_time_spent_invoking = 0  
                    rand_pick = string.ascii_uppercase + string.digits + string.ascii_lowercase
                    for payload in payloads_for_lamba_invokers:
                        t = time.time()
                        msg = {
                            "payloads_serialized": payload,
                            "lambda_function_name": self.executor_function_name
                        }
                        msg_serialized = ujson.dumps(msg)
                        if sys.getsizeof(msg_serialized) > 256000:
                            _key = ''.join(random.choice(rand_pick) for _ in range(20))
                            _new_msg_serialized = ujson.dumps({"lambda_function_name": self.executor_function_name, "redis_key": _key, "redis_address": self.redis_address})
                            self.redis_client.set(_key, msg_serialized)
                            e = time.time()
                            total_time_spent_serializing += (e - t)
                            time_invoke_start = time.time()
                            self.lambda_client.invoke(FunctionName=self.invoker_function_name, InvocationType='Event', Payload=_new_msg_serialized)
                            time_invoke_end = time.time()
                            diff_invoke = time_invoke_end - time_invoke_start
                            total_time_spent_invoking += diff_invoke           
                        else:
                            e = time.time()
                            total_time_spent_serializing += (e - t)
                            time_invoke_start = time.time()
                            self.lambda_client.invoke(FunctionName=self.invoker_function_name, InvocationType='Event', Payload=msg_serialized)
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
                    #print("\n[ {} ] BatchedLambdaInvoker - INFO: Invoked {} Lambda invokers in {} seconds.".format(timestamp_now, len(scheduler_payload), send_done_time - send_start_time))
                    #print("                               Serialization took {} seconds. Invocation took {} seconds.".format(total_time_spent_serializing, total_time_spent_invoking))
                    #print("                               Total Lambdas Invoked: {}. Total # Tasks Invoked: {}".format(self.total_lambdas_invoked, self.num_tasks_invoked))
                    #print("                               The Scheduler has spent {} seconds calling invoke() so far.".format(self.time_spent_invoking))                                      
                else:
                    timestamp_now = datetime.datetime.utcnow()
                    print("[ {} ] BatchedLambdaInvoker - invoking {} tasks directly/normally.".format(timestamp_now, len(scheduler_payload)))
                    send_start_time = time.time()
                    # Send each chunk to an invocation of the AWS Lambda function for evaluation.
                    total_time_spent_serializing = 0
                    total_time_spent_invoking = 0
                    for payload in scheduler_payload:
                        # Synchronize time for benchmarking cold starts (and start-times for Lambdas in general).
                        #remote = self.ntp_client.request("north-america.pool.ntp.org")
                        #invoke_time = remote.dest_time + remote.offset        
                        #_payload = {"event": payload, "invoke-time": invoke_time}            
                        time_invoke_start = time.time()
                        self.lambda_client.invoke(FunctionName=self.executor_function_name, InvocationType='Event', Payload=payload)
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

    def invoker_polling_process(self, ID, conn, scheduler_address, redis_channel_names, aws_region, redis_address):
        """ This function runs as a separate process, invoking Lambda functions in parallel.
            
            It continually polls the given Connection object, checking for new payloads. If
            there are no messages in the Pipe, then the process sleeps. The sleep time increases 
            for consecutive empty Pipes."""
        print("[ {} ] - Lambda Invoker Process {} - INFO: Lambda Invoker Process began executing...".format(datetime.datetime.utcnow(), ID))
        lambda_client = boto3.client('lambda', region_name=aws_region)
        current_redis_channel_index = 0
        expected_messages = None                      # This is how many messages we are expecting to get.
        base_sleep_interval = 0.005                   # The starting sleep interval.
        sleep_interval_increment = 0.005              # We increment the sleep interval by this much for consecutive sleeps.
        max_sleep_interval = 0.025                    # We do not sleep longer than this (we do not want to leave messages in the pipe for too long).
        current_sleep_interval = base_sleep_interval  # This is the amount by which we will sleep during the next sleep.
        current_payload = None                        # This is the current payload we are processing and submitting to AWS Lambda.
        redis_channel_index = 0                       # Used to pick the Redis channel to assign to a given Lambda function during invocation.
        _redis_client = redis.Redis(host = redis_address, port = 6379, db = 0)
        while True:
            data_available = conn.poll()
            if data_available:
                # Grab the message from the connection.
                _msg = conn.recv()
                sent_time = _msg["sent-time"]
                use_invoker_lambdas = _msg["use-invoker-lambdas"]
                current_payload = _msg["payload"]
                received_time = time.time()
                #print("[ {} ] Lambda Invoker Process {} - INFO: Received message from connection. Took {} seconds to transfer through conn.".format(datetime.datetime.utcnow(), ID, received_time - sent_time))
                num_lambdas_submitted = 0
                start = time.time()
                if use_invoker_lambdas:
                    payloads_for_lamba_invokers = [current_payload[x : x + 50] for x in range(0, len(current_payload), 50)]
                    #print("[ {} ] Invoking {} INVOKER Lambdas.".format(datetime.datetime.utcnow(), len(payloads_for_lamba_invokers)))
                    rand_pick = string.ascii_uppercase + string.digits + string.ascii_lowercase
                    for payload in payloads_for_lamba_invokers:
                        msg = {
                            "payloads_serialized": payload,
                            "lambda_function_name": self.executor_function_name
                        }
                        msg_serialized = ujson.dumps(msg)
                        if (sys.getsizeof(msg_serialized) > 256000):
                            _key = ''.join(random.choice(rand_pick) for _ in range(20))
                            _redis_client.set(_key, msg_serialized)
                            new_msg_serialized = ujson.dumps({"lambda_function_name": self.executor_function_name, "redis_key": _key, "redis_address": self.redis_address})
                            lambda_client.invoke(FunctionName=self.invoker_function_name, InvocationType='Event', Payload=new_msg_serialized)
                        else:
                            lambda_client.invoke(FunctionName=self.invoker_function_name, InvocationType='Event', Payload=msg_serialized)
                        num_lambdas_submitted += 1
                    stop = time.time()                
                    time_to_submit = stop - start 
                    #print("[ {} ] Lambda Invoker Process {} - INFO: submitted {} INVOKER Lambdas in {} seconds.".format(datetime.datetime.utcnow(), ID, num_lambdas_submitted, time_to_submit))
                    # Since there was a message in the pipe, we set the sleep interval back to the base amount.
                    current_sleep_interval = base_sleep_interval                
                else:
                    for payload in current_payload:
                        # Synchronize time for benchmarking cold starts (and start-times for Lambdas in general).
                        #remote = self.ntp_client.request("north-america.pool.ntp.org")
                        #invoke_time = remote.dest_time + remote.offset
                        #_payload = {"event": payload, "invoke-time": invoke_time}
                        lambda_client.invoke(FunctionName=self.executor_function_name, InvocationType='Event', Payload=payload)
                        num_lambdas_submitted += 1
                    stop = time.time()                
                    time_to_submit = stop - start 
                    print("[ {} ] Lambda Invoker Process {} - INFO: submitted {} EXECUTOR Lambdas in {} seconds.".format(datetime.datetime.utcnow(), ID, num_lambdas_submitted, time_to_submit))
                    # Since there was a message in the pipe, we set the sleep interval back to the base amount.
                    current_sleep_interval = base_sleep_interval
            else:
                time.sleep(current_sleep_interval)
                current_sleep_interval += sleep_interval_increment
                # Constrain the sleep interval to the specified max.
                if current_sleep_interval > max_sleep_interval:
                    current_sleep_interval = base_sleep_interval      