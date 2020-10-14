
from collections import deque

from tornado import gen, locks
from tornado.ioloop import IOLoop

from multiprocessing import Process, Pipe

from random import randint
import boto3
import ntplib
import datetime
import json
import ujson
import time 
from numbers import Number 
import datetime
from datetime import timedelta
from math import ceil

timedelta_sizes = {
    "s": 1,
    "ms": 1e-3,
    "us": 1e-6,
    "ns": 1e-9,
    "m": 60,
    "h": 3600,
    "d": 3600 * 24,
}

tds2 = {
    "second": 1,
    "minute": 60,
    "hour": 60 * 60,
    "day": 60 * 60 * 24,
    "millisecond": 1e-3,
    "microsecond": 1e-6,
    "nanosecond": 1e-9,
}
tds2.update({k + "s": v for k, v in tds2.items()})
timedelta_sizes.update(tds2)
timedelta_sizes.update({k.upper(): v for k, v in timedelta_sizes.items()})

class ProxyLambdaInvoker(object):
    """ Batch Lambda invocations 

    This takes an IOStream and an interval (in ms) and ensures that we invoke tasks grouped together every interval milliseconds.

    Batching several tasks at once helps performance when sending
    a myriad of tiny tasks.
    
    """
    
    def __init__(self, interval = "5ms", use_multiple_invokers = True, function_name="WukongExecutor", num_invokers = 8, redis_channel_names = None, debug_print = False, 
            chunk_size = 5, loop=None, serializers=None, minimum_tasks_for_multiple_invokers = 8, redis_channel_names_for_proxy = None):
        # XXX is the loop arg useful?
        self.loop = loop or IOLoop.current()
        self.interval = parse_timedelta(interval, default="ms")
        self.waker = locks.Event()
        self.stopped = locks.Event()
        self.please_stop = False
        self.redis_channel_names = redis_channel_names
        self.redis_channel_names_for_proxy = redis_channel_names_for_proxy
        self.current_redis_channel_index = 0
        self.current_redis_channel_index_for_proxy = 0
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
        self.serializers = serializers
        #self.ntp_client = ntplib.NTPClient()
        
    def start(self, lambda_client, scheduler_address):
        print("Starting BatchedLambdaInvoker with interval {}...".format(self.interval))
        self.lambda_client = lambda_client
        self.loop.add_callback(self._background_send)
        self.scheduler_address = scheduler_address
        
        print("[ {} ] BatchedLambdaInvoker - INFO: Launching {} ''Lambda Invoker'' processes.".format(datetime.datetime.utcnow(), self.num_invokers))
        for i in range(0, self.num_invokers):
            #(self, conn, chunk_size, scheduler_address, redis_channel_names)
            receiving_conn, sending_conn = Pipe()
            
            invoker = Process(target = self.invoker_polling_process, args = (i, receiving_conn, self.scheduler_address, self.redis_channel_names, self.redis_channel_names_for_proxy))
            invoker.daemon = True 
            self.lambda_pipes.append(sending_conn)
            self.lambda_invokers.append(invoker)
        
        for invoker in self.lambda_invokers:
            invoker.start()
        

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
            payloads = [payload[x : x + payload_chunk_size] for x in range(0, len(payload), payload_chunk_size)]
            
            # The scheduler gets the first payload. This is important because,
            # in the case where there is only one payload, we want the Scheduler 
            # to invoke the Lambda function itself.
            scheduler_payload = payloads[0] # Last element 
            
            # Create a separate index to walk through the invoker list.
            invoker_index = 0
            
            # Send each invoker its respective payload.
            for i in range(1, len(payloads)):
                sent_time = time.time()
                msg = {"payload": payloads[i], "sent-time": sent_time}
                conn = self.lambda_pipes[invoker_index]
                conn.send(ujson.dumps(msg)) #TODO Can't we just send this without the json.dumps()?
                invoker_index += 1
            try:
                # send_start_time = time.time()

                # Send each chunk to an invocation of the AWS Lambda function for evaluation.
                for payload in scheduler_payload:
                    self.lambda_client.invoke(FunctionName=self.lambda_function_name, InvocationType='Event', Payload = payload)
            except Exception:
                #logger.exception("Error in batched write")
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

    def invoker_polling_process(self, ID, conn, scheduler_address, redis_channel_names, redis_channel_names_for_proxy):
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
                msg  = ujson.loads(_msg)
                sent_time = msg["sent-time"]
                current_payload = msg["payload"]
                received_time = time.time()
                # print("[ {} ] Lambda Invoker Process {} - INFO: Received message from connection. Took {} seconds to transfer through conn.".format(datetime.datetime.utcnow(), ID, received_time - sent_time))
                num_lambdas_submitted = 0
                start = time.time()
                for payload in current_payload:
                    #remote = self.ntp_client.request("north-america.pool.ntp.org")
                    #invoke_time = remote.dest_time + remote.offset        
                    #_payload = {"event": payload, "invoke-time": invoke_time}                    
                    lambda_client.invoke(FunctionName=self.lambda_function_name, InvocationType='Event', Payload = payload)
                    num_lambdas_submitted += 1
                stop = time.time()                
                time_to_submit = stop - start 
                # print("[ {} ] Lambda Invoker Process {} - INFO: submitted {} Lambdas in {} seconds.".format(datetime.datetime.utcnow(), ID, num_lambdas_submitted, time_to_submit))
                # Since there was a message in the pipe, we set the sleep interval back to the base amount.
                current_sleep_interval = base_sleep_interval
            else:
                time.sleep(current_sleep_interval)
                current_sleep_interval += sleep_interval_increment
                # Constrain the sleep interval to the specified max.
                if current_sleep_interval > max_sleep_interval:
                    current_sleep_interval = base_sleep_interval 

def parse_timedelta(s, default="seconds"):
    """ Parse timedelta string to number of seconds

    Examples
    --------
    >>> parse_timedelta('3s')
    3
    >>> parse_timedelta('3.5 seconds')
    3.5
    >>> parse_timedelta('300ms')
    0.3
    >>> parse_timedelta(timedelta(seconds=3))  # also supports timedeltas
    3
    """
    if s is None:
        return None
    if isinstance(s, timedelta):
        return s.total_seconds()
    if isinstance(s, Number):
        s = str(s)
    s = s.replace(" ", "")
    if not s[0].isdigit():
        s = "1" + s

    for i in range(len(s) - 1, -1, -1):
        if not s[i].isalpha():
            break
    index = i + 1

    prefix = s[:index]
    suffix = s[index:] or default

    n = float(prefix)

    multiplier = timedelta_sizes[suffix.lower()]

    result = n * multiplier
    if int(result) == result:
        result = int(result)
    return result