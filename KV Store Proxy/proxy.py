import sys
import traceback
sys.path.append('/usr/local/lib/python3.7/site-packages')
sys.path.append('/usr/local/lib64/python3.7/site-packages')

from pathing import Path, PathNode 
import aioredis
import asyncio
import redis # For the polling processes 

import hashlib
import argparse 

import math

import errno
import socket
import struct

from collections import defaultdict
import pickle
import cloudpickle 
#import json
import ujson
import multiprocessing
import base64
import boto3 
import time
import datetime 

import multiprocessing
from multiprocessing import Pipe, Process
import queue 
from uhashring import HashRing 

from serialization import Serialized, dumps, from_frames
from network import CommClosedError, get_stream_address, TCP
from proxy_lambda_invoker import ProxyLambdaInvoker 

from tornado.ioloop import IOLoop
from tornado.ioloop import PeriodicCallback
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
from tornado.netutil import bind_sockets
import tornado 

define('proxy_port', default=8989, help="TCP port to use for the proxy itself")
define('encoding', default='utf-8', help="String encoding")
define('redis_port1', default=6379, help="Port for the Redis cluster")
define('redis_port2', default=6380, help="Port for the Redis cluster")

TASK_KEY = "task_key"

# Used when mapping PathNode --> Fargate Task with a dictionary. These are the keys.
FARGATE_ARN_KEY = "taskARN"
FARGATE_ENI_ID_KEY = "eniID"
FARGATE_PUBLIC_IP_KEY = "publicIP"
FARGATE_PRIVATE_IP_KEY = "privateIpv4Address"

class RedisProxy(object):
    """Tornado asycnrhonous TCP server co-located with a Redis cluster."""

    def __init__(self, lambda_client, print_debug = False, redis_host = None):
        self.lambda_client = lambda_client
        self.print_debug = print_debug
        self.completed_tasks = set()

        self.redis_host = redis_host
        self.scheduler_address = ""                 # The address of the modified Dask Distributed scheduler 
        self.serialized_paths = {}                  # List of serialized paths retrieved from Redis 
        self.path_nodes = {}                        # Mapping of task keys to path nodes 
        self.serialized_path_nodes = {}             # List of serialized path nodes.
        self.handlers = {
                "set": self.handle_set,                 # Store a value in Redis.
                "graph-init": self.handle_graph_init,   # DAG from the Scheduler.
                "start": self.handle_start,             # 'START' operation from the Scheduler.
                "redis-io": self.handle_IO              # Generic IO operation from a Lambda worker.
            }
        self.hostnames_to_redis = dict()
        self.io_operations = queue.Queue()          # FIFO queue for I/O operations received from AWS Lambda functions.

    def start(self):
        self.server = TCPServer()
        self.server.handle_stream = self.handle_stream
        self.server.listen(options.proxy_port)
        self.port = options.proxy_port
        self.need_to_process = []
        #print("Redis proxy listening at {}:{}".format(self.redis_endpoint, self.port))
        print("Redis proxy listening on port {}".format(self.port))
        
        self.loop = IOLoop.current()

        # On the next iteration of the IOLoop, we will attempt to connect to the Redis servers.
        self.loop.add_callback(self.connect_to_redis_servers)

        # Start the IOLoop! 
        self.loop.start()

        # Begin asynchronously processing IO operations for Lambdas.
        self.process_IO = PeriodicCallback(self.process_IO_operation, callback_time = 100)
        self.process_IO.start()

    @gen.coroutine
    def connect_to_redis_servers(self):
        print("[ {} ] Connecting to Redis server...".format(datetime.datetime.utcnow()))
        self.redis_client = yield aioredis.create_redis(address = (self.redis_host, 6379))
        print("[ {} ] Connected to Redis successfully!".format(datetime.datetime.utcnow()))

    @gen.coroutine
    def process_task(self, task_key, _value_encoded = None, message = None):   
        """
            Args:
                task_key (str): The key of the task we're processing.

                _value_encoded (str): The base64 string encoding of the serialized value of the task we're processing.

                message (dict): The message that was originally sent to the proxy.
        """   
        if self.print_debug:
            print("[ {} ] Processing task {} now...".format(datetime.datetime.utcnow(), task_key))
        
        # Decode the value but keep it serialized.              
        value_encoded = _value_encoded or message["value-encoded"]
        value_serialized = base64.b64decode(value_encoded)
        task_node = self.path_nodes[task_key]  
        #fargate_ip = message[FARGATE_PUBLIC_IP_KEY]  
        fargate_ip = message[FARGATE_PRIVATE_IP_KEY]
        fargate_node = task_node.fargate_node
        #fargate_ip = task_node.getFargatePublicIP()
        task_payload = task_node.task_payload

        redis_client = None 
        if fargate_ip in self.hostnames_to_redis:
            redis_client = self.hostnames_to_redis[fargate_ip]
        else:
            redis_client = yield aioredis.create_redis(address = (fargate_ip, 6379))

            # Cache new Redis instance.
            self.hostnames_to_redis[fargate_ip] = redis_client
        
        yield redis_client.set(task_key, value_serialized)

        # Store the result in redis.
        #if sys.getsizeof(value_serialized) > task_payload["storage_threshold"]:
        #    redis_instance = self.big_hash_ring.get_node_instance(task_key) #[task_key].set(task_key, value_serialized)
        #    print("[ {} ] Storing task {} in Big Redis instance listening at {}".format(datetime.datetime.utcnow(), task_key, redis_instance.address))
        #    yield redis_instance.set(task_key, value_serialized)
        #else:
        #    redis_instance = self.small_hash_ring.get_node_instance(task_key) #[task_key].set(task_key, value_serialized)
        #    print("[ {} ] Storing task {} in Small Redis instance listening at {}".format(datetime.datetime.utcnow(), task_key, redis_instance.address))
        #    yield redis_instance.set(task_key, value_serialized)

        self.completed_tasks.add(task_key)

        num_dependencies_of_dependents = task_payload["num-dependencies-of-dependents"]
        can_now_execute = []

        print("[ {} ] Value for {} successfully stored in Redis. Checking dependencies/invoke nodes now...".format(datetime.datetime.utcnow(), task_key))

        if self.print_debug:
            print("[ {} ] [PROCESSING] Now processing the downstream tasks for task {}".format(datetime.datetime.utcnow(), task_key))

        for invoke_key in task_node.invoke:
            futures = []
            invoke_node = self.path_nodes[invoke_key]

            # Grab the key of the "invoke" node and check how many dependencies it has.
            # Next, check Redis to see how many dependencies are available. If all dependencies
            # are available, then we append this node to the can_now_execute list. Otherwise, we skip it.
            # The invoke node will be executed eventually once its final dependency resolves.
            dependency_counter_key = invoke_key + "---dep-counter"
            num_dependencies = num_dependencies_of_dependents[invoke_key]

            # We create a pipeline for incrementing the counter and getting its value (atomically). 
            # We do this to reduce the number of round trips required for this step of the process.
            #redis_pipeline = self.get_redis_client(invoke_key).pipeline()
            redis_pipeline = self.redis_client.pipeline()

            futures.append(redis_pipeline.incr(dependency_counter_key))  # Enqueue the atomic increment operation.
            futures.append(redis_pipeline.get(dependency_counter_key))   # Enqueue the atomic get operation AFTER the increment operation.

            # Execute the pipeline; save the responses in a local variable.
            result = yield redis_pipeline.execute()
            responses = yield asyncio.gather(*futures)

            # Decode and cast the result of the 'get' operation.
            dependencies_completed = int(responses[1])                    

            # Check if the task is ready for execution. If it is, put it in the "can_now_execute" list. 
            # If the task is not yet ready, then we don't do anything further with it at this point.
            if dependencies_completed == num_dependencies:
                if self.print_debug:
                    print("[DEP. CHECKING] - task {} is now ready to execute as all {} dependencies have been computed.".format(invoke_key, num_dependencies))
                can_now_execute.append(invoke_node)
            else:
                if self.print_debug:
                    print("[DEP. CHECKING] - task {} cannot execute yet. Only {} out of {} dependencies have been computed.".format(invoke_key, dependencies_completed, num_dependencies))
                    print("\nMissing dependencies: ")
                    deps = invoke_node.task_payload["dependencies"]
                    for dep_task_key in deps:
                        if dep_task_key not in self.completed_tasks:
                            print("     ", dep_task_key)
                            print("\n")
            
        # Invoke all of the ready-to-execute tasks in parallel.
        # print("[ {} ] Invoking {} of the {} downstream tasks of current task {}:".format(datetime.datetime.utcnow(), len(can_now_execute), len(task_node.invoke), task_key))
        # for node in can_now_execute:
        #     print("     ", node.task_key)
        # print("\n")
        for invoke_node in can_now_execute:
            # We're going to check and see if we can send the previous task's data along with the path. 
            # If not, then the Lambda function will just have to retrieve the data from Redis instead.
            payload = self.serialized_paths[invoke_node.task_key]
            payload_size = sys.getsizeof(payload)
            relevant_data_size = sys.getsizeof(value_encoded)
            combined_size = payload_size + relevant_data_size
            # We can only send a payload of size 256,000 bytes or less to a Lambda function directly.
            # If the payload is too large, then the Lambda will retrieve the data from Redis.
            if combined_size < 256000:
                #print("Payload (before any deserialization or anything): ", payload)

                # Deserialize the payload so we can modify it. 
                payload = ujson.loads(payload)

                # Add the data for 'this' task to the payload. The Lambda function expects the value to be 
                # in a dictionary stored at key "previous-results". The value should be encoded.
                payload["previous-results"] = {task_key: value_encoded}
                payload["invoked-by"] = task_key
                # Re-serialize the payload.
                payload = ujson.dumps(payload)
            # If the path + data was too big, see if we can get away with just sending the path. If not, then the Lambda can get that from Redis too.
            elif sys.getsizeof(payload) > 256000:
                payload = ujson.dumps({"path-key": invoke_node.task_key + "---path", "invoked-by": task_key})
            
            if self.print_debug:
                print("[INVOKE] Invoking Task Executor for task {}.".format(invoke_node.task_key))
            self.lambda_invoker.send(payload)            

    @gen.coroutine
    def deserialize_and_process_message(self, stream, address = None, **kwargs):
        message = None 

        # This uses the dask protocol to break up the message/receive the chunks of the message.
        try:
            n_frames = yield stream.read_bytes(8)
            n_frames = struct.unpack("Q", n_frames)[0]
            lengths = yield stream.read_bytes(8 * n_frames)
            lengths = struct.unpack("Q" * n_frames, lengths)
            
            frames = []
            for length in lengths:
                if length:
                    frame = bytearray(length)
                    n = yield stream.read_into(frame)
                    assert n == length, (n, length)
                else:
                    frame = b""
                frames.append(frame)
        except StreamClosedError as e:
            print("[ {} ] [ERROR] - Stream Closed Error: stream from address {} is closed...".format(datetime.datetime.utcnow(), address))
            print("Real Error: ", e.real_error.__str__())
            raise
        except AssertionError as e:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred on line {} in statement {}. Currently processing a stream from addresss {}.'.format(line, text, address))
            raise 
        else:
            try:
                message = yield from_frames(frames)
            except EOFError:
                print("[ {} ] [ERROR] - Aborted stream on truncated data".format(datetime.datetime.utcnow()))
                raise CommClosedError("aborted stream on truncated data")
        
        # The 'op' (operation) entry specifies what the Proxy should do. 
        op = message["op"]

        print("[ DEBUG ] \n\tMessage: {}".format(message))

        yield self.handlers[op](message, address = address, stream = stream)

    @gen.coroutine
    def handle_IO(self, message, **kwargs):
        print("handle_io kwargs: {}".format(kwargs))
        stream = kwargs['stream']
        address = kwargs['address']
        local_address = "tcp://" + get_stream_address(stream)
        lambda_comm = TCP(stream, local_address, "tcp://" + address[0], deserialize = True)
        self.io_operations.put([message, lambda_comm])

    @gen.coroutine
    def process_IO_operation(self):
        if (self.io_operations.qsize() != 0):
            arr = self.io_operations.get()
            message = arr[0] 
            stream = arr[1]

            fargate_node = message['fargate-node']
            #fargate_ip = fargate_node[FARGATE_PUBLIC_IP_KEY]
            fargate_ip = fargate_node[FARGATE_PRIVATE_IP_KEY]
            fargate_arn = fargate_node[FARGATE_ARN_KEY]
            task_key = message[TASK_KEY]
            redis_operation = message['redis-op']
            print("[ {} ] Handling Redis IO {} for task {}, Fargate Node {} listening at {}:6379".format(datetime.datetime.utcnow(), redis_operation, task_key, fargate_arn, fargate_ip))

            if redis_operation == "set":
                # Grab the associated task node.
                if task_key not in self.path_nodes:
                    # This can happen if the Lambda function executes before the proxy finishes processing the DAG info sent by the Scheduler. 
                    # In these situations, we add the messages to a list that gets processed once the DAG-processing concludes.
                    # print("[ {} ] [WARNING] {} is not currently contained within self.path_nodes... Will try to process again later...".format(datetime.datetime.utcnow(), task_key))
                    self.need_to_process.append([message])
                else:
                    # print("[ {} ] The task {} is contained within self.path_nodes. Processing now...".format(datetime.datetime.utcnow(), task_key))
                    yield self.process_task(task_key, _value_encoded = None, message = message)                
                

    @gen.coroutine
    def handle_set(self, message, **kwargs):
        # The "set" operation is sent by Lambda functions so that the proxy can store results in Redis and  
        # possibly invoke downstream tasks, particularly when there is a large fan-out factor for a given node/task.
 
        task_key = message[TASK_KEY]
        print("[ {} ] [OPERATION - set] Received 'set' operation from a Lambda. Task Key: {}.".format(datetime.datetime.utcnow(), task_key))

        # Grab the associated task node.
        if task_key not in self.path_nodes:
            # This can happen if the Lambda function executes before the proxy finishes processing the DAG info sent by the Scheduler. 
            # In these situations, we add the messages to a list that gets processed once the DAG-processing concludes.
            # print("[ {} ] [WARNING] {} is not currently contained within self.path_nodes... Will try to process again later...".format(datetime.datetime.utcnow(), task_key))
            self.need_to_process.append([message])
            return
        else:
            # print("[ {} ] The task {} is contained within self.path_nodes. Processing now...".format(datetime.datetime.utcnow(), task_key))
            yield self.process_task(task_key, message = message, _value_encoded = None)

    @gen.coroutine
    def handle_graph_init(self, message, **kwargs):
        # Grab the paths from Redis directly.
        path_keys = message["path-keys"]

        if len(path_keys) == 0:
            print("[WARNING] Received graph-init operation from Scheduler, but the list of path keys was empty...")
        else:
            response = yield self.redis_client.mget(*path_keys)

            # Iterate over all of the serialized paths and deserialize them/deserialize the nodes.
            for serialized_path in response:
                path = ujson.loads(serialized_path)
                starting_node_key = path["starting-node-key"]

                # Map the task key corresponding to the beginning of the path to the path itself.
                self.serialized_paths[starting_node_key] = serialized_path # May want to check if path is already in the list?

                # Iterate over all of the encoded-and-serialized nodes and deserialize them. 
                for task_key, encoded_node in path["nodes-map"].items():
                    decoded_node = base64.b64decode(encoded_node)
                    deserialized_node = cloudpickle.loads(decoded_node)
                        
                    # The 'frames' stuff is related to the Dask protocol. We use Dask's deserialization algorithm here.
                    frames = []
                    for encoded in deserialized_node.task_payload:
                        frames.append(base64.b64decode(encoded))
                    deserialized_task_payload = yield deserialize_payload(frames)
                    deserialized_node.task_payload = deserialized_task_payload
                    self.path_nodes[task_key] = deserialized_node
            for lst in self.need_to_process:
                msg = lst[0]
                task_key = msg[TASK_KEY]
                value_encoded = msg["value-encoded"]
                yield self.process_task(task_key, _value_encoded = value_encoded, message = msg) # Could also pass *lst.
            # Clear the list.
            self.need_to_process = [] 

    @gen.coroutine
    def handle_start(self, message, **kwargs):
        stream = kwargs["stream"]
        address = kwargs["address"]
        # This operation needs to happen before anything else. Scheduler should be started *after* the proxy is started in order for this to work.
        print("[ {} ] [OPERATION - start] Received 'start' operation from Scheduler.".format(datetime.datetime.utcnow()))
        self.redis_channel_names = message["redis-channel-names"]
        self.scheduler_address = message["scheduler-address"]

        # We need the number of cores available as this determines how many processes total we can have.
        num_cores = multiprocessing.cpu_count()            
        cores_remaining = num_cores - len(self.redis_channel_names)
            
        # Start a certain number of Redis polling processes to listen for results.
        num_redis_processes_to_create = math.ceil(cores_remaining * 0.5)
        print("Creating {} 'Redis Polling' processes.".format(num_redis_processes_to_create))

        self.redis_channel_names_for_proxy = []
        self.base_channel_name = "redis-proxy-"
        for i in range(num_redis_processes_to_create):
            name = self.base_channel_name + str(i)
            self.redis_channel_names_for_proxy.append(name)

        # Create a list to keep track of the processes as well as the Queue object, which we use for communication between the processes.
        self.redis_polling_processes = []
        self.redis_polling_queue = multiprocessing.Queue()
            
        # For each channel, we create a process and store a reference to it in our list.
        for channel_name in self.redis_channel_names_for_proxy:
            redis_polling_process = Process(target = self.poll_redis_process, args = (self.redis_polling_queue, channel_name, self.redis_host))
            redis_polling_process.daemon = True 
            self.redis_polling_processes.append(redis_polling_process)
                
        # Start the processes.
        for redis_polling_process in self.redis_polling_processes:
            redis_polling_process.start()  
                
        self.lambda_invoker = ProxyLambdaInvoker(interval = "2ms", chunk_size = 1, redis_channel_names = self.redis_channel_names, redis_channel_names_for_proxy = self.redis_channel_names_for_proxy, loop = self.loop)
        self.lambda_invoker.start(self.lambda_client, scheduler_address = self.scheduler_address)

        # The Scheduler stores its address 
        #self.scheduler_address = yield self.small_redis_clients[0].get("scheduler-address")
        #self.scheduler_address = yield self.redis_client.get("scheduler-address").decode()

        print("[START Operation] Retrieved Scheduler's address from Redis: {}".format(self.scheduler_address))

        payload = {"op": "redis-proxy-channels", "num_channels": len(self.redis_channel_names_for_proxy), "base_name": self.base_channel_name}
        print("[ {} ] Payload for Scheduler: {}.".format(datetime.datetime.utcnow(), payload))
        local_address = "tcp://" + get_stream_address(stream)
        self.scheduler_comm = TCP(stream, local_address, "tcp://" + address[0], deserialize = True)
        print("[ {} ] Writing message to Scheduler...".format(datetime.datetime.utcnow()))
        bytes_written = yield self.scheduler_comm.write(payload)
        print("[ {} ] Wrote {} bytes to Scheduler...".format(datetime.datetime.utcnow(), bytes_written))
        self.loop.spawn_callback(self.consume_redis_queue)
        print("Now for handle comm")
        #payload2 = {"op": "debug-msg", "message": "[ {} ] Goodbye, world!".format(datetime.datetime.utcnow())}
        #yield self.scheduler_comm.write(payload2)
        #self.loop.spawn_callback(self.hello_world)
        yield self.handle_comm(self.scheduler_comm)
        
    @gen.coroutine
    def hello_world(self):
        print("Hello World starting...")
        counter = 0
        while True:
            _now = datetime.datetime.utcnow()
            payload2 = {"op": "debug-msg", "message": "[ {} ] Hello, world {}!".format(_now, str(counter))}
            yield self.scheduler_comm.write(payload2)
            counter = counter + 1
            yield gen.sleep(2)
                
    @gen.coroutine 
    def handle_stream(self, stream, address):
        print("[ {} ] Starting established connection with {}".format(datetime.datetime.utcnow(), address))

        io_error = None
        closed = False        

        try:
            while not closed:
                if self.print_debug:
                    print("[ {} ] Message received from address {}. Handling now...".format(datetime.datetime.utcnow(), address))
                yield self.deserialize_and_process_message(stream, address = address)
        except (CommClosedError, EnvironmentError) as e:
            io_error = e
            closed = True 
        except StreamClosedError as e:
            print("[ERROR] Stream closed")
            print("Real Error: ", e.real_error.__str__())
            closed = True 
        except AssertionError as e:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred in file {} on line {} in statement "{}". Currently processing a stream from addresss {}.'.format(filename, line, text, address))
            raise
        except Exception as e:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred in file {} on line {} in statement "{}". Currently processing a stream from addresss {}.'.format(filename, line, text, address))
            raise 
        finally:
            stream.close()
            assert stream.closed()
    
    @gen.coroutine
    def handle_comm(self, comm, extra=None, every_cycle=[]):
        extra = extra or {}
        print("[ {} ] Starting established TCP Comm connection with {}".format(datetime.datetime.utcnow(), comm._peer_addr))

        io_error = None
        closed = False
        while True:
            try:
                msg = yield comm.read()

                if not isinstance(msg, dict):
                    raise TypeError(
                        "Bad message type.  Expected dict, got\n  " + str(msg) + " of type " + str(type(msg))
                    )
                try:
                    op = msg["op"]
                except KeyError:
                    raise ValueError(
                        "Received unexpected message without 'op' key: " % str(msg)
                    )
                yield self.handlers[op](msg)
                close_desired = msg.get("close", False)
                msg = result = None
                if close_desired:
                    print("[ {} ] Close desired. Closing comm.".format(datetime.datetime.utcnow()))
                    yield comm.close()
                if comm.closed():
                    break
            except (CommClosedError, EnvironmentError) as e:
                io_error = e
                print("[ {} ] [ERROR] CommClosedError, EnvironmentError: {}".format(datetime.datetime.utcnow(), e.__str__()))
                raise 
                break
            except Exception as e:
                print("[ {} ] [ERROR] Exception: {}".format(datetime.datetime.utcnow(), e.__str__()))
                raise 
                break

    def poll_redis_process(self, _queue, redis_channel_name, redis_endpoint):
        ''' This function defines a process which continually polls Redis for messages. 
        
            When a message is found, it is passed to the main Scheduler process via the queue given to this process. ''' 
        print("Redis Polling Process started. Polling channel ", redis_channel_name)

        redis_client = redis.StrictRedis(host=redis_endpoint, port = 6379, db = 0)

        print("[ {} ] Redis polling processes connected to Redis Client at {}:{}".format(datetime.datetime.utcnow(), redis_endpoint, 6379))

        base_sleep_interval = 0.05 
        max_sleep_interval = 0.15 
        current_sleep_interval = base_sleep_interval
        consecutive_misses = 0
        
        # We just do pub-sub on the first redis client.
        redis_channel = redis_client.pubsub(ignore_subscribe_messages = True)
        redis_channel.subscribe(redis_channel_name)
        
        # This process will just loop endlessly polling Redis for messages. When it finds a message,
        # it will decode it (from bytes to UTF-8) and send it to the Scheduler process via the queue. 
        #
        # If no messages are found, then the thread will sleep before continuing to poll. 
        while True:
            message = redis_channel.get_message()
            if message is not None:
                timestamp_now = datetime.datetime.utcnow()
                # print("[ {} ] Received message from channel {}.".format(timestamp_now, redis_channel_name))   
                data = message["data"]
                # The message should be a "bytes"-like object so we decode it.
                # If we neglect to turn off the subscribe/unsubscribe confirmation messages,
                # then we may get messages that are just numbers. 
                # We ignore these by catching the exception and simply passing.
                data = data.decode()
                data = ujson.loads(data)
                # print("Data: ", data)
                _queue.put([data])
                consecutive_misses = 0
                current_sleep_interval = base_sleep_interval
            else:
                time.sleep(current_sleep_interval)
                consecutive_misses = consecutive_misses + 1
                current_sleep_interval += 0.05 
                if (current_sleep_interval > max_sleep_interval):
                    current_sleep_interval = max_sleep_interval

    @gen.coroutine
    def consume_redis_queue(self):
        ''' This function executes periodically (as a PeriodicCallback on the IO loop). 
        It reads messages from the message queue until none are available.'''
        # print("Consume Redis Queue is being executed...")
        while True:
            messages = []
            # 'end' is the time at which we should stop iterating. By default, it is 50ms.
            stop_at = datetime.datetime.utcnow().microsecond + 5000
            while datetime.datetime.utcnow().microsecond < stop_at and len(messages) < 50:
                try:
                    timestamp_now = datetime.datetime.utcnow()
                    # Attempt to get a payload from the Queue. A 'payload' consists of a message
                    # and possibly some benchmarking data. The message will be at index 0 of the payload.
                    payload = self.redis_polling_queue.get(block = False, timeout = None)
                    message = payload[0]
                    messages.append(message)
                # In the case that the queue is empty, break out of the loop and process what we already have.
                except queue.Empty:
                    break
            if len(messages) > 0:
                # print("[ {} ] Processing {} messages from Redis Message Queue.".format(datetime.datetime.utcnow(), len(messages)))
                for message in messages:
                    if "op" in message:
                        op = message["op"]
                        if op == "set":
                            task_key = message[TASK_KEY]
                            value_encoded = message["value-encoded"]
                            print("[ {} ] [OPERATION - set] Received 'set' operation from a Lambda. Task Key: {}.".format(datetime.datetime.utcnow(), task_key))

                            # Grab the associated task node.
                            if task_key not in self.path_nodes:
                                # This can happen if the Lambda function executes before the proxy finishes processing the DAG info sent by the Scheduler. 
                                # In these situations, we add the messages to a list that gets processed once the DAG-processing concludes.
                                # print("[ {} ] [WARNING] {} is not currently contained within self.path_nodes... Will try to process again later...".format(datetime.datetime.utcnow(), task_key))
                                self.need_to_process.append([message])
                                continue 
                            else:
                                # print("[ {} ] The task {} is contained within self.path_nodes. Processing now...".format(datetime.datetime.utcnow(), task_key))
                                yield self.process_task(task_key, _value_encoded = value_encoded, message = message)
                        else:
                            print("[ {} ] [ERROR] Unknown Operation from Redis Queue... Message: {}".format(datetime.datetime.utcnow(), message))
                    else:
                        print("[ {} ] [ERROR] Message from Redis Queue did NOT contain an operation... Message: {}".format(datetime.datetime.utcnow(), message))
            # Sleep for 5 milliseconds...
            yield gen.sleep(0.005)
@gen.coroutine
def deserialize_payload(payload):
   msg = yield from_frames(payload)
   raise gen.Return(msg)

if __name__ == "__main__":
    # Set up command-line arguments.
    parser = argparse.ArgumentParser(description='Process some values.')
    parser.add_argument("-pd", "--print-debug", dest="print_debug", nargs=1, type=bool, default = False)
    parser.add_argument("-reg", "--region", dest="aws_region", nargs=1, default = ["us-east-1"])
    parser.add_argument("-res", "--redis", dest="redis_hostname", nargs = 1, type = str)
    args = vars(parser.parse_args())

    print_debug = args["print_debug"]
    aws_region = args["aws_region"][0]
    redis_host = args["redis_hostname"][0]

    print("aws_region: ", aws_region)
    lambda_client = boto3.client('lambda', region_name=aws_region)

    # Start the proxy.
    proxy = RedisProxy(lambda_client, print_debug = print_debug, redis_host = redis_host)
    proxy.start()