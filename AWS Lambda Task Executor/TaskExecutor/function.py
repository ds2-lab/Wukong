#import json
import ujson
import cloudpickle
from datetime import timedelta
import datetime
import base64 
import redis
import time
import dask
import random
import dask.array
import dask.dataframe
import dask.bag
import pandas
#import dask_ml 
import sklearn 
import math
import hashlib
from dask.core import istask
import uuid
import sys
import socket
import pickle 
import queue 
import logging
import os
from zipfile import ZipFile
import tornado
from collections import defaultdict
from tornado import gen, netutil
from tornado.iostream import StreamClosedError, IOStream
from tornado.tcpclient import TCPClient
from tornado.gen import Return
from tornado.ioloop import IOLoop
import boto3
#from multiprocessing import Process, Pipe
from uhashring import HashRing 

from wukong_metrics import TaskExecutionBreakdown, LambdaExecutionBreakdown, WukongEvent
from utils import funcname, key_split
from exception import error_message
from serialization import Serialized, _extract_serialize, extract_serialize, to_frames, dumps, from_frames
from network import CommClosedError, FatalCommClosedError, parse_address, parse_host_port, connect_to_address, get_stream_address, unparse_host_port, TCP, connect_to_proxy

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.async_context import AsyncContext
xray_recorder.configure(service='my_service', sampling=True, context_missing='LOG_ERROR') #context=AsyncContext()
# This is the code of the AWS Lambda function. 
# Note that it requires the cloudpickle library.
# See the following link for documentation on
# how to deploy the function with cloudpickle included:
# https://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html#python-package-dependencies

no_value = "--no-value-sentinel--"
collection_types = (tuple, list, set, frozenset)

# These are automatically passed by scheduler/other Lambdas.
# The hostname for the KV Store Proxy.
proxy_address = None
use_fargate = True

# These will be passed to us in invocation payloads; we're just using default values as placeholders here.
executor_function_name = "WukongExecutor"
invoker_function_name = "WukongInvoker"

# Connects to the Redis instance on which we store dependency counters and paths. (DCP = Dependency Counter and Path.)
dcp_redis = None #redis.StrictRedis(host = proxy_address, port = 6379, db = 0, socket_connect_timeout  = 20, socket_timeout = 20)

pubsub = None #dcp_redis.pubsub()

# Leaf Task Lambdas will subscribe to a Redis Pub/Sub channel prefixed by this. The suffix will be the corresponding leaf task key.
leaf_task_channel_prefix = "__keyspace@0__:"

EC2_REDIS_METRIC_KEY = "EC2-Redis"

# Maintains a list of tasks executed locally on this Lambda function.
executed_tasks = dict()

# Key used in dictionary sent to Lambdas (the dictionary contains information from Path objects).
TASK_TO_FARGATE_MAPPING = "tasks-to-fargate-mapping"
NODES_MAP = "nodes-map"

# Map between IP addresses and Redis clients (each of which would be connected to the Redis instance at the respective IP address).
hostnames_to_clients = dict()

# These are used as keys in dictionaries passed to and between Lambdas (i.e., Scheduler --> Lambda, Lambda --> Lambda, Lambda --> Proxy, Proxy --> Lambda, etc.)
starting_node_payload_key = "starts-at"
path_key_payload_key = "path-key"
previous_results_payload_key = "previous-results"
invoked_by_payload_key = "invoked-by"

# Keys used for the 'op' field in messages sent to the Wukong Scheduler.
EXECUTED_TASK_KEY = "executed-task"
RETRIEVED_FROM_PREVIOUS_RESULTS_KEY = "retrieved-from-previous-result"
TASK_RETRIEVED_FROM_REDIS_KEY = "task-retrieved-from-redis"
TASK_ERRED_KEY = "task-erred"
EXECUTING_TASK_KEY = "executing-task"
LAMBDA_RESULT_KEY = "lambda-result"
START_TIME_KEY = "start-time"
STOP_TIME_KEY = "stop-time"
EXECUTION_TIME_KEY = "execution-time"
LAMBDA_ID_KEY = "lambda_id"
TASK_KEY = "task_key"
OP_KEY = "op"
DATA_SIZE = "data-size"
VALUE_SERIALIZED_KEY = 'value-serialized'

# This string is appended to the end of task keys to get the Redis key for the associated task's dependency counter. 
DEPENDENCY_COUNTER_SUFFIX = "---dep-counter"

# This string is appended to the end of task keys (that are located at the start of a Path/static schedule) to get the Redis key for the associated Path object. 
PATH_KEY_SUFFIX = "---path"

ITERATION_COUNTER_SUFFIX = "---iteration"

# Appended to the end of a task key to store fargate node metadata in Redis.
FARGATE_DATA_SUFFIX = "---fargate"

# Redis pub-sub channel used to transfer messages to the Scheduler.
REDIS_PUB_SUB_CHANNEL = "dask-workers-1"

# Used to tell Task Executors whether or not to use the Task Queue (large objects wait for tasks to become ready instead of writing data).
EXECUTOR_TASK_QUEUE_KEY = "executors-use-task-queue"

# Used when mapping PathNode --> Fargate Task with a dictionary. These are the keys.
FARGATE_ARN_KEY = "taskARN"
FARGATE_ENI_ID_KEY = "eniID"

# TODO: Figure out when each of these is used (sometimes private works, sometimes public works).
FARGATE_PUBLIC_IP_KEY = "publicIP"
FARGATE_PRIVATE_IP_KEY = "privateIpv4Address"

# Used when processing downstream tasks of big tasks that were not ready to execute when the associated big task was executed.
EXECUTED_TASKS_KEY = "executed-tasks"
STILL_NOT_READY_KEY = "still-not-ready"

lambda_client = boto3.client('lambda')
ecs_client = boto3.client('ecs')

S3 = boto3.resource('s3')

# Downstream tasks from some large task executed locally. They were not ready when originally checked, but
# we would like to avoid writing the data if at all possible. 
task_queue = queue.Queue()

# See https://stackoverflow.com/questions/37703609/using-python-logging-with-aws-lambda for explanation of the following code.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

if sys.version_info[0] == 2:
   unicode = unicode 
if sys.version_info[0] == 3:
   unicode = str           

def install_dependencies():
   """The AWS Lambda deployment package is limited to roughly 256MB. 

      In order to get around this limit, we've uploaded some pre-installed dependencies to an S3 bucket.
      We can retrieve these dependencies and install them at runtime to circumvent the deployment package size limit.

      Presently, the dependencies we're installing from S3 include:
         (1) llvmlite (for dask-ml) 
      
      This function is based on code included in this article: https://medium.com/pipedrive-engineering/ml-code-vs-aws-lambda-limits-6deb78a7e911?
   """
   if os.path.isdir('/tmp/deps'):
        return
   
   logger.debug("Downloading \"numba-llvmlite.zip\" from S3 bucket \"wukong-dependencies\".")
   S3.Bucket("wukong-dependencies").download_file("numba-llvmlite.zip", '/tmp/deps.zip')
   logger.debug("Successfully downloaded \"numba-llvmlite\" from S3 bucket \"wukong-dependencies\".")

   with ZipFile('/tmp/deps.zip') as zipo:
      logger.debug("Unzipping file...")
      zipo.extractall('/tmp/deps')

   sys.path.append('/tmp/deps')

#install_dependencies()
#import llvmlite
#import numba
#import dask_ml

def lambda_handler(event, context):
   """
   This is the entry point into the Wukong Task Executor. This function begins by installing additional
   dependencies that are required from S3. These dependencies are used for dask_ml. If you know you
   aren't using going to use dask_ml, then you can comment out the call to install_dependencies().
   """
   global invoker_function_name
   global executor_function_name
   global proxy_address
   global dcp_redis
   handler_start_time = time.time()

   install_deps_from_S3_start = time.time()
   install_dependencies()
   install_deps_from_S3_stop = time.time()
   install_deps_from_S3_duration = install_deps_from_S3_stop - install_deps_from_S3_start

   install_deps_event = WukongEvent(
      name = "Download Dependency From S3",
      start_time = install_deps_from_S3_start,
      end_time = install_deps_from_S3_stop,
      metadata = {
         "Duration (seconds)": install_deps_from_S3_duration
      }
   )

   """ This is the function directly executed by AWS Lambda. """
   # Start time for the entire function.
   lambda_function_start_time = time.time()
   
   # A wrapper around a bunch of diagnostic metrics.
   task_execution_breakdowns = dict() # TaskExecutionBreakdown()
   lambda_execution_breakdown = LambdaExecutionBreakdown(aws_request_id = context.aws_request_id,
                                                         start_time = lambda_function_start_time)
   lambda_execution_breakdown.install_deps_from_S3 = install_deps_from_S3_duration

   lambda_execution_breakdown.add_event(install_deps_event)
   
   invoked_event = WukongEvent(
      name = "Lambda Started Running",
      start_time = handler_start_time - 0.25, # Start a quarter second before.
      end_time = handler_start_time,
      metadata = {
         "Explanation": "The 'end time' of this event is timestamp collected right after the Lambda was invoked.",
         "Explanation continued": "The 'start time' is a quarter second prior to this just so the event is visible in Gantt charts."
      }
   )
   lambda_execution_breakdown.add_event(invoked_event)

   invoker_function_name = event["invoker_function_name"]
   executor_function_name = event["executor_function_name"]
   proxy_address = event["proxy_address"]

   logger.debug("Received Lambda function names and proxy address from invocation payload.")
   logger.debug("Executor function name: \"{}\"".format(executor_function_name))
   logger.debug("Invoker function name: \"{}\"".format(invoker_function_name))
   logger.debug("Proxy address: {}".format(proxy_address))

   # Now that we have the proxy address, connect to Redis (co-located with the proxy).
   dcp_redis = redis.StrictRedis(host = proxy_address, port = 6379, db = 0, socket_connect_timeout  = 20, socket_timeout = 20)

   # Begin executing tasks.
   res = task_executor(event, context, previous_results = dict(), task_execution_breakdowns = task_execution_breakdowns, lambda_execution_breakdown = lambda_execution_breakdown)

   result = res["result"]
   is_leaf = res["is-leaf"]
   channel = res["channel"]
   leaf_key = res["leaf-key"]
   counter_value = res["counter-value"]

   # Grab whatever this is using for its previous results. We'll pass it back to task_executor if this Lambda is reused.
   previous_results = res["previous-results"] 

   reused = False # Flag indicating whether or not we were re-used this iteration

   # We count this as the end, unless we're a leaf Lambda and we get reused. In that case, we update the
   # value of 'total_duration' for the lambda_execution_breakdown to capture the time spent during reuse.
   lambda_execution_breakdown.total_duration = time.time() - lambda_function_start_time
   
   # This big block of code was just an attempt to add support for re-using existing Task Executors
   # during an interative workload (i.e., a workload which consisted of multiple jobs being fired off
   # in a for-loop from the central Scheduler). Each job would reuse data, and so it would make sense
   # to just reuse the Lambdas which already retrieved the data from intermediate storage instead of 
   # having to retrieve it for each new phase of the workload.
   #
   # This is a work-in-progress so it's quite ugly.

   # If this Lambda was originally assigned a leaf task and re-use was enabled on the Scheduler side, then we'll poll for messages for a bit.
   if is_leaf:
      num_tries = 1
      max_tries = 10
      sleep_base = 0.05
      sleep_cap = 0.1
      start = time.time()
      try_interval = 60 # how long we listen for messages before giving up 
      finish = start + try_interval
      last_message_time = start # the time at which we last got a message
      num_loops = 0
      notify_long_wait_base = 5
      notify_long_wait = notify_long_wait_base # If it has been 10 seconds since we've gotten a message, print a warning.
      notify_inc = 5       # How much longer it has to be before next msg (we increment notify_long_wait by this amount)
      
      # Safety catch in case I get us stuck in an infinite loop, so we don't loop forever.
      # Note that if a legitimate workload goes for this many iterations, the Lambda will wrongfully terminate.
      max_loops = 50

      if leaf_key in previous_results:
         # Remove all existing entries except for the leaf task's entry.
         leaf_result = previous_results[leaf_key]
         previous_results = {
            leaf_key: leaf_result
         }
      else:
         # If the leaf key is not in the dict, then just clear it.
         previous_results = dict()

      logger.debug("This Lambda was originally task {}.".format(leaf_key))
      
      #while num_tries <= max_tries and num_loops < max_loops:
      while time.time() < finish:
         new_val = counter_value
         new_val_encoded = dcp_redis.get(leaf_key + ITERATION_COUNTER_SUFFIX)
         logger.debug("Leaf Task Lambda associated with task {} retrieved value {} for iteration counter encoded.".format(leaf_key, new_val_encoded))
         if new_val_encoded is not None:
            new_val = int(new_val_encoded.decode())

         # -1 is a 'stop' operation.
         if new_val == -1:
            logger.debug("Lambda associated with leaf task {} received 'stop' operation from Scheduler. Writing metric data and exiting.".format(leaf_key))
            dcp_redis.set(new_val_encoded, 0)
            # The Scheduler is attempting to kill all Lambdas associated with this workload. Let's exit the loop, write our metric/debug data, and exit.
            break             
         elif new_val > counter_value:
            logger.debug("[LEAF] Iteration counter for task {} has been incremented! Old value: {}. New value: {}.".format(leaf_key, counter_value, new_val))
            
            # Update our counter_value.
            counter_value = new_val 

            # Reset the number of tries.
            num_tries = 1 
            notify_long_wait = notify_long_wait_base
            args = {
               path_key_payload_key: leaf_key + PATH_KEY_SUFFIX # The 'task_executor' function will look for the path key in this dictionary.
            }    

            # We're just subscribed to the path channel so we shouldn't need to unsubscribe... we want whatever messages are performed on the path channel.
            res = task_executor(args, 
                           context, 
                           previous_results = previous_results, 
                           task_execution_breakdowns = task_execution_breakdowns, 
                           lambda_execution_breakdown = lambda_execution_breakdown)
            
            # Grab whatever this is using for its previous results. We'll pass it back to task_executor if this Lambda is reused.
            previous_results = res["previous-results"]

            if leaf_key in previous_results:
               # Remove all existing entries except for the leaf task's entry.
               leaf_result = previous_results[leaf_key]
               previous_results = {
                  leaf_key: leaf_result
               }
            else:
               # If the leaf key is not in the dict, then just clear it.
               previous_results = dict()            

            # Extend the loop since we successfully were reused.
            finish = time.time() + try_interval
            logger.debug("[INFO] Updated message-loop stop time is {}. Current time: {}".format(finish, time.time()))
            reused = True 

            returned_counter_value = res["counter-value"]

            if counter_value != returned_counter_value:
               logger.debug("[ERROR] Leaf counter for task {} returned a different iteration counter value after reuse. Old value: {}. New value: {}.".format(leaf_key, counter_value, new_val))

            # Increment this AFTER we are done executing, since otherwise all the time we spent executing will be counted, which is pointless.
            last_message_time = time.time() 

            # Consider this the end of the Lambda if we never get reused again since, 
            # as far as the workload is concerned, the Lambda stopped here. It never did anything else related to the workload if we're not reused again.
            lambda_execution_breakdown.total_duration = time.time() - lambda_function_start_time
            lambda_execution_breakdown.reuse_count += 1            
         elif new_val < counter_value:
            logger.debug("[ERROR] Iteration counter for task {} got SMALLER. Old value: {}. New value: {}.".format(leaf_key, counter_value, new_val))
            # If we've been waiting for a while, print something about how long we've been waiting.
            time_since_message = time.time() - last_message_time
            if time_since_message > notify_long_wait:
               logger.debug("[WARNING] Leaf Lambda for task {} has been waiting {} seconds for a message...".format(leaf_key, time_since_message))
               notify_long_wait += notify_inc # Notify again in another 'notify_inc' seconds.
            
            # Exponential backoff.
            sleep_interval = ((2 ** num_tries) * sleep_base) + (random.randint(0, 1000) / 1000)
            sleep_amount = min(sleep_cap, sleep_interval) 
            #sleep_amount = sleep_base
            logger.debug("Sleeping for {} seconds. (try #{})".format(sleep_amount, num_tries))
            time.sleep(sleep_amount)
            logger.debug("Done sleeping.")
            num_tries += 1   
         else:
            # If we've been waiting for a while, print something about how long we've been waiting.
            time_since_message = time.time() - last_message_time
            if time_since_message > notify_long_wait:
               logger.debug("[WARNING] Leaf Lambda for task {} has been waiting {} seconds for a message...".format(leaf_key, time_since_message))
               notify_long_wait += notify_inc # Notify again in another 'notify_inc' seconds.
            
            # Exponential backoff.
            sleep_interval = ((2 ** num_tries) * sleep_base) + (random.randint(0, 1000) / 1000)
            sleep_amount = min(sleep_cap, sleep_interval) 
            #sleep_amount = sleep_base
            logger.debug("No message received yet. Sleeping for {} seconds. (try #{})".format(sleep_amount, num_tries))
            time.sleep(sleep_amount)
            logger.debug("Done sleeping.")
            num_tries += 1

   if channel is not None:
      # Explicitly unsubscribe so it's clear we aren't looking for messages anymore.
      pubsub.unsubscribe(channel)

   if len(task_execution_breakdowns) > 0:
      dcp_redis.lpush("task_breakdowns", *[cloudpickle.dumps(breakdown) for breakdown in list(task_execution_breakdowns.values())])
   dcp_redis.lpush("lambda_durations", cloudpickle.dumps(lambda_execution_breakdown))

   if len(lambda_execution_breakdown.fan_outs) > 0:
      dcp_redis.lpush("fan-out-data", *[cloudpickle.dumps(fanout_data) for fanout_data in lambda_execution_breakdown.fan_outs])
   
   if len(lambda_execution_breakdown.fan_ins) > 0:
      dcp_redis.lpush("fan-in-data", *[cloudpickle.dumps(fanin_data) for fanin_data in lambda_execution_breakdown.fan_ins])

   # Store some diagnostics that we can see from the AWS X-Ray console.
   subsegment = xray_recorder.begin_subsegment("diagnostics")
   subsegment.put_annotation("tasks_executed", str(lambda_execution_breakdown.number_of_tasks_executed))
   subsegment.put_annotation("tasks_pulled_down", str(lambda_execution_breakdown.tasks_pulled_down))
   subsegment.put_annotation("total_redis_read_time", str(lambda_execution_breakdown.redis_read_time))
   subsegment.put_annotation("total_redis_write_time", str(lambda_execution_breakdown.redis_write_time))
   xray_recorder.end_subsegment()

   logger.debug("\n\n\nThis Lambda executed {} tasks in total.".format(lambda_execution_breakdown.number_of_tasks_executed))

   return {
      "statusCode": 200,
      "body": "Hello, World!"
   } 

@xray_recorder.capture("task_executor")
def task_executor(event, context, previous_results = dict(), task_execution_breakdowns = None, lambda_execution_breakdown = None):
   """
      The main execution loop for AWS Task Executors.

      Parameters
      ----------
      event : (dict)
         Passed to Lambda function as AWS payload. If this Lambda is re-used, then the 'event' may be generated by code on the Lambda and passed in a loop.
      
      context : Context
         AWS Context object.

      task_execution_breakdowns : [TaskExecutionBreakdown]
         List of TaskExecutionBreakdown objects created during this function's execution.
      
      lambda_execution_breakdown : LambdaExecutionBreakdown
         LambdaExecutionBreakdown which encapsulates all of the metric information for this Task Executor.
   """
   global use_fargate
   payload = None 
   channel = None
   leaf_key = None
   counter_value = -1

   use_fargate = event.get("use-fargate", True)

   # Check if we were sent the path directly (i.e., it is contained within the Lambda 'event' parameter). 
   # If not, then grab it from Redis.
   if path_key_payload_key in event:
      path_key = event[path_key_payload_key]
      logger.debug("[INIT] Obtained key {} in Lambda package. Payload must be obtained from Redis.".format(path_key))
      
      # Retrieve the Path from Redis.
      path_serialized = get_path_from_redis(path_key = path_key, task_execution_breakdown = None, lambda_execution_breakdown = lambda_execution_breakdown)

      deserialization_start = time.time()
      
      # Deserialize the Path.
      payload = ujson.loads(path_serialized.decode())
      
      # Record de-serialization time.
      deserialization_stop = time.time()
      lambda_execution_breakdown.deserialization_time += (deserialization_stop - deserialization_start)
   elif "special-op" in event:
      _start = time.time()
      operation = event[OP_KEY]
      metrics = dict()

      # NOP for testing.
      if operation == "NOP":
         layer = str(event["layer"])
         layer_payloads = event["layer-payloads"]
         invoked_at = event["invoked-at"]

         metrics["difference"] = _start - invoked_at

         payload = layer_payloads[layer]

         do_when_done = payload["DO-WHEN-DONE"] # One of {"INVOKE", "NOTHING"}.
         sleep = payload.get("SLEEP", 0)
         num_execute = payload.get("num-execute", 0)
         logger.debug("Lambda in Layer #{} - Do When Done: {}".format(layer, do_when_done))

         t = time.time()
         # Execute the no-op operations.
         for i in range(0, num_execute):
            logger.debug("Executing no-op #{}".format(i))
            no_op(sleep = sleep) # Execute NOP operation.
         e = time.time()

         metrics["execute"] = e - t

         if do_when_done == "INVOKE":
            t = time.time()

            stop = dcp_redis.get("stop")
            
            # Fail safe.
            if stop:
               dcp_redis.incr("lambda-count")
               return {
                  "result": layer,
                  "is-leaf": False,
                  "channel": None,
                  "leaf-key": None, 
                  "counter-value": -1,
                  "previous-results": dict()
               }
            
            num_invoke = payload["NUM-INVOKE"]
            next_layer = int(layer) - 1

            if next_layer <= 1:
               do_when_done = "INCREMENT"
            else:
               do_when_done = "INVOKE"

            payload = {
               "special-op": True,
               OP_KEY: "NOP",
               "layer": next_layer,
               #"DO-WHEN-DONE": do_when_done,
               #"NUM-INVOKE": num_invoke,
               #"SLEEP": sleep,
               "layer-payloads": layer_payloads,
               "invoked-at": time.time()
            }         

            logger.debug("Invoking {} new Lambdas...".format(num_invoke))
            
            payload_serialized = ujson.dumps(payload)

            # Invoke the next layer of Lambdas.
            for i in range(0, num_invoke):
               lambda_client.invoke(FunctionName=executor_function_name, InvocationType='Event', Payload=payload_serialized)   
            
            e = time.time()

            metrics["invoke"] = e - t
         #else:
         #   raise ValueError("Unknown special operation do when done value: {}".format(do_when_done))   
         
         t = time.time()
         dcp_redis.incr("lambda-count")

         if num_execute > 0:
            val = dcp_redis.incrby("NOP", amount = num_execute) # Increment the NOP counter to indicate that we're done.
         
         if num_execute == 0:
            val = dcp_redis.get("NOP")
         logger.debug("Value of NOP after increments: " + str(val))
         e = time.time()

         metrics["update"] = e - t

         try:
            rc = redis.Redis(host = proxy_address, port = 6380, db = 0)
            rc.lpush("scale-metrics", ujson.dumps(metrics))
         except Exception:
            pass

         return {
            "result": layer,
            "is-leaf": False,
            "channel": None,
            "leaf-key": None, 
            "counter-value": -1,
            "previous-results": dict()
         }                
   else: 
       payload = event 
   
   # Grab some information from the payload.
   nodes_map_serialized = payload[NODES_MAP]                   # Dictionary of TASK_KEY -> Serialized PathNode
   task_to_fargate_mapping = payload[TASK_TO_FARGATE_MAPPING]  # Mapping of TASK_KEY -> AWS Fargate Endpoint
   
   # Task Executors can place tasks in a queue to be executed later when they discover
   # that the task is still waiting on some dependencies to finish up. For example, let's
   # say we have some downstream task T2, and we executed one of T2's dependencies locally.
   # Let's call the locally-executed dependency T1. If T1 is "large" to the point that we would
   # prefer to avoid transferring the data over the network, we can put T2 in a queue and wait
   # for all of its other dependencies to finish. Then, we can execute T2 locally, thereby avoiding
   # the need to send T1's data over the network (which would be quite slow if T1 is large).
   use_task_queue = payload[EXECUTOR_TASK_QUEUE_KEY]           
   
   # Controls the printing and collection of certain debug data.
   lambda_debug = payload["lambda-debug"]                      

   # There are two ways we can track if task dependencies have resolved. One of them is 
   # built around mapping tasks to certain bits in an integer and checking if those bits
   # are toggled or not. The other uses a counting mechanism. This checks which one we use.
   use_bit_dep_checking = payload["use-bit-counters"]
   
   # If some other task invoked us, we may use a leaf-node path. That doesn't make this Lambda a leaf Lambda though.
   is_leaf = payload["is-leaf"] and invoked_by_payload_key not in event
   starting_node_key = str(payload["starting-node-key"])

   # If it's a leaf, subscribe to the channel in case we'll have future tasks. Note that this value will only be true
   # if the first task is a leaf task AND the Scheduler's 'reuse_lambdas' field is set to True.
   if is_leaf:
      leaf_key = starting_node_key

      # If this is a leaf task (and the Scheduler may try to re-use Lambdas),
      # then subscribe to the channel so we can receive messages when the key is updated.
      channel = starting_node_key + PATH_KEY_SUFFIX
      logger.debug("This Lambda is a leaf task (task {}). Subscribing to Redis Pub/Sub channel {}".format(leaf_key, channel))
      
      # If we're already subscribed, then this shouldn't have any ill-effects given Redis just uses a HashTable to map subscriptions...
      #pubsub.subscribe(channel) 

      counter_value = int(dcp_redis.get(leaf_key + ITERATION_COUNTER_SUFFIX).decode())

   logger.debug("-+-+-+-+-+- Lambda Debug: {} -+-+- Use Bitwise Dependency Checking: {} -+-+- Use Task Queue: {} -+-+- Is Leaf: {} -+-+- Starting Node Key: {} -+-+-+-+-+-".format(lambda_debug, use_bit_dep_checking, use_task_queue, is_leaf, starting_node_key))

   # Check if the top-level payload (the one sent directly to the Lambda function) has a 'starts-at' key. If so,
   # then we should start there; some other function has decided that the node we should start on is 'starts-at'. 
   # The path may not start there, but the node will be contained within the path. Presumably everything before it
   # has already been executed (perhaps it was pulled down during a big task collapse).
   starting_node_key = payload["starting-node-key"]
   if starting_node_payload_key in event:
      logger.debug("The key {} was included in the payload to this Lambda function with the value {}".format(starting_node_payload_key, event[starting_node_payload_key]))
      starting_node_key = event[starting_node_payload_key]
   
   # If we were invoked by another Lambda, then we print this to the console for debugging purposes. 
   # It is often helpful to know who invoked who when you're tracing through a workload in the debug logs.
   if invoked_by_payload_key in event:
      invoked_by_task = event[invoked_by_payload_key]
      logger.debug("This Lambda (which is first executing task {}) was invoked by task {}!".format(starting_node_key, invoked_by_task))
   else:
      logger.debug("This Lambda (which is first executing task {}) was invoked by the Scheduler!".format(starting_node_key))
   
   logger.debug("The initial Nodes Map (Serialized) contains all of the following task nodes: {}".format(str(list(nodes_map_serialized.keys()))))

   # Check if we have some previous results data in the Lambda payload. If so, we'll use that.
   if previous_results_payload_key in event:
      previous_results = event[previous_results_payload_key]
      deserialization_start = time.time()
      for key, value_encoded in previous_results.items():
         logger.debug("Decoding and deserializing directly-sent data {}".format(key))
         value_serialized = base64.b64decode(value_encoded)
         value = cloudpickle.loads(value_serialized)
         previous_results[key] = value
      
      # Record deserialization time.
      deserialization_stop = time.time()
      lambda_execution_breakdown.deserialization_time += (deserialization_stop - deserialization_start)
   
   # We're gonna pass this value to 'process_path' as the keyword argument for 'leaf_key'. 
   # We only want it to be non-null if this Lambda began as a leaf Lambda. The reason for this
   # is because we will be preserving the leaf data in the previous_results dict so it can be reused.
   # But we only want to do this if we might reuse it, otherwise the data will just be wasting space.
   pass_as_leaf_key_to_process_path = None 
   if is_leaf:
      pass_as_leaf_key_to_process_path = leaf_key 

   process_path_start = time.time()

   # Process the "static schedule" assigned to us.
   result = process_path(nodes_map_serialized, 
                         starting_node_key, 
                         task_to_fargate_mapping,
                         lambda_debug = lambda_debug,
                         use_bit_dep_checking = use_bit_dep_checking,
                         previous_results = previous_results, 
                         lambda_execution_breakdown = lambda_execution_breakdown, 
                         task_execution_breakdowns = task_execution_breakdowns,
                         leaf_key = pass_as_leaf_key_to_process_path,
                         context = context,
                         use_task_queue = use_task_queue)
   
   # Record the total time spent processing the current path. This is almost equivalent to the entire duration.
   lambda_execution_breakdown.process_path_time += time.time() - process_path_start

   return {
      "result": result, 
      "is-leaf": is_leaf,
      "channel": channel,
      "leaf-key": leaf_key,
      "counter-value": counter_value,
      "previous-results": previous_results
   }                         

def scalability_tests():


   # Just return a bunch of nothing values.
   return {
      "result": None, 
      "is-leaf": False,
      "channel": None,
      "leaf-key": None,
      "counter-value": None,
      "previous-results": dict()
   }

@xray_recorder.capture("publish_dcp_message")
def publish_dcp_message(channel, payload, serialized = True, max_tries = 8, base_sleep = 0.1, max_sleep = 5):
   payload_serialized = payload 

   if not serialized:
      payload_serialized = ujson.dumps(payload)
   
   num_tries = 1
   
   success = False 

   while num_tries <= max_tries:
      try:
         dcp_redis.publish(channel, payload_serialized)
         logger.debug("Successfully published Redis pub-sub message.")
         success = True
         break
      except (ConnectionError, Exception) as ex:
         logger.debug("[ERROR] {} when attempting to publish a message to channel {} on EC2-Redis.".format(type(ex), channel))
         sleep_interval = ((2 ** num_tries) * base_sleep) + (random.randint(0, 1000) / 1000)
         if sleep_interval > max_sleep:
            sleep_interval = max_sleep + (random.randint(0, 500) / 1000)
         logger.debug("\tWill sleep for {} seconds before trying again... (try {} of {})".format(sleep_interval, num_tries, max_tries))
         num_tries += 1
         time.sleep(sleep_interval)
   
   if not success:
      raise Exception("Failed to publish message on channel {} on EC2-Redis after {} attempts.".format(channel, num_tries))

@xray_recorder.capture("get_path_from_redis")
def get_path_from_redis(path_key = None, task_execution_breakdown = None, lambda_execution_breakdown = None):
   """ Retrieve a Path object from Redis. This is a separate method because it does not use a path_node object
       and it always uses the Redis instance to which dcp_redis is connected.

       Args:
         path_key (String): The Redis key for the desired Path object.

         task_execution_breakdown (TaskExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with the currently-processing task.
         
         lambda_execution_breakdown (LambdaExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with this Lambda invocation.         

       Returns:
         Path: the Path object stored at the given key in the Redis instance to which dcp_redis is connected.
   """

   logger.debug("Obtaining path from Redis for path_key {}".format(path_key))

   read_start = time.time()

   # Retrieve the Path object from Redis.
   path_serialized = dcp_redis.get(path_key)
   
   # Compute read time and size for metric collection/debugging.
   read_stop = time.time()
   redis_read_time = read_stop - read_start
   read_size = sys.getsizeof(path_serialized)

   # Accumulate metric information.
   lambda_execution_breakdown.add_read_time(EC2_REDIS_METRIC_KEY, path_key, read_size, redis_read_time, read_start, read_stop)
   lambda_execution_breakdown.bytes_read += read_size 

   # Depending on where in the process we are, there may be no existing task_execution_breakdown, in which case it will be None.
   if task_execution_breakdown is not None:
      task_execution_breakdown.redis_read_time += redis_read_time
   
   lambda_execution_breakdown.redis_read_time += redis_read_time

   get_path_event = WukongEvent(
      name = "Read Path from Redis",
      start_time = read_start,
      end_time = read_stop,
      metadata = {
         "Duration": read_stop - read_start,
         "Read Size (bytes)": read_size,
         "Path Key": path_key
      }
   )
   lambda_execution_breakdown.add_event(get_path_event)

   return path_serialized

@xray_recorder.capture("get_data_from_redis")
def get_data_from_redis(task_to_fargate_mapping, 
                        path_node = None, 
                        key = None, 
                        exception_on_failure = True, 
                        just_check_exists = False, 
                        current_scheduler_id = -1,
                        current_update_graph_id = -1,
                        task_execution_breakdown = None, 
                        lambda_execution_breakdown = None):
   """ Retrieve data for the PathNode from its associated Fargate-based Redis instance.
   
      Args:
         path_node (PathNode): The node whose data we will retrieve. We will use the node's
                               associated Fargate data to locate the Redis instance on which
                               the data is stored.

         key (String or None): If 'None', then we will just use the 'task_key' of the PathNode
                               as our key for Redis. If given, then we will still use the Redis
                               instance associated with the PathNode, but we'll use the parameterized key.
                               Note that if we are using 'key', then that 'key' MUST be a task contained
                               in the current path, or else we'll be able to retrieve the Redis information
                               for that key.         

         task_to_fargate_mapping (dict): Mapping between task keys and fargate dicts. This defines which tasks are stored in which Fargate nodes.

         exception_on_failure (bool): If True, then this will raise exceptions on failure. If False, then it will just print a warning message and return None.

         just_check_exists (bool): If True, then this function simply checks if a value exists without retrieving and returning the value itself.

         task_execution_breakdown (TaskExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with the currently-processing task.
         
         lambda_execution_breakdown (LambdaExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with this Lambda invocation.

      Returns:
         object: The data stored on the associated Redis instance.
   """
   global use_fargate
   redis_key = None 
   fargate_ip = None 
   fargate_arn = "dcp_redis" # Default value of "dcp_redis"

   if use_fargate:
      # If the PathNode object is non-null, then we'll use the data contained on that object.
      if path_node is not None:
         redis_key = path_node.task_key
         fargate_ip = path_node.fargate_node[FARGATE_PUBLIC_IP_KEY] 
         #fargate_ip = path_node.fargate_node[FARGATE_PRIVATE_IP_KEY]
         fargate_arn = path_node.fargate_node[FARGATE_ARN_KEY]
      else:
         if key is None:
            raise ValueError("Both path_node and key are None! One of path_node or key should be non-None in order for 'get_data_from_redis' to work...")
         redis_key = key

         if key not in task_to_fargate_mapping:
            logger.debug("[WARNING] The key '{}' is not contained within task_to_fargate_mapping. The tasks contained in the mapping are:\n\t{}\nRetrieving Fargate info from Redis now...".format(redis_key, list(task_to_fargate_mapping.keys())))
            read_start = time.time()
            fargate_dict_serialized = dcp_redis.get(key + FARGATE_DATA_SUFFIX)
            read_stop = time.time()
            fargate_dict = ujson.loads(fargate_dict_serialized)
            deser_stop = time.time()

            read_dur = read_stop - read_start 
            deserialization_dur = deser_stop - read_stop 

            read_size = sys.getsizeof(fargate_dict_serialized)

            lambda_execution_breakdown.deserialization_time += deserialization_dur
            lambda_execution_breakdown.redis_read_time += read_dur
            lambda_execution_breakdown.bytes_read += read_size

            task_execution_breakdown.deserialization_time += deserialization_dur
            task_execution_breakdown.redis_read_time += read_dur
            
            fargate_ip = fargate_dict[FARGATE_PUBLIC_IP_KEY]    
            #fargate_ip = fargate_dict[FARGATE_PRIVATE_IP_KEY]    
            fargate_arn = fargate_dict[FARGATE_ARN_KEY]

            redis_read_event = WukongEvent(
               name = "Get Fargate Info from Redis",
               start_time = read_start,
               end_time = read_stop,
               metadata = {
                  "Duration": read_stop - read_start,
                  "Read Size (bytes)": read_size,
                  "Key": key + FARGATE_DATA_SUFFIX
               }
            )
         else:
            # If all is well, then get the Fargate IP from the Fargate Node dictionary associated with this key.
            fargate_dict = task_to_fargate_mapping[key]
            fargate_ip = fargate_dict[FARGATE_PUBLIC_IP_KEY]   
            #fargate_ip = fargate_dict[FARGATE_PRIVATE_IP_KEY]   
            fargate_arn = fargate_dict[FARGATE_ARN_KEY]      
   else:
      if path_node is not None:
         redis_key = path_node.task_key 
      else:
         redis_key = key 
   
   redis_client = None
   if use_fargate:
      logger.debug("Obtaining data for key {} [sid-{}] from Redis instance listening at {}:6379".format(redis_key, current_scheduler_id, fargate_ip))

      # Check if we have a connection to this Fargate node cached. If so, then we'll use the existing connection.
      # If not, then we'll create the new connection, making sure to cache it for future use.
      if fargate_ip in hostnames_to_clients:
         redis_client = hostnames_to_clients[fargate_ip]

      else:
         redis_client = redis.StrictRedis(host = fargate_ip, port = 6379, db = 0, socket_connect_timeout  = 20, socket_timeout = 20)

         # Cache the connection.
         hostnames_to_clients[fargate_ip] = redis_client      
   else:
      logger.debug("Obtaining data for key {} [sid-{}] from DCP Redis.".format(redis_key, current_scheduler_id))

      redis_client = dcp_redis
   
   read_start = time.time() 

   # If just_check_exists is True, then we're just supposed to check if a value exists without returning it.
   if just_check_exists:
      try: 
         exists = redis_client.get(redis_key)
         
         # Accumulate metric information.
         read_stop = time.time()
         redis_read_duration = read_stop - read_start 
         task_execution_breakdown.redis_read_time += redis_read_duration
         lambda_execution_breakdown.redis_read_time += redis_read_duration  

         read_size = sys.getsizeof(exists)
         lambda_execution_breakdown.bytes_read += read_size 
         if exists > 0:
            return True 
         else:
            exists = dcp_redis.get(redis_key)

            read_size = sys.getsizeof(exists)
            lambda_execution_breakdown.bytes_read += read_size 

            if exists > 0:
               return True
            return False 
      except Exception as ex:
         return False

   num_tries = 1
   max_tries = 8
   sleep_base = 0.25
   sleep_max = 24
   
   quit_on_none = False 

   val = None

   # Exponential backoff...
   while (num_tries < max_tries):
      try:
         logger.debug("\tAttempting to read value for {} [sid-{}] now... (try {}/{})".format(redis_key, current_scheduler_id, num_tries, max_tries))
         read_start = time.time() # Re-initialize the start time here in case we've looped.
         # Retrieve and return the data.
         val = redis_client.get(redis_key) 

         # If the value is None, then we're going to try ONE more time to read the value from Redis. 
         if val is None:    
            if quit_on_none is True:
               break 
            else:
               if use_fargate:
                  logger.debug("[ERROR] Retrieved 'None' after reading value at key {} [sid-{}] from Redis at {}:6379 ({}). Will try one more time before checking EC2-Redis...".format(redis_key, current_scheduler_id, fargate_ip, fargate_arn))
               else: 
                  logger.debug("[ERROR] Retrieved 'None' after reading value at key {} [sid-{}] from DCP Redis.".format(redis_key, current_scheduler_id))
               # We flip the 'quit_on_none' to True so if we get None again, we'll just exit and try reading from EC2-Redis.
               quit_on_none = True

               # Sleep for 2 seconds plus some random amount of time between 0 and 1 seconds.
               sleep_amount = 2 + (random.randint(0, 1000) / 1000)
               sleep_amount = min(sleep_amount, sleep_max) + (random.randint(0, 2500) / 1000) # Clamp the sleep value to prevent super long sleeps, then add a random amount between 1 and 5 seconds.
               logger.debug("\tSleeping for {} seconds before trying again...".format(sleep_amount))
               time.sleep(sleep_amount)

               num_tries = num_tries + 1
         else:
            # If value was not None, then we break out of the loop.
            break 
      except Exception as ex:
         if use_fargate:
            logger.debug("[ERROR] Exception while attempting to read data at key {} [sid-{}] from Redis at {}:6379 ({}) (Try {}/{}).".format(redis_key, current_scheduler_id, fargate_ip, fargate_arn, num_tries, max_tries))
         else:
            logger.debug("[ERROR] Exception while attempting to read data at key {} [sid-{}] from DCP-Redis (Try {}/{}).".format(redis_key, current_scheduler_id, num_tries, max_tries))
         logger.debug("\tException: [{}] {}".format(type(ex), ex.__str__()))
         sleep_amount = ((2 ** num_tries) * sleep_base) + (random.randint(0, 1000) / 1000)
         sleep_amount = min(sleep_amount, sleep_max) + (random.randint(0, 5000) / 1000) # Clamp the sleep value to prevent super long sleeps, then add a random amount between 1 and 5 seconds.
         logger.debug("\t\tSleeping for {} seconds before trying again...".format(sleep_amount))
         time.sleep(sleep_amount)
         num_tries = num_tries + 1

   # Compute read time and size for metric collection/debugging.
   read_stop = time.time()
   
   if val is None:
      # I put this in its own variable to make it easier to read; the line was really long before.
      if use_fargate:
         warning_error_msg = "[WARNING - ERROR] The value retrieved for Task {} [sid-{}] from Fargate node {} was still None... Perhaps the value was a final result in a prev. computation. Checking EC2-Redis."
         logger.debug(warning_error_msg.format(key, current_scheduler_id, fargate_ip))
      else:
         warning_error_msg = "[WARNING - ERROR] The value retrieved for Task {} [sid-{}] from DCP-Redis was still None..."
         logger.debug(warning_error_msg.format(key, current_scheduler_id))
      
      if use_fargate:
         num_tries = 1
         max_tries = 5
         sleep_base = 0.25
         sleep_max = 24

         # Exponential backoff...
         while (num_tries < max_tries):
            try:
               read_start = time.time() # Re-initialize the start time here in case we've looped or checked Fargate-Redis previously.
               # Retrieve and return the data.
               val = dcp_redis.get(redis_key)
               read_stop = time.time()
               break 
            except Exception as ex:
               logger.debug("[ERROR] Exception while attempting to read data at key {} [sid-{}] from Redis EC2-Redis (dcp-redis) (Try {}/{}).".format(redis_key, current_scheduler_id, num_tries, max_tries))
               logger.debug("\tException: [{}] {}".format(type(ex), ex.__str__()))
               sleep_amount = ((2 ** num_tries) * sleep_base) + (random.randint(0, 1000) / 1000)
               sleep_amount = min(sleep_amount, sleep_max) + (random.randint(0, 5000) / 1000) # Clamp the sleep value to prevent super long sleeps, then add a random amount between 1 and 5 seconds.
               logger.debug("\t\tSleeping for {} seconds before trying again...".format(sleep_amount))
               time.sleep(sleep_amount)
               num_tries = num_tries + 1

         if val is None:
            logger.debug("The value is STILL None after trying EC2-Redis...")

            if exception_on_failure:
               raise ValueError("Value for Task {} [sid-{}] is missing in Fargate-Redis and EC2-Redis! Fargate Node ARN: {}. Fargate Node IP: {}".format(key,
                                                                                                                                                current_scheduler_id,
                                                                                                                                                fargate_arn,
                                                                                                                                                fargate_ip))
            else:
               logger.debug("[WARNING - ERROR] Value for Task {} [sid-{}] is missing in Fargate-Redis and EC2-Redis! Fargate Node ARN: {}. Fargate Node IP: {}".format(key,
                                                                                                                                                current_scheduler_id,
                                                                                                                                                fargate_arn,
                                                                                                                                                fargate_ip))     
               logger.debug("\tHowever, user specified that this is okay... Returning None.")
               return None                                                                                                                                                                                                                                                                      
         else:
            logger.debug("Successfully retrieved non-None value from EC2-Redis for Task {}.".format(key))        

            redis_read_duration = read_stop - read_start 
            read_size = sys.getsizeof(val)

            # Accumulate metric information.
            logger.debug("Adding read time for Fargate ARN: {}".format(fargate_arn))
            lambda_execution_breakdown.add_read_time(fargate_arn, redis_key, read_size, redis_read_duration, read_start, read_stop)
            task_execution_breakdown.redis_read_time += redis_read_duration
            lambda_execution_breakdown.redis_read_time += redis_read_duration                                                                                                                                        

   read_size = sys.getsizeof(val)
   lambda_execution_breakdown.bytes_read += read_size    

   return val

@xray_recorder.capture("store_value_in_redis")
def store_value_in_redis(path_node, 
                         value, 
                         key = None, 
                         check_first = False, 
                         serialized = True, 
                         task_execution_breakdown = None, 
                         lambda_execution_breakdown = None):
   """
      Store the given value in the Redis instance hosted on the Fargate node associated with the given path_node.

      Args:
         path_node (PathNode): The node, whose associated Redis instance/Fargate node will serve as the destination for this data storage operation.

         value (bytes):        The value we are storing in Redis.

         key (String or None): If 'None', then we will just use the 'task_key' of the PathNode
                               as our key for Redis. If given, then we will still use the Redis
                               instance associated with the PathNode, but we'll use the parameterized key.

         check_first (bool):   Indicates whether or not we should check for an existing value first. If we're supposed to check first, then
                               we'll only write a value if there is NO existing value.

         serialized (bool):    Indicates whether or not 'value' is serialized. This is only used when 'check_first' is True.

         task_execution_breakdown (TaskExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with the currently-processing task.
         
         lambda_execution_breakdown (LambdaExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with this Lambda invocation.         
      
      Returns:
         bool: Flag indicating whether or not we stored the value in
   """
   global use_fargate
   task_key = path_node.task_key 
   fargate_node = path_node.fargate_node
   fargate_ip = None 
   fargate_arn = "dcp_redis"

   if fargate_node is not None:
      fargate_ip = fargate_node[FARGATE_PUBLIC_IP_KEY] 
      #fargate_ip = fargate_node[FARGATE_PRIVATE_IP_KEY] 
      fargate_arn = fargate_node[FARGATE_ARN_KEY]

   # If key is non-null, we'll use that. Otherwise just use the task_key.
   redis_key = key or task_key 

   if use_fargate:
      logger.debug("Storing data for key {} in Redis instance listening at {}".format(redis_key, fargate_ip))
   else:
      logger.debug("Storing data for key {} in DCP Redis".format(redis_key))

   redis_client = None

   if use_fargate:
      # Check if we have a connection to this Fargate node cached. If so, then we'll use the existing connection.
      # If not, then we'll create the new connection, making sure to cache it for future use.
      if fargate_ip in hostnames_to_clients:
         redis_client = hostnames_to_clients[fargate_ip]
      else:
         redis_client = redis.StrictRedis(host = fargate_ip, port = 6379, db = 0, socket_connect_timeout  = 20, socket_timeout = 20)

         # Cache the connection.
         hostnames_to_clients[fargate_ip] = redis_client
   else:
      redis_client = dcp_redis
   
   # If we're supposed to check for an existing value first, then we'll see if a value already exists. Otherwise simply store the data w/o checking.
   if check_first:
      # Get the number of entries which exist for 'redis_key'.
      num_exist = redis_client.exists(redis_key)

      # Check if the value exists. If it does, then we won't write anything.
      if num_exist > 0:
         logger.debug("Task data for task {} already exists in Redis listening at {}:6379. Not writing the data.".format(redis_key, fargate_ip))
         return False 

   # We calculate the size early so we can report it in error messages, should we encounter errors.
   write_size = None

   # Serialize the value first if specified by the user.
   if serialized == False:
      serialization_start = time.time()
      # Serialize 'value' first, then we'll store it.
      value = cloudpickle.dumps(value)
      serialization_end = time.time()

      lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
      task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

      write_size = sys.getsizeof(value) # Get the value *after* we serialize, since that's what we're actually going to write.
   else:
      write_size = sys.getsizeof(value) # Already serialized...

   write_start = time.time()

   try:
      # Store the data and return True.
      redis_client.set(redis_key, value)            
   except Exception as ex1:
      logger.debug("[ERROR] Exception encountered whilst storing value ({} bytes) for task {} at key {}.\nException: [{}] {}".format(write_size, task_key, redis_key, type(ex1), ex1.__str__()))
      if use_fargate:
         logger.debug("\tRedis instance at {}:6379 -- ARN: {}".format(fargate_ip, fargate_arn))
         logger.debug("\tWill try to disable readonly mode and try again...\n")
         
         time.sleep(0.5 + (random.randint(0, 1000) / 1000)) # Delay things to give Redis a break...

         slave_of_succeeded = False
         try:
            logger.debug("Attempting to call slaveof() on Redis @ {}:6379 ({}).".format(fargate_ip, fargate_arn))
            # Try disabling readonly mode and try again...
            redis_client.slaveof() # Call with no arguments to promote instance to a Master
            slave_of_succeeded = True 
         except Exception as ex1:
            logger.debug("\t[ERROR] Exception encountered whilst calling slaveof() on Redis @ {}:6379 ({}).".format(fargate_ip, fargate_arn))
            logger.debug("\tSkipping that call for now...")
            time.sleep(0.5 + (random.randint(0, 1000) / 1000)) # Delay things to give Redis a break...

         logger.debug("\tslave_of_succeeded: {}".format(slave_of_succeeded))

      time.sleep(0.5 + (random.randint(0, 1000) / 1000)) # Delay things (possibly again) to give Redis a break...

      num_tries = 1
      max_tries = 8
      sleep_base = 0.25
      sleep_max = 24
      while (num_tries <= max_tries):
         try:
            write_start = time.time() # Restart the write timer
            redis_client.set(redis_key, value)
            break
         except Exception as ex2:
            logger.debug("[ERROR] Another exception encountered whilst storing value ({} bytes) for task {} at key {}. (Try {} of {}.)".format(write_size, task_key, redis_key, num_tries, max_tries))
            if use_fargate:
               logger.debug("\tRedis instance at {}:6379 -- ARN: {}".format(fargate_ip, fargate_arn))
            logger.debug("\tException: [{}] {}".format(type(ex2), ex2.__str__()))
            sleep_amount = ((2 ** num_tries) * sleep_base) + (random.randint(0, 1000) / 1000)
            sleep_amount = min(sleep_amount, sleep_max) + (random.randint(0, 5000) / 1000) # Clamp the sleep value to prevent super long sleeps, then add a random amount between 1 and 5 seconds.
            logger.debug("\t\tSleeping for {} seconds before trying again (assuming we aren't about to give up)...".format(sleep_amount))
            time.sleep(sleep_amount) # Exponential backoff. 
            num_tries = num_tries + 1

   write_stop = time.time()
   redis_write_time = write_stop - write_start
   
   # Record metric information.
   task_execution_breakdown.redis_write_time += redis_write_time 
   lambda_execution_breakdown.redis_write_time += redis_write_time 
   if use_fargate:
      logger.debug("Adding write time of {} seconds for Fargate ARN: {}.".format(redis_write_time, fargate_arn))
      lambda_execution_breakdown.add_write_time(fargate_arn, redis_key, write_size, redis_write_time, write_start, write_stop) 

      write_event = WukongEvent(
         name = "Store Intermediate Data in Fargate Redis",
         start_time = write_start,
         end_time = write_stop,
         metadata = {
            "Duration (seconds)": redis_write_time,
            "Size (bytes)": write_size,
            "Fargate ARN": fargate_arn,
            "Task Key": redis_key
         }
      )
      lambda_execution_breakdown.add_event(write_event)      
   else:
      logger.debug("Adding write time of {} seconds for DCP Redis.".format(redis_write_time))
      lambda_execution_breakdown.add_write_time("dcp_redis", redis_key, write_size, redis_write_time, write_start, write_stop) 

      write_event = WukongEvent(
         name = "Store Intermediate Data in EC2 Redis",
         start_time = write_start,
         end_time = write_stop,
         metadata = {
            "Duration (seconds)": redis_write_time,
            "Size (bytes)": write_size,
            "Redis Key": redis_key
         }
      )
      lambda_execution_breakdown.add_event(write_event)        

   lambda_execution_breakdown.bytes_written += write_size

   return True 

def create_mask(n, omit = []):
   """ Create a bit mask for a dependency key with 'n' dependencies. We shift
       and modify the bit mask to work with the way Redis and EC2 store their values.

       In Redis, when you do:

         client.setbit("key", 0, 1)
         client.setbit("key", 1, 1)
         client.setbit("key", 3, 1)
         client.setbit("key", 5, 1)
      
      The result is:

      Position:   0  1  2  3     4  5  6  7
      Value:      1  1  0  1     0  1  0  0

      And Linux on EC2 will then interpret position 0 to be the MSB and position 7 to be the LSB. 
      We account for this when creating our bitmask.

      Args:
         n (int):       the number of dependencies 

         omit ([int]):  bits that should be zero'd out regardless of whether or not they'd normally be included in the mask.

      Returns:
         (int):      a bitmask for the number of dependencies given.
   """
   next_multiple_of_8 = (n + 7) & -8
   diff = next_multiple_of_8 - n
   mask = 0
   for i in range(0, n):
      mask = (mask << 1) | 1
   for i in range(0, diff):
      mask = mask << 1
   
   # Toggle off (i.e., set to 0) all positions specified in 'omit'.
   for pos in omit:
      mask = (mask & ~(1 << (next_multiple_of_8 - pos - 1)))
   return mask

@xray_recorder.capture("check_dependency_counter_bits")
def check_dependency_counter_bits(dependent_path_node, num_dependencies, increment = False, dependency_path_node = None, task_execution_breakdown = None, lambda_execution_breakdown = None):
   """ Check if the given task is ready for execution based on whether 
       or not all of its dependencies have completed. 
       
       We check by comparing its dependency counter (which is an integer stored in Redis) against 
       the number of dependencies it has, specified by 'num_dependencies'. In some instances, we 
       want to check how many of its dependencies have resolved without also incrementing the counter. 
       Whether or not we increment is determined by the value of the given 'increment' flag. 
       
       Finally, the task_node parameter just enables us to print the list of dependencies for a given task (specifically, we print the task keys).
       
       Args:
         dependent_path_node (PathNode): The PathNode associated with the task for which we are checking the dependency counter.

         num_dependencies (int): The number of dependencies of the given task.

         increment (bool): Flag indicating whether or not we should increment the dependency counter while we check it.

         dependency_path_node (PathNode): The PathNode of the task which finished executing. This must not be None.

         task_execution_breakdown (TaskExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with the currently-processing task.
         
         lambda_execution_breakdown (LambdaExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with this Lambda invocation.         

       Returns:
         bool: Flag indicating whether or not the task is ready for execution.
   """
   check_deps_start = time.time()
   
   dependent_task_key = dependent_path_node.task_key
   key_counter = dependent_task_key + DEPENDENCY_COUNTER_SUFFIX

   logger.debug("Checking dependencies (via bits) for task {} [sid-{} uid-{}]. Increment: {}".format(dependent_task_key, dependent_path_node.scheduler_id, dependent_path_node.update_graph_id, increment))

   # Verify that dependency_path_node is non-null.
   if dependency_path_node == None:
      raise ValueError("The parameter 'dependency_path_node' cannot be None. dependent_path_node.task_key =", dependent_path_node.task_key)
   
   # If we're incrementing/updating the counter...
   if increment:
      bit_offset = dependency_path_node.dep_index_map[dependent_task_key]
      logger.debug("bit_offset for {}: {}".format(dependency_path_node.task_key, bit_offset))

      # Construct transaction.
      dep_pipeline = dcp_redis.pipeline(transaction = True)
      dep_pipeline.setbit(key_counter, bit_offset, 1)
      dep_pipeline.get(key_counter)

      res = list()      # Will put value returned from the pipeline's execution here...
      success = False

      num_tries = 1
      max_tries = 10
      sleep_base = 0.1
      max_sleep = 20

      while num_tries <= max_tries:
         try:
            res = dep_pipeline.execute()
            success = True
            break                                                                                                               
         except (ConnectionError, Exception) as ex:
            # Exponential backoff.
            logger.debug("[ERROR] {} when attempting to get and update bit dependency counter associated with key {}.".format(type(ex), key_counter))
            sleep_interval = ((2 ** num_tries) * sleep_base) + (random.randint(0, 500) / 1000)
            sleep_amount = min(max_sleep, sleep_interval) + (random.randint(0, 500) / 1000) # Clamp to 'sleep_cap' then add some random value.
            logger.debug("\tSleeping for {} seconds before trying again, assuming we aren't out of tries... (try {}/{})".format(sleep_amount, num_tries, max_tries))
            num_tries += 1
            time.sleep(sleep_amount)

      # If we failed, then raise an Exception here so we don't fail "silently".
      if not success:
         raise Exception("Unable to retrieve bit dependency counter for task {} from Redis after {} attempts.".format(dependent_task_key, num_tries))

      # Get results.
      dep_counter_bytes = res[1] # Note that the previous value of the bit is stored in res[0]

      if dep_counter_bytes is None:
         logger.debug("Task {} is NOT ready for execution. None of the {} dependencies have been completed yet.".format(dependent_task_key, num_dependencies))
         return False         

      # Convert to an integer.
      dep_counter = int(dep_counter_bytes.hex(), base = 16)

      logger.debug("{} -- bin(dep_counter)".format(bin(dep_counter)))

      # Create bitmask.
      bitmask = create_mask(num_dependencies)

      logger.debug("{} -- bin(bitmask)".format(bin(bitmask)))

      # Test if ready.
      ready = ((dep_counter ^ bitmask) == 0)

      logger.debug("{} -- bin(dep_counter ^ bitmask)".format(bin((dep_counter ^ bitmask))))

      # Check if the task is done.
      if ready:
         logger.debug("Task {} IS ready for execution. All {} dependencies completed.".format(dependent_task_key, num_dependencies))

         check_deps_stop = time.time() 
         check_deps_duration = check_deps_stop - check_deps_start
         task_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration
         lambda_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration

         check_dependency_counter_event = WukongEvent(
            name = "Check Dependency Counter",
            start_time = check_deps_start,
            end_time = check_deps_stop,
            metadata = {
               "Duration": check_deps_stop - check_deps_start,
               "Task Key": dependent_task_key,
               "Total Number of Dependencies": num_dependencies,
               "# Ready": num_dependencies,
               "# Not Ready": 0
            }
         )
         lambda_execution_breakdown.add_event(check_dependency_counter_event)

         return True
      else:
         # The number of dependencies completed is just the number of 1's in the key.
         num_completed = bin(dep_counter).count("1")

         # We use this value to get the offset of a given dependency since Position 0 on Redis is the MSB in this case.
         next_multiple_of_8 = (num_dependencies + 7) & -8
         logger.debug("Task {} is NOT ready for execution. Only {} of {} dependencies have been completed.".format(dependent_task_key, num_completed, num_dependencies))
         deps = list(dependent_path_node.task_payload['dependencies'])
         for i in range(0, len(deps)):
            dep_key = deps[i]
            idx = next_multiple_of_8 - i - 1
            finished = dep_counter & (1 << idx)
            if not finished:
               logger.debug("\tDependency {} is NOT finished yet.".format(dep_key))

         check_deps_stop = time.time() 
         check_deps_duration = check_deps_stop - check_deps_start
         task_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration
         lambda_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration

         check_dependency_counter_event = WukongEvent(
            name = "Check Dependency Counter",
            start_time = check_deps_start,
            end_time = check_deps_stop,
            metadata = {
               "Duration": check_deps_stop - check_deps_start,
               "Task Key": dependent_task_key,
               "Total Number of Dependencies": num_dependencies,
               "# Ready": num_completed,
               "# Not Ready": num_dependencies - num_completed
            }
         )
         lambda_execution_breakdown.add_event(check_dependency_counter_event)

         return False
   else:
      dep_counter_bytes = None

      success = False

      num_tries = 1
      max_tries = 10
      sleep_base = 0.1
      max_sleep = 20

      while num_tries <= max_tries:
         try:
            dep_counter_bytes = dcp_redis.get(key_counter)
            success = True
            break                                                                                                               
         except (ConnectionError, Exception) as ex:
            # Exponential backoff.
            logger.debug("[ERROR] {} when attempting to get and update bit dependency counter associated with key {}.".format(type(ex), key_counter))
            sleep_interval = ((2 ** num_tries) * sleep_base) + (random.randint(0, 500) / 1000)
            sleep_amount = min(max_sleep, sleep_interval) + (random.randint(0, 500) / 1000) # Clamp to 'sleep_cap' then add some random value.
            logger.debug("\tSleeping for {} seconds before trying again, assuming we aren't out of tries... (try {}/{})".format(sleep_amount, num_tries, max_tries))
            num_tries += 1
            time.sleep(sleep_amount)

      # If we failed, then raise an Exception here so we don't fail "silently".
      if not success:
         raise Exception("Unable to retrieve bit dependency counter for task {} from Redis after {} attempts.".format(dependent_task_key, num_tries))

      dep_counter = 0

      # If the result is None, then that just means nothing is ready yet.
      if dep_counter_bytes is not None:
         # Convert to an integer.
         dep_counter = int(dep_counter_bytes.hex(), base = 16)     

         logger.debug("{} -- bin(dep_counter)".format(bin(dep_counter)))

         bit_offset = dependency_path_node.dep_index_map[dependent_task_key]

         # Create bitmask. Don't toggle the bit to which the dependency is assigned since we are not "incrementing".
         bitmask = create_mask(num_dependencies, omit = [bit_offset])
         
         # We also check w/o omitting. It's possible we're executing/re-executing a leaf task, in which case
         # the counter will already be incremented. If that's the case, then the dep counter will have a 1
         # in the corresponding bit. So if we omit the task but we've already executed it once before,
         # the dependency check will incorrectly fail.
         bitmask_no_omit = create_mask(num_dependencies)

         logger.debug("{} -- bin(bitmask)".format(bin(bitmask)))

         logger.debug("{} -- bin(bitmask_no_omit)".format(bin(bitmask_no_omit)))

         # Test if ready.
         ready = ((dep_counter ^ bitmask) == 0) or ((dep_counter ^ bitmask_no_omit) == 0)  
         
         logger.debug("{} -- bin(dep_counter ^ bitmask)".format(bin((dep_counter ^ bitmask))))

         logger.debug("{} -- bin(dep_counter ^ bitmask_no_omit)".format(bin((dep_counter ^ bitmask_no_omit))))

         # Check if the task is done.
         if ready:
            logger.debug("Task {} IS ready for execution. All {} dependencies completed.".format(dependent_task_key, num_dependencies))

            check_deps_stop = time.time() 
            check_deps_duration = check_deps_stop - check_deps_start
            task_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration
            lambda_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration       

            check_dependency_counter_event = WukongEvent(
               name = "Check Dependency Counter",
               start_time = check_deps_start,
               end_time = check_deps_stop,
               metadata = {
                  "Duration": check_deps_stop - check_deps_start,
                  "Task Key": dependent_task_key,
                  "Total Number of Dependencies": num_dependencies,
                  "# Ready": num_dependencies,
                  "# Not Ready": 0
               }
            )
            lambda_execution_breakdown.add_event(check_dependency_counter_event)      

            return True
         else:
            # The number of dependencies completed is just the number of 1's in the key.
            num_completed = bin(dep_counter).count("1")

            # We use this value to get the offset of a given dependency since Position 0 on Redis is the MSB in this case.
            next_multiple_of_8 = (num_dependencies + 7) & -8
            logger.debug("Task {} is NOT ready for execution. Only {} of {} dependencies have been completed.".format(dependent_task_key, num_completed, num_dependencies))
            deps = list(dependent_path_node.task_payload['dependencies'])
            for i in range(0, len(deps)):
               dep_key = deps[i]
               idx = next_multiple_of_8 - i - 1
               finished = dep_counter & (1 << idx)
               if not finished:
                  logger.debug("\tDependency {} is NOT finished yet.".format(dep_key))

            check_deps_stop = time.time() 
            check_deps_duration = check_deps_stop - check_deps_start
            task_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration
            lambda_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration

            check_dependency_counter_event = WukongEvent(
               name = "Check Dependency Counter",
               start_time = check_deps_start,
               end_time = check_deps_stop,
               metadata = {
                  "Duration": check_deps_stop - check_deps_start,
                  "Task Key": dependent_task_key,
                  "Total Number of Dependencies": num_dependencies,
                  "# Ready": num_completed,
                  "# Not Ready": num_dependencies - num_completed
               }
            )
            lambda_execution_breakdown.add_event(check_dependency_counter_event)            
            return False    
      else:
         logger.debug("Task {} is NOT ready for execution. None of the {} dependencies have been completed yet.".format(dependent_task_key, num_dependencies))

         check_deps_stop = time.time() 
         check_deps_duration = check_deps_stop - check_deps_start
         task_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration
         lambda_execution_breakdown.checking_and_incrementing_dependency_counters += check_deps_duration   

         check_dependency_counter_event = WukongEvent(
            name = "Check Dependency Counter",
            start_time = check_deps_start,
            end_time = check_deps_stop,
            metadata = {
               "Duration": check_deps_stop - check_deps_start,
               "Task Key": dependent_task_key,
               "Total Number of Dependencies": num_dependencies,
               "# Ready": 0,
               "# Not Ready": num_dependencies
            }
         )
         lambda_execution_breakdown.add_event(check_dependency_counter_event)               
         return False

@xray_recorder.capture("check_dependency_counter")
def check_dependency_counter(path_node, num_dependencies, increment = False, task_execution_breakdown = None, lambda_execution_breakdown = None):
   """ Check if the given task is ready for execution based on whether 
       or not all of its dependencies have completed. 
       
       We check by comparing its dependency counter (which is an integer stored in Redis) against 
       the number of dependencies it has, specified by 'num_dependencies'. In some instances, we 
       want to check how many of its dependencies have resolved without also incrementing the counter. 
       Whether or not we increment is determined by the value of the given 'increment' flag. 
       
       Finally, the task_node parameter just enables us to print the list of dependencies for a given task (specifically, we print the task keys).
       
       Args:
         path_node (PathNode): The PathNode associated with the task for which we are checking the dependency counter.

         num_dependencies (int): The number of dependencies of the given task.

         increment (bool): Flag indicating whether or not we should increment the dependency counter while we check it.

         task_execution_breakdown (TaskExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with the currently-processing task.
         
         lambda_execution_breakdown (LambdaExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with this Lambda invocation.         

       Returns:
         bool: Flag indicating whether or not the task is ready for execution.
   """
   task_key = path_node.task_key
   
   key_counter = task_key + DEPENDENCY_COUNTER_SUFFIX

   logger.debug("Checking dependencies (via standard) for task {} [sid-{} uid-{}]. Increment: {}".format(task_key, path_node.scheduler_id, path_node.update_graph_id, increment))

   if increment:
      read_start = time.time()
      success = False
      num_tries = 1
      max_tries = 10
      sleep_base = 0.1
      max_sleep = 20

      dependencies_completed = -1

      while num_tries <= max_tries:
         try:
            dependencies_completed = dcp_redis.incr(key_counter)
            success = True
            break                                                                                                            
         except (ConnectionError, Exception) as ex:
            # Exponential backoff.
            logger.debug("[ERROR] {} when attempting to increment standard dependency counter associated with key {}.".format(type(ex), key_counter))
            sleep_interval = ((2 ** num_tries) * sleep_base) + (random.randint(0, 500) / 1000)
            sleep_amount = min(max_sleep, sleep_interval) + (random.randint(0, 500) / 1000) # Clamp to 'sleep_cap' then add some random value.
            logger.debug("\tSleeping for {} seconds before trying again, assuming we aren't out of tries... (try {}/{})".format(sleep_amount, num_tries, max_tries))
            num_tries += 1
            time.sleep(sleep_amount)

      # If we failed, then raise an Exception here so we don't fail "silently".
      if not success:
         raise Exception("Unable to retrieve standard dependency counter for task {} from Redis after {} attempts.".format(task_key, num_tries))

      # Calculate metric information.
      read_stop = time.time()
      redis_read_time = read_stop - read_start 
      read_size = sys.getsizeof(dependencies_completed)

      # Store metric information.
      lambda_execution_breakdown.add_read_time(EC2_REDIS_METRIC_KEY, key_counter,read_size, redis_read_time, read_start, read_stop)
      task_execution_breakdown.redis_read_time += redis_read_time
      lambda_execution_breakdown.redis_read_time += redis_read_time

      # Check if the task is done.
      if dependencies_completed == num_dependencies:
         logger.debug("Task {} IS ready for execution. All {} dependencies completed.".format(task_key, num_dependencies))
         return True 
      else:
         logger.debug("Task {} is NOT ready for execution. Only {} of {} dependencies have been completed.".format(task_key, dependencies_completed, num_dependencies))
         logger.debug("Dependencies for {}: {}".format(task_key, path_node.task_payload["dependencies"]))
         return False
   else:
      read_start = time.time()

      # Use default value of '0'. 
      dependencies_completed = 0

      success = False 

      num_tries = 1
      max_tries = 10
      sleep_base = 0.1
      max_sleep = 20

      while num_tries <= max_tries:
         try:
            dependencies_completed = int(dcp_redis.get(key_counter).decode())
            success = True 
            break
         except AttributeError:
            logger.debug("[ERROR] AttributeError: 'NoneType' object has no attribute 'decode'."
                     + " Occurred while retrieving dependency counter for Task {} [sid-{} uid-{}] (key counter = {})".format(task_key, 
                                                                                                                           path_node.scheduler_id, 
                                                                                                                           path_node.update_graph_id, 
                                                                                                                           key_counter))
            return False                                                                                                                 
         except (ConnectionError, Exception) as ex:
            # Exponential backoff.
            logger.debug("[ERROR] {} when attempting to get standard dependency counter associated with key {}.".format(type(ex), key_counter))
            sleep_interval = ((2 ** num_tries) * sleep_base) + (random.randint(0, 500) / 1000)
            sleep_amount = min(max_sleep, sleep_interval) + (random.randint(0, 500) / 1000) # Clamp to 'sleep_cap' then add some random value.
            logger.debug("\tSleeping for {} seconds before trying again, assuming we aren't out of tries... (try {}/{})".format(sleep_amount, num_tries, max_tries))
            num_tries += 1
            time.sleep(sleep_amount)
         
      # If we failed, then raise an Exception here so we don't fail "silently".
      if not success:
         raise Exception("Unable to retrieve standard dependency counter for task {} from Redis after {} attempts.".format(task_key, num_tries))         
         
      # Calculate metric information.
      read_stop = time.time()
      redis_read_time = read_stop - read_start 
      read_size = sys.getsizeof(dependencies_completed)

      # Store metric information.
      lambda_execution_breakdown.add_read_time(EC2_REDIS_METRIC_KEY, key_counter, read_size, redis_read_time, read_start, read_stop)
      task_execution_breakdown.redis_read_time += redis_read_time
      lambda_execution_breakdown.redis_read_time += redis_read_time
      lambda_execution_breakdown.bytes_read += read_size          

      # Since we're NOT incrementing the key in this case, we check against (num_dependencies - 1).
      if dependencies_completed == (num_dependencies - 1):
         logger.debug("Task {} IS ready for execution. All {} dependencies completed.".format(task_key, num_dependencies))
         return True 
      else:
         logger.debug("Task {} is NOT ready for execution. Only {} of {} dependencies have been completed.".format(task_key, dependencies_completed, num_dependencies))
         logger.debug("Dependencies for {}: {}".format(task_key, path_node.task_payload["dependencies"]))         
         return False 

@xray_recorder.capture("process_path")
def process_path(nodes_map_serialized, 
                 starting_node_key, 
                 tasks_to_fargate_nodes, 
                 previous_results = dict(), 
                 use_task_queue = True,
                 context = None,
                 lambda_debug = True,
                 use_bit_dep_checking = False,    
                 leaf_key = None,             
                 lambda_execution_breakdown = None, 
                 task_execution_breakdowns = None):
   """ Process the current static schedule. This involves [attempting to] executing all of the tasks contained within the path.

      Args:
         nodes_map_serialized {task-key --> PathNode}:   A map from task keys to the PathNode object corresponding to that task.

         starting_node_key (String):                     The task key of the first node in the Path, or at least the first node we're supposed to process.

         previous_results {task-key --> Object}:         A map from task keys to the data generated by the execution of the associated task.

         use_task_queue (bool):                          If True, large objects will wait for downstream tasks to become ready instead of writing data.

         context (Context):                              Context object from AWS Lambda. Used to get request ID of this Lambda function for logging purposes.
         
         lambda_execution_breakdown (LambdaExecutionBreakdown): The WukongMetrics object encapsulating all metrics associated with this Lambda invocation.  

         task_execution_breakdown (TaskExecutionBreakdown):     The WukongMetrics object encapsulating all metrics associated with the currently-processing task.  

         leaf_key (str):   The key of the leaf task of this Lambda. Used to make sure we don't remove this data from previous_results. Will only be non-null
                           if we care about not deleting the data from previous results. 

         lambda_debug (bool): If True, Lambdas print more debug messages and send debug info to Scheduler.

         use_bit_dep_checking (bool): If True, use check_dependency_counter_bits for checking dependencies. If False, use check_dependency_counter.

      Returns:
         dict -- A dictionary containing information about the execution of this Lambda function. If an error occurred, then this dictionary will contain 
                 information about said error.
   """

   # Dictionary to store local results.
   # We map task keys to the result of the task.
   # results = {}
   
   # This is just like 'nodes_map_serialized' except we place deserialized nodes in here.
   nodes_map_deserialized = dict()

   subsegment = xray_recorder.begin_subsegment("deserializing_path")
   
   deserialization_start = time.time()

   # Deserialize the first node in the static schedule.
   current_path_node_encoded = nodes_map_serialized[starting_node_key]
   current_path_node_serialized = base64.b64decode(current_path_node_encoded)
   current_path_node = cloudpickle.loads(current_path_node_serialized)

   deserialization_end = time.time()

   lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)

   xray_recorder.end_subsegment()
   
   # We put the out-edges of tasks we've executed in this queue for processing.
   # Processing primarily involves checking if they're ready to execute (i.e., are all dependencies available?).
   nodes_to_process = queue.Queue()

   # Value resulting from execution of current path node.
   value = None 

   # Process all of the tasks contained in the path.
   while current_path_node is not None or not nodes_to_process.empty():
      # 'current_path_node' might be done if we're only still looping because 'nodes_to_process' is non-empty 
      # after successfully  executing some downstream tasks of big tasks that were previously not ready to execute.
      if current_path_node is not None:
         # Create a TaskExecutionBreakdown object for the current task.
         # We use this to keep track of a bunch of different metrics related to the current task.
         current_task_execution_breakdown = TaskExecutionBreakdown("TEMP_KEY")
         current_path_node.task_breakdown = current_task_execution_breakdown
         current_task_execution_breakdown.task_processing_start_time = time.time()
         lambda_execution_breakdown.number_of_tasks_executed += 1
         _start_for_current_task = time.time()

         # If the task_payload field of the current PathNode is a list, then that means
         # it is still in serialized form and we need to deserialize it. 
         # (The payloads are serialized using Dask code into a list of 'frames').
         if type(current_path_node.task_payload) is list:
            deserialization_start = time.time()
            serialized_task_payload = current_path_node.task_payload

            # Collect the serialized frames and deserialize them.
            frames = []
            for encoded in serialized_task_payload:
               frames.append(base64.b64decode(encoded))
            current_path_node.task_payload = deserialize_payload(frames)
            deserialization_end = time.time()
            lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
            current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)            
         task_key = current_path_node.task_key
         
         # Update metrics related to the current task execution 
         current_task_execution_breakdown.deserialization_time = (time.time() - _start_for_current_task)
         current_task_execution_breakdown.task_key = task_key
         task_execution_breakdowns[current_task_execution_breakdown.task_key] = current_task_execution_breakdown
         current_task_execution_breakdown.update_graph_id = current_path_node.update_graph_id
         logger.debug("[PROCESSING sid-{} uid-{}] Processing Task {}. Dependencies have already been checked.".format(current_path_node.scheduler_id, current_path_node.update_graph_id, task_key))

         process_task_start = time.time()

         # Process the task via the process_task method. Pass in the 
         # local results variable for the "previous_results" keyword argument.
         result = process_task(current_path_node.task_payload, 
                              task_key, 
                              previous_results, 
                              tasks_to_fargate_nodes,
                              current_scheduler_id = current_path_node.scheduler_id,
                              current_update_graph_id = current_path_node.update_graph_id,
                              lambda_debug = lambda_debug, 
                              lambda_execution_breakdown = lambda_execution_breakdown, 
                              current_task_execution_breakdown = current_task_execution_breakdown,
                              context = context)
         lambda_execution_breakdown.process_task_time += (time.time() - process_task_start)
         
         data_written = False

         # Check the result of executing the task.
         # If the task erred, just return... 
         if result[OP_KEY] == TASK_ERRED_KEY:
            logger.debug("[ERROR] Execution of task {} resulted in an error.".format(task_key))
            logger.debug("Result: " + str(result))
            logger.debug("Result['exception']: " + str(result["exception"]))
            return {
               'statusCode': 400,
               'body': ujson.dumps(result["exception"])
            }   
         elif result[OP_KEY] == EXECUTED_TASK_KEY:
            logger.debug("[EXECUTION] Result of task {} computed in {} seconds.".format(task_key, result[EXECUTION_TIME_KEY]))
            value = result["result"]                   
            data_written = False
         elif result[OP_KEY] == RETRIEVED_FROM_PREVIOUS_RESULTS_KEY:
            logger.debug("[RETRIEVAL] Result of task {} retrieved from local cache.".format(task_key))
            value = result["result"]                   
            data_written = True            
         elif result[OP_KEY] == TASK_RETRIEVED_FROM_REDIS_KEY:
            data_written = True 
            value = result["result"] # We deserialized the value in process_task so it looks the same to everyone calling the function. (Like they expect a deserialized result.)

         current_path_node_unprocessed = UnprocessedNode(current_path_node, result, data_written = data_written)
         nodes_to_process.put_nowait(current_path_node_unprocessed)  

      # These should always be reset, even if current_path_node is None.
      become_node = None 
      become_path = None 
      need_to_download_become_path = False
      
      # Iterate over each node, processing its out edges.
      while not nodes_to_process.empty():
         node_processing = nodes_to_process.get_nowait()
         if (hasattr(node_processing.path_node, "task_breakdown") and node_processing.path_node.task_breakdown != None):
            logger.debug("Unprocessed Node {} had a Task Breakdown associated with it.".format(node_processing.path_node.task_key))
            current_task_execution_breakdown = node_processing.path_node.task_breakdown
         else:
            logger.debug("Unprocessed Node {} did NOT have a Task Breakdown associated with it. Creating one now...".format(node_processing.path_node.task_key))
            current_task_execution_breakdown = TaskExecutionBreakdown(node_processing.path_node.task_key, update_graph_id = node_processing.path_node.update_graph_id)
            _start_for_current_task = time.time()
            current_task_execution_breakdown.task_processing_start_time = _start_for_current_task
            node_processing.path_node.task_breakdown = current_task_execution_breakdown
         # Check if we need to deserialize the task payload...
         if type(node_processing.path_node.task_payload) is list:
            serialized_task_payload = node_processing.path_node.task_payload
            frames = []
            for encoded in serialized_task_payload:
               frames.append(base64.b64decode(encoded))
            deserialization_start = time.time()
            node_processing.path_node.task_payload = deserialize_payload(frames)
            deserialization_end = time.time()
            lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
            current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)            
         logger.debug("Processing node with task key: " + str(node_processing.path_node.task_key))
         #logger.debug("node_processing.path_node: ", node_processing.path_node.__str__())
         out_edges = list()
         
         # TO-DO:
         # (1) CHECK IF NODES ARE IN nodes_map_deserialized
         # (2) IF NOT, CHECK IF NODES ARE IN nodes_map_serialized
         # (3) IF NOT, DOWNLOAD RELEVANT NODES/PATHS FROM REDIS USING 'starts_at' FIELD ON PATH NODE.
         # (3) IF NOT, DOWNLOAD RELEVANT NODES/PATHS FROM REDIS USING 'starts_at' FIELD ON PATH NODE.

         # We will likely need to download these nodes/their paths... For each task we may need to invoke,
         # check and see if we have a path/path node corresponding to the task locally here on the Lambda.
         # If we don't, then we're going to retrieve it from Redis. We add each PathNode (corresponding to
         # the 'invoke_key') to the out_edges list.
         for invoke_key in node_processing.path_node.get_downstream_tasks():
            # Check DESERIALIZED nodes.
            if invoke_key in nodes_map_deserialized:
               out_edges.append(nodes_map_deserialized[invoke_key])
            # Check SERIALIZED nodes.
            elif invoke_key in nodes_map_serialized:
               invoke_node_encoded = nodes_map_serialized[invoke_key]

               deserialization_start = time.time()

               invoke_node_node_serialized = base64.b64decode(invoke_node_encoded)
               _invoke_node = cloudpickle.loads(invoke_node_node_serialized)
               
               # Record metrics.
               deserialization_end = time.time()
               lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
               current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
               nodes_map_deserialized[invoke_key] = _invoke_node
               out_edges.append(_invoke_node)
            # Retrieve from Redis.
            else:
               key_at_path_start = node_processing.path_node.starts_at 
               path_key = key_at_path_start + PATH_KEY_SUFFIX
               path_encoded = get_path_from_redis(path_key = path_key, task_execution_breakdown = current_task_execution_breakdown, lambda_execution_breakdown = lambda_execution_breakdown)
               
               deserialization_start = time.time()
               path_payload = ujson.loads(path_encoded.decode())
               deserialization_end = time.time()

               lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
               current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)               

               # Create a reference to the serialized nodes map.
               new_nodes_map_serialized = path_payload[NODES_MAP]

               # Add the new serialized nodes to the local 'nodes_map_serialized'.
               nodes_map_serialized.update(new_nodes_map_serialized)

               # Update our mapping from tasks to Fargate IP addresses.
               tasks_to_fargate_nodes.update(path_payload[TASK_TO_FARGATE_MAPPING])

               deserialization_start = time.time()

               # Decode, deserialize, and store the node in nodes_map_deserialized.
               invoke_node_encoded = nodes_map_serialized[invoke_key]
               invoke_node_node_serialized = base64.b64decode(invoke_node_encoded)
               _invoke_node = cloudpickle.loads(invoke_node_node_serialized)
               nodes_map_deserialized[invoke_key] = _invoke_node
               out_edges.append(_invoke_node)

               deserialization_end = time.time()

               lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
               current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)               
         
         # If 'node_processing' has out-edges that require processing, then process them.
         if len(out_edges) > 0:
            next_nodes, become_candidate, need_to_download_become_path = process_out_edges(out_edges,
                                                                           node_processing,
                                                                           nodes_map_serialized,
                                                                           nodes_map_deserialized,
                                                                           previous_results,
                                                                           tasks_to_fargate_nodes,
                                                                           use_bit_dep_checking = use_bit_dep_checking,
                                                                           needs_become = (become_node == None),
                                                                           use_task_queue = use_task_queue,
                                                                           lambda_debug = lambda_debug,
                                                                           lambda_execution_breakdown = lambda_execution_breakdown,
                                                                           current_task_execution_breakdown = current_task_execution_breakdown,
                                                                           task_execution_breakdowns = task_execution_breakdowns,
                                                                           leaf_key = leaf_key,
                                                                           context = context)
            # Add the new nodes to 'nodes_to_process'
            for new_node_to_process in next_nodes:
               nodes_to_process.put_nowait(new_node_to_process)
            current_task_execution_breakdown.total_time_spent_on_this_task = (time.time() - _start_for_current_task)
            current_task_execution_breakdown.task_processing_end_time = time.time()
            task_execution_breakdowns[current_task_execution_breakdown.task_key] = current_task_execution_breakdown
            
            # If we do not have a value for our 'become' node yet and a value was returned, then use that value for our 'become 'node.
            if (become_node is None and become_candidate is not None):
               logger.debug("The current 'become_node' is None and we have a 'become_candidate' in that of {}. We're gonna use it.".format(become_candidate.task_key))
               become_node = become_candidate
               if need_to_download_become_path:
                  path_key = become_node.task_key + PATH_KEY_SUFFIX
                  _path = get_path_from_redis(path_key = path_key, task_execution_breakdown = current_task_execution_breakdown, lambda_execution_breakdown = lambda_execution_breakdown)
                  
                  deserialization_start = time.time()
                  become_path = ujson.loads(_path.decode())
                  deserialization_end = time.time()
                  lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)                
         
         if use_task_queue:
            # Before we attempt to 'become' the next node, we're going to check all of the tasks we've enqueued in the 'task_queue' variable.
            # These tasks correspond to downstream tasks that have a dependency on some Big Task T which we've executed locally. That is, we 
            # executed some task T whose output was large (according to the user-defined large object threshold). The tasks in the queue were
            # not ready to execute at the time, but we'd like to avoid writing large intermediate data to Redis so instead, we just record that
            # we'd like to execute the task, and we check the tasks in the queue routinely until they're ready.           
            res = process_enqueued_tasks(previous_results, tasks_to_fargate_nodes, nodes_to_process, aws_context = context, use_bit_dep_checking = use_bit_dep_checking, lambda_debug = lambda_debug, lambda_execution_breakdown = lambda_execution_breakdown)

            # If there was an error in executing a task, return an error!
            op = res.pop(OP_KEY)
            if op == TASK_ERRED_KEY:
               return res 

      # The loop "while not nodes_to_process.empty():" ends here.

      # If we were supposed to download the path, then we need to deserialize the becomes node.
      if become_node is not None and need_to_download_become_path:
         next_nodes_map_serialized = become_path[NODES_MAP]
         
         # Add all of the new serialized nodes to the next_nodes_map_serialized dictionary.
         nodes_map_serialized.update(next_nodes_map_serialized)
         
         # Update our mapping of tasks to Fargate IPs with the new path's data.
         tasks_to_fargate_nodes.update(become_path[TASK_TO_FARGATE_MAPPING])
         next_become_node_encoded = nodes_map_serialized[become_node.task_key]
         
         deserialization_start = time.time()
         
         next_become_node_serialized = base64.b64decode(next_become_node_encoded)
         become_node = cloudpickle.loads(next_become_node_serialized)

         deserialization_end = time.time()
         lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
         current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)

      _now = time.time()
      current_task_execution_breakdown.total_time_spent_on_this_task = (_now - _start_for_current_task)
      current_task_execution_breakdown.task_processing_end_time = _now
      task_execution_breakdowns[current_task_execution_breakdown.task_key] = current_task_execution_breakdown

      # If we have a become node, then we'll go ahead and 'become' it and execute the task.
      if become_node is not None: 
         # The current path node may be done if we had just processed some downstream tasks from previously-executed big tasks. Specifically, these tasks
         # would have been "not ready" when we first executed the big task, so we came back to them later and they were ready.
         if current_path_node is not None:
            logger.debug("[BECOMING] Task {} [sid-{} uid-{}] is going to become task {} [sid-{} uid-{}].".format(current_path_node.task_key, 
                                                                                                          current_path_node.scheduler_id,
                                                                                                          current_path_node.update_graph_id,
                                                                                                          become_node.task_key,
                                                                                                          become_node.scheduler_id,
                                                                                                          become_node.update_graph_id))
         else:
            logger.debug("[BECOMING] This Task Executor, which presently has no explicitly assigned path node, is going to become task {} [sid-{} uid-{}].".format(become_node.task_key,
                                                                                                                                                            become_node.scheduler_id,
                                                                                                                                                            become_node.update_graph_id))
         current_path_node = become_node
      else:
         # Otherwise, we'll see if we need to process any tasks in the task queue, write any data, etc. It's possible that we've just executed the root node
         # and may need to inform the Scheduler that final results are available, for example.
         if current_path_node is None:
            logger.debug("[PROCESSING] Current become node is NONE. This is due to the fact that we processed some dependents of big tasks after waiting for them to become ready...")
            
            if use_task_queue:
               # Try to execute tasks in the queue for a while.
               process_enqueued_tasks_looped(previous_results, tasks_to_fargate_nodes, nodes_to_process, 
                                             aws_context = context, lambda_debug = lambda_debug, use_bit_dep_checking = use_bit_dep_checking,
                                             lambda_execution_breakdown = lambda_execution_breakdown)

         # For now, I am just using the "dask-workers-1" (or whatever the current value of the constant 'REDIS_PUB_SUB_CHANNEL' is) channel to publish messages to the Scheduler.
         # As long as I am creating the Redis-polling processes on the Scheduler, we can arbitrarily use whatever channel we want.
         # We only send the message if there are no downstream tasks bc otherwise this isn't a root node.
         elif current_path_node.num_downstream_tasks() == 0:
            logger.debug("[PROCESSING] - Task {} [sid-{} uid-{}] does not have any downstream tasks. It's the last task on its path.".format(current_path_node.task_key, current_path_node.scheduler_id, current_path_node.update_graph_id))
            
            serialization_start = time.time()
            serialized_value = cloudpickle.dumps(value)
            serialization_end = time.time()

            write_size = sys.getsizeof(serialized_value)
            lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
            current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

            write_start = time.time()
            
            num_tries = 0
            max_tries = 10
            max_sleep = 30
            sleep_base = 0.25
            success = False 

            # Exponential backoff.
            while (num_tries <= max_tries and not success):
               try:  
                  # By convention, store final results in the big node cluster.
                  dcp_redis.set(task_key, serialized_value)
                  success = True
               except Exception as ex:
                  logger.debug("[ERROR] Connection to DCP Redis timed out while calling set() for task {}. (try {}/{}).".format(
                     task_key, num_tries, max_tries))
                  num_tries += 1
                  sleep_amount = ((2 ** num_tries) * sleep_base) 
                  sleep_amount = min(sleep_amount, 30)
                  sleep_amount += random.randint(0, int(sleep_amount)) # Add a random amount.

                  if (num_tries > max_tries):
                     logger.debug("OUT OF TRIES For storing task {} data in DCP Redis...".format(task_key))
                     raise ex
                  else:
                     logger.debug("\t\tSleeping for {} seconds before trying again...".format(sleep_amount))
                     time.sleep(sleep_amount)                     

            # Collect, calculate, and store diagnostic/metric/debug information.
            write_stop = time.time()
            write_duration = write_stop - write_start
            current_task_execution_breakdown.redis_write_time += write_duration
            lambda_execution_breakdown.redis_write_time += write_duration
            lambda_execution_breakdown.add_write_time(EC2_REDIS_METRIC_KEY, task_key, write_size, write_duration, write_start, write_stop)
            lambda_execution_breakdown.bytes_written += write_size

            logger.debug("[MESSAGING] - Publishing '{}' message to Redis Channel: {}".format(LAMBDA_RESULT_KEY, REDIS_PUB_SUB_CHANNEL))
            logger.debug("Final result: " + str(value))
            
            # The result dict won't have an execution time entry if we retrieved the result from Redis instead of executing the task ourselves.
            execution_time = 0
            if EXECUTION_TIME_KEY in result:
               execution_time = result[EXECUTION_TIME_KEY]

            payload = {
               OP_KEY: LAMBDA_RESULT_KEY,
               LAMBDA_ID_KEY: context.aws_request_id,
               TASK_KEY: current_path_node.task_key,
               EXECUTION_TIME_KEY: execution_time,
               'time_sent': time.time()
            }
            serialization_start = time.time()
            payload_serialized = ujson.dumps(payload)
            serialization_end = time.time()

            lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
            current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

            publish_start = time.time() 
            # By convention, use the dependency counter and path Redis client for message passing.
            #dcp_redis.publish(REDIS_PUB_SUB_CHANNEL, payload_serialized)
            publish_dcp_message(REDIS_PUB_SUB_CHANNEL, payload_serialized, serialized = True)

            publish_stop = time.time()
            publish_duration = publish_stop - publish_start 
            current_task_execution_breakdown.publishing_messages += publish_duration
            lambda_execution_breakdown.publishing_messages += publish_duration

            #logger.debug("[INFO] Made two calls to .exists() for task {}. Before publishing: {} After publishing: {}".format(task_key, exists_before, exists_after))
         else:
            logger.debug("[PROCESSING] - There are no available downstream tasks which depend upon task {}.".format(current_path_node.task_key))
            logger.debug("Become for {} [sid-{} uid-{}]: {}".format(current_path_node.task_key, current_path_node.scheduler_id, current_path_node.update_graph_id, current_path_node.become))
            logger.debug("Invoke for {} [sid-{} uid-{}]: {}".format(current_path_node.task_key, current_path_node.scheduler_id, current_path_node.update_graph_id, current_path_node.invoke))         

            # If the current node corresponds with a "small" task, then we can definitely just go ahead and write it.
            if current_path_node.is_big == False:
               # Make sure we write the data to Redis. First check if it is there. If it isn't, write it.
               # We don't want to waste time serializing the value unless we're actually going to store it.
               stored = store_value_in_redis(current_path_node, 
                                             value, 
                                             key = None, 
                                             check_first = True,
                                             serialized = False, 
                                             task_execution_breakdown = current_task_execution_breakdown, 
                                             lambda_execution_breakdown = lambda_execution_breakdown)  
               if (stored):
                  logger.debug("Data for task {} [sid-{} uid-{}] has been written to Redis.".format(task_key, current_path_node.scheduler_id, current_path_node.update_graph_id))   

            # If the current path node is large, we should first try to process the task queue. 
            # The data will be written if we fail to process the tasks in the queue within a certain number of tries.
            if use_task_queue:
               # Try to execute tasks in the queue for a while.
               process_enqueued_tasks_looped(previous_results, tasks_to_fargate_nodes, nodes_to_process, 
                                             aws_context = context, lambda_debug = lambda_debug, use_bit_dep_checking = use_bit_dep_checking,
                                             lambda_execution_breakdown = lambda_execution_breakdown)

         current_path_node = None         
      #xray_recorder.end_subsegment()
   return {
      'statusCode': 202,
      'body': ujson.dumps("Success")  
   }
    
@xray_recorder.capture("process-out-edges")
def process_out_edges(out_edges, 
                      node_processing,
                      nodes_map_serialized, 
                      nodes_map_deserialized,
                      previous_results, 
                      tasks_to_fargate_nodes,
                      use_bit_dep_checking = False,
                      needs_become = True,
                      use_task_queue = True,
                      lambda_debug = False,
                      lambda_execution_breakdown = None, 
                      current_task_execution_breakdown = None,
                      leaf_key= None,
                      task_execution_breakdowns = dict(),
                      context = None):
   """ 
      Process the out-edges of a given task (which we presumably just executed). 

      This function doesn't do much of the processing itself; instead, it offloads the processing to one of two functions: process_small() or process_big(). The function
      used is dependant upon the size of the given task's intermediate output data.

      This function also processes the previous_results dictionary to determine if any locally cached data can be uncached in the interest of saving space/not running
      out of memory within the Lambda function. For each item stored in previous_results, we iterate over the tasks which use that item. If the task has (1) been executed
      locally or (2) has a value existing in Redis already (meaning some other Lambda executed it), then we do not need to keep track of the data. If at least one task
      which depends on the cached item has not been executed yet (locally or externally), then we keep the data cached for now.

      Args:
         out_edges [PathNode]: The out-edges we're processing. These are the immediate down-stream edges from the last task we executed on this Lambda function.
         
         node_processing (UnprocessedNode): The UnprocessedNode object associated with the task we are now processing.
         
         nodes_map_serialized (Dict): This is a map of TASK-KEY --> SERIALIZED PathNode OBJECT for all of the nodes in the current path.
         
         nodes_map_deserialized (Dict): This is a cache of deserialized PathNodes. The nodes contained within are a subnet of those within 'nodes_map_serialized'.
         
         previous_results (Dict): Local cache of intermediate output data from tasks executed on this Lambda function.
         
         tasks_to_fargate_nodes (Dict): A map of TASK-KEY --> FARGATE NODE IP's.
         
         use_bit_dep_checking (bool): If True, use check_dependency_counter_bits for checking dependencies. If False, use check_dependency_counter.
         
         needs_become = True (Bool): If true, then we still don't have a node/task we can 'become' within the current path. We will attempt to find one while processing.
         
         lambda_execution_breakdown (LambdaExecutionBreakdown): Metric data for the Lambda function itself.
         
         current_task_execution_breakdown (TaskExecutionBreakdown): Metric data associated with the task we just executed.
         
         task_execution_breakdowns (Dict): A map from TASK-KEY --> TaskExecutionBreakdown for all the tasks we've executed on this Lambda function.
         
         leaf_key (str): This will only be non-null if this Lambda began as a leaf task AND the Scheduler may try to re-use the Lambda function.
                         If it is non-null, then its value will be the key of the leaf task. We use this to make sure we leave the leaf task's data
                         in the previous_results dictionary.

         context (Context): AWS Lambda Context object.

      Returns:
         ([Nodes], Node, Bool) -- Returns a 3-tuple (x,y,z) such that:
            x: A list of nodes that we need to process next (the next set of out-edges to process, basically),
            y: The current 'become' node. If we need a become node still, then this node will potentially be used.
            z: Whether or not we already have the Path in which 'current_become_node' is contained. 
   """
   global use_fargate

   current_path_node = node_processing.path_node   # The PathNode object associated with the UnprocessedNode that we are currently processing.
   current_task_key = current_path_node.task_key   # The unique key of the task we just executed (on this Lambda function).
   result = node_processing.result                 # The output data of the task we just executed.
   task_payload = current_path_node.task_payload   # The Dask task payload of the task we just executed. Contains information about the task's code, dependencies, etc.
   
   value = result.pop('result', None)

   # Cache the result locally for future task executions.
   # In some cases, we may have already cached the results (i.e. if we previously pulled down and executed locally the current task).
   # In these situations, 'value' would currently be None, and we'd overwrite the existing entry in 'previous_results' if we
   # did not perform this check first.
   if value is not None or current_task_key not in previous_results:
      previous_results[current_task_key] = value
   
   if value is None:
      raise ValueError("[ERROR] Previous result value is none for task {} [sid-{} uid-{}]".format(current_task_key, current_path_node.scheduler_id, current_path_node.update_graph_id))

   # Will be set to True or False if necessary, otherwise this will just be ignored.
   need_to_download_become_path = False # Default value; doesn't mean anything really.
   
   # Serialize the resulting value.
   subsegment = xray_recorder.begin_subsegment("serializing-value")
   serialization_start = time.time()
   value_serialized = cloudpickle.dumps(value)
   serialization_end = time.time()
   lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
   current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)
   xray_recorder.end_subsegment()
   size = sys.getsizeof(value_serialized)
   result[VALUE_SERIALIZED_KEY] = value_serialized

   # We temporarily use chunk_task_threshold to determine if a task is big enough to execute its downstream tasks locally.
   execute_local_threshold = task_payload["big-task-threshold"]   
   next_nodes_for_processing = list()
   ready_to_invoke = list()
   current_data_written = node_processing.data_written  
   current_become_node = None
   become_is_ready = (not needs_become) # If 'needs_become' is True, then the become node is certainly not ready.
                                        # However, if 'needs_become' is False (i.e., we already have a 'become' node),
                                        # then the 'become' node IS ready, so we just initialize this to true.

   logger.debug("execute_local_threshold: " + str(execute_local_threshold))
   logger.debug("Size of serialized data for {}: {} bytes.".format(current_path_node.task_key, size))
   if size >= execute_local_threshold:
      next_nodes_for_processing = process_big(current_path_node, 
                                   result,
                                   nodes_map_serialized,
                                   nodes_map_deserialized,
                                   previous_results,
                                   tasks_to_fargate_nodes,
                                   use_bit_dep_checking = use_bit_dep_checking,
                                   out_edges_to_process = out_edges, 
                                   current_data_written = current_data_written,
                                   use_task_queue = use_task_queue,
                                   lambda_debug = lambda_debug,
                                   threshold = execute_local_threshold,
                                   lambda_execution_breakdown = lambda_execution_breakdown,
                                   current_task_execution_breakdown = current_task_execution_breakdown,
                                   task_execution_breakdowns = task_execution_breakdowns,
                                   context = context)
   else:
      if needs_become:
         # We do check to see if the type is list first though. If it is a list,
         # then that means it is still in its serialized form. If it isn't a list,
         # then we must've deseralized it earlier.
         if current_path_node.become in nodes_map_deserialized:
            current_become_node = nodes_map_deserialized[current_path_node.become]
            # Deserialize the payload so we can check its dependencies and all.
            if type(current_become_node.task_payload) is list:
               serialized_task_payload = current_become_node.task_payload
               frames = []
               for encoded in serialized_task_payload:
                  frames.append(base64.b64decode(encoded))
               
               deserialization_start = time.time()
               current_become_node.task_payload = deserialize_payload(frames)
               deserialization_end = time.time()
               lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
               current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
         else:
            current_become_node_encoded = nodes_map_serialized[current_path_node.become]

            deserialization_start = time.time()

            current_become_node_serialized = base64.b64decode(current_become_node_encoded)
            current_become_node = cloudpickle.loads(current_become_node_serialized)
            
            # Deserialize the payload so we can check its dependencies and all.
            if type(current_become_node.task_payload) is list:
               serialized_task_payload = current_become_node.task_payload
               frames = []
               for encoded in serialized_task_payload:
                  frames.append(base64.b64decode(encoded))
               current_become_node.task_payload = deserialize_payload(frames)
               
               # Now we're done with deserialization so just record metrics.
               deserialization_end = time.time()
               lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
               current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)               
            else:
               # We're done with deserialization so just record metrics.
               deserialization_end = time.time()
               lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
               current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)               
            nodes_map_deserialized[current_path_node.become] = current_become_node
         logger.debug("Checking if the pre-assigned become node {} (for task {}) is ready to execute. Not incrementing...".format(current_become_node.task_key, current_path_node.task_key))
         
         # Use one of the two methods for dependency counting.
         if use_bit_dep_checking == False:
            become_is_ready = check_dependency_counter(current_become_node, 
                                                      len(current_become_node.task_payload["dependencies"]), 
                                                      increment = False,
                                                      task_execution_breakdown = current_task_execution_breakdown, 
                                                      lambda_execution_breakdown = lambda_execution_breakdown)         
         else:
            become_is_ready = check_dependency_counter_bits(current_become_node, 
                                                      len(current_become_node.task_payload["dependencies"]), 
                                                      increment = False,
                                                      dependency_path_node = current_path_node,
                                                      task_execution_breakdown = current_task_execution_breakdown, 
                                                      lambda_execution_breakdown = lambda_execution_breakdown)
         if become_is_ready:
            logger.debug("Task {} is ready for execution and can be used as a become node for task {}! Removing it from out-edges before process_small()...".format(current_become_node.task_key, current_path_node.task_key))
            # If become_is_ready is true at this point, then it means 
            # the 'become' node is the one that was originally assigned 
            # and we therefore do not need to download anything.
            need_to_download_become_path = False

            # We're going to 'become' this node; we do not want to process (and invoke) it. 
            # Thus, we remove it from the out_edges list.
            out_edges.remove(current_become_node)

      ready_to_invoke, current_become_node, need_to_download_become_path, current_data_written = process_small(current_path_node, 
                                                                                                               become_is_ready, 
                                                                                                               current_become_node, 
                                                                                                               out_edges,
                                                                                                               needs_become,
                                                                                                               result,
                                                                                                               nodes_map_serialized,
                                                                                                               nodes_map_deserialized,
                                                                                                               lambda_debug = lambda_debug,
                                                                                                               use_bit_dep_checking = use_bit_dep_checking,
                                                                                                               current_data_written = current_data_written,
                                                                                                               lambda_execution_breakdown = lambda_execution_breakdown,
                                                                                                               current_task_execution_breakdown = current_task_execution_breakdown,
                                                                                                               task_execution_breakdowns = task_execution_breakdowns,
                                                                                                               context = context)                                                                                   
   
   # Keep track of the keys we're going to remove.
   keys_to_remove = list() 

   # Remove any entries in previous_results that are no longer needed.
   for key, val in previous_results.items():
      # Don't want to remove leaf task data since we'll need possibly it in next iteration.
      if key == leaf_key:
         logger.debug("[DEBUG - INFO] Skipping {} during processing of previous results as it was the leaf task for this Lambda...".format(key))
         continue 

      logger.debug("[DEBUG - INFO] Processing previous result for task {}".format(key))
      prev_node = None

      # Check if we have a PathNode object saved for the data entry. 
      # If we do -- but we haven't serialized it yet -- then deserialize it
      # and cache it in the nodes_map_deserialized object.
      if key in nodes_map_deserialized:
         prev_node = nodes_map_deserialized[key]
      elif key in nodes_map_serialized:
         prev_node_encoded = nodes_map_serialized[key]

         deserialization_start = time.time()

         prev_node_serialized = base64.b64decode(prev_node_encoded)
         prev_node = cloudpickle.loads(prev_node_serialized)
         
         # Make sure to deserialize the task payload as well.
         if type(prev_node.task_payload) is list:
            # Deserialize the payload so we can check its dependencies and all.
            serialized_task_payload = prev_node.task_payload
            frames = []
            for encoded in serialized_task_payload:
               frames.append(base64.b64decode(encoded))
            prev_node.task_payload = deserialize_payload(frames)

            # Now we're done with deserialization so just record metrics.
            deserialization_end = time.time()
            lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
            current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)                  
         else:
            # We're done with deserialization so just record metrics.
            deserialization_end = time.time()
            lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
            current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)            
         nodes_map_deserialized[key] = prev_node   
      
      # We're going to iterate over all of the tasks which require this data. First, we check if we've executed
      # the task locally on this Lambda. If we haven't, then we see if it exists in Redis. If we've neither
      # executed it locally nor does a value exist for the task in Redis, then we cannot uncache the data yet.
      if prev_node is not None:
         # This is a dict where each key is a downstream task of the task associated with 'prev_node'. 
         # We do not care about the value associated with each key in the dict (in this case).
         num_dependencies_of_dependents = prev_node.task_payload["num-dependencies-of-dependents"]
         logger.debug("Dependent tasks for task {}: {}".format(key, str(num_dependencies_of_dependents)))
         can_remove = True  
         for dependent_key in num_dependencies_of_dependents:
            # Executed locally?
            if dependent_key in executed_tasks:
               continue
            # Does a value exist for the task in Redis? 
            # Note that this will only catch tasks which were final results... so not exactly useful...
            elif dcp_redis.exists(dependent_key) != 0:
               continue 
            else:
               logger.debug("[INFO] Cannot delete previous result {} yet as at least one task ({}) is still incomplete.".format(key, dependent_key))
               can_remove = False
               break
         
         # Check if we can remove it.
         if can_remove == True:
            logger.debug("Will remove task {} [sid-{} uid-{}] from previous results.".format(prev_node.task_key, prev_node.scheduler_id, prev_node.update_graph_id))
            keys_to_remove.append(prev_node.task_key)
   
   lambda_execution_breakdown.fan_outs.append({
      "fan-out-task-key": current_path_node.task_key,
      "size": sys.getsizeof(previous_results[current_path_node.task_key]),
      "fan-out-factor": current_path_node.num_downstream_tasks()
   })

   # Remove whatever keys we're finished with.
   for prev_result_key in keys_to_remove:
      logger.debug("[INFO] Removing data generated by task {} from local cache.".format(prev_result_key))
      try:
         del previous_results[prev_result_key]
      except KeyError:
         logger.debug("[WARNING] Attempted to remove 'previous results' entry for task {}, but no such entry exists...".format(prev_result_key))

   # Invoke everything that is ready to be invoked.
   for node_that_can_execute in ready_to_invoke:
      # We're going to attempt to pack some data into this Lambda function invocation so that it doesn't need to go to Redis for the data.
      data_for_invocation = dict()
      # size in bytes of the dependencies we're sending 
      total_size = 0 
      total_size += sys.getsizeof(node_that_can_execute.starts_at + PATH_KEY_SUFFIX) # Account for the size of the path key...
      total_size += sys.getsizeof(node_that_can_execute.task_key)              # Account for the size of the first node that should be executed...
      total_size += sys.getsizeof(current_task_key)                            # Account for 'invoked by' field (for debugging)
      max_size = 256000
      for dep in node_that_can_execute.task_payload["dependencies"]:
         logger.debug("Checking if we have {} locally, and if so, can we send it to the new Lambda directly?".format(dep))
         if dep in previous_results:
            val = previous_results[dep]
            serialization_start = time.time()
            val_serialized = cloudpickle.dumps(val)
            val_encoded = base64.encodestring(val_serialized).decode('utf-8')
            serialization_end = time.time()
            lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
            current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)            
            val_size = sys.getsizeof(val_encoded)
            logger.debug("The value for {} is {} bytes after serialization and encoding.".format(dep, val_size))
            # If the size of the serialized-and-encoded value is greater than 256kB, then just skip it.
            if (val_size > max_size):
               continue
            size_remaining = max_size - total_size
            
            # If we have space remaining for the value, then we'll use it.
            if (val_size < size_remaining):
               logger.debug("We had {} bytes remaining, and since {} is {} bytes, we can send it directly.".format(size_remaining, dep, val_size))
               total_size += val_size
               data_for_invocation[dep] = val_encoded
      
      payload = {
         path_key_payload_key: node_that_can_execute.starts_at + PATH_KEY_SUFFIX, 
         previous_results_payload_key: data_for_invocation, 
         starting_node_payload_key: node_that_can_execute.task_key,
         invoked_by_payload_key: current_task_key,
         "use-fargate": use_fargate,
         "proxy_address": proxy_address,
         "executor_function_name": executor_function_name,
         "invoker_function_name": invoker_function_name
      }

      # Invoke the downstream task.
      logger.debug("[INVOCATION] - Invoking downstream task {} [sid-{} uid-{}].".format(node_that_can_execute.task_key, node_that_can_execute.scheduler_id, node_that_can_execute.update_graph_id))
      
      serialization_start = time.time()
      payload_serialized = ujson.dumps(payload)
      serialization_end = time.time()

      lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
      current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)      
      
      _start_invoke = time.time()
      lambda_client.invoke(FunctionName=executor_function_name, InvocationType='Event', Payload=payload_serialized)
      _end_invoke = time.time()
      _invoke_duration = _end_invoke - _start_invoke 
      lambda_execution_breakdown.invoking_downstream_tasks += _invoke_duration
      current_task_execution_breakdown.invoking_downstream_tasks += _invoke_duration

      invoke_event = WukongEvent(
         name = "Invoke Lambda",
         start_time = _start_invoke,
         end_time = _end_invoke,
         metadata = {
            "Duration (seconds)": _invoke_duration,
            "Downstream Task Key": node_that_can_execute.task_key
         }
      )
      lambda_execution_breakdown.add_event(invoke_event)

   logger.debug("Returning from 'process_out_edges()'")
   logger.debug("next_nodes_for_processing: " + str(next_nodes_for_processing))
   logger.debug("current_become_node: " + str(current_become_node))
   logger.debug("need_to_download_become_path: " + str(need_to_download_become_path))
   return next_nodes_for_processing, current_become_node, need_to_download_become_path

@xray_recorder.capture("process_downstream_tasks_for_small_output")
def process_small(current_path_node, 
                  become_is_ready, 
                  current_become_node, 
                  out_edges_to_process, 
                  needs_become, 
                  result,
                  nodes_map_serialized, 
                  nodes_map_deserialized,
                  use_bit_dep_checking = False,
                  current_data_written = False,
                  lambda_debug = False,
                  lambda_execution_breakdown = None, 
                  current_task_execution_breakdown = None,
                  task_execution_breakdowns = dict(),
                  context = None):
   """
      Process downstream tasks (i.e., out-edges) for the most recently executed task. 

      This function is used when the size of the most recently executed task's output data is smaller than a user-configured threshold. When the data is 
      greater-than-or-equal-to the user-defined threshold, the 'process_big' function is used.

      Arguments:
         current_path_node (PathNode):   This is the task we are currently processing.

         become_is_ready (bool): Indicates if the already-assigned 'become' node for the current task ready to execute

         current_become_node (PathNode): The pre-assigned/currently assigned 'become' node for current_path_node.

         out_edges_to_process (list):    List of PathNode objects; the downstream tasks from the currently-processing task/node.

         needs_become (bool): Indicates whether or not current_path_node needs a become node.

         result (dict): Result from calling process_task. Result of executing the task. 

         nodes_map_serialized (dict):    Map of TASK_KEY -> Serialized PathNode.

         nodes_map_deserialized (dict):  Map of TASK_KEY -> Deserialized PathNode.

         use_bit_dep_checking (bool): If True, use check_dependency_counter_bits for checking dependencies. If False, use check_dependency_counter.

         current_data_written (bool):    Flag indicating whether or not the current task's data has been written to Redis already.

         context: AWS Lambda context object (same that is passed to AWS Lambda handler at very beginning).
   """
   value_serialized = result[VALUE_SERIALIZED_KEY] 

   # If we're supposed to use the proxy to parallelize the invocation of downstream tasks, then do so...
   if current_path_node.use_proxy == True:
      logger.debug("Task {} is going to use the proxy...".format(current_path_node.task_key))

      # Some X-Ray diagnostics.
      subsegment = xray_recorder.begin_subsegment("store-redis-proxy")
      serialization_start = time.time()
      payload_for_proxy = ujson.dumps({OP_KEY: "set", 
                                       TASK_KEY: current_path_node.task_key, 
                                       FARGATE_PUBLIC_IP_KEY: current_path_node.fargate_node[FARGATE_PUBLIC_IP_KEY],
                                       #FARGATE_PRIVATE_IP_KEY: current_path_node.fargate_node[FARGATE_PRIVATE_IP_KEY],
                                       "value-encoded": base64.encodestring(value_serialized).decode('utf-8'),
                                       LAMBDA_ID_KEY: context.aws_request_id})
      serialization_end = time.time()

      lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
      current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

      size_of_payload = sys.getsizeof(payload_for_proxy)
      subsegment.put_annotation("size_payload_for_proxy", str(size_of_payload))

      # Grab the channel on which we shall publish the message.
      redis_proxy_channel = current_path_node.task_payload["proxy-channel"]
      
      publish_start = time.time()

      # Public the message.
      #dcp_redis.publish(redis_proxy_channel, payload_for_proxy)
      publish_dcp_message(redis_proxy_channel, payload_for_proxy, serialized = True)

      publish_stop = time.time()
      publish_duration = publish_stop - publish_start 
      current_task_execution_breakdown.publishing_messages += publish_duration
      lambda_execution_breakdown.publishing_messages += publish_duration

      xray_recorder.end_subsegment()

      if needs_become:
         if type(current_become_node.task_payload) is list:
            # Deserialize the payload so we can check its dependencies and all.
            serialized_task_payload = current_become_node.task_payload
            frames = []
            for encoded in serialized_task_payload:
               frames.append(base64.b64decode(encoded))
            deserialization_start = time.time()
            current_become_node.task_payload = deserialize_payload(frames)
            deserialization_end = time.time()
            lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
            current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
         logger.debug("Processing pre-assigned become node {}...".format(current_become_node.task_key))

         ready_to_execute = False
         
         # Use one of the two methods for dependency checking.
         if use_bit_dep_checking == False:
            ready_to_execute = check_dependency_counter(current_become_node, 
                                                        len(current_become_node.task_payload["dependencies"]), 
                                                        increment = True,
                                                        task_execution_breakdown = current_task_execution_breakdown,
                                                        lambda_execution_breakdown = lambda_execution_breakdown)
         else:
            ready_to_execute = check_dependency_counter_bits(current_become_node, 
                                                             len(current_become_node.task_payload["dependencies"]), 
                                                             dependency_path_node = current_path_node,
                                                             increment = True,
                                                             task_execution_breakdown = current_task_execution_breakdown,
                                                             lambda_execution_breakdown = lambda_execution_breakdown)

         # Since we're using the proxy, we just attempt to use the pre-assigned 'become' node. We check it here; if it's not ready, then we'll return 'None.'
         # The variables we're turning are: ready_to_invoke, current_become_node, need_to_download_become_path, current_data_written
         if ready_to_execute:
            return [], current_become_node, False, False

      return [], None, False, False
   
   # Not using the proxy...
   ready_to_invoke = list()
   downstream_tasks = current_path_node.get_downstream_tasks()
   logger.debug("Processing SMALL for current task {} [sid-{} uid-{}]. There are {} out-edges to process.".format(current_path_node.task_key, 
                                                                                                           current_path_node.scheduler_id,
                                                                                                           current_path_node.update_graph_id,
                                                                                                           len(out_edges_to_process)))
   logger.debug("The task node for {} [sid-{} uid-{}] indicates that it has {} downstream tasks. These tasks are: {}".format(current_path_node.task_key, 
                                                                                                                      current_path_node.scheduler_id,
                                                                                                                      current_path_node.update_graph_id,   
                                                                                                                      current_path_node.num_downstream_tasks(), 
                                                                                                                      downstream_tasks))

   # As long as there are at least two dependents OR the only dependent has multiple dependencies, we'll write our data.   
   if not current_data_written:
      logger.debug("Writing resulting data from execution of task {} [sid-{} uid-{}] to Redis".format(current_path_node.task_key, current_path_node.scheduler_id, current_path_node.update_graph_id))

      # Store the result in redis.
      subsegment = xray_recorder.begin_subsegment("store-redis-direct")
      obj_size = sys.getsizeof(value_serialized)
      subsegment.put_annotation("size_payload_redis_direct", str(obj_size))
      value_serialized_str = str(value_serialized)
      if len(value_serialized_str) > 350:
         value_serialized_str = value_serialized_str[0:349] + "..."
      logger.debug("[DEBUG] Writing serialized value {} (size: {} bytes) for task {} to Redis...".format(value_serialized_str, obj_size, current_path_node.task_key))
      store_value_in_redis(current_path_node, 
                           value_serialized, 
                           key = None, 
                           check_first = False, 
                           task_execution_breakdown = current_task_execution_breakdown, 
                           lambda_execution_breakdown = lambda_execution_breakdown)

   if lambda_debug:
      payload = {
         OP_KEY: EXECUTED_TASK_KEY,
         TASK_KEY: current_path_node.task_key,
         START_TIME_KEY: result[START_TIME_KEY],
         STOP_TIME_KEY: result[STOP_TIME_KEY],
         EXECUTION_TIME_KEY: result[EXECUTION_TIME_KEY],
         LAMBDA_ID_KEY: context.aws_request_id,
         DATA_SIZE: str(sys.getsizeof(result[VALUE_SERIALIZED_KEY])),
         'time_sent': time.time()
      }

      serialization_start = time.time()
      payload_serialized = ujson.dumps(payload)
      serialization_end = time.time()

      lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
      current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

      publish_start = time.time() 

      logger.debug("Sending Scheduler \"{}\" message via Redis Pub-Sub for task {} now.".format(EXECUTED_TASK_KEY, current_path_node.task_key))

      #dcp_redis.publish(REDIS_PUB_SUB_CHANNEL, payload_serialized)
      publish_dcp_message(REDIS_PUB_SUB_CHANNEL, payload_serialized, serialized = True)

      publish_stop = time.time()
      publish_duration = publish_stop - publish_start 
      current_task_execution_breakdown.publishing_messages += publish_duration
      lambda_execution_breakdown.publishing_messages += publish_duration 

   need_to_download_become_path = False # Default value is false bc if we enter this function with a become node already selected and ready to execute,
                                        # then that become node is going to be the pre-assigned one, meaning we won't need to download anything. If instead
                                        # we need to find a become node during this function's execution, we'll set this value to true if necessary. So it
                                        # should be false to begin with since either (1) we have a become node already, and we have all its data or (2) we
                                        # won't have any become node at all and thus we have nothing we'd need to download anyway.
   logger.debug("About to loop over 'out_edges_to_process'. There are {} nodes contained within 'out_edges_to_process' right now.".format(len(out_edges_to_process)))
   
   for out_edge_node in out_edges_to_process:
      logger.debug("Processing out_edge {} [sid-{} uid-{}]...".format(out_edge_node.task_key, out_edge_node.scheduler_id, out_edge_node.update_graph_id))
      # We do check to see if the type is list first though. If it is a list,
      # then that means it is still in its serialized form. If it isn't a list,
      # then we must've deseralized it earlier.
      if type(out_edge_node.task_payload) is list:
         # Deserialize the payload so we can check its dependencies and all.
         serialized_task_payload = out_edge_node.task_payload
         frames = []
         for encoded in serialized_task_payload:
            frames.append(base64.b64decode(encoded))
         deserialization_start = time.time()
         out_edge_node.task_payload = deserialize_payload(frames)
         deserialization_end = time.time()
         lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
         current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)

      ready_to_execute = False

      # Use one of the two methods for dependency checking.
      if use_bit_dep_checking == False:
         ready_to_execute = check_dependency_counter(out_edge_node, 
                                                     len(out_edge_node.task_payload["dependencies"]), 
                                                     increment = True, 
                                                     task_execution_breakdown = current_task_execution_breakdown, 
                                                     lambda_execution_breakdown = lambda_execution_breakdown)
      else:
         ready_to_execute = check_dependency_counter_bits(out_edge_node, 
                                                         len(out_edge_node.task_payload["dependencies"]), 
                                                         dependency_path_node = current_path_node,
                                                         increment = True, 
                                                         task_execution_breakdown = current_task_execution_breakdown, 
                                                         lambda_execution_breakdown = lambda_execution_breakdown)
      if ready_to_execute:
         # If we don't have a 'become' node yet, then
         # we'll use this node instead of invoking it. 
         if needs_become is True and become_is_ready is False:
            logger.debug("Since task {} [sid-{} uid-{}] was ready for execution AND we needed a 'become' node, we're going to use it!".format(out_edge_node.task_key, out_edge_node.scheduler_id, out_edge_node.update_graph_id))
            current_become_node = out_edge_node 
            become_is_ready = True 
            needs_become = False 
            
            # If we end up re-assigning our original 'become' 
            # node as our final, official 'become' node, then 
            # we actually have the path data and do not need 
            # to download it. Otherwise we will need to download 
            # the path data, so set the flag accordingly.
            if current_become_node.task_key == current_path_node.become:
               need_to_download_become_path = False 
            else:
               need_to_download_become_path = True 
         else: #need to make sure we don't try to invoke the 'become' node
            ready_to_invoke.append(out_edge_node)
      else:
         # It isn't ready. Ignore it -- move on.
         pass

   if become_is_ready:
      logger.debug("Returning from process_small() with ready become node: ")
      logger.debug("ready_to_invoke: " + str(ready_to_invoke))
      logger.debug("current_become_node: " + str(current_become_node))
      logger.debug("need_to_download_become_path: " + str(need_to_download_become_path) + ", current_data_written: " + str(current_data_written))
      return ready_to_invoke, current_become_node, need_to_download_become_path, current_data_written
   else:
      logger.debug("Returning from process_small() without ready become node: ")
      logger.debug("ready_to_invoke: " + str(ready_to_invoke))
      logger.debug("current_become_node: None")
      logger.debug("need_to_download_become_path: N/A, current_data_written: " + str(current_data_written))
      return ready_to_invoke, None, need_to_download_become_path, current_data_written

@xray_recorder.capture("process_downstream_tasks_for_BIG_output")
def process_big(current_path_node, 
                result, 
                nodes_map_serialized, 
                nodes_map_deserialized,
                previous_results,
                tasks_to_fargate_nodes,
                use_bit_dep_checking = False,
                out_edges_to_process = list(), 
                current_data_written = False, 
                threshold = 50000000, 
                use_task_queue = True,
                lambda_debug = True,
                lambda_execution_breakdown = None, 
                current_task_execution_breakdown = None,
                task_execution_breakdowns = dict(),
                context = None):
   """
      Process downstream tasks (i.e., out-edges) for the most recently executed task. 

      This function is used when the size of the most recently executed task's output data is greater-than-or-equal-to a user-configured threshold. When the data is 
      smaller than the user-defined threshold, the 'process_small' function is used.

      Args:
         current_path_node (PathNode):   This is the task we are currently processing.
         
         result (dict):                  Result from calling process_task. Result of executing the task. 

         nodes_map_serialized (dict):    Map of TASK_KEY -> Serialized PathNode.

         nodes_map_deserialized (dict):  Map of TASK_KEY -> Deserialized PathNode.

         previous_results (dict):        Map of TASK_KEY -> previously computed results. 

         tasks_to_fargate_nodes (dict):  Map of TASK_KEY -> Fargate IP address. 

         out_edges_to_process (list):    List of PathNode objects; the downstream tasks from the currently-processing task/node.

         current_data_written (bool):    Flag indicating whether or not the current task's data has been written to Redis already.

         context: AWS Lambda context object (same that is passed to AWS Lambda handler at very beginning).

         use_bit_dep_checking (bool): If True, use check_dependency_counter_bits for checking dependencies. If False, use check_dependency_counter.

         threshold (int):             Size (in bytes) to which task output data is compared against. If the data is >= this threshold, it is considered 'large' or 'big'.

         use_task_queue (bool):       If True, large objects will wait for downstream tasks to become ready instead of writing data.
   """
   value_serialized = result[VALUE_SERIALIZED_KEY]
   large_data_size = sys.getsizeof(result[VALUE_SERIALIZED_KEY])

   tasks_pulled_down = list()
   next_nodes_for_processing = []
   
   # Clearly this node is 'big', so update its 'is_big' field to reflect that.
   current_path_node.is_big = True 

   logger.debug("Processing BIG for current task {} [sid-{} uid-{}]".format(current_path_node.task_key,
                                                                     current_path_node.scheduler_id,
                                                                     current_path_node.update_graph_id))
   for out_edge in out_edges_to_process:
      # We do check to see if the type is list first though. If it is a list,
      # then that means it is still in its serialized form. If it isn't a list,
      # then we must've deseralized it earlier.
      if type(out_edge.task_payload) == list:
         # Deserialize the payload so we can check its dependencies and all.
         serialized_task_payload = out_edge.task_payload
         frames = []
         for encoded in serialized_task_payload:
            frames.append(base64.b64decode(encoded))
         deserialization_start = time.time()
         out_edge.task_payload = deserialize_payload(frames)
         deserialization_end = time.time()
         lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
         current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
      logger.debug("Processing out_edge {} [sid-{} uid-{}]...".format(out_edge.task_key, out_edge.scheduler_id, out_edge.update_graph_id))

      ready_to_execute = False 
      
      # Use one of the two methods for dependency checking.
      if use_bit_dep_checking == False:
         ready_to_execute = check_dependency_counter(out_edge, 
                                                     len(out_edge.task_payload["dependencies"]), 
                                                     increment = False,
                                                     task_execution_breakdown = current_task_execution_breakdown, 
                                                     lambda_execution_breakdown = lambda_execution_breakdown)
      else:    
         ready_to_execute = check_dependency_counter_bits(out_edge, 
                                                     len(out_edge.task_payload["dependencies"]), 
                                                     dependency_path_node = current_path_node,
                                                     increment = False,
                                                     task_execution_breakdown = current_task_execution_breakdown, 
                                                     lambda_execution_breakdown = lambda_execution_breakdown)

      # If it is ready to execute, then pull it down for local execution. 
      if ready_to_execute:
         tasks_pulled_down.append(out_edge)
         
         # Increment the dependecy counter associated with this task for book-keeping purposes, though no other task should look at it.
         # This will not result in the out_edge possibly being executed twice, as in order for 'ready_to_execute' to be True, the value 
         # of the dependency counter for out_edge had to have been equal to (out_edge.NUM_DEPENDENCIES - 1), meaning every other task
         # which might try to invoke/execute it has finished.
         key_counter = str(out_edge.task_key) + DEPENDENCY_COUNTER_SUFFIX
         
         # If we're using the bit-method, then we should toggle the bit, not increment.
         if use_bit_dep_checking == False:
            dcp_redis.incr(key_counter)
         else:
            offset = current_path_node.dep_index_map[out_edge.task_key]
            dcp_redis.setbit(key_counter, offset, 1)
      else:
         # In some cases, big tasks should not use the task queue. For example, in GEMM, tasks are often dependent on exclusively large tasks (and a large number
         # of them) so the current version of this strategy would not work correctly. For now, it is up to the user to determine when they should attempt to use the task queue.
         if use_task_queue:
            # Get the prefix for the current task key...
            current_prefix = key_split(current_path_node.task_key)

            # Grab the list of dependencies for the current downstream task.
            dependencies = list(out_edge.task_payload["dependencies"])

            # Remove the current task from this list. Leaving it in would cause the 
            # 'multiple_big_tasks_possible' flag to always be set to True.
            dependencies.remove(current_path_node.task_key)

            # Indicates whether or not one of the dependencies of the current out_edge we're processing
            # has the same prefix as the current task. If the prefix is the same, then the current out_edge
            # may have more than one large object dependency, in which case we should write the data and
            # increment the dependency counter.
            multiple_big_tasks_possible = False 

            # Check all of the dependencies to see if any share the same prefix as the current path node/current task.
            for dep_key in dependencies:
               if dep_key.startswith(current_prefix):
                  logger.debug("Task {} was not ready to execute and is possibly dependent on > 1 big tasks including {} and {}...".format(out_edge.task_key, current_path_node.task_key, dep_key))
                  multiple_big_tasks_possible = True 
                  break 
            
            # If none of the prefixes are the same, then enqueue the task for future processing. We'll keep
            # trying to execute it until it becomes ready for execution.
            if not multiple_big_tasks_possible:
               logger.debug("Task {} was not ready to execute, but has big task {} as a dependency. Will try again later.".format(out_edge.task_key, current_path_node.task_key))
               
               delayed_node = DelayedProcessingNode(out_edge, current_path_node, value_serialized)
               
               # Enqueue this out edge for future processing.
               task_queue.put(delayed_node)
               lambda_execution_breakdown.clustering_bytes_saved += large_data_size
               continue 

         logger.debug("Big task {} must write its data because downstream task {} is not ready to execute yet.".format(current_path_node.task_key, out_edge.task_key))
         # If we haven't already written our data to Redis yet,
         # then first write the data, then increment the dependency
         # counter. Make sure to do one last check to see if the task
         # is ready to execute, though.
         if current_data_written is False:        

            # Store the result in redis.
            subsegment = xray_recorder.begin_subsegment("store-redis-direct")
            subsegment.put_annotation("size_payload_redis_direct", str(sys.getsizeof(value_serialized)))
            #associated_redis_hostname = hash_ring.get_node_hostname(current_path_node.task_key)
            logger.debug("Writing data for big task {} to Redis".format(current_path_node.task_key))
            #success = hash_ring[current_path_node.task_key].set(current_path_node.task_key, value_serialized)
            store_value_in_redis(current_path_node, 
                                 value_serialized, 
                                 key = None, 
                                 check_first = False, 
                                 serialized = True,
                                 task_execution_breakdown = current_task_execution_breakdown,
                                 lambda_execution_breakdown = lambda_execution_breakdown)          
            current_data_written = True 

            payload = {
               OP_KEY: EXECUTED_TASK_KEY,
               TASK_KEY: current_path_node.task_key,
               START_TIME_KEY: result[START_TIME_KEY],
               STOP_TIME_KEY: result[STOP_TIME_KEY],
               EXECUTION_TIME_KEY: result[EXECUTION_TIME_KEY],
               LAMBDA_ID_KEY: context.aws_request_id,
               DATA_SIZE: str(sys.getsizeof(result[VALUE_SERIALIZED_KEY])),
               'time_sent': time.time(),
               "data-available-now": True
            }

            serialization_start = time.time()
            payload_serialized = ujson.dumps(payload)
            serialization_end = time.time()

            lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
            current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

            publish_start = time.time() 

            #dcp_redis.publish(REDIS_PUB_SUB_CHANNEL, payload_serialized)
            publish_dcp_message(REDIS_PUB_SUB_CHANNEL, payload_serialized, serialized = True)

            publish_stop = time.time()
            publish_duration = publish_stop - publish_start 
            current_task_execution_breakdown.publishing_messages += publish_duration
            lambda_execution_breakdown.publishing_messages += publish_duration 

            # Check if the downstream task (out-edge) is ready to execute again...
            # Use one of the two methods for dependency checking.
            if use_bit_dep_checking == False:
               ready_to_execute = check_dependency_counter(out_edge, 
                                                         len(out_edge.task_payload["dependencies"]), 
                                                         increment = True,
                                                         task_execution_breakdown = current_task_execution_breakdown, 
                                                         lambda_execution_breakdown = lambda_execution_breakdown)
            else:    
               ready_to_execute = check_dependency_counter_bits(out_edge, 
                                                         len(out_edge.task_payload["dependencies"]), 
                                                         dependency_path_node = current_path_node,
                                                         increment = True,
                                                         task_execution_breakdown = current_task_execution_breakdown, 
                                                         lambda_execution_breakdown = lambda_execution_breakdown)                                                           
            
            # If it's now ready, then that means the other tasks finished in the time we were writing our data. We need to execute the task locally then.      
            if ready_to_execute:
               tasks_pulled_down.append(out_edge)          
         else:
            # We have to increment now. We may as well check again just in case it has since finished. (We _need_ to check again, since if it has since 
            # finished, we are technically last as far as the Wukong is concerned, and thus we need to execute the task.)
            # Use one of the two methods for dependency checking.
            if use_bit_dep_checking == False:
               ready_to_execute = check_dependency_counter(out_edge, 
                                                         len(out_edge.task_payload["dependencies"]), 
                                                         increment = True,
                                                         task_execution_breakdown = current_task_execution_breakdown, 
                                                         lambda_execution_breakdown = lambda_execution_breakdown)
            else:    
               ready_to_execute = check_dependency_counter_bits(out_edge, 
                                                         len(out_edge.task_payload["dependencies"]), 
                                                         dependency_path_node = current_path_node,
                                                         increment = True,
                                                         task_execution_breakdown = current_task_execution_breakdown, 
                                                         lambda_execution_breakdown = lambda_execution_breakdown) 

            # If it's ready now, then pull it down for local execution. The other tasks finished executing between when we last checked and now, so we need
            # to execute this task locally now, or else no other tasks will execute it. 
            if ready_to_execute:
               tasks_pulled_down.append(out_edge)         

   payload = {
      OP_KEY: EXECUTED_TASK_KEY,
      TASK_KEY: current_path_node.task_key,
      START_TIME_KEY: result[START_TIME_KEY],
      STOP_TIME_KEY: result[STOP_TIME_KEY],
      EXECUTION_TIME_KEY: result[EXECUTION_TIME_KEY],
      LAMBDA_ID_KEY: context.aws_request_id,
      DATA_SIZE: str(large_data_size),
      'time_sent': time.time(),
      "data-available-now": False 
   }

   serialization_start = time.time()
   payload_serialized = ujson.dumps(payload)
   serialization_end = time.time()

   lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
   current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

   publish_start = time.time() 

   #dcp_redis.publish(REDIS_PUB_SUB_CHANNEL, payload_serialized)
   publish_dcp_message(REDIS_PUB_SUB_CHANNEL, payload_serialized, serialized = True)

   publish_stop = time.time()
   publish_duration = publish_stop - publish_start 
   current_task_execution_breakdown.publishing_messages += publish_duration
   lambda_execution_breakdown.publishing_messages += publish_duration 

   for task_node in tasks_pulled_down:
      logger.debug("[sid-{} uid-{}] Executing task {} locally...".format(task_node.scheduler_id, task_node.update_graph_id, task_node.task_key))
      pulled_down_task_breakdown = TaskExecutionBreakdown(task_node.task_key, update_graph_id = task_node.update_graph_id)
      pulled_down_task_breakdown.task_processing_start_time = time.time()
      task_node.task_breakdown = pulled_down_task_breakdown

      task_execution_breakdowns[task_node.task_key] = pulled_down_task_breakdown

      # Execute the task locally.
      result = process_task(task_node.task_payload, 
                            task_node.task_key, 
                            previous_results, 
                            tasks_to_fargate_nodes,
                            current_scheduler_id = task_node.scheduler_id,
                            current_update_graph_id = task_node.update_graph_id,                            
                            lambda_debug = lambda_debug,
                            lambda_execution_breakdown = lambda_execution_breakdown, 
                            current_task_execution_breakdown = pulled_down_task_breakdown,
                            context = context)
      # logger.debug("Result of local execution of {}: {}".format(task_node.task_key, result.__str__()))
      logger.debug("Finished local execution of task {}".format(task_node.task_key))
      value = None
      if "result" in result:
         value = result["result"]
      
      previous_results[task_node.task_key] = value
      logger.debug("Number of downstream tasks for {} (which we just executed locally): {}".format(task_node.task_key, task_node.num_downstream_tasks()))

      lambda_execution_breakdown.number_of_tasks_executed += 1
      lambda_execution_breakdown.tasks_pulled_down += 1
      # Saved reading it. Count for every task we pull down.
      lambda_execution_breakdown.clustering_bytes_saved += large_data_size

      # If we just processed a root node, then we need to send a message to the Scheduler via PubSub.
      if task_node.num_downstream_tasks() == 0:
         # Serialize the resulting value.
         subsegment = xray_recorder.begin_subsegment("serializing-value")
         serialization_start = time.time()
         value_serialized = cloudpickle.dumps(value)
         serialization_end = time.time()
         xray_recorder.end_subsegment()
         lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
         current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)         

         logger.debug("[PROCESSING] - There are no downstream tasks which depend upon task {}.".format(task_node.task_key))
         logger.debug("Become for {}: {}".format(task_node.task_key, task_node.become))
         logger.debug("Invoke for {}: {}".format(task_node.task_key, task_node.invoke))
         logger.debug("Final result: " + str(value))
         logger.debug("[MESSAGING] - Publishing '{}' message to Redis Channel: {}.".format(LAMBDA_RESULT_KEY, REDIS_PUB_SUB_CHANNEL))
         payload = {
            OP_KEY: LAMBDA_RESULT_KEY,
            TASK_KEY: task_node.task_key,
            EXECUTION_TIME_KEY: result[EXECUTION_TIME_KEY],
            LAMBDA_ID_KEY: context.aws_request_id,
            'time_sent': time.time()
         }
         
         write_start = time.time()
         # By convention, store final results in the big node cluster.
         dcp_redis.set(task_node.task_key, value_serialized)         
         write_stop = time.time() 

         write_duration = write_stop - write_start 
         write_size = sys.getsizeof(value_serialized)

         write_event = WukongEvent(
            name = "Store Intermediate Data in EC2 Redis",
            start_time = write_start,
            end_time = write_stop,
            metadata = {
               "Duration (seconds)": write_duration,
               "Size (bytes)": write_size,
               "Task Key": task_node.task_key
            }
         )
         lambda_execution_breakdown.add_event(write_event)

         serialization_start = time.time()
         payload_serialized = ujson.dumps(payload)
         serialization_end = time.time()

         lambda_execution_breakdown.redis_write_time += write_duration
         lambda_execution_breakdown.bytes_written += write_size 
         lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)

         current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)         
         current_task_execution_breakdown.redis_write_time += write_duration

         publish_start = time.time() 

         #dcp_redis.publish(REDIS_PUB_SUB_CHANNEL, payload_serialized) 
         publish_dcp_message(REDIS_PUB_SUB_CHANNEL, payload_serialized, serialized = True)

         publish_stop = time.time()
         publish_duration = publish_stop - publish_start 
         current_task_execution_breakdown.publishing_messages += publish_duration
         lambda_execution_breakdown.publishing_messages += publish_duration         
      else:
         unprocessed = UnprocessedNode(task_node, result)
         next_nodes_for_processing.append(unprocessed)
   
   # If we didn't write our data, then note that we saved bytes there.
   if (current_data_written == False):
      lambda_execution_breakdown.clustering_bytes_saved += large_data_size
   
   return next_nodes_for_processing

@xray_recorder.capture("process_task")
def process_task(task_definition, 
                 task_key, 
                 previous_results, 
                 task_to_fargate_mapping,
                 lambda_debug = False, 
                 current_scheduler_id = -1, 
                 current_update_graph_id = -1,
                 lambda_execution_breakdown = None, 
                 current_task_execution_breakdown = None,
                 context = None):
   """ This function is responsible for executing the current task.
       
       It is responsible for retrieving and processing (i.e., deserializing) the dependencies for the current task.

      Arguments:
         task_definition (dict): Contains all of the information about the task. In particular, it contains the  task's code, arguments, etc.
      
         task_key (string): Unique identifier for the task.

         previous_results (dict): Mapping of task_key -> data. This contains the output of tasks previously executed on this Task Exector.

         task_to_fargate_mapping (dict): Mapping of task_key -> fargate IP address. Used for determining where to store/retrieve task data remotely.

         lambda_debug (bool): Controls collecting and printing of certain debug info.

         current_scheduler_id (str): Used for debugging; each time you run Wukong, the scheduler randomly 
                                     generates an ID. We use this for associated Lambdas with particular 
                                     executions of Wukong (in case you start and stop the program a lot). 
                                     Makes parsing the logs easier.

         current_update_graph_id (str): Similar to 'current_scheduler_id', but is generated for each individual job/workload.
   """

   # Grab the key associated with this task and use it to store the task's result in Elasticache.
   key = task_key or task_definition['key']   

   # See if we've already executed the task (i.e., it could be a leaf task from a previous iteration).
   if key in previous_results:
      start = time.time()
      logger.debug("[EXECUTION] Task {} [sid-{} uid-{}] was found in previous results. Returning cached value.".format(key, current_scheduler_id, current_update_graph_id))
      
      stop = time.time()

      msg = {
         OP_KEY: RETRIEVED_FROM_PREVIOUS_RESULTS_KEY,
         "status": "OK",
         "result": previous_results[key],
         "key": key,
         START_TIME_KEY: start, 
         STOP_TIME_KEY: stop,
         EXECUTION_TIME_KEY: stop - start
      }  

      return msg

   logger.debug("[EXECUTION] Executing task {} [sid-{} uid-{}].".format(key, current_scheduler_id, current_update_graph_id))
   args_serialized = None 
   func_serialized = None
   kwargs_serialized = None
   if "function" in task_definition:
      func_serialized = task_definition["function"]
   if "args" in task_definition:
      args_serialized = task_definition["args"]
   task_serialized = no_value
   if "task" in task_definition:
      task_serialized = task_definition["task"]
   if "kwargs" in task_definition:
      kwargs_serialized = task_definition["kwargs"]

   # Deserialize the code, arguments, and key-word arguments for the task.
   func, args, kwargs = _deserialize(func_serialized, args_serialized, kwargs_serialized, task_serialized)

   subsegment = xray_recorder.begin_subsegment("getting-dependencies-from-redis")
   
   # List of keys of tasks whose data is needed in order to execute the current task.
   dependencies = task_definition["dependencies"]
   data = {}
   logger.debug("[PREP] {} dependencies required.".format(len(dependencies)))
   logger.debug("Dependencies Required: " + str(dependencies))

   responses = dict()
   
   # Initialize this variable as we're going to use it shortly.
   time_spent_retrieving_dependencies_from_redis = 0
   num_read = 0
   start_reads = time.time()
   aggregate_size = 0

   # We were using transactions/mget for this, but most tasks do not have a large number of dependencies
   # so mget() probably wasn't helping much. Additionally, the keys are hashed evenly across all of the 
   # shards so they'll mostly be spread out evenly, further decreasing the effectiveness of mget().
   for dep in dependencies:
      if dep in previous_results:
         continue
      read_start = time.time() 
      val = get_data_from_redis(task_to_fargate_mapping, 
                                path_node = None, 
                                key = dep, 
                                current_scheduler_id = current_scheduler_id,
                                task_execution_breakdown = current_task_execution_breakdown, 
                                lambda_execution_breakdown = lambda_execution_breakdown)
      num_read += 1
      aggregate_size += sys.getsizeof(val)

      # Increment the aggregate total of the dependency retrievals.
      time_spent_retrieving_dependencies_from_redis += time.time() - read_start
      responses[dep] = val
    
   # And update the Redis read metric.
   lambda_execution_breakdown.redis_read_time += time_spent_retrieving_dependencies_from_redis
   current_task_execution_breakdown.redis_read_time += time_spent_retrieving_dependencies_from_redis

   reads_end = time.time()

   read_dependencies_event = WukongEvent(
      name = "Read Dependencies from Redis",
      start_time = start_reads,
      end_time = reads_end,
      metadata = {
         "Duration (seconds)": reads_end - start_reads,
         "Number of Dependencies Read": num_read,
         "Aggregate Size (bytes)": aggregate_size
      }
   )
   lambda_execution_breakdown.add_event(read_dependencies_event)

   _process_deps_start = time.time()

   xray_recorder.end_subsegment()
   subsegment = xray_recorder.begin_subsegment("processing-dependencies")
   subsegment.put_annotation("num_dependencies", str(len(dependencies)))

   # Add the previous results to the data dictionary.
   logger.debug("[PREP] Appending previous_results to current data list.")
   printable_prev_res = list()
   for previous_result_key, previous_result_value in previous_results.items():
      if previous_result_value is not None:
         printable_prev_res.append(previous_result_key + ": Non-None value")
      else:
         printable_prev_res.append(previous_result_key + ": None")
   logger.debug("Contents of previous_results: " + str(printable_prev_res))
   data.update(previous_results)
   size_of_deps = 0

   # Keep track of any chunked data that we need to de-chunk.
   chunked_data = dict()

   for dependency_task_key, serialized_dependency_data in responses.items():
      # TO-DO: Handle missing dependencies?
      # Make multiple attempts to retrieve missing dependency from Redis in case we've been timing out.
      if serialized_dependency_data is None:
         logger.debug("[ {} ] [ERROR] Dependency {} is None... Failed to retrieve dependency {} from Redis. Exiting.".format(datetime.datetime.utcnow(), dependency_task_key, dependency_task_key))
         return {
            OP_KEY: TASK_ERRED_KEY,   
            "statusCode": 400,
            "exception": "dependency {} is None".format(dependency_task_key),
            "body": "dependency {} is None".format(dependency_task_key)
         } 
      deserialization_start = time.time()
      deserialized_data = cloudpickle.loads(serialized_dependency_data)
      deserialization_end = time.time()
      lambda_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)
      current_task_execution_breakdown.deserialization_time = (deserialization_end - deserialization_start)      
      
      data[dependency_task_key] = deserialized_data
      # Keep a local record of the value obtained from Redis.
      previous_results[dependency_task_key] = deserialized_data
   
   # Record debug info/metrics concerning sizes of fan-in task data.
   if lambda_debug:
      fan_in_data = dict()
      fan_in_data['dependency-sizes'] = dict()
      if lambda_debug:
         for dependency in dependencies:
            data_size = sys.getsizeof(data[dependency])
            size_of_deps = size_of_deps + data_size
            fan_in_data['dependency-sizes'][dependency] = data_size        
   
      fan_in_data['fan-in-task'] = task_key
      fan_in_data['fan-in-factor'] = len(dependencies)
      lambda_execution_breakdown.fan_ins.append(fan_in_data)

   subsegment.put_annotation("size_of_deps", str(size_of_deps))
   xray_recorder.end_subsegment()
   subsegment = xray_recorder.begin_subsegment("pack_data")
   args2 = pack_data(args, data, key_types=(bytes, unicode))
   kwargs2 = pack_data(kwargs, data, key_types=(bytes, unicode))
   
   # Update dependency processing metric. 
   current_task_execution_breakdown.dependency_processing = (time.time() - _process_deps_start)
   xray_recorder.end_subsegment()

   args_str = str(args)
   kwargs_str = str(kwargs)
   if len(args_str) > 50:
      args_str = args_str[0:50] + "..."
   if len(kwargs_str) > 50:
      kwargs_str = kwargs_str[0:50] + "..."
   logger.debug("Applying function {} with arguments {}, kwargs {} (key is {})".format(func, args_str, kwargs_str, key))
   
   # If Lambda Debugging is enabled, send a message to the Scheduler indicating that we're now executing the task.
   if lambda_debug:
      payload = {
         OP_KEY: EXECUTING_TASK_KEY,
         TASK_KEY: key,
         LAMBDA_ID_KEY: context.aws_request_id,
         'time_sent': time.time()
      }

      payload_serialized = ujson.dumps(payload)

      publish_start = time.time()

      #dcp_redis.publish(REDIS_PUB_SUB_CHANNEL, payload_serialized)
      publish_dcp_message(REDIS_PUB_SUB_CHANNEL, payload_serialized, serialized = True)

      publish_stop = time.time()
      publish_duration = publish_stop - publish_start 
      current_task_execution_breakdown.publishing_messages += publish_duration
      lambda_execution_breakdown.publishing_messages += publish_duration      

   function_start_time = time.time()
   result = apply_function(func, args2, kwargs2, key)
   function_end_time = time.time()
   execution_time = function_end_time - function_start_time  
   result[EXECUTION_TIME_KEY] = execution_time
   result[START_TIME_KEY] = function_start_time
   result[STOP_TIME_KEY] = function_end_time

   # Create an event for executing the task.
   execute_event = WukongEvent(
      name = "Execute Task",
      start_time = function_start_time,
      end_time = function_end_time,
      metadata = {
         "ExecutedTaskID": key, 
         "# Tasks Executed": lambda_execution_breakdown.number_of_tasks_executed, 
         "Aggregate Time (sec) Executing Tasks": lambda_execution_breakdown.execution_time,
         "Duration": function_end_time - function_start_time
      }
   )
   lambda_execution_breakdown.add_event(execute_event)
 
   if result[OP_KEY] == EXECUTED_TASK_KEY:
      # The task executed successfully so store its task_definition in executed_tasks to indicate this.
      executed_tasks[key] = task_definition

   # If the Lambda's execution resulted in an error, then we want to inform the Scheduler of this regardless of whether or not debugging is enabled.
   elif result[OP_KEY] == TASK_ERRED_KEY:
      payload = {
         OP_KEY: TASK_ERRED_KEY,
         TASK_KEY: key,
         START_TIME_KEY: function_start_time,
         LAMBDA_ID_KEY: context.aws_request_id,
         "actual-exception": str(result["actual-exception"]),
         'time_sent': time.time()
      }

      serialization_start = time.time()
      payload_serialized = ujson.dumps(payload)
      serialization_end = time.time()

      lambda_execution_breakdown.serialization_time = (serialization_end - serialization_start)
      current_task_execution_breakdown.serialization_time = (serialization_end - serialization_start)

      publish_start = time.time() 

      #dcp_redis.publish(REDIS_PUB_SUB_CHANNEL, payload_serialized)
      publish_dcp_message(REDIS_PUB_SUB_CHANNEL, payload_serialized, serialized = True)

      publish_stop = time.time()
      publish_duration = publish_stop - publish_start 
      current_task_execution_breakdown.publishing_messages += publish_duration
      lambda_execution_breakdown.publishing_messages += publish_duration          

   # Update execution-related metrics.
   current_task_execution_breakdown.task_execution_start_time = function_start_time
   current_task_execution_breakdown.task_execution_end_time = function_end_time
   current_task_execution_breakdown.task_execution = execution_time

   lambda_execution_breakdown.execution_time += execution_time

   return result

@xray_recorder.capture("process_enqueued_tasks")
def process_enqueued_tasks(previous_results, tasks_to_fargate_nodes, nodes_to_process, use_bit_dep_checking = False, lambda_debug = True, aws_context = None, lambda_execution_breakdown = None):
   """When a big task finds it has a dependency that is not ready for execution, the big task will enqueue that task in the Lambda function's
      task_queue variable. The Lambda function will then periodically attempt to execute tasks in this queue in the interest of not having
      to write the big task's data to Redis (and similarly the downstream tasks won't have to read it). This function iterates over all of
      the enqueued tasks and attempts to execute them. If a task is still not ready for execution, then it is simply enqueued again.

      Args:
         previous_results (dict): Local cache of intermediate output data from tasks executed on this Lambda function.

         tasks_to_fargate_nodes (dict): A map of TASK-KEY --> FARGATE NODE IP's.

         nodes_to_process (list): List of UnprocessedNodes that need processing (checking dependencies, invoking, executing locally, etc.)

         use_bit_dep_checking (bool): If True, use check_dependency_counter_bits for checking dependencies. If False, use check_dependency_counter.

         aws_context (Context): AWS Lambda Context object.

         lambda_execution_breakdown (LambdaExecutionBreakdown): Metric data for the Lambda function itself.

      Returns: 
         A dictionary with two entries. The first entry contains PathNode objects of tasks we were able to execute.
         The second entry contains PathNode objects of tasks that were still not ready for execution.
         {
            "executed-tasks": [PathNode, PathNode, ..., PathNode],
            "still-not-ready": [PathNode, PathNode, ..., PathNode]
         }
   """
   if task_queue.qsize() > 0:
      logger.debug("There are {} tasks in the task_queue variable. Processing them now...".format(task_queue.qsize()))
      still_not_ready = list()   # Tasks that are still not ready to execute yet.
      ready_tasks = list()       # Tasks that are now ready to execute.

      # Iterate over all of the tasks in the queue. For tasks that still aren't ready, we'll just re-enqueue them at the end.
      while task_queue.qsize() > 0:
         delayed_node = task_queue.get_nowait()
         path_node = delayed_node.path_node 

         task_execution_breakdown = None

         # Check if the task as a TaskExecutionBreakdown object associated with it.
         if (hasattr(path_node, "task_breakdown") and path_node.task_breakdown != None):
            task_execution_breakdown = path_node.task_breakdown
         else:
            task_execution_breakdown = TaskExecutionBreakdown(path_node.task_key, update_graph_id = path_node.update_graph_id)
            _start_for_current_task = time.time()
            task_execution_breakdown.task_processing_start_time = _start_for_current_task
            path_node.task_breakdown = task_execution_breakdown

         ready_to_execute = False
         
         if use_bit_dep_checking == False:
            ready_to_execute = check_dependency_counter(path_node, 
                                                      len(path_node.task_payload["dependencies"]), 
                                                      # Do NOT increment. We haven't written the big task data needed by this task so we can't increment the associated counter.
                                                      increment = False, 
                                                      task_execution_breakdown = task_execution_breakdown,
                                                      lambda_execution_breakdown = lambda_execution_breakdown)         
         else: 
            ready_to_execute = check_dependency_counter_bits(path_node, 
                                                         len(path_node.task_payload["dependencies"]), 
                                                         # Do NOT increment. We haven't written the big task data needed by this task so we can't increment the associated counter.
                                                         increment = False, 
                                                         dependency_path_node = delayed_node.large_node,
                                                         task_execution_breakdown = task_execution_breakdown,
                                                         lambda_execution_breakdown = lambda_execution_breakdown) 
         
         if ready_to_execute:
            logger.debug("Downstream task {} IS ready to execute!".format(path_node.task_key))
            ready_tasks.append(delayed_node)

            # Increment the dependecy counter associated with this task for book-keeping purposes, though no other task should look at it.
            # This will not result in the out_edge possibly being executed twice, as in order for 'ready_to_execute' to be True, the value 
            # of the dependency counter for out_edge had to have been equal to (out_edge.NUM_DEPENDENCIES - 1), meaning every other task
            # which might try to invoke/execute it has finished.
            key_counter = str(path_node.task_key) + DEPENDENCY_COUNTER_SUFFIX   

            # If we're using the bit-method, then we should toggle the bit, not increment.
            if use_bit_dep_checking == False:
               dcp_redis.incr(key_counter)
            else:
               offset = delayed_node.large_node.dep_index_map[path_node.task_key]
               dcp_redis.setbit(key_counter, offset, 1) 
         else:
            logger.debug("Downstream task {} is still not ready to execute. Will put it back into the task_queue.".format(path_node.task_key))
            still_not_ready.append(delayed_node)
      
      # Enqueue all of the tasks that were still not ready.
      for delayed_node in still_not_ready:
         task_queue.put(delayed_node)
      
      # Execute each task that was ready to execute.
      for ready_delayed_node in ready_tasks:
         ready_task = ready_delayed_node.path_node 
         logger.debug("[sid-{} uid-{}] Processing now-ready task {}".format(ready_task.scheduler_id, ready_task.update_graph_id, ready_task.task_key))
         process_task_start = time.time()

         # Process the task via the process_task method. Pass in the 
         # local results variable for the "previous_results" keyword argument.
         result = process_task(ready_task.task_payload, 
                                 ready_task.task_key, 
                                 previous_results, 
                                 tasks_to_fargate_nodes,
                                 current_scheduler_id = ready_task.scheduler_id,
                                 current_update_graph_id = ready_task.update_graph_id,
                                 lambda_debug = lambda_debug, 
                                 lambda_execution_breakdown = lambda_execution_breakdown, 
                                 current_task_execution_breakdown = ready_task.task_breakdown,
                                 context = aws_context)
         lambda_execution_breakdown.process_task_time += (time.time() - process_task_start)

         data_written = False 
         task_key = ready_task.task_key
         # If the task erred, just return... 
         if result[OP_KEY] == TASK_ERRED_KEY:
            return {
               OP_KEY: TASK_ERRED_KEY,
               'statusCode': 400,
               'key': ready_task.task_key,
               'body': ujson.dumps(result["exception"])
            }  
         elif result[OP_KEY] == EXECUTED_TASK_KEY:
            logger.debug("[EXECUTION] Result of task {} computed in {} seconds.".format(task_key, result[EXECUTION_TIME_KEY]))
            value = result["result"]                   
            data_written = False

            # Cache the result locally.
            previous_results[ready_task.task_key] = value 
         elif result[OP_KEY] == RETRIEVED_FROM_PREVIOUS_RESULTS_KEY:
            logger.debug("[RETRIEVAL] Result of task {} retrieved from local cache.".format(task_key))
            value = result["result"]                   
            data_written = True                 
         else:
            raise ValueError("Unknown Operation from process_task: {}".format(result[OP_KEY]))

         # Add the node to the nodes_to_process list so we can process it.
         current_path_node_unprocessed = UnprocessedNode(ready_task, result, data_written = data_written)
         nodes_to_process.put_nowait(current_path_node_unprocessed)   
      return {
         OP_KEY: 'okay',
         EXECUTED_TASKS_KEY: ready_tasks,
         STILL_NOT_READY_KEY: still_not_ready
      }      
   else:
      logger.debug("There were no tasks in the task_queue variable.")
      return {
         OP_KEY: 'okay',
         EXECUTED_TASKS_KEY: [],
         STILL_NOT_READY_KEY: []
      }

def process_enqueued_tasks_looped(previous_results, 
        tasks_to_fargate_nodes, 
        nodes_to_process, 
        sleep = 1.5,
        max_tries = 20,
        use_bit_dep_checking = False,
        aws_context = None, 
        lambda_debug = True,
        lambda_execution_breakdown = None):
   """
      Calls process_enqueued_tasks() on a loop. Breaks out of loop and returns if at least one task is executed successfully.

      Args:
         previous_results (dict): Local cache of intermediate output data from tasks executed on this Lambda function.

         tasks_to_fargate_nodes (dict): A map of TASK-KEY --> FARGATE NODE IP's.

         nodes_to_process (list): List of UnprocessedNodes that need processing (checking dependencies, invoking, executing locally, etc.)

         aws_context (Context): AWS Lambda Context object.

         use_bit_dep_checking (bool): If True, use check_dependency_counter_bits for checking dependencies. If False, use check_dependency_counter.

         lambda_execution_breakdown (LambdaExecutionBreakdown): Metric data for the Lambda function itself.

         sleep (float): Base interval for sleeping in between attempts at executing tasks.

         max_tries (int): The number of times this will try to execute tasks before giving up and returning "empty-handed".

      Returns: 
         A dictionary with two entries. The first entry contains PathNode objects of tasks we were able to execute.
         The second entry contains PathNode objects of tasks that were still not ready for execution.
         {
            "executed-tasks": [PathNode, PathNode, ..., PathNode],
            "still-not-ready": [PathNode, PathNode, ..., PathNode]
         }
   """        
   # Check if there are still some tasks we need to process (dependents of big tasks we executed).
   if task_queue.qsize() > 0:
      logger.debug("There are {} tasks remaining in the task queue. Will try to execute those for a bit.".format(task_queue.qsize()))
      #max_tries = 20
      #sleep = 1.5
      num_tries = 1 

      # Keep trying for a bit. Sleep in between tries for increasing intervals.
      while task_queue.qsize() > 0 and num_tries <= max_tries:
         res = process_enqueued_tasks(previous_results, tasks_to_fargate_nodes, nodes_to_process, 
                                 aws_context = aws_context, lambda_debug = lambda_debug, use_bit_dep_checking = use_bit_dep_checking,
                                 lambda_execution_breakdown = lambda_execution_breakdown)
         
         # If we ended up processing all the remaining tasks, then break out of the loop before we sleep.
         if (task_queue.qsize() == 0):
            logger.debug("All tasks in the task queue have been processed!")
            return res 

         # If we executed something, then break out and process the out-edges for whatever we processed!
         if len(res[EXECUTED_TASKS_KEY]) > 0:
            logger.debug("Executed {} dependents of big tasks that weren't initially ready! (There are still {} tasks remaining in the queue.)".format(len(res[EXECUTED_TASKS_KEY]),
                                                                                                                                                len(res[STILL_NOT_READY_KEY])))
            return res  

         sleep_time = min(10, sleep * num_tries) # cap at 10 seconds

         logger.debug("There are still {} tasks remaining in the queue. Sleeping for {} seconds (try #{}).".format(task_queue.qsize(), sleep_time, num_tries))
         time.sleep(sleep_time)
         num_tries = num_tries + 1

      # If there are STILL tasks left, then we'll write their data to Redis and increment their dependency counters.
      # TODO: What if task exits before we have time to write the data?
      # TODO: What if task exits before downstream tasks can execute?
      if task_queue.qsize() > 0:
         logger.debug("There are still {} tasks remaining in the queue, but we have reached the maximum number of tries...".format(task_queue.qsize()))
         
         ready_tasks = list()
         still_not_ready = list()

         # Increment task counters. After we increment, we need to check if they're now ready in case
         # some other task also incremented in the same since we last checked. If they're ready,
         # then we'll execute them.
         while task_queue.qsize() > 0:
            delayed_node = task_queue.get_nowait()
            path_node = delayed_node.path_node 

            # We are incrementing so that another task may execute this. We are giving up. 
            ready_to_execute = False 

            if use_bit_dep_checking == False:
               ready_to_execute = check_dependency_counter(path_node, len(path_node.task_payload['dependencies']), increment = True,
                                                            task_execution_breakdown = path_node.task_breakdown,
                                                            lambda_execution_breakdown = lambda_execution_breakdown)
            else:
               ready_to_execute = check_dependency_counter_bits(path_node, len(path_node.task_payload['dependencies']), increment = True,
                                                            dependency_path_node = delayed_node.large_node,
                                                            task_execution_breakdown = path_node.task_breakdown,
                                                            lambda_execution_breakdown = lambda_execution_breakdown)                                                         
            
            # If it's ready however, then we'll still execute it.
            if ready_to_execute:
               ready_tasks.append(delayed_node)
            else:
               # We need to iterate over all dependencies we have locally and write whichever haven't been written.
               logger.debug("Task {} is officially not ready. Writing its large dependency (that we have locally cached) to Redis.".format(path_node.task_key))
               still_not_ready.append(delayed_node)
               
         # Execute each task that was ready to execute.
         for ready_delayed_node in ready_tasks:
            ready_task = ready_delayed_node.path_node 
            logger.debug("[sid-{} uid-{}] Processing now-ready task {} (which is a downstream task of the large-task {})".format(ready_task.scheduler_id, ready_task.update_graph_id, ready_task.task_key, ready_delayed_node.large_node.task_key))
            process_task_start = time.time()

            # Process the task via the process_task method. Pass in the 
            # local results variable for the "previous_results" keyword argument.
            result = process_task(ready_task.task_payload, 
                                    ready_task.task_key, 
                                    previous_results, 
                                    tasks_to_fargate_nodes,
                                    current_scheduler_id = ready_task.scheduler_id,
                                    current_update_graph_id = ready_task.update_graph_id,                                    
                                    lambda_debug = lambda_debug, 
                                    lambda_execution_breakdown = lambda_execution_breakdown, 
                                    current_task_execution_breakdown = ready_task.task_breakdown,
                                    context = aws_context)
            lambda_execution_breakdown.process_task_time += (time.time() - process_task_start)

            # If the task erred, print a message...
            if result[OP_KEY] == TASK_ERRED_KEY:
               logger.debug("The execution of task {} resulted in an error...".format(ready_task.task_key))
            elif result[OP_KEY] == EXECUTED_TASK_KEY:
               logger.debug("[EXECUTION] Result of task {} computed in {} seconds.".format(ready_task.task_key, result[EXECUTION_TIME_KEY]))
               value = result["result"]                   
               data_written = False

               # Cache the result locally.
               previous_results[ready_task.task_key] = value 
            elif result[OP_KEY] == RETRIEVED_FROM_PREVIOUS_RESULTS_KEY:
               logger.debug("[RETRIEVAL] Result of task {} retrieved from local cache.".format(ready_task.task_key))
               value = result["result"]                   
               data_written = True                 
            else:
               raise ValueError("Unknown Operation from process_task: {}".format(result[OP_KEY]))
            # Add the node to the nodes_to_process list so we can process it.
            current_path_node_unprocessed = UnprocessedNode(ready_task, result, data_written = data_written)
            nodes_to_process.put_nowait(current_path_node_unprocessed)      
         
         # Write the data to Redis for all of the not-ready tasks.
         for not_ready_delayed_node in still_not_ready:
            large_node = not_ready_delayed_node.large_node

            # Make sure we have a TaskExecutionBreakdown to pass to 'store_value_in_redis' and also just attached to large_node in general.
            breakdown = None 
            if hasattr(large_node, "task_breakdown"):
               breakdown = large_node.task_breakdown
            else:
               breakdown = TaskExecutionBreakdown(large_node.task_key, update_graph_id = large_node.update_graph_id)
               large_node.task_breakdown = breakdown

            # Write the value to Redis.
            store_value_in_redis(large_node, 
                                 not_ready_delayed_node.large_value, 
                                 key = None, 
                                 check_first = True, 
                                 serialized = True, 
                                 task_execution_breakdown = breakdown, 
                                 lambda_execution_breakdown = lambda_execution_breakdown)            
         
         return {
            OP_KEY: 'okay',
            EXECUTED_TASKS_KEY: ready_tasks,
            STILL_NOT_READY_KEY: still_not_ready
         }
   
   # If we get to this point, then we failed to execute anything so return this default value (same as what process_enqueued_tasks returns upon executing zero tasks).
   return {
         OP_KEY: 'okay',
         EXECUTED_TASKS_KEY: [],
         STILL_NOT_READY_KEY: []
      }   

def deserialize_payload(payload):
   return from_frames(payload)

@xray_recorder.capture("_deserialize")
def _deserialize(function=None, args=None, kwargs=None, task=no_value):
   """ Deserialize task inputs and regularize to func, args, kwargs """
   if function is not None:
      function = pickle.loads(function)
   if args:
      args = pickle.loads(args)
   if kwargs:
      kwargs = pickle.loads(kwargs)

   if task is not no_value:
      assert not function and not args and not kwargs
      function = execute_task
      args = (task,)

   return function, args or (), kwargs or {}

@xray_recorder.capture("execute_task")
def execute_task(task):
   """ Evaluate a nested task

   >>> inc = lambda x: x + 1
   >>> execute_task((inc, 1))
   2
   >>> execute_task((sum, [1, 2, (inc, 3)]))
   7
   """
   if istask(task):
      func, args = task[0], task[1:]
      return func(*map(execute_task, args))
   elif isinstance(task, list):
      return list(map(execute_task, task))
   else:
      return task

@xray_recorder.capture("apply_function")
def apply_function(function, args, kwargs, key):
   """ Run a function, collect information

   Returns
   -------
   msg: dictionary with status, result/error, timings, etc..
   """
   try:
      result = function(*args, **kwargs)
   except Exception as e:
      logger.debug("Exception e: " + str(e))
      msg = error_message(e)
      msg[OP_KEY] = TASK_ERRED_KEY
      msg["actual-exception"] = e
   else:
      msg = {
         OP_KEY: EXECUTED_TASK_KEY,
         "status": "OK",
         "result": result,
         "key": key
     }
   return msg  

def pack_data(o, d, key_types=object):
   """ Merge known data into tuple or dict

   Parameters
   ----------
   o:
      core data structures containing literals and keys
   d: dict
      mapping of keys to data

   Examples
   --------
   >>> data = {'x': 1}
   >>> pack_data(('x', 'y'), data)
   (1, 'y')
   >>> pack_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
   {'a': 1, 'b': 'y'}
   >>> pack_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
   {'a': [1], 'b': 'y'}
   """
   typ = type(o)
   try:
      if isinstance(o, key_types) and o in d:
         return d[o]
   except TypeError:
      pass

   if typ in collection_types:
      return typ([pack_data(x, d, key_types=key_types) for x in o])
   elif typ is dict:
      return {k: pack_data(v, d, key_types=key_types) for k, v in o.items()}
   else:
      return o
   
def no_op(sleep = 0):
   """ NOP (no-op) operation. Used for testing."""
   end = time.time() + sleep 
   while (time.time() < end):
      pass 
   pass 

class UnprocessedNode(object):
   """ 
   This is a wrapper around PathNode used for keeping track of which nodes have and have not
   yet been processed by the Lambda function.

   Attributes
   ----------
   path_node : PathNode
         
      The PathNode instance with which this UnprocessedNode shall be associated/shall represent.

   result : object
         
      The result of the execution of the task associated with the path_node variable.

   data_written : bool
         
      Flag indicating whether or not the result object has been written to Redis already. This will
      be initialized to True if process_task() retrieved the task's data from Redis instead
      of executing the task locally.
       
   """
   def __init__(self, path_node, result, data_written = False):
      self.path_node = path_node
      self.result = result
      self.data_written = data_written 

   def __eq__(self, value):
      """Overrides the default implementation"""
      if isinstance(value, UnprocessedNode):
         return self.path_node.__eq__(value.path_node)
      return False

class DelayedProcessingNode(object):
   """ 
   Wrapper around a downstream task of some large task. A given large task may be associated with multiple DelayedProcessingNode instances.
   
   Attributes
   ----------
   path_node : PathNode
   
      The downstream task of large_node.

   large_node : PathNode
   
      The large task which is upstream from path_node.

   large_value : bytes
      
      Serialized result of the execution of large_node
   """

   def __init__(self, path_node, large_node, large_value):
      self.path_node = path_node
      self.large_node = large_node
      self.large_value = large_value
   
   def get_large_task_key(self):
      return self.large_node.task_key
   
   def get_task_key(self):
      return self.path_node.task_key

   def __eq__(self, value):
      """Overrides the default implementation"""
      if value is None:
         return False

      if isinstance(value, UnprocessedNode):
         return self.path_node.__eq__(value.path_node)
      return False   