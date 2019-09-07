import json
import cloudpickle
from datetime import timedelta
import datetime
import base64 
import redis
import time
import dask
import dask.array
import pandas
import dask_ml 
import sklearn 
import math
import hashlib
from dask.core import istask
import uuid
import sys
import socket
import pickle 
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

from lambdag_metrics import TaskExecutionBreakdown, LambdaExecutionBreakdown 
from utils import funcname
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

proxy_endpoint = "Public IPv4 of EC2 instance on which Proxy is running goes here."
# This is a list of tuples where each tuple is of the form ("Public IPv4 of EC2 Instance", PORT).
# For each KV Store shard, there should be a corresponding entry in this list. The "IPv4" string 
# refers to the public IPv4 of the EC2 instance on which the KV Store shard is running. The port 
# is the port on which the KV Store shard is listening. 
redis_endpoints = [("IPv4 1", 123), ("IPv4 2", 234), ("IPv4 3", 345)]
# This can be whatever you want. Just make sure it is consistent across the proxy, scheduler, and AWS Lambda function.
lambda_function_name = "WukongTaskExecutor"

redis_clients = []
nodes = dict()

hostnames_to_clients = dict()

for IP, port in redis_endpoints:
    redis_client = redis.StrictRedis(host=IP, port = port, db = 0)
    redis_clients.append(redis_client)
    key_string = "node-" + str(IP) + ":" + str(port)
    host_name = key_string + ".FQDN"
    hostnames_to_clients[host_name] = redis_client 
    nodes[key_string] = {
        "hostname": host_name,
        "nodename": key_string,
        "instance": redis_client,
        "port": port,
        "vnodes": 40
    }

hash_ring = HashRing(nodes)

num_redis_clients = len(redis_clients)

#redis_client1 = redis.StrictRedis(host=redis_and_proxy_endpoint, port = 6379, db = 0)
#redis_client2 = redis.StrictRedis(host=redis_and_proxy_endpoint, port = 6380, db = 0)

lambda_client = boto3.client('lambda')
        
if sys.version_info[0] == 2:
   unicode = unicode 
if sys.version_info[0] == 3:
   unicode = str 

scheduler_address_from_redis = redis_clients[0].get("scheduler-address").decode('utf-8')

# print("INIT - Connection to Redis Proxy at {}:{}".format(redis_and_proxy_endpoint, 8989))
#proxy_iostream = IOLoop.current().run_sync(lambda: connect_to_proxy(redis_and_proxy_endpoint, 8989))
#proxy_comm = IOLoop.instance().run_sync(lambda: connect_to_address("tcp://" + redis_and_proxy_endpoint + ":8989", connection_args = {}))
#if not proxy_comm.closed():
#   print("Successfully connected to Redis Proxy!")
#else:
#   print("ERROR CONNECTING TO REDIS PROXY...")

def lambda_handler(event, context):
   """ This is the function directly executed by AWS Lambda. """
   return sequential(event, start_time = time.time())

#def get_redis_client(task_key):
    #hash_obj = hashlib.md5(task_key.encode())
    #val = int(hash_obj.hexdigest(), 16)
    #idx = val % num_redis_clients
    #return redis_clients[idx]
    #if val % 2 == 0:
    #    return redis_client1
    #return redis_client2

def get_incorrect_redis_client(task_key):
    hash_obj = hashlib.md5(task_key.encode())
    val = int(hash_obj.hexdigest(), 16)
    if val % 2 != 0:
        return redis_client1
    return redis_client2

def sequential(event, previous_results = None, start_time = time.time()):
   lambda_start_time = time.time()
   path_encoded = None 
   
   payload = None 

   # A wrapper around a bunch of diagnostic metrics.
   task_execution_breakdowns = list() # TaskExecutionBreakdown()
   lambda_execution_breakdown = LambdaExecutionBreakdown()
   lambda_execution_breakdown.start_time = start_time

   # Check if we were sent the path directly. If not, then grab it from Redis.
   if "path-key" in event:
      path_key = event["path-key"]
      print("[INIT] Obtained KEY {} in Lambda package. Payload must be obtained from Redis.".format(path_key))
      payload = json.loads(hash_ring[path_key[:-7]].get(path_key).decode())
   else: 
       payload = event 
   nodes_map_serialized = payload["nodes-map"]
   starting_node_key = payload["starting-node-key"]

   if previous_results is None:
       if "previous-results" in event:
           previous_results = event["previous-results"]
       else:
           previous_results = dict()
   
   process_path_start = time.time()
   result = process_path(nodes_map_serialized, 
                         starting_node_key, 
                         previous_results = previous_results, 
                         lambda_execution_breakdown = lambda_execution_breakdown, 
                         task_execution_breakdowns = task_execution_breakdowns)

   # Record the total time spent processing the current path. This is almost equivalent to the entire duration.
   lambda_execution_breakdown.process_path_time = time.time() - process_path_start
   
   # lengths = [cloudpickle.dumps(x) for x in task_execution_lengths]
   # redis_clients[0].lpush("timings", *task_execution_lengths)
   
   lambda_execution_breakdown.total_duration = time.time() - lambda_start_time
   redis_clients[0].lpush("task_breakdowns", *[cloudpickle.dumps(breakdown) for breakdown in task_execution_breakdowns])
   redis_clients[0].lpush("lambda_durations", cloudpickle.dumps(lambda_execution_breakdown))

   return result 

@xray_recorder.capture("process_path")
def process_path(nodes_map_serialized, starting_node_key, lambda_execution_breakdown, previous_results = dict(), task_execution_breakdowns = None):
   # Dictionary to store local results.
   # We map task keys to the result of the task.
   results = {}
   
   subsegment = xray_recorder.begin_subsegment("deserializing_path")
    
   current_path_node_encoded = nodes_map_serialized[starting_node_key]
   current_path_node_serialized = base64.b64decode(current_path_node_encoded)
   current_path_node = cloudpickle.loads(current_path_node_serialized)

   xray_recorder.end_subsegment()

   # This flag indicates whether or not we should check the dependencies of the current task before executing it.
   # The dependencies are checked already for tasks that were invoked. If we are executing a 'become' task, however,
   # then the dependencies will not yet be checked. As a result, this flag is 'True' by default but will be set to False
   # once we start processing 'become't asks.
   deps_checked = True  

   # Process all of the tasks contained in the path.
   while current_path_node is not None:
      # Create a TaskExecutionBreakdown object for the current task.
      # We use this to keep track of a bunch of different metrics related to the current task.
      current_task_execution_breakdown = TaskExecutionBreakdown("TEMP_KEY")
      current_task_execution_breakdown.task_processing_start_time = time.time()
      lambda_execution_breakdown.number_of_tasks_executed += 1
      _start_for_current_task = time.time()
      serialized_task_payload = current_path_node.task_payload
      frames = []
      for encoded in serialized_task_payload:
          frames.append(base64.b64decode(encoded))
      task_payload = IOLoop.instance().run_sync(lambda: deserialize_payload(frames))
      task_key = current_path_node.task_key
      
      # Update metrics related to the current task execution 
      current_task_execution_breakdown.deserialize_time = (time.time() - _start_for_current_task)
      current_task_execution_breakdown.task_key = task_key

      # See the comment above for an explanation of 'deps_checked'
      if not deps_checked: 
         _dependency_checking_start = time.time()
         print("[PROCESSING] Processing Task ", task_key, ". Checking dependencies first.")
         subsegment = xray_recorder.begin_subsegment("checking dependencies of begin task")
         key_counter = str(task_key) + "---dep-counter"

         # Check to make sure all of the dependencies are resolved before continuing onto execution.
         # We create a Transaction for incrementing the counter and getting its value (atomically). 
         # We do this to reduce the number of round trips required for this step of the process.
         redis_client = hash_ring.get_node_instance(task_key)
         incr_get_pipe = redis_client.pipeline()     # Create the Transaction object.
         incr_get_pipe.incr(key_counter)             # Enqueue the atomic increment operation.
         incr_get_pipe.get(key_counter)              # Enqueue the atomic get operation AFTER the increment operation.

         _redis_start = time.time()
         responses = incr_get_pipe.execute()         # Execute the Transaction; save the responses in a local variable.

         # Increment the total redis read time metric.
         _duration = (time.time() - _redis_start)
         lambda_execution_breakdown.redis_read_time += _duration
         current_task_execution_breakdown.redis_read_time += _duration
         # Decode the result and cast  it to an int.
         dependencies_completed = int(responses[1].decode())     # Decode and cast the result of the 'get' operation.

         xray_recorder.end_subsegment()

         if dependencies_completed != len(task_payload["dependencies"]):
            print("[WARNING] Cannot execute 'become' task {} as only {}/{} dependencies are finished and available.".format(task_payload["key"], dependencies_completed, len(task_payload["dependencies"])))
            return   
         current_task_execution_breakdown.dependency_checking = (time.time() - _dependency_checking_start)
      else:
         print("[PROCESSING] Processing Task ", task_key, ". Dependencies have already been checked.")
      #print("Task Payload: ", task_payload)

      _start = time.time()
      # Process the task via the process_task method. Pass in the 
      # local results variable for the "previous_results" keyword argument.
      result = process_task(task_payload, task_key, previous_results = previous_results, lambda_execution_breakdown = lambda_execution_breakdown, current_task_execution_breakdown = current_task_execution_breakdown)
      lambda_execution_breakdown.process_task_time += (time.time() - _start)
      
      # If the task erred, just return... 
      if result["op"] == "task-erred":
         print("[ERROR] Execution of task {} resulted in an error.".format(task_key))
         print("Result: ", result)
         print("Result['exception']: ", result["exception"])
         return {
            'statusCode': 400,
            'body': json.dumps(result["exception"])
         }   
      print("[EXECUTION] Result of task {} computed in {} seconds.".format(task_key, result["execution-length"]))

      # Create the previous_results dictionary if it does not exist.
      if previous_results is None:
          previous_results = dict()

      _start = time.time()
      # Process the result as well as the downstream tasks.
      can_now_execute = process_downstream_tasks(task_key, 
                                                 current_path_node,
                                                 result, 
                                                 task_payload, 
                                                 nodes_map_serialized, 
                                                 previous_results, 
                                                 lambda_execution_breakdown = lambda_execution_breakdown,
                                                 current_task_execution_breakdown = current_task_execution_breakdown)
      lambda_execution_breakdown.process_downstream_tasks_time += (time.time() - _start)
      current_task_execution_breakdown.process_downstream_tasks_time += (time.time() - _start)
      
      # If the type of \can_now_execute\ is dict, then process_downstream_tasks returned an error message because it failed to connect to the proxy.
      # So we return this so it can be returned by this Lambda.
      if type(can_now_execute) is dict:
          return can_now_execute

      subsegment = xray_recorder.begin_subsegment("executing-ready-invokes")
      _start = time.time()
      # Invoke all of the ready-to-execute tasks.
      for node_that_can_execute in can_now_execute:
         # TO-DO: Check if we already have this path/node and send it directly if so.
         payload = {"path-key": node_that_can_execute.task_key + "---path"}
         print("[INVOCATION] - Invoking downstream task {}.".format(node_that_can_execute.task_key))
         lambda_client.invoke(FunctionName=lambda_function_name, InvocationType='Event', Payload=json.dumps(payload))
      # Increment the 'invoke' metric.
      lambda_execution_breakdown.invoking_downstream_tasks += (time.time() - _start)
      current_task_execution_breakdown.invoking_downstream_tasks += (time.time() - _start)
      xray_recorder.end_subsegment()
      subsegment = xray_recorder.begin_subsegment("processing-become")
      
      # Store the current task metric object in the list.
      _now = time.time()
      current_task_execution_breakdown.total_time_spent_on_this_task = (_now - _start_for_current_task)
      current_task_execution_breakdown.task_processing_end_time = _now
      task_execution_breakdowns.append(current_task_execution_breakdown)
      if current_path_node.become is not None: 
         print("[BECOMING] Task {} is going to ATTEMPT to become task {}. Will check dependencies shortly...".format(current_path_node.task_key, current_path_node.become))
         become_node_encoded = nodes_map_serialized[current_path_node.become]
         become_node_serialized = base64.b64decode(become_node_encoded)
         current_path_node = cloudpickle.loads(become_node_serialized)
         deps_checked = False # Make sure to set this to 'False' since we haven't checked the dependencies for this task yet! 
      else:
         print("[PROCESSING] - There are no downstream tasks which depend upon this task.")
         print("[MESSAGING] - Publishing 'lambda-result' message to Redis Channel: dask-workers-1")
         payload = dict(
            op="lambda-result",
            task_key=current_path_node.task_key,
            execution_length=result["execution-length"]
         )
         # For now, I am just using the "dask-workers-1" channel to publish messages to the Scheduler.
         # As long as I am creating the Redis-polling processes on the Scheduler, we can arbitrarily use whatever channel we want.
         redis_clients[0].publish("dask-workers-1", json.dumps(payload))
         current_path_node = None 
      xray_recorder.end_subsegment()
   return {
      'statusCode': 202,
      'body': json.dumps("Success")  
   }

@xray_recorder.capture("process_task")
def process_task(task_definition, task_key = None, previous_results = None, lambda_execution_breakdown = None, current_task_execution_breakdown = None):
   """ This function is responsible for executing the current task.
       
       It is responsible for grabbing and processing the dependencies for the current task."""
   # Grab the key associated with this task and use it to store the task's result in Elasticache.
   key = task_key or task_definition['key']   
   print("[EXECUTION] Executing task {} .".format(key))
   args_serialized = None 
   func_serialized = None
   if "function" in task_definition:
      func_serialized = task_definition["function"]
   if "args" in task_definition:
      args_serialized = task_definition["args"]
   task_serialized = no_value
   if "task" in task_definition:
      task_serialized = task_definition["task"]
   func, args, kwargs = _deserialize(func_serialized, args_serialized, None, task_serialized)

   # This is a list of dependencies in the form of string keys (which are keys to the Elasticache server)
   subsegment = xray_recorder.begin_subsegment("getting-dependencies-from-redis")
   dependencies = task_definition["dependencies"]
   data = {}
   print("[PREP] {} dependencies required.".format(len(dependencies)))
   print("Dependencies Required: ", dependencies)

   responses = dict()
   
   # Initialize this variable as we're going to use it shortly.
   time_spent_retrieving_dependencies_from_redis = 0

   # We were using transactions/mget for this, but most tasks do not have a large number of dependencies
   # so mget() probably wasn't helping much. Additionally, the keys are hashed evenly across all of the 
   # shards so they'll mostly be spread out evenly, further decreasing the effectiveness of mget().
   for dep in dependencies:
      if dep in previous_results:
         continue
      _start = time.time()
      val = hash_ring[dep].get(dep)
                           
      # Increment the aggregate total of the dependency retrievals.
      time_spent_retrieving_dependencies_from_redis += (time.time() - _start)
      addr = hash_ring.get_node_hostname(dep)
      if val is None:
         print("[ {} ] [ERROR] Dependency {} is None... Failed to retrieve dependency from Redis Client at {}".format(datetime.datetime.utcnow(), dep, addr))
      responses[dep] = val
    
   # And update the Redis read metric.
   lambda_execution_breakdown.redis_read_time += time_spent_retrieving_dependencies_from_redis
   current_task_execution_breakdown.redis_read_time += time_spent_retrieving_dependencies_from_redis

   _process_deps_start = time.time()

   xray_recorder.end_subsegment()
   subsegment = xray_recorder.begin_subsegment("processing-dependencies")
   subsegment.put_annotation("num_dependencies", str(len(dependencies)))

   # Add the previous results to the data dictionary.
   print("[PREP] Appending previous_results to current data list.")
   data.update(previous_results)
   size_of_deps = 0

   # Keep track of any chunked data that we need to de-chunk.
   chunked_data = dict()

   for task_key, serialized_dependency_data in responses.items():
      size_of_deps = size_of_deps + sys.getsizeof(serialized_dependency_data)
      # TO-DO: Handle missing dependencies?
      # Make multiple attempts to retrieve missing dependency from Redis in case we've been timing out.
      if serialized_dependency_data is None:
         print("[ {} ] [ERROR] Dependency {} is None... Failed to retrieve dependency {} from Redis. Exiting.".format(datetime.datetime.utcnow(), task_key))
         return {
            "op": "task-erred",   
            "statusCode": 400,
            "exception": "dependency {} is None".format(task_key),
            "body": "dependency {} is None".format(task_key)
         } 
      deserialized_data = cloudpickle.loads(serialized_dependency_data)
      # print("Deserialized Type -- ", type(deserialized_data), " = ", deserialized_data)
      # Check if we got chunked data. If so, we need to retrieve all of the pieces and reconstruct the data.
      # payload = {"op": "chunked-data", "num-chunks": num_chunks, "chunk-size": chunk_size, "chunk-keys": keys}
      if type(deserialized_data) is dict and "op" in deserialized_data and deserialized_data["op"] == "chunked-data":
          num_chunks = deserialized_data["num-chunks"]
          chunk_size = deserialized_data["chunk-size"]
          chunk_keys = deserialized_data["chunk-keys"]
          print("Obtained chunked data for task {}. Number of chunks: {} Chunk size: {}".format(task_key, num_chunks, chunk_size))
          
          # Grab each of the chunks; reconstruct the original data.
          chunk_string = b""
          chunk_payloads = defaultdict(list)
          lst = [None] * num_chunks
          pos_map = dict()
          pos = 0 
          for chunk_key in chunk_keys:
              hostname = hash_ring.get_node_hostname(chunk_key)
              chunk_payloads[hostname].append(chunk_key)
              #chunk = hash_ring[chunk_key].get(chunk_key)
              #print("Obtained chunk ", chunk)
              #chunk_string = chunk_string + chunk 
              pos_map[chunk_key] = pos
              pos = pos + 1
          for hostname, chunk_payload in chunk_payloads.items():
              client = hostnames_to_clients[hostname]
              vals = client.mget(chunk_payload)
              _zipped = dict(zip(chunk_payload, vals))
              for k,v in _zipped.items():
                  idx = pos_map[k]
                  lst[idx] = v 
          for chunk in lst:
              chunk_string = chunk_string + chunk
          deserialized_data = cloudpickle.loads(chunk_string)
          # print("De-chunked data serialized: ", chunk_string)
          # print("De-chunked Data: ", deserialized_data)
      data[task_key] = deserialized_data
      # Keep a local record of the value obtained from Redis.
      previous_results[task_key] = deserialized_data
   subsegment.put_annotation("size_of_deps", str(size_of_deps))
   xray_recorder.end_subsegment()
   subsegment = xray_recorder.begin_subsegment("pack_data")
   args2 = pack_data(args, data, key_types=(bytes, unicode))
   kwargs2 = pack_data(kwargs, data, key_types=(bytes, unicode))
   
   # Update dependency processing metric. 
   current_task_execution_breakdown.dependency_processing = (time.time() - _process_deps_start)
   function_start_time = time.time()
   xray_recorder.end_subsegment()

   # Execute the function.
   print("Applying function {} (key is {})".format(func, key))
   result = apply_function(func, args2, kwargs2, key)
   function_end_time = time.time()
   execution_length = function_end_time - function_start_time  
   result["execution-length"] = execution_length
   result["start-time"] = function_start_time
   result["end_time"] = function_end_time

   # Update execution-related metrics.
   current_task_execution_breakdown.task_execution_start_time = function_start_time
   current_task_execution_breakdown.task_execution_end_time = function_end_time
   current_task_execution_breakdown.task_execution = execution_length
   return result

@xray_recorder.capture("process-downstream-tasks")
def process_downstream_tasks(current_task_key, current_path_node, result, task_payload, nodes_map_serialized, previous_results, lambda_execution_breakdown = None, current_task_execution_breakdown = None):
   """This function checks the dependencies of downstream tasks and determines if they are able to execute or not.

      It either invokes the functions directly, or it uses the proxy to offload that task.
      
      It also stores the result in Redis (either directly or via the proxy)."""
   #global proxy_comm # Not ideal but this should work 
   value = result.pop('result', None)

   # Cache the result locally for future task executions.
   previous_results[current_task_key] = value

   # Serialize the resulting value.
   subsegment = xray_recorder.begin_subsegment("serializing-value")
   value_serialized = cloudpickle.dumps(value)
   xray_recorder.end_subsegment()
   
   # If we're supposed to use the proxy, then just do that.
   if current_path_node.use_proxy == True:
      # Send message to proxy.
      subsegment = xray_recorder.begin_subsegment("store-redis-proxy")
      size = sys.getsizeof(value_serialized)
      payload_for_proxy = json.dumps({"op": "set", "task-key": current_task_key, "value-encoded": base64.encodestring(value_serialized).decode('utf-8')})
      size_of_payload = sys.getsizeof(payload_for_proxy)
      subsegment.put_annotation("size_payload_for_proxy", str(size_of_payload))
      redis_proxy_channel = task_payload["proxy-channel"]
      _start = time.time()
      redis_clients[0].publish(redis_proxy_channel, payload_for_proxy)
      lambda_execution_breakdown.redis_write_time += (time.time() - _start)
      current_task_execution_breakdown.redis_write_time += (time.time() - _start)
      xray_recorder.end_subsegment()
      return []
   else:   
      addr = hash_ring.get_node_hostname(current_task_key)
      print("Storing the result in Redis DIRECTLY (no proxy) at hostname ", addr)

      size = sys.getsizeof(value_serialized)

      # Should we break up large tasks into smaller tasks when storing them?
      # If so, what is the threshold (in bytes) for when a task is considered "large"?
      chunk_large_tasks = task_payload["chunk-large-tasks"]
      chunk_task_threshold = task_payload["chunk-task-threshold"]
      num_chunks_for_large_tasks = task_payload["num-chunks-for-large-tasks"]
      # If this value is -1, then it was not specified by the user. In that case, we break
      # large objects up such that the chunks are of size 'chunk_task_threshold'. 
      if num_chunks_for_large_tasks == -1:
          num_chunks_for_large_tasks = math.ceil(size / chunk_task_threshold)

      # If current_path_node.become is None, then this is a root task. We should just store
      # the final value for simplicity's sake so the client has an easy time obtaining it.
      # Possible TO-DO: Chunk final results; client can de-chunk the data if necessary.
      if chunk_large_tasks and size >= chunk_task_threshold and current_path_node.become is not None:
          subsegment = xray_recorder.begin_subsegment("store-redis-direct-chunking")
          print("Data from task {} is {} bytes, which is greater than the 'chunking threshold' of {} bytes. Breaking data up into chunks before storing.".format(current_task_key, size, chunk_task_threshold)) 
          chunk_size = math.ceil(size / num_chunks_for_large_tasks)
          chunks = [value_serialized[x:x+chunk_size] for x in range(0, size, chunk_size)]
          base_chunk_string = "chunk-"
          keys = [current_task_key + "chunk-" + str(i) for i in range(num_chunks_for_large_tasks)]
          zipped = dict(zip(keys, chunks))
          _start_redis = time.time()
          chunk_payloads = defaultdict(dict)
          for chunk_key, chunk in zipped.items():
              # print("Storing chunk ", chunk, " at key ", chunk_key)
              node_hostname = hash_ring.get_node_hostname(chunk_key)
              chunk_payloads[node_hostname][chunk_key] = chunk
              # hash_ring[chunk_key].set(chunk_key, chunk)
          for hostname, chunk_payload in chunk_payloads.items():
              client = hostnames_to_clients[hostname]
              client.mset(chunk_payload)
          payload = {"op": "chunked-data", "num-chunks": num_chunks_for_large_tasks, "chunk-size": chunk_size, "chunk-keys": keys}
          hash_ring[current_task_key].set(current_task_key, cloudpickle.dumps(payload))
          # print("Original (non-chunked) data: ", value)
          # print("Original (non-chunked) data serialized: ", value_serialized)

          # Update performance metrics.
          lambda_execution_breakdown.redis_write_time += (time.time() - _start_redis)
          current_task_execution_breakdown.redis_write_time += (time.time() - _start_redis)

          # Store some annotations so we can see chunking data in X-Ray traces if we so desire.
          xray_recorder.current_subsegment().put_annotation("chunk_size", chunk_size)
          xray_recorder.current_subsegment().put_annotation("original_size", size)
          xray_recorder.current_subsegment().put_annotation("num_chunks_for_large_tasks", num_chunks_for_large_tasks)

          xray_recorder.end_subsegment()
      else:
          # Store the result in redis.
          subsegment = xray_recorder.begin_subsegment("store-redis-direct")
          subsegment.put_annotation("size_payload_redis_direct", str(size))
          _start = time.time()
          success = hash_ring[current_task_key].set(current_task_key, value_serialized)

          # Update performance metrics.
          lambda_execution_breakdown.redis_write_time += (time.time() - _start)
          current_task_execution_breakdown.redis_write_time += (time.time() - _start)
          xray_recorder.end_subsegment()

      can_now_execute = []
      num_dependencies_of_dependents = task_payload["num-dependencies-of-dependents"]
   
      subsegment = xray_recorder.begin_subsegment("checking-invoke-deps")

      # Iterate over all of the path nodes in the "invoke" list and check which ones have all  
      # dependencies completed. Invoke the nodes which satisfy this condition. Skip the nodes that do not.
      for invoke_key in current_path_node.invoke:
         print("[INVOKE-PROCESSING] Checking if downstream task {} is ready to execute...".format(invoke_key))

         # Deserialize the next invoke node.
         invoke_node_encoded = nodes_map_serialized[invoke_key]
         invoke_node_serialized = base64.b64decode(invoke_node_encoded)
         invoke_node = cloudpickle.loads(invoke_node_serialized)

         # Grab the key of the "invoke" node and check how many dependencies it has.
         # Next, check Redis to see how many dependencies are available. If all dependencies
         # are available, then we append this node to the can_now_execute list. Otherwise, we skip it.
         # The invoke node will be executed eventually by its final dependency.
         key_counter = str(invoke_key) + "---dep-counter"
         num_dependencies = num_dependencies_of_dependents[invoke_key]

         # We create a Transaction for incrementing the counter and getting its value (atomically). 
         # We do this to reduce the number of round trips required for this step of the process.
         associated_redis_client = hash_ring.get_node_instance(invoke_key)
         incr_get_pipe = associated_redis_client.pipeline()     # Create the Transaction object.
         incr_get_pipe.incr(key_counter)                             # Enqueue the atomic increment operation.
         incr_get_pipe.get(key_counter)                              # Enqueue the atomic get operation AFTER the increment operation.
         _start = time.time()
         responses = incr_get_pipe.execute()                         # Execute the Transaction; save the responses in a local variable.
         
         # Increment redis read metric for current Lambda.
         lambda_execution_breakdown.redis_read_time += (time.time() - _start)
         current_task_execution_breakdown.redis_read_time += (time.time() - _start)
         # Decode the result and cast  it to an int.
         dependencies_completed = int(responses[1].decode())     # Decode and cast the result of the 'get' operation.

         # Check if the task is ready for execution. If it is, put it in the "can_now_execute" list. 
         # If the task is not yet ready, then we don't do anything further with it at this point.
         if dependencies_completed == num_dependencies:
            print("[DEP. CHECKING] - task {} is now ready to execute as all {} dependencies have been computed.".format(invoke_key, num_dependencies))
            can_now_execute.append(invoke_node)
         else:
            print("[DEP. CHECKING] - task {} cannot execute yet. Only {} out of {} dependencies have been computed.".format(invoke_key, dependencies_completed, num_dependencies))
      xray_recorder.end_subsegment()

      return can_now_execute

@xray_recorder.capture_async("deserialize_payload")
@gen.coroutine
def deserialize_payload(payload):
   msg = yield from_frames(payload)
   raise gen.Return(msg)
   
@xray_recorder.capture_async("send_message_to_proxy")
@gen.coroutine
def send_message_to_proxy(msg):
   print("[ {} ] Sending message to proxy. Message contents: {}".format(datetime.datetime.utcnow(), msg))
   proxy_comm = yield connect_to_address("tcp://" + proxy_endpoint + ":8989", connection_args = {})
   if proxy_comm.closed():
       print("[ {} ] [ERROR] Connection attempt #1 to proxy failed... Trying again...".format(datetime.datetime.utcnow()))
       max_tries = 4
       num_tries = 2
       while proxy_comm.closed() and num_tries <= max_tries:
           proxy_comm = yield connect_to_address("tcp://" + proxy_endpoint + ":8989", connection_args = {})
           print("[ {} ] [ERROR] Connection attempt #{} to proxy failed... Trying again...".format(datetime.datetime.utcnow(), num_tries))
           num_tries += 1
       if proxy_comm.closed():
           print("[ {} ] [ERROR] Cannot connect to proxy...")
           return False 
   bytes_written = yield proxy_comm.write(msg, serializers=["msgpack"]) 
   xray_recorder.current_subsegment().put_annotation("bytes_written_to_proxy", str(bytes_written))
   print("[ {} ] Wrote {} bytes to the Redis Proxy.".format(datetime.datetime.utcnow(), bytes_written))
   return True 
   
@xray_recorder.capture_async("send_message_to_scheduler")
@gen.coroutine
def send_message_to_scheduler(msg, scheduler_address):
   #connection_args = {'ssl_context': None, 'require_encryption': None}
   connection_args = {}
   comm = yield connect_to_address(scheduler_address, connection_args = connection_args)
   comm.name = "Lambda->Scheduler"
   #comm._server = weakref.ref(self)
   print("Sending msg ", msg, " to scheduler")
   yield comm.write(msg, serializers=["msgpack"])   
   #yield comm.close()
   
@xray_recorder.capture_async("send_msg_to_scheduler_with_comm")
@gen.coroutine
def send_msg_to_scheduler_with_comm(msg, comm = None):
   if comm is None:
      comm = scheduler_comm
   # If Comm is STILL none, then re-establish a connection with the Scheduler.
   if comm is None or (comm is not None and comm.closed()):
      print("[WARNING] Connection with scheduler is no longer established. Attempting to reconnect...")
      scheduler_address_from_redis = redis_clients[0].get("scheduler-address").decode('utf-8')
      connection_args = {}
      scheduler_comm = IOLoop.instance().run_sync(lambda: connect_to_address(scheduler_address_from_redis, connection_args = connection_args))
      scheduler_comm.name = "Lambda->Scheduler"    
      comm = scheduler_comm
   yield comm.write(msg, serializers=["msgpack"])  
   redis_clients[0].incr("num-sent", amount = 1)

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
      print("Exception e: ", e)
      msg = error_message(e)
      msg["op"] = "task-erred"
      msg["actual-exception"] = e
   else:
      msg = {
         "op": "task-finished",
         "status": "OK",
         "result": result,
         "key": key
         #"nbytes": sizeof(result),
         #"type": type(result) if result is not None else None,
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

class Reschedule(Exception):
   """ Reschedule this task

   Raising this exception will stop the current execution of the task and ask
   the scheduler to reschedule this task, possibly on a different machine.

   This does not guarantee that the task will move onto a different machine.
   The scheduler will proceed through its normal heuristics to determine the
   optimal machine to accept this task.  The machine will likely change if the
   load across the cluster has significantly changed since first scheduling
   the task.
   """

   pass     
   
def get_msg_safe_str(msg):
   """ Make a worker msg, which contains args and kwargs, safe to cast to str:
   allowing for some arguments to raise exceptions during conversion and
   ignoring them.
   """

   class Repr(object):
      def __init__(self, f, val):
         self._f = f
         self._val = val

      def __repr__(self):
         return self._f(self._val)

   msg = msg.copy()
   if "args" in msg:
      msg["args"] = Repr(convert_args_to_str, msg["args"])
   if "kwargs" in msg:
      msg["kwargs"] = Repr(convert_kwargs_to_str, msg["kwargs"])
   return msg

def convert_args_to_str(args, max_len=None):
   """ Convert args to a string, allowing for some arguments to raise
   exceptions during conversion and ignoring them.
   """
   length = 0
   strs = ["" for i in range(len(args))]
   for i, arg in enumerate(args):
      try:
         sarg = repr(arg)
      except Exception:
         sarg = "< could not convert arg to str >"
      strs[i] = sarg
      length += len(sarg) + 2
      if max_len is not None and length > max_len:
         return "({}".format(", ".join(strs[: i + 1]))[:max_len]
   else:
      return "({})".format(", ".join(strs))

def convert_kwargs_to_str(kwargs, max_len=None):
   """ Convert kwargs to a string, allowing for some arguments to raise
   exceptions during conversion and ignoring them.
   """
   length = 0
   strs = ["" for i in range(len(kwargs))]
   for i, (argname, arg) in enumerate(kwargs.items()):
      try:
         sarg = repr(arg)
      except Exception:
         sarg = "< could not convert arg to str >"
      skwarg = repr(argname) + ": " + sarg
      strs[i] = skwarg
      length += len(skwarg) + 2
      if max_len is not None and length > max_len:
         return "{{{}".format(", ".join(strs[: i + 1]))[:max_len]
   else:
      return "{{{}}}".format(", ".join(strs))
   
@xray_recorder.capture("def store_value_in_redis(value, key)")
def store_value_in_redis(key, value):
    print("[REDIS PROCESS] Storing value {} in Redis at key {}.".format(value, key))
    value_serialized = cloudpickle.dumps(value)
    get_redis_client(key).set(key, value)
    print("[REDIS PROCESS] Value stored successfully.")