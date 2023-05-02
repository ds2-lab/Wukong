from collections import defaultdict
from enum import Enum

# All of the possible event names.
event_names = ["Store Intermediate Data in Cloud Storage", "Store PathNodes in Cloud Storage", "Get IP from Coordinator", "Invoke Cluster Schedulers", 
                "Write Cluster Payload to S3", "Read Cluster Payload from S3", "P2P Connect to Cluster Scheduler", "Coordinator Send to Join Cluster", 
                "Coordinator Connect to Join Cluster", "Store Dask DAG in S3", "Download Dask DAG from S3", "Store Final Result in S3", 
                "Dask DAG Generation", "DFS", "Coordinator Connect as Big Task", "Coordinator Send as Big Task", "Coordinator Receive as Big Task", 
                "Execute Task", "Coordinator Connect as Upstream", "Coordinator Send as Upstream", "Coordinator Receive as Upstream", "Invoke Lambda", 
                "Coordinator Receive After Invoke", "P2P Connect as Become", "P2P Connect as Upstream", "Coordinator Connect as Downstream", 
                "Coordinator Send as Downstream", "P2P Poll Loop", "Coordinator Receive as Downstream", "Coordinator Connect", "P2P Connect as Downstream", 
                "P2P Receive Data", "P2P Send Become Switched", "Process Nodes from Invocation Payload", "Retrieve Values from Cloud Storage", "Update Cluster Status", 
                "P2P Connect to Cluster Node", "Idle", "P2P Send", "Download Dependency From S3", "Unzip Dependency Downloaded From S3", "Lambda Started Running", 
                "Read Path from Redis", "Get Fargate Info from Redis", "Store Intermediate Data in EC2 Redis", "Store Intermediate Data in Fargate Redis",
                "Read Dependencies from Redis", "Check Dependency Counter"]

class WukongEvent(object):
    """
    Used to record events that occur during execution. Intended to be plotted in a Gantt chart.
    
    Attributes:
        name (str)          : Name of the event (e.g., "SQS Send Message")
        start_time (int)    : Start time of the event.
        end_time (int)      : End time of the event.
        duration (int)      : Duration of the event (in seconds).
        metadata_str (dict) : Additional data.
    """
    def __init__(self, name = None, start_time = None, end_time = None, metadata = None, workload_id = None):
        self.name = name
        self.start_time = start_time
        self.end_time = end_time
        self.duration = end_time - start_time 
        self.metadata = metadata,
        self.workload_id = workload_id

class TaskExecutionBreakdown(object):
    # Basically just a container for the breakdown of a task's execution on a Lambda function. 
    # There are some imperfections in the record-taking.
    def __init__(
         self,
         task_key,                              # Key of the corresponding task
         cloud_storage_read_time = 0, 
         dependency_processing = 0,             # Deserializing dependencies and all that
         task_execution_start_time = 0,         # Time stamp that execution of task code started
         task_execution = 0,                    # Length of executing task code 
         task_execution_end_time = 0,           # Time stamp that execution of task code ended
         cloud_storage_write_time = 0,          
         invoking_downstream_tasks = 0,         # invoking ready-to-execute dependent tasks 
         total_time_spent_on_this_task = 0,     # how long we spent processing this task
         task_processing_start_time = 0,        # timestamp of when we started processing the task
         task_processing_end_time = 0,          # timestamp of when we finished processing the task
         update_graph_id = None,
         workload_id = None):         
        self.task_key = task_key
        self.cloud_storage_read_time = cloud_storage_read_time
        self.cloud_storage_write_time = cloud_storage_write_time
        self.task_execution = task_execution
        self.checking_and_incrementing_dependency_counters = 0
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.dependency_processing = dependency_processing
        self.serialization_time = 0
        self.publishing_messages = 0
        self.deserialization_time = 0
        self._graph_to_payload_time = 0
        self.task_execution = task_execution
        self.update_graph_id = update_graph_id 
        self.workload_id = workload_id

        self.redis_read_time = 0
        self.redis_write_time = 0        
        self.process_task_time = 0
        self.bytes_read = 0

        self.task_execution_start_time = task_execution_start_time
        self.task_execution_end_time = task_execution_end_time
        self.total_time_spent_on_this_task = total_time_spent_on_this_task
        self.task_processing_start_time = task_processing_start_time
        self.task_processing_end_time = task_processing_end_time 

class LambdaExecutionBreakdown(object):
    """
        .. attribute:: start_time 
        
            The time at which the Lambda function began execution.

        .. attribute:: process_path_time

            The time spent executing the 'process_path' function.

        .. attribute:: checking_and_incrementing_dependency_counters

            The time spent checking if downstream tasks are ready to execute. This includes incrementing/checking dependency counters.

        .. attribute:: cloud_storage_read_time

            Time spent writing data to Redis. This does NOT include checking dependency counters.

        .. attribute:: cloud_storage_write_time

            Time spent reading data from Redis. This does NOT include incrementing dependency counters.

        .. attribute:: publishing_messages

            Time spent sending a message to the proxy via 'publish'.

        .. attribute:: invoking_downstream_tasks   

            Time spent calling the boto3 API's 'invoke' function.

        .. attribute:: number_of_tasks_executed

            The number of tasks executed directly by this Lambda function.

        .. attribute:: serialization_time

            The time spent serializing data.

        .. attribute:: deserialization_time

            The time spent deserializing data.

        .. attribute:: total_duration               

            Total runtime of the Lambda function.

        .. attribute:: execution_time

            Time spent explicitly executing tasks.

        .. attribute:: cloud_storage_read_times

            Dictionary mapping task keys to the amount of time spent reading them.

        .. attribute:: cloud_storage_write_times                              

            Dictionary mapping task keys to the amount of time spent writing them.

        .. attribute:: tasks_pulled_down

            How many tasks this Lambda "pulled down" to execute locally (after executing a "big" task).

        .. attribute:: write_counters

            Mapping of task keys to how many time data was written to that key.

        .. attribute:: read_counters

            Mapping of task keys to how many time data was read from that key.

        .. attribute:: aws_request_id     

            The AWS Request ID of this Lambda function.

        .. attribute:: fan_outs

            Fan-out data for each fan-out processed by this Lambda.

        .. attribute:: fan_ins                          

            Fan-in data for each fan-in processed by this Lambda.
        
        .. attribute:: install_deps_from_S3

            Time it takes to download and unzip dependencies from S3.
        
        ..attribute:: idle_time (int)

            Time spent in an idle state waiting for more work from the Cluster Scheduler.
    """
    def __init__(
         self,
         start_time = 0,
         process_path_time = 0,
         cloud_storage_read_time = 0,
         cloud_storage_write_time = 0,
         invoking_downstream_tasks = 0,
         number_of_tasks_executed = 0,
         total_duration = 0,
         aws_request_id = None,
         workload_id = None):
        self.start_time = start_time
        self.number_of_tasks_executed = number_of_tasks_executed
        self.process_path_time = process_path_time
        self.checking_and_incrementing_dependency_counters = 0
        self.cloud_storage_read_time = cloud_storage_read_time
        self.cloud_storage_write_time = cloud_storage_write_time
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.publishing_messages = 0
        self.serialization_time = 0
        self.deserialization_time = 0
        self.invoking_dfs_lambdas_time = 0      
        self.execution_time = 0
        self.total_duration = total_duration
        self.workload_id = workload_id

        self.redis_read_time = 0
        self.redis_write_time = 0
        self.process_task_time = 0
        self.bytes_written = 0
        self.bytes_read = 0

        self.events = list()

        # Collections of Metrics (instead of single variables, these are often dicts/arrays)  
        # Map from Task Key --> (Size of Data, Time) 
        self.cloud_storage_read_times = dict()
        # Map from Task Key --> (Size of Data, Time) 
        self.cloud_storage_write_times = dict()
        self.install_deps_from_S3 = 0
        self.tasks_pulled_down = 0
        self.reuse_count = 0
        self.write_counters = defaultdict(int)
        self.read_counters = defaultdict(int)
        self.aws_request_id = aws_request_id 
        self.fan_outs = list() # List where we keep track of task sizes in the context of fan-outs.
        self.fan_ins = list()  # List where we keep track of task sizes in the context of fan-ins.

    def add_event(self, event):
        self.events.append(event)

    def combine(self, other):
        """
            Add the contents of another instance of LambdaExecutionBreakdown to 'this' instance.

            Note that this will NOT add/replace "this" instance's 'start_time' attribute with the other instance's. 
            That is, the 'start_time' attribute of "this" instance will NOT be modified.

            Arguments:
                other (LambdaExecutionBreakdown) : The other instance of LambdaExecutionBreakdown whose
                                                   data will be added/combined with "this" instance's data.
        """
        # Filter out methods/functions.
        members = [attr for attr in dir(other) if not callable(getattr(other, attr)) and not attr.startswith("__")]

        for var_name in members:
            if (var_name == "start_time"):
                continue
            
            var_other = getattr(other, var_name, None)

            if var_other is None:
                print("[WARNING] Variable {} is None for 'other' LambdaExecutionBreakdown.".format(var_name))
            
            var_self = None 
            if type(var_other) is list:
                var_self = getattr(self, var_name, None)
                var_self.extend(var_other)
            elif type(var_other) is dict:
                var_self = getattr(self, var_name, None)
                var_self.update(var_other)
            elif type(var_other) is int or type(var_other) is float:
                var_self = getattr(self, var_name, None)
                var_self += var_other
            
            setattr(self, var_name, var_self)

    def add_write_time(self, fargateARN, data_key, size, _redis_write_duration, start_time, stop_time):
        """ Add an entry to the cloud_storage_write_times dictionary.
        
                data_key (str)              -- The task/path/etc. we're entering data for.
                size (int)                  -- The size of the data being written.
                _redis_write_duration (int) -- The time the write operation took to complete.
        """
        # I do not think the same task key could be added more than once for writes, but just in case,
        # I have code to handle the case where a value already exists (we just use a list of values
        # for such a situation).
        if data_key in self.cloud_storage_write_times:
            count = self.write_counters[data_key]
            count += 1
            self.write_counters[data_key] = count
            data_key = data_key + "---" + str(count)
            self.cloud_storage_write_times[data_key] = {
                "size": size,
                "duration": _redis_write_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN
            }
            # (size, _redis_write_duration, start_time, stop_time)
        else:
            self.cloud_storage_write_times[data_key] = {
                "size": size,
                "duration": _redis_write_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN                
            }
            # (size, _redis_write_duration, start_time, stop_time)

    def add_read_time(self, fargateARN, data_key, size, _redis_read_duration, start_time, stop_time):
        """ Add an entry to the cloud_storage_write_times dictionary.
        
                data_key (str)              -- The task/path/etc. we're entering data for.
                size (int)                  -- The size of the data being read.
                _redis_write_duration (int) -- The time the read operation took to complete.
        """
        # For read times, there can absolutely be multiple values per key.
        if data_key in self.cloud_storage_read_times:
            count = self.read_counters[data_key]
            count += 1
            self.read_counters[data_key] = count
            data_key = data_key + "---" + str(count)
            self.cloud_storage_write_times[data_key] = {
                "size": size,
                "duration": _redis_read_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN              
            }
            # (size, _redis_read_duration, start_time, stop_time)
        else:
            self.cloud_storage_read_times[data_key] = {
                "size": size,
                "duration": _redis_read_duration,
                "start": start_time,
                "stop": stop_time,
                "fargateARN": fargateARN              
            }
            # (size, _redis_read_duration, start_time, stop_time)