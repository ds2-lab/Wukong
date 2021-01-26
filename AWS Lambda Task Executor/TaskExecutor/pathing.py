import uuid 

# Used when mapping PathNode --> Fargate Task with a dictionary. These are the keys.
FARGATE_ARN_KEY = "taskARN"
FARGATE_ENI_ID_KEY = "eniID"
FARGATE_PUBLIC_IP_KEY = "publicIP"

class Path(object):
    def __init__(self, tasks, task_map, next_paths, previous_paths, tasks_to_fargate_nodes):
        """Represents a path in a DAG starting at an arbitrary node and ending at a root node.

        Paths are used by Workers to execute tasks in the DAG. The Path contains all information required to execute
        tasks (ideally). In some circumstances, workers will need to grab the path from Redis. The worker may also
        need to grab other paths invoked by the current path from Redis (and then send these paths to the Lambda invocations). 
        Another option is passing the Redis key to the Lambda invocation, and the new Lambda function would just grab its path from Redis.

        AWS Lambda invocations are limited to a 256KB payload so that is what we are constrained by in terms of sending paths directly
        to workers versus grabbing paths from Redis.

        Attributes:
            tasks:                                    List of tasks 
            task_map {key:PathNode}:                  Map of keys to path nodes.
            next_paths [Path]:                        List of paths that begin where this path ends.
            previous_paths [Path]:                    List of paths that end where this path begins.
            tasks_to_fargate_nodes {key --> dict}:    Mapping of each task in the Path to a Fargate node.
        """
        
        self.tasks = tasks or []
        self.task_map = task_map or dict()
        self.next_paths = next_paths or []
        self.previous_paths = previous_paths or []
        self.id = uuid.uuid4().__str__()
        self.tasks_to_fargate_nodes = tasks_to_fargate_nodes or {}
        
    def __eq__(self, value):
        """Overrides the default implementation of equals."""
        if isinstance(value, Path):
            return self.id == value.id 
        return False 

    def get_start(self):
        """ Return the beginning of the path, or None if the path is empty."""
        if len(self.tasks) > 0:
            return self.tasks[0]
        return None

    def add_node(self, key, node):
        """ Add a new task to this path."""

        self.task_map[key] = node 
        self.tasks.append(node)

    def add_next_path(self, path):
        """ Add a 'next path' entry to this path."""
        self.next_paths.append(path)

    def add_previous_path(self, path):
        """ Add a 'previous path' entry to this path."""
        self.previous_paths.append(path)

    def length(self):
        """Return the size of this path, or the number of tasks within the path."""
        return self.get_length()

    def get_length(self):
        """Return the size of this path, or the number of tasks within the path."""
        return len(self.tasks)

    def task_in_path(self, task_state):
        # Since nodes are equal if they just represent the same task, we can 
        # create a dummy path node to check if this task state is in the path.
        tempNode = PathNode(task_state, task_state.key, None, None, None, False)
        return tempNode in self.tasks

    def get_index_of_pathnode(self, node):
        return self.tasks.index(node)
    
    def get_fargate_ip(self, task_key):
        """ Return the Fargate IP address to which the given task_key is mapped.

            Args:
                task_key (String) - The unique key of some task contained in this Path.
            Returns:
                String - The IP address of the Fargate instance associated with the given task key."""
        if task_key in self.tasks_to_fargate_nodes:
            return self.tasks_to_fargate_nodes[task_key]
        else:
            raise ValueError("There is no entry in self.tasks_to_fargate_nodes for task key '{}'".format(task_key))

class PathNode(object):
    """ Represents a node in a path. 
        
        Two path nodes are considered equal if they represent the same task.
        
        Attributes:
            task_payload   - Dictionary: Contains information about the task required for execution (code, dependencies, dependents, etc.) 
            task_key       - String:     The unique identifier for the task associated with this node (also contained within 'task_payload' under "key")
            invoke         - [PathNode]: List of keys of PathNode objects that should be invoked (assuming dependencies are available) when this node finishes execution.
            become         - String:     The key of the PathNode object that should be executed on the same Lambda as this one, once this node finishes execution. 
            starts_at      - String:     The key of the PathNode/task located at the beginning of the path on which this node is located.
            fargate_node   - Dictionary: Information about the Fargate node mapped to this task. Includes ENI ID, PublicIP, and ARN.
            is_big         - bool:       Updated at runtime. This flag indicates whether or not this PathNode is associated with a "large" task, meaning the output
                                         (i.e., intermediate data) generated by the task is greater than the user-defined 'large task threshold'. This value will
                                         obviously not be updated remotely (i.e., for serialized instances of this class stored in Redis).                                        
            scheduler_id   - string:     Uniquely identifies the instance of the Scheduler class which generated this PathNode. Useful for if we restart Wukong
                                         while some Lambdas are still running (and thus generating CloudWatch logs). We can distinguish which Lambdas are associated
                                         with which instance of Wukong.
            dep_index_map   - dict:      Map of the indices of the bits of the dependency counters to which this PathNode/task is associated.        
            update_graph_id - string:    Uniquely identifies the call to update_graph() which generated this PathNode. Used for similar purposes as scheduler_id.
        """
    def __init__(self, task_payload, task_key, path, invoke, become, fargate_node, scheduler_id = -1, update_graph_id = -2, dep_index_map = -1, use_proxy = False, starts_at = None):
        self.task_payload = task_payload or None
        self.task_key = task_key
        self.invoke = invoke or []
        self.path = path 
        self.become = become or None 
        self.use_proxy = use_proxy
        self.starts_at = None
        self.fargate_node = fargate_node
        self.is_big = False
        self.scheduler_id = scheduler_id
        self.update_graph_id = update_graph_id
        self.dep_index_map = dep_index_map

    def getFargatePublicIP(self):
        """ Return the public IPv4 address of the Fargate task to which this PathNode is mapped."""
        return self.fargate_node[FARGATE_PUBLIC_IP_KEY]
    
    def getFargateTaskARN(self):
        """ Return the task ARN of the Fargate task to which this PathNode is mapped."""
        return self.fargate_node[FARGATE_ARN_KEY]
    
    def getFargateEniID(self):
        """ Return the ENI ID of the Elastic Network Interface associated with Fargate task to which this PathNode is mapped."""
        return self.fargate_node[FARGATE_ENI_ID_KEY]

    def add_downstream_task(self, node):
        """Add a downstream task to either the 'invoke' list or as the 'become' node. 
           The 'become' node should have the fewest number of dependencies as this is 
           preferable (i.e. just one dependency is a guaranteed 'become' in that there are
           no other tasks that could invoke a task with one dependency)."""
        # If we do not have a 'become' yet, then just set this to become.
        if self.become is None:
            self.become = node
            return 
        current_deps = len(self.become.task_payload["dependencies"])
        # If the new task has fewer dependencies than our current 'become' task, then use the new one.
        if len(node.task_payload["dependencies"]) < current_deps:
            self.invoke.append(self.become)
            self.become = node 
        else:
            self.invoke.append(node)

    def get_task_key(self):
        return self.task_key

    def num_downstream_tasks(self):
       num_tasks = 0
       if self.become is not None:
          num_tasks += 1
       num_tasks += len(self.invoke)
       return num_tasks 
    
    def get_downstream_tasks(self):
       """Return a list containing all downstream tasks of this node (which includes the 'become' node and any 'invoke' nodes)."""
       downstream_tasks = [] 
       if self.become is not None:
          downstream_tasks.append(self.become)
       downstream_tasks.extend(self.invoke)
       return downstream_tasks

    def __key(self):
        return self.task_key

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, value):
        """Overrides the default implementation"""
        if isinstance(value, PathNode):
            return self.get_task_key() == value.get_task_key()
        return False

    def to_string_no_task_payload(self):
        invoke_str = ""
        become_str = ""
        for key in self.invoke: 
            invoke_str = invoke_str + "PathNode for " + key + ", "
        if self.become is not None:
            become_str = become_str + "PathNode for " + self.become + ", "
        str = "[PathNode]\nTask Key: {}\n\t\tInvoke: [{}]\n\t\tBecome:".format(self.get_task_key(), invoke_str)
        if self.fargate_node is not None:
            str = str + "[{}]\n\t\tFargate ARN: {}, Fargate ENI ID: {}, Fargate PublicIP: {}".format(become_str,
                                                                                             self.fargate_node[FARGATE_ARN_KEY],
                                                                                             self.fargate_node[FARGATE_ENI_ID_KEY],
                                                                                             self.fargate_node[FARGATE_PUBLIC_IP_KEY])
        return str         

    def __str__(self):
        """Overrides default implementation of toString"""
        invoke_str = ""
        become_str = ""
        for key in self.invoke: 
            invoke_str = invoke_str + "PathNode for " + key + ", "
        if self.become is not None:
            become_str = become_str + "PathNode for " + self.become + ", "
        str = "[PathNode]\nTask Key: {}\n\t\tTask Payload: {}\n\t\tInvoke: [{}]\n\t\tBecome:".format(self.get_task_key(), self.task_payload, invoke_str)
        if self.fargate_node is not None:
            str = str + "[{}]\n\t\tFargate ARN: {}, Fargate ENI ID: {}, Fargate PublicIP: {}".format(become_str,
                                                                                             self.fargate_node[FARGATE_ARN_KEY],
                                                                                             self.fargate_node[FARGATE_ENI_ID_KEY],
                                                                                             self.fargate_node[FARGATE_PUBLIC_IP_KEY])
        return str 
