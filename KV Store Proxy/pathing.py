import uuid 

class Path(object):
    def __init__(self, tasks, task_map, next_paths, previous_paths):
        """Represents a path in a DAG starting at an arbitrary node and ending at a root node.

        Paths are used by Workers to execute tasks in the DAG. The Path contains all information required to execute
        tasks (ideally). In some circumstances, workers will need to grab the path from Redis. The worker may also
        need to grab other paths invoked by the current path from Redis (and then send these paths to the Lambda invocations). 
        Another option is passing the Redis key to the Lambda invocation, and the new Lambda function would just grab its path from Redis.

        AWS Lambda invocations are limited to a 256KB payload so that is what we are constrained by in terms of sending paths directly
        to workers versus grabbing paths from Redis.

        Attributes:
            tasks:                     List of tasks 
            task_map {key:PathNode}:   Map of keys to path nodes.
            next_paths [Path]:         List of paths that begin where this path ends.
            previous_paths [Path]:     List of paths that end where this path begins.
        """
        
        self.tasks = tasks or []
        self.task_map = task_map or dict()
        self.next_paths = next_paths or []
        self.previous_paths = previous_paths or []
        self.id = uuid.uuid4().__str__()
        
    def __eq__(self, value):
        """Overrides the default implementation of equals."""
        if isinstance(value, Path):
            return self.id == value.id 
        return false 

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

class PathNode(object):
    """ Represents a node in a path. 
        
        Two path nodes are considered equal if they represent the same task.
        
        Attributes:
            task_payload   - Dictionary: Contains information about the task required for execution (code, dependencies, dependents, etc.) 
            task_key       - String:     The unique identifier for the task associated with this node (also contained within 'task_payload' under "key")
            invoke         - [PathNode]: List of keys of PathNode objects that should be invoked (assuming dependencies are available) when this node finishes execution.
            become         - PathNode:   The key of the PathNode object that should be executed on the same Lambda as this one, once this node finishes execution. 
        """
    def __init__(self, task_payload, task_key, path, invoke, become, use_proxy = False):
        self.task_payload = task_payload or None
        self.task_key = task_key
        self.invoke = invoke or []
        self.path = path 
        self.become = become or None 
        self.use_proxy = use_proxy

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

    def __eq__(self, value):
        """Overrides the default implementation"""
        if isinstance(value, PathNode):
            return self.get_task_key() == value.get_task_key()
        return False

    def __str__(self):
        """Overrides default implementation of toString"""
        invoke_str = ""
        become_str = ""
        for key in self.invoke: 
            invoke_str = invoke_str + "PathNode for Task " + key + ", "
        if self.become is not None:
            become_str = become_str + "PathNode for Task " + self.become + ", "
        str = "[PathNode]\nTask State (key): {}\n\t\tInvoke: [{}]\n\t\tBecome: [{}]".format(self.get_task_key(), invoke_str, become_str)
        return str 
