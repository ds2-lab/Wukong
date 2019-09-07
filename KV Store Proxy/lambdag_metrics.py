class TaskExecutionBreakdown(object):
    # Basically just a container for the breakdown of a task's execution on a Lambda function. 
    # There are some imperfections in the record-taking. For 'dependency_checking', this is only 
    # collected for "Begin" tasks. 
    def __init__(
         self,
         task_key,
         dependency_checking = 0, 
         redis_read_time = 0, 
         dependency_processing = 0, 
         task_start_time = 0,
         task_execution = 0, 
         task_end_time = 0,
         redis_write_time = 0, 
         process_downstream_tasks_time = 0, 
         invoking_downstream_tasks = 0,
         total_time_spent_on_this_task = 0):
        self.task_key = task_key
        self.dependency_checking = dependency_checking
        self.redis_read_time = redis_read_time
        self.dependency_processing = dependency_processing
        self.task_start_time = task_start_time
        self.task_end_time = task_end_time
        self.task_execution = task_execution
        self.redis_write_time = redis_write_time
        self.process_downstream_tasks_time = process_downstream_tasks_time
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.total_time_spent_on_this_task = total_time_spent_on_this_task

class LambdaExecutionBreakdown(object):
    def __init__(
         self,
         start_time = 0,
         process_path_time = 0,
         process_task_time = 0,
         process_downstream_tasks_time = 0,
         redis_read_time = 0,
         redis_write_time = 0,
         invoking_downstream_tasks = 0,
         number_of_tasks_executed = 0,
         total_duration = 0):
        self.start_time = start_time
        self.process_path_time = process_path_time
        self.process_task_time = process_task_time
        self.process_downstream_tasks_time = process_downstream_tasks_time
        self.redis_read_time = redis_read_time
        self.redis_write_time = redis_write_time
        self.invoking_downstream_tasks = invoking_downstream_tasks
        self.number_of_tasks_executed = number_of_tasks_executed
        self.total_duration = total_duration
