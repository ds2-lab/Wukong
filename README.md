# Wukong

A fast and efficient serverless DAG engine.

## What is Wukong?

Wukong is a serverless DAG scheduler attuned to AWS Lambda. Wukong provides decentralized scheduling using a combination of static and dynamic scheduling. Wukong supports general Python data analytics workloads. 

### Notes

When setting up Wukong, make sure to update the variables referencing the name of the AWS Lambda function used as the Wukong Task Executor. For example, in "AWS Lambda Task Executor/function.py", this is a variable *lambda_function_name* whose value should be the same as the name of the Lambda function as defined in AWS Lambda itself.

There is also a variable referencing the function's name in "Static Scheduler/distributed/batched_lambda_invoker.py" (as a keyword argument to the constructor of the BatchedLambdaInvoker object) and in "KV Store Proxy/proxy_lambda_invoker.py" (also as a keyword argument to the constructor of ProxyLambdaInvoker).

By default, Wukong is configured to run within the us-east-1 region. If you would like to use a different region, then you need to pass the "region_name" parameter to the Lambda Client objects created in "Static Scheduler/distributed/batched_lambda_invoker.py", "KV Store Proxy/proxy_lambda_invoker.py", "KV Store Proxy/proxy.py", "AWS Lambda Task Executor/function.py", and "Static Scheduler/distributed/scheduler.py". 

## Code Examples

In the following examples, modifying the value of the *chunks* parameter will essentially change the granularity of the tasks generated in the DAG. Essentially, *chunks* specifies how the initial input data is partitioned. Increasing the size of *chunks* will yield fewer individual tasks, and each task will operate over a large proportion of the input data. Decreasing the size of *chunks* will result in a greater number of individual tasks, with each task operating on a smaller portion of the input data. 

### LocalCluster Overview
```
LocalCluster(object):
  host : string
    The public DNS IPv4 address associated with the EC2 instance on which the Scheduler process is executing, along with the port on 
    which the Scheduler is listening. The format of this string should be "IPv4:port". 
  n_workers : int,
    Artifact from Dask. Leave this at zero.
  proxy_adderss : string,
    The public DNS IPv4 address associated with the EC2 instance on which the KV Store Proxy process is executing.
  proxy_port : 8989,
    The port on which the KV Store Proxy process is listening.
  redis_endpoints : list of tuples of the form (string, int)
    List of the public DNS IPv4 addresses and ports on which KV Store (Redis) instances are listening. The format
    of this list should be [("IP_1", port_1), ("IP_2", port_2), ..., ("IP_n", port_n)] 
  num_lambda_invokers : int
    This value specifies how many 'Initial Task Executor Invokers' should be created by the Scheduler. The 'Initial Task 
    Executor Invokers' are processes that are used by the Scheduler to parallelize the invocation of Task Executors
    associated with leaf tasks. These are particularly useful for large workloads with a big number of leaf tasks.
  max_task_fanout : int
    This specifies the size of a "fanout" required for a Task Executor to utilize the KV Store Proxy for parallelizing downstream
    task invocation. The principle here is the same as with the initial task invokers. Our tests found that invoking Lambda functions
    takes about 50ms on average. As a result, if a given Task T has a large fanout (i.e., there are a large number of downstream tasks 
    directly dependent on T), then it may be advantageous to parallelize the invocation of these downstream tasks.
```

### SVD of 'Tall-and-Skinny' Matrix 
```python
import dask.array as da
from distributed import LocalCluster, Client
local_cluster = LocalCluster(host = "ec2-203-0-113-25.compute-1.amazonaws.com:8786",
                             n_workers = 0,
                             proxy_address = "ec2-204-0-113-25.compute-1.amazonaws.com",
                             proxy_port = 8989,
                             redis_endpoints = [("ec2-205-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-206-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-207-0-113-25.compute-1.amazonaws.com", 6379)],
                             num_lambda_invokers = 10,
                             max_task_fanout = 10)
client = Client(local_cluster)

# Compute the SVD of 'Tall-and-Skinny' Matrix 
X = da.random.random((200000, 1000), chunks=(10000, 1000))
u, s, v = da.linalg.svd(X)

# Start the computation.
v.compute()
```

### SVD of Square Matrix with Approximation Algorithm
```python
import dask.array as da
from distributed import LocalCluster, Client
local_cluster = LocalCluster(host = "ec2-203-0-113-25.compute-1.amazonaws.com:8786",
                             n_workers = 0,
                             proxy_address = "ec2-204-0-113-25.compute-1.amazonaws.com",
                             proxy_port = 8989,
                             redis_endpoints = [("ec2-205-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-206-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-207-0-113-25.compute-1.amazonaws.com", 6379)],
                             num_lambda_invokers = 10,
                             max_task_fanout = 10)
client = Client(local_cluster)

# Compute the SVD of 'Tall-and-Skinny' Matrix 
X = da.random.random((10000, 10000), chunks=(2000, 2000))
u, s, v = da.linalg.svd_compressed(X, k=5)

# Start the computation.
v.compute()
```

### Tree Reduction
``` python
from dask import delayed 
import operator 
from distributed import LocalCluster, Client
local_cluster = LocalCluster(host = "ec2-203-0-113-25.compute-1.amazonaws.com:8786",
                             n_workers = 0,
                             proxy_address = "ec2-204-0-113-25.compute-1.amazonaws.com",
                             proxy_port = 8989,
                             redis_endpoints = [("ec2-205-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-206-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-207-0-113-25.compute-1.amazonaws.com", 6379)],
                             num_lambda_invokers = 10,
                             max_task_fanout = 10)
client = Client(local_cluster)

L = range(1024)
while len(L) > 1:
  L = list(map(delayed(operator.add), L[0::2], L[1::2]))

# Start the computation.
L[0].compute()
```

### GEMM (Matrix Multiplication) 
``` python
import dask.array as da
from distributed import LocalCluster, Client
local_cluster = LocalCluster(host = "ec2-203-0-113-25.compute-1.amazonaws.com:8786",
                             n_workers = 0,
                             proxy_address = "ec2-204-0-113-25.compute-1.amazonaws.com",
                             proxy_port = 8989,
                             redis_endpoints = [("ec2-205-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-206-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-207-0-113-25.compute-1.amazonaws.com", 6379)],
                             num_lambda_invokers = 10,
                             max_task_fanout = 10)
client = Client(local_cluster)

x = da.random.random((10000, 10000), chunks = (1000, 1000))
y = da.random.random((10000, 10000), chunks = (1000, 1000))
z = da.matmul(x, y)

# Start the computation.
z.compute() 
```

### Parallelizing Prediction (sklearn.svm.SVC)
``` python
import pandas as pd
import seaborn as sns
import sklearn.datasets
from sklearn.svm import SVC

import dask_ml.datasets
from dask_ml.wrappers import ParallelPostFit
from distributed import LocalCluster, Client
local_cluster = LocalCluster(host = "ec2-203-0-113-25.compute-1.amazonaws.com:8786",
                             n_workers = 0,
                             proxy_address = "ec2-204-0-113-25.compute-1.amazonaws.com",
                             proxy_port = 8989,
                             redis_endpoints = [("ec2-205-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-206-0-113-25.compute-1.amazonaws.com", 6379),
                                                ("ec2-207-0-113-25.compute-1.amazonaws.com", 6379)],
                             num_lambda_invokers = 10,
                             max_task_fanout = 10)
client = Client(local_cluster)

X, y = sklearn.datasets.make_classification(n_samples=1000)
clf = ParallelPostFit(SVC(gamma='scale'))
clf.fit(X, y)

X, y = dask_ml.datasets.make_classification(n_samples=800000,
                                            random_state=800000,
                                            chunks=800000 // 20)

# Start the computation.
clf.predict(X).compute()

```
