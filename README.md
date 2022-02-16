# Wukong

[![Documentation Status](https://readthedocs.org/projects/leap-wukong/badge/?version=latest)](https://leap-wukong.readthedocs.io/en/latest/?badge=latest)

A high-performance and highly scalable serverless parallel computing framework. 

## What is Wukong?

Wukong is a serverless parallel computing framework attuned to FaaS platforms such as AWS Lambda. Wukong provides decentralized scheduling using a combination of static and dynamic scheduling. Wukong supports general Python data analytics workloads at any scale. 

![Architecture](https://i.imgur.com/QDqMiFs.png "Wukong's Architecture")

## Publications

First Paper: In Search of a Fast and Efficient Serverless DAG Engine (Appeared at PDSW '19)
https://arxiv.org/abs/1910.05896

Latest Paper: Wukong: A Scalable and Locality-Enhanced Framework for Serverless Parallel Computing (Appeared at ACM SoCC '20) https://arxiv.org/abs/2010.07268 .
If you use our source code for a publication or project, please cite the paper using this [bibtex](#to-cite-wukong).

This branch contains the source code of Wukong corresponding to the SoCC 2020 publication, which is a later version than the PDSW paper referenced above.

## Installation 

Refer to the documentation in the `setup/` directory for information on installing Wukong.

## Code Overview/Explanation 

*This section is currently under development...*

### The Static Scheduler

Generally speaking, a user submits a job by calling the `.compute()` function on an underlying Dask collection. Support for Dask's asynchronous `client.compute()` API is coming soon.

When the `.compute()` function is called, the `update_graph()` function is called within the static Scheduler, specifically in **scheduler.py**. This function is responsible for adding computations to the Scheduler's internal graph. It's triggered whenever a Client calls `.submit()`, `.map()`, `.get()`, or `.compute()`. The depth-first search (DFS) method is defined with the `update_graph` function, and the DFS also occurs during the `update_graph` function's execution. 

Once the DFS has completed, the Scheduler will serialize all of the generated paths and store them in the KV Store (Redis). Next, the Scheduler will begin the computation by submitting the leaf tasks to the `BatchedLambdaInvoker` object (which is defined in **batched_lambda_invoker.py**. The "Leaf Task Invoker" processes are defined within the `BatchedLambdaInvoker` class as the `invoker_polling_process` function. Additionally, the `_background_send` function is running asynchronously on an interval (using [Tornado](https://www.tornadoweb.org/en/stable/)). This function takes whatever tasks have been submitted by the Scheduler and divdes them up among itself and the Leaf Task Invoker processes, which then invoke the leaf tasks. 

The Scheduler listens for results from Lambda using a "Subscriber Process", which is defined by the `poll_redis_process` function. This process is created in the Scheduler's `start` function. (All of this is defined in **scheduler.py**.) The Scheduler is also executing the `consume_redis_queue()` function asynchronously (i.e., on the [Tornado IOLoop](https://www.tornadoweb.org/en/stable/ioloop.html)). This function processes whatever messages were received by the aforementioned "Subscriber Process(es)". Whenever a message is processed, it is passed to the `result_from_lambda()` function, which begins the process of recording the fact that a "final result" is available. 

### The KV Store Proxy

This component is used to parallelize Lambda function invocations in the middle of a workload's execution.

...

### The AWS Lambda Task Executor

The Task Executors are responsible for executing tasks and performing dynamic scheduling. 

...

### Developer Setup Notes

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
local_cluster = LocalCluster(host='0.0.0.0:8786',
                  proxy_address = '3.83.198.204', 
                  num_fargate_nodes = 10) 
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
local_cluster = LocalCluster(host='0.0.0.0:8786',
                  proxy_address = '3.83.198.204', 
                  num_fargate_nodes = 10) 
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
local_cluster = LocalCluster(host='0.0.0.0:8786',
                  proxy_address = '3.83.198.204', 
                  num_fargate_nodes = 10) 
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
local_cluster = LocalCluster(host='0.0.0.0:8786',
                  proxy_address = '3.83.198.204', 
                  num_fargate_nodes = 10) 
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
local_cluster = LocalCluster(host='0.0.0.0:8786',
                  proxy_address = '3.83.198.204', 
                  num_fargate_nodes = 10) 
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

## To Cite Wukong

```
@inproceedings{10.1145/3419111.3421286,
author = {Carver, Benjamin and Zhang, Jingyuan and Wang, Ao and Anwar, Ali and Wu, Panruo and Cheng, Yue},
title = {Wukong: A Scalable and Locality-Enhanced Framework for Serverless Parallel Computing},
year = {2020},
isbn = {9781450381376},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3419111.3421286},
doi = {10.1145/3419111.3421286},
abstract = {Executing complex, burst-parallel, directed acyclic graph (DAG) jobs poses a major challenge for serverless execution frameworks, which will need to rapidly scale and schedule tasks at high throughput, while minimizing data movement across tasks. We demonstrate that, for serverless parallel computations, decentralized scheduling enables scheduling to be distributed across Lambda executors that can schedule tasks in parallel, and brings multiple benefits, including enhanced data locality, reduced network I/Os, automatic resource elasticity, and improved cost effectiveness. We describe the implementation and deployment of our new serverless parallel framework, called Wukong, on AWS Lambda. We show that Wukong achieves near-ideal scalability, executes parallel computation jobs up to 68.17X faster, reduces network I/O by multiple orders of magnitude, and achieves 92.96% tenant-side cost savings compared to numpywren.},
booktitle = {Proceedings of the 11th ACM Symposium on Cloud Computing},
pages = {1–15},
numpages = {15},
location = {Virtual Event, USA},
series = {SoCC '20}
}
```
