Wukong Design
===================

.. toctree::
   architecture

   core

*******************
Design Introduction
*******************

Wukong is a serverless-oriented, decentralized, locality-aware, and cost-effective parallel computing framework. The key insight of Wukong is that partitioning the work of a centralized scheduler (i.e., tracking task completions, identifying and dispatching ready tasks, etc.) across a large number of Lambda executors, can greatly improve performance by permitting tasks to be scheduled in parallel, reducing resource contention during scheduling, and making task scheduling data locality-aware, with automatic resource elasticity and improved cost effectiveness.

Scheduling is decentralized by partitioning a DAG into multiple, possibly overlapping, subgraphs. Each subgraph is assigned to a task Executor (implemented as an AWS Lambda function runtime) that is responsible for scheduling and executing tasks in its assigned subgraph. This decentralization brings multiple key benefits.

The design consists of three major components: a static schedule generator which runs on Amazon EC2, a pool of Lambda Executors, and a storage cluster.

Enhanced Data Locality & Reduced Resource Contention
----------------------------------------------------

Decentralization improves the data locality of scheduling. Unlike PyWren and numpywren, which require executors to perform network I/Os to obtain each task they execute (since numpywren’s task executor is completely stateless), Wukong preserves task dependency information on the Lambda side. This allows Lambda executors to cache intermediate data and schedule the downstream tasks in their subgraph locally, i.e., without constant remote interaction with a centralized scheduler.

Harnessing Scale & Local Optimization Opportunities
---------------------------------------------------

Decentralizing scheduling allows an Executor to make local data-aware scheduling decisions about the level of task granularity (or parallelism) appropriate for its subgraph. Agile executors can scale out compute resources in the face of burst-parallel workloads by partitioning their subgraphs into smaller graphs that are assigned to other executors for an even higher level of parallel task scheduling and execution. Alternately, an executor can execute tasks locally, when the cost of data communication between the tasks outweighs the benefit of parallel execution.

Automatic Resource Elasticity & Improved Cost Effectiveness
-----------------------------------------------------------

Decentralization does not require users to explicitly tune the number of active Lambdas running as workers
and thus is easier to use, more cost effective, and more resource efficient.

********************
Scheduling in Wukong
********************

Scheduling in Wukong is decentralized and uses a combination of **static** and **dynamic scheduling**. A **static schedule** is a subgraph of the DAG. Each static schedule is assigned to a separate Executor. 

An Executor uses **dynamic scheduling** to enforce the data dependencies of the tasks in its static schedule. An Executor can invoke additional Executors to increase task parallelism, or cluster tasks to eliminate any communication delay between them. Executors store intermediate task results in an elastic in-memory key-value storage (KVS) cluster (hosted using AWS Fargate) and job-related metadata (e.g., counters) in a separate KVS that we call metadata store (MDS).

Static Scheduling
=================

Wukong users submit a Python computing job to the DAG generator, which uses the Dask library to convert the job into a DAG. The Static-Schedule Generator generates *static schedules* from the DAG.

Task Execution & Dynamic Scheduling
===================================

Wukong workflow execution starts when the static scheduler’s Initial-Executor Invokers assign each static schedule produced by the Static-Schedule Generator to a separate Executor. Recall that each static schedule begins with one of the leaf node tasks in the DAG. The InitialExecutor invokes these "leaf node" Executors in parallel. After executing its leaf node task, each Executor then executes the tasks along a single path through its schedule. 

An Executor may execute a sequence of tasks before it reaches a fan-out operation with more than one out edge or it reaches a fan-in operation. For such a sequence of tasks, there is no communication required for making the output of the earlier tasks available to later tasks for input. All intermediate task outputs are cached in the Executor’s local memory with inherently enhanced data locality. Executors also look ahead to see what data their tasks may need, which allows them to discard data in their local memory that is no longer needed.