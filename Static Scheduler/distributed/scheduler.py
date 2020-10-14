from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque, OrderedDict
import datetime
from datetime import timedelta
from functools import partial
import itertools
import traceback
import json
import ujson 
import logging
from numbers import Number
import operator
import os
import pickle
import random
import six
import uuid
import sys
import hashlib
import string
import socket
import numpy as np

import redis 
from uhashring import HashRing 

import weakref
import cloudpickle
import base64 
import yaml
import boto3

import psutil
import sortedcontainers

import time as pythontime

try:
    from cytoolz import frequencies, merge, pluck, merge_sorted, first
except ImportError:
    from toolz import frequencies, merge, pluck, merge_sorted, first
from toolz import valmap, second, compose, groupby
from tornado import gen
from tornado.gen import Return
from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient
import multiprocessing
from multiprocessing import Pipe, Process
import queue 

import dask

import sys, os
sys.path.insert(0, os.path.abspath('..'))
from pathing import Path, PathNode
from wukong_metrics import TaskExecutionBreakdown, LambdaExecutionBreakdown

from .protocol import dumps
from .comm.utils import from_frames
from .batched import BatchedSend
from .comm import (
    normalize_address,
    resolve_address,
    get_address_host,
    unparse_host_port,
)
from .batched_lambda_invoker import BatchedLambdaInvoker
from .comm.addressing import address_from_user_args
from .compatibility import finalize, unicode, Mapping, Set
from .core import rpc, connect, send_recv, clean_exception, CommClosedError
from . import profile
from .metrics import time
from .node import ServerNode
from .proctitle import setproctitle
from .security import Security
from .utils import (
    All,
    ignoring,
    get_ip,
    get_fileno_limit,
    log_errors,
    key_split,
    validate_key,
    no_default,
    DequeHandler,
    parse_timedelta,
    parse_bytes,
    PeriodicCallback,
    shutting_down,
)
from .utils_comm import scatter_to_workers, gather_from_workers
from .utils_perf import enable_gc_diagnosis, disable_gc_diagnosis

from .publish import PublishExtension
from .queues import QueueExtension
from .recreate_exceptions import ReplayExceptionScheduler
from .lock import LockExtension
from .pubsub import PubSubSchedulerExtension
from .stealing import WorkStealing
from .variable import VariableExtension

logger = logging.getLogger(__name__)

ENCODING = 'utf-8' 

# Used when mapping PathNode --> Fargate Task with a dictionary. These are the keys.
FARGATE_ARN_KEY = "taskARN"
FARGATE_ENI_ID_KEY = "eniID"
FARGATE_PUBLIC_IP_KEY = "publicIP"
FARGATE_PRIVATE_IP_KEY = "privateIpv4Address"

ECS_SERVICE_NAME = "WukongStorageService"
# If the task was created by the Wukong service, then its group will be the prefix "service:" with the name of the service.
# The 'group' property is used to organize the Fargate tasks.
# Additionally, the Proxy can just query ECS for all of the tasks in this group; it doesn't need to be explicitly passed
# the IP addresses for Fargate nodes or anything like that. It can discover the Redis Fargate nodes automatically via this metadata.
FARGATE_TASK_GROUP = "service:" + ECS_SERVICE_NAME

# Doesn't really do anything right now. It's added to the service we create for Fargate, but we don't do anything with it.
FARGATE_TASK_TAG = "Wukong"

# Keys for Fargate metrics.
FARGATE_NUM_SELECTED = "NumSelected"

# The maximum number of tasks ECS will allow us to launch.
MAX_FARGATE_TASKS = 250

# Default number of Fargate nodes to use.
DEFAULT_NUM_FARGATE_NODES = 25

# This string is appended to the end of task keys to get the Redis key for the associated task's dependency counter. 
DEPENDENCY_COUNTER_SUFFIX = "---dep-counter"

# This string is appended to the end of task keys (that are located at the start of a Path/static schedule) to get the Redis key for the associated Path object. 
PATH_KEY_SUFFIX = "---path"

ITERATION_COUNTER_SUFFIX = "---iteration"

# Appended to the end of a task key to store fargate node metadata in Redis.
FARGATE_DATA_SUFFIX = "---fargate"

# Leaf Task Lambdas will subscribe to a Redis Pub/Sub channel prefixed by this. The suffix will be the corresponding leaf task key.
#leaf_task_channel_prefix = "__keyspace@0__:"

# Keys used for the 'op' field in messages from AWS Lambda functions.
EXECUTED_TASK_KEY = "executed-task"
TASK_ERRED_KEY = "task-erred"
EXECUTING_TASK_KEY = "executing-task"
LAMBDA_RESULT_KEY = "lambda-result"

# Used to tell Task Executors whether or not to use the Task Queue (large objects wait for tasks to become ready instead of writing data).
EXECUTOR_TASK_QUEUE_KEY = "executors-use-task-queue"

DATA_SIZE = "data-size"

# Keys associated with the storage of Lambda execution metrics in Redis.
TASK_BREAKDOWNS = "task_breakdowns"
LAMBDA_DURATIONS = "lambda_durations"

# Key used to send Redis address to Client objects.
REDIS_ADDRESS_KEY = "redis-address"

# Key used in dictionary sent to Lambdas (the dictionary contains information from Path objects).
TASK_TO_FARGATE_MAPPING = "tasks-to-fargate-mapping"

# Create a list to store the channel names.
redis_channel_names = []
redis_channel_name_prefix = "dask-workers-"

# The Lambdas are only using one channel now that the Scheduler only gets final results. The channel is hard-coded.
redis_channel_names.append(redis_channel_name_prefix + "1")

# print("There are {} cores available so we will have {} redis channels.".format(num_cores, num_channels))
# Create the channel names based on the number of cores available. 
# for i in range(0, num_channels):
#     redis_channel_names.append(redis_channel_name_prefix + str(i))
                        
ALLOWED_FAILURES = dask.config.get("distributed.scheduler.allowed-failures")

LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")
DEFAULT_DATA_SIZE = dask.config.get("distributed.scheduler.default-data-size")

DEFAULT_EXTENSIONS = [
    LockExtension,
    PublishExtension,
    ReplayExceptionScheduler,
    QueueExtension,
    VariableExtension,
    PubSubSchedulerExtension,
]

#if dask.config.get("distributed.scheduler.work-stealing"):
    #DEFAULT_EXTENSIONS.append(WorkStealing)

ALL_TASK_STATES = {"released", "waiting", "no-worker", "processing", "erred", "memory"}


class ClientState(object):
    """
    A simple object holding information about a client.

    .. attribute:: client_key: str

       A unique identifier for this client.  This is generally an opaque
       string generated by the client itself.

    .. attribute:: wants_what: {TaskState}

       A set of tasks this client wants kept in memory, so that it can
       download its result when desired.  This is the reverse mapping of
       :class:`TaskState.who_wants`.

       Tasks are typically removed from this set when the corresponding
       object in the client's space (for example a ``Future`` or a Dask
       collection) gets garbage-collected.

    """

    __slots__ = ("client_key", "wants_what", "last_seen")

    def __init__(self, client):
        self.client_key = client
        self.wants_what = set()
        self.last_seen = time()

    def __repr__(self):
        return "<Client %r>" % (self.client_key,)

    def __str__(self):
        return self.client_key


class WorkerState(object):
    """
    A simple object holding information about a worker.

    .. attribute:: address

       This worker's unique key.  This can be its connected address
       (such as ``'tcp://127.0.0.1:8891'``) or an alias (such as ``'alice'``).

    .. attribute:: processing: {TaskState: cost}

       A dictionary of tasks that have been submitted to this worker.
       Each task state is asssociated with the expected cost in seconds
       of running that task, summing both the task's expected computation
       time and the expected communication time of its result.

       Multiple tasks may be submitted to a worker in advance and the worker
       will run them eventually, depending on its execution resources
       (but see :doc:`work-stealing`).

       All the tasks here are in the "processing" state.

       This attribute is kept in sync with :attr:`TaskState.processing_on`.

    .. attribute:: has_what: {TaskState}

       The set of tasks which currently reside on this worker.
       All the tasks here are in the "memory" state.

       This is the reverse mapping of :class:`TaskState.who_has`.

    .. attribute:: nbytes: int

       The total memory size, in bytes, used by the tasks this worker
       holds in memory (i.e. the tasks in this worker's :attr:`has_what`).

    .. attribute:: ncores: int

       The number of CPU cores made available on this worker.

    .. attribute:: resources: {str: Number}

       The available resources on this worker like ``{'gpu': 2}``.
       These are abstract quantities that constrain certain tasks from
       running at the same time on this worker.

    .. attribute:: used_resources: {str: Number}

       The sum of each resource used by all tasks allocated to this worker.
       The numbers in this dictionary can only be less or equal than
       those in this worker's :attr:`resources`.

    .. attribute:: occupancy: Number

       The total expected runtime, in seconds, of all tasks currently
       processing on this worker.  This is the sum of all the costs in
       this worker's :attr:`processing` dictionary.

    .. attribute:: status: str

       The current status of the worker, either ``'running'`` or ``'closed'``

    .. attribute:: nanny: str

       Address of the associated Nanny, if present

    .. attribute:: last_seen: Number

       The last time we received a heartbeat from this worker, in local
       scheduler time.

    .. attribute:: actors: {TaskState}

       A set of all TaskStates on this worker that are actors.  This only
       includes those actors whose state actually lives on this worker, not
       actors to which this worker has a reference.

    """

    # XXX need a state field to signal active/removed?

    __slots__ = (
        "actors",
        "address",
        "has_what",
        "last_seen",
        "local_directory",
        "memory_limit",
        "metrics",
        "name",
        "nanny",
        "nbytes",
        "ncores",
        "occupancy",
        "pid",
        "processing",
        "resources",
        "services",
        "status",
        "time_delay",
        "used_resources",
    )

    def __init__(
        self,
        address=None,
        pid=0,
        name=None,
        ncores=0,
        memory_limit=0,
        local_directory=None,
        services=None,
        nanny=None,
    ):
        self.address = address
        self.pid = pid
        self.name = name
        self.ncores = ncores
        self.memory_limit = memory_limit
        self.local_directory = local_directory
        self.services = services or {}
        self.nanny = nanny

        self.status = "running"
        self.nbytes = 0
        self.occupancy = 0
        self.metrics = {}
        self.last_seen = 0
        self.time_delay = 0

        self.actors = set()
        self.has_what = set()
        self.processing = {}
        self.resources = {}
        self.used_resources = {}
 
    @property
    def host(self):
        return get_address_host(self.address)

    def clean(self):
        """ Return a version of this object that is appropriate for serialization """
        ws = WorkerState(
            address=self.address,
            pid=self.pid,
            name=self.name,
            ncores=self.ncores,
            memory_limit=self.memory_limit,
            local_directory=self.local_directory,
            services=self.services,
            nanny=self.nanny,
        )
        ws.processing = {ts.key for ts in self.processing}
        return ws

    def __repr__(self):
        return "<Worker %r, memory: %d, processing: %d>" % (
            self.address,
            len(self.has_what),
            len(self.processing),
        )

    def __str__(self):
        return self.address

    def identity(self):
        return {
            "type": "Worker",
            "id": self.name,
            "host": self.host,
            "resources": self.resources,
            "local_directory": self.local_directory,
            "name": self.name,
            "ncores": self.ncores,
            "memory_limit": self.memory_limit,
            "last_seen": self.last_seen,
            "services": self.services,
            "metrics": self.metrics,
            "nanny": self.nanny,
        }


class TaskState(object):
    """
    A simple object holding information about a task.

    .. attribute:: key: str

       The key is the unique identifier of a task, generally formed
       from the name of the function, followed by a hash of the function
       and arguments, like ``'inc-ab31c010444977004d656610d2d421ec'``.

    .. attribute:: prefix: str

       The key prefix, used in certain calculations to get an estimate
       of the task's duration based on the duration of other tasks in the
       same "family" (for example ``'inc'``).

    .. attribute:: run_spec: object

       A specification of how to run the task.  The type and meaning of this
       value is opaque to the scheduler, as it is only interpreted by the
       worker to which the task is sent for executing.

       As a special case, this attribute may also be ``None``, in which case
       the task is "pure data" (such as, for example, a piece of data loaded
       in the scheduler using :meth:`Client.scatter`).  A "pure data" task
       cannot be computed again if its value is lost.

    .. attribute:: priority: tuple

       The priority provides each task with a relative ranking which is used
       to break ties when many tasks are being considered for execution.

       This ranking is generally a 2-item tuple.  The first (and dominant)
       item corresponds to when it was submitted.  Generally, earlier tasks
       take precedence.  The second item is determined by the client, and is
       a way to prioritize tasks within a large graph that may be important,
       such as if they are on the critical path, or good to run in order to
       release many dependencies.  This is explained further in
       :doc:`Scheduling Policy <scheduling-policies>`.

    .. attribute:: state: str

       This task's current state.  Valid states include ``released``,
       ``waiting``, ``no-worker``, ``processing``, ``memory``, ``erred``
       and ``forgotten``.  If it is ``forgotten``, the task isn't stored
       in the ``tasks`` dictionary anymore and will probably disappear
       soon from memory.

    .. attribute:: dependencies: {TaskState}

       The set of tasks this task depends on for proper execution.  Only
       tasks still alive are listed in this set.  If, for whatever reason,
       this task also depends on a forgotten task, the
       :attr:`has_lost_dependencies` flag is set.

       A task can only be executed once all its dependencies have already
       been successfully executed and have their result stored on at least
       one worker.  This is tracked by progressively draining the
       :attr:`waiting_on` set.

    .. attribute:: dependents: {TaskState}

       The set of tasks which depend on this task.  Only tasks still alive
       are listed in this set.

       This is the reverse mapping of :attr:`dependencies`.

    .. attribute:: has_lost_dependencies: bool

       Whether any of the dependencies of this task has been forgotten.
       For memory consumption reasons, forgotten tasks are not kept in
       memory even though they may have dependent tasks.  When a task is
       forgotten, therefore, each of its dependents has their
       :attr:`has_lost_dependencies` attribute set to ``True``.

       If :attr:`has_lost_dependencies` is true, this task cannot go
       into the "processing" state anymore.

    .. attribute:: waiting_on: {TaskState}

       The set of tasks this task is waiting on *before* it can be executed.
       This is always a subset of :attr:`dependencies`.  Each time one of the
       dependencies has finished processing, it is removed from the
       :attr:`waiting_on` set.

       Once :attr:`waiting_on` becomes empty, this task can move from the
       "waiting" state to the "processing" state (unless one of the
       dependencies errored out, in which case this task is instead
       marked "erred").

    .. attribute:: waiters: {TaskState}

       The set of tasks which need this task to remain alive.  This is always
       a subset of :attr:`dependents`.  Each time one of the dependents
       has finished processing, it is removed from the :attr:`waiters`
       set.

       Once both :attr:`waiters` and :attr:`who_wants` become empty, this
       task can be released (if it has a non-empty :attr:`run_spec`) or
       forgotten (otherwise) by the scheduler, and by any workers
       in :attr:`who_has`.

       .. note:: Counter-intuitively, :attr:`waiting_on` and
          :attr:`waiters` are not reverse mappings of each other.

    .. attribute:: who_wants: {ClientState}

       The set of clients who want this task's result to remain alive.
       This is the reverse mapping of :attr:`ClientState.wants_what`.

       When a client submits a graph to the scheduler it also specifies
       which output tasks it desires, such that their results are not released
       from memory.

       Once a task has finished executing (i.e. moves into the "memory"
       or "erred" state), the clients in :attr:`who_wants` are notified.

       Once both :attr:`waiters` and :attr:`who_wants` become empty, this
       task can be released (if it has a non-empty :attr:`run_spec`) or
       forgotten (otherwise) by the scheduler, and by any workers
       in :attr:`who_has`.

    .. attribute:: who_has: {WorkerState}

       The set of workers who have this task's result in memory.
       It is non-empty iff the task is in the "memory" state.  There can be
       more than one worker in this set if, for example, :meth:`Client.scatter`
       or :meth:`Client.replicate` was used.

       This is the reverse mapping of :attr:`WorkerState.has_what`.

    .. attribute:: processing_on: WorkerState (or None)

       If this task is in the "processing" state, which worker is currently
       processing it.  Otherwise this is ``None``.

       This attribute is kept in sync with :attr:`WorkerState.processing`.

    .. attribute:: retries: int

       The number of times this task can automatically be retried in case
       of failure.  If a task fails executing (the worker returns with
       an error), its :attr:`retries` attribute is checked.  If it is
       equal to 0, the task is marked "erred".  If it is greater than 0,
       the :attr:`retries` attribute is decremented and execution is
       attempted again.

    .. attribute:: nbytes: int (or None)

       The number of bytes, as determined by ``sizeof``, of the result
       of a finished task.  This number is used for diagnostics and to
       help prioritize work.

    .. attribute:: type: str

       The type of the object as a string.  Only present for tasks that have
       been computed.

    .. attribute:: exception: object

       If this task failed executing, the exception object is stored here.
       Otherwise this is ``None``.

    .. attribute:: traceback: object

       If this task failed executing, the traceback object is stored here.
       Otherwise this is ``None``.

    .. attribute:: exception_blame: TaskState (or None)

       If this task or one of its dependencies failed executing, the
       failed task is stored here (possibly itself).  Otherwise this
       is ``None``.

    .. attribute:: suspicious: int

       The number of times this task has been involved in a worker death.

       Some tasks may cause workers to die (such as calling ``os._exit(0)``).
       When a worker dies, all of the tasks on that worker are reassigned
       to others.  This combination of behaviors can cause a bad task to
       catastrophically destroy all workers on the cluster, one after
       another.  Whenever a worker dies, we mark each task currently
       processing on that worker (as recorded by
       :attr:`WorkerState.processing`) as suspicious.

       If a task is involved in three deaths (or some other fixed constant)
       then we mark the task as ``erred``.

    .. attribute:: host_restrictions: {hostnames}

       A set of hostnames where this task can be run (or ``None`` if empty).
       Usually this is empty unless the task has been specifically restricted
       to only run on certain hosts.  A hostname may correspond to one or
       several connected workers.

    .. attribute:: worker_restrictions: {worker addresses}

       A set of complete worker addresses where this can be run (or ``None``
       if empty).  Usually this is empty unless the task has been specifically
       restricted to only run on certain workers.

       Note this is tracking worker addresses, not worker states, since
       the specific workers may not be connected at this time.

    .. attribute:: resource_restrictions: {resource: quantity}

       Resources required by this task, such as ``{'gpu': 1}`` or
       ``{'memory': 1e9}`` (or ``None`` if empty).  These are user-defined
       names and are matched against the contents of each
       :attr:`WorkerState.resources` dictionary.

    .. attribute:: loose_restrictions: bool

       If ``False``, each of :attr:`host_restrictions`,
       :attr:`worker_restrictions` and :attr:`resource_restrictions` is
       a hard constraint: if no worker is available satisfying those
       restrictions, the task cannot go into the "processing" state and
       will instead go into the "no-worker" state.

       If ``True``, the above restrictions are mere preferences: if no worker
       is available satisfying those restrictions, the task can still go
       into the "processing" state and be sent for execution to another
       connected worker.

    .. attribute: actor: bool

       Whether or not this task is an Actor.
    """

    __slots__ = (
        # === General description ===
        "actor",
        # Key name
        "key",
        # Key prefix (see key_split())
        "prefix",
        # How to run the task (None if pure data)
        "run_spec",
        # Alive dependents and dependencies
        "dependencies",
        "dependents",
        # Compute priority
        "priority",
        # Restrictions
        "host_restrictions",
        "worker_restrictions",  # not WorkerStates but addresses
        "resource_restrictions",
        "loose_restrictions",
        # === Task state ===
        "state",
        # Whether some dependencies were forgotten
        "has_lost_dependencies",
        # If in 'waiting' state, which tasks need to complete
        # before we can run
        "waiting_on",
        # If in 'waiting' or 'processing' state, which tasks needs us
        # to complete before they can run
        "waiters",
        # In in 'processing' state, which worker we are processing on
        "processing_on",
        # If in 'memory' state, Which workers have us
        "who_has",
        # Which clients want us
        "who_wants",
        "exception",
        "traceback",
        "exception_blame",
        "suspicious",
        "retries",
        "nbytes",
        "type",
    )

    def __init__(self, key, run_spec):
        self.key = key
        self.prefix = key_split(key)
        self.run_spec = run_spec
        self.state = None
        self.exception = self.traceback = self.exception_blame = None
        self.suspicious = self.retries = 0
        self.nbytes = None
        self.priority = None
        self.who_wants = set()
        self.dependencies = set()
        self.dependents = set()
        self.waiting_on = set()
        self.waiters = set()
        self.who_has = set()
        self.processing_on = None
        self.has_lost_dependencies = False
        self.host_restrictions = None
        self.worker_restrictions = None
        self.resource_restrictions = None
        self.loose_restrictions = False
        self.actor = None
        self.type = None

    def get_nbytes(self):
        nbytes = self.nbytes
        return nbytes if nbytes is not None else DEFAULT_DATA_SIZE

    def set_nbytes(self, nbytes):
        old_nbytes = self.nbytes
        diff = nbytes - (old_nbytes or 0)
        for ws in self.who_has:
            ws.nbytes += diff
        self.nbytes = nbytes

    def __repr__(self):
        return "<Task %r %s>" % (self.key, self.state)

    def validate(self):
        try:
            for cs in self.who_wants:
                assert isinstance(cs, ClientState), (repr(cs), self.who_wants)
            #for ws in self.who_has:
            #    assert isinstance(ws, WorkerState), (repr(ws), self.who_has)
            for ts in self.dependencies:
                assert isinstance(ts, TaskState), (repr(ts), self.dependencies)
            for ts in self.dependents:
                assert isinstance(ts, TaskState), (repr(ts), self.dependents)
            validate_task_state(self)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()

class _StateLegacyMapping(Mapping):
    """
    A mapping interface mimicking the former Scheduler state dictionaries.
    """

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return iter(self._states)

    def __len__(self):
        return len(self._states)

    def __getitem__(self, key):
        return self._accessor(self._states[key])

    def __repr__(self):
        return "%s(%s)" % (self.__class__, dict(self))


class _OptionalStateLegacyMapping(_StateLegacyMapping):
    """
    Similar to _StateLegacyMapping, but a false-y value is interpreted
    as a missing key.
    """

    # For tasks etc.

    def __iter__(self):
        accessor = self._accessor
        for k, v in self._states.items():
            if accessor(v):
                yield k

    def __len__(self):
        accessor = self._accessor
        return sum(bool(accessor(v)) for v in self._states.values())

    def __getitem__(self, key):
        v = self._accessor(self._states[key])
        if v:
            return v
        else:
            raise KeyError


class _StateLegacySet(Set):
    """
    Similar to _StateLegacyMapping, but exposes a set containing
    all values with a true value.
    """

    # For loose_restrictions

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return (k for k, v in self._states.items() if self._accessor(v))

    def __len__(self):
        return sum(map(bool, map(self._accessor, self._states.values())))

    def __contains__(self, k):
        st = self._states.get(k)
        return st is not None and bool(self._accessor(st))

    def __repr__(self):
        return "%s(%s)" % (self.__class__, set(self))


def _legacy_task_key_set(tasks):
    """
    Transform a set of task states into a set of task keys.
    """
    return {ts.key for ts in tasks}


def _legacy_client_key_set(clients):
    """
    Transform a set of client states into a set of client keys.
    """
    return {cs.client_key for cs in clients}


def _legacy_worker_key_set(workers):
    """
    Transform a set of worker states into a set of worker keys.
    """
    return {ws.address for ws in workers}


def _legacy_task_key_dict(task_dict):
    """
    Transform a dict of {task state: value} into a dict of {task key: value}.
    """
    return {ts.key: value for ts, value in task_dict.items()}


def _task_key_or_none(task):
    return task.key if task is not None else None


class Scheduler(ServerNode):
    """ Dynamic distributed task scheduler

    The scheduler tracks the current state of workers, data, and computations.
    The scheduler listens for events and responds by controlling workers
    appropriately.  It continuously tries to use the workers to execute an ever
    growing dask graph.

    All events are handled quickly, in linear time with respect to their input
    (which is often of constant size) and generally within a millisecond.  To
    accomplish this the scheduler tracks a lot of state.  Every operation
    maintains the consistency of this state.

    The scheduler communicates with the outside world through Comm objects.
    It maintains a consistent and valid view of the world even when listening
    to several clients at once.

    A Scheduler is typically started either with the ``dask-scheduler``
    executable::

         $ dask-scheduler
         Scheduler started at 127.0.0.1:8786

    Or within a LocalCluster a Client starts up without connection
    information::

        >>> c = Client()  # doctest: +SKIP
        >>> c.cluster.scheduler  # doctest: +SKIP
        Scheduler(...)

    Users typically do not interact with the scheduler directly but rather with
    the client object ``Client``.

    **State**

    The scheduler contains the following state variables.  Each variable is
    listed along with what it stores and a brief description.

    * **tasks:** ``{task key: TaskState}``
        Tasks currently known to the scheduler
    * **unrunnable:** ``{TaskState}``
        Tasks in the "no-worker" state

    * **workers:** ``{worker key: WorkerState}``
        Workers currently connected to the scheduler
    * **idle:** ``{WorkerState}``:
        Set of workers that are not fully utilized
    * **saturated:** ``{WorkerState}``:
        Set of workers that are not over-utilized

    * **host_info:** ``{hostname: dict}``:
        Information about each worker host

    * **clients:** ``{client key: ClientState}``
        Clients currently connected to the scheduler

    * **services:** ``{str: port}``:
        Other services running on this scheduler, like Bokeh
    * **loop:** ``IOLoop``:
        The running Tornado IOLoop
    * **client_comms:** ``{client key: Comm}``
        For each client, a Comm object used to receive task requests and
        report task status updates.
    * **stream_comms:** ``{worker key: Comm}``
        For each worker, a Comm object from which we both accept stimuli and
        report results
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    * **coroutines:** ``[Futures]``:
        A list of active futures that control operation
    """

    default_port = 8786
    _instances = weakref.WeakSet()

    # Scheduler __init__()
    def __init__(
        self,
        loop=None,
        delete_interval="500ms",
        synchronize_worker_interval="60s",
        services=None,
        service_kwargs=None,
        allowed_failures=ALLOWED_FAILURES,
        extensions=None,
        validate=False,
        scheduler_file=None,
        security=None,
        worker_ttl=None,
        idle_timeout=None,
        interface=None,
        host=None,
        port=8786,
        protocol=None,
        dashboard_address=None,
        proxy_address = None,
        proxy_port = None,
        num_lambda_invokers = 16,
        aws_region = 'us-east-1',
        reuse_lambdas = False,
        print_debug = False, # Print debug information
        print_level = 1, # Possible: {1, 2, 3}. Lower is more in-depth.
        reuse_existing_fargate_tasks_on_startup = True, # If there are already some Fargate tasks appropriately tagged/grouped and already running, should we just use those?
        executors_use_task_queue = True,                # Large tasks don't write data; instead, they wait for the tasks to become ready to execute locally.
        use_bit_dep_checking = False,                   # If True, use bit-method of dependency counters.
        debug_mode = False,    # When enabled, the user will step through each call to update_graph, and a significantly larger amount of debug info will print each iteration.
        lambda_debug = False,
        use_fargate = True,
        ecs_cluster_name = 'WukongFargateStorage',
        ecs_task_definition = 'WukongRedisNode:1',
        ecs_network_configuration = {
            'awsvpcConfiguration': {
                'subnets': ['subnet-0d6d83219a53e4e72'],
                'securityGroups': ['sg-0316757e47d10fce8'],
                'assignPublicIp': 'ENABLED' } },
        # ecs_network_configuration = {
        #                     'awsvpcConfiguration': {
        #                         'subnets': ['subnet-0df77d3c433e46fd0',
        #                                     'subnet-0d169f20d4fdf90a7',
        #                                     'subnet-075f20e7075f1be7d',
        #                                     'subnet-033375027137f61bd',
        #                                     'subnet-06cac58c4a8abdeec',
        #                                     'subnet-04e6391f47afa1492',
        #                                     'subnet-02e033f99a669e161',
        #                                     'subnet-0cc89d0da8726e056',
        #                                     'subnet-00789d1dc555dafa2'], 
        #                         'securityGroups': [ 'sg-0f4ea153447b2c910' ], 
        #                         'assignPublicIp': 'ENABLED' } },         
        num_fargate_nodes = DEFAULT_NUM_FARGATE_NODES, # The maximum number of Fargate tasks that can be started. Caps out at 250 for FARGATE_SPOT and 100 for FARGATE.
        max_task_fanout = 10,                          # The threshold for when a node will use the proxy to parallelize downstream task invocations.
        chunk_large_tasks = False,                     # Flag indicating whether or not Lambda functions should break up large tasks and store them in chunks.
        big_task_threshold = 200_000_000,              # The threshold, in bytes, above which an object should be broken up into chunks when stored.
        num_chunks_for_large_tasks = None,             # We break up large objects into "this" many chunks. 
                                                       # If this is 'None', then we break large objects up such that
                                                       # each chunk is size 'big_task_threshold'.
        use_invoker_lambdas_threshold = 10000,
        force_use_invoker_lambdas = False,        
        **kwargs
    ):
        self._setup_logging()
        self.proxy_address = proxy_address or ""    # Address of the redis proxy (should be same as redis, right?)

        # Max value for 'num_fargate_nodes' is 250. Min value is 1. 
        # If > 250 given, 'num_fargate_nodes' clamped to 250.
        # If < 1 given, 'num_fargate_nodes' set to 100.
        if (num_fargate_nodes > 250):
            print("[WARNING] The value given for 'num_fargate_nodes' ({}) is too high. Clamping to 250.".format(num_fargate_nodes))
            num_fargate_nodes = 250
        elif (num_fargate_nodes < 1):
            print("[WARNING] The value given for 'num_fargate_nodes' ({}) is too low. Using default of 100.".format(num_fargate_nodes))
            num_fargate_nodes = 100

        # Attributes
        self.allowed_failures = allowed_failures
        self.validate = validate
        self.status = None
        self.proc = psutil.Process()
        self.delete_interval = parse_timedelta(delete_interval, default="ms")
        self.synchronize_worker_interval = parse_timedelta(
            synchronize_worker_interval, default="ms"
        )
        self.digests = None
        self.service_specs = services or {}
        self.service_kwargs = service_kwargs or {}
        self.services = {}
        self.scheduler_file = scheduler_file
        worker_ttl = worker_ttl or dask.config.get("distributed.scheduler.worker-ttl")
        self.worker_ttl = parse_timedelta(worker_ttl) if worker_ttl else None
        idle_timeout = idle_timeout or dask.config.get(
            "distributed.scheduler.idle-timeout"
        )
        if idle_timeout:
            self.idle_timeout = parse_timedelta(idle_timeout)
        else:
            self.idle_timeout = None
        self.time_started = time()
        self.bandwidth = parse_bytes(dask.config.get("distributed.scheduler.bandwidth"))
        self.print_serialized = False 
        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("scheduler")
        self.listen_args = self.security.get_listen_args("scheduler")

        if dashboard_address is not None:
            try:
                from distributed.bokeh.scheduler import BokehScheduler
            except ImportError:
                logger.debug("To start diagnostics web server please install Bokeh")
            else:
                self.service_specs[("bokeh", dashboard_address)] = (
                    BokehScheduler,
                    (service_kwargs or {}).get("bokeh", {}),
                )

        # Communication state
        self.loop = loop or IOLoop.current()
        self.client_comms = dict()
        self.stream_comms = dict()
        self.coroutines = []
        self._worker_coroutines = []
        self._ipython_kernel = None
        
        # Task state
        self.tasks = dict()
        for old_attr, new_attr, wrap in [
            ("priority", "priority", None),
            ("dependencies", "dependencies", _legacy_task_key_set),
            ("dependents", "dependents", _legacy_task_key_set),
            ("retries", "retries", None),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(self.tasks, func))

        for old_attr, new_attr, wrap in [
            ("nbytes", "nbytes", None),
            ("who_wants", "who_wants", _legacy_client_key_set),
            ("who_has", "who_has", _legacy_worker_key_set),
            ("waiting", "waiting_on", _legacy_task_key_set),
            ("waiting_data", "waiters", _legacy_task_key_set),
            ("rprocessing", "processing_on", None),
            ("host_restrictions", "host_restrictions", None),
            ("worker_restrictions", "worker_restrictions", None),
            ("resource_restrictions", "resource_restrictions", None),
            ("suspicious_tasks", "suspicious", None),
            ("exceptions", "exception", None),
            ("tracebacks", "traceback", None),
            ("exceptions_blame", "exception_blame", _task_key_or_none),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _OptionalStateLegacyMapping(self.tasks, func))

        for old_attr, new_attr, wrap in [
            ("loose_restrictions", "loose_restrictions", None)
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacySet(self.tasks, func))

        self.generation = 0
        self._last_client = None
        self._last_time = 0
        self.unrunnable = set()

        self.n_tasks = 0
        self.task_metadata = dict()
        self.datasets = dict()
        
        # Prefix-keyed containers
        self.task_duration = {prefix: 0.00001 for prefix in fast_tasks}
        self.unknown_durations = defaultdict(set)

        # Client state
        self.clients = dict()
        for old_attr, new_attr, wrap in [
            ("wants_what", "wants_what", _legacy_task_key_set)
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(self.clients, func))
        self.clients["fire-and-forget"] = ClientState("fire-and-forget")

        # Worker state
        self.workers = sortedcontainers.SortedDict()
        for old_attr, new_attr, wrap in [
            ("ncores", "ncores", None),
            ("worker_bytes", "nbytes", None),
            ("worker_resources", "resources", None),
            ("used_resources", "used_resources", None),
            ("occupancy", "occupancy", None),
            ("worker_info", "metrics", None),
            ("processing", "processing", _legacy_task_key_dict),
            ("has_what", "has_what", _legacy_task_key_set),
        ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr, _StateLegacyMapping(self.workers, func))

        self.idle = sortedcontainers.SortedSet(key=operator.attrgetter("address"))
        self.saturated = set()

        self.total_ncores = 0
        self.total_occupancy = 0
        self.host_info = defaultdict(dict)
        self.resources = defaultdict(dict)
        self.aliases = dict()

        self._task_state_collections = [self.unrunnable]

        self._worker_collections = [
            self.workers,
            self.host_info,
            self.resources,
            self.aliases,
        ]

        self.extensions = {}
        self.plugins = []
        self.transition_log = deque(
            maxlen=dask.config.get("distributed.scheduler.transition-log-length")
        )
        self.log = deque(
            maxlen=dask.config.get("distributed.scheduler.transition-log-length")
        )
        self.worker_plugins = []
        
        worker_handlers = {
            "task-finished": self.handle_task_finished,
            "task-erred": self.handle_task_erred,
            "release": self.handle_release_data,
            "release-worker-data": self.release_worker_data,
            "add-keys": self.add_keys,
            "missing-data": self.handle_missing_data,
            "long-running": self.handle_long_running,
            "reschedule": self.reschedule,
            "redis-proxy-channels": self.handle_redis_proxy_channels,
            "debug-msg": self.handle_debug_message2
        }

        client_handlers = {
            "update-graph": self.update_graph,
            "client-desires-keys": self.client_desires_keys,
            "update-data": self.update_data,
            "report-key": self.report_on_key,
            "client-releases-keys": self.client_releases_keys,
            "heartbeat-client": self.client_heartbeat,
            "close-client": self.remove_client,
            "restart": self.restart,
        }

        self.handlers = {
            "register-client": self.add_client,
            "scatter": self.scatter,
            "register-worker": self.add_worker,
            "unregister": self.remove_worker,
            "gather": self.gather,
            "cancel": self.stimulus_cancel,
            "retry": self.stimulus_retry,
            "feed": self.feed,
            "terminate": self.close,
            "broadcast": self.broadcast,
            "proxy": self.proxy,
            "ncores": self.get_ncores,
            "has_what": self.get_has_what,
            "who_has": self.get_who_has,
            "processing": self.get_processing,
            "call_stack": self.get_call_stack,
            "profile": self.get_profile,
            "logs": self.get_logs,
            "worker_logs": self.get_worker_logs,
            "nbytes": self.get_nbytes,
            "versions": self.versions,
            "add_keys": self.add_keys,
            "rebalance": self.rebalance,
            "replicate": self.replicate,
            "start_ipython": self.start_ipython,
            "run_function": self.run_function,
            "update_data": self.update_data,
            "set_resources": self.add_resources,
            "retire_workers": self.retire_workers,
            "get_metadata": self.get_metadata,
            "set_metadata": self.set_metadata,
            "heartbeat_worker": self.heartbeat_worker,
            "get_task_status": self.get_task_status,
            "get_task_stream": self.get_task_stream,
            "register_worker_plugin": self.register_worker_plugin,
            "lambda-result": self.result_from_lambda,
            "get_fargate_info_for_task": self.get_fargate_info_for_task,
            "task-erred-lambda": self.handle_task_erred_lambda,
            "debug-msg": self.handle_debug_message2
        }
        
        # New _transitions dict that uses the Lambda-aware versions of the transition functions.
        self._transitions = {
            ("released", "waiting"): self.transition_released_waiting_lambda,
            ("waiting", "released"): self.transition_waiting_released_lambda,
            ("waiting", "processing"): self.transition_waiting_processing_lambda,
            ("waiting", "memory"): self.transition_waiting_memory_lambda,
            ("processing", "released"): self.transition_processing_released_lambda,
            ("processing", "memory"): self.transition_processing_memory_lambda,
            ("processing", "erred"): self.transition_processing_erred_lambda,
            ("no-worker", "released"): self.transition_no_worker_released_lambda,
            ("no-worker", "waiting"): self.transition_no_worker_waiting_lambda,
            ("released", "forgotten"): self.transition_released_forgotten_lambda,
            ("memory", "forgotten"): self.transition_memory_forgotten_lambda,
            ("erred", "forgotten"): self.transition_released_forgotten_lambda,
            ("erred", "released"): self.transition_erred_released_lambda,
            ("memory", "released"): self.transition_memory_released_lambda,
            ("released", "erred"): self.transition_released_erred_lambda,
        }        

        connection_limit = get_fileno_limit() / 2

        self._start_address = address_from_user_args(
            host=host,
            port=port,
            interface=interface,
            protocol=protocol,
            security=security,
        )

        super(Scheduler, self).__init__(
            handlers=self.handlers,
            stream_handlers=merge(worker_handlers, client_handlers),
            io_loop=self.loop,
            connection_limit=connection_limit,
            deserialize=False,
            connection_args=self.connection_args,
            **kwargs
        )

        self.num_lambda_invokers = num_lambda_invokers
        
        # Map from task-key --> bool where a value of True indicates that the task has finished execution.
        # Only used during debugging.
        self.completed_tasks = defaultdict(bool)
        
        # Count how many times a given task has been completed by AWS Lambda (i.e., how many times we got a notification saying it was completed).
        self.completed_task_counts = defaultdict(int)

        # Map from task-key --> bool where a value of True indicates that the task has begun execution on a Lambda.
        # Only used during debugging.
        self.executing_tasks = defaultdict(bool)

        # Count how many times a given task started executing on AWS Lambda (i.e., how many times we got a notification saying it was starting execution).
        self.executing_tasks_counters = defaultdict(int)

        # Contains the start time, end time, and duration of execution of completed tasks (when lambda_debug is enabled).
        self.completed_task_data = dict()

        self.path_nodes = dict()
        self.task_payloads = dict()

        # Sometimes Tasks are re-used. In these cases, we put their old payloads and PathNode objects here for safe-keeping.
        self.old_path_nodes = defaultdict(list)
        self.old_task_payloads = defaultdict(list)

        self.lambda_debug = lambda_debug

        # Redis instance for storing dependency counters and paths.
        self.dcp_redis = redis.StrictRedis(host = proxy_address, port = 6379, db = 0)
        self.dcp_pubsub = self.dcp_redis.pubsub()

        self.use_invoker_lambdas_threshold = use_invoker_lambdas_threshold
        self.force_use_invoker_lambdas = force_use_invoker_lambdas

        # Track info such as how many times each Fargate node has been selected.
        self.fargate_metrics = dict()

        self.aws_region = aws_region
        self.lambda_client = boto3.client('lambda', region_name=self.aws_region)
        self.ecs_client = boto3.client('ecs', region_name = self.aws_region)
        self.ec2_client = boto3.client('ec2', region_name = self.aws_region)
        self.ecs_cluster_name = ecs_cluster_name
        self.ecs_task_definition = ecs_task_definition
        self.ecs_network_configuration = ecs_network_configuration
        self.reuse_lambdas = reuse_lambdas              # Re-use Lambdas between iterations of iterative workloads.
        self.workload_fargate_tasks = defaultdict(list) # For each workload, keep a list of the Fargate tasks created so that they may be closed when we're done.
        self.num_fargate_nodes = num_fargate_nodes
        # List of timedelta objects representing the difference in time between when a Lambda invocation sent a message to the scheduler
        # and then the scheduler actually received the message.
        self.timedeltas_from_lambda = []                # times that lambda sent msg to when scheduler got it, i think
        self.times_lambda_to_processes = []             # list of times from when a lambda published a msg to when the polling processes got it
        self.times_processes_to_scheduler = []          # list of times from when a polling process sent a msg to the scheduler to when the scheduler actually got the message
        self.num_results_received_from_lambda = 0       # number of results we got back from lambda
        self.num_messages_received_from_lambda = 0      # number of messages we got back from lambda (one message may have multiple results, like results from more than one task)
        #self.lambda_diagnostic = PeriodicCallback(self.print_lambda_diagnostic_info, callback_time = 1000, io_loop = loop)
        #self.periodic_callbacks["lambda-diagnostic"] = self.lambda_diagnostic
        #self.poll_redis = PeriodicCallback(self.poll_redis_channel, callback_time = 50, io_loop = loop)
        self.poll_redis = PeriodicCallback(self.consume_redis_queue, callback_time = 5, io_loop = loop)
        self.task_execution_lengths = dict()        
        self.use_fargate = use_fargate              # If True, use Fargate cluster for Storage. Otherwise, use single Redis instance (DCP Redis).
        self.redis_channel_index = 0                # used to tell lambdas which redis pub-sub channel to use
        self.start_end_times = dict()               # start and end times for tasks
        self.debug_print = False                    # controls certain prints
        self.periodic_callbacks["poll-redis"] = self.poll_redis
        self.sum_lambda_lengths = 0                 # running sum of values in lambda_lengths 
        self.lambda_lengths = []                    # execution lengths of lambdas (ENTIRE lambdas, not just the tasks right?)
        self.proxy_port = proxy_port            # Port of the Redix proxy
        self.num_zero_processed = 0             # number of times a call to consume_redis_queue resulted in the processing of zero messages.
        self.max_task_fanout = max_task_fanout  # If a task has this many or more downstream tasks, it will use Redis proxy to invoke them.
        self.proxy_comm = None
        self.big_task_threshold = big_task_threshold     # The threshold, in bytes, above which an object should be broken up into chunks when stored.
        self.chunk_large_tasks = chunk_large_tasks # Flag indicating whether or not Lambda functions should break up large tasks and store them in chunks.
        self.num_chunks_for_large_tasks = num_chunks_for_large_tasks
        self.print_debug = print_debug  # Print debug info to console?
        self.print_level = print_level  # Level of debugging
        self.reuse_existing_fargate_tasks_on_startup = reuse_existing_fargate_tasks_on_startup # Reuse existing, already-running Fargate tasks when possible (instead of creating new ones).
        self.last_job_tasks = list()                # List of task keys/payloads for the last-submitted job.
        self.last_job_counter = 0                   # How many tasks from last_job_tasks have finished executing.
        self.tasks_to_fargate_nodes = dict()        # Mapping of TaskID --> FargateNode
        self.use_bit_dep_checking = use_bit_dep_checking            # If True, use bit-method of dep counters. If False, use traditional way (incrementing integers).
        self.executors_use_task_queue = executors_use_task_queue, # Large tasks don't write data; instead, they wait for the tasks to become ready to execute locally.
        self.scheduler_id = str(random.randint(0, 9999)) + random.choice(string.ascii_letters).upper() # Unique ID so we can distinguish between Lambdas from different Scheduler's and whatnot.
        
        # We keep track of all the leaf tasks that we've seen in this dictionary. Specifically, this is a mapping from TASK_KEY (str) --> {True, False}. 
        self.seen_leaf_tasks = dict()

        # If we're printing something like a piece of data that could conceivably be 1,000's of characters, we'll probably want to truncate the string representation
        # of it unless we want to flood the console. This is the point at which we truncate. If it is a negative number, then we don't truncate it.
        self.print_debug_max_chars = 100    
        
        # When enabled, the user will step through each call to update_graph, and a significantly larger amount of debug info will print each iteration.
        # Useful for K-Means and other Dask-ML workloads in which there are multiple, successive (and automatic) calls to update_graph() in which the results
        # of one job/workload are used in the next.
        self.debug_mode = debug_mode 
        self.num_tasks_processed = 0 # Sum of the lengths of the tasks parameter in update_graph()
        self.executed_tasks = [] 
        self.executed_tasks_old = dict()
        self.number_update_graph_calls = 0

        with open("./wukong-config.yaml") as f:
            self.wukong_config = yaml.load(f, Loader = yaml.FullLoader) 
            self.aws_config = self.wukong_config["aws_lambda"]
        
        resolve_via_cloudformation = self.aws_config["retrieve_function_names_from_cloudformation"]

        # Check if we're supposed to retrieve the function names via cloud formation.
        if resolve_via_cloudformation:
            aws_sam_app_name = self.aws_config["aws_sam_app_name"]
            print("Retrieving AWS Lambda function names from CloudFormation. AWS SAM app name: \"{}\"".format(aws_sam_app_name))
            cloudformation_client = boto3.client('cloudformation', region_name = self.aws_region)

            stacks_response = cloudformation_client.describe_stacks(StackName = aws_sam_app_name)
            stack_outputs = stacks_response["Stacks"][0]["Outputs"]
            
            # stack_outputs is a list so we iterate over it and extract the desired information.
            # There should only be two entries, each of which is one that we want.
            for stack_output in stack_outputs:
                if stack_output["OutputKey"] == "ExecutorFunctionName":
                    self.executor_function_name = stack_output["OutputValue"]
                elif stack_output["OutputKey"] == "InvokerFunctionName":
                    self.invoker_function_name = stack_output["OutputValue"]

            print("Executor Function Name: \"{}\"".format(self.executor_function_name))
            print("Invoker Function Name: \"{}\"".format(self.invoker_function_name))            
        else:
            self.executor_function_name = self.aws_config["executor_function_name"]
            self.invoker_function_name = self.aws_config["invoker_function_name"]

            print("AWS Lambda function names specified directly in configuration file.")
            print("Executor Function Name: \"{}\"".format(self.executor_function_name))
            print("Invoker Function Name: \"{}\"".format(self.invoker_function_name))

        if self.worker_ttl:
            pc = PeriodicCallback(self.check_worker_ttl, self.worker_ttl, io_loop=loop)
            self.periodic_callbacks["worker-ttl"] = pc

        if self.idle_timeout:
            pc = PeriodicCallback(self.check_idle, self.idle_timeout / 4, io_loop=loop)
            self.periodic_callbacks["idle-timeout"] = pc

        if extensions is None:
            extensions = DEFAULT_EXTENSIONS
        for ext in extensions:
            ext(self)
        setproctitle("dask-scheduler [not started]")
        Scheduler._instances.add(self)

    ##################
    # Administration #
    ##################

    def __repr__(self):
        return '<Scheduler: "%s" processes: %d cores: %d>' % (
            self.address,
            len(self.workers),
            self.total_ncores,
        )

    def identity(self, comm=None):
        """ Basic information about ourselves and our cluster """
        d = {
            "type": type(self).__name__,
            "id": str(self.id),
            "address": self.address,
            "services": {key: v.port for (key, v) in self.services.items()},
            "workers": {
                worker.address: worker.identity() for worker in self.workers.values()
            },
        }
        return d

    def get_worker_service_addr(self, worker, service_name, protocol=False):
        """
        Get the (host, port) address of the named service on the *worker*.
        Returns None if the service doesn't exist.

        Parameters
        ----------
        worker : address
        service_name : str
            Common services include 'bokeh' and 'nanny'
        protocol : boolean
            Whether or not to include a full address with protocol (True)
            or just a (host, port) pair
        """
        ws = self.workers[worker]
        port = ws.services.get(service_name)
        if port is None:
            return None
        elif protocol:
            return "%(protocol)s://%(host)s:%(port)d" % {
                "protocol": ws.address.split("://")[0],
                "host": ws.host,
                "port": port,
            }
        else:
            return ws.host, port

    def start(self, addr_or_port=None, start_queues=True):
        """ Clear out old state and restart all running coroutines """
        enable_gc_diagnosis()

        addr_or_port = addr_or_port or self._start_address

        self.clear_task_state()

        with ignoring(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        for cor in self.coroutines:
            if cor.done():
                exc = cor.exception()
                if exc:
                    raise exc

        if self.status != "running":
            if isinstance(addr_or_port, int):
                # Listen on all interfaces.  `get_ip()` is not suitable
                # as it would prevent connecting via 127.0.0.1.
                self.listen(("", addr_or_port), listen_args=self.listen_args)
                self.ip = get_ip()
                listen_ip = ""
            else:
                self.listen(addr_or_port, listen_args=self.listen_args)
                self.ip = get_address_host(self.listen_address)
                listen_ip = self.ip

            if listen_ip == "0.0.0.0":
                listen_ip = ""

            if isinstance(addr_or_port, str) and addr_or_port.startswith("inproc://"):
                listen_ip = "localhost"

            # Services listen on all addresses
            self.start_services(listen_ip)

            self.status = "running"
            logger.info("  Scheduler at: %25s", self.address)
            for k, v in self.services.items():
                logger.info("%11s at: %25s", k, "%s:%d" % (listen_ip, v.port))

            self.loop.add_callback(self.reevaluate_occupancy)

        if self.scheduler_file:
            with open(self.scheduler_file, "w") as f:
                json.dump(self.identity(), f, indent=2)

            fn = self.scheduler_file  # remove file when we close the process

            def del_scheduler_file():
                if os.path.exists(fn):
                    os.remove(fn)

            finalize(self, del_scheduler_file)
            
        self.batched_lambda_invoker = BatchedLambdaInvoker(interval = "2ms", 
                                                           chunk_size = 2, 
                                                           num_invokers = self.num_lambda_invokers, 
                                                           redis_channel_names = redis_channel_names, 
                                                           loop = self.loop,
                                                           redis_address = self.proxy_address,
                                                           aws_region = self.aws_region,
                                                           executor_function_name = self.executor_function_name,
                                                           invoker_function_name = self.invoker_function_name,
                                                           use_invoker_lambdas_threshold = self.use_invoker_lambdas_threshold,
                                                           force_use_invoker_lambdas = self.force_use_invoker_lambdas)
        self.batched_lambda_invoker.start(self.lambda_client, scheduler_address = self.address)        
        # Write the address to Elasticache so the Lambda function can access it without being told explicitly.
        address_key = "scheduler-address"
        logger.info("Writing value %s to key %s in Redis" % (self.address , address_key))
        print("Writing value {} to key {} in Redis...".format(self.address, address_key))
        self.dcp_redis.set(address_key, self.address)          
        print("Done.")

        # Create a list to keep track of the processes as well as the Queue object, which we use for communication between the processes.
        self.redis_polling_processes = []
        self.redis_polling_queue = multiprocessing.Queue()
        
        logger.info("Creating %s redis-polling processes." % (len(redis_channel_names)))
        print("Creating {} redis-polling processes.".format(len(redis_channel_names)))
        # For each channel, we create a process and store a reference to it in our list.
        for channel_name in redis_channel_names:
            redis_polling_process = Process(target = self.poll_redis_process, args = (self.redis_polling_queue, channel_name, self.proxy_address))
            redis_polling_process.daemon = True 
            self.redis_polling_processes.append(redis_polling_process)

        # Start the processes.
        for redis_polling_process in self.redis_polling_processes:
            redis_polling_process.start()            
            
        self.start_periodic_callbacks()

        setproctitle("dask-scheduler [%s]" % (self.address,))
        
        #payload_for_proxy = {"op": "start", "redis-channel-names": redis_channel_names, "scheduler-address": self.address}

        #self.loop.add_callback(self.send_message_to_proxy, payload = payload_for_proxy, start_handling = True)

        #scheduler_conn, poller_conn = multiprocessing.Pipe()
        #self.scheduler_conn = scheduler_conn
        #sqs_polling = multiprocessing.Process(target = sqs_polling_process, args = (queue_url, poller_conn, 2))
        
        print("-=-=-=-=-=-=-=- SCHEDULER ID: {} -=-=-=-=-=-=-=-".format(self.scheduler_id))

        return self.finished()

    def __await__(self):
        self.start()

        @gen.coroutine
        def _():
            return self

        return _().__await__()

    @gen.coroutine
    def finished(self):
        """ Wait until all coroutines have ceased """
        while any(not c.done() for c in self.coroutines):
            yield All(self.coroutines)

    @gen.coroutine
    def close(self, comm=None, fast=False, close_workers=False):
        """ Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        if self.status.startswith("clos"):
            return
        self.status = "closing"

        logger.info("Scheduler closing...")
        setproctitle("dask-scheduler [closing]")

        if close_workers:
            for worker in self.workers:
                self.worker_send(worker, {"op": "close"})
            for i in range(20):  # wait a second for send signals to clear
                if self.workers:
                    yield gen.sleep(0.05)
                else:
                    break

        for pc in self.periodic_callbacks.values():
            pc.stop()
        self.periodic_callbacks.clear()

        self.stop_services()
        for ext in self.extensions:
            with ignoring(AttributeError):
                ext.teardown()
        logger.info("Scheduler closing all comms")

        futures = []
        for w, comm in list(self.stream_comms.items()):
            if not comm.closed():
                comm.send({"op": "close", "report": False})
                comm.send({"op": "close-stream"})
            with ignoring(AttributeError):
                futures.append(comm.close())

        for future in futures:
            yield future

        if not fast:
            yield self.finished()

        for comm in self.client_comms.values():
            comm.abort()

        self.rpc.close()

        self.status = "closed"
        self.stop()
        yield super(Scheduler, self).close()
        
        setproctitle("dask-scheduler [closed]")
        disable_gc_diagnosis()

    @gen.coroutine
    def close_worker(self, stream=None, worker=None, safe=None):
        """ Remove a worker from the cluster

        This both removes the worker from our local state and also sends a
        signal to the worker to shut down.  This works regardless of whether or
        not the worker has a nanny process restarting it
        """
        logger.info("Closing worker %s", worker)
        with log_errors():
            self.log_event(worker, {"action": "close-worker"})
            nanny_addr = self.workers[worker].nanny
            address = nanny_addr or worker

            self.worker_send(worker, {"op": "close", "report": False})
            self.remove_worker(address=worker, safe=safe)

    def _setup_logging(self):
        self._deque_handler = DequeHandler(
            n=dask.config.get("distributed.admin.log-length")
        )
        self._deque_handler.setFormatter(
            logging.Formatter(dask.config.get("distributed.admin.log-format"))
        )
        logger.addHandler(self._deque_handler)
        finalize(self, logger.removeHandler, self._deque_handler)

    ###########
    # Stimuli #
    ###########

    @gen.coroutine 
    def add_proxy(self, comm = None, address = None):
        self.proxy_comm = comm 

    @gen.coroutine
    def heartbeat_worker(
        self,
        comm=None,
        address=None,
        resolve_address=True,
        now=None,
        resources=None,
        host_info=None,
        metrics=None,
    ):
        raise gen.Return(None)

    @gen.coroutine
    def add_worker(
        self,
        comm=None,
        address=None,
        keys=(),
        ncores=None,
        name=None,
        resolve_address=True,
        nbytes=None,
        types=None,
        now=None,
        resources=None,
        host_info=None,
        memory_limit=None,
        metrics=None,
        pid=0,
        services=None,
        local_directory=None,
        nanny=None,
    ):
        raise gen.Return(None)

    def update_graph(
        self,
        client=None,
        tasks=None,
        keys=None,
        dependencies=None,
        restrictions=None,
        priority=None,
        loose_restrictions=None,
        resources=None,
        submitting_task=None,
        retries=None,
        user_priority=0,
        actors=None,
        persist=False,
        fifo_timeout=0
    ):
        """
        Add new computations to the internal dask graph

        This happens whenever the Client calls submit, map, get, or compute.
        """
        start = time()
        fifo_timeout = parse_timedelta(fifo_timeout)
        keys = set(keys)
        update_graph_id = str(random.randint(0, 9999)) + random.choice(string.ascii_letters).upper() + random.choice(string.ascii_letters).upper()
        self.number_update_graph_calls += 1
        print("\n=-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-==-=-=-=")
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= [SCHEDULER] update_graph() #{} --- ID: {} (Scheduler ID is {}) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=".format(self.number_update_graph_calls, update_graph_id, self.scheduler_id))
        print("=-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-==-=-=-=\n")

        if self.print_debug and persist:
            print("=== This is a persist operation! ===")

        if self.debug_mode:
            print("\tlen(tasks) = {}, len(keys) = {}".format(len(tasks), len(keys)))
            self.num_tasks_processed += len(tasks)
            print("\tNumber of tasks processed: {}\n".format(self.num_tasks_processed))

        if len(tasks) > 1:
            self.log_event(
                ["all", client], {"action": "update_graph", "count": len(tasks)}
            )
        
        fargate_launcher = None 
        manager = multiprocessing.Manager()
        fargate_tasks = manager.list()

        if self.use_fargate:        
            # Check if we need to launch or remove Fargate tasks.
            if (len(self.workload_fargate_tasks['current']) > self.num_fargate_nodes):
                # Add all of the Fargate tasks we currently know about to fargate_tasks
                fargate_tasks.extend(self.workload_fargate_tasks['current'])
                fargate_launcher = Process(target = self.launch_fargate_nodes, args = [self.num_fargate_nodes, fargate_tasks], kwargs = {"max_retries": 10})
                fargate_launcher.start()
            elif  (len(self.workload_fargate_tasks['current']) < self.num_fargate_nodes):
                self.workload_fargate_tasks['current'].clear()
                # Add all of the Fargate tasks we currently know about to fargate_tasks
                fargate_launcher = Process(target = self.launch_fargate_nodes, args = [self.num_fargate_nodes, fargate_tasks], kwargs = {"max_retries": 10})
                fargate_launcher.start()
        
        # Remove aliases
        for k in list(tasks):
            if tasks[k] is k:
                del tasks[k]

        if self.print_debug and self.print_level <= 1:
            print("\nNew Tasks: ")
            for key in list(tasks):
                print(key)
            print("\n\n")

        dependencies = dependencies or {}

        n = 0
        while len(tasks) != n:  # walk through new tasks, cancel any bad deps
            n = len(tasks)
            for k, deps in list(dependencies.items()):
                if any(
                    dep not in self.tasks and dep not in tasks for dep in deps
                ):  # bad key
                    logger.info("User asked for computation on lost data, %s", k)
                    print("[WARNING] User asked for computation on lost data, {}.".format(k))
                    del tasks[k]
                    del dependencies[k]
                    if k in keys:
                        keys.remove(k)
                    self.report({"op": "cancelled-key", "key": k}, client=client)
                    self.client_releases_keys(keys=[k], client=client)

        # Remove any self-dependencies (happens on test_publish_bag() and others)
        for k, v in dependencies.items():
            deps = set(v)
            if k in deps:
                deps.remove(k)
            dependencies[k] = deps

        # Avoid computation that is already finished
        already_in_memory = set()  # tasks that are already done
        for k, v in dependencies.items():
            if v and k in self.tasks and self.tasks[k].state in ("memory", "erred"):
                already_in_memory.add(k)

        if already_in_memory:
            dependents = dask.core.reverse_dict(dependencies)
            stack = list(already_in_memory)
            done = set(already_in_memory)
            while stack:  # remove unnecessary dependencies
                key = stack.pop()
                ts = self.tasks[key]
                try:
                    deps = dependencies[key]
                except KeyError:
                    #deps = self.dependencies[key]
                    deps = dependencies[key]
                for dep in deps:
                    if dep in dependents:
                        child_deps = dependents[dep]
                    else:
                        #child_deps = self.dependencies[dep]
                        child_deps = dependencies[dep]
                    if all(d in done for d in child_deps):
                        if dep in self.tasks:
                            done.add(dep)
                            stack.append(dep)

            for d in done:
                #print("Task {} is already done (in-memory).".format(d))
                tasks.pop(d, None)
                dependencies.pop(d, None)

        # Get or create task states
        stack = list(keys)
        touched_keys = set()
        touched_tasks = []
        while stack:
            k = stack.pop()
            if k in touched_keys:
                continue
            # XXX Have a method get_task_state(self, k) ?
            ts = self.tasks.get(k)
            if ts is None:
                ts = self.tasks[k] = TaskState(k, tasks.get(k))
                ts.state = "released"
            elif not ts.run_spec:
                ts.run_spec = tasks.get(k)

            touched_keys.add(k)
            touched_tasks.append(ts)
            stack.extend(dependencies.get(k, ()))

        self.client_desires_keys(keys=keys, client=client)

        # Add dependencies
        for key, deps in dependencies.items():
            ts = self.tasks.get(key)
            if ts is None or ts.dependencies:
                continue
            for dep in deps:
                dts = self.tasks[dep]
                ts.dependencies.add(dts)
                dts.dependents.add(ts)
            #print("\nDependencies for task {}:\n\t{}".format(key, ts.dependencies))

        # Compute priorities
        if isinstance(user_priority, Number):
            user_priority = {k: user_priority for k in tasks}

        # Add actors
        if actors is True:
            actors = list(keys)
        for actor in actors or []:
            self.tasks[actor].actor = True

        priority = priority or dask.order.order(
            tasks
        )  # TODO: define order wrt old graph

        if submitting_task:  # sub-tasks get better priority than parent tasks
            ts = self.tasks.get(submitting_task)
            if ts is not None:
                generation = ts.priority[0] - 0.01
            else:  # super-task already cleaned up
                generation = self.generation
        elif self._last_time + fifo_timeout < start:
            self.generation += 1  # older graph generations take precedence
            generation = self.generation
            self._last_time = start
        else:
            generation = self.generation

        for key in set(priority) & touched_keys:
            ts = self.tasks[key]
            if ts.priority is None:
                ts.priority = (-user_priority.get(key, 0), generation, priority[key])

        # Ensure all runnables have a priority
        runnables = [ts for ts in touched_tasks if ts.run_spec]

        if self.debug_mode:
            executing_again = []
            print("\n\n-=-=-=-=-=-=- Runnables (len = {}) -=-=-=-=-=-=-\n".format(len(runnables)))
            for ts in runnables:
                print("{} -- {}".format(ts.key, ts.state))
                if ts.key in self.completed_tasks:
                    executing_again.append(ts)
                if ts.priority is None and ts.run_spec:
                    ts.priority = (self.generation, 0)
            print("\n-=-=-=-=-=-=- End of Runnables -=-=-=-=-=-=-\n\n")
            
            print("\n\n-=-=-=-=-=-=- Executing {} tasks again (not necessarily for the second time) -=-=-=-=-=-=-\n".format(len(executing_again)))
            for ts in executing_again:
                num_executed_so_far = self.completed_task_counts[ts.key]
                print("Potentially executing task {} for time #{}".format(ts.key, (num_executed_so_far+1)))
            print("\n-=-=-=-=-=-=- End of executing again -=-=-=-=-=-=-\n\n")

        # runnables_dict  = {ts.key:ts for ts in runnables} # Used when populating mapping for dependencies --> bit position
        for ts in runnables:
            if ts.priority is None and ts.run_spec:
                ts.priority = (self.generation, 0)

        if resources:
            for k, v in resources.items():
                if v is None:
                    continue
                assert isinstance(v, dict)
                ts = self.tasks.get(k)
                if ts is None:
                    continue
                ts.resource_restrictions = v

        if retries:
            for k, v in retries.items():
                assert isinstance(v, int)
                ts = self.tasks.get(k)
                if ts is None:
                    continue
                ts.retries = v

        # Compute recommendations
        waiting = []

        for ts in sorted(runnables, key=operator.attrgetter("priority"), reverse=True):
            if ts.state == "released" and ts.run_spec:
                waiting.append(ts)

        # for ts in touched_tasks:
        #     for dts in ts.dependencies:
        #         if dts.exception_blame:
        #             ts.exception_blame = dts.exception_blame
        #            recommendations[ts.key] = "erred"
        #           break

        for plugin in self.plugins[:]:
            try:
                plugin.update_graph(
                    self,
                    client=client,
                    tasks=tasks,
                    keys=keys,
                    restrictions=restrictions or {},
                    dependencies=dependencies,
                    priority=priority,
                    loose_restrictions=loose_restrictions,
                    resources=resources,
                )
            except Exception as e:
                logger.exception(e)
        
        if self.debug_mode:
            print("\n\n-=-=-=-=-=-=- Tasks executed since last call to update_graph ({} tasks were executed) -=-=-=-=-=-=-\n".format(len(self.executed_tasks)))
            self.executed_tasks.sort()
            for task_key in self.executed_tasks:
                print(task_key)
            print("\n-=-=-=-=-=-=- End of tasks executed since last call to update_graph -=-=-=-=-=-=-\n\n")

            self.executed_tasks_old[self.number_update_graph_calls] = self.executed_tasks.copy()
            self.executed_tasks = []

        # These are what is serialized and stored on Redis. They are dictionaries containing the following information for each Task:
        # - Scheduler's IP Address 
        # - Serialized Code (or location of serialized code)
        # - Dependencies (what this task needs to execute)
        # + Keys of direct descendants 
        # - Location of payloads for downstream tasks (tasks that need this task to execute) 
        task_payloads = dict()
        
        # We will use a pipeline to store all of the payloads in a bulk, batch operation. This should be faster than doing them one-at-a-time.
        # task_payload_pipeline = self.redis_client.pipeline()

        # We pass this to mset for one big initial payload.
        initial_payloads = defaultdict(dict)

        # List of sizes of all tasks. This is so we can attempt to compute the average size of tasks. 
        task_sizes = []
        
        # Same as above but for paths.
        path_sizes = []

        # These are submitted to Lambda functions for execution.
        paths = []

        # The maximum AWS Lambda function payload size in bytes.
        max_path_size_bytes = 256000

        # These are the tasks with ZERO dependencies. They'll be found at the bottom of the tree.
        leaf_tasks = dict()

        tasks_to_path_nodes = defaultdict(dict)           # Mapping a task to its path node(s)
        tasks_to_path_starts = dict()                     # Map of task_key --> starting_node_of_path
        
        tasks_to_serialized_path_node = dict()

        serialized_tasks = dict()

        # Used just for diagnostics. We print out the largest fanout for the current workload.
        largest_fanout = 0
        largest_fanout_task_key = ""

        self.last_job_counter = 0
        self.last_job_tasks.clear() 
        
        #print("\nTasks contained in parameter Tasks:")
        #for tsk in tasks:
        #    print(tsk)
        
        #print("\nTasks contained in runnables:")
        #for tsk in runnables:
        #    print(tsk)
        
        #print("\nTasks contained in touched_tasks:")
        #for tsk in touched_tasks:
        #    print(tsk)
        
        if self.print_debug or self.debug_mode:
            print("len(tasks): {}\nlen(runnables): {}\nlen(touched_tasks): {}\nlen(self.tasks): {}".format(len(tasks), len(runnables), len(touched_tasks), len(self.tasks)))

        # Collect all leaf tasks.
        #for task_key, ts in self.tasks.items():
        for ts in runnables:
            if ts.run_spec and (ts.state == "released" or ts.state == "waiting"):
                task_key = ts.key
                #if self.print_debug and self.print_level <= 1:
                #    print("Checking if task {} is a leaf task...".format(task_key))
            #     # Leaf tasks are tasks with no dependencies -- they can be executed right away. We only want tasks that are in a state which indicates they have
            #     # not yet been executed. We also check that we have no explicit record of executing the task in the completed_tasks dictionary.
            #     if self.completed_tasks[task_key] == False:
            #         # First, the task shouldn't be one that is already processing or in-memory.
            #         if ts.state == "released" or ts.state == "waiting" or ts.state == "no-worker":
            #             # Next, it should either have no dependencies...
                if len(ts.dependencies) == 0:
                    if self.print_debug and self.print_level <= 3:
                        print("\tAdding task {} to leaf tasks. Task state: {}\n".format(task_key, ts.state))
                    leaf_tasks[task_key] = ts
                else:
                    # It is possible that we'll have a task dependent only on tasks that have already been executed. In that case, we can invoke 
                    # the task as all of its data is theoretically available (unless the databases have been wiped since then).
                    # 
                    # TODO: Keep track of user-triggered DB wipes and keep track of which data is erased so we know when we'd have to recompute it.
                    deps = list(ts.dependencies)
                    add_to_leaf_tasks = True 
                    
                    # Basically, we check and see if all the dependencies are ones that we have already. If not, then we won't be invoking the task with the leaf wave.
                    for dep in deps:
                        if self.print_debug and self.print_level <= 1:
                            print("\tProcessing dep of {}: {}".format(task_key, dep.key))
                        #if self.completed_tasks[dep.key] == False:
                        if dep.key not in self.tasks or dep.state != "memory":
                            if self.print_debug and self.print_level <= 1:
                                print("\t\tDependency {} of task {} has not been completed yet...\n".format(dep.key, task_key))
                            add_to_leaf_tasks = False 
                            break 
                    
                    if add_to_leaf_tasks:
                        if self.print_debug and self.print_level <= 3:
                            print("\tAdding task {} to leaf tasks. NOTE: It actually has {} dependencies, but we have their data already.\n".format(task_key, len(list(ts.dependencies))))
                        leaf_tasks[task_key] = ts
                    #else:
                    #    if self.print_debug:
                    #        print("\t[WARNING] Task {} has already been completed...?".format(task_key))

        if self.debug_mode or (self.print_debug and self.print_level <= 2):
            print("num_leaf_tasks =", len(leaf_tasks))
            if len(leaf_tasks) == 0 and not self.debug_mode:
                for ts in runnables:
                    print("{} -- State: {}\nDependencies:".format(ts.key, ts.state))
                    for dep in list(ts.dependencies):
                        print("\t{} -- State: {}".format(dep.key, dep.state))
                        print("\tPreviously a Leaf Task: {}\n".format(self.seen_leaf_tasks.get(dep.key, False)))
        
        sum_tasks = 0
        for k,c in self.completed_task_counts.items():
            sum_tasks = sum_tasks + c
        
        if self.debug_mode or (self.print_debug and self.print_level <= 2):
            print("num_leaf_tasks =", len(leaf_tasks))        
            print("Number of tasks executed so far:", sum_tasks)
            print("[SCHEDULER] update-graph() done.")
            if self.debug_mode:
                input("\n-=-=-=-=-=-=- Type something and then enter to continue... -=-=-=-=-=-=-\n\n")

        def DFS(current_task, current_path, current_path_size, visited, isNewPath = False, incrDepCounter = False):
            """ Depth-first search. This method is used to construct paths through 
                the DAG. These paths are then sent to AWS Lambda functions for execution.

                The benefit of paths is that we can execute many tasks [that appear sequentially
                within the DAG] on the same Lambda invocation. This in turn reduces the number of
                times the Lambda functions need to access Redis to obtain code and data.

                Note that Empty paths are only created during "fan-out" situations from a node. The tasks that get 
                invoked from the node with the fan-out are put on new paths.
                
                At its core, this algorithm is just a depth-first search. In order to suit our purposes however, I have augmented it slightly.
                The algorithm constructs so-called paths from leaf nodes as far into the tree as possible. 
                
                For portions of the DAG that are
                perfectly linear (i.e. every node/task has at most one outgoing edge), this algorithm will append each new node onto the current
                path.

                During the algorithm, we essentially look ahead in the graph by one edge. So if we are processing a node N, we will look at nodes
                that are one edge away from N. This is where the much of the logic happens.
                
                Let's say we are currently visiting node N and node N has two or more outgoing edges. We will look at each of N's children (i.e. the
                nodes which are adjacent via the outgoing edges) are consider three different cases:

                Case #1 - We have seen this node before, and there is a new path STARTING at this node. 
                Case #2 - We have seen this node before, and it is simply on some other existing path.
                Case #3 - We have not yet seen this node before.

                For each of the three cases, the steps we follow differ slightly depending on whether or not we have already designated a child node
                that our current node N will 'become'. What this means is that once N is finished with its task, it will grab the next task code 
                and execute it itself as opposed to invoking a new Lambda.

                For Case #1, if we have not designated a 'become' node/task already, then we simply merge the existing path with our own and configure the current
                task to become the next one. If we have already designated a 'become' node/task, then we tell our current task to invoke the first
                task on the existing path.

                For Case #2, if we have not designated a 'become' node/task already, then we create a sub-path beginning at the downstream/dependent
                node/task and merge it with our current path. We tell the current task to 'become' the dependent task/node and continue executing down
                the now-merged path. If we have already designated a 'become' node/task, then we just tell our current task to invoke the sub-path.

                For Case #3, if we have not designated a 'become' node yet, then we make the unseen node our 'become' node and continue the DFS. If we
                have already designated a 'become' node, then we create a new path beginning with the unseen node and continue the DFS with the new path.

                Args:
                    current_task (TaskState): The current TaskState being processed from the DAG.

                    current_path (Path):      The current Path being constructed.

                    current_path_size (int):  The size in bytes of all of the task payloads in the current path.

                    visited (Dict):           A map of task keys (str) --> bool indicating which tasks have already 
                                              been visited by the DFS.
                    
                    isNewPath (bool):         Indicates whether the current_path var is newly-created or not (i.e., the current path node is first on the path)
                    
                    incrDepCounter (bool):    In some cases, some data may already be computed. So initial dependency counters may need to be incremented instead of set to zero.
                Returns:
                    This returns the 'node' processed during the invocation of the method."""
            nonlocal largest_fanout
            nonlocal largest_fanout_task_key

            visited[current_task.key] = True

            # We may have already executed this task in a previous job.
            already_executed = False #(current_task.key in self.tasks and current_task.state == "memory")

            if self.print_debug and self.print_level <= 1:
                if already_executed:
                    print("[DFS] Visiting already-executed task", current_task.key)
                else:
                    print("[DFS] Visiting task", current_task.key)
                #print_me = [c.__str__() for c in current_task.who_wants]
                #print("\tClients who want this task: ", print_me)

            # Construct a task payload with the task key and task state.
            payload = self.construct_basic_task_payload(current_task.key, current_task, already_executed = already_executed, persist = persist)

            #if current_task.key in self.task_payloads:
                #print("\n[WARNING] Task {} already has an existing Task Payload...".format(current_task.key))

            #    old_task_payload = self.task_payloads[current_task.key]
            #    self.old_task_payloads[current_task.key].append(old_task_payload)

                # completed = self.completed_tasks[current_task.key]
                # executing = self.executing_tasks[current_task.key]
                # if completed:
                #     print("\tTask {} HAS already been executed!")
                # elif executing:
                #     print("\tTask {} is EXECUTING right now...")
                #     #pythontime.sleep(5)
                # else:
                #     print("\tTask {} has NOT been completed, NOR has it been executed...\n")
                #     #pythontime.sleep(5)
            
            dep_index_map = dict()

            if self.use_bit_dep_checking and len(current_task.dependents) > 0:
                for dts in list(current_task.dependents):
                    # Get the index of the current task's key in each of its downstream task's dependency lists.
                    idx = -1
                    dep_list = list(dts.dependencies)

                    # Search for the current task's key in the list of dependencies (each element is a TaskState object).
                    for i in range(0, len(dep_list)):
                        ts = dep_list[i]
                        if ts.key == current_task.key:
                            idx = i
                            break
                    
                    # Make sure we definitely found it. If we don't, something is wrong...
                    if idx == -1:
                        raise ValueError("Never found current_task.key ({}) in downstream task's dependencies (dts is {})".format(current_task.key, dts.key))
                    
                    dep_index_map[dts.key] = idx

            # Put the payload in the "master list (dict)" of task payloads. 
            # Presently we don't do anything with this master list though...
            task_payloads[current_task.key] = payload
            self.task_payloads[current_task.key] = payload 

            # Serialize the entire payload using Dask serialization.
            serialized_payload = list(dumps(payload))
            payload_bytes = []
            for bytes_object in serialized_payload:
                # Encode in Base64 so we can store this as a JSON object for sending to AWS Lambda.
                payload_bytes.append(base64.encodestring(bytes_object).decode(ENCODING))

            current_payload_size = sys.getsizeof(payload_bytes)
            serialized_tasks[current_task.key] = payload_bytes
            task_sizes.append(current_payload_size)

            if self.print_debug and self.print_level <= 1:
                print("Size of serialized task payload for task {}: {} bytes".format(current_task.key, current_payload_size))
            
            fargate_task_for_node = None

            if self.use_fargate:
                # If we're re-using a task from a previous computation, then we've already 
                # mapped its data somewhere. We would like to reuse the data/mapping.
                if current_task.key not in self.tasks_to_fargate_nodes:
                    # (Pseudo-)randomly select a Fargate task to map to the current PathNode (which we're about to create).
                    fargate_task_for_node = random.choice(self.workload_fargate_tasks['current'])
                else:
                    if self.print_debug and self.print_level <= 1:
                        print("\n\tReusing existing Fargate mapping for task {}".format(current_task.key))
                    # Re-use previous mapping.
                    fargate_task_for_node = self.tasks_to_fargate_nodes[current_task.key]
            
                # Increment our internal record of how many times we've selected this Fargate node in a workload.
                fargate_task_ARN = fargate_task_for_node[FARGATE_ARN_KEY]
                self.fargate_metrics[fargate_task_ARN][FARGATE_NUM_SELECTED] += 1

                # Store the mapping.
                self.tasks_to_fargate_nodes[current_task.key] = fargate_task_for_node
                current_path.tasks_to_fargate_nodes[current_task.key] = fargate_task_for_node

            # Create new path node.
            current_path_node = PathNode(payload_bytes, 
                                         payload["key"], 
                                         current_path, 
                                         None, 
                                         None, 
                                         fargate_task_for_node, 
                                         scheduler_id = self.scheduler_id, 
                                         update_graph_id = update_graph_id, 
                                         dep_index_map = dep_index_map,
                                         starts_at = None) # Value for starts_at will be updated later...
            
            # We're gonna store this PathNode in Redis.
            # initial_payloads[self.dcp_redis][current_task.key] = current_path_node

            #if current_task.key in self.path_nodes:
            #    old_path_node = self.path_nodes[current_task.key]
            #    self.old_path_nodes[current_task.key].append(old_path_node)
                #print("\n[WARNING] Task {} already has an existing PathNode...".format(current_task.key))
                # print("\n\n[WARNING] Task {} already has an existing Task Payload...".format(current_task.key))
                # completed = self.completed_tasks[current_task.key]
                # executing = self.executing_tasks[current_task.key]
                # if completed:
                #     print("\tTask {} HAS already been executed!")
                # elif executing:
                #     print("\tTask {} is EXECUTING right now...")
                #     #pythontime.sleep(5)
                # else:
                #     print("\tTask {} has NOT been completed, NOR has it been executed...\n")                
                #     #pythontime.sleep(5)

            # Record the PathNode in our local dictionary of TASK_KEY --> PathNode.
            self.path_nodes[current_task.key] = current_path_node

            self.last_job_tasks.append((current_path_node, payload))

            tasks_to_path_nodes[current_task.key][current_path.id] = current_path_node
            
            # If this is a brand new path, then the current node is the first of the path so add it to the dictionary.
            if isNewPath:
                if self.print_debug and self.print_level <= 1:
                    print("[NEW PATH] Task {} is the first node in a new path.".format(current_task.key))
                tasks_to_path_starts[current_task.key] = current_path

            # Add the new path node to the current path. We should do this AFTER the previous step (where we
            # attempted to update the previous node) as it makes it easier to correctly update the "previous"
            # path node.
            current_path.add_node(current_task.key, current_path_node)
            current_path_size += current_payload_size 
            
            # If this task has dependencies, then we need to store payload/path and dependency counters in Redis.
            #if len(current_task.dependencies) > 0:

            if self.use_bit_dep_checking == False:
                # Create dependency counters for all tasks, not just non-leaf tasks (even though non-leaf tasks don't need dependency counters). 
                # Just doing it for consistency's sake.
                redis_dep_counter_key = str(current_task.key) + DEPENDENCY_COUNTER_SUFFIX
                if self.print_debug and self.print_level <= 1:
                    print("Will be storing dependency counter for task {} at key {}".format(current_task.key, redis_dep_counter_key))
                
                initial_dep_value = 0

                # Look at each dependency. If it is in memory, then increment the counter.
                for dts in current_task.dependencies:
                    if dts.key in self.tasks and self.tasks[dts.key].state == "memory":
                        initial_dep_value += 1

                        if self.print_debug and self.use_fargate:
                            # Make sure we include the task-to-fargate mapping for the dependency so the current task can find it.
                            current_path.tasks_to_fargate_nodes[dts.key] = self.tasks_to_fargate_nodes[dts.key]
                            print("Dependency {} of task {} is in memory!".format(dts.key, current_task.key))
                            print("\tIncrementing initial dependency counter value for task {}. (Was {}, is now {}.)".format(
                                current_task.key, (initial_dep_value - 1), initial_dep_value))
                    else:
                        if self.print_debug:
                            print("Dependency {} of task {} is in state {}. Cannot consider it done.".format(dts.key, current_task.key, self.tasks[dts.key].state))

                initial_payloads[self.dcp_redis][redis_dep_counter_key] = initial_dep_value

            # If there are no downstream tasks from this node, then just pass.
            if payload["num-dependencies-of-dependents"] == 0:
                if self.print_debug and self.print_level <= 1:
                    print("[DEPENDENTS] Task {} has no dependents. Path ended.".format(current_task.key))
                pass
            else:
                dependents = list(current_task.dependents)
                if len(dependents) > largest_fanout:
                    largest_fanout = len(dependents)
                    largest_fanout_task_key = current_task.key
                # This flag is switched to true after we process the first unvisited downstream task. 
                # The reason for this is that only one outgoing edge from the current node may be included
                # on the same path. Additional outgoing edges will require their own paths.
                # next_unvisited_node_should_go_on_new_path = False
                if self.print_debug and self.print_level <= 1:
                    print("[DEPENDENTS] Task {} has {} dependents.".format(current_task.key, len(dependents)))
                # Iterate over the remaining dependents.
                for i in range(0, len(dependents)):
                    dependent_task_state = dependents[i]
                    key = dependent_task_state.key
                    if self.print_debug and self.print_level <= 1:
                        print("\t[DEPENDENTS] Looking at dependent {}...".format(key))
                    # Check if we've already visited the dependent node. If we have,
                    # then that means that at some point in a previous DFS, we touched this node.
                    if visited.get(key, False) == True:
                        # We're going to check and see if there happens to be an existing path that STARTS at 
                        # the dependent task. If there is, then we'll be able to use this path very easily.
                        existing_path = tasks_to_path_starts.get(key, None)
                        # If the past is not none, then a path was already created at this node. Just use that path.
                        if existing_path is not None:
                            if self.print_debug and self.print_level <= 1:
                                print("\t\t[PATH-V] Dependent task {} is already the start of an existing path.".format(key))
                            
                            # Grab the start of the existing path (which should be the dependent task)
                            # and add that to the invoke list of the current path node. That's all we need to do.
                            dependent_task_path_node = existing_path.get_start()

                            if self.use_fargate:
                                # Make sure we have all of the Fargate IPs for dependencies of the downstream node in our task mapping.
                                dependent_task_payload = task_payloads[dependent_task_path_node.task_key]                                
                                for dependency_key in dependent_task_payload["dependencies"]:
                                    # If we've cached a mapping in our global TASK-KEY --> FARGATE IP mapping AND said mapping does not exist
                                    # in the current path's task-fargate mapping, then add it to the current path's task-fargate mapping.
                                    if dependency_key in self.tasks_to_fargate_nodes and dependency_key not in current_path.tasks_to_fargate_nodes:
                                        current_path.tasks_to_fargate_nodes[dependency_key] = self.tasks_to_fargate_nodes[dependency_key]  

                            # If the current node does not yet have a 'become' field, then we will have the current node
                            # 'become' the dependent node. We will  add the dependent node's path to the current path.
                            if current_path_node.become is None:
                                # Add the contents of the new path to this path.
                                current_path.tasks.extend(existing_path.tasks)

                                if self.use_fargate:
                                    # Update our Task --> FargateNode mapping.
                                    current_path.tasks_to_fargate_nodes.update(existing_path.tasks_to_fargate_nodes)

                                    # Make sure the mapping for THIS task is in the next path's Fargate mapping since it may need
                                    # to retrieve the current task's data. That is, the current task is a data dependency for whatever task
                                    # starts the next path, so the next path needs to know where the current task's data will be located.
                                    existing_path.tasks_to_fargate_nodes[current_path_node.task_key] = current_path_node.fargate_node

                                # Since we added the existing path to the current path, any paths that start where the existing path ended
                                # should be added to the current path's "next_paths" field.
                                for path in existing_path.next_paths:
                                    current_path.add_next_path(path)

                                current_path_node.become = dependent_task_path_node.task_key                               
                            else:
                                # If the current node already has a 'become', then we have to invoke the dependent node being processed.
                                # Update the path information for the current path and the path of the dependent node. Then add the 
                                # dependent node to the current node's invoke field.
                                existing_path.add_previous_path(current_path)
                                current_path.add_next_path(existing_path)
                                current_path_node.invoke.append(dependent_task_path_node.task_key)

                                if self.use_fargate:
                                    # Make sure the mapping for THIS task is in the next path's Fargate mapping since it may need
                                    # to retrieve the current task's data. That is, the current task is a data dependency for whatever task
                                    # starts the next path, so the next path needs to know where the current task's data will be located.
                                    existing_path.tasks_to_fargate_nodes[current_path_node.task_key] = current_path_node.fargate_node      

                                    # Add the existing downstream task's Fargate information to the current path's mapping.
                                    current_path.tasks_to_fargate_nodes[dependent_task_path_node.task_key] = dependent_task_path_node.fargate_node                                                          
                        else:
                            if self.print_debug and self.print_level <= 1:
                                print("\t\t[PATH-V] Dependent task {} exists on some path. Using sub-path created from existing path.".format(key))
                            # We're basically going to create a new path that starts at the dependent node
                            # using the dependent node's existing path. We're just going to discard everything
                            # that came before it.

                            # Get the existing path node of the dependent task.
                            dependent_task_path_node = next(iter(tasks_to_path_nodes[key].values())) # Grab the first path from the dictionary...
                            # TODO: We may want to pick a specific path as opposed to arbitrarily picking the original path (which
                            # is stored at position zero). For example, we may want to use the longest path. Or maybe even the shortest.

                            # Get the existing path of the dependent task.
                            dependent_task_original_path = dependent_task_path_node.path

                            # Get the index of the dependent task/path node in its existing path.
                            idx = dependent_task_original_path.tasks.index(dependent_task_path_node)

                            if self.use_fargate:
                                # Add the existing downstream task's Fargate information to the current path's mapping.
                                current_path.tasks_to_fargate_nodes[dependent_task_path_node.task_key] = dependent_task_path_node.fargate_node 

                                # Make sure we have all of the Fargate IPs for dependencies of the downstream node in our task mapping.
                                dependent_task_payload = task_payloads[dependent_task_path_node.task_key]                               
                                for dependency_key in dependent_task_payload["dependencies"]:
                                    # If we've cached a mapping in our global TASK-KEY --> FARGATE IP mapping AND said mapping does not exist
                                    # in the current path's task-fargate mapping, then add it to the current path's task-fargate mapping.
                                    if dependency_key in self.tasks_to_fargate_nodes and dependency_key not in current_path.tasks_to_fargate_nodes:
                                        current_path.tasks_to_fargate_nodes[dependency_key] = self.tasks_to_fargate_nodes[dependency_key]   

                            # If we currently lack a 'become' node, then we'll just add this next path to ourselves.
                            if current_path_node.become is None:
                                current_path.tasks.extend(dependent_task_original_path.tasks[idx:])
                                current_path_node.become = dependent_task_path_node.task_key

                                if self.use_fargate:
                                    # Also make sure the mapping for THIS task is in the next path's Fargate mapping since it may need
                                    # to retrieve the current task's data. That is, the current task is a data dependency for whatever task
                                    # starts the next path, so the next path needs to know where the current task's data will be located.
                                    dependent_task_original_path.tasks_to_fargate_nodes[current_path_node.task_key] = current_path_node.fargate_node                            
                            else:
                                # Create a new path.
                                new_path = Path(None, None, None, None, None)
                                new_path.add_previous_path(current_path)
                                current_path.add_next_path(new_path)

                                # Grab all of the tasks beginning with the dependent task and store them in the new path.
                                new_path.tasks = dependent_task_original_path.tasks[idx:]

                                # Update the master list of paths.
                                paths.append(new_path)

                                # The dependent task is now the start of a new path, so update its entry in tasks_to_path_starts.
                                tasks_to_path_starts[dependent_task_path_node.get_task_key()] = new_path
                                
                                if self.use_fargate:
                                    # Add the current path node's fargate node to the new path's mapping. The new path may need to
                                    # retrieve the data of the current path node from Redis, so it needs the IP address of where the data is stored.
                                    new_path.tasks_to_fargate_nodes[current_path_node.task_key] = current_path_node.fargate_node
                                
                                current_path_node.invoke.append(dependent_task_path_node.task_key)
                    # If we have NOT visited the node yet, then we will DFS from that node with a brand new path.
                    else:
                        # If this is the first out-going edge, we will arbitarily 'become' this node and invoke the others.
                        if current_path_node.become is None:
                            dependent_task_path_node = DFS(dependent_task_state, current_path, current_path_size, visited, isNewPath = False, incrDepCounter = already_executed)
                            if self.print_debug and self.print_level <= 1:
                                print("\t\t[PATH] Staying on same path for dependent task {}.\n\tTask {} will BECOME task {}.".format(key, current_path_node.get_task_key(), key))
                            current_path_node.become = dependent_task_path_node.task_key 
                        else:
                            if self.print_debug and self.print_level <= 1:
                                print("\n\n\t\t[PATH] Creating new path for dependent task {}.\n\tTask {} will INVOKE task {}.".format(key, current_path_node.get_task_key(), key))
                            # Create a new path. We will invoke the dependent task and it will continue down this new path.
                            new_path = Path(None, None, None, None, None)

                            # Link the new path and current path together.
                            current_path.add_next_path(new_path)
                            new_path.add_previous_path(current_path)

                            if self.use_fargate:
                                # Add the current node's Fargate mapping info to the new Path (since it may need 
                                # it given the first node of the new path has the current node as a dependency).
                                new_path.tasks_to_fargate_nodes[current_path_node.task_key] = current_path_node.fargate_node

                            # Continue DFS.
                            dependent_task_path_node = DFS(dependent_task_state, new_path, 0, visited, isNewPath = True, incrDepCounter = already_executed)

                            # Update the invoke list for the current node with the new PathNode returned from the DFS.
                            current_path_node.invoke.append(dependent_task_path_node.task_key)
                            
                            # Update the master list of paths.
                            paths.append(new_path)
                if len(dependents) >= self.max_task_fanout:
                    current_path_node.use_proxy = True
            # Serialize this node. This check is redundant/unnecessary?
            if current_task.key not in tasks_to_serialized_path_node:
                # Temporarily remove the Path reference before we serialize as we don't want to serialize the path reference.
                current_path_node.starts_at = current_path.get_start().task_key
                current_path_node.path = None 
                node_serialized = cloudpickle.dumps(current_path_node)
                tasks_to_serialized_path_node[current_task.key] = node_serialized
                current_path_node.path = current_path
            return current_path_node
        
        # If we created a process to launch Fargate tasks, then we need to wait for it to finish before we begin invoking Lambdas.
        if self.use_fargate and fargate_launcher is not None:
            print("[ {} ] Scheduler is waiting for FARGATE LAUNCHER progress to finish...".format(datetime.datetime.utcnow()))
            fargate_launcher.join()
            print("[ {} ] Fargate-launching process has finished.".format(datetime.datetime.utcnow()))
            #print("fargate_tasks:", fargate_tasks)
            for fargate_task in fargate_tasks:
                #print("Is fargate_task {} in self.workload_fargate_tasks['current']?".format(fargate_task))
                if not fargate_task in self.workload_fargate_tasks['current']:
                    #print("It is not! Adding it now.")
                    self.workload_fargate_tasks['current'].append(fargate_task)
                taskArn = fargate_task[FARGATE_ARN_KEY]
                if taskArn not in self.fargate_metrics:
                    #print("Creating entry in fargate_metrics for Fargate task {}.".format(taskArn))
                    self.fargate_metrics[taskArn] = {
                        FARGATE_NUM_SELECTED: 0
                    }
            #self.workload_fargate_tasks['current'].extend(fargate_tasks)
            
            #for _task in tasks_removed:
            #    self.workload_fargate_tasks['current'].remove(_task)
            
        visited = dict()
        metrics = dict()
        DFS_start = pythontime.time()

        # Construct "paths" by performing depth-first searches from leaves.
        for leaf_task_key, leaf_task_state in leaf_tasks.items():
            # If the task is processing already, then do not do anything.
            #if leaf_task_state.state == "processing":
            #    if self.print_debug:
            #        print("[ {} Scheduler - PREP: Skipping task {} because it is already processing.".format(datetime.datetime.utcnow(), leaf_task_key))
            #    continue

            current_path_size = 0
            new_path = Path([], {}, [], [], {})

            paths.append(new_path)

            tasks_to_path_starts[leaf_task_key] = new_path

            # Note that we pass 'isNewPath = False' since the path is NOT empty; it has the leaf task already.
            # Empty paths are only created during "fan-out" situations from a node. The tasks that get invoked
            # from the node with the fan-out are put on new paths.
            if self.print_debug and self.print_level <= 2:
                print("[ {} ] - Performing DFS for leaf task {}.".format(datetime.datetime.utcnow(), leaf_task_key))
            DFS(leaf_task_state, new_path, current_path_size, visited, isNewPath = False, incrDepCounter = False)
        
        DFS_end = pythontime.time()
        
        DFS_length = DFS_end - DFS_start

        metrics["DFS"] = DFS_length

        # Debug. Print the paths.
        if self.print_debug and self.print_level <= 1:
            counter = 0
            for path in paths:
                print("\n\nPath #", counter)
                for task in path.tasks:
                    # The task payload is mostly just serialized gibberish here.
                    task_str = task.to_string_no_task_payload()
                    if self.print_debug_max_chars > 0:
                        task_str = task_str[0:self.print_debug_max_chars] + "..."
                    print(task_str)
                if self.use_fargate:
                    print("\nPath #{} Tasks-to-Fargate-Nodes Map:".format(counter))
                    for task_key, map in path.tasks_to_fargate_nodes.items():
                        print("\t{} --> {}".format(task_key, str(map)))
                counter = counter + 1

        print("\n[ {} ] Number of Paths created: {}.".format(datetime.datetime.utcnow(), len(paths)))

        if self.print_debug and self.print_level <= 3:
            print("Serializing path starts...")

        _serialization_start = pythontime.time()

        # Serialize all of the paths and store them in Redis.
        serialized_paths = {}
        path_counter = 1
        encoded_nodes = {}
        for task_key, path in tasks_to_path_starts.items():
            nodes = {}
            starting_node_key = path.get_start().task_key

            # Store each node in the dictionary under its associated task key. We encode the bytes-form of the nodes so we can send it to Lambda (can't send bytes directly).
            for node in path.tasks:
                if node.task_key in encoded_nodes:
                    nodes[node.task_key] = encoded_nodes[node.task_key]
                else:
                    encoded = base64.encodestring(tasks_to_serialized_path_node[node.task_key]).decode(ENCODING)
                    nodes[node.task_key] = encoded
                    encoded_nodes[node.task_key] = encoded
                for invoke_node_key in node.invoke:
                    if invoke_node_key in encoded_nodes:
                        nodes[invoke_node_key] = encoded_nodes[invoke_node_key]
                    else:
                        encoded = base64.encodestring(tasks_to_serialized_path_node[invoke_node_key]).decode(ENCODING)
                        nodes[invoke_node_key] = encoded
                        encoded_nodes[invoke_node_key] = encoded                    
            if type(self.executors_use_task_queue) is tuple:
                self.executors_use_task_queue = self.executors_use_task_queue[0]
            payload = {
                "nodes-map": nodes, 
                "lambda-debug": self.lambda_debug, 
                "use-bit-counters": self.use_bit_dep_checking, 
                EXECUTOR_TASK_QUEUE_KEY: self.executors_use_task_queue, 
                "starting-node-key": starting_node_key, 
                "use-fargate": self.use_fargate,
                "executor_function_name": self.executor_function_name,
                "invoker_function_name": self.invoker_function_name,
                "proxy_address": self.proxy_address,
                TASK_TO_FARGATE_MAPPING: path.tasks_to_fargate_nodes,
                # If self.reuse_lambdas is False, then we don't care if this is a leaf task or not.
                # We're not going to use it no matter what, so we may as well treat it like its not.
                "is-leaf": leaf_tasks.get(task_key, False) and self.reuse_lambdas 
            }
            serialized_payload = ujson.dumps(payload)
            serialized_paths[task_key] = serialized_payload
            #if self.print_debug:
            #    host = self.big_hash_ring.get_node_hostname(task_key)
            #    port = self.big_hash_ring.get_node_port(task_key)
            #    print("Path beginning at task {} will be stored in Redis instance listening at {}:{}".format(task_key, host, port))
            path_key = task_key + PATH_KEY_SUFFIX
            if self.print_debug and self.print_level <= 1:
                print("\nPath #{} - {}".format(path_counter, task_key))
                print("\tLength of Path:", len(nodes), "tasks")
                path_size = sys.getsizeof(serialized_payload)
                print("\tSize of Path:", path_size, "bytes")
                path_sizes.append(path_size)
            #associated_redis_client = self.big_hash_ring.get_node_instance(task_key)
            initial_payloads[self.dcp_redis][path_key] = serialized_payload  
            path_counter += 1
            
        _serialization_done = pythontime.time()
        _serialization_length = _serialization_done - _serialization_start
        
        metrics["Path-Serialization"] = _serialization_length

        print("[ {} ] Done serializing all {} payloads. Took {} seconds. Storing paths in Redis now.".format(datetime.datetime.utcnow(), len(serialized_paths), _serialization_length))

        if self.print_debug and self.print_level <= 3:
            print("\nStoring dependency counters and paths now...")
            print("Number of paths to store: ", len(tasks_to_path_starts))
        
        _store_paths_redis_start = pythontime.time()

        if self.use_fargate:
        # Add metadata to payload.
            # TODO: Optimize this process; fix any and all issues with using DFS exclusively for fargate data.
            for task_key, fargate_dict in self.tasks_to_fargate_nodes.items():
                fargate_metadata_key = task_key + FARGATE_DATA_SUFFIX
                initial_payloads[self.dcp_redis][fargate_metadata_key] = ujson.dumps(fargate_dict)

        # Store everything.
        for client, payload in initial_payloads.items():
            if len(payload) > 0:
                client.mset(payload)

        _store_paths_redis_stop = pythontime.time()
        _store_paths_redis_length = _store_paths_redis_stop - _store_paths_redis_start

        metrics["Store-Paths-Redis"] = _store_paths_redis_length

        print("[ {} ] Done storing paths in Redis. Invoking Lambdas now.".format(datetime.datetime.utcnow()))

        # Construct a payload to send to the Redis proxy containing the keys for all paths 
        # in this graph. The proxy can then just grab the paths from Redis (and thus the path nodes).
        path_keys_for_proxy = [_key + PATH_KEY_SUFFIX for _key in serialized_paths]
        if self.print_debug and self.print_level <= 1:
            print("Path keys for Redis Proxy: ", path_keys_for_proxy)
        #payload_for_proxy = {"op": "graph-init", "path-keys": path_keys_for_proxy, "scheduler-address": self.address}

        #self.loop.add_callback(self.send_message_to_proxy, payload = payload_for_proxy)

        if self.print_debug and self.print_level <= 1:
            print("Stored the following paths in Redis: ")
            for task_key in serialized_paths:
                path_key = task_key + PATH_KEY_SUFFIX
                print(path_key)

        _invoke_leaf_tasks_start = pythontime.time()
        
        num_invoked = 0
        num_existing = 0
        # Invoke all of the leaf tasks.
        for leaf_task_key, leaf_task_state in leaf_tasks.items():
            # If we're just not re-using Lambdas, then bypass the check. If we are
            # re-using Lambdas, then we'll only want to invoke this if we haven't seen
            # it before. If we have, then writing its path to Redis should've triggered
            # the Lambda's execution for the next phase/iteration of the workload.
            if self.reuse_lambdas == False:
                payload = serialized_paths[leaf_task_key]

                # We can only send a payload of size 256,000 bytes or less to a Lambda function directly.
                # If the payload is too large, then the Lambda will retrieve it from Redis via the key we provide.
                if sys.getsizeof(payload) > max_path_size_bytes:
                    # Create a new payload (that is definitely smaller than 256,000 bytes).
                    # The new payload will specify where to retrieve the original payload.
                    updated_payload = {
                        "path-key": leaf_task_key + PATH_KEY_SUFFIX,
                        "use-fargate": self.use_fargate,
                        "executor_function_name": self.executor_function_name,
                        "invoker_function_name": self.invoker_function_name,
                        "proxy_address": self.proxy_address                        
                    }
                    payload = ujson.dumps(updated_payload)
                self.batched_lambda_invoker.send(payload)
                num_invoked += 1                
            elif self.seen_leaf_tasks.get(leaf_task_key, False) == False:
                payload = serialized_paths[leaf_task_key]

                # We can only send a payload of size 256,000 bytes or less to a Lambda function directly.
                # If the payload is too large, then the Lambda will retrieve it from Redis via the key we provide.
                if sys.getsizeof(payload) > max_path_size_bytes:
                    # Create a new payload (that is definitely smaller than 256,000 bytes).
                    # The new payload will specify where to retrieve the original payload.
                    updated_payload = {
                        "path-key": leaf_task_key + PATH_KEY_SUFFIX,
                        "use-fargate": self.use_fargate,
                        "executor_function_name": self.executor_function_name,
                        "invoker_function_name": self.invoker_function_name,
                        "proxy_address": self.proxy_address                        
                    }
                    payload = ujson.dumps(updated_payload)
                self.dcp_redis.set(leaf_task_key + ITERATION_COUNTER_SUFFIX, 0)
                self.batched_lambda_invoker.send(payload)
                num_invoked += 1

                # Record that we've now seen this leaf task and increment is counter in Redis.
                self.seen_leaf_tasks[leaf_task_key] = True 
            else:
                if self.print_debug and self.print_level <= 2:
                    print("[LEAF TASK] Encountered leaf task {} while invoking. It has been seen before.".format(leaf_task_key))
                    
                    # Leaf Lambdas will be subscribed to a channel with name of the form PREFIX + leaf_task_key + PATH_SUFFIX.
                    #channel = leaf_task_channel_prefix + leaf_task_key + PATH_KEY_SUFFIX
                    #channel = leaf_task_key + PATH_KEY_SUFFIX
                    #num_subscribed = self.dcp_redis.execute_command('PUBSUB', 'NUMSUB', channel)[1]

                    #if self.print_debug and self.print_level <= 2:
                    #    print("\tNumber of Subscribers to {}: {}".format(channel, num_subscribed))

                    # Make sure that there is at least one Lambda subscribed to the channel. If there aren't any, then it's quite possible
                    # that the leaf Lambda gave up already and thus we need to invoke a new leaf Lambda, or else the task will never get executed.
                    #if num_subscribed < 1:
                    #    print("\t[WARNING] No subscribers found for channel {}... explicitly invoking leaf task to be safe...".format(channel))
                    #    payload = serialized_paths[leaf_task_key]

                        # We can only send a payload of size 256,000 bytes or less to a Lambda function directly.
                        # If the payload is too large, then the Lambda will retrieve it from Redis via the key we provide.
                    #    if sys.getsizeof(payload) > max_path_size_bytes:
                    #        payload = ujson.dumps({"path-key": leaf_task_key + PATH_KEY_SUFFIX})
                    #    self.batched_lambda_invoker.send(payload)
                    #    self.seen_leaf_tasks[leaf_task_key] = True    
                    #    num_invoked += 1
                    #else:
                    #num_existing += 1
                    print("\tOpting not to invoke Lambda for {}".format(leaf_task_key))
                #channel = leaf_task_key + PATH_KEY_SUFFIX
                num_existing += 1
                # Publish message to the Lambda.
                # self.dcp_redis.publish(channel, "set")
                self.dcp_redis.incr(leaf_task_key + ITERATION_COUNTER_SUFFIX)


        _invoke_leaf_tasks_stop = pythontime.time()
        _invoke_leaf_tasks_length = _invoke_leaf_tasks_stop - _invoke_leaf_tasks_start

        metrics["Invoke-Leaf-Tasks"] = _invoke_leaf_tasks_length

        print("[ {} ] - Scheduler: {} leaf tasks have been submitted for invocation while {} were already existing ({} total).".format(datetime.datetime.utcnow(), num_invoked, num_existing, len(leaf_tasks)))

        print("[INFO] Largest Fanout: Task {} with a fanout factor of {}!".format(largest_fanout_task_key, largest_fanout))
        if (self.print_debug and self.print_level <= 1):
            # Avoid ZeroDivisionErrors by checking length of arrays before attempting to divide by said lengths. That being said,
            # these lengths should almost always be non-zero.
            if len(task_sizes) != 0:
                print("[INFO] Average task size: {} bytes.".format(sum(task_sizes) / len(task_sizes)))
            else:
                # Print some sort of warning since zero tasks may indicate an error...
                print("[WARNING] There were no tasks. Average task size in bytes: N/A.")
            if len(path_sizes) != 0:
                print("[INFO] Average path size: {} bytes.".format(sum(path_sizes) / len(path_sizes)))
            else:
                # Print some sort of warning since zero tasks may indicate an error...
                print("[WARNING] There were no paths. Average path size in bytes: N/A.")

        # TO-DO: 
        # - Serialize each path.
        # - Store necessary path in Redis.
        #       - Will probably just iterate through the tasks that serve as a starting node for paths and store those in Redis...
        #       - Store payload/path under <task key> + "---payload" or <task key> + "---path".
        # - Invoke each leaf task, sending the associated path as its payload.

        #print("[ {} ] Scheduler - INFO: Invoked {} leaf tasks.".format(datetime.datetime.utcnow(), len(immediately_invocable_task_payloads)))

        # Transition everything to processing.
        for tk, ts in self.tasks.items():
            self.transition_waiting_processing_lambda(tk)
        
        #self.transitions(recommendations)

        for ts in touched_tasks:
            if ts.state in ("memory", "erred"):
                self.report_on_key(ts.key, client=client)

        end = pythontime.time()
        if self.digests is not None:
            self.digests["update-graph-duration"].add(end - start)
        _now = datetime.datetime.utcnow()
        print("Number of Tasks: ", len(tasks))
        print("Number of Leaf Tasks: ", len(leaf_tasks))        
        print("[ {} ] Scheduler - INFO: Update graph duration was {} seconds.".format(_now, end - start))
        for _label,_length in metrics.items():
            print("{} took {} seconds...".format(_label, _length))
        # TODO: balance workers

    def construct_basic_task_payload(self, task_key, ts, already_executed = False, persist = False):
        """Construct a standard payload for a given task state and task key. Used by AWS Lambda functions when executing tasks.
        
            Args:
                task_key (str):             the corresponding task key.

                ts (TaskState):             the TaskState object used to construct the basic payload.

                already_executed (bool):    indicates whether or not this task has previously been executed (i.e., in a prev. workload)
                                            and therefore does not need to be executed again. The data can just be retrieved from Redis.
                                            If the data is not found, then the Task Executor responsible for this task will just re-execute
                                            the task to obtain the data.
                
                persist (bool):             If true, then this task is part of a Persist operation. This means that the task should write its 
                                            data to Fargate EVEN IF IT IS A FINAL RESULT.

            Returns:
                dict: A dictionary with the information needed by the AWS Lambda Task Executors for executing the given task.
        """

        # Compute the number of dependents for this task. The dependents are downstream tasks that require this task's data.
        num_dependencies_of_dependents = {dependent.key : len(dependent.dependencies) for dependent in ts.dependents}
            
        # The basic payload.
        payload = {
            "key": task_key,
            "persist": persist,
            "dependencies": [dep.key for dep in ts.dependencies], # We want the keys as they correspond to entries in the Elasticache.
            "scheduler-address": self.address,
            "use-fargate": self.use_fargate,
            "num-dependencies-of-dependents": num_dependencies_of_dependents,
            #"proxy-channel": self.redis_channel_names_for_proxy[self.current_redis_proxy_channel_index],
            "chunk-large-tasks": self.chunk_large_tasks,
            "big-task-threshold": self.big_task_threshold,
            "num-chunks-for-large-tasks": self.num_chunks_for_large_tasks or -1,
            "already-executed": already_executed
        }  

        # Reset the index if it is now too large.
        #self.current_redis_proxy_channel_index += 1
        #if self.current_redis_proxy_channel_index >= self.num_redis_proxy_channels:
        #    self.current_redis_proxy_channel_index = 0

        # The run spec defines how to execute the task. This includes the task's code.
        task_run_spec = ts.run_spec
           
        # If it's a dictionary, we just update the payload with the values. The Lambda will know how to construct and execute the task from these values.
        if type(task_run_spec) is dict:
            payload.update(task_run_spec)
        else:
            # If the task is not a dictionary, then it's presumably a serialized object so we just include it directly. The Lambda will deserialize it.
            payload["task"] = task_run_spec
        
        if self.print_debug and self.print_level <= 1:
            run_spec_str = str(task_run_spec)
            if self.print_debug_max_chars > 0:
                run_spec_str = run_spec_str[0:self.print_debug_max_chars] + "..."
            print("Run Spec for {}: {}".format(task_key, run_spec_str))

        return payload 
    
    def launch_fargate_nodes(self, desired_num_tasks, fargate_tasks, max_retries = 30):
        # Check if the user is requests more Fargate tasks than the platform allows.
        if (desired_num_tasks > MAX_FARGATE_TASKS):
            print("[WARNING] Specified {} Fargate nodes, which is greater than maximum of {}. Clamping value to {}.".format(desired_num_tasks, MAX_FARGATE_TASKS, MAX_FARGATE_TASKS))
            desired_num_tasks = MAX_FARGATE_TASKS
        
        if (desired_num_tasks <= 0):
            print("[Warning] Specified {} Fargate nodes, which is not valid. Using {} instead.".format(desired_num_tasks, DEFAULT_NUM_FARGATE_NODES))
            desired_num_tasks = DEFAULT_NUM_FARGATE_NODES
            
        print("[FARGATE] There are already {} tasks running according to the Scheduler's internal records...".format(len(self.workload_fargate_tasks['current'])))

        # Check for existing service.
        describe_services_response = self.ecs_client.describe_services(
            cluster = self.ecs_cluster_name,
            services = [ECS_SERVICE_NAME]
        )

        services = describe_services_response['services']
        
        # Check if there already exists a service.
        if len(services) == 0:
            if self.print_debug:
                print("[FARGATE] No existing services. Creating new service {} with task definition {}, desired count {}, etc.".format(ECS_SERVICE_NAME, self.ecs_task_definition, desired_num_tasks))

            # No existing service...
            self.ecs_client.create_service(
                cluster = self.ecs_cluster_name,
                serviceName = ECS_SERVICE_NAME,
                taskDefinition = self.ecs_task_definition,
                desiredCount = desired_num_tasks,
                platformVersion = 'LATEST',
                networkConfiguration = self.ecs_network_configuration,
                #loadBalancers = [None],
                schedulingStrategy = 'REPLICA',
                tags = [{
                    'key': FARGATE_TASK_TAG,
                    'value': FARGATE_TASK_TAG
                }]
            )                                      
        else:
            wukong_service = services[0]
            if self.print_debug:
                print("Existing service's status: {}".format(wukong_service['status']))
            # Make sure the existing service is active before attempting to use it/interact with it. 
            if wukong_service['status'] == 'INACTIVE':

                if self.print_debug:
                    print("[FARGATE] Existing service is 'INACTIVE.' Creating new service {} with task definition {}, desired count {}, etc.".format(ECS_SERVICE_NAME, self.ecs_task_definition, desired_num_tasks))

                self.ecs_client.create_service(
                    cluster = self.ecs_cluster_name,
                    serviceName = ECS_SERVICE_NAME,
                    taskDefinition = self.ecs_task_definition,
                    desiredCount = desired_num_tasks,
                    platformVersion = 'LATEST',
                    networkConfiguration = self.ecs_network_configuration,
                    #loadBalancers = [None],
                    schedulingStrategy = 'REPLICA',
                    tags = [{
                        'key': FARGATE_TASK_TAG,
                        'value': FARGATE_TASK_TAG
                    }]
                )        
            else:
                existing_service_desired_count = wukong_service['desiredCount']

                # Check if we need to update the existing service to match our desired number of tasks.
                if existing_service_desired_count != desired_num_tasks:

                    if self.print_debug:
                        print("[FARGATE] Updating existing service {} with new desired count of {}. (It was {})".format(ECS_SERVICE_NAME, desired_num_tasks, existing_service_desired_count))

                    self.ecs_client.update_service(
                        cluster = self.ecs_cluster_name,
                        service = ECS_SERVICE_NAME,
                        desiredCount = desired_num_tasks
                    )
        
        num_running = -1

        num_checks = 0
        while num_running != desired_num_tasks:
            describe_services_response = self.ecs_client.describe_services(
                        cluster = self.ecs_cluster_name,
                        services = [ECS_SERVICE_NAME]
                    )    
            services = describe_services_response['services']
            wukong_service = services[0]
            num_running = wukong_service['runningCount']

            # If we're growing the Fargate cluster, then we should pri.nt how many tasks have started running so far out of how
            # many we asked for. If we're shrinking the Fargate cluster, then we should print how many tasks still need to stop.
            if num_running < desired_num_tasks:
                print("[FARGATE - {}] {}/{} have entered the 'RUNNING' state...".format(datetime.datetime.utcnow(), num_running, desired_num_tasks))
            elif num_running < desired_num_tasks:
                num_to_stop = desired_num_tasks - num_running
                print("[FARGATE] Still waiting for another {} tasks to stop...".format(num_to_stop))
            num_checks = num_checks + 1
            
            # Only sleep if this isn't our first check and the tasks haven't finished starting.
            if num_checks != 1 and num_running != desired_num_tasks:
                pythontime.sleep(5)
        
        print("[FARGATE] All {} tasks have entered the 'RUNNING state. Collecting metadata now.".format(desired_num_tasks))

        # TODO: Add option to skip this if the number of nodes hasn't changed.
        # Or just show new nodes/nodes that we don't already have (without having
        # to call list_tasks and describe_tasks for all of them). Like perhaps filter
        # through which tasks we already have first, before calling describe_tasks... 
        
        task_arn_lists = list()
        # List the tasks.
        list_tasks_response = self.ecs_client.list_tasks(cluster = self.ecs_cluster_name)
        taskArns = list_tasks_response['taskArns']
        task_arn_lists.append(taskArns)
        while 'nextToken' in list_tasks_response:
            nextToken =list_tasks_response['nextToken']
            list_tasks_response = self.ecs_client.list_tasks(cluster = self.ecs_cluster_name, nextToken = nextToken)
            taskArns = list_tasks_response['taskArns']
            task_arn_lists.append(taskArns)  
        task_descriptions = list() 
        for task_arn_list in task_arn_lists:
            descriptions = self.ecs_client.describe_tasks(cluster = self.ecs_cluster_name, tasks = task_arn_list)['tasks']
            task_descriptions.extend(descriptions)
        
        # Iterate over all of the running tasks...
        for task_description in task_descriptions:
            if task_description['group'] == FARGATE_TASK_GROUP:
                taskArn = task_description['taskArn']
                privateIP = task_description['containers'][0]['networkInterfaces'][0][FARGATE_PRIVATE_IP_KEY]
                # Otherwise, collect the information and store it.
                eniID = task_description['attachments'][0]['details'][1]['value']
                network_interface_description = self.ec2_client.describe_network_interfaces(NetworkInterfaceIds = [eniID])
                publicIP = network_interface_description['NetworkInterfaces'][0]['Association']['PublicIp']                    
                fargate_node = {
                    FARGATE_ARN_KEY: taskArn,
                    FARGATE_ENI_ID_KEY: eniID,
                    FARGATE_PUBLIC_IP_KEY: publicIP,
                    FARGATE_PRIVATE_IP_KEY: privateIP
                }
                # We may already have this task in our collection, in which case just ignore it.
                if not fargate_node in fargate_tasks:
                    fargate_tasks.append(fargate_node)         

    def launch_tasks(self, remaining, max_retries, starting):
        """ Launch Fargate tasks.

            Args:
                remaining (int):   the number of tasks we (still) need to launch.
                
                max_retries (int): the number of times we'll attempt to make a call to self.ecs_client.run_tasks(...) before giving up.

                starting (list):       list of Fargate tasks we've started.
            Returns:
                bool -- Flag indicating whether or not we launched all of the tasks (as far as we can tell).
        """
        # The base wait interval in milliseconds.
        base_wait_interval = 100

        # Keep invoking tasks (10 at a time, or less if there are less than 10 tasks that need to be invoked).
        while (remaining > 0):
            retry = True # Starts out as true so we enter the while-loop body at least once.
            retries = 0
            
            num_invoke = -1 
            # Again, you can only invoke ten tasks at a time via the run_task API.
            if remaining > 10:
                num_invoke = 10
            else:
                num_invoke = remaining
            remaining = remaining - num_invoke
            
            # Eventually we'll just give up. 
            while (retries < max_retries and retry):
                print("[FARGATE] Trying to launch {} of the {} remaining Fargate tasks...".format(num_invoke, remaining))
                try:
                    response = self.ecs_client.run_task(cluster = self.ecs_cluster_name,
                                                        #capacityProviderStrategy = [{
                                                        #    'capacityProvider': 'FARGATE_SPOT', # This has a maximum of 250 tasks so we'll use this ('FARGATE' only allows 100 tasks).
                                                        #    'weight': 1,            # Relative percentage of tasks that should be launched with this strategy/provider.
                                                        #                            # This is our only strategy so the value shouldn't manner.
                                                        #    'base': 1               # This defines the minimum # of tasks to be launched with this strategy.
                                                        #                            # This is our only strategy so the value shouldn't manner.
                                                        #}],
                                                        #launchType = 'FARGATE',
                                                        taskDefinition = self.ecs_task_definition,
                                                        group = FARGATE_TASK_GROUP,
                                                        count = num_invoke,
                                                        platformVersion = 'LATEST',
                                                        networkConfiguration = self.ecs_network_configuration)
                    retry = False
                    
                    if self.print_debug:
                        #print(response, "\n\n")
                        print("[FARGATE] Launched {} of the {} remaining Fargate tasks...".format(num_invoke, remaining))

                    starting.extend(response['tasks'])

                    pythontime.sleep(0.5)
                except Exception as ex:
                    print("[{}] Fargate exception encountered...".format(datetime.datetime.utcnow()))
                    print(ex)

                    sleep_interval = ((2 ** retries) * base_wait_interval) / 1000.0
                    print("[FARGATE] Sleeping for {} milliseconds...".format(sleep_interval * 1000))
                    pythontime.sleep(sleep_interval)
                    
                    retry = True
                    retries = retries + 1 

                    # If we reached the maximum number of tries, return False indicating a failure to launch the nodes.
                    if retries >= max_retries:
                        return False       
        return True 

    def close_fargate_tasks(self):
        """Close all Fargate tasks associated with the last job (or last jobs if you didn't close them in between workloads)."""
        fargate_tasks = self.workload_fargate_tasks['current']

        if self.print_debug:
            print("[FARGATE] Stopping {} tasks...".format(len(fargate_tasks)))

        for task_definition in fargate_tasks:
            task_arn = task_definition['taskArn']
            self.ecs_client.stop_task(
                cluster = self.ecs_cluster_name,
                task = task_arn)
            fargate_tasks.remove(task_definition)

    @gen.coroutine 
    def send_message_to_proxy(self, payload, start_handling = False):
        if self.proxy_comm is None:
            print("[ {} ] Connection to proxy is None. Attempting to connect now...".format(datetime.datetime.utcnow()))
            self.proxy_comm = yield connect("tcp://" + self.proxy_address + ":8989", deserialize=self.deserialize, connection_args={})
        elif self.proxy_comm is not None and self.proxy_comm.closed(): 
           print("[ {} ] [WARNING] Connection to proxy is closed... attempting to reconnect now...".format(datetime.datetime.utcnow()))
           self.proxy_comm = yield connect("tcp://" + self.proxy_address + ":8989", deserialize=self.deserialize, connection_args={})
        print("[ {} ] Sending message to Redis proxy now.".format(datetime.datetime.utcnow()))
        # if (self.print_debug):
            # print("[ {} ] Message contents:\n\t{}".format(datetime.datetime.utcnow(), payload))
        yield self.proxy_comm.write(payload, on_error="raise", serializers=["msgpack"]) 
        if start_handling:
            yield self.handle_stream(comm = self.proxy_comm)
    
    def check_health_of_fargate_tasks(self):
        fargate_nodes = self.workload_fargate_tasks['current']
        good_nodes = []
        bad_nodes = []
        counter = 1
        for fn in fargate_nodes:
            arn = fn[FARGATE_ARN_KEY]
            #ip = fn[FARGATE_PUBLIC_IP_KEY]
            ip = fn[FARGATE_PRIVATE_IP_KEY]
            print("\nChecking Fargate node #{} -- Redis @ {}:6379 ({})".format(counter, ip, arn))
            counter = counter + 1
            try:
                rc = redis.Redis(host = ip, port = 6379, db = 0, socket_timeout = 10, socket_connect_timeout = 10)
                res = rc.ping() 
                if res == False:
                    print("\tCould not connect/ping...")
                    bad_nodes.append(fn)
                else:
                    print("\tConnected and pinged successfully!")
                    good_nodes.append(fn)
            except:
                bad_nodes.append(fn)
        
        return {
            "good": good_nodes,
            "bad": bad_nodes
        }

    def stop_fargate_tasks(self, task_arns):
        """ Tells the Scheduler to stop the Fargate tasks with ARNs defined in the task_arns parameter.

            Args:
                task_arns (list): List of Fargate Task ARNs associated with the tasks we'd like to stop.
                                  Can also pass dicts so long as they have a key FARGATE_ARN_KEY (the key is equal to 
                                  the value of the FARGATE_ARN_KEY constant) and the value associated with said
                                  key is the ARN of the task. 
            
            Returns:
                dict: A dictionary which maps ARNs --> the AWS response from the stop_task ECS boto3 function.

        """
        responses = {}
        for x in task_arns:
            arn = x 
            if type(x) is dict:
                arn = arn[FARGATE_ARN_KEY]
            response = self.ecs_client.stop_task(cluster = self.ecs_cluster_name, task = arn)
            responses[arn] = response

        # Remove all the stopped Fargate nodes/tasks from our local mapping.
        self.workload_fargate_tasks['current'] = [fn for fn in self.workload_fargate_tasks['current'] if fn[FARGATE_ARN_KEY] not in task_arns]

        return responses

    def flush_data_on_redis_shards(self, asynchronous = True, rewrite_address = True, socket_connect_timeout = 5, socket_timeout = 5):   
        """ Clear all of the data on each Fargate shard and the EC2 Redis instance using the flushall command."""
        self.dcp_redis.flushall(asynchronous = asynchronous)
        for fargate_node in self.workload_fargate_tasks['current']:
            #fargate_ip = fargate_node[FARGATE_PUBLIC_IP_KEY]
            fargate_ip = fargate_node[FARGATE_PRIVATE_IP_KEY]
            redis_client = redis.StrictRedis(host = fargate_ip, port = 6379, db = 0, socket_connect_timeout  = socket_connect_timeout, socket_timeout = socket_timeout)
            redis_client.flushall(asynchronous = asynchronous)
        if rewrite_address:
            self.dcp_redis.set("scheduler-address", self.address)

    def flush_db_data_on_redis_shards(self, asynchronous = True, rewrite_address = True, socket_connect_timeout = 5, socket_timeout = 5):   
        """ Clear all of the data (in the current db) on each Fargate shard and the EC2 Redis instance using the flushdb command.
        
            Args:
                asynchronous (bool): If True, execute the Redis command asynchronously (do not wait for response).

                rewrite_address (bool): If True, rewrite the Scheduler's address to the dcp_redis server.
            
            Returns:
                dict: Dictionary with two entries. 
                    (1) The Fargate Node dictionaries (contain ARN, IP, and ENI ID) of nodes that had an error when executing the flushdb command
                    (2) Just the IP addresses of those nodes (for easy passing to the 'stop_fargate_tasks' function)
        """      
        self.dcp_redis.flushdb(asynchronous = asynchronous)
        counter = 1
        bad_nodes = []
        bad_ips = []
        for fargate_node in self.workload_fargate_tasks['current']:
            fargate_arn = fargate_node[FARGATE_ARN_KEY]
            #fargate_ip = fargate_node[FARGATE_PUBLIC_IP_KEY] 
            fargate_ip = fargate_node[FARGATE_PRIVATE_IP_KEY]
            if self.print_debug:
                print("\nAttempting to connect to Redis @ {}:6379 (ARN {})...".format(fargate_ip, fargate_arn))
            redis_client = redis.StrictRedis(host = fargate_ip, port = 6379, db = 0, socket_connect_timeout  = socket_connect_timeout, socket_timeout = socket_timeout)
            if self.print_debug:
                print("\tConnection successful! Executing 'flushdb' command now...")
            try:
                redis_client.flushdb(asynchronous = asynchronous)
            except:
                print("Redis is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails.")
                print("\tInstance is listening at {}:6379 ({}).".format(fargate_ip, fargate_arn))
                bad_nodes.append(fargate_node)
                bad_ips.append(fargate_ip)
            if self.print_debug:
                print("Flushed current db for Redis instance at {}:6379 ({}) [{}/{}]".format(fargate_ip, fargate_arn, counter, len(self.workload_fargate_tasks['current'])))
            counter += 1
        if rewrite_address:
            self.dcp_redis.set("scheduler-address", self.address)      
        
        return {
            "nodes": bad_nodes,
            "ips": bad_ips
        }

    def stimulus_task_finished(self, key=None, worker=None, **kwargs):
        """ Mark that a task has finished execution on a particular worker """
        logger.debug("Stimulus task finished %s, %s", key, worker)

        ts = self.tasks.get(key)
        if ts is None:
            return {}
        ws = self.workers[worker]

        if ts.state == "processing":
            recommendations = self.transition(key, "memory", worker=worker, **kwargs)

            if ts.state == "memory":
                assert ws in ts.who_has
        else:
            logger.debug(
                "Received already computed task, worker: %s, state: %s"
                ", key: %s, who_has: %s",
                worker,
                ts.state,
                key,
                ts.who_has,
            )
            if ws not in ts.who_has:
                self.worker_send(worker, {"op": "release-task", "key": key})
            recommendations = {}

        return recommendations

    #def get_redis_client(self, task_key):
    #    hash_obj = hashlib.md5(task_key.encode())
    #    val = int(hash_obj.hexdigest(), 16)
    #    client_index = val % self.num_redis_clients
    #    return self.redis_clients[client_index]

    def stimulus_task_finished_lambda(self, key = None, **kwargs):
        """Mark that a particular task has finished execution on AWS Lambda """
        # print("[ {} ] Scheduler - DEBUG: Stimulus task finished executing on AWS Lambda ".format(datetime.datetime.utcnow(), key))
        logger.debug("Stimulus task finished on AWS Lambda %s", key)
        # print("Stimulus task finished on AWS Lambda ", key)
        ts = self.tasks.get(key)
        if ts is None:
            # print("Task State for key {} is None. Returning empty dict.".format(key))
            return {}
        # print("Task State BEFORE transitioning for Task {} is {}.".format(key, ts.state))
        if ts.state == "processing":
            # print("**kwargs: ", kwargs)
            recommendations = self.transition(key, "memory", worker = None, **kwargs)
            
            if self.print_debug and self.print_level <= 1:
                print("Task State AFTER transitioning for Task {} is {}.".format(key, ts.state))
                print("Recommendations: ", recommendations)
            #if ts.state == "memory":
                # assert memcache_client.get(key.replace(" ", "_")) != None 
                # print("redis_client.get(key): ", redis_client.get(key))
                #assert self.small_hash_ring[key].exists(key) != 0 or self.big_hash_ring[key].exists(key) != 0
                #assert self.dcp_redis.exists(key) != 0
                #assert self.get_redis_client(key).exists(key) != 0
        else:
            # print("DEBUG: Received already computed task from AWS Lambda. State {}, Key: {}".format(ts.state, key))
            #logger.debug(
            #    "Received already computed task, state: %s"
            #    ", key: %s",
            #    ts.state,
            #    key,
            #)        
            #print("Received already compute task, state: {}, key: {}".format(ts.state, key))            
            recommendations = {}
        return recommendations
        
    def stimulus_task_erred(
        self, key=None, worker=None, exception=None, traceback=None, **kwargs
    ):
        """ Mark that a task has erred on a particular worker """
        logger.debug("Stimulus task erred %s, %s", key, worker)

        ts = self.tasks.get(key)
        if ts is None:
            return {}

        if ts.state == "processing":
            retries = ts.retries
            if retries > 0:
                ts.retries = retries - 1
                recommendations = self.transition(key, "waiting")
            else:
                recommendations = self.transition(
                    key,
                    "erred",
                    cause=key,
                    exception=exception,
                    traceback=traceback,
                    worker=worker,
                    **kwargs
                )
        else:
            recommendations = {}

        return recommendations

    def stimulus_task_erred_lambda(self, key = None, exception = None, traceback = None, **kwargs):
        """ Mark that a task has erred on a particular worker """
        #logger.debug("Stimulus task erred %s, %s", key, worker) 
        print("Stimulus task erred {}".format(key))
        print("Exception: {}\nTraceback: {}".format(exception, traceback))
        ts = self.tasks.get(key)
        if ts is None:
            return {}
        if ts.state == "processing":
            retries = ts.retries
            if retries > 0:
                ts.retries = retries - 1
                recommendations = self.transition(key, "waiting")
            else:
                recommendations = self.transition(key, "erred", cause = key, exception = exception, traceback = traceback, worker = None, **kwargs)
        else:
            recommendations = {}
        return recommendations
    
    def stimulus_missing_data(
        self, cause=None, key=None, worker=None, ensure=True, **kwargs
    ):
        """ Mark that certain keys have gone missing.  Recover. """
        with log_errors():
            logger.debug("Stimulus missing data %s, %s", key, worker)

            ts = self.tasks.get(key)
            if ts is None or ts.state == "memory":
                return {}
            cts = self.tasks.get(cause)

            recommendations = OrderedDict()

            if cts is not None and cts.state == "memory":  # couldn't find this
                for ws in cts.who_has:  # TODO: this behavior is extreme
                    ws.has_what.remove(cts)
                    ws.nbytes -= cts.get_nbytes()
                cts.who_has.clear()
                recommendations[cause] = "released"

            if key:
                recommendations[key] = "released"

            self.transitions(recommendations)

            if self.validate:
                assert cause not in self.who_has

            return {}

    def stimulus_retry(self, comm=None, keys=None, client=None):
        logger.info("Client %s requests to retry %d keys", client, len(keys))
        if client:
            self.log_event(client, {"action": "retry", "count": len(keys)})

        stack = list(keys)
        seen = set()
        roots = []
        while stack:
            key = stack.pop()
            seen.add(key)
            erred_deps = [
                dts.key for dts in self.tasks[key].dependencies if dts.state == "erred"
            ]
            if erred_deps:
                stack.extend(erred_deps)
            else:
                roots.append(key)

        recommendations = {key: "waiting" for key in roots}
        self.transitions(recommendations)

        if self.validate:
            for key in seen:
                assert not self.tasks[key].exception_blame

        return tuple(seen)

    def remove_worker(self, comm=None, address=None, safe=False, close=True):
        """
        Remove worker from cluster

        We do this when a worker reports that it plans to leave or when it
        appears to be unresponsive.  This may send its tasks back to a released
        state.
        """
        with log_errors():
            if self.status == "closed":
                return
            if address not in self.workers:
                return "already-removed"

            address = self.coerce_address(address)
            host = get_address_host(address)

            ws = self.workers[address]

            self.log_event(
                ["all", address],
                {
                    "action": "remove-worker",
                    "worker": address,
                    "processing-tasks": dict(ws.processing),
                },
            )
            logger.info("Remove worker %s", address)
            if close:
                with ignoring(AttributeError, CommClosedError):
                    self.stream_comms[address].send({"op": "close", "report": False})

            self.remove_resources(address)

            self.host_info[host]["cores"] -= ws.ncores
            self.host_info[host]["addresses"].remove(address)
            self.total_ncores -= ws.ncores

            if not self.host_info[host]["addresses"]:
                del self.host_info[host]

            self.rpc.remove(address)
            del self.stream_comms[address]
            del self.aliases[ws.name]
            self.idle.discard(ws)
            self.saturated.discard(ws)
            del self.workers[address]
            ws.status = "closed"
            self.total_occupancy -= ws.occupancy

            recommendations = OrderedDict()

            for ts in list(ws.processing):
                k = ts.key
                recommendations[k] = "released"
                if not safe:
                    ts.suspicious += 1
                    if ts.suspicious > self.allowed_failures:
                        del recommendations[k]
                        e = pickle.dumps(
                            KilledWorker(task=k, last_worker=ws.clean()), -1
                        )
                        r = self.transition(k, "erred", exception=e, cause=k)
                        recommendations.update(r)

            for ts in ws.has_what:
                ts.who_has.remove(ws)
                if not ts.who_has:
                    if ts.run_spec:
                        recommendations[ts.key] = "released"
                    else:  # pure data
                        recommendations[ts.key] = "forgotten"
            ws.has_what.clear()

            self.transitions(recommendations)

            for plugin in self.plugins[:]:
                try:
                    plugin.remove_worker(scheduler=self, worker=address)
                except Exception as e:
                    logger.exception(e)

            if not self.workers:
                logger.info("Lost all workers")

            @gen.coroutine
            def remove_worker_from_events():
                # If the worker isn't registered anymore after the delay, remove from events
                if address not in self.workers and address in self.events:
                    del self.events[address]

            cleanup_delay = parse_timedelta(
                dask.config.get("distributed.scheduler.events-cleanup-delay")
            )
            self.loop.call_later(cleanup_delay, remove_worker_from_events)
            logger.debug("Removed worker %s", address)

        return "OK"

    def stimulus_cancel(self, comm, keys=None, client=None, force=False):
        """ Stop execution on a list of keys """
        logger.info("Client %s requests to cancel %d keys", client, len(keys))
        if client:
            self.log_event(
                client, {"action": "cancel", "count": len(keys), "force": force}
            )
        for key in keys:
            self.cancel_key(key, client, force=force)

    def cancel_key(self, key, client, retries=5, force=False):
        """ Cancel a particular key and all dependents """
        # TODO: this should be converted to use the transition mechanism
        ts = self.tasks.get(key)
        try:
            cs = self.clients[client]
        except KeyError:
            return
        if ts is None or not ts.who_wants:  # no key yet, lets try again in a moment
            if retries:
                self.loop.add_future(
                    gen.sleep(0.2), lambda _: self.cancel_key(key, client, retries - 1)
                )
            return
        if force or ts.who_wants == {cs}:  # no one else wants this key
            for dts in list(ts.dependents):
                self.cancel_key(dts.key, client, force=force)
        logger.info("Scheduler cancels key %s.  Force=%s", key, force)
        self.report({"op": "cancelled-key", "key": key})
        clients = list(ts.who_wants) if force else [cs]
        for c in clients:
            self.client_releases_keys(keys=[key], client=c.client_key)

    def client_desires_keys(self, keys=None, client=None):
        cs = self.clients.get(client)
        if cs is None:
            # For publish, queues etc.
            cs = self.clients[client] = ClientState(client)
        for k in keys:
            ts = self.tasks.get(k)
            if ts is None:
                # For publish, queues etc.
                ts = self.tasks[k] = TaskState(k, None)
                ts.state = "released"
            ts.who_wants.add(cs)
            cs.wants_what.add(ts)

            if ts.state in ("memory", "erred"):
                self.report_on_key(k, client=client)

    def client_releases_keys(self, keys=None, client=None):
        """ Remove keys from client desired list """
        logger.debug("Client %s releases keys: %s", client, keys)
        cs = self.clients[client]
        tasks2 = set()
        for key in list(keys):
            ts = self.tasks.get(key)
            if ts is not None and ts in cs.wants_what:
                cs.wants_what.remove(ts)
                s = ts.who_wants
                s.remove(cs)
                if not s:
                    tasks2.add(ts)

        recommendations = {}
        for ts in tasks2:
            if not ts.dependents:
                # No live dependents, can forget
                recommendations[ts.key] = "forgotten"
            elif ts.state != "erred" and not ts.waiters:
                recommendations[ts.key] = "released"

        self.transitions(recommendations)

    def client_heartbeat(self, client=None):
        """ Handle heartbeats from Client """
        self.clients[client].last_seen = time()

    ###################
    # Task Validation #
    ###################

    def validate_released(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("validate_released"))
        ts = self.tasks[key]
        assert ts.state == "released"
        assert not ts.waiters
        assert not ts.waiting_on
        assert not ts.who_has
        assert not ts.processing_on
        assert not any(ts in dts.waiters for dts in ts.dependencies)
        assert ts not in self.unrunnable

    def validate_waiting(self, key):
        ts = self.tasks[key]
        assert ts.waiting_on
        assert not ts.who_has
        assert not ts.processing_on
        assert ts not in self.unrunnable
        for dts in ts.dependencies:
            # We are waiting on a dependency iff it's not stored
            assert bool(dts.who_has) + (dts in ts.waiting_on) == 1
            assert ts in dts.waiters  # XXX even if dts.who_has?

    def validate_processing(self, key):
        ts = self.tasks[key]
        assert not ts.waiting_on
        ws = ts.processing_on
        assert ws
        assert ts in ws.processing
        assert not ts.who_has
        for dts in ts.dependencies:
            assert dts.who_has
            assert ts in dts.waiters

    def validate_memory(self, key):
        ts = self.tasks[key]
        assert ts.who_has
        assert not ts.processing_on
        assert not ts.waiting_on
        assert ts not in self.unrunnable
        for dts in ts.dependents:
            assert (dts in ts.waiters) == (dts.state in ("waiting", "processing"))
            assert ts not in dts.waiting_on

    def validate_no_worker(self, key):
        ts = self.tasks[key]
        assert ts in self.unrunnable
        assert not ts.waiting_on
        assert ts in self.unrunnable
        assert not ts.processing_on
        assert not ts.who_has
        for dts in ts.dependencies:
            assert dts.who_has

    def validate_erred(self, key):
        ts = self.tasks[key]
        assert ts.exception_blame
        assert not ts.who_has

    def validate_key(self, key, ts=None):
        try:
            if ts is None:
                ts = self.tasks.get(key)
            if ts is None:
                logger.debug("Key lost: %s", key)
            else:
                ts.validate()
                try:
                    func = getattr(self, "validate_" + ts.state.replace("-", "_"))
                except AttributeError:
                    logger.error(
                        "self.validate_%s not found", ts.state.replace("-", "_")
                    )
                else:
                    func(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def validate_state(self, allow_overlap=False):
        validate_state(self.tasks, self.workers, self.clients)

        if not (set(self.workers) == set(self.stream_comms)):
            raise ValueError("Workers not the same in all collections")

        for w, ws in self.workers.items():
            assert isinstance(w, (str, unicode)), (type(w), w)
            assert isinstance(ws, WorkerState), (type(ws), ws)
            assert ws.address == w
            if not ws.processing:
                assert not ws.occupancy
                assert ws in self.idle

        for k, ts in self.tasks.items():
            assert isinstance(ts, TaskState), (type(ts), ts)
            assert ts.key == k
            self.validate_key(k, ts)

        for c, cs in self.clients.items():
            # client=None is often used in tests...
            assert c is None or isinstance(c, str), (type(c), c)
            assert isinstance(cs, ClientState), (type(cs), cs)
            assert cs.client_key == c

        a = {w: ws.nbytes for w, ws in self.workers.items()}
        b = {
            w: sum(ts.get_nbytes() for ts in ws.has_what)
            for w, ws in self.workers.items()
        }
        assert a == b, (a, b)

        actual_total_occupancy = 0
        for worker, ws in self.workers.items():
            assert abs(sum(ws.processing.values()) - ws.occupancy) < 1e-8
            actual_total_occupancy += ws.occupancy

        assert abs(actual_total_occupancy - self.total_occupancy) < 1e-8, (
            actual_total_occupancy,
            self.total_occupancy,
        )

    ###################
    # Manage Messages #
    ###################

    def report(self, msg, ts=None, client=None):
        """
        Publish updates to all listening Queues and Comms

        If the message contains a key then we only send the message to those
        comms that care about the key.
        """
        if client is not None:
            try:
                comm = self.client_comms[client]
                comm.send(msg)
            except CommClosedError:
                if self.status == "running":
                    logger.critical("Tried writing to closed comm: %s", msg)
            except KeyError:
                pass

        if ts is None and "key" in msg:
            ts = self.tasks.get(msg["key"])
        if ts is None:
            # Notify all clients
            comms = self.client_comms.values()
        else:
            # Notify clients interested in key
            comms = [
                self.client_comms[c.client_key]
                for c in ts.who_wants
                if c.client_key in self.client_comms
            ]
        for c in comms:
            try:
                c.send(msg)
                # logger.debug("Scheduler sends message to client %s", msg)
            except CommClosedError:
                if self.status == "running":
                    logger.critical("Tried writing to closed comm: %s", msg)

    @gen.coroutine
    def add_client(self, comm, client=None):
        """ Add client to network

        We listen to all future messages from this Comm.
        """
        assert client is not None
        comm.name = "Scheduler->Client"
        logger.info("Receive client connection: %s", client)
        self.log_event(["all", client], {"action": "add-client", "client": client})
        self.clients[client] = ClientState(client)
        try:
            bcomm = BatchedSend(interval="2ms", loop=self.loop)
            bcomm.start(comm)
            self.client_comms[client] = bcomm
            bcomm.send({"op": "stream-start", REDIS_ADDRESS_KEY: self.proxy_address}) 
            try:
                yield self.handle_stream(comm=comm, extra={"client": client})
            finally:
                self.remove_client(client=client)
                logger.debug("Finished handling client %s", client)
        finally:
            if not comm.closed():
                self.client_comms[client].send({"op": "stream-closed"})
            try:
                if not shutting_down():
                    yield self.client_comms[client].close()
                    del self.client_comms[client]
                    if self.status == "running":
                        logger.info("Close client connection: %s", client)
            except TypeError:  # comm becomes None during GC
                pass

    def remove_client(self, client=None):
        """ Remove client from network """
        if self.status == "running":
            logger.info("Remove client %s", client)
        self.log_event(["all", client], {"action": "remove-client", "client": client})
        try:
            cs = self.clients[client]
        except KeyError:
            # XXX is this a legitimate condition?
            pass
        else:
            self.client_releases_keys(
                keys=[ts.key for ts in cs.wants_what], client=cs.client_key
            )
            del self.clients[client]

        @gen.coroutine
        def remove_client_from_events():
            # If the client isn't registered anymore after the delay, remove from events
            if client not in self.clients and client in self.events:
                del self.events[client]

        cleanup_delay = parse_timedelta(
            dask.config.get("distributed.scheduler.events-cleanup-delay")
        )
        self.loop.call_later(cleanup_delay, remove_client_from_events)

    #def submit_task_to_lambda(self, key):
        #for line in traceback.format_stack():
        #    print(line.strip())    
        # Debug-related...
        # print("[", str(datetime.datetime.utcnow()), "]   Submitting Task ", key, " to Lambda...")
        #debug_msg = "[" + str(datetime.datetime.utcnow()) + "]   Submitting Task " + key + " to Lambda..."
        #logger.debug(debug_msg)
        #task_state = self.tasks[key]   
        #dependencies = task_state.dependencies
        # TO-DO: Need to check if the result already exists in the Elasticache cluster...
        #payload = {
        #    "key": key,
        #    "proxy-channel": self.redis_channel_names_for_proxy[self.current_redis_proxy_channel_index],
        #    "deps": [dep.key for dep in dependencies],# We want the keys as they correspond to entries in the Elasticache.
        #}
        #self.current_redis_proxy_channel_index += 1
        #if self.current_redis_proxy_channel_index >= self.num_redis_proxy_channels:
        #    self.current_redis_proxy_channel_index = 0
        #task = task_state.run_spec
        #if type(task) is dict:
        #    payload.update(task)
        #else:
        #    payload["task"] = task 
            #task_deserialized = task.deserialize()
        #payload_serialized = list(dumps(payload))
        # temp = list(dumps(payload))
        #payload_bytes = []
        #for bytes_object in temp:
        #    payload_bytes.append(base64.encodestring(bytes_object).decode(ENCODING))
#
        #self.batched_lambda_invoker.send(payload_serialized)
    
    @gen.coroutine
    def deserialize_payload(self, payload):
        msg = yield from_frames(payload)
        print("\nDeserialized Message: ", msg)   
        raise gen.Return(msg)        
        
    def simulate_lambda(self, event):
        """Simulate an AWS Lambda function receiving the payload and deserializing it."""
        temp = event['payload']
        payload_bytes = []
        for encoded_object in temp:
            payload_bytes.append(base64.b64decode(encoded_object))        
        #msg = IOLoop.current().run_sync(lambda: deserialize_payload(payload_bytes))    
        self.loop.add_callback(lambda: self.deserialize_payload(payload_bytes))
       
    def send_task_to_worker(self, worker, key):
        """ Send a single computational task to a worker """
        try:
            ts = self.tasks[key]
            msg = {
                "op": "compute-task",
                "key": key,
                "priority": ts.priority,
                "duration": self.get_task_duration(ts),
            }
            if ts.resource_restrictions:
                msg["resource_restrictions"] = ts.resource_restrictions
            if ts.actor:
                msg["actor"] = True

            deps = ts.dependencies
            if deps:
                msg["who_has"] = {
                    dep.key: [ws.address for ws in dep.who_has] for dep in deps
                }
                msg["nbytes"] = {dep.key: dep.nbytes for dep in deps}

            if self.validate and deps:
                assert all(msg["who_has"].values())

            task = ts.run_spec
            if type(task) is dict:
                msg.update(task)
            else:
                msg["task"] = task

            self.worker_send(worker, msg)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise       
    
    def poll_redis_process(self, _queue, redis_channel_name, redis_endpoint):
        ''' This function defines a process which continually polls Redis for messages. 
        
            When a message is found, it is passed to the main Scheduler process via the queue given to this process. ''' 
        print("Redis Polling Process started. Polling channel ", redis_channel_name)

        redis_client = redis.StrictRedis(host=redis_endpoint, port = 6379, db = 0)
        
        base_sleep_interval = 0.05 
        max_sleep_interval = 0.1 
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
                #timestamp_now = datetime.datetime.utcnow()
                data = message["data"]
                data = data.decode()
                data = ujson.loads(data)
                #if self.print_debug:
                #    print("[ {} ] Received message from AWS Lambda...".format(timestamp_now))
                #    print("\tMessage Contents: {}\n".format(data))
                _queue.put([data])
                consecutive_misses = 0
                current_sleep_interval = base_sleep_interval
            else:
                pythontime.sleep(current_sleep_interval + (random.uniform(0, 0.5) / 1000))
                consecutive_misses = consecutive_misses + 1
                current_sleep_interval += 0.05 
                if (current_sleep_interval > max_sleep_interval):
                    current_sleep_interval = max_sleep_interval

                
    def consume_redis_queue(self):
        ''' This function executes periodically (as a PeriodicCallback on the IO loop). 
        
        It reads messages from the message queue until none are available.'''
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
        for msg in messages:
            self.result_from_lambda(None, **msg)
               
    def result_from_lambda(self, comm, op, task_key, lambda_id = None, time_sent = None, **msg):  
        """ Handle the result from executing a task on AWS Lambda. """   
        self.num_messages_received_from_lambda = self.num_messages_received_from_lambda + 1
        if op == LAMBDA_RESULT_KEY:
            self.num_results_received_from_lambda = self.num_results_received_from_lambda + 1
            if self.print_debug == True:
                print("\n[ {} ] Received result from Lambda for task {}.".format(datetime.datetime.utcnow(), task_key))
                print("\tExecution Time: {} seconds".format(msg["execution-time"]))
                print("\t{} of {} tasks of current job completed.".format(self.last_job_counter, len(self.last_job_tasks)))
                print("\tTime Sent:", datetime.datetime.fromtimestamp(time_sent).isoformat())
                print("\t\tTotal # messages received from Lambda:", self.num_messages_received_from_lambda)                 
                print("\t\tTotal # results received from Lambda:", self.num_results_received_from_lambda, "\n")
            self.obtained_valid_result_from_lambda(task_key = task_key, **msg)    
            # We only keep track of this information if debug mode is enabled.
            if self.debug_mode and task_key not in self.executed_tasks:
                self.executed_tasks.append(task_key)
        elif op == EXECUTING_TASK_KEY:
            # We only keep track of this information if debug mode is enabled.
            if self.debug_mode:
                self.executing_tasks[task_key] = True

            # Avoid an invalid key exception by checking first. If key exists, update state and metadata of corresponding task.
            if task_key in self.tasks:
                self.tasks[task_key].state = "processing" 
                self.executing_tasks_counters[task_key] = self.executing_tasks_counters[task_key] + 1
                if self.print_debug:
                    print("\n[DEBUG - {}] Task {} began execution on Lambda {}.".format(datetime.datetime.utcnow(), task_key, lambda_id))   
                    if time_sent is not None:
                        print("\tTime Sent: {}".format(datetime.datetime.fromtimestamp(time_sent).isoformat()))             
            elif self.print_debug and self.print_level <= 2:
                print("[WARNING] Got notification that Task {} began executing on Lambda, but no entry for said task exists in self.tasks...".format(task_key))
            #self.transition_waiting_processing_lambda(task_key)
        elif op == EXECUTED_TASK_KEY:
            self.last_job_counter += 1
            self.executed_tasks.append(task_key)
            # Record that we've completed the task.
            self.completed_tasks[task_key] = True
            self.completed_task_counts[task_key] = self.completed_task_counts[task_key] + 1
            self.executing_tasks[task_key] = False
            #if task_key in self.tasks:
            #    self.tasks[task_key].state = "released"
            ##else:
            #    print("[WARNING] Got notification that Task {} finished execution on Lambda, but no entry for said task exists in self.tasks...".format(task_key))
            # If there already exists an entry for this task, then print a 'WARNING' message indicating this. 
            # We record all entries (not just the first) in the dictionary. Otherwise, just add an entry for the executed task.
            if task_key in self.completed_task_data:
                if type(self.completed_task_data[task_key]) is list:
                    self.completed_task_data[task_key].append({
                                                        "start": msg['start-time'],
                                                        "stop": msg['stop-time'],
                                                        "execution-time": msg['execution-time'],
                                                        "lambda-id": lambda_id,
                                                        "task-key": task_key,
                                                        DATA_SIZE: msg[DATA_SIZE],
                                                        "time_sent": time_sent,
                                                        })
                    if self.print_debug:
                        print("\n\n[WARNING] Task {} has now been executed {} times by AWS Lambda!\n".format(task_key, len(self.completed_task_data[task_key])))
                else:
                    prev_entry = self.completed_task_data[task_key]
                    new_entry = {
                            "start": msg['start-time'],
                            "stop": msg['stop-time'],
                            "execution-time": msg['execution-time'],
                            "lambda-id": lambda_id,
                            "task-key": task_key,
                            "time_sent": time_sent,
                        }
                    lst = [prev_entry, new_entry]
                    self.completed_task_data[task_key] = lst 
                    if self.print_debug:
                        print("\n\n[WARNING] Task {} has now been executed 2 times by AWS Lambda!\n".format(task_key))
            else:        
                self.completed_task_data[task_key] = {
                                "start": msg['start-time'],
                                "stop": msg['stop-time'],
                                "execution-time": msg['execution-time'],
                                "lambda-id": lambda_id,
                                "task-key": task_key,
                                "time_sent": time_sent,
                            }
            self.obtained_valid_result_from_lambda(task_key = task_key, **msg)
            if self.print_debug:
                print("[DEBUG - {}] Task {} has been executed successfully by Lambda {}.\n\tStart Time: {}\n\tStop Time: {}\n\tExecution Time: {} seconds.\n\t{} of {} tasks of last job completed\n\tTime Sent: {}\n".format(
                                                                                                                                                datetime.datetime.utcnow(),
                                                                                                                                                task_key, 
                                                                                                                                                lambda_id,
                                                                                                                                                datetime.datetime.fromtimestamp(msg['start-time']).isoformat(),
                                                                                                                                                datetime.datetime.fromtimestamp(msg['stop-time']).isoformat(),
                                                                                                                                                msg['execution-time'],
                                                                                                                                                self.last_job_counter,
                                                                                                                                                len(self.last_job_tasks),
                                                                                                                                                datetime.datetime.fromtimestamp(time_sent).isoformat()))
        elif op == TASK_ERRED_KEY:
            print("!!!!!!!!!!!!!!!!!!!!!!!!\n[TASK ERROR - {}] The execution of task {} resulted in an error. Start time: {}. Lambda ID: {}. Actual Exception: {}.\n\tTime Sent: {}\n".format(datetime.datetime.utcnow(), task_key,
                                                                                                                                              lambda_id,
                                                                                                                                              datetime.datetime.fromtimestamp(msg['start-time']).isoformat(),
                                                                                                                                              msg["actual-exception"],
                                                                                                                                              datetime.datetime.fromtimestamp(time_sent).isoformat())) 
            if task_key in self.tasks:
                self.tasks[task_key].state = "erred"                                                                                                                               
            else:
                print("[WARNING] Received error message for execution of task {} from Lambda, but there is no entry for this task in self.tasks...".format(task_key))
        else:
            raise ValueError("Unknown operation '{}' included in message from Lambda {}".format(op, lambda_id))
        
        time_diff = pythontime.time() - time_sent
        if time_diff >= 5:
            print("\n\n[WARNING] Long time difference between msg sent by Lambda and received by Scheduler: {} seconds\n\n".format(time_diff))
         
    def get_wukong_metrics(self):
        task_metrics = self.dcp_redis.lrange(TASK_BREAKDOWNS, 0, -1)
        lambda_metrics = self.dcp_redis.lrange(LAMBDA_DURATIONS, 0, -1)

        task_metrics = [cloudpickle.loads(res) for res in task_metrics]
        lambda_metrics = [cloudpickle.loads(res) for res in lambda_metrics]

        return {"task-metrics": task_metrics, "lambda-metrics": lambda_metrics}
    
    # def small_redis_statistics(self, n = 1000000):
    #     units = ""
    #     if n == 1:
    #         units = "bytes"
    #     elif n == 1000:
    #         units = "KB"
    #     elif n == 1000000:
    #         units = "MB"
    #     elif n == 1000000000:
    #         units = "GB"
    #     elif n == 1000000000000:
    #         units = "TB"
    #     else:
    #         raise  ValueError("Improper value for n")
            
    #     small_clients_memory_usage = []
    #     total_commands = []
    #     total_connections = []
    #     small_infos = []
    #     command_stats = dict()
    #     command_stat_keys = ["cmdstat_info", "cmdstat_exists", "cmdstat_flushall", "cmdstat_mget", "cmdstat_get", "cmdstat_mset", "cmdstat_set"]
    #     command_stats_formatted = dict()
    #     counter = 1

    #     for client in self.small_redis_clients:
    #         data = client.info()
    #         cmd_data = client.info("commandstats")
    #         command_stats[counter] = cmd_data
    #         small_infos.append(data)
    #         small_clients_memory_usage.append(data["used_memory"] / n)   
    #         total_connections.append(data["total_connections_received"])
    #         total_commands.append(data["total_commands_processed"])
    #         line_str = ""
    #         for key in command_stat_keys:
    #             if key in cmd_data:
    #                 line_str = line_str + str(cmd_data[key]['calls']) + ","
    #             else:
    #                 line_str = line_str + "0,"
    #         command_stats_formatted[counter] = line_str
    #         counter = counter + 1        

        
    #     x1 = np.array(small_clients_memory_usage)
    #     x2 = np.array(total_commands)
    #     x3 = np.array(total_connections)

    #     return ({"varience of mem used": str(np.var(x1)) + units + "^2", 
    #             "standard-deviation of mem used": str(np.std(x1)) + units, 
    #             "mean mem used": str(np.mean(x1)) + units,
    #             "min mem used": str(np.min(x1)) + units,
    #             "max mem used": str(np.max(x1)) + units,
    #             "range mem used": str(np.max(x1) - np.min(x1)) + units,
    #             "avg conn received": str(np.mean(x3)) + " connections",
    #             "min conn received": str(np.min(x3)) + " connections",
    #             "max conn received": str(np.max(x3)) + " connections",
    #             "avg commands executed": str(np.mean(x2)) + " commands",
    #             "min commands executed": str(np.min(x2)) + " commands",
    #             "max commands executed": str(np.max(x2)) + " commands"}, small_clients_memory_usage, command_stats_formatted, command_stats)

    def check_status_of_task_debug(self, task_key, print_all = False):
        """ Check the status of an individual task. 

            print_all :: bool : If True, this will recursively print all dependencies of all waiting tasks, not just the task specified by 'task_key'.
            
            If a value for 'max_layers' is given, then this will only print dependencies for that many layers deep."""
        if (not self.lambda_debug):
            raise RuntimeError("Lambda Debugging is not enabled. Cannot retrieve debug information.")
        
        if self.executing_tasks[task_key] == True:
            return "Task {} is currently executing.".format(task_key)
        elif self.completed_tasks[task_key] == True:
            return "Task {} finished executing.".format(task_key)
        else:
            # TODO: Iterate over dependencies, check which are done and which aren't done. Return this information to user.
            return_msg = "Task {} has not started executing yet.".format(task_key)
            ts = self.tasks[task_key]
            dependencies = ts.dependencies
            for task_state in dependencies:
                dependency_key = task_state.key
                if self.executing_tasks[dependency_key] == True:
                    return_msg = return_msg + "\t\n{} - EXECUTION IN PROGRESS".format(dependency_key)
                elif self.completed_tasks[dependency_key] == True:
                    return_msg = return_msg + "\t\n{} - COMPLETED".format(dependency_key)
                else:
                    return_msg = return_msg + "\t\n{} - WAITING".format(dependency_key)
                    if (print_all):
                        raise NotImplementedError("Haven't implemented print_all feature yet.")
            return return_msg
        
    def check_status_of_tasks(self):
        """ Checks which tasks from the last-submitted job are done and which are not done.

            For the tasks that are not done, this also returns how many of their dependencies have been resolved."""
        print("[SCHEDULER] Checking status of last job. Need to check {} tasks.".format(len(self.last_job_tasks)))
        # Get current timestamp.
        _now = datetime.datetime.utcnow()        
        
        complete = list()
        incomplete = list()
        waiting_on = dict()
        timeouts = list()

        counter = 1
        for task_node, payload in self.last_job_tasks:
            task_key = task_node.task_key 
            if self.print_debug:
                print("[INFO] Processing status for task {} ({}/{}).".format(task_key, counter, len(self.last_job_tasks)))
            
            if self.completed_tasks[task_key] == True:
                _payload = payload.copy()
                _payload["function"] = None
                _payload["args"] = None
                complete.append((task_node, _payload))
                continue
            
            #fargate_ip = task_node.fargate_node[FARGATE_PUBLIC_IP_KEY] 
            fargate_ip = task_node.fargate_node[FARGATE_PRIVATE_IP_KEY] 
            redis_client = redis.StrictRedis(host = fargate_ip, port = 6379, db = 0, socket_connect_timeout  = 5, socket_timeout = 5)
            val = 0
            try:
                val = redis_client.exists(task_key)
            except Exception as ex:
                exception_type = type(ex)
                print("{} when attempting to check if value for key {} exists at Redis instance {}:6379. ARN: {}".format(exception_type, task_key, fargate_ip, task_node.fargate_node[FARGATE_ARN_KEY]))
                timeouts.append(task_node)
                continue 
            if val == 0:
                # Remove the function/args entries as they will just be serialized nonsense.
                _payload = payload.copy()
                _payload["function"] = None
                _payload["args"] = None                    
                incomplete.append((task_node, _payload))
                dep_counter = task_key + DEPENDENCY_COUNTER_SUFFIX
                remaining = self.dcp_redis.get(dep_counter).decode()
                total = len(payload["dependencies"])
                waiting_on[task_key] = (remaining, total)
            else:
                # Remove the function/args entries as they will just be serialized nonsense.
                _payload = payload.copy()
                _payload["function"] = None
                _payload["args"] = None
                complete.append((task_node, _payload))
            counter = counter + 1
        
        print("[SCHEDULER] {}/{} of the tasks in the last workload have finished executing.".format(len(complete), len(self.last_job_tasks)))
        print("Additionally, there were {} timeouts.".format(len(timeouts)))
        return {
            "completed-tasks": complete,
            "incomplete-tasks": incomplete,
            "timeouts": timeouts,
            "dependency-counter-values": waiting_on
        }

    # def big_redis_statistics(self, n = 1000000):
    #     units = ""
    #     if n == 1:
    #         units = "bytes"
    #     elif n == 1000:
    #         units = "KB"
    #     elif n == 1000000:
    #         units = "MB"
    #     elif n == 1000000000:
    #         units = "GB"
    #     elif n == 1000000000000:
    #         units = "TB"
    #     else:
    #         raise  ValueError("Improper value for n")

    #     big_clients_memory_usage = []
    #     big_infos = []
    #     total_commands = []
    #     total_connections = []        
    #     command_stats = dict()
    #     command_stat_keys = ["cmdstat_info", "cmdstat_exists", "cmdstat_flushall", 
    #                             "cmdstat_mget", "cmdstat_get", "cmdstat_mset", "cmdstat_set", 
    #                                 "cmdstat_lpush", "cmdstat_subscribe", "cmdstat_incrby", "cmdstat_publish"]
    #     command_stats_formatted = dict()
    #     counter = 1

    #     command_stats_formatted["keys"] = "'cmdstat_info', 'cmdstat_exists', 'cmdstat_flushall', 'cmdstat_mget', 'cmdstat_get', 'cmdstat_mset', 'cmdstat_set', 'cmdstat_lpush', 'cmdstat_subscribe', 'cmdstat_incrby', 'cmdstat_publish'"

    #     for client in self.big_redis_clients:
    #         data = client.info()
    #         cmd_data = client.info("commandstats")
    #         command_stats[counter] = cmd_data
    #         big_infos.append(data)
    #         big_clients_memory_usage.append(data["used_memory"] / n)
    #         total_connections.append(data["total_connections_received"])
    #         total_commands.append(data["total_commands_processed"])  
    #         line_str = ""
    #         for key in command_stat_keys:
    #             if key in cmd_data:
    #                 line_str = line_str + str(cmd_data[key]['calls']) + ","
    #             else:
    #                 line_str = line_str + "0,"
    #         command_stats_formatted[counter] = line_str
    #         counter = counter + 1                        
        
    #     x1 = np.array(big_clients_memory_usage)
    #     x2 = np.array(total_commands)
    #     x3 = np.array(total_connections)

    #     return ({"varience of mem used": str(np.var(x1)) + units + "^2",  
    #             "standard-deviation of mem used": str(np.std(x1)) + units, 
    #             "mean mem used": str(np.mean(x1)) + units,
    #             "min mem used": str(np.min(x1)) + units,
    #             "max mem used": str(np.max(x1)) + units,
    #             "range mem used": str(np.max(x1) - np.min(x1)) + units,
    #             "avg conn received": str(np.mean(x3)) + " connections",
    #             "min conn received": str(np.min(x3)) + " connections",
    #             "max conn received": str(np.max(x3)) + " connections",
    #             "avg commands executed": str(np.mean(x2)) + " commands",
    #             "min commands executed": str(np.min(x2)) + " commands",
    #             "max commands executed": str(np.max(x2)) + " commands"}, big_clients_memory_usage, command_stats_formatted, command_stats)     

    def workload_finished(self):
        """
            The function resets data in Redis and locally that is used during a workload to track the progress of the workload and whatnot.

            The data would potentially cause issues if not reset and we were to encounter tasks with the same keys and whatnot.
        """
        mapping = dict()
        for key, b in self.seen_leaf_tasks.items():
            mapping[key] = -1 # This tells all Lambdas to stop. They'll set the value to 'zero' for us.

        if len(mapping) > 0:
            print("Setting all seen-leaf-task interation counters to -1.")
            # Reset all counters. It's possible we could use the same keys again,
            # particularly if the users are seeding the RNG or using the same input data.
            self.dcp_redis.mset(mapping)

        # Clear this.
        self.seen_leaf_tasks = dict()

    def reset_wukong_metrics(self, task_breakdowns = True, lambda_durations = True, fan_in_data = True, fan_out_data = True):
        """Clear the data stored at keys corresponding to Wukong metrics on the Redis instance."""
        if task_breakdowns:
            self.dcp_redis.delete(TASK_BREAKDOWNS)
        if lambda_durations:
            self.dcp_redis.delete(LAMBDA_DURATIONS)
        if fan_in_data:
            self.dcp_redis.delete("fan-in-data")
        if fan_out_data:
            self.dcp_redis.delete("fan-out-data")
        return True 
    
    def reset_fargate_metrics(self, full_clear = False):
        """ Clear the fargate_metrics dictionary by setting all values to default (most likely 0).

            Args:
                full_clear (bool): If True, then this will clear out the keys of the fargate_metrics dictionary entirely.
        """
        if full_clear:
            if self.print_debug:
                print("[WARNING] Scheduler is clearing fargate_metrics dictionary...")
            self.fargate_metrics.clear()
        else:
            if self.print_debug:
                print("[WARNING] Scheduler is clearing entries for fargate_metrics (not getting rid of keys, just resetting all values to default).")
            for key in self.fargate_metrics:
                # Reset this to default value of 0.
                self.fargate_metrics[key][FARGATE_NUM_SELECTED] = 0

    @gen.coroutine 
    def handle_redis_proxy_channels(self, num_channels, base_name, **msg):
        """ Handles receiving message from Proxy telling Scheduler how to construct
            list of Redis Channel Names. """
        print("[ {} ] Received message from Proxy!\nNumber of Redis Proxy Channels: {}".format(datetime.datetime.utcnow(), num_channels))
        self.redis_channel_names_for_proxy = []
        # self.proxy_comm = comm
        # Populate the list with channel names.
        for i in range(num_channels):
            self.redis_channel_names_for_proxy.append(base_name + str(i))
        self.current_redis_proxy_channel_index = 0
        self.num_redis_proxy_channels = num_channels
        # yield self.handle_stream(comm = self.proxy_comm)

    def result_from_lambda_centralized(self, comm, messages, time_received_from_scheduler, time_sent_to_scheduler, lambda_length = None, **msg):  
        """ Handle the result from executing a task on AWS Lambda. """
        self.num_messages_received_from_lambda = self.num_messages_received_from_lambda + 1
        if (lambda_length != None):
            self.sum_lambda_lengths = self.sum_lambda_lengths + lambda_length
            self.lambda_lengths.append(lambda_length)
        for message in messages:
            task_key = message["task_key"]
            self.num_results_received_from_lambda = self.num_results_received_from_lambda + 1
            if message["op"] == "lambda-result":
                timestamp_now = datetime.datetime.utcnow()
                timestamp_sent = datetime.datetime.strptime(time_sent_to_scheduler, "%Y-%m-%d %H:%M:%S.%f")
                self.task_execution_lengths[task_key] = message["execution_length"]
                self.start_end_times[task_key] = [message["start_time"], message["end_time"]]
                time_delta = timestamp_now - timestamp_sent
                self.timedeltas_from_lambda.append(time_delta)
                self.obtained_valid_result_from_lambda(task_key = task_key, **msg)   
            elif message["op"] == "task-erred":
                self.handle_task_erred_lambda(key = task_key, exception = message["exception"], traceback = message["traceback"])
            else:
                print("ERROR: Unknown operation for result from lambda: {}".format(message["op"]))

    def handle_debug_message2(self, message):
        """ Print a message received from some remote object (probably AWS Lambda) """
        timestamp_now = datetime.datetime.utcnow()
        print("[{}]    {}".format(timestamp_now, message))
        
    def obtained_valid_result_from_lambda(self, task_key, **msg):
        validate_key(task_key)
        r = self.stimulus_task_finished_lambda(key=task_key, **msg)
        self.transitions(r)        
      
    def handle_uncaught_error(self, **msg):
        logger.exception(clean_exception(**msg)[1])

    def handle_task_finished(self, key=None, worker=None, **msg):
        if worker not in self.workers:
            return
        validate_key(key)
        r = self.stimulus_task_finished(key=key, worker=worker, **msg)
        self.transitions(r)

    def handle_task_erred(self, key=None, **msg):
        r = self.stimulus_task_erred(key=key, **msg)
        self.transitions(r)
    
    def handle_task_erred_lambda(self, key=None, **msg):
        print("Task Erred on Lambda!")
        r = self.stimulus_task_erred_lambda(key = key, **msg)
        self.transitions(r)        
        
    def handle_release_data(self, key=None, worker=None, client=None, **msg):
        ts = self.tasks.get(key)
        if ts is None:
            return
        ws = self.workers[worker]
        if ts.processing_on is not ws:
            return
        r = self.stimulus_missing_data(key=key, ensure=False, **msg)
        self.transitions(r)

    def handle_missing_data(self, key=None, errant_worker=None, **kwargs):
        logger.debug("handle missing data key=%s worker=%s", key, errant_worker)
        self.log.append(("missing", key, errant_worker))

        ts = self.tasks.get(key)
        if ts is None or not ts.who_has:
            return
        if errant_worker in self.workers:
            ws = self.workers[errant_worker]
            if ws in ts.who_has:
                ts.who_has.remove(ws)
                ws.has_what.remove(ts)
                ws.nbytes -= ts.get_nbytes()
        if not ts.who_has:
            if ts.run_spec:
                self.transitions({key: "released"})
            else:
                self.transitions({key: "forgotten"})

    def release_worker_data(self, stream=None, keys=None, worker=None):
        ws = self.workers[worker]
        tasks = {self.tasks[k] for k in keys}
        removed_tasks = tasks & ws.has_what
        ws.has_what -= removed_tasks

        recommendations = {}
        for ts in removed_tasks:
            ws.nbytes -= ts.get_nbytes()
            wh = ts.who_has
            wh.remove(ws)
            if not wh:
                recommendations[ts.key] = "released"
        if recommendations:
            self.transitions(recommendations)

    def handle_long_running(self, key=None, worker=None, compute_duration=None):
        """ A task has seceded from the thread pool

        We stop the task from being stolen in the future, and change task
        duration accounting as if the task has stopped.
        """
        ts = self.tasks[key]
        if "stealing" in self.extensions:
            self.extensions["stealing"].remove_key_from_stealable(ts)

        ws = ts.processing_on
        if ws is None:
            logger.debug(
                "Received long-running signal from duplicate task. " "Ignoring."
            )
            return

        if compute_duration:
            prefix = ts.prefix
            old_duration = self.task_duration.get(prefix, 0)
            new_duration = compute_duration
            if not old_duration:
                avg_duration = new_duration
            else:
                avg_duration = 0.5 * old_duration + 0.5 * new_duration

            self.task_duration[prefix] = avg_duration

        ws.occupancy -= ws.processing[ts]
        self.total_occupancy -= ws.processing[ts]
        ws.processing[ts] = 0
        self.check_idle_saturated(ws)

    @gen.coroutine
    def handle_worker(self, comm=None, worker=None):
        """
        Listen to responses from a single worker

        This is the main loop for scheduler-worker interaction

        See Also
        --------
        Scheduler.handle_client: Equivalent coroutine for clients
        """
        comm.name = "Scheduler connection to worker"
        worker_comm = self.stream_comms[worker]
        worker_comm.start(comm)
        logger.info("Starting worker compute stream, %s", worker)
        try:
            yield self.handle_stream(comm=comm, extra={"worker": worker})
        finally:
            if worker in self.stream_comms:
                worker_comm.abort()
                self.remove_worker(address=worker)

    def add_plugin(self, plugin=None, idempotent=False, **kwargs):
        """
        Add external plugin to scheduler

        See https://distributed.readthedocs.io/en/latest/plugins.html
        """
        if isinstance(plugin, type):
            plugin = plugin(self, **kwargs)

        if idempotent and any(isinstance(p, type(plugin)) for p in self.plugins):
            return

        self.plugins.append(plugin)

    def remove_plugin(self, plugin):
        """ Remove external plugin from scheduler """
        self.plugins.remove(plugin)

    def worker_send(self, worker, msg):
        """ Send message to worker

        This also handles connection failures by adding a callback to remove
        the worker on the next cycle.
        """
        try:
            self.stream_comms[worker].send(msg)
        except (CommClosedError, AttributeError):
            self.loop.add_callback(self.remove_worker, address=worker)

    ############################
    # Less common interactions #
    ############################

    @gen.coroutine
    def get_fargate_info_for_task(self, comm = None, keys = None):
        keys = list(keys)
        result = {}
        for key in keys:
            result[key] = self.tasks_to_fargate_nodes[key]
            
        raise gen.Return(result)
    
    @gen.coroutine
    def scatter(
        self,
        comm=None,
        data=None,
        workers=None,
        client=None,
        broadcast=False,
        timeout=2,
    ):
        """ Send data out to workers

        See also
        --------
        Scheduler.broadcast:
        """
        start = time()
        while not self.workers:
            yield gen.sleep(0.2)
            if time() > start + timeout:
                raise gen.TimeoutError("No workers found")

        if workers is None:
            ncores = {w: ws.ncores for w, ws in self.workers.items()}
        else:
            workers = [self.coerce_address(w) for w in workers]
            ncores = {w: self.workers[w].ncores for w in workers}

        assert isinstance(data, dict)

        keys, who_has, nbytes = yield scatter_to_workers(
            ncores, data, rpc=self.rpc, report=False
        )

        self.update_data(who_has=who_has, nbytes=nbytes, client=client)

        if broadcast:
            if broadcast == True:  # noqa: E712
                n = len(ncores)
            else:
                n = broadcast
            yield self.replicate(keys=keys, workers=workers, n=n)

        self.log_event(
            [client, "all"], {"action": "scatter", "client": client, "count": len(data)}
        )
        raise gen.Return(keys)

    @gen.coroutine
    def gather(self, comm=None, keys=None, serializers=None):
        """ Collect data in from workers """
        # TODO - Redo this method to gather from Elasticache and not the workers themselves. 
        keys = list(keys)
        who_has = {}
        for key in keys:
            ts = self.tasks.get(key)
            if ts is not None:
                who_has[key] = [ws.address for ws in ts.who_has]
            else:
                who_has[key] = []

        data, missing_keys, missing_workers = yield gather_from_workers(
            who_has, rpc=self.rpc, close=False, serializers=serializers
        )
        if not missing_keys:
            result = {"status": "OK", "data": data}
            #print("result['data']: ", result['data'])
        else:
            missing_states = [
                (self.tasks[key].state if key in self.tasks else None)
                for key in missing_keys
            ]
            logger.debug(
                "Couldn't gather keys %s state: %s workers: %s",
                missing_keys,
                missing_states,
                missing_workers,
            )
            result = {"status": "error", "keys": missing_keys}
            with log_errors():
                for worker in missing_workers:
                    self.remove_worker(address=worker)  # this is extreme
                for key, workers in missing_keys.items():
                    if not workers:
                        continue
                    ts = self.tasks[key]
                    logger.exception(
                        "Workers don't have promised key: %s, %s",
                        str(workers),
                        str(key),
                    )
                    for worker in workers:
                        ws = self.workers.get(worker)
                        if ws is not None and ts in ws.has_what:
                            ws.has_what.remove(ts)
                            ts.who_has.remove(ws)
                            ws.nbytes -= ts.get_nbytes()
                            self.transitions({key: "released"})

        self.log_event("all", {"action": "gather", "count": len(keys)})
        raise gen.Return(result)

    def clear_task_state(self):
        # XXX what about nested state such as ClientState.wants_what
        # (see also fire-and-forget...)
        logger.info("Clear task state")
        for collection in self._task_state_collections:
            collection.clear()

    @gen.coroutine
    def restart(self, client=None, timeout=3):
        """ Restart all workers.  Reset local state. """
        with log_errors():

            n_workers = len(self.workers)

            logger.info("Send lost future signal to clients")
            for cs in self.clients.values():
                self.client_releases_keys(
                    keys=[ts.key for ts in cs.wants_what], client=cs.client_key
                )

            nannies = {addr: ws.nanny for addr, ws in self.workers.items()}

            for addr in list(self.workers):
                try:
                    # Ask the worker to close if it doesn't have a nanny,
                    # otherwise the nanny will kill it anyway
                    self.remove_worker(address=addr, close=addr not in nannies)
                except Exception as e:
                    logger.info(
                        "Exception while restarting.  This is normal", exc_info=True
                    )

            self.clear_task_state()

            for plugin in self.plugins[:]:
                try:
                    plugin.restart(self)
                except Exception as e:
                    logger.exception(e)

            logger.debug("Send kill signal to nannies: %s", nannies)

            nannies = [
                rpc(nanny_address, connection_args=self.connection_args)
                for nanny_address in nannies.values()
                if nanny_address is not None
            ]

            try:
                resps = All(
                    [
                        nanny.restart(
                            close=True, timeout=timeout * 0.8, executor_wait=False
                        )
                        for nanny in nannies
                    ]
                )
                resps = yield gen.with_timeout(timedelta(seconds=timeout), resps)
                if not all(resp == "OK" for resp in resps):
                    logger.error(
                        "Not all workers responded positively: %s", resps, exc_info=True
                    )
            except gen.TimeoutError:
                logger.error(
                    "Nannies didn't report back restarted within "
                    "timeout.  Continuuing with restart process"
                )
            finally:
                for nanny in nannies:
                    nanny.close_rpc()

            self.batched_lambda_invoker.close()

            self.start()

            self.log_event([client, "all"], {"action": "restart", "client": client})
            start = time()
            while time() < start + 10 and len(self.workers) < n_workers:
                yield gen.sleep(0.01)

            self.report({"op": "restart"})

    @gen.coroutine
    def broadcast(
        self,
        comm=None,
        msg=None,
        workers=None,
        hosts=None,
        nanny=False,
        serializers=None,
    ):
        """ Broadcast message to workers, return all results """
        if workers is None:
            if hosts is None:
                workers = list(self.workers)
            else:
                workers = []
        if hosts is not None:
            for host in hosts:
                if host in self.host_info:
                    workers.extend(self.host_info[host]["addresses"])
        # TODO replace with worker_list

        if nanny:
            addresses = [self.workers[w].nanny for w in workers]
        else:
            addresses = workers

        @gen.coroutine
        def send_message(addr):
            comm = yield connect(
                addr, deserialize=self.deserialize, connection_args=self.connection_args
            )
            comm.name = "Scheduler Broadcast"
            resp = yield send_recv(comm, close=True, serializers=serializers, **msg)
            raise gen.Return(resp)

        results = yield All(
            [send_message(address) for address in addresses if address is not None]
        )

        raise Return(dict(zip(workers, results)))

    @gen.coroutine
    def proxy(self, comm=None, msg=None, worker=None, serializers=None):
        """ Proxy a communication through the scheduler to some other worker """
        d = yield self.broadcast(
            comm=comm, msg=msg, workers=[worker], serializers=serializers
        )
        raise gen.Return(d[worker])

    @gen.coroutine
    def rebalance(self, comm=None, keys=None, workers=None):
        """ Rebalance keys so that each worker stores roughly equal bytes

        **Policy**

        This orders the workers by what fraction of bytes of the existing keys
        they have.  It walks down this list from most-to-least.  At each worker
        it sends the largest results it can find and sends them to the least
        occupied worker until either the sender or the recipient are at the
        average expected load.
        """
        with log_errors():
            if keys:
                tasks = {self.tasks[k] for k in keys}
                missing_data = [ts.key for ts in tasks if not ts.who_has]
                if missing_data:
                    raise Return({"status": "missing-data", "keys": missing_data})
            else:
                tasks = set(self.tasks.values())

            if workers:
                workers = {self.workers[w] for w in workers}
                workers_by_task = {ts: ts.who_has & workers for ts in tasks}
            else:
                workers = set(self.workers.values())
                workers_by_task = {ts: ts.who_has for ts in tasks}

            tasks_by_worker = {ws: set() for ws in workers}

            for k, v in workers_by_task.items():
                for vv in v:
                    tasks_by_worker[vv].add(k)

            worker_bytes = {
                ws: sum(ts.get_nbytes() for ts in v)
                for ws, v in tasks_by_worker.items()
            }

            avg = sum(worker_bytes.values()) / len(worker_bytes)

            sorted_workers = list(
                map(first, sorted(worker_bytes.items(), key=second, reverse=True))
            )

            recipients = iter(reversed(sorted_workers))
            recipient = next(recipients)
            msgs = []  # (sender, recipient, key)
            for sender in sorted_workers[: len(workers) // 2]:
                sender_keys = {ts: ts.get_nbytes() for ts in tasks_by_worker[sender]}
                sender_keys = iter(
                    sorted(sender_keys.items(), key=second, reverse=True)
                )

                try:
                    while worker_bytes[sender] > avg:
                        while (
                            worker_bytes[recipient] < avg and worker_bytes[sender] > avg
                        ):
                            ts, nb = next(sender_keys)
                            if ts not in tasks_by_worker[recipient]:
                                tasks_by_worker[recipient].add(ts)
                                # tasks_by_worker[sender].remove(ts)
                                msgs.append((sender, recipient, ts))
                                worker_bytes[sender] -= nb
                                worker_bytes[recipient] += nb
                        if worker_bytes[sender] > avg:
                            recipient = next(recipients)
                except StopIteration:
                    break

            to_recipients = defaultdict(lambda: defaultdict(list))
            to_senders = defaultdict(list)
            for sender, recipient, ts in msgs:
                to_recipients[recipient.address][ts.key].append(sender.address)
                to_senders[sender.address].append(ts.key)

            result = yield {
                r: self.rpc(addr=r).gather(who_has=v) for r, v in to_recipients.items()
            }
            for r, v in to_recipients.items():
                self.log_event(r, {"action": "rebalance", "who_has": v})

            self.log_event(
                "all",
                {
                    "action": "rebalance",
                    "total-keys": len(tasks),
                    "senders": valmap(len, to_senders),
                    "recipients": valmap(len, to_recipients),
                    "moved_keys": len(msgs),
                },
            )

            if not all(r["status"] == "OK" for r in result.values()):
                raise Return(
                    {
                        "status": "missing-data",
                        "keys": sum([r["keys"] for r in result if "keys" in r], []),
                    }
                )

            for sender, recipient, ts in msgs:
                assert ts.state == "memory"
                ts.who_has.add(recipient)
                recipient.has_what.add(ts)
                recipient.nbytes += ts.get_nbytes()
                self.log.append(
                    ("rebalance", ts.key, time(), sender.address, recipient.address)
                )

            result = yield {
                r: self.rpc(addr=r).delete_data(keys=v, report=False)
                for r, v in to_senders.items()
            }

            for sender, recipient, ts in msgs:
                ts.who_has.remove(sender)
                sender.has_what.remove(ts)
                sender.nbytes -= ts.get_nbytes()

            raise Return({"status": "OK"})

    @gen.coroutine
    def replicate(
        self,
        comm=None,
        keys=None,
        n=None,
        workers=None,
        branching_factor=2,
        delete=True,
    ):
        """ Replicate data throughout cluster

        This performs a tree copy of the data throughout the network
        individually on each piece of data.

        Parameters
        ----------
        keys: Iterable
            list of keys to replicate
        n: int
            Number of replications we expect to see within the cluster
        branching_factor: int, optional
            The number of workers that can copy data in each generation.
            The larger the branching factor, the more data we copy in
            a single step, but the more a given worker risks being
            swamped by data requests.

        See also
        --------
        Scheduler.rebalance
        """
        assert branching_factor > 0

        workers = {self.workers[w] for w in self.workers_list(workers)}
        if n is None:
            n = len(workers)
        else:
            n = min(n, len(workers))
        if n == 0:
            raise ValueError("Can not use replicate to delete data")

        tasks = {self.tasks[k] for k in keys}
        missing_data = [ts.key for ts in tasks if not ts.who_has]
        if missing_data:
            raise Return({"status": "missing-data", "keys": missing_data})

        # Delete extraneous data
        if delete:
            del_worker_tasks = defaultdict(set)
            for ts in tasks:
                del_candidates = ts.who_has & workers
                if len(del_candidates) > n:
                    for ws in random.sample(del_candidates, len(del_candidates) - n):
                        del_worker_tasks[ws].add(ts)

            yield [
                self.rpc(addr=ws.address).delete_data(
                    keys=[ts.key for ts in tasks], report=False
                )
                for ws, tasks in del_worker_tasks.items()
            ]

            for ws, tasks in del_worker_tasks.items():
                ws.has_what -= tasks
                for ts in tasks:
                    ts.who_has.remove(ws)
                    ws.nbytes -= ts.get_nbytes()
                self.log_event(
                    ws.address,
                    {"action": "replicate-remove", "keys": [ts.key for ts in tasks]},
                )

        # Copy not-yet-filled data
        while tasks:
            gathers = defaultdict(dict)
            for ts in list(tasks):
                n_missing = n - len(ts.who_has & workers)
                if n_missing <= 0:
                    # Already replicated enough
                    tasks.remove(ts)
                    continue

                count = min(n_missing, branching_factor * len(ts.who_has))
                assert count > 0

                for ws in random.sample(workers - ts.who_has, count):
                    gathers[ws.address][ts.key] = [wws.address for wws in ts.who_has]

            results = yield {
                w: self.rpc(addr=w).gather(who_has=who_has)
                for w, who_has in gathers.items()
            }
            for w, v in results.items():
                if v["status"] == "OK":
                    self.add_keys(worker=w, keys=list(gathers[w]))
                else:
                    logger.warning("Communication failed during replication: %s", v)

                self.log_event(w, {"action": "replicate-add", "keys": gathers[w]})

        self.log_event(
            "all",
            {
                "action": "replicate",
                "workers": list(workers),
                "key-count": len(keys),
                "branching-factor": branching_factor,
            },
        )

    def workers_to_close(self, memory_ratio=None, n=None, key=None, minimum=None):
        """
        Find workers that we can close with low cost

        This returns a list of workers that are good candidates to retire.
        These workers are not running anything and are storing
        relatively little data relative to their peers.  If all workers are
        idle then we still maintain enough workers to have enough RAM to store
        our data, with a comfortable buffer.

        This is for use with systems like ``distributed.deploy.adaptive``.

        Parameters
        ----------
        memory_factor: Number
            Amount of extra space we want to have for our stored data.
            Defaults two 2, or that we want to have twice as much memory as we
            currently have data.
        n: int
            Number of workers to close
        minimum: int
            Minimum number of workers to keep around
        key: Callable(WorkerState)
            An optional callable mapping a WorkerState object to a group
            affiliation.  Groups will be closed together.  This is useful when
            closing workers must be done collectively, such as by hostname.

        Examples
        --------
        >>> scheduler.workers_to_close()
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.2:1234']

        Group workers by hostname prior to closing

        >>> scheduler.workers_to_close(key=lambda ws: ws.host)
        ['tcp://192.168.0.1:1234', 'tcp://192.168.0.1:4567']

        Remove two workers

        >>> scheduler.workers_to_close(n=2)

        Keep enough workers to have twice as much memory as we we need.

        >>> scheduler.workers_to_close(memory_ratio=2)

        Returns
        -------
        to_close: list of worker addresses that are OK to close

        See Also
        --------
        Scheduler.retire_workers
        """
        if n is None and memory_ratio is None:
            memory_ratio = 2

        with log_errors():
            if not n and all(ws.processing for ws in self.workers.values()):
                return []

            if key is None:
                key = lambda ws: ws.address

            groups = groupby(key, self.workers.values())

            limit_bytes = {
                k: sum(ws.memory_limit for ws in v) for k, v in groups.items()
            }
            group_bytes = {k: sum(ws.nbytes for ws in v) for k, v in groups.items()}

            limit = sum(limit_bytes.values())
            total = sum(group_bytes.values())

            def key(group):
                is_idle = not any(ws.processing for ws in groups[group])
                bytes = -group_bytes[group]
                return (is_idle, bytes)

            idle = sorted(groups, key=key)

            to_close = []
            n_remain = len(self.workers)

            while idle:
                group = idle.pop()
                if n is None and any(ws.processing for ws in groups[group]):
                    break

                if minimum and n_remain - len(groups[group]) < minimum:
                    break

                limit -= limit_bytes[group]

                if (n is not None and len(to_close) < n) or (
                    memory_ratio is not None and limit >= memory_ratio * total
                ):
                    to_close.append(group)
                    n_remain -= len(groups[group])

                else:
                    break

            result = [ws.address for g in to_close for ws in groups[g]]
            if result:
                logger.info("Suggest closing workers: %s", result)

            return result

    @gen.coroutine
    def retire_workers(
        self, comm=None, workers=None, remove=True, close_workers=False, **kwargs
    ):
        """ Gracefully retire workers from cluster

        Parameters
        ----------
        workers: list (optional)
            List of worker IDs to retire.
            If not provided we call ``workers_to_close`` which finds a good set
        remove: bool (defaults to True)
            Whether or not to remove the worker metadata immediately or else
            wait for the worker to contact us
        close_workers: bool (defaults to False)
            Whether or not to actually close the worker explicitly from here.
            Otherwise we expect some external job scheduler to finish off the
            worker.
        **kwargs: dict
            Extra options to pass to workers_to_close to determine which
            workers we should drop

        Returns
        -------
        Dictionary mapping worker ID/address to dictionary of information about
        that worker for each retired worker.

        See Also
        --------
        Scheduler.workers_to_close
        """
        with log_errors():
            if workers is None:
                while True:
                    try:
                        workers = self.workers_to_close(**kwargs)
                        if workers:
                            workers = yield self.retire_workers(
                                workers=workers,
                                remove=remove,
                                close_workers=close_workers,
                            )
                        raise gen.Return(workers)
                    except KeyError:  # keys left during replicate
                        pass

            workers = {self.workers[w] for w in workers if w in self.workers}
            if len(workers) > 0:
                # Keys orphaned by retiring those workers
                keys = set.union(*[w.has_what for w in workers])
                keys = {ts.key for ts in keys if ts.who_has.issubset(workers)}
            else:
                keys = set()

            other_workers = set(self.workers.values()) - workers
            if keys:
                if other_workers:
                    yield self.replicate(
                        keys=keys,
                        workers=[ws.address for ws in other_workers],
                        n=1,
                        delete=False,
                    )
                else:
                    raise gen.Return([])

            worker_keys = {ws.address: ws.identity() for ws in workers}
            if close_workers and worker_keys:
                yield [self.close_worker(worker=w, safe=True) for w in worker_keys]
            if remove:
                for w in worker_keys:
                    self.remove_worker(address=w, safe=True)

            self.log_event(
                "all",
                {
                    "action": "retire-workers",
                    "workers": worker_keys,
                    "moved-keys": len(keys),
                },
            )
            self.log_event(list(worker_keys), {"action": "retired"})

            raise gen.Return(worker_keys)

    def add_keys(self, comm=None, worker=None, keys=()):
        """
        Learn that a worker has certain keys

        This should not be used in practice and is mostly here for legacy
        reasons.  However, it is sent by workers from time to time.
        """
        if worker not in self.workers:
            return "not found"
        ws = self.workers[worker]
        for key in keys:
            ts = self.tasks.get(key)
            if ts is not None and ts.state == "memory":
                if ts not in ws.has_what:
                    ws.nbytes += ts.get_nbytes()
                    ws.has_what.add(ts)
                    ts.who_has.add(ws)
            else:
                self.worker_send(
                    worker, {"op": "delete-data", "keys": [key], "report": False}
                )

        return "OK"

    def update_data(
        self, comm=None, who_has=None, nbytes=None, client=None, serializers=None
    ):
        """
        Learn that new data has entered the network from an external source

        See Also
        --------
        Scheduler.mark_key_in_memory
        """
        with log_errors():
            who_has = {
                k: [self.coerce_address(vv) for vv in v] for k, v in who_has.items()
            }
            logger.debug("Update data %s", who_has)

            for key, workers in who_has.items():
                ts = self.tasks.get(key)
                if ts is None:
                    ts = self.tasks[key] = TaskState(key, None)
                ts.state = "memory"
                if key in nbytes:
                    ts.set_nbytes(nbytes[key])
                for w in workers:
                    ws = self.workers[w]
                    if ts not in ws.has_what:
                        ws.nbytes += ts.get_nbytes()
                        ws.has_what.add(ts)
                        ts.who_has.add(ws)
                self.report(
                    {"op": "key-in-memory", "key": key, "workers": list(workers)}
                )

            if client:
                self.client_desires_keys(keys=list(who_has), client=client)

    def report_on_key(self, key=None, ts=None, client=None):
        assert (key is None) + (ts is None) == 1, (key, ts)
        if ts is None:
            try:
                ts = self.tasks[key]
            except KeyError:
                self.report({"op": "cancelled-key", "key": key}, client=client)
                return
        else:
            key = ts.key
        if ts.state == "forgotten":
            self.report({"op": "cancelled-key", "key": key}, ts=ts, client=client)
        elif ts.state == "memory":
            self.report({"op": "key-in-memory", "key": key}, ts=ts, client=client)
        elif ts.state == "erred":
            failing_ts = ts.exception_blame
            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                },
                ts=ts,
                client=client,
            )

    @gen.coroutine
    def feed(
        self, comm, function=None, setup=None, teardown=None, interval="1s", **kwargs
    ):
        """
        Provides a data Comm to external requester

        Caution: this runs arbitrary Python code on the scheduler.  This should
        eventually be phased out.  It is mostly used by diagnostics.
        """
        import pickle

        interval = parse_timedelta(interval)
        with log_errors():
            if function:
                function = pickle.loads(function)
            if setup:
                setup = pickle.loads(setup)
            if teardown:
                teardown = pickle.loads(teardown)
            state = setup(self) if setup else None
            if isinstance(state, gen.Future):
                state = yield state
            try:
                while self.status == "running":
                    if state is None:
                        response = function(self)
                    else:
                        response = function(self, state)
                    yield comm.write(response)
                    yield gen.sleep(interval)
            except (EnvironmentError, CommClosedError):
                pass
            finally:
                if teardown:
                    teardown(self, state)

    def get_processing(self, comm=None, workers=None):
        if workers is not None:
            workers = set(map(self.coerce_address, workers))
            return {w: [ts.key for ts in self.workers[w].processing] for w in workers}
        else:
            return {
                w: [ts.key for ts in ws.processing] for w, ws in self.workers.items()
            }

    def get_who_has(self, comm=None, keys=None):
        if keys is not None:
            return {
                k: [ws.address for ws in self.tasks[k].who_has]
                if k in self.tasks
                else []
                for k in keys
            }
        else:
            return {
                key: [ws.address for ws in ts.who_has] for key, ts in self.tasks.items()
            }

    def get_has_what(self, comm=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {
                w: [ts.key for ts in self.workers[w].has_what]
                if w in self.workers
                else []
                for w in workers
            }
        else:
            return {w: [ts.key for ts in ws.has_what] for w, ws in self.workers.items()}

    def get_ncores(self, comm=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {w: self.workers[w].ncores for w in workers if w in self.workers}
        else:
            return {w: ws.ncores for w, ws in self.workers.items()}

    @gen.coroutine
    def get_call_stack(self, comm=None, keys=None):
        if keys is not None:
            stack = list(keys)
            processing = set()
            while stack:
                key = stack.pop()
                ts = self.tasks[key]
                if ts.state == "waiting":
                    stack.extend(dts.key for dts in ts.dependencies)
                elif ts.state == "processing":
                    processing.add(ts)

            workers = defaultdict(list)
            for ts in processing:
                if ts.processing_on:
                    workers[ts.processing_on.address].append(ts.key)
        else:
            workers = {w: None for w in self.workers}

        if not workers:
            raise gen.Return({})

        else:
            response = yield {
                w: self.rpc(w).call_stack(keys=v) for w, v in workers.items()
            }
            response = {k: v for k, v in response.items() if v}
            raise gen.Return(response)

    def get_nbytes(self, comm=None, keys=None, summary=True):
        with log_errors():
            if keys is not None:
                result = {k: self.tasks[k].nbytes for k in keys}
            else:
                result = {
                    k: ts.nbytes
                    for k, ts in self.tasks.items()
                    if ts.nbytes is not None
                }

            if summary:
                out = defaultdict(lambda: 0)
                for k, v in result.items():
                    out[key_split(k)] += v
                result = dict(out)

            return result

    def get_comm_cost(self, ts, ws):
        """
        Get the estimated communication cost (in s.) to compute the task
        on the given worker.
        """
        return sum(dts.nbytes for dts in ts.dependencies - ws.has_what) / self.bandwidth

    def get_task_duration(self, ts, default=0.5):
        """
        Get the estimated computation cost of the given task
        (not including any communication cost).
        """
        prefix = ts.prefix
        try:
            return self.task_duration[prefix]
        except KeyError:
            self.unknown_durations[prefix].add(ts)
            return default

    def run_function(self, stream, function, args=(), kwargs={}, wait=True):
        """ Run a function within this process

        See Also
        --------
        Client.run_on_scheduler:
        """
        from .worker import run

        self.log_event("all", {"action": "run-function", "function": function})
        return run(self, stream, function=function, args=args, kwargs=kwargs, wait=wait)

    def set_metadata(self, stream=None, keys=None, value=None):
        try:
            metadata = self.task_metadata
            for key in keys[:-1]:
                if key not in metadata or not isinstance(metadata[key], (dict, list)):
                    metadata[key] = dict()
                metadata = metadata[key]
            metadata[keys[-1]] = value
        except Exception as e:
            import pdb

            pdb.set_trace()

    def get_metadata(self, stream=None, keys=None, default=no_default):
        metadata = self.task_metadata
        for key in keys[:-1]:
            metadata = metadata[key]
        try:
            return metadata[keys[-1]]
        except KeyError:
            if default != no_default:
                return default
            else:
                raise

    def get_task_status(self, stream=None, keys=None):
        return {
            key: (self.tasks[key].state if key in self.tasks else None) for key in keys
        }

    def get_task_stream(self, comm=None, start=None, stop=None, count=None):
        from distributed.diagnostics.task_stream import TaskStreamPlugin

        self.add_plugin(TaskStreamPlugin, idempotent=True)
        ts = [p for p in self.plugins if isinstance(p, TaskStreamPlugin)][0]
        return ts.collect(start=start, stop=stop, count=count)

    @gen.coroutine
    def register_worker_plugin(self, comm, plugin, name=None):
        """ Registers a setup function, and call it on every worker """
        self.worker_plugins.append(plugin)

        responses = yield self.broadcast(
            msg=dict(op="plugin-add", plugin=plugin, name=name)
        )
        raise gen.Return(responses)

    #####################
    # State Transitions #
    #####################
    #
    # Classic Dask
    #
    def _remove_from_processing(self, ts, send_worker_msg=None):
        """
        Remove *ts* from the set of processing tasks.
        """
        ws = ts.processing_on
        ts.processing_on = None
        w = ws.address
        if w in self.workers:  # may have been removed
            duration = ws.processing.pop(ts)
            if not ws.processing:
                self.total_occupancy -= ws.occupancy
                ws.occupancy = 0
            else:
                self.total_occupancy -= duration
                ws.occupancy -= duration
            self.check_idle_saturated(ws)
            self.release_resources(ts, ws)
            if send_worker_msg:
                self.worker_send(w, send_worker_msg)

    def _add_to_memory(self, ts, ws, recommendations, type=None, typename=None, **kwargs):
        """
        Add *ts* to the set of in-memory tasks.
        """
        if self.validate:
            assert ts not in ws.has_what

        ts.who_has.add(ws)
        ws.has_what.add(ts)
        ws.nbytes += ts.get_nbytes()

        deps = ts.dependents
        if len(deps) > 1:
            deps = sorted(deps, key=operator.attrgetter("priority"), reverse=True)
        for dts in deps:
            s = dts.waiting_on
            if ts in s:
                s.discard(ts)
                if not s:  # new task ready to run
                    recommendations[dts.key] = "processing"

        for dts in ts.dependencies:
            s = dts.waiters
            s.discard(ts)
            if not s and not dts.who_wants:
                recommendations[dts.key] = "released"

        if not ts.waiters and not ts.who_wants:
            recommendations[ts.key] = "released"
        else:
            msg = {"op": "key-in-memory", "key": ts.key}
            if type is not None:
                msg["type"] = type
            self.report(msg)

        ts.state = "memory"
        ts.type = typename

        cs = self.clients["fire-and-forget"]
        if ts in cs.wants_what:
            self.client_releases_keys(client="fire-and-forget", keys=[ts.key])

    def _add_to_memory_lambda(self, ts, recommendations, type=None, typename=None, **kwargs):
        """
        Add *ts* to the set of in-memory tasks.
        """
        #if self.validate:
        #    assert ts not in ws.has_what

        #ts.who_has.add(ws)
        #ws.has_what.add(ts)
        #ws.nbytes += ts.get_nbytes()
        #print("Adding task {} to memory.".format(ts.key))
        #deps = ts.dependents
        #if len(deps) > 1:
        #    deps = sorted(deps, key=operator.attrgetter("priority"), reverse=True)
        #for dts in deps:
        #    s = dts.waiting_on
        #    if ts in s:
        #        s.discard(ts)
        #        if not s:  # new task ready to run
        #            recommendations[dts.key] = "processing"

        #for dts in ts.dependencies:
        #    s = dts.waiters
        #    s.discard(ts)
        #    if not s and not dts.who_wants:
        #        recommendations[dts.key] = "released"

        #if not ts.waiters and not ts.who_wants:
        #    print("Recommending released for task {}".format(ts.key))
        #    recommendations[ts.key] = "released"
        recommendations[ts.key] = "memory"
        #else:
        msg = {"op": "key-in-memory", "key": ts.key}
        if type is not None:
            msg["type"] = type
        self.report(msg)

        ts.state = "memory"
        ts.type = typename

        cs = self.clients["fire-and-forget"]
        if ts in cs.wants_what:
            self.client_releases_keys(client="fire-and-forget", keys=[ts.key])
            
    def transition_released_waiting(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.run_spec
                assert not ts.waiting_on
                assert not ts.who_has
                assert not ts.processing_on
                assert not any(dts.state == "forgotten" for dts in ts.dependencies)

            if ts.has_lost_dependencies:
                return {key: "forgotten"}

            ts.state = "waiting"

            recommendations = OrderedDict()

            for dts in ts.dependencies:
                if dts.exception_blame:
                    ts.exception_blame = dts.exception_blame
                    recommendations[key] = "erred"
                    return recommendations

            for dts in ts.dependencies:
                dep = dts.key
                if not dts.who_has:
                    ts.waiting_on.add(dts)
                if dts.state == "released":
                    recommendations[dep] = "waiting"
                else:
                    dts.waiters.add(ts)

            ts.waiters = {dts for dts in ts.dependents if dts.state == "waiting"}

            if not ts.waiting_on:
                if self.workers:
                    recommendations[key] = "processing"
                else:
                    self.unrunnable.add(ts)
                    ts.state = "no-worker"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_no_worker_waiting(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts in self.unrunnable
                assert not ts.waiting_on
                assert not ts.who_has
                assert not ts.processing_on

            self.unrunnable.remove(ts)

            if ts.has_lost_dependencies:
                return {key: "forgotten"}

            recommendations = OrderedDict()

            for dts in ts.dependencies:
                dep = dts.key
                if not dts.who_has:
                    ts.waiting_on.add(dep)
                if dts.state == "released":
                    recommendations[dep] = "waiting"
                else:
                    dts.waiters.add(ts)

            ts.state = "waiting"

            if not ts.waiting_on:
                if self.workers:
                    recommendations[key] = "processing"
                else:
                    self.unrunnable.add(ts)
                    ts.state = "no-worker"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def decide_worker(self, ts):
        """
        Decide on a worker for task *ts*.  Return a WorkerState.
        """
        valid_workers = self.valid_workers(ts)

        if not valid_workers and not ts.loose_restrictions and self.workers:
            self.unrunnable.add(ts)
            ts.state = "no-worker"
            return None

        if ts.dependencies or valid_workers is not True:
            worker = decide_worker(
                ts,
                self.workers.values(),
                valid_workers,
                partial(self.worker_objective, ts),
            )
        elif self.idle:
            if len(self.idle) < 20:  # smart but linear in small case
                worker = min(self.idle, key=operator.attrgetter("occupancy"))
            else:  # dumb but fast in large case
                worker = self.idle[self.n_tasks % len(self.idle)]
        else:
            if len(self.workers) < 20:  # smart but linear in small case
                worker = min(
                    self.workers.values(), key=operator.attrgetter("occupancy")
                )
            else:  # dumb but fast in large case
                worker = self.workers.values()[self.n_tasks % len(self.workers)]

        if self.validate:
            assert worker is None or isinstance(worker, WorkerState), (
                type(worker),
                worker,
            )
            assert worker.address in self.workers

        return worker

    def transition_waiting_processing(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.waiting_on
                assert not ts.who_has
                assert not ts.exception_blame
                assert not ts.processing_on
                assert not ts.has_lost_dependencies
                assert ts not in self.unrunnable
                assert all(dts.who_has for dts in ts.dependencies)

            ws = self.decide_worker(ts)
            if ws is None:
                return {}
            worker = ws.address

            duration = self.get_task_duration(ts)
            comm = self.get_comm_cost(ts, ws)

            ws.processing[ts] = duration + comm
            ts.processing_on = ws
            ws.occupancy += duration + comm
            self.total_occupancy += duration + comm
            ts.state = "processing"
            self.consume_resources(ts, ws)
            self.check_idle_saturated(ws)
            self.n_tasks += 1

            if ts.actor:
                ws.actors.add(ts)

            # logger.debug("Send job to worker: %s, %s", worker, key)

            # Do both for now... eventually workers won't be involved.
            # self.send_task_to_worker(worker, key)
            #self.submit_task_to_lambda(key)
            
            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_waiting_memory(self, key, nbytes=None, worker=None, **kwargs):
        try:
            ws = self.workers[worker]
            ts = self.tasks[key]

            if self.validate:
                assert not ts.processing_on
                assert ts.waiting_on
                assert ts.state == "waiting"

            ts.waiting_on.clear()

            if nbytes is not None:
                ts.set_nbytes(nbytes)

            self.check_idle_saturated(ws)

            recommendations = OrderedDict()

            self._add_to_memory(ts, ws, recommendations, **kwargs)

            if self.validate:
                assert not ts.processing_on
                assert not ts.waiting_on
                assert ts.who_has

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_processing_memory(
        self,
        key,
        nbytes=None,
        type=None,
        typename=None,
        worker=None,
        startstops=None,
        **kwargs):
        try:
            ts = self.tasks[key]
            assert worker
            assert isinstance(worker, (str, unicode))

            if self.validate:
                assert ts.processing_on
                ws = ts.processing_on
                assert ts in ws.processing
                assert not ts.waiting_on
                assert not ts.who_has, (ts, ts.who_has)
                assert not ts.exception_blame
                assert ts.state == "processing"

            ws = self.workers.get(worker)
            if ws is None:
                return {key: "released"}

            if ws is not ts.processing_on:  # someone else has this task
                logger.info(
                    "Unexpected worker completed task, likely due to"
                    " work stealing.  Expected: %s, Got: %s, Key: %s",
                    ts.processing_on,
                    ws,
                    key,
                )
                return {}

            if startstops:
                L = [(b, c) for a, b, c in startstops if a == "compute"]
                if L:
                    compute_start, compute_stop = L[0]
                else:  # This is very rare
                    compute_start = compute_stop = None
            else:
                compute_start = compute_stop = None

            #############################
            # Update Timing Information #
            #############################
            if compute_start and ws.processing.get(ts, True):
                # Update average task duration for worker
                prefix = ts.prefix
                old_duration = self.task_duration.get(prefix, 0)
                new_duration = compute_stop - compute_start
                if not old_duration:
                    avg_duration = new_duration
                else:
                    avg_duration = 0.5 * old_duration + 0.5 * new_duration

                self.task_duration[prefix] = avg_duration

                for tts in self.unknown_durations.pop(prefix, ()):
                    if tts.processing_on:
                        wws = tts.processing_on
                        old = wws.processing[tts]
                        comm = self.get_comm_cost(tts, wws)
                        wws.processing[tts] = avg_duration + comm
                        wws.occupancy += avg_duration + comm - old
                        self.total_occupancy += avg_duration + comm - old

            ############################
            # Update State Information #
            ############################
            if nbytes is not None:
                ts.set_nbytes(nbytes)

            recommendations = OrderedDict()

            self._remove_from_processing(ts)

            self._add_to_memory(ts, ws, recommendations, type=type, typename=typename)

            if self.validate:
                assert not ts.processing_on
                assert not ts.waiting_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_memory_released(self, key, safe=False):
        print("\n\n\n\n{}\n\n\n\n".format("transition_memory_released"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.waiting_on
                assert not ts.processing_on
                if safe:
                    assert not ts.waiters

            if ts.actor:
                for ws in ts.who_has:
                    ws.actors.discard(ts)
                if ts.who_wants:
                    ts.exception_blame = ts
                    ts.exception = "Worker holding Actor was lost"
                    return {ts.key: "erred"}  # don't try to recreate

            recommendations = OrderedDict()

            for dts in ts.waiters:
                if dts.state in ("no-worker", "processing"):
                    recommendations[dts.key] = "waiting"
                elif dts.state == "waiting":
                    dts.waiting_on.add(ts)

            # XXX factor this out?
            for ws in ts.who_has:
                ws.has_what.remove(ts)
                ws.nbytes -= ts.get_nbytes()
                self.worker_send(
                    ws.address, {"op": "delete-data", "keys": [key], "report": False}
                )
            ts.who_has.clear()

            ts.state = "released"

            self.report({"op": "lost-data", "key": key})

            if not ts.run_spec:  # pure data
                recommendations[key] = "forgotten"
            elif ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif ts.who_wants or ts.waiters:
                recommendations[key] = "waiting"

            if self.validate:
                assert not ts.waiting_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_released_erred(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert ts.exception_blame
                    assert not ts.who_has
                    assert not ts.waiting_on
                    assert not ts.waiters

            recommendations = {}

            failing_ts = ts.exception_blame

            for dts in ts.dependents:
                dts.exception_blame = failing_ts
                if not dts.who_has:
                    recommendations[dts.key] = "erred"

            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                }
            )

            ts.state = "erred"

            # TODO: waiting data?
            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_erred_released(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_erred_released"))
        try:
            ts = self.tasks[key]

            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert all(dts.state != "erred" for dts in ts.dependencies)
                    assert ts.exception_blame
                    assert not ts.who_has
                    assert not ts.waiting_on
                    assert not ts.waiters

            recommendations = OrderedDict()

            ts.exception = None
            ts.exception_blame = None
            ts.traceback = None

            for dep in ts.dependents:
                if dep.state == "erred":
                    recommendations[dep.key] = "waiting"

            self.report({"op": "task-retried", "key": key})
            ts.state = "released"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_waiting_released(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_waiting_released"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.who_has
                assert not ts.processing_on

            recommendations = {}

            for dts in ts.dependencies:
                s = dts.waiters
                if ts in s:
                    s.discard(ts)
                    if not s and not dts.who_wants:
                        recommendations[dts.key] = "released"
            ts.waiting_on.clear()

            ts.state = "released"

            if ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif not ts.exception_blame and (ts.who_wants or ts.waiters):
                recommendations[key] = "waiting"
            else:
                ts.waiters.clear()

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_processing_released(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_processing_released"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.processing_on
                assert not ts.who_has
                assert not ts.waiting_on
                assert self.tasks[key].state == "processing"

            self._remove_from_processing(
                ts, send_worker_msg={"op": "release-task", "key": key}
            )

            ts.state = "released"

            recommendations = OrderedDict()

            if ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif ts.waiters or ts.who_wants:
                recommendations[key] = "waiting"

            if recommendations.get(key) != "waiting":
                for dts in ts.dependencies:
                    if dts.state != "released":
                        s = dts.waiters
                        s.discard(ts)
                        if not s and not dts.who_wants:
                            recommendations[dts.key] = "released"
                ts.waiters.clear()

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_processing_erred(self, key, cause=None, exception=None, traceback=None, **kwargs):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert cause or ts.exception_blame
                assert ts.processing_on
                assert not ts.who_has
                assert not ts.waiting_on

            if ts.actor:
                ws = ts.processing_on
                ws.actors.remove(ts)

            self._remove_from_processing(ts)

            if exception is not None:
                ts.exception = exception
            if traceback is not None:
                ts.traceback = traceback
            if cause is not None:
                failing_ts = self.tasks[cause]
                ts.exception_blame = failing_ts
            else:
                failing_ts = ts.exception_blame

            recommendations = {}

            for dts in ts.dependents:
                dts.exception_blame = failing_ts
                recommendations[dts.key] = "erred"

            for dts in ts.dependencies:
                s = dts.waiters
                s.discard(ts)
                if not s and not dts.who_wants:
                    recommendations[dts.key] = "released"

            ts.waiters.clear()  # do anything with this?

            ts.state = "erred"

            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                }
            )

            cs = self.clients["fire-and-forget"]
            if ts in cs.wants_what:
                self.client_releases_keys(client="fire-and-forget", keys=[key])

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_no_worker_released(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_no_worker_released"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert self.tasks[key].state == "no-worker"
                assert not ts.who_has
                assert not ts.waiting_on

            self.unrunnable.remove(ts)
            ts.state = "released"

            for dts in ts.dependencies:
                dts.waiters.discard(ts)

            ts.waiters.clear()

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    #
    # Serverless Dask
    #
    def transition_released_waiting_lambda(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_released_waiting_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.run_spec
                assert not ts.waiting_on
                #assert not ts.who_has
                #assert not ts.processing_on
                assert not any(dts.state == "forgotten" for dts in ts.dependencies)

            if ts.has_lost_dependencies:
                return {key: "forgotten"}

            ts.state = "waiting"

            recommendations = OrderedDict()

            for dts in ts.dependencies:
                if dts.exception_blame:
                    ts.exception_blame = dts.exception_blame
                    recommendations[key] = "erred"
                    return recommendations

            for dts in ts.dependencies:
                dep = dts.key
                #if not dts.who_has:
                    #ts.waiting_on.add(dts)
                # If the key is not in-memory, then the result doesn't exist so this task is waiting on it.
                #if memcache_client.get(dts.key.replace(" ", "_")) == None:
                #    ts.waiting_on.add(dts)
                #if self.get_redis_client(dts.key).exists(dts.key) == 0:
                #    ts.waiting_on.add(dts)
                # Check both the big and small hash rings.
                #if self.big_hash_ring[dts.key].exists(dts.key) == 0 and self.small_hash_ring[dts.key].exists(dts.key) == 0:
                if self.dcp_redis.exists(dts.key) == 0:
                    ts.waiting_on.add(dts)
                if dts.state == "released":
                    recommendations[dep] = "waiting"
                else:
                    dts.waiters.add(ts)

            ts.waiters = {dts for dts in ts.dependents if dts.state == "waiting"}

            if not ts.waiting_on:
                # Since we're using Lambda, we can run this task as soon as it is ready.
                recommendations[key] = "processing"
                # if self.workers:
                    # recommendations[key] = "processing"
                # else:
                    # self.unrunnable.add(ts)
                    # ts.state = "no-worker"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise    
    
    def transition_no_worker_waiting_lambda(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_no_worker_waiting_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts in self.unrunnable
                assert not ts.waiting_on
                #assert not ts.who_has
                #assert not ts.processing_on

            self.unrunnable.remove(ts)

            if ts.has_lost_dependencies:
                return {key: "forgotten"}

            recommendations = OrderedDict()

            for dts in ts.dependencies:
                dep = dts.key
                # If the key is not in-memory, then the result doesn't exist so this task is waiting on it.
                # if memcache_client.get(dts.key.replace(" ", "_")) == None:
                    # ts.waiting_on.add(dts)        
                #if self.get_redis_client(dts.key).exists(dts.key) == 0:
                #    ts.waiting_on.add(dts)     
                #if self.big_hash_ring[dts.key].exists(dts.key) == 0 and self.small_hash_ring[dts.key].exists(dts.key) == 0:
                if self.dcp_redis.exists(dts.key) == 0:
                    ts.waiting_on.add(dts)
                # if not dts.who_has:
                    # ts.waiting_on.add(dep)
                if dts.state == "released":
                    recommendations[dep] = "waiting"
                else:
                    dts.waiters.add(ts)

            ts.state = "waiting"

            if not ts.waiting_on:
                # Since we're using Lambda, we can run this task as soon as it is ready.
                recommendations[key] = "processing"                
                # if self.workers:
                    # recommendations[key] = "processing"
                # else:
                    # self.unrunnable.add(ts)
                    # ts.state = "no-worker"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise    
    
    def transition_waiting_processing_lambda(self, key):
        #print("\n\n\n\n{}\n\n\n\n".format("transition_waiting_processing_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.waiting_on
                #assert not ts.who_has
                assert not ts.exception_blame
                #assert not ts.processing_on
                assert not ts.has_lost_dependencies
                assert ts not in self.unrunnable
                # Ensure that all of the dependencies are in the Elasticache cluster. 
                #assert all (memcache_client.get(dts.key.replace(" ", "_")) for dts in ts.dependencies)
                # assert all (redis_client.get(dts.key) for dts in ts.dependencies)
                #assert all(dts.who_has for dts in ts.dependencies)
                
            ts.state = "processing"
            self.n_tasks += 1

            # self.submit_task_to_lambda(key)
            
            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def transition_waiting_memory_lambda(self, key, nbytes=None, **kwargs):
        print("\n\n\n\n{}\n\n\n\n".format("transition_waiting_memory_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.waiting_on
                assert not ts.exception_blame
                assert ts.state == "processing"

            #############################
            # Update Timing Information #
            #############################
            #if compute_start and ws.processing.get(ts, True):
            #    # Update average task duration for worker
            #    prefix = ts.prefix
            #    old_duration = self.task_duration.get(prefix, 0)
            #    new_duration = compute_stop - compute_start
            #    if not old_duration:
            #        avg_duration = new_duration
            #    else:
            #        avg_duration = 0.5 * old_duration + 0.5 * new_duration
            
            #    self.task_duration[prefix] = avg_duration
            
                # for tts in self.unknown_durations.pop(prefix, ()):
                    # if tts.processing_on:
                        # wws = tts.processing_on
                        # old = wws.processing[tts]
                        # comm = self.get_comm_cost(tts, wws)
                        # wws.processing[tts] = avg_duration + comm
                        # wws.occupancy += avg_duration + comm - old
                        # self.total_occupancy += avg_duration + comm - old

            ############################
            # Update State Information #
            ############################
            if nbytes is not None:
                ts.set_nbytes(nbytes)

            recommendations = OrderedDict()

            self._add_to_memory_lambda(ts, recommendations, type=type, typename=typename)
            
            if self.validate:
                #assert not ts.processing_on
                assert not ts.waiting_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
      
    def transition_processing_memory_lambda(
        self,
        key,
        nbytes=None,
        type=None,
        typename=None,
        startstops=None,
        **kwargs):
        #print("\n\n\n\n{}\n\n\n\n".format("transition_processing_memory_lambda"))
        try:
            ts = self.tasks[key]
            #assert worker
            #assert isinstance(worker, (str, unicode))

            if self.validate:
                #assert ts.processing_on
                #ws = ts.processing_on
                #assert ts in ws.processing
                assert not ts.waiting_on
                #assert not ts.who_has, (ts, ts.who_has)
                assert not ts.exception_blame
                assert ts.state == "processing"

            if startstops:
                L = [(b, c) for a, b, c in startstops if a == "compute"]
                if L:
                    compute_start, compute_stop = L[0]
                else:  # This is very rare
                    compute_start = compute_stop = None
            else:
                compute_start = compute_stop = None

            #############################
            # Update Timing Information #
            #############################
            # if compute_start and ws.processing.get(ts, True):
                #Update average task duration for worker
                # prefix = ts.prefix
                # old_duration = self.task_duration.get(prefix, 0)
                # new_duration = compute_stop - compute_start
                # if not old_duration:
                    # avg_duration = new_duration
                # else:
                    # avg_duration = 0.5 * old_duration + 0.5 * new_duration

                # self.task_duration[prefix] = avg_duration

                # for tts in self.unknown_durations.pop(prefix, ()):
                    # if tts.processing_on:
                        # wws = tts.processing_on
                        # old = wws.processing[tts]
                        # comm = self.get_comm_cost(tts, wws)
                        # wws.processing[tts] = avg_duration + comm
                        # wws.occupancy += avg_duration + comm - old
                        # self.total_occupancy += avg_duration + comm - old

            ############################
            # Update State Information #
            ############################
            if nbytes is not None:
                ts.set_nbytes(nbytes)

            recommendations = OrderedDict()

            #self._remove_from_processing(ts)

            #self._add_to_memory(ts, ws, recommendations, type=type, typename=typename)
            self._add_to_memory_lambda(ts, recommendations, type=type, typename=typename)

            if self.validate:
                #assert not ts.processing_on
                assert not ts.waiting_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def transition_memory_released_lambda(self, key, safe=False):
        #print("\n\n\n\n{}\n\n\n\n".format("transition_memory_released_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.waiting_on
                #assert not ts.processing_on
                if safe:
                    assert not ts.waiters

            # if ts.actor:
                # for ws in ts.who_has:
                    # ws.actors.discard(ts)
                # if ts.who_wants:
                    # ts.exception_blame = ts
                    # ts.exception = "Worker holding Actor was lost"
                    # return {ts.key: "erred"}  # don't try to recreate

            recommendations = OrderedDict()

            for dts in ts.waiters:
                if dts.state in ("no-worker", "processing"):
                    recommendations[dts.key] = "waiting"
                elif dts.state == "waiting":
                    dts.waiting_on.add(ts)

            # XXX factor this out?
            # for ws in ts.who_has:
                # ws.has_what.remove(ts)
                # ws.nbytes -= ts.get_nbytes()
                # self.worker_send(
                    # ws.address, {"op": "delete-data", "keys": [key], "report": False}
                # )
            #ts.who_has.clear()

            ts.state = "released"

            self.report({"op": "lost-data", "key": key})

            if not ts.run_spec:  # pure data
                recommendations[key] = "forgotten"
            elif ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif ts.who_wants or ts.waiters:
                recommendations[key] = "waiting"

            if self.validate:
                assert not ts.waiting_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def transition_released_erred_lambda(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert ts.exception_blame
                    #assert not ts.who_has
                    assert not ts.waiting_on
                    assert not ts.waiters

            recommendations = {}

            failing_ts = ts.exception_blame

            for dts in ts.dependents:
                dts.exception_blame = failing_ts
                if not dts.who_has:
                    recommendations[dts.key] = "erred"

            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                }
            )

            ts.state = "erred"

            # TODO: waiting data?
            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def transition_erred_released_lambda(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_erred_released_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert all(dts.state != "erred" for dts in ts.dependencies)
                    assert ts.exception_blame
                    #assert not ts.who_has
                    assert not ts.waiting_on
                    assert not ts.waiters

            recommendations = OrderedDict()

            ts.exception = None
            ts.exception_blame = None
            ts.traceback = None

            for dep in ts.dependents:
                if dep.state == "erred":
                    recommendations[dep.key] = "waiting"

            self.report({"op": "task-retried", "key": key})
            ts.state = "released"

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def transition_waiting_released_lambda(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_waiting_released_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert not ts.who_has
                #assert not ts.processing_on

            recommendations = {}

            for dts in ts.dependencies:
                s = dts.waiters
                if ts in s:
                    s.discard(ts)
                    if not s and not dts.who_wants:
                        recommendations[dts.key] = "released"
            ts.waiting_on.clear()

            ts.state = "released"

            if ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif not ts.exception_blame and (ts.who_wants or ts.waiters):
                recommendations[key] = "waiting"
            else:
                ts.waiters.clear()

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def transition_processing_released_lambda(self, key):
        #print("\n\n{}\n\n".format("transition_processing_released_lambda for key: {}".format(key)))
        try:
            ts = self.tasks[key]

            if self.validate:
                #assert ts.processing_on
                assert not ts.who_has
                assert not ts.waiting_on
                assert self.tasks[key].state == "processing"

            # self._remove_from_processing(
                # ts, send_worker_msg={"op": "release-task", "key": key}
            # )

            ts.state = "released"

            recommendations = OrderedDict()

            if ts.has_lost_dependencies:
                recommendations[key] = "forgotten"
            elif ts.waiters or ts.who_wants:
                recommendations[key] = "waiting"

            if recommendations.get(key) != "waiting":
                for dts in ts.dependencies:
                    if dts.state != "released":
                        s = dts.waiters
                        s.discard(ts)
                        if not s and not dts.who_wants:
                            recommendations[dts.key] = "released"
                ts.waiters.clear()

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise      
        
    def transition_processing_erred_lambda(self, key, cause=None, exception=None, traceback=None, **kwargs):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert cause or ts.exception_blame
                assert ts.processing_on
                #assert not ts.who_has
                assert not ts.waiting_on

            # if ts.actor:
                # ws = ts.processing_on
                # ws.actors.remove(ts)

            #self._remove_from_processing(ts)

            if exception is not None:
                ts.exception = exception
            if traceback is not None:
                ts.traceback = traceback
            if cause is not None:
                failing_ts = self.tasks[cause]
                ts.exception_blame = failing_ts
            else:
                failing_ts = ts.exception_blame

            recommendations = {}

            for dts in ts.dependents:
                dts.exception_blame = failing_ts
                recommendations[dts.key] = "erred"

            for dts in ts.dependencies:
                s = dts.waiters
                s.discard(ts)
                if not s and not dts.who_wants:
                    recommendations[dts.key] = "released"

            ts.waiters.clear()  # do anything with this?

            ts.state = "erred"

            self.report(
                {
                    "op": "task-erred",
                    "key": key,
                    "exception": failing_ts.exception,
                    "traceback": failing_ts.traceback,
                }
            )

            cs = self.clients["fire-and-forget"]
            if ts in cs.wants_what:
                self.client_releases_keys(client="fire-and-forget", keys=[key])

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def transition_no_worker_released_lambda(self, key):
        print("\n\n\n\n{}\n\n\n\n".format("transition_no_worker_released_lambda"))
        try:
            ts = self.tasks[key]

            if self.validate:
                assert self.tasks[key].state == "no-worker"
                #assert not ts.who_has
                assert not ts.waiting_on

            self.unrunnable.remove(ts)
            ts.state = "released"

            for dts in ts.dependencies:
                dts.waiters.discard(ts)

            ts.waiters.clear()

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        
    def remove_key(self, key):
        ts = self.tasks.pop(key)
        assert ts.state == "forgotten"
        self.unrunnable.discard(ts)
        for cs in ts.who_wants:
            cs.wants_what.remove(ts)
        ts.who_wants.clear()
        ts.processing_on = None
        ts.exception_blame = ts.exception = ts.traceback = None

        if key in self.task_metadata:
            del self.task_metadata[key]

    def remove_key_lambda(self, key):
        ts = self.tasks.pop(key)
        assert ts.state == "forgotten"
        self.unrunnable.discard(ts)
        for cs in ts.who_wants:
            cs.wants_what.remove(ts)
        ts.who_wants.clear()
        #ts.processing_on = None
        ts.exception_blame = ts.exception = ts.traceback = None

        if key in self.task_metadata:
            del self.task_metadata[key]            
            
    def _propagate_forgotten(self, ts, recommendations):
        ts.state = "forgotten"
        key = ts.key
        for dts in ts.dependents:
            dts.has_lost_dependencies = True
            dts.dependencies.remove(ts)
            dts.waiting_on.discard(ts)
            if dts.state not in ("memory", "erred"):
                # Cannot compute task anymore
                recommendations[dts.key] = "forgotten"
        ts.dependents.clear()
        ts.waiters.clear()

        for dts in ts.dependencies:
            dts.dependents.remove(ts)
            s = dts.waiters
            s.discard(ts)
            if not dts.dependents and not dts.who_wants:
                # Task not needed anymore
                assert dts is not ts
                recommendations[dts.key] = "forgotten"
        ts.dependencies.clear()
        ts.waiting_on.clear()

        for ws in ts.who_has:
            ws.has_what.remove(ts)
            ws.nbytes -= ts.get_nbytes()
            w = ws.address
            if w in self.workers:  # in case worker has died
                self.worker_send(
                    w, {"op": "delete-data", "keys": [key], "report": False}
                )
        ts.who_has.clear()

    def _propagate_forgotten_lambda(self, ts, recommendations):
        ts.state = "forgotten"
        key = ts.key
        for dts in ts.dependents:
            dts.has_lost_dependencies = True
            dts.dependencies.remove(ts)
            dts.waiting_on.discard(ts)
            if dts.state not in ("memory", "erred"):
                # Cannot compute task anymore
                recommendations[dts.key] = "forgotten"
        ts.dependents.clear()
        ts.waiters.clear()

        for dts in ts.dependencies:
            dts.dependents.remove(ts)
            s = dts.waiters
            s.discard(ts)
            if not dts.dependents and not dts.who_wants:
                # Task not needed anymore
                assert dts is not ts
                recommendations[dts.key] = "forgotten"
        ts.dependencies.clear()
        ts.waiting_on.clear()

        # for ws in ts.who_has:
            # ws.has_what.remove(ts)
            # ws.nbytes -= ts.get_nbytes()
            # w = ws.address
            # if w in self.workers:  # in case worker has died
                # self.worker_send(
                    # w, {"op": "delete-data", "keys": [key], "report": False}
                # )
        # ts.who_has.clear()      
        # Remove the key from the Elasticache cluster.
        #memcache_client.delete(ts.key.replace(" ", "_"))
        #self.get_redis_client(ts.key).delete(ts.key)
        #self.hash_ring[ts.key].delete(ts.key)
       
    def transition_memory_forgotten(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.state == "memory"
                assert not ts.processing_on
                assert not ts.waiting_on
                if not ts.run_spec:
                    # It's ok to forget a pure data task
                    pass
                elif ts.has_lost_dependencies:
                    # It's ok to forget a task with forgotten dependencies
                    pass
                elif not ts.who_wants and not ts.waiters and not ts.dependents:
                    # It's ok to forget a task that nobody needs
                    pass
                else:
                    assert 0, (ts,)

            recommendations = {}

            if ts.actor:
                for ws in ts.who_has:
                    ws.actors.discard(ts)

            self._propagate_forgotten(ts, recommendations)

            self.report_on_key(ts=ts)
            self.remove_key(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_released_forgotten(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.state in ("released", "erred")
                assert not ts.who_has
                assert not ts.processing_on
                assert not ts.waiting_on, (ts, ts.waiting_on)
                if not ts.run_spec:
                    # It's ok to forget a pure data task
                    pass
                elif ts.has_lost_dependencies:
                    # It's ok to forget a task with forgotten dependencies
                    pass
                elif not ts.who_wants and not ts.waiters and not ts.dependents:
                    # It's ok to forget a task that nobody needs
                    pass
                else:
                    assert 0, (ts,)

            recommendations = {}
            self._propagate_forgotten(ts, recommendations)

            self.report_on_key(ts=ts)
            self.remove_key(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_memory_forgotten_lambda(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.state == "memory"
                #assert not ts.processing_on
                assert not ts.waiting_on
                if not ts.run_spec:
                    # It's ok to forget a pure data task
                    pass
                elif ts.has_lost_dependencies:
                    # It's ok to forget a task with forgotten dependencies
                    pass
                elif not ts.who_wants and not ts.waiters and not ts.dependents:
                    # It's ok to forget a task that nobody needs
                    pass
                else:
                    assert 0, (ts,)

            recommendations = {}

            # if ts.actor:
                # for ws in ts.who_has:
                    # ws.actors.discard(ts)

            #self._propagate_forgotten(ts, recommendations)
            self._propagate_forgotten_lambda(ts, recommendations)
            
            self.report_on_key(ts=ts)
            #self.remove_key(key)
            self.remove_key_lambda(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transition_released_forgotten_lambda(self, key):
        try:
            ts = self.tasks[key]

            if self.validate:
                assert ts.state in ("released", "erred")
                # assert not ts.who_has
                # assert not ts.processing_on
                assert not ts.waiting_on, (ts, ts.waiting_on)
                if not ts.run_spec:
                    # It's ok to forget a pure data task
                    pass
                elif ts.has_lost_dependencies:
                    # It's ok to forget a task with forgotten dependencies
                    pass
                elif not ts.who_wants and not ts.waiters and not ts.dependents:
                    # It's ok to forget a task that nobody needs
                    pass
                else:
                    assert 0, (ts,)

            recommendations = {}
            #self._propagate_forgotten(ts, recommendations)
            self._propagate_forgotten_lambda(ts, recommendations)

            self.report_on_key(ts=ts)
            #self.remove_key(key)
            self.remove_key_lambda(key)
            
            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise            
            
    def transition(self, key, finish, *args, **kwargs):
        """ Transition a key from its current state to the finish state

        Examples
        --------
        >>> self.transition('x', 'waiting')
        {'x': 'processing'}

        Returns
        -------
        Dictionary of recommendations for future transitions

        See Also
        --------
        Scheduler.transitions: transitive version of this function
        """
        try:
            try:
                ts = self.tasks[key]
            except KeyError:
                return {}
            start = ts.state
            if start == finish:
                return {}

            if self.plugins:
                dependents = set(ts.dependents)
                dependencies = set(ts.dependencies)
            
            #if self.print_debug:
            #    print("Scheduler.transition() -- Task: {} Start: {}, Finish: {}".format(key, start, finish))
            if (start, finish) in self._transitions:
                func = self._transitions[start, finish]
                recommendations = func(key, *args, **kwargs)
            elif "released" not in (start, finish):
                func = self._transitions["released", finish]
                assert not args and not kwargs
                a = self.transition(key, "released")
                if key in a:
                    func = self._transitions["released", a[key]]
                b = func(key)
                a = a.copy()
                a.update(b)
                recommendations = a
                start = "released"
            else:
                raise RuntimeError(
                    "Impossible transition from %r to %r" % (start, finish)
                )

            finish2 = ts.state
            self.transition_log.append((key, start, finish2, recommendations, time()))
            if self.validate:
                logger.debug(
                    "Transitioned %r %s->%s (actual: %s).  Consequence: %s",
                    key,
                    start,
                    finish2,
                    ts.state,
                    dict(recommendations),
                )
                print("Transitioned {} {}->{} (actual: {}). Consequence: {}".format(key, start, finish2, ts.state, dict(recommendations)))
            if self.plugins:
                # Temporarily put back forgotten key for plugin to retrieve it
                if ts.state == "forgotten":
                    try:
                        ts.dependents = dependents
                        ts.dependencies = dependencies
                    except KeyError:
                        pass
                    self.tasks[ts.key] = ts
                for plugin in list(self.plugins):
                    try:
                        plugin.transition(key, start, finish2, *args, **kwargs)
                    except Exception:
                        logger.info("Plugin failed with exception", exc_info=True)
                if ts.state == "forgotten":
                    del self.tasks[ts.key]

            return recommendations
        except Exception as e:
            logger.exception("Error transitioning %r from %r to %r", key, start, finish)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def transitions(self, recommendations):
        """ Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        keys = set()
        recommendations = recommendations.copy()
        while recommendations:
            key, finish = recommendations.popitem()
            keys.add(key)
            new = self.transition(key, finish)
            recommendations.update(new)

        if self.validate:
            for key in keys:
                self.validate_key(key)

    def story(self, *keys):
        """ Get all transitions that touch one of the input keys """
        keys = set(keys)
        return [
            t for t in self.transition_log if t[0] in keys or keys.intersection(t[3])
        ]

    transition_story = story

    def reschedule(self, key=None, worker=None):
        """ Reschedule a task

        Things may have shifted and this task may now be better suited to run
        elsewhere
        """
        print("[WARNING] Rescheduling Task ", key)
        ts = self.tasks[key]
        if ts.state != "processing":
            return
        if worker and ts.processing_on.address != worker:
            return
        self.transitions({key: "released"})

    ##############################
    # Assigning Tasks to Workers #
    ##############################

    def check_idle_saturated(self, ws, occ=None):
        """ Update the status of the idle and saturated state

        The scheduler keeps track of workers that are ..

        -  Saturated: have enough work to stay busy
        -  Idle: do not have enough work to stay busy

        They are considered saturated if they both have enough tasks to occupy
        all of their cores, and if the expected runtime of those tasks is large
        enough.

        This is useful for load balancing and adaptivity.
        """
        if self.total_ncores == 0 or ws.status == "closed":
            return
        if occ is None:
            occ = ws.occupancy
        nc = ws.ncores
        p = len(ws.processing)

        avg = self.total_occupancy / self.total_ncores

        if p < nc or occ / nc < avg / 2:
            self.idle.add(ws)
            self.saturated.discard(ws)
        else:
            self.idle.discard(ws)

            pending = occ * (p - nc) / p / nc
            if p > nc and pending > 0.4 and pending > 1.9 * avg:
                self.saturated.add(ws)
            else:
                self.saturated.discard(ws)

    def valid_workers(self, ts):
        """ Return set of currently valid workers for key

        If all workers are valid then this returns ``True``.
        This checks tracks the following state:

        *  worker_restrictions
        *  host_restrictions
        *  resource_restrictions
        """
        s = True

        if ts.worker_restrictions:
            s = {w for w in ts.worker_restrictions if w in self.workers}

        if ts.host_restrictions:
            # Resolve the alias here rather than early, for the worker
            # may not be connected when host_restrictions is populated
            hr = [self.coerce_hostname(h) for h in ts.host_restrictions]
            # XXX need HostState?
            ss = [self.host_info[h]["addresses"] for h in hr if h in self.host_info]
            ss = set.union(*ss) if ss else set()
            if s is True:
                s = ss
            else:
                s |= ss

        if ts.resource_restrictions:
            w = {
                resource: {
                    w
                    for w, supplied in self.resources[resource].items()
                    if supplied >= required
                }
                for resource, required in ts.resource_restrictions.items()
            }

            ww = set.intersection(*w.values())

            if s is True:
                s = ww
            else:
                s &= ww

        if s is True:
            return s
        else:
            return {self.workers[w] for w in s}

    def consume_resources(self, ts, ws):
        if ts.resource_restrictions:
            for r, required in ts.resource_restrictions.items():
                ws.used_resources[r] += required

    def release_resources(self, ts, ws):
        if ts.resource_restrictions:
            for r, required in ts.resource_restrictions.items():
                ws.used_resources[r] -= required

    #####################
    # Utility functions #
    #####################

    def add_resources(self, stream=None, worker=None, resources=None):
        ws = self.workers[worker]
        if resources:
            ws.resources.update(resources)
        ws.used_resources = {}
        for resource, quantity in ws.resources.items():
            ws.used_resources[resource] = 0
            self.resources[resource][worker] = quantity
        return "OK"

    def remove_resources(self, worker):
        ws = self.workers[worker]
        for resource, quantity in ws.resources.items():
            del self.resources[resource][worker]

    def coerce_address(self, addr, resolve=True):
        """
        Coerce possible input addresses to canonical form.
        *resolve* can be disabled for testing with fake hostnames.

        Handles strings, tuples, or aliases.
        """
        # XXX how many address-parsing routines do we have?
        if addr in self.aliases:
            addr = self.aliases[addr]
        if isinstance(addr, tuple):
            addr = unparse_host_port(*addr)
        if not isinstance(addr, six.string_types):
            raise TypeError("addresses should be strings or tuples, got %r" % (addr,))

        if resolve:
            addr = resolve_address(addr)
        else:
            addr = normalize_address(addr)

        return addr

    def coerce_hostname(self, host):
        """
        Coerce the hostname of a worker.
        """
        if host in self.aliases:
            return self.workers[self.aliases[host]].host
        else:
            return host

    def workers_list(self, workers):
        """
        List of qualifying workers

        Takes a list of worker addresses or hostnames.
        Returns a list of all worker addresses that match
        """
        if workers is None:
            return list(self.workers)

        out = set()
        for w in workers:
            if ":" in w:
                out.add(w)
            else:
                out.update({ww for ww in self.workers if w in ww})  # TODO: quadratic
        return list(out)

    def start_ipython(self, comm=None):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from ._ipython_utils import start_ipython

        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip, ns={"scheduler": self}, log=logger
            )
        return self._ipython_kernel.get_connection_info()

    def worker_objective(self, ts, ws):
        """
        Objective function to determine which worker should get the task

        Minimize expected start time.  If a tie then break with data storage.
        """
        comm_bytes = sum(
            [dts.get_nbytes() for dts in ts.dependencies if ws not in dts.who_has]
        )
        stack_time = ws.occupancy / ws.ncores
        start_time = comm_bytes / self.bandwidth + stack_time

        if ts.actor:
            return (len(ws.actors), start_time, ws.nbytes)
        else:
            return (start_time, ws.nbytes)

    @gen.coroutine
    def get_profile(
        self,
        comm=None,
        workers=None,
        merge_workers=True,
        start=None,
        stop=None,
        key=None,
    ):
        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)
        result = yield {
            w: self.rpc(w).profile(start=start, stop=stop, key=key) for w in workers
        }
        if merge_workers:
            result = profile.merge(*result.values())
        raise gen.Return(result)

    @gen.coroutine
    def get_profile_metadata(
        self,
        comm=None,
        workers=None,
        merge_workers=True,
        start=None,
        stop=None,
        profile_cycle_interval=None,
    ):
        dt = profile_cycle_interval or dask.config.get(
            "distributed.worker.profile.cycle"
        )
        dt = parse_timedelta(dt, default="ms")

        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)
        result = yield {
            w: self.rpc(w).profile_metadata(start=start, stop=stop) for w in workers
        }

        counts = [v["counts"] for v in result.values()]
        counts = itertools.groupby(merge_sorted(*counts), lambda t: t[0] // dt * dt)
        counts = [(time, sum(pluck(1, group))) for time, group in counts]

        keys = set()
        for v in result.values():
            for t, d in v["keys"]:
                for k in d:
                    keys.add(k)
        keys = {k: [] for k in keys}

        groups1 = [v["keys"] for v in result.values()]
        groups2 = list(merge_sorted(*groups1, key=first))

        last = 0
        for t, d in groups2:
            tt = t // dt * dt
            if tt > last:
                last = tt
                for k, v in keys.items():
                    v.append([tt, 0])
            for k, v in d.items():
                keys[k][-1][1] += v

        raise gen.Return({"counts": counts, "keys": keys})

    def get_logs(self, comm=None, n=None):
        deque_handler = self._deque_handler
        if n is None:
            L = list(deque_handler.deque)
        else:
            L = deque_handler.deque
            L = [L[-i] for i in range(min(n, len(L)))]
        return [(msg.levelname, deque_handler.format(msg)) for msg in L]

    @gen.coroutine
    def get_worker_logs(self, comm=None, n=None, workers=None):
        results = yield self.broadcast(msg={"op": "get_logs", "n": n}, workers=workers)
        raise gen.Return(results)

    ###########
    # Cleanup #
    ###########

    def reevaluate_occupancy(self, worker_index=0):
        """ Periodically reassess task duration time

        The expected duration of a task can change over time.  Unfortunately we
        don't have a good constant-time way to propagate the effects of these
        changes out to the summaries that they affect, like the total expected
        runtime of each of the workers, or what tasks are stealable.

        In this coroutine we walk through all of the workers and re-align their
        estimates with the current state of tasks.  We do this periodically
        rather than at every transition, and we only do it if the scheduler
        process isn't under load (using psutil.Process.cpu_percent()).  This
        lets us avoid this fringe optimization when we have better things to
        think about.
        """
        DELAY = 0.1
        try:
            if self.status == "closed":
                return

            last = time()
            next_time = timedelta(seconds=DELAY)

            if self.proc.cpu_percent() < 50:
                workers = list(self.workers.values())
                for i in range(len(workers)):
                    ws = workers[worker_index % len(workers)]
                    worker_index += 1
                    try:
                        if ws is None or not ws.processing:
                            continue
                        self._reevaluate_occupancy_worker(ws)
                    finally:
                        del ws  # lose ref

                    duration = time() - last
                    if duration > 0.005:  # 5ms since last release
                        next_time = timedelta(seconds=duration * 5)  # 25ms gap
                        break

            self.loop.add_timeout(
                next_time, self.reevaluate_occupancy, worker_index=worker_index
            )

        except Exception:
            logger.error("Error in reevaluate occupancy", exc_info=True)
            raise

    def _reevaluate_occupancy_worker(self, ws):
        """ See reevaluate_occupancy """
        old = ws.occupancy

        new = 0
        nbytes = 0
        for ts in ws.processing:
            duration = self.get_task_duration(ts)
            comm = self.get_comm_cost(ts, ws)
            ws.processing[ts] = duration + comm
            new += duration + comm

        ws.occupancy = new
        self.total_occupancy += new - old
        self.check_idle_saturated(ws)

        # significant increase in duration
        if (new > old * 1.3) and ("stealing" in self.extensions):
            steal = self.extensions["stealing"]
            for ts in ws.processing:
                steal.remove_key_from_stealable(ts)
                steal.put_key_in_stealable(ts)

    def check_worker_ttl(self):
        now = time()
        for ws in self.workers.values():
            if ws.last_seen < now - self.worker_ttl:
                logger.warning(
                    "Worker failed to heartbeat within %s seconds. " "Closing: %s",
                    self.worker_ttl,
                    ws,
                )
                self.remove_worker(address=ws.address)

    def check_idle(self):
        if any(ws.processing for ws in self.workers.values()):
            return
        if self.unrunnable:
            return

        if not self.transition_log:
            close = time() > self.time_started + self.idle_timeout
        else:
            last_task = self.transition_log[-1][-1]
            close = time() > last_task + self.idle_timeout

        if close:
            self.loop.add_callback(self.close)


def decide_worker(ts, all_workers, valid_workers, objective):
    """
    Decide which worker should take task *ts*.

    We choose the worker that has the data on which *ts* depends.

    If several workers have dependencies then we choose the less-busy worker.

    Optionally provide *valid_workers* of where jobs are allowed to occur
    (if all workers are allowed to take the task, pass True instead).

    If the task requires data communication because no eligible worker has
    all the dependencies already, then we choose to minimize the number
    of bytes sent between workers.  This is determined by calling the
    *objective* function.
    """
    deps = ts.dependencies
    assert all(dts.who_has for dts in deps)
    if ts.actor:
        candidates = all_workers
    else:
        candidates = frequencies([ws for dts in deps for ws in dts.who_has])
    if valid_workers is True:
        if not candidates:
            candidates = all_workers
    else:
        candidates = valid_workers & set(candidates)
        if not candidates:
            candidates = valid_workers
            if not candidates:
                if ts.loose_restrictions:
                    return decide_worker(ts, all_workers, True, objective)
                else:
                    return None
    if not candidates:
        return None

    if len(candidates) == 1:
        return first(candidates)

    return min(candidates, key=objective)


def validate_task_state(ts):
    """
    Validate the given TaskState.
    """
    assert ts.state in ALL_TASK_STATES or ts.state == "forgotten", ts

    if ts.waiting_on:
        assert ts.waiting_on.issubset(ts.dependencies), (
            "waiting not subset of dependencies",
            str(ts.waiting_on),
            str(ts.dependencies),
        )
    if ts.waiters:
        assert ts.waiters.issubset(ts.dependents), (
            "waiters not subset of dependents",
            str(ts.waiters),
            str(ts.dependents),
        )

    for dts in ts.waiting_on:
        assert not dts.who_has, ("waiting on in-memory dep", str(ts), str(dts))
        assert dts.state != "released", ("waiting on released dep", str(ts), str(dts))
    for dts in ts.dependencies:
        assert ts in dts.dependents, (
            "not in dependency's dependents",
            str(ts),
            str(dts),
            str(dts.dependents),
        )
        if ts.state in ("waiting", "processing"):
            assert dts in ts.waiting_on or dts.who_has, (
                "dep missing",
                str(ts),
                str(dts),
            )
        assert dts.state != "forgotten"

    for dts in ts.waiters:
        assert dts.state in ("waiting", "processing"), (
            "waiter not in play",
            str(ts),
            str(dts),
        )
    for dts in ts.dependents:
        assert ts in dts.dependencies, (
            "not in dependent's dependencies",
            str(ts),
            str(dts),
            str(dts.dependencies),
        )
        assert dts.state != "forgotten"

    assert (ts.processing_on is not None) == (ts.state == "processing")
    assert bool(ts.who_has) == (ts.state == "memory"), (ts, ts.who_has)

    if ts.state == "processing":
        assert all(dts.who_has for dts in ts.dependencies), (
            "task processing without all deps",
            str(ts),
            str(ts.dependencies),
        )
        assert not ts.waiting_on

    if ts.who_has:
        assert ts.waiters or ts.who_wants, (
            "unneeded task in memory",
            str(ts),
            str(ts.who_has),
        )
        if ts.run_spec:  # was computed
            assert ts.type
            assert isinstance(ts.type, str)
        assert not any(ts in dts.waiting_on for dts in ts.dependents)
        for ws in ts.who_has:
            assert ts in ws.has_what, (
                "not in who_has' has_what",
                str(ts),
                str(ws),
                str(ws.has_what),
            )

    if ts.who_wants:
        for cs in ts.who_wants:
            assert ts in cs.wants_what, (
                "not in who_wants' wants_what",
                str(ts),
                str(cs),
                str(cs.wants_what),
            )

    if ts.actor:
        if ts.state == "memory":
            assert sum([ts in ws.actors for ws in ts.who_has]) == 1
        if ts.state == "processing":
            assert ts in ts.processing_on.actors


def validate_worker_state(ws):
    for ts in ws.has_what:
        assert ws in ts.who_has, (
            "not in has_what' who_has",
            str(ws),
            str(ts),
            str(ts.who_has),
        )

    for ts in ws.actors:
        assert ts.state in ("memory", "processing")


def validate_state(tasks, workers, clients):
    """
    Validate a current runtime state

    This performs a sequence of checks on the entire graph, running in about
    linear time.  This raises assert errors if anything doesn't check out.
    """
    for ts in tasks.values():
        validate_task_state(ts)

    for ws in workers.values():
        validate_worker_state(ws)

    for cs in clients.values():
        for ts in cs.wants_what:
            assert cs in ts.who_wants, (
                "not in wants_what' who_wants",
                str(cs),
                str(ts),
                str(ts.who_wants),
            )


_round_robin = [0]


fast_tasks = {"rechunk-split", "shuffle-split"}

 
def heartbeat_interval(n):
    """
    Interval in seconds that we desire heartbeats based on number of workers
    """
    if n <= 10:
        return 0.5
    elif n < 50:
        return 1
    elif n < 200:
        return 2
    else:
        return 5


class KilledWorker(Exception):
    def __init__(self, task, last_worker):
        super(KilledWorker, self).__init__(task, last_worker)
        self.task = task
        self.last_worker = last_worker