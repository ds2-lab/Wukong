import time 
import datetime 
import plotly.figure_factory as ff
import plotly as py 
import plotly.graph_objects as go
import pandas as pd
import math
import cloudpickle 
from wukong_metrics import WukongEvent, LambdaExecutionBreakdown, TaskExecutionBreakdown
from random import randint, uniform

from bokeh.io import output_file, show
from bokeh.models import (PanTool, BoxZoomTool, Circle, HoverTool, WheelZoomTool, SaveTool,
                          MultiLine, Plot, Range1d, ResetTool, TapTool, EdgesAndLinkedNodes, NodesAndLinkedEdges)
from bokeh.palettes import Spectral4
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.models.graphs import from_networkx
from bokeh.plotting import figure, ColumnDataSource, curdoc

import networkx as nx

from dask.core import istask, ishashable, get_dependencies

import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

all_events = ["Store Intermediate Data in Cloud Storage", "Store PathNodes in Cloud Storage", "Get IP from Coordinator", "Invoke Cluster Schedulers", 
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

all_events = list(set(all_events))

# There should be 61 colors.
all_colors = ["rgb(102,0,0)", "rgb(153,84,38)", "rgb(242,214,182)", "rgb(242,238,182)", "rgb(212,255,128)", "rgb(48,64,52)", "rgb(61,242,230)", "rgb(0,153,230)", "rgb(102,129,204)", "rgb(208,191,255)", "rgb(255,0,238)", "rgb(255,64,166)", "rgb(127,64,72)", "rgb(191,143,143)", "rgb(242,170,121)", "rgb(115,77,0)", "rgb(238,255,0)", "rgb(34,255,0)", "rgb(102,204,143)", "rgb(96,185,191)", "rgb(29,75,115)", "rgb(35,49,140)", "rgb(54,38,77)", "rgb(51,0,41)", "rgb(242,0,97)", "rgb(255,34,0)", "rgb(89,76,67)", "rgb(64,43,0)", "rgb(73,77,19)", "rgb(67,191,48)", "rgb(115,153,135)", "rgb(0,204,255)", "rgb(182,214,242)", "rgb(57,57,230)", "rgb(170,0,255)", "rgb(115,29,98)", "rgb(64,32,45)", "rgb(178,62,45)", "rgb(229,122,0)", "rgb(204,153,51)", "rgb(150,153,115)", "rgb(29,115,29)", "rgb(0,140,112)", "rgb(0,112,140)", "rgb(96,108,128)", "rgb(31,0,115)", "rgb(170,45,179)", "rgb(191,96,159)", "rgb(64,0,17)", "rgb(51,20,0)", "rgb(115,88,57)", "rgb(255,217,64)", "rgb(112,140,0)", "rgb(0,64,9)", "rgb(0,77,71)", "rgb(0,61,77)", "rgb(0,17,64)", "rgb(121,96,191)", "rgb(125,96,128)", "rgb(242,182,222)", "rgb(204,102,116)"]

colors = {x:y for (x,y) in zip(all_events, all_colors)}

def name(x):
    try:
        return str(hash(x))
    except TypeError:
        return str(hash(str(x)))

def plot_networkx_graph(graph, **kwargs):
    workload_id = kwargs.pop("workload_id", "")
    plot_width = kwargs.pop("plot_width", 1280)
    plot_height = kwargs.pop("plot_height", 720)
    scale = kwargs.pop("scale", 3)
    circle_size = kwargs.pop("circle_size", 25)

    plot = Plot(plot_width = plot_width, plot_height = plot_height, x_range=Range1d(-1.1, 1.1), y_range=Range1d(-1.1, 1.1))
    plot.title.text = "Workload {} DAG".format(workload_id)

    index_map = dict()

    plot.add_tools(HoverTool(tooltips=[("Task Key", "@task_key"), ("Task State", "@task_state")]), TapTool(), PanTool(), WheelZoomTool(), SaveTool())
    plot.output_backend = "svg"

    graph_renderer = from_networkx(graph, nx.spring_layout, scale=scale, center=(0, 0))

    graph_renderer.node_renderer.glyph = Circle(size=circle_size, fill_color="node_color")

    graph_renderer.edge_renderer.glyph = MultiLine(line_color="black", line_alpha=0.8, line_width=1) 
    
    current_index = 0
    for task_key in graph_renderer.node_renderer.data_source.data['task_key']:
        index_map[task_key] = current_index
        current_index += 1

    plot.renderers.append(graph_renderer)

    return plot, graph_renderer, index_map

def to_networkx(dsk, **kwargs):
    g = nx.DiGraph(directed=True)
    seen = set()
    connected = set()
    current_index = 0
    for k, v in dsk.items():
        k_name = name(k)
        if istask(v):
            for dep in get_dependencies(dsk, k):
                dep_name = name(dep)
                if dep_name not in seen:
                    seen.add(dep_name)
                    g.add_node(dep_name, task_key = str(dep), task_state = "NOT READY", node_color = "royalblue")
                    current_index += 1
                g.add_edge(dep_name, k_name)
                connected.add(dep_name)
                connected.add(k_name)
        elif ishashable(v) and v in dsk:
            v_name = name(v)
            g.add_edge(v_name, k_name)
            connected.add(v_name)
            connected.add(k_name)
        if k_name not in seen:
            seen.add(k_name)
            g.add_node(k_name, task_key = str(k), task_state = "NOT READY", node_color = "royalblue")
            current_index += 1
    return g

def plot_gantt_chart(
    lambda_metrics = None,
    output_file_name = None
):
    """
    Plot a Gantt chart of the execution of the workload described by the LambdaExecutionBreakdown objects within lambda_metrics.

    Arguments:
    ----------
        lambda_metrics (list or dict): If dict, will just use the values of dict. Contains LambdaExecutionBreakdown from a given workload.

        output_file_name: The filename to use for the Gantt chart.
    """
    if type(lambda_metrics) is dict:
        lm = list(lambda_metrics.values())
    else:
        lm = lambda_metrics

    data = []
    current_index = 0
    lambda_counter = 0
    event_counter = 0
    key_to_lambda = dict()

    for leb in lm:
        #logger.debug("leb = {}".format(leb))
        for event in leb.events: 
            if event.name not in all_events:
                raise ValueError("ERROR: Unknown event: \"{}\"".format(event.name))
            if event.metadata is not None: 
                if type(event.metadata) is tuple:   # For some reason, I am seeing them returned as tuples, like ({"key": "value"},)
                    event.metadata = event.metadata[0]
                if event.name == "Execute Task":
                    #logger.debug("event.metadata = {}".format(event.metadata))
                    task_key = event.metadata["ExecutedTaskID"]
            
                    # If a task was executed on this Lambda, then we know this is the 
                    # Lambda that would be sending the task out to other Lambdas.
                    if task_key not in key_to_lambda:
                        key_to_lambda[task_key] = current_index 
                    else:
                        existing_index = key_to_lambda[task_key]
                        if existing_index != current_index:
                            logger.warning("Inconsistent upstream task {}. Prev: {}, Current: {}.".format(upstream_key, existing_index, current_index))
                            #raise Exception("Inconsistent upstream task {}. Prev: {}, Current: {}.".format(task_key, existing_index, current_index))               
                elif event.name == "P2P Send":
                    pairing_name = event.metadata.get("PairingName", "N/A")
                    upstream_key = event.metadata.get("UpstreamTaskKey", "N/A")
                    downstream_key = event.metadata.get("DownstreamTaskKey", "N/A")
            
                    # If we have the upstream key, then that's the Lambda that executed the task. Encode association.
                    if upstream_key not in key_to_lambda:
                        key_to_lambda[upstream_key] = current_index
                    else:
                        existing_index = key_to_lambda[upstream_key]
                        if existing_index != current_index:
                            logger.warning("Inconsistent upstream task {}. Prev: {}, Current: {}.".format(upstream_key, existing_index, current_index))
                            #raise Exception("Inconsistent upstream task {}. Prev: {}, Current: {}.".format(upstream_key, existing_index, current_index))
        current_index += 1
        event_counter = 1
        lambda_counter += 1  

    current_index = 0
    lambda_counter = 0
    event_counter = 0

    for leb in lm:
        #logger.debug("\n-=-=-= Lambda #{} =-=-=-".format(lambda_counter))
        event_counts = {}
        for event in leb.events:
            start = event.start_time
            start_ts = datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S.%f')
            end = event.end_time 
            end_ts = datetime.datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S.%f')
            name = event.name 
            event_count = 1
            if name in event_counts:
                event_counts[name] += 1
                event_count = event_counts[name]
            else:
                event_counts[name] = event_count
            #logger.debug("\tEvent #{} -- {} #{}:\n\t\tStart: {}\n\t\tStop: {}\n".format(event_counter, name, event_count, start_ts, end_ts))
            event_counter += 1
            #if name not in colors:
                #logger.debug("ERROR: \"{}\"".format(name))
            description = "===== {} =====<br />".format(event.name)
            if event.metadata is not None:
                if event.name == "P2P Connect as Downstream":      
                    pairing_name = event.metadata["PairingName"]
                    downstream_key = pairing_name
                    duration = event.metadata["Duration"]
                    description = description + "{}={}<br />".format("PairingName", pairing_name)
                    description = description + "{}={}<br />".format("DownstreamTask",downstream_key)  
                    description = description + "{}={}<br />".format("Duration", duration)
                elif event.name == "P2P Connect as Upstream":
                    pairing_name = event.metadata["PairingName"]
                    downstream_key = pairing_name
                    upstream_key = event.metadata["UpstreamTaskKey"]
                    duration = event.metadata["Duration"]
                    description = description + "{}={}<br />".format("PairingName", pairing_name)
                    description = description + "{}={}<br />".format("UpstreamTask", upstream_key)     
                    description = description + "{}={}<br />".format("DownstreamTask", downstream_key)
                    description = description + "{}={}<br />".format("Duration", duration)
                    recipient_id = "N/A"
                    if upstream_key not in key_to_lambda:
                        key_to_lambda[upstream_key] = current_index
                    else:
                        existing_index = key_to_lambda[upstream_key]
                        if existing_index != current_index:
                            logger.warning("Inconsistent upstream task {}. Prev: {}, Current: {}.".format(upstream_key, existing_index, current_index))
                            #raise Exception("Inconsistent upstream task {}. Prev: {}, Current: {}.".format(upstream_key, existing_index, current_index))
                    if downstream_key in key_to_lambda:
                        recipient_id = key_to_lambda[downstream_key]
                    description = description + "{}={}<br />".format("DownstreamPeer","Lambda " + str(recipient_id))            
                elif event.name == "P2P Send":  
                    pairing_name = event.metadata.get("PairingName", "N/A")
                    upstream_key = event.metadata.get("UpstreamTaskKey", "N/A")
                    downstream_key = event.metadata.get("DownstreamTaskKey", "N/A")          
                    duration = event.metadata["Duration"]
                    description = description + "{}={}<br />".format("PairingName", pairing_name)
                    description = description + "{}={}<br />".format("UpstreamTask", upstream_key)
                    description = description + "{}={}<br />".format("DownstreamTask",downstream_key)   
                    description = description + "{}={}<br />".format("Duration", duration)
                    recipient_id = "N/A"
                    if upstream_key not in key_to_lambda:
                        key_to_lambda[upstream_key] = current_index
                    else:
                        existing_index = key_to_lambda[upstream_key]
                        if existing_index != current_index:
                            logger.warning("WARNING: Inconsistent upstream task {}. Prev: {}, Current: {}.".format(upstream_key, existing_index, current_index))
                            #raise Exception("Inconsistent upstream task {}. Prev: {}, Current: {}.".format(upstream_key, existing_index, current_index))
                    if downstream_key in key_to_lambda:
                        recipient_id = key_to_lambda[downstream_key]
                    description = description + "{}={}<br />".format("Recipient","Lambda " + str(recipient_id))
                elif event.name == "P2P Poll Loop" or event.name == "P2P Receive Data":
                    #pairing_name = event.metadata.get("PairingName", "N/A")
                    #description = description + "{}={}<br />".format("PairingName", pairing_name)
                    for k,v in event.metadata.items():
                        if k == "TasksReceived" or k == "NumBytesRead":
                            continue 
                        description = description + "{}={}<br />".format(k,v)       
                    description = description + "NumBytesRead: {:,}<br />".format(event.metadata.get("NumBytesRead", -1))
                    tasks_received = event.metadata.get("TasksReceived", "NONE")             
                    description = description + "=== Tasks Received ===<br />"
                    for task_key in tasks_received:
                        upstream_lambda = key_to_lambda.get(task_key, "N/A")
                        description = description + "   {} (Sender = Lambda {})<br />".format(task_key, upstream_lambda)    
                    description = description + "===================<br />"
                else:
                    for k,v in event.metadata.items():
                        description = description + "{}={}<br />".format(k,v)
            data.append(dict(
                Task = "Lambda " + str(current_index), 
                Start = datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S.%f'), 
                Finish = datetime.datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S.%f'),
                Resource = name,
                Description = description))
        current_index += 1
        event_counter = 1
        lambda_counter += 1

    fig = ff.create_gantt(data, colors = colors, index_col = 'Resource', show_colorbar = True, group_tasks = True, showgrid_x=True, showgrid_y=True)
    
    logger.debug("Number of LambdaExecutionBreakdown objects: {}".format(len(lm)))

    height = (len(lm) * 50) + 500

    logger.debug("Resulting height of Gantt chart: {} pixels(?)".format(height))

    fig.update_layout(
        autosize = True,
        width = 1920,
        height = height
    )

    fig.show()
    py.offline.plot(fig, filename = output_file_name)