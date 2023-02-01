import ujson
import cloudpickle 
import time 
import redis 
#import multiprocessing
import boto3 

lc = boto3.client("lambda", region_name = "us-east-1")
redis_client = None 

def lambda_handler(event, context):
    """
    This is the handler for the "Invoker" AWS Lambda function. 
    The "Invoker" serverless function is used to parallelize large fan-outs in a tree-like structure. 
    
    Arguments:
    ----------
        event: JSON-formatted document that contains data for a Lambda function to process. These are essentially input arguments passed to the function during invocation.
        
        context: Provides methods and properties that provide information about the invocation, function, and execution environment. Passed by AWS Lambda to the function at runtime. 
    """
    global redis_client

    payloads_serialized = None 
    lambda_function_name = event["lambda_function_name"]
    st = time.time()
    if "redis_key" in event: 
        redis_key = event["redis_key"]
        redis_addr = event["redis_address"]
        if redis_client is None:
            print("Connecting to Redis at {}.".format(redis_addr))
            redis_client = redis.Redis(host = redis_addr, port = 6379, db = 0)
            print("Connection successful.")
        print("Retrieving data from Redis at key {}.".format(redis_key))
        t = time.time()
        payloads_serialized = redis_client.get(redis_key)
        e = time.time()
        print("Time to retrieve payloads from Redis: {} seconds.".format(e - t))
        
        t = time.time()
        payloads_serialized = ujson.loads(payloads_serialized)["payloads_serialized"]
        e = time.time()
        print("Time to deserialize payloads from Redis: {} seconds".format((e-t)))
    else:    
        payloads_serialized = event["payloads_serialized"]
    
    print("Invoking {} lambdas with function name {}.".format(len(payloads_serialized), lambda_function_name))
    
    t = time.time()
    for payload in payloads_serialized:
        lc.invoke(FunctionName=lambda_function_name, InvocationType='Event', Payload=payload)
    end = time.time()
    
    print("Time to invoke {} functions: {} seconds. Total time: {} seconds.".format(len(payloads_serialized), (end - t), (end - st)))
    return {
        'statusCode': 200,
        'body': ujson.dumps('Invoked {} Lambda functions.'.format(len(payloads_serialized)))
    }
