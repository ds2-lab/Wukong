import boto3
import json
import logging 
import yaml

# Set up logging.
import logging 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(ch)

PATH_PROMPT = "Please enter the path to the Wukong Setup Configuration File. Enter nothing for default (same directory as this script).\n> "

def create_wukong_vpc(aws_region : str, wukong_vpc_config : dict):
    """
    This function first creates a Virtual Private Cloud (VPC). 
    Next, it creates an Internet Gateway, allocates an Elastic IP Address, and creates a NAT Gateway.

    Arguments:
    ----------
        aws_region (string)

        wukong_vpc_config (dict)
    
    Returns:
    --------
        dict {
            "VpcId": The VPC ID of the newly-created VPC.
            "LambdaSecurityGroupId": The ID of the security group created for the AWS Lambda functions.
            "PrivateSubnetIds": The ID's of the newly-created private subnets (used for AWS Lambda functions).
        }
    """
    ec2_resource = boto3.resource('ec2', region_name = aws_region)
    ec2_client = boto3.client('ec2', region_name = aws_region)

    CidrBlock = wukong_vpc_config["CidrBlock"]
    PublicSubnetCidrBlock = wukong_vpc_config["PublicSubnetCidrBlock"]
    #PrivateSubnetCidrBlocks = wukong_vpc_config["PrivateSubnetCidrBlocks"]
    #lambda_security_group_name = wukong_vpc_config["lambda_security_group_name"]
    serverful_security_group_name = wukong_vpc_config["serverful_security_group_name"]

    # Validate parameters/arguments.
    #assert(len(lambda_security_group_name) >= 1 and len(lambda_security_group_name) <= 64)
    assert(len(serverful_security_group_name) >= 1 and len(serverful_security_group_name) <= 64)

    logger.info("Creating VPC now...")

    # Create the VPC.
    create_vpc_response = ec2_client.create_vpc(CidrBlock = CidrBlock)
    vpc = ec2_resource.Vpc(create_vpc_response["Vpc"]["VpcId"])
    vpc.wait_until_available()

    logger.info("Successfully created a VPC. VPC ID: " + vpc.id)
    logger.info("Next, creating the public subnet...")

    # Create the public subnet (used by EC2 and AWS Fargate).
    public_subnet = vpc.create_subnet(CidrBlock = PublicSubnetCidrBlock)
    ec2_client.modify_subnet_attribute(SubnetId = public_subnet.id, MapPublicIpOnLaunch = {'Value': True})

    logger.info("Successfully created the public subnet. Subnet ID: " + public_subnet.id)
    
    # We print a different message depending on how many private subnets we're creating.
    # if len(PrivateSubnetCidrBlocks) == 1:
    #     logger.info("Next, creating 1 private subnet...")
    # else:
    #     logger.info("Next, creating {} private subnets...".format(len(PrivateSubnetCidrBlocks))) 
    
    # Create all of the private subnets.
    # private_subnets = list()
    # for PrivateSubnetCidrBlock in PrivateSubnetCidrBlocks:
    #     private_subnet = vpc.create_subnet(CidrBlock = PrivateSubnetCidrBlock)
    #     private_subnets.append(private_subnet)

    # # We print a different message depending on how many private subnets we created.
    # if len(private_subnets) == 1:
    #     private_subnet = private_subnets[0]
    #     logger.info("Successfully created 1 private subnet. Subnet ID: " + private_subnet.id)
    # else:
    #     private_subnet_ids = [private_subnet.id for private_subnet in private_subnets]
    #     logger.info("Successfully created {} private subnets. Subnet IDs: " + str(private_subnet_ids))
    logger.info("Next, creating an internet gateway...")
    
    # Create and attach an internet gateway.
    create_internet_gateway_response = ec2_client.create_internet_gateway()
    internet_gateway_id = create_internet_gateway_response["InternetGateway"]["InternetGatewayId"]
    vpc.attach_internet_gateway(InternetGatewayId = internet_gateway_id)

    logger.info("Successfully created an Internet Gateway and attached it to the VPC. Internet Gateway ID: " + internet_gateway_id)
    #logger.info("Next, allocating elastic IP address and creating NAT gateway...")

    #elastic_ip_response = ec2_client.allocate_address(Domain = 'vpc')
    #nat_gateway = ec2_client.create_nat_gateway(SubnetId = public_subnet.id, AllocationId = elastic_ip_response['AllocationId'])

    #logger.info("Successfully allocated elastic IP address and created NAT gateway. NAT Gateway ID: " + nat_gateway.id)
    logger.info("Next, creating route tables and associated public route table with public subnet...")

    # The VPC creates a route table, so we have one to begin with. We use this as the public route table.
    initial_route_table = list(vpc.route_tables.all())[0] 
    initial_route_table.create_route(
        DestinationCidrBlock = '0.0.0.0/0',
        GatewayId = internet_gateway_id
    )
    initial_route_table.associate_with_subnet(SubnetId = public_subnet.id)

    # Now create the private route table.
    # private_route_table = vpc.create_route_table()
    # private_route_table.create_route(
    #     DestinationCidrBlock = '0.0.0.0/0',
    #     GatewayId = nat_gateway.id
    # )

    logger.info("Successfully created the route tables and associated public route table with public subnet.")

    # We print a different message depending on how many private subnets we created.
    # if len(private_subnets) == 1:
    #     logger.info("Next, associating private route table with the private subnet.")
    # else:
    #     logger.info("Next, associating private route table with the private subnets.")
    
    # # Associate the private route table with each private subnet.
    # for private_subnet in private_subnets:
    #     private_route_table.associate_with_subnet(SubnetId = private_subnet.id)
    
    # # We print a different message depending on how many private subnets we created.
    # if len(private_subnets) == 1:
    #     logger.info("Successfully associated the private route table with the private subnet.")
    # else:
    #     logger.info("Successfully associated the private route table with the private subnets.")
    logger.info("Next, creating and configuring the security group...")

    # The security group used by AWS Lambda functions.
    #lambda_security_group = ec2_resource.create_security_group(
    #   GroupName = lambda_security_group_name, VpcId = vpc.id)

    # The security group used by AWS EC2 and AWS Fargate instances/nodes.
    serverful_security_group = ec2_resource.create_security_group(
        Description='Wukong Security Group', GroupName = serverful_security_group_name, VpcId = vpc.id)
    
    #lambda_security_group.authorize_ingress(CidrIp = '0.0.0.0/0', IpProtocol = '-1', FromPort = 0, ToPort = 65535)
    serverful_security_group.authorize_ingress(CidrIp = '0.0.0.0/0', IpProtocol = '-1', FromPort = 0, ToPort = 65535)

    logger.info("Successfully created and configured the security group.\n\n")
    logger.info("==========================")
    logger.info("Wukong VPC setup complete.")
    logger.info("==========================")

    return {
        "VpcId": vpc.id
        #"LambdaSecurityGroupId": lambda_security_group.id,
        #"PrivateSubnetIds": [private_subnet.id for private_subnet in private_subnets]
    }

def setup_aws_lambda(aws_region : str, wukong_lambda_config : dict, vpc_id : str, private_subnet_ids : list, lambda_security_group_id : str):
    """
    Create the two AWS Lambda functions required by Wukong.

    Arguments:
    ----------
        aws_region (str)

        wukong_lambda_config (dict)

        vpc_id (str)

        private_subnet_ids (list)

        lambda_security_group_id (str)
    """
    executor_function_name = wukong_lambda_config["executor_function_name"]
    invoker_function_name = wukong_lambda_config["invoker_function_name"]
    iam_role_name = wukong_lambda_config["iam_role_name"]
    function_memory_mb = wukong_lambda_config["function_memory_mb"]
    function_timeout_seconds = wukong_lambda_config["function_timeout_seconds"]
    
    # Validate the parameters.
    assert(len(executor_function_name) >= 1 and len(executor_function_name) <= 64)
    assert(len(invoker_function_name) >= 1 and len(invoker_function_name) <= 64)
    assert(function_memory_mb >= 128 and function_memory_mb <= 3008)
    assert(function_timeout_seconds >= 1 and function_timeout_seconds <= 900)

    lambda_client = boto3.client("lambda", region_name = aws_region)
    iam = boto3.client('iam')

    logger.info("First, we need to create an IAM role for the AWS Lambda functions. Creating role now...")
    description = "Role used for AWS Lambda functions within the Wukong serverless DAG execution framework."

    # This allows AWS Lambda functions to assume the role that we're creating here.
    AssumeRolePolicyDocument = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                },
                "Action": [
                    "sts:AssumeRole"
                ]
            }
        ]
    }

    create_role_response = iam.create_role(
        RoleName = iam_role_name, Description = description, AssumeRolePolicyDocument = json.dumps(AssumeRolePolicyDocument)) 
    role_arn = create_role_response['Role']['Arn']
    
    # Now we must attach all of the required policies.
    iam.attach_role_policy(
        PolicyArn = 'arn:aws:iam::aws:policy/AWSLambdaFullAccess',
        RoleName = iam_role_name)
    iam.attach_role_policy(
        PolicyArn = 'arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess',
        RoleName = iam_role_name)
    iam.attach_role_policy(
        PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess',
        RoleName = iam_role_name)
    iam.attach_role_policy(
        PolicyArn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole',
        RoleName = iam_role_name)                              

    logger.info("Successfully created and configured IAM role for AWS Lambda functions.")
    logger.info("Next, creating AWS Lambda function for Wukong Executor...")
    lambda_client.create_function(
        Code = "", # TODO: Fill in code somehow... 
        FunctionName = executor_function_name,
        Runtime = 'python3.7',
        Role = role_arn,
        Handler = 'function.lambda_handler',
        MemorySize = function_memory_mb,
        Timeout = function_timeout_seconds,
        VpcConfig = {
            'SubnetIds': private_subnet_ids,
            'SecurityGroupIds': lambda_security_group_id
        },
        Layers = [
            # This is an Amazon-created layer containing NumPy and SciPy.
            "arn:aws:lambda:us-east-1:668099181075:layer:AWSLambda-Python37-SciPy1x:2",
            # This contains Dask itself.
            "arn:aws:lambda:us-east-1:561589293384:layer:DaskLayer2:2",
            # This contains Dask's dependencies as well as the AWS X-Ray module/library.
            "arn:aws:lambda:us-east-1:561589293384:layer:DaskDependenciesAndXRay:6",
            # This contains DaskML and its dependencies.
            "arn:aws:lambda:us-east-1:561589293384:layer:dask-ml-layer:9"
        ]
    )

def setup_aws_fargate(aws_region : str, wukong_ecs_config : dict):
    cluster_name = wukong_ecs_config["cluster_name"]

    assert(len(cluster_name) >= 1 and len(cluster_name) <= 255)

    ecs_client = boto3.client('ecs', region_name = aws_region)

    logger.info("First, creating the ECS Fargate cluster...")

    ecs_client.create_cluster(clusterName = cluster_name, capacityProviders = ["FARGATE", "FARGATE_SPOT"])

    logger.info("Successfully created the ECS Fargate cluster.")
    logger.info("Next, registering a task definition to use as the Fargate Redis nodes...")

    ecs_client.register_task_definition(
        family = 'Wukong',
        executionRoleArn = 'arn:aws:iam::833201972695:role/ecsTaskExecutionRole',
        networkMode = 'awsvpc',
        containerDefinitions = [
            {
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                    "awslogs-group": "/ecs/WukongRedisNode",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": "ecs"
                    }
                },
                "portMappings": [
                    {
                    "hostPort": 6379,
                    "protocol": "tcp",
                    "containerPort": 6379
                    }
                ],
                "cpu": 0,
                "environment": [],
                "mountPoints": [],
                "memoryReservation": 30000,
                "volumesFrom": [],
                "image": "redis",
                "essential": True,
                "name": "Redis"
            }
        ]
    )

    logger.info("Successfully registered the task definition!")


if __name__ == "__main__":
    print("Welcome to the Wukong Interactive Setup. Please note that many of the components created for running Wukong")
    print("cost money (e.g., NAT gateways, elastic IP addresses, etc.). Be aware that your account will begin incurring")
    print("cost once these components are setup.")

    config_file_path = input(PATH_PROMPT)
    
    # If the user entered nothing (i.e., the empty string), then we assume the configuration file is in the same directory as the script.
    if config_file_path == "":
        config_file_path = "./wukong_setup_config.yaml"

    # Open the Wukong Setup Configuration file. 
    with open(config_file_path) as f:
        wukong_setup_config = yaml.load(f, Loader = yaml.FullLoader)
    
    aws_region = wukong_setup_config["aws_region"]

    
    wukong_vpc_config = wukong_setup_config["vpc"]              # VPC configuration.
    wukong_lambda_config = wukong_setup_config["aws_lambda"]    # AWS Lambda configuration.
    wukong_ecs_config = wukong_setup_config["ecs"]                  # AWS Fargate/AWS ECS configuration.

    # Step 1: Create the VPC
    results = create_wukong_vpc(aws_region, wukong_vpc_config)
    vpc_id = results['VpcId']
    #private_subnet_ids = results['PrivateSubnetIds']
    #lambda_security_group_id = results['LambdaSecurityGroupId']

    # Step 2: Create AWS Lambda functions.
    #setup_aws_lambda(aws_region, wukong_lambda_config, vpc_id, private_subnet_ids, lambda_security_group_id)

    # Step 3: Create AWS ECS Cluster.
    setup_aws_fargate(aws_region, wukong_ecs_config)