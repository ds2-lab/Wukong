import argparse 
import boto3
import botocore 
import json
import logging 
import yaml
import time 
import os 
from requests import get

os.system("color")

# Set up logging.
# import logging 
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# ch.setFormatter(formatter)

# logger.addHandler(ch)

PATH_PROMPT = "Please enter the path to the Wukong Setup Configuration File. Enter nothing for default (same directory as this script).\n> "

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_error(msg, no_header = False, no_color = False):
    if not no_color and not no_header: 
        msg = bcolors.FAIL + "[ERROR] " + msg + bcolors.ENDC
    elif not no_color and no_header:
        msg = bcolors.FAIL + msg + bcolors.ENDC
    elif no_color and not no_header: 
        msg = "[ERROR] " + msg
    print(msg)

def print_warning(msg, no_header = False, no_color = False):
    if not no_color and not no_header: 
        msg = bcolors.WARNING + "[WARNING] " + msg + bcolors.ENDC
    elif not no_color and no_header:
        msg = bcolors.WARNING + msg + bcolors.ENDC
    elif no_color and not no_header: 
        msg = "[WARNING] " + msg
    print(msg)

def get_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c", "--config", dest = 'config_file_path', type = str, default = None, help = "The path to the configuration file. If nothing is passed, then the user will be explicitly prompted for the configuration path once this script begins executing.")
    parser.add_argument("-p", "--aws-profile", dest = 'aws_profile', default = None, type = str, help = "The AWS credentials profile to use when creating the resources. If nothing is specified, then this script will ultimately use the default AWS credentials profile.")
    
    parser.add_argument("--skip-vpc", dest = "skip_vpc_creation", action = 'store_true', help = "If passed, then skip the VPC creation step. Note that skipping this step may require additional configuration. See the comments in the provided `wukong_setup_config.yaml` for further information.")
    parser.add_argument("--skip-lambda", dest = "skip_aws_lambda_creation", action = 'store_true', help = "If passed, then skip the creation of the AWS Lambda function(s).")
    
    parser.add_argument("--no-color", dest = "no_color", action = 'store_true', help = "If passed, then no color will be used when printing messages to the terminal.")
    
    return parser.parse_args()

def create_wukong_vpc(aws_region : str, user_ip: str, wukong_vpc_config : dict, aws_profile = None, no_color = False):
    """
    This function first creates a Virtual Private Cloud (VPC). 
    Next, it creates an Internet Gateway, allocates an Elastic IP Address, and creates a NAT Gateway.

    Arguments:
    ----------
        aws_region (string)

        wukong_vpc_config (dict)
        
    Keyword Arguments:
    ------------------
        aws_profile (str):
            The AWS credentials profile to use when creating the resources. 
            If None, then this script will ultimately use the default AWS credentials profile.

        no_color (bool):
            If True, then print messages will not contain color. Note that colored prints are
            only supported when running on Linux.
        
    Returns:
    --------
        dict {
            "VpcId": The VPC ID of the newly-created VPC.
            "LambdaSecurityGroupId": The ID of the security group created for the AWS Lambda functions.
            "PrivateSubnetIds": The ID's of the newly-created private subnets (used for AWS Lambda functions).
        }
    """
    if aws_profile is not None:
        print("Attempting to create AWS Session using explicitly-specified credentials profile \"%s\" now..." % aws_profile)
        try:
            session = boto3.Session(profile_name = aws_profile)
            print("Success!")
        except Exception as ex: 
            print_error("Exception encountered while trying to use AWS credentials profile \"%s\"." % aws_profile, no_color = no_color, no_header = False)
            raise ex 
        
        ec2_resource = session.resource('ec2', region_name = aws_region)
        ec2_client = session.client('ec2', region_name = aws_region)
    else:
        ec2_resource = boto3.resource('ec2', region_name = aws_region)
        ec2_client = boto3.client('ec2', region_name = aws_region)

    CidrBlock = wukong_vpc_config["CidrBlock"]
    PublicSubnetCidrBlock = wukong_vpc_config["PublicSubnetCidrBlock"]
    PrivateSubnetCidrBlocks = wukong_vpc_config["PrivateSubnetCidrBlocks"]
    #lambda_security_group_name = wukong_vpc_config["lambda_security_group_name"]
    serverful_security_group_name = wukong_vpc_config["serverful_security_group_name"]

    # Validate parameters/arguments.
    #assert(len(lambda_security_group_name) >= 1 and len(lambda_security_group_name) <= 64)
    assert(len(serverful_security_group_name) >= 1 and len(serverful_security_group_name) <= 64)

    print("Creating VPC now...")

    # Create the VPC.
    create_vpc_response = ec2_client.create_vpc(
        CidrBlock = CidrBlock, 
        TagSpecifications = [{
            'ResourceType': 'vpc',
            'Tags': [{
                'Key': 'Name',
                'Value': wukong_vpc_config['Name']
            }]
        }])
    vpc = ec2_resource.Vpc(create_vpc_response["Vpc"]["VpcId"])
    vpc.wait_until_available()

    print("Successfully created a VPC. VPC ID: " + vpc.id)
    print("Next, creating the public subnet...")

    # Create the public subnet (used by EC2 and AWS Fargate).
    public_subnet = vpc.create_subnet(
        CidrBlock = PublicSubnetCidrBlock,
        TagSpecifications = [{
            'ResourceType': 'subnet',
            'Tags': [{
                'Key': 'Name',
                'Value': "WukongPublicSubnet"
            }]}])
    ec2_client.modify_subnet_attribute(SubnetId = public_subnet.id, MapPublicIpOnLaunch = {'Value': True})

    print("Successfully created the public subnet. Subnet ID: " + public_subnet.id)
    
    # We print a different message depending on how many private subnets we're creating.
    if len(PrivateSubnetCidrBlocks) == 1:
        print("Next, creating 1 private subnet...")
    else:
        print("Next, creating {} private subnets...".format(len(PrivateSubnetCidrBlocks))) 
    
    # Create all of the private subnets.
    private_subnets = list()
    for i, PrivateSubnetCidrBlock in enumerate(PrivateSubnetCidrBlocks):
        print("Creating private subnet #%d with CIDR block %s..." % (i, PrivateSubnetCidrBlock))
        private_subnet = vpc.create_subnet(
            CidrBlock = PrivateSubnetCidrBlock,
            TagSpecifications = [{
            'ResourceType': 'subnet',
            'Tags': [{
                'Key': 'Name',
                'Value': "WukongPrivateSubnet" + str(i)
            }]
        }])
        private_subnets.append(private_subnet)

    # We print a different message depending on how many private subnets we created.
    if len(private_subnets) == 1:
        private_subnet = private_subnets[0]
        print("Successfully created 1 private subnet. Subnet ID: " + private_subnet.id)
    else:
        private_subnet_ids = [private_subnet.id for private_subnet in private_subnets]
        print("Successfully created {} private subnets. Subnet IDs: " + str(private_subnet_ids))
    print("Next, creating an internet gateway...")
    
    # Create and attach an internet gateway.
    create_internet_gateway_response = ec2_client.create_internet_gateway(
        TagSpecifications = [{
            'ResourceType': 'internet-gateway',
            'Tags': [{
                'Key': 'Name',
                'Value': "WukongInternetGateway"
            }]
        }])
    internet_gateway_id = create_internet_gateway_response["InternetGateway"]["InternetGatewayId"]
    vpc.attach_internet_gateway(InternetGatewayId = internet_gateway_id)

    print("Successfully created an Internet Gateway and attached it to the VPC. Internet Gateway ID: " + internet_gateway_id)
    print("Next, allocating elastic IP address and creating NAT gateway...")

    elastic_ip_response = ec2_client.allocate_address(
        Domain = 'vpc',
        TagSpecifications = [{
            'ResourceType': 'elastic-ip',
            'Tags': [{
                'Key': 'Name',
                'Value': "wuking-nat-gateway-ip"
            }]
        }])
    nat_gateway = ec2_client.create_nat_gateway(
        SubnetId = public_subnet.id, 
        AllocationId = elastic_ip_response['AllocationId'], 
        TagSpecifications = [{
            'ResourceType': 'natgateway',
            'Tags': [{
                'Key': 'Name',
                'Value': "WukongNatGateway"
            }]
        }])
    
    # print(nat_gateway)
    nat_gateway_id = nat_gateway["NatGateway"]["NatGatewayId"]

    print("Successfully allocated elastic IP address and created NAT gateway. NAT Gateway ID: " + nat_gateway_id)
    print("Next, creating route tables and associated public route table with public subnet.")
    print("But first, sleeping for ~30 seconds so that NAT gateway can be created.")

    time.sleep(30)

    # The VPC creates a route table, so we have one to begin with. We use this as the public route table.
    initial_route_table = list(vpc.route_tables.all())[0] 
    initial_route_table.create_route(
        DestinationCidrBlock = '0.0.0.0/0',
        GatewayId = internet_gateway_id
    )
    initial_route_table.associate_with_subnet(SubnetId = public_subnet.id)

    # Now create the private route table.
    private_route_table = vpc.create_route_table(
        TagSpecifications = [{
            'ResourceType': 'route-table',
            'Tags': [{
                'Key': 'Name',
                'Value': "WukongPrivateRouteTable"
            }]
        }])
    private_route_table.create_route(
        DestinationCidrBlock = '0.0.0.0/0',
        GatewayId = nat_gateway_id
    )

    print("Successfully created the route tables and associated public route table with public subnet.")

    # We print a different message depending on how many private subnets we created.
    if len(private_subnets) == 1:
        print("Next, associating private route table with the private subnet.")
    else:
        print("Next, associating private route table with the private subnets.")
    
    # # Associate the private route table with each private subnet.
    for private_subnet in private_subnets:
        private_route_table.associate_with_subnet(SubnetId = private_subnet.id)
    
    # We print a different message depending on how many private subnets we created.
    if len(private_subnets) == 1:
        print("Successfully associated the private route table with the private subnet.")
    else:
        print("Successfully associated the private route table with the private subnets.")
    print("Next, creating and configuring the security group...")

    # The security group used by AWS Lambda functions.
    #lambda_security_group = ec2_resource.create_security_group(
    #   GroupName = lambda_security_group_name, VpcId = vpc.id)

    # The security group used by AWS EC2 and AWS Fargate instances/nodes.
    serverful_security_group = ec2_resource.create_security_group(
        Description='Wukong Security Group', GroupName = serverful_security_group_name, VpcId = vpc.id,
        TagSpecifications = [{
            "ResourceType": "security-group",
            "Tags": [
                {"Key": "Name", "Value": serverful_security_group_name}
            ]
        }])
    
    #lambda_security_group.authorize_ingress(CidrIp = '0.0.0.0/0', IpProtocol = '-1', FromPort = 0, ToPort = 65535)
    #serverful_security_group.authorize_ingress(CidrIp = '0.0.0.0/0', IpProtocol = '-1', FromPort = 0, ToPort = 65535)
    serverful_security_group.authorize_ingress(IpPermissions = [
        {
        # All traffic that originates from within the security group itself.
        "FromPort": 0,
        "ToPort": 65535,
        "IpProtocol": "-1",
        "UserIdGroupPairs": [{
            "GroupId": serverful_security_group.id,
            "VpcId": vpc.id}]
        },
        {
        # SSH traffic from your machine's IP address. 
        "FromPort": 22,
        "ToPort": 22,
        "IpProtocol": "tcp",
        "IpRanges": [
            {"CidrIp": user_ip + "/32", "Description": "SSH from my PC"}]
        }
    ])
    #serverful_security_group.authorize_ingress(CidrIp = '0.0.0.0/0', IpProtocol = '-1', FromPort = 0, ToPort = 65535)

    print("Successfully created and configured the security group.\n\n")
    print("==========================")
    print("Wukong VPC setup complete.")
    print("==========================")

    return {
        "VpcId": vpc.id,
        "LambdaSecurityGroupId": serverful_security_group.id,
        "PrivateSubnetIds": [private_subnet.id for private_subnet in private_subnets]
    }

def setup_aws_lambda(aws_region : str, wukong_lambda_config : dict, private_subnet_ids : list, lambda_security_group_id : str, aws_profile = None, no_color = False):
    """
    Create the two AWS Lambda functions required by Wukong.

    Arguments:
    ----------
        aws_region (str)

        wukong_lambda_config (dict)

        private_subnet_ids (list)

        lambda_security_group_id (str)
    
    Keyword Arguments:
    ------------------
        aws_profile (str):
            The AWS credentials profile to use when creating the resources. 
            If None, then this script will ultimately use the default AWS credentials profile.
        
        no_color (bool):
            If True, then print messages will not contain color. Note that colored prints are
            only supported when running on Linux.
    
    """
    print("Next, we must create the AWS Lambda function that acts as the Wukong Executor.")
    
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
    
    if aws_profile is not None:
        print("Attempting to create AWS Session using explicitly-specified credentials profile \"%s\" now..." % aws_profile)
        try:
            session = boto3.Session(profile_name = aws_profile)
            print("Success!")
        except Exception as ex: 
            print_error("Exception encountered while trying to use AWS credentials profile \"%s\"." % aws_profile, no_color = no_color, no_header = False)
            raise ex 
        
        lambda_client = session.client("lambda", region_name = aws_region)
        iam = session.client('iam')
    else:
        lambda_client = boto3.client("lambda", region_name = aws_region)
        iam = boto3.client('iam')

    print("First, we need to create an IAM role for the AWS Lambda functions. Creating role now...")
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

    try:
        role_response = iam.create_role(
            RoleName = iam_role_name, Description = description, AssumeRolePolicyDocument = json.dumps(AssumeRolePolicyDocument)) 
    except botocore.errorfactory.EntityAlreadyExistsException:
        print_warning("Exception encountered when creating IAM role for the AWS Lambda functions: `botocore.errorfactory.EntityAlreadyExistsException`", no_color = no_color, no_header = False)
        print_warning("Attempting to fetch ARN of existing role with name \"%s\" now..." % iam_role_name, no_color = no_color, no_header = True)
        
        try:
            role_response = iam.get_role(RoleName = iam_role_name)
        except botocore.errorfactory.NoSuchEntityException as ex:
            # This really shouldn't happen, as we tried to create the role and were told that the role exists.
            # So, we'll just terminate the script here. The user needs to figure out what's going on at this point. 
            print_error("Exception encountered while attempting to fetch existing IAM role with name \"%s\": `botocore.errorfactory.NoSuchEntityException`" % iam_role_name, no_color = no_color, no_header = False)
            print_error("Please verify that the AWS role exists and re-execute the script. Terminating now.", no_color = no_color, no_header = True)
            exit(1) 
        
    role_arn = role_response['Role']['Arn']
    print("Success! Next, attaching required IAM role polices.")
    
    # Now we must attach all of the required policies.
    iam.attach_role_policy(
        PolicyArn = 'arn:aws:iam::aws:policy/AWSLambda_FullAccess',
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

    print("Successfully created and configured IAM role for AWS Lambda functions.")
    print("Next, creating AWS Lambda function for Wukong Executor...")
    lambda_client.create_function(
        Code = {"ZipFile": open("./wukong_aws_lambda_code.zip", "rb").read()},
        FunctionName = executor_function_name,
        Runtime = 'python3.7',
        Role = role_arn,
        Handler = 'function.lambda_handler',
        MemorySize = function_memory_mb,
        Timeout = function_timeout_seconds,
        VpcConfig = {
            'SubnetIds': private_subnet_ids,
            'SecurityGroupIds': [lambda_security_group_id]
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


# def setup_aws_fargate(aws_region : str, wukong_ecs_config : dict):
#     cluster_name = wukong_ecs_config["cluster_name"]

#     assert(len(cluster_name) >= 1 and len(cluster_name) <= 255)

#     ecs_client = boto3.client('ecs', region_name = aws_region)

#     print("First, creating the ECS Fargate cluster...")

#     ecs_client.create_cluster(clusterName = cluster_name, capacityProviders = ["FARGATE", "FARGATE_SPOT"])

#     print("Successfully created the ECS Fargate cluster.")
#     print("Next, registering a task definition to use as the Fargate Redis nodes...")

#     ecs_client.register_task_definition(
#         family = 'Wukong',
#         executionRoleArn = 'arn:aws:iam::833201972695:role/ecsTaskExecutionRole',
#         networkMode = 'awsvpc',
#         containerDefinitions = [
#             {
#                 "logConfiguration": {
#                     "logDriver": "awslogs",
#                     "options": {
#                     "awslogs-group": "/ecs/WukongRedisNode",
#                     "awslogs-region": "us-east-1",
#                     "awslogs-stream-prefix": "ecs"
#                     }
#                 },
#                 "portMappings": [
#                     {
#                     "hostPort": 6379,
#                     "protocol": "tcp",
#                     "containerPort": 6379
#                     }
#                 ],
#                 "cpu": 0,
#                 "environment": [],
#                 "mountPoints": [],
#                 "memoryReservation": 30000,
#                 "volumesFrom": [],
#                 "image": "redis",
#                 "essential": True,
#                 "name": "Redis"
#             }
#         ]
#     )

#     print("Successfully registered the task definition!")

if __name__ == "__main__":
    print("Welcome to the Wukong Interactive Setup. Please note that many of the components created for running Wukong")
    print("cost money (e.g., NAT gateways, elastic IP addresses, etc.). Be aware that your account will begin incurring")
    print("cost once these components are setup.")
    
    command_line_args = get_arguments()
    no_color = command_line_args.no_color
    aws_profile_name = command_line_args.aws_profile

    config_file_path = command_line_args.config_file_path
    if not config_file_path:
        config_file_path = input(PATH_PROMPT)
    
    # If the user entered nothing (i.e., the empty string), then we assume
    # the configuration file is in the same directory as the script.
    if config_file_path == "" or config_file_path is None:
        config_file_path = "./wukong_setup_config.yaml"

    # Open the Wukong Setup Configuration file. 
    try:
        with open(config_file_path) as f:
            wukong_setup_config = yaml.load(f, Loader = yaml.FullLoader)
            print("Successfully loaded configuration file at \"%s\"" % config_file_path)
    except FileNotFoundError as ex:
        print("Failed to load configuration file \"%s\". Please verify that the file exists." % config_file_path)
        print("The actual exception that was encountered:\n", ex)
    except yaml.YAMLError as ex:
        print("Error parsing configuration file \"%s\". Please verify that the file does not contain any YAML errors." % config_file_path)
        print("The actual exception that was encountered:\n", ex)
    
    aws_region = wukong_setup_config["aws_region"]
    user_public_ip = wukong_setup_config["user_public_ip"]
    
    if user_public_ip == "DEFAULT_VALUE":
        user_public_ip = get('https://api.ipify.org').content.decode('utf8')
    
    wukong_vpc_config = wukong_setup_config["vpc"]              # VPC configuration.
    wukong_lambda_config = wukong_setup_config["aws_lambda"]    # AWS Lambda configuration.
    wukong_ecs_config = wukong_setup_config["ecs"]                  # AWS Fargate/AWS ECS configuration.
    
    # Step 1: Create the VPC
    if not command_line_args.skip_vpc_creation:
        results = create_wukong_vpc(aws_region, user_public_ip, wukong_vpc_config, aws_profile = aws_profile_name, no_color = no_color)
        private_subnet_ids = results['PrivateSubnetIds']
        lambda_security_group_id = results['LambdaSecurityGroupId']
    else:
        print("Skipping the VPC-creation step.")
        # If we skip the creation of the VPC, then we need to obtain the private_subnet_ids
        # and lambda_security_group_id from the configuration file.
        lambda_security_group_id = wukong_vpc_config["lambda_security_group_name"] 
        
        # If the private subnet IDs were not explicitly specified in the configuration file,
        # then we will attempt to retrieve them from AWS by examining the route tables within the VPC.
        # Specifically, we will look for route tables routing a subnet to a NAT Gateway, as these are
        # the private subnets. Public subnets will have a route to an Internet Gateway.
        if "private_subnet_ids" not in wukong_vpc_config or len(wukong_vpc_config["vpc_private_subnet_ids"]) == 0:
            print("Attempting to automatically retrieve private subnet IDs now...")
            
            if aws_profile_name is not None:
                print("Attempting to create AWS Session using explicitly-specified credentials profile \"%s\" now..." % aws_profile_name)
                try:
                    session = boto3.Session(profile_name = aws_profile_name)
                    print("Success!")
                except Exception as ex: 
                    print_error("Exception encountered while trying to use AWS credentials profile \"%s\"." % aws_profile_name, no_color = no_color, no_header = False)
                    raise ex                 
                
                ec2 = session.resource('ec2')
            else:
                ec2 = boto3.resource('ec2')
            
            vpc_name = wukong_vpc_config['Name'] # Grab the VPC name from the config. Hopefully it already exists... 
            try:
                # First, we need to retrieve the VPC ID.
                vpc_response = ec2.vpcs.filter(Filters = [{'Name': 'tag:Name', 'Values': [vpc_name]}])
            except Exception as ex:
                print_error("Exception encountered while attempting to retrieve VPC with name \"%s\"." % vpc_name, no_color = no_color, no_header = False)
                raise ex
            
            if len(vpc_response) == 0:
                raise ValueError("Failed to find existing VPC with name \"%s\". The VPC must already exist if you are going to skip the VPC-creation step." % vpc_name)
            
            vpc_id = vpc_response[0].id
            
            # Now we can describe the route tables within the VPC.
            # Note that we are assuming that the number of route tables is less than the maximum
            # that can be returned by a single query to `ec2.describe_route_tables()`. If there are
            # more route tables than this, then pagination would be required to retrieve them all.
            # This script does not support pagination at this time. 
            route_tables_response = ec2.route_tables.filter(Filters = [
                {
                    'Name': 'vpc-id',
                    'Values': [
                        vpc_id
                    ]
                }
            ])
            
            # Attempt to automatically retrieve the private subnet IDs by examining the routes.
            # The 'GatewayID' attribute is: "The ID of an internet gateway or virtual private gateway attached to your VPC."
            private_subnet_ids = []
            for route_table in list(route_tables_response):
                for route_attr in route_table.routes_attribute:
                    if route_attr.get('DestinationCidrBlock') == '0.0.0.0/0' and route_attr.get('GatewayId') is None:
                        no_nat_gateway = False 
                        # Verify that there's a route to a NAT Gateway. If there's not, then we'll still
                        # count the subnet as a private subnet, but it may cause issues down the line, as
                        # without a NAT Gateway, the subnet will not have external internet access... 
                        if route_attr.get('NatGatewayId') is None:
                            no_nat_gateway = True 
                            print_warning("\nDiscovered one or more subnets in VPC \"%s\" with no configured route(s) to an Internet Gateway or Virtual Private Gateway, but also no route to a NAT Gateway...", no_color = no_color, no_header = False)
                        
                        # If `no_nat_gateway` is True, then we'll note the IDs of the potentially problematic subnets 
                        # and report them to the user. Again, they are problematic because these subnets have no configured 
                        # route to a NAT Gateway and thus will have no external internet access, which is required by Wukong.
                        problematic_subnet_ids = []   
                        for route_association_attr in route_table.associations_attribute:
                            if route_association_attr.get('SubnetId') is not None:
                                private_subnet_ids.append(route_association_attr.get('SubnetId'))
                                
                                if no_nat_gateway:
                                    problematic_subnet_ids.append(route_association_attr.get('SubnetId'))

                        # Note that the value of `len(problematic_subnet_ids) > 0` will only be greater than 0 when `no_nat_gateway` is True.
                        if len(problematic_subnet_ids) > 0:
                            if len(problematic_subnet_ids == 1):
                                print_warning("Specifically, there is 1 private subnet with no configured route to a NAT Gateway. This subnet's ID is: %s" % problematic_subnet_ids[0], no_color = no_color, no_header = True)
                                print_warning("Using this subnet with Wukong will likely cause problems, as a NAT Gateway is required for a private subnet to have external internet access (which is needed by Wukong).", no_color = no_color, no_header = True)
                                print_warning("It is HIGHLY recommended that you explicitly configure a route targeting a NAT Gateway for these subnets.\n", no_color = no_color, no_header = True)
                            else:
                                print_warning("Specifically, there are %d private subnets with no configured route(s) to a NAT Gateway." % len(problematic_subnet_ids), no_color = no_color, no_header = True)
                                print_warning("The IDs of these subnets are: %s" % problematic_subnet_ids, no_color = no_color, no_header = True) 
                                print_warning("Using these subnet with Wukong will likely cause problems, as a NAT Gateway is required for a private subnet to have external internet access (which is needed by Wukong).", no_color = no_color, no_header = True)
                                print_warning("It is HIGHLY recommended that you explicitly configure a route targeting a NAT Gateway for these subnets.\n", no_color = no_color, no_header = True)
            
            if len(private_subnet_ids) == 0:
                print_error("Failed to find any private subnets in the VPC with name=\"%s\"." % vpc_name, no_color = no_color, no_header = False)
                print_error("Terminating now.", no_color = no_color, no_header = True)
                exit(1)
            else:
                print("Successfully retrieved private subnet IDs from AWS: %s" % private_subnet_ids)
        else:
            private_subnet_ids = wukong_vpc_config["vpc_private_subnet_ids"] 

    # Step 2: Create AWS Lambda functions.
    if not command_line_args.skip_aws_lambda_creation:
        setup_aws_lambda(aws_region, wukong_lambda_config, private_subnet_ids, lambda_security_group_id, aws_profile = aws_profile_name, no_color = no_color)
    else:
        print("Skipping the creation of the AWS Lambda function(s).")

    # Step 3: Create AWS ECS Cluster.
    # setup_aws_fargate(aws_region, wukong_ecs_config)