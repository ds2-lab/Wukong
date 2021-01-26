AWS VPC Deployment & Installation
=================================

Wukong is designed to run atop AWS Lambda and AWS EC2. In order for Wukong to execute properly, there are several AWS components that must be created. These include a Virtual Private Cloud (VPC), an Internet Gateway, a NAT Gateway, various subnets, and an AWS Fargate cluster.

************************
Automatic VPC Deployment
************************

To simplify the deployment process, we provide a setup script that automatically creates the required AWS infrastructure. This script is located in ``Wukong/Static Scheduler/install/aws_setup.py``.

You may run this script in order to create the required AWS components automatically. Alternatively, you may follow the instructions below to create and deploy the necessary AWS infrastructure manually.

*********************
Manual VPC Deployment 
*********************

Users who wish to go through the deployment process manually should follow these instructions. Deploying Wukong manually is more complicated than using the script, but doing so allows users to configure the AWS resources according to their exact needs.

Step 1 - Create the Virtual Private Cloud (VPC)
-----------------------------------------------
A VPC must be configured as EC2 instances are required to run within a VPC.

Go to the VPC Console within AWS and click on “Your VPCs” (from the menu on the left-hand side of the screen). Next, click the blue “Create VPC” button. See Figure 14 for a snapshot. Provide a name tag and an IPv4 CIDR block. It is not necessary to provide an IPv6 CIDR Block. 

For Tenancy, select *Default*. Once you have completed all of the fields, click the blue Create button. You should wait until the VPC has been created before continuing.

Step 2 - Create the Security Group
----------------------------------
Next, you should create a security group for the AWS EC2 and AWS Fargate virtual machines.

From the VPC Console, select Security Groups from the menu on the left-hand side of the screen. Then click the blue Create Security Group button.

You will need to provide a security group name and a description. In the VPC field, select the VPC you created above.

Step 3 - Configure the Inbound & Outbound Rules for the Security Group
----------------------------------------------------------------------

Next, configure the inbound and outbound rules for the security group. This ensures that the different components will be able to communicate with one another (e.g.
AWS Lambda and the KV Store).

Step 4 - Allocate an Elastic IP Address 
---------------------------------------

Next, allocate an Elastic IP Address. From the VPC console, select “Elastic IPs” and click the blue “Allocate new address” button.

Step 5 - Create the Internet Gateway
------------------------------------

From the VPC console, select “Internet Gateways” and click the blue “Create internet gateway” button. Supply a name tag and click “Create”.

Step 6 - Create Subnets for the VPC
-----------------------------------

If you want to allocate more IPv4 CIDR blocks to your VPC, then simply go to the VPC console and select the VPC from the list of VPCs (under the Your VPCs menu option). Click the white Actions button, then select “Edit CIDRs”. At the bottom, you will see a table with a white “Add IPv4 CIDR” button underneath it. Click that button and specify the CIDR block.

You then need to create a subnet for your KV Store and EC2 instances. To create a subnet, first go to the VPC console. Then select “Subnets” from the menu on the left. Finally, click the blue “Create subnet” button.

Step 7 - Modify Route Tables
----------------------------

From the VPC Console, select “Route Tables” and then click the blue “Created route table” button. 

Step 8 - Create a NAT Gateway
-----------------------------

Next, create a NAT Gateway. A NAT Gateway is placed into a public VPC subnet to enable outbound internet traffic from instances in a private subnet. We placed this NAT Gateway in one of the subnets created for the EC2/KV Store.