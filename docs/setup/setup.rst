.. _installing_wukong:

Installing Wukong
=================

In this section, you will learn how to install and deploy Wukong.

As of right now, only AWS Lambda is supported. Support for additional serverless frameworks will be provided in the future.

.. toctree::
   :maxdepth: 2
   
   setup_vpc
   setup_aws_lambda
   
A majority of the required AWS infrastructure can be created using the provided aws_setup.py script in Wukong/Static Scheduler/install/ directory. Please be sure to read through the wukong_setup_config.yaml configuration file located in the same directory prior to running the script. In particular, your public IP address should be added to the configuration file if you'd like SSH to be enabled from your machine to VMs created in the Wukong VPC.
