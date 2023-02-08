# Installing Wukong

There are several scripts provided in order to make the installation of Wukong easier.

To setup the AWS Lambda functions, please refer to the AWS SAM ReadMe file located at `Wukong/AWS Lambda Task Executor/SAMREADME.md`. This document describes the process of using Wukong's AWS SAM template to automatically create the required AWS Lambda resources.

The Static Scheduler and KV Store Proxy can be installed by simply cloning this GitHub repository. The Static Scheduler is typically executed in an interactive Python session, while the KV Store Proxy is typically executed as its own Python process.

A majority of the required AWS infrastructure can be created using the provided `aws_setup.py` script in `Wukong/Static Scheduler/install/` directory. Please be sure to read through the `wukong_setup_config.yaml` configuration file located in the same directory prior to running the script. In particular, your public IP address should be added to the configuration file if you'd like SSH to be enabled from your machine to VMs created in the Wukong VPC.

Wukong was created from Dask Distributed v1.23.1 and Dask v1.2.2. 
