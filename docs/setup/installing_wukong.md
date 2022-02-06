# Installing Wukong

There are several scripts provided in order to make the installation of Wukong easier.

To setup the AWS Lambda functions, please refer to the AWS SAM ReadMe file located at `Wukong/AWS Lambda Task Executor/SAMREADME.md`. This document describes the process of using Wukong's AWS SAM template to automatically create the required AWS Lambda resources.

The Static Scheduler and KV Store Proxy can be installed by simply cloning this GitHub repository. The Static Scheduler is typically executed in an interactive Python session, while the KV Store Proxy is typically executed as its own Python process.

There is a script provided to provision the necessary AWS infrastructure. This script is located at `Wukong/Static Scheduler/install/aws_setup.py`.
