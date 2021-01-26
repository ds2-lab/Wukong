AWS Lambda Setup Guide
======================

Wukong uses two AWS Lambda functions to execute workloads. One function is simply used to rapidly scale-up, and we refer to this as a Wukong Invoker Function. The other function is the actual Wukong Serverless Executor, often referred to simply as an Executor.

Just as with the AWS VPC setup, users may choose between using an automated and manual setup. The automated setup for AWS Lambda involves using an AWS Serverless Application Model (SAM) template, which automatically configures and deploys the AWS Lambda functions.

Alternatively, users can opt to manually create and deploy the AWS Lambda functions. This may enable users to configure the Lambda functions more precisely.

*******************************
Automatic AWS Lambda Deployment
*******************************

The files required to deploy the AWS Lambda functions via AWS SAM template can be found in the ``/Wukong/AWS Lambda Task Executor/`` directory. Much of the following information can be found in the ``SAMREADME.md`` file.

****************************
Manual AWS Lambda Deployment 
****************************

This section will explain the steps required to deploy the AWS Lambda functions manually. We will first create an AWS Lambda function for the Serverless Executor. After that, we will create an AWS Lambda function for the Serverless Invoker.

Step 1 - Clone the Source Code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, you should ensure you have cloned the source code from the GitHub repository.::

    git clone https://github.com/mason-leap-lab/Wukong/tree/socc2020

Next, you will want to create a ZIP of all the source files in ``Wukong/AWS Lambda Task Executor/TaskExecutor/``. You may name this zip file whatever you want; for example, ``deployment.zip``.

Step 2 - Create an AWS Lambda Function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Next, we need to create an AWS Lambda function. This can be done in several ways. In this case, we will be using the AWS Lambda web console. Navigate to the AWS Lambda console and click the "Create function" button (pictured below).

.. image:: /images/lambda_console_create.png
   :width: 600

You will be shown a creation screen through which you may specify various information about the function. 

For the name, you should use ``WukongExecutor``. If you use a different name, then you will have to modify the ``executor_function_name`` property in the configuration file for the Static Scheduler so that it knows which function to use. This file is called ``wukong-config.yaml`` and can be found in the ``Wukong/Static Scheduler/`` directory.

For the Runtime, select ``Python 3.8``. 

.. image:: /images/lambda_basic_info.png
   :width: 600

Create an IAM Role
""""""""""""""""""

Under Permissions, select ``Change default execution role``. Some additional options will be displayed. Click the hyperlink "IAM Console"; this should open the IAM role creation page in a new browser tab.

.. image:: /images/iam_role_create.png
   :width: 600

From here, you should select the following three polices:
    * ``arn:aws:iam::aws:policy/AWSLambdaFullAccess``
    * ``arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess``
    * ``arn:aws:iam::aws:policy/AmazonS3FullAccess``

You can use the search functionality to quickly locate these polices in the list. Once you've selected these three polices, you can click the blue "Next: Tags" button in the lower-right, and then immediately the "Next: Review" button.

For ``Role name``, you may specify whatever you want -- for example, ``wukong-role``. Once you've typed in a name, click the "Create role".

Return to the AWS Lambda tab. Click the "reload" button to the right of the "Existing role" drop-down menu. Then find the newly-created IAM role in the list and select the role. 

Once selected, click the orange "Create function" button in the lower right. (You may need to scroll down a bit first in order to see the button.)

Add the Required Lambda Layers
""""""""""""""""""""""""""""""

Next, you will need to add four layers to the function. AWS Lambda Layers are basically archives that may cointain libraries, custom runtimes, or other required dependencies. Layers are useful as they allow users to include additional libraries in their function without needing to include the libraries in the deployment package.

Scroll down to the "Layers" section and click the "Add a layer" button. Select "Specify an ARN". Below is a list of layer ARN's. You should repeat these steps, specifying each of the ARN's found in the list.

    1. ``arn:aws:lambda:us-east-1:668099181075:layer:AWSLambda-Python37-SciPy1x:2``
    2. ``arn:aws:lambda:us-east-1:561589293384:layer:DaskDependenciesAndXRay:6``
    3. ``arn:aws:lambda:us-east-1:561589293384:layer:DaskLayer2:2``
    4. ``arn:aws:lambda:us-east-1:561589293384:layer:dask-ml-layer:9``

The first layer contains ``Numpy`` and ``Scipy``, two Python modules required by the Wukong Executor. The next layer contains the Python dependencies of Dask along with the AWS X-Ray API, which is used for debugging and metadata. The third layer contains ``Dask`` itself, and the last layer contains ``Dask-ML`` and its dependencies.

.. image:: /images/lambda_add_layer.png
   :width: 600

General Configuration
"""""""""""""""""""""

Once you have added the Lambda Layers to the function, you should modify the "General configuration" of the function. This includes the function's memory (RAM) and Timeout (i.e., how long the function can execute for). To change these values, select the "Configuration" tab. Then select "General configuration" from the list of buttons on the left. Finally, click the "Edit" button.

.. image:: /images/lambda_configure.png
   :width: 600

You will be presented with a "Basic settings" menu through which you may modify the amount of RAM that gets allocated to the function as well as the function's timeout. 

.. attention:: If you are not sure what values to specify for ``Memory (MB)`` or ``Timeout``, we recommend at least 1,024 MB and 30 seconds. 

.. warning:: The amount billed for executing an AWS Lambda function is dependent on memory. Increasing the amount of memory allocated to your function may make it more expensive to run. 

Uploading the Deployment Package
""""""""""""""""""""""""""""""""

The last step is to upload the deployment package, which contains the source code for your AWS Lambda function. 

First, select the "Code" tab (from the same section that had the "Configuration" tab). On the right, you will see an "Upload from" button. Click this, and then select ".zip file". 

Use the upload dialog to upload the .ZIP file you created earlier. 

Next, scroll down to the "Runtime settings" section. Select the "Edit" button on the right. You just need to modify the ``Handler`` field.

Replace whatever is currently there with ``function.lambda_handler`` and click "Save".

Congratulations! You have successfully deployed the Wukong Serverless Executor.