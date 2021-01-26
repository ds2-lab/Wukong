.. Wukong documentation master file, created by
   sphinx-quickstart on Fri Jan 22 12:33:03 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

================

Wukong Documentation
==================================

.. toctree::
   :maxdepth: 2
   
   setup/setup
   
   design/design

   examples/examples

Introduction
==================

Wukong is a serverless-oriented, locality-aware DAG scheduler built atop AWS Lambda and AWS EC2. Wukong utilizes the programming model of Dask & Dask Distributed to convert user-written Python code into an executable directed acyclic graph (DAG). These DAGs are then executed on AWS Lambda in order to provide a fast, cost-effective, and easy-to-use DAG execution engine.

Getting Started
---------------

Wukong's serverless design enables the framework to be both easy to deploy and easy to use. Visit :ref:`installing_wukong` to learn how to deploy Wukong.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
