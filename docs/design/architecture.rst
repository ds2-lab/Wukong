Wukong Architecture
===================

Wuking is composed of three primary components:
1. Static Scheduler 
2. Serverless Task Executors 
3. Storage Manager

.. image:: /images/WukongArchitecture.png
   :width: 600

The following sections will describe these components in greater detail. 

================
Static Scheduler
================
The **Static Scheduler** in Wukong serves as a front-end client interface to the framework. Users can submit jobs via the Client Interface. These jobs will be passed to the DAG Generator, which will convert the user-written Python code into an executable DAG. 

The generated DAG will then be handed off to Wukong’s Schedule Generator. The schedule generator performs a series of depth-first searches on the DAG to partition it into a collection of sub-graphs, referred to as **static schedules**. Finally, the static schedules corresponding to so-called “leaf tasks” (i.e., tasks with no data dependencies) are assigned to Serverless Executors by the Initial Task Executor Invokers. Specifically, the static schedules are serialized and included in invocation payloads for the Serverless Executors.

=========================
Serverless Task Executors
=========================

The Serverless Task Executors (often referred to simply as “Executors”) are the workers of Wukong. Each Executor is simply an on-going invocation of the AWS Lambda function. When an Executor begins running, it retrieves its assigned static schedule from the invocation payload or from intermediate storage. (Invocation payloads have a size limit of 256kB, meaning some data may need to be stored in intermediate storage rather than included directly within the invocation payload.) When an Executor runs out of work, it simply terminates, rather than waiting for more work from the Scheduler or fetching more work from an external queue. 

Dependency Tracking
-------------------
Executors communicate with one another through intermediate storage. Each task in the DAG has an associated “dependency counter” maintained within the Metadata Store (MDS), a component of the Storage Manager. Each time an Executor completes a task, the Executor increments the dependency counter of each of the completed task’s dependents. Executors can check whether a task is ready to execute by examining the value of the task’s dependency counter. If the value of the counter is equal to the number of dependencies of the task, then the task is ready to execute. 
To better illustrate this process, consider the following example.

Dependency Tracking Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The diagram below shows a simple DAG containing three tasks: Task A, Task B, and Task C. This DAG will be executed by two Executors: Executor #1 and Executor #2.

Looking at the structure of the DAG, we can see that Task C is dependent on Task A and Task B. As a result, Task C will not be able to execute until both Task A and Task B have been completed.

.. raw:: html

   <embed>
      <body><div class="mxgraph" style="max-width:100%;border:1px solid transparent;" data-mxgraph="{&quot;highlight&quot;:&quot;#0000ff&quot;,&quot;nav&quot;:true,&quot;resize&quot;:true,&quot;toolbar&quot;:&quot;zoom layers lightbox&quot;,&quot;edit&quot;:&quot;_blank&quot;,&quot;xml&quot;:&quot;&lt;mxfile host=\&quot;app.diagrams.net\&quot; modified=\&quot;2021-01-24T17:41:18.616Z\&quot; agent=\&quot;5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36\&quot; etag=\&quot;hvGhjrebzMkWd-HDRFd7\&quot; version=\&quot;14.2.7\&quot;&gt;&lt;diagram id=\&quot;oS73EPcNbp2nGjv1_ACD\&quot; name=\&quot;Page-1\&quot;&gt;5VlNl5owFP01LttDCAFcqjNtF+2ix55+LCN5Ih0kNMYR++ubSEAwOM5MRe3paniXfJB773tJxgGeLIv3guaLT5xBOnAdVgzw3cB1kYND9Ucj2xIhASmBWCTMNNoD0+Q3VD0Nuk4YrFoNJeepTPI2GPEsg0i2MCoE37SbzXnanjWnMVjANKKpjX5LmFyUaEicPf4BknhRzYwc82ZJq8YGWC0o45sGhO8HeCI4l+XTsphAqsmreCn7vTvytv4wAZl8TofR56H46X+dforDKPjwsArvP3pvzCiPNF2bBQ9cP1XjjedcDasIo1H5wv+11l86fg9cxAndA+opNn93HVc5zfRy5TZt99QjvlntFB6pBtjPi+OjzCpgUiFqZbPDVgorp7Pg8vMr2G19kKuIUf5RwXizSCRM83KRG2VhhS3kMlURUo90lZemmicFML2GJE0nPOViNxAGxAgEetFS8AdovBn6AaZ+PfkjCAnFUeVQ7QeVSMCXIMVWNTEdsGcsZHIIVZbaNBxpoEXDjBVGTQ7E9ch7m6gH45QXuMa3GBV8nTHN0J2j1syFXPCYZzT9yHluuPwJUm5NgtO15G2moUjkd939LTHRDzOYfr4rmsG2CjK1mO/N4Ec1mg72nXZR1auUqkpmr5YImFUJDgRSK+ZrYfKhmxlsShQVMchTeWcLLiClMnlsf8fZ5cOnkv7FufuKMlHn8uhEgl8mkxmFcB51ZbIfhTCbnyeT3YNMrlP0Wpkc2IyqNJiaMOOZpva6yY1uJrm9fyG5vZvf0ce3vKPPwwiizjowC4lHnPPUAe/W6kBouea+gEglshjowwZGFuNq8bJNbZsxUzua9BqIpkmcqTBSZIHCx5rKRB25R+bFMmEsPaZluxhpU1TVClVxWYbcM2nlOsFb0lILB7ZayCO2XG5fciH73N7Wy/1/9fJuUK9q4FZVPhAoVkzlHQWJsiEjXQWJgoeU0E+QZi7FdFbN4byUTOIfXD2ITWV9qGlSiYLevO8e3eHq3eQLXT107zJP3ltO7B2HZj08QhIImdelVOjOsP/sy+ATDrJ1alra7dCB9CbD8VuEtak/91qBvK4DxB3koETKIr3eiRJMVyHX+bqb+Nj54H/QvjjQ/XpesA+do37pPtNt7UV0b9ssnmLb641tYrHt9Mr2DDE2d7rYRk6Ah3ARc98M+77Ffr+l5Vw3ktd4fXh1toMLe/1vjlvn9vr12bevhZNe2T/X/9Nf43WEr0738MJmZ04E4HbRTXyCgosW9j7pV+H+d7bdu8avlfj+Dw==&lt;/diagram&gt;&lt;/mxfile&gt;&quot;}"></div>
      <script type="text/javascript" src="https://viewer.diagrams.net/js/viewer-static.min.js"></script>
      </body>
   </embed>

Assume that Executor #1 completes Task A first (i.e., before Executor #2 completes Task B). Executor #1 will next examine the dependents -- also known as the *downstream tasks* -- of Task A. In this case, there is just one downstream task, Task C. Executor #1 will increment the dependency counter of Task C by 1, thereby indicating that one dependency of Task C has resolved (i.e., has been executed). 

After incrementing Task C's dependency counter, Executor #1 will check to see if Task C is ready to execute. It will compare the current value of Task C's dependency counter against the number of data dependencies of Task C. In this case, Task C has 2 dependencies and its dependency counter has the value 1. Because these values are not equal, Executor #1 will determine that Task C cannot be executed yet, and Executor #1 will terminate. The result of this is shown in the diagram below.

.. raw:: html 

   <embed>
      <body><div class="mxgraph" style="max-width:100%;border:1px solid transparent;" data-mxgraph="{&quot;highlight&quot;:&quot;#0000ff&quot;,&quot;nav&quot;:true,&quot;resize&quot;:true,&quot;toolbar&quot;:&quot;zoom layers lightbox&quot;,&quot;edit&quot;:&quot;_blank&quot;,&quot;xml&quot;:&quot;&lt;mxfile host=\&quot;app.diagrams.net\&quot; modified=\&quot;2021-01-24T17:45:50.198Z\&quot; agent=\&quot;5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36\&quot; etag=\&quot;xllabHlDIc32IkOJbXX3\&quot; version=\&quot;14.2.7\&quot;&gt;&lt;diagram id=\&quot;oS73EPcNbp2nGjv1_ACD\&quot; name=\&quot;Page-1\&quot;&gt;5VpNl6I4FP01LrsPIQRw6UdN96J7Mcc5Pd3LCE+kCwkTY4nz6yeRRMFgaVWDOqdXkpcvcu99L3nBAZ6syk+cFsuvLIZs4DpxOcDTgesiB4fyR1l2lYUEpDIkPI11o6Nhlv4Lpqe2btIY1o2GgrFMpEXTGLE8h0g0bJRztm02W7CsOWtBE7AMs4hmtvXvNBbLyhoS52j/DGmyNDMjR9esqGmsDesljdm2ZsJPAzzhjInqaVVOIFPgGVyqfn+cqT28GIdcXNNh9OeQ//S/zb4mYRR8fl6HT1+8D3qUF5pt9IIHrp/J8cYLJoeVgNGoqvD/2ag3HX8CxpOUHg3yKdG/+47rguZquWKXNXuqET+s9wyPZAPsF+X5UebGMDEWubL5aStpq6azzNXrG7PbeCFXAiP1Iwvj7TIVMCuqRW6lhKVtKVaZLCH5SNdFJapFWkKs1pBm2YRljO8HwoBiAoFatODsGWo1Qz/A1D9M/gJcQHmWOXTQg3QkYCsQfCeb6A7Y0xLSPoSMpLY1RWrTsiZGY6PaB5LDyEeZyAetlDeoxrcQ5WyTxwqhqSPXzLhYsoTlNPvCWKGx/AlC7LSD041gTaShTMV31f0j0aUfejD1PC3rhZ0p5HIx3+uFH2Y0VTh22pdMr4oq48zegSKIrUhwQpBcMdtw7Q/tyGAdoihPQFzyO5twDhkV6UvzPTqnD19y+jf77jvCxMGXRxcc/DaeHFMIF1GbJ/tRCPNFN57snnjywUXv5cmBjah0g5ku5ixX0N7XudHDOLf3f3Bu7+F39PEj7+iLMIKoNQ7MQ+IRp5s44D1aHAgt1TyVEElH5gN12MDIQlwuXjShbSKmY0cdXm2iWZrkshhJsEDaxwrKVB65R7pilcZxdo7LZjBSojDRCplyFYbcjrhCQ9I8fRGbK+QRmyy3L7KQfWpvsuX+vmx5TvCxyRcO7s2XGbgRk08ISiRSRUs4ovEwJm3hiIKHJNGvgKZTYjo3czhvBZP4J4lHi/QPR5o6lCjoTfvu2f3tsJf8RdfP7XvMq1nLhZ3jVKynB0gCYey1MRW6c+xfnQq+oiCbp7qk3RYeSG80nM8hrC392qQCeW3HhykUIEnKI7XeiSRMRSHX+baf+Nzp4Hfgvjzh/X5asI+co37h7ihXexPcuyaKl9D2ekObWGg7vaI9R3G8cNrQRk6Ah3ATcT8M+r6Ffr+hpat85D1aH94d7eDGWv+V41bXWr8/+nZSOOkV/a5u09+jdYTvDvfQgtvOujvdRp0IwG2Dm/gEBTcN7PeH3734KezyAfbKL1psVWQgJIu/cPFtJ/TXJ+oc5DvrtFDpomBpLvZ4kvGATNVYG8GqdfWVp7u4KQDcJgCnRQC4NwHYSfnt7sJNTftduH2r3c5IJ7fc5nbp8jV3T/fcsnj8Kr6vq/23AD/9Bw==&lt;/diagram&gt;&lt;/mxfile&gt;&quot;}"></div>
      <script type="text/javascript" src="https://viewer.diagrams.net/js/viewer-static.min.js"></script>
      </body>
   </embed>

Next, assume that Executor #2 completes Task B. Executor #2 will next check for any downstream tasks of Task B and discover Task C. Now Executor #2 will increment the dependency counter of Task C. Prior to this increment operation, the value of Task C's dependency counter is 1. After Executor #2 increments Task C's dependency counter, the value of the counter is 2. Executor #2 compares the value of Task C's dependency counter against the number of dependencies of Task C. 

At this point, Executor #2 will find that the two values are equal (they are both 2). Executor #2 will conclude that Task C is ready to execute. In order to execute Task C, Executor #2 will first retrieve the output of Task A from intermediate storage. Once obtained, Executor #2 will have satisfied all data dependencies of Task C and will proceed to execute Task C.

.. raw:: html

   <embed>
      <body><div class="mxgraph" style="max-width:100%;border:1px solid transparent;" data-mxgraph="{&quot;highlight&quot;:&quot;#0000ff&quot;,&quot;nav&quot;:true,&quot;resize&quot;:true,&quot;toolbar&quot;:&quot;zoom layers lightbox&quot;,&quot;edit&quot;:&quot;_blank&quot;,&quot;xml&quot;:&quot;&lt;mxfile host=\&quot;app.diagrams.net\&quot; modified=\&quot;2021-01-24T17:54:25.245Z\&quot; agent=\&quot;5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36\&quot; etag=\&quot;xNMCTBo2e7AtC3GntJ6y\&quot; version=\&quot;14.2.7\&quot;&gt;&lt;diagram id=\&quot;oS73EPcNbp2nGjv1_ACD\&quot; name=\&quot;Page-1\&quot;&gt;7VrLlps2GH4aL9ODJAR46cs0WSSLHvekyVIGGZPBiMryGPfpKxkJg4UHzwRs96SrQb8uoO/y6+IZodmm+MhJvv7CIpqOoBMVIzQfQQgcFMg/KnIoI9jHZSDmSaQbnQKL5B9qeuroLonottFQMJaKJG8GQ5ZlNBSNGOGc7ZvNVixtvjUnMbUCi5CkdvSvJBLrMhpg5xT/RJN4bd4MHF2zIaaxDmzXJGL7Wgg9jdCMMybKp00xo6kCz+BS9vv9Qm31YZxm4poOkz/G/If3dfElDkL/0/M2ePrsftCjvJB0pyc8gl4qx5uumBxWAkbCssL7e6e+dPqRMh4n5BSQT7H+e+y4zUmmpisOabOnGvHD9sjwRDZAXl5cHmVpAjMTkTNbnreSsfJ1Vrj8fBOGjQ+CEhipH1mY7teJoIu8nOReSljG1mKTyhKQj2Sbl6JaJQWN1BySNJ2xlPHjQIiCCFNfTVpw9kxrNWPPR8SrXv5CuaDFReZApQdpJMo2VPCDbKI7IFdLSHsIGEnta4rUoXVNjCZGtAfiauSTTOSDVsobVONZiHK2yyKF0NyRc2ZcrFnMMpJ+ZizXWP6gQhy0wclOsCbStEjEN9X9N6xL3/Vg6nle1AsHU8jkZL7VC9/NaKpw6nQsmV4lVcbMbkURjaxMcEaQnDHbce2HdmSQTlGEx1R0+c4mnNOUiOSl+R2904e6TP9m774jTVRennQY/DZOjggNVmGbk70woMtVP06GZ06uLHovJ/s2otIGC13MWKagva+5wcOY2/0vmNt9+BV9+sgr+ioIadiaB5YBdrHTTx5wHy0PBJZqngoaSiPzkdpsIGAhLicvmtA2EdO5ow6vDpE0iTNZDCVYVManCspEbrknumKTRFF6ictmMlKiMNkKmHKZhmBPXIExbu6+sM0VcLFNFhyKLGDv2ptswV+XLRc6D8aWGbiRkc/oiSVOeUsyItE4wm3JiFAXSJpfgUwfiMnSvMN5K5TY64ay2tDUoQT+YMqHF1e3aiX5k2yf21eYV88sHevGuVTPt4+YBpHbxlQAl8i7+iD4ioJsnuqShi084MFouHyCsBb0a48UwG3bPMxpTiVJWajmO5OEqRwEna/HF1/aG/wK3BdnvN9PC/aGczIs3D2d1N4E96GJYhfa7mBoYwttZ1C0lyCKVk4b2sDx0ZjeRNwPg75noT9saunrNPIerY/vjrZ/Y63/zHarb63fH337SDgbFP2+7tLfo3WA7g732ILbPib0uow6IaWwDW7sYeDfNLHfH37Y+UNY9wb2yt+z2CZPqZAs/sS1t32cv/6Yzqn8Zn0sVLrIWZKJI554OsJzNdZOsHJeQ53SIWoKALUJwGkRABpMALbbbncTbmrab8LtO+12Rnq54zZ3S92X3He95Yad19z/+7VHv7o39Kssnv4p4lhX+9cS9PQv&lt;/diagram&gt;&lt;/mxfile&gt;&quot;}"></div>
      <script type="text/javascript" src="https://viewer.diagrams.net/js/viewer-static.min.js"></script>
      </body>
   </embed>

Nam erat dolor, porta sit amet ultricies vel, scelerisque at sapien. Quisque eleifend magna at pharetra suscipit. Proin eu pretium nisi. Praesent ante velit, hendrerit vitae sagittis sit amet, ultricies ac dolor. Vivamus pharetra vitae nisl et ornare. Pellentesque tincidunt eleifend accumsan. Sed augue nisl, sagittis ut scelerisque eu, imperdiet quis nisi. Praesent auctor consectetur risus, in lacinia elit consequat ac.

===============
Storage Manager
=============== 

The Storage Manager abstractly defines the intermediate storage of Wukong. It is composed of a Metadata Store (MDS) and an Intermediate KV Store (KVS). The MDS is simply a single instance of Redis running on an AWS EC2 virtual machine. The KVS is an AWS Fargate cluster in which each AWS Fargate node is running a Redis server.

The Metadata Store (MDS)
------------------------

The MDS is simply a Redis server running on an AWS EC2 virtual machine. Typically, the MDS will be running on a separate EC2 VM from the Static Scheduler. There is also a KVS Proxy running on the MDS virtual machine. The KVS Proxy aids in invoking downstream tasks and storing and transferring intermediate data. Typically, the KVS Proxy is only utilized in cases where many downstream tasks need to be invoked all at once. The higher network bandwidth of the KVS Proxy is beneficial in transferring intermediate data to newly-invoked Executors.

The Static Scheduler stores static schedules and dependency counters in the MDS at the very beginning of a workload. Executors will fetch static schedules and increment/retrieve dependency counters during a workload's execution. Additionally, the final result(s) of a workload will be stored within the MDS rather than the KVS.

The Intermediate Key-Value Store (KVS)
--------------------------------------

The KVS consists of an AWS Fargate cluster. The Wukong static scheduler will scale-up the AWS Fargate cluster according to the user's specification. Each virtual machine within the AWS Fargate cluster will be configured with 4 CPU cores and 32GB of RAM (though users could modify these values if they desire). Each of the AWS Fargate nodes houses an active Redis server. Intermediate data is stored in these servers during a workload's execution. Data is consistently hashed across the entire AWS Fargate cluster in order to ensure a relatively balanced distribution of data.