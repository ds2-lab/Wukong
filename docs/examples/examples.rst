============
Example Code
============

Initialization Code
===================

Everytime you run a job on Wukong, you'll need to create an instance of the ``LocalCluster`` class as well as an instance of the ``Client`` class.

.. code-block:: python 
    :linenos:

    import dask.array as da
    from distributed import LocalCluster, Client
    local_cluster = LocalCluster(host='0.0.0.0:8786',
                    proxy_address = '3.83.198.204', 
                    num_fargate_nodes = 10) 
    client = Client(local_cluster)

In all of the following examples, the code given assumes you've created a local cluster and client object first.

Linear Algebra
==============

Wukong supports many popular linear algebra operations such as Singular Value Decomposition (SVD) and TSQR (Tall-and-Skinny QR Reduction).

Singular Value Decomposition (SVD)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Tall-and-Skinny Matrix
""""""""""""""""""""""

Here, we are computing the SVD of a 200,000 x 100 matrix. In this case, we partition the original matrix into chunks of size 10,000 x 100.

.. code-block:: python 
    :linenos:

    X = da.random.random((200000, 100), chunks=(10000, 100)).persist()
    u, s, v = da.linalg.svd(X)
    v.compute()  

Square Matrix
"""""""""""""

We can also compute the SVD of a square matrix -- in this case, the input matrix is size 10,000 x 10,000. We partition this input matrix into chunks of size 2,000 x 2,000 in this example.

.. code-block:: python 
    :linenos:

    X = da.random.random((10000, 10000), chunks=(2000, 2000)).persist()
    u, s, v = da.linalg.svd_compressed(X, k=5)
    v.compute()  

QR Reduction
^^^^^^^^^^^^

.. code-block:: python 
    :linenos:

    X = da.random.random((128, 128), chunks = (16, 16))
    q, r = da.linalg.qr(X)
    r.compute()    

Tall-and-Skinny QR Reduction (TSQR)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can also compute the tall-and-skinny QR reduction of matrices using Wukong.

.. code-block:: python 
    :linenos:

    X = da.random.random((262_144, 128), chunks = (8192, 128))
    q, r = da.linalg.tsqr(X)
    r.compute()

Cholesky Decomposition
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python 
    :linenos:

    def get_sym(input_size):
        A = da.ones((input_size,input_size), chunks = chunks)
        lA = da.tril(A)
        return lA.dot(lA.T)
    
    input_matrix = get_sym(100)
    X = da.asarray(input_matrix, chunks = (25,25))
    
    # Pass 'True' for the 'lower' parameter if you wish to compute the lower cholesky decomposition.
    chol = da.linalg.cholesky(X, lower = False) 
    chol.compute()

General Matrix Multiplication (GEMM)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python 
    :linenos:

    x = da.random.random((10000, 10000), chunks = (2000, 2000))
    y = da.random.random((10000, 10000), chunks = (2000, 2000))    
    
    z = da.matmul(x, y)
    z.compute()

Machine Learning 
================

Wukong also supports many machine learning workloads through the use of ``Dask-ML``. 

Support Vector Classification (SVC)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python 
    :linenos:
    
    import pandas as pd
    import seaborn as sns
    from collections import defaultdict
    import sklearn.datasets
    from sklearn.svm import SVC

    import dask_ml.datasets
    from dask_ml.wrappers import ParallelPostFit

    X, y = sklearn.datasets.make_classification(n_samples=1000)
    clf = ParallelPostFit(SVC(gamma='scale'))
    clf.fit(X, y)

    results = defaultdict(list)

    X, y = dask_ml.datasets.make_classification(n_samples = 100000,
                                                random_state = 100000,
                                                chunks = 100000 // 20)