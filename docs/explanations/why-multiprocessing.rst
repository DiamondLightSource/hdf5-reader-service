Why use Multiprocessing?
========================

The main issue with Python is working with the Global Interpreter Lock (GIL).
This lock means Python can only run natively in a single thread, making it
super difficult to do any CPU heavy workloads.

One workaround for this would be to deploy the webapp on a cluster, such as
Kubernetes, and have some load balancing code to spin up new instances whenever
a call is made. 
The easier solution, however, is to use the Multiprocessing library. This
allows for a new process to be spawned for a specific function call, hence 
avoiding the GIL.

Processes
---------

For the HDF5 Reader Service, Multiprocessing has been used in every
endpoint. This allows for multiple calls to be made at once without any 
blocking.

For example, in the ``/info`` endpoint, the Multiprocessing code is:

.. code-block:: python

    p = mp.Process(target=fetch_info, args=(path, subpath, queue))
    p.start()
    p.join()

where ``fetch_info`` is the function containing the logic for fetching the 
metadata. The process has to be ``start``\ed and then ``join``\ed once completed
to prevent memory leaks.

One downside of using Multiprocessing is that standard Python constructs in
the global namespace, such as dictionaries, cannot be called from within the 
Process. This requires using a Multiprocessing Queue object, which can have
data ``put`` onto it in the process, and can be ``get`` from it back in the
main process.

.. code-block:: python
    
    queue: mp.Queue = mp.Queue()

Pooling
-------

A more efficient way of using Process objects is with a Pool. This would provide 
a convienient way of parallelizing a set number of processes for the webapp.
This hasn't been added yet, but is an open issue 
(https://github.com/DiamondLightSource/python-hdf5-reader-service/issues/8).