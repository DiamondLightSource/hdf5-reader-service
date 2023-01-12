Running the HDF5 Reader Service server
======================================

This tutorial shows how to run the HDF5 Reader Service server.

Start the server
----------------

It is very easy to start the HDF5 reader service. All that is needed is to
run the following command in a terminal:

    $ hdf5-reader-service --host <host> --port <port>

However, it is also possible to run the server on a specific host or port. To
do this, just use the ``--host`` and ``--port`` flags.

    $ hdf5-reader-service --host 127.0.0.1 --port 8000
