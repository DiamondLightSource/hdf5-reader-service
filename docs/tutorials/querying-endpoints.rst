Query the HDF5 Reader Service Endpoints
=======================================

The HDF5 Reader Service provides REST endpoints for querying HDF5 files.

Tree
----

The ``/tree`` endpoint returns a JSON representation of the HDF5 file tree structure.

    http://0.0.0.0/8000/tree/?path=<path>

Info
----

The ``/info`` endpoint returns metadata about the given node in the HDF5 file.

    http://0.0.0.0/8000/info/?path=<path>&subpath=<subpath>

Shapes
------

The ``/shapes`` endpoint fetches the shapes of the datasets.

    http://0.0.0.0/8000/shapes/?path=<path>

Search
------

The ``/search`` endpoint fetches the subnode structure of the current subnode.

    http://0.0.0.0/8000/search/?path=<path>&subpath=<subpath>

Slice
-----

The ``/slice`` endpoint fetches the requested slice of the given dataset.

http://0.0.0.0/8000/search/?path=<path>&subpath=<subpath>&slice=0:0:0,0:0:0

