Why make the HDF5 Reader Service?
=================================

The HDF5 Reader Service was designed as a replacement to the existing DAWN
Dataserver for the General Data Aquisition (GDA) system at Diamond Light Source.

The DAWN Dataserver for GDA is a fairly old and hard to maintain data-broker
service written in Java. It was decided to try and make a much simpler and 
easier-to-maintain microservice in Python, using REST endpoints for querying 
instead of hard-coding the logic into GDA.