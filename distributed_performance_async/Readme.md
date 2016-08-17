# Distributed Calculation (async)

This scripts are based on `zeromq`, in particular the `aiozmq` library for Python 3.4/3.5 as well as `zmq` for the worker node. 

Purpose of this script is to have a single control (publish) node which distributes the tasks and collects the results iin an non-blocking (async) manner.


