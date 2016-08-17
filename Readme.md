# Concurrent and Distributed Python

This repository keeps some utilities and testing scripts to measure and display(visualize) the performance 
of Python operations in concurrent and distributed scenarios (e.g. multithreading vs. multiprocessing) in CPU intensive scenarios. 

----

## Multithreading vs. Multiprocessing
Script to measure the performance differences in executing CPU intensive tasks (thread- vs. process-pools) due to the well known cPython GIL issue.


----

## Distributed Python

### Broker less

Measuring the performance in distributed scenarios by having a control-node and many worker nodes. 


#### SnakeMQ
In this example the controller is not able to distribute tasks and collect results in the same time. 
This can for example be solved by initiating two sockets (send, receive) in two threads and for example having them talk via a threadsafe implementation of a queue. 


#### ZeroMQ
Based on Python 3.4/3.5 coroutines the publisher is implemented in a non-blocking way.

### Broker based
	
