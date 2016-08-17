import random
import time
import pickle
#import matplotlib.pyplot as plt
import copy

from memory_profiler import profile
from collections import defaultdict
from itertools import chain

import snakemq.link
import snakemq.packeter
import snakemq.messaging
import snakemq.message

# global
my_messaging = None
workers = []
free_workers = []
index = 0
results = defaultdict(list)
done = False
tasks = None


# constants
# number of tasks
N = 200

# list of sample tasks
PRIMES_SHORT = [
    112272535095293,
    112582705942171,
    112272535095293,
    115280095190773,
    115797848077099,
    1099726899285419
    ]



def get_tasks(n, source):
    """ return a generator for n elements """

    # map the asked number of elements on the given array of numbers
    for i in range(0, n):
        index = i % len(source)
        yield PRIMES_SHORT[index]


def eval_results():
    """ if all tasks have been sent out (no  guaranteed processing) evaluate the results """

    # number of workers 
    workers = len(results)

    # processing time per worker
    processing_time_worker = {worker: sum([elem['duration'] for elem in results[worker]]) for worker in results.keys()}

    # average processing time per worker
    processing_time_worker_avg = {worker: processing_time_worker[worker] / len(results[worker]) for
                                                                              worker in
                                                                              results.keys()}

    # total processing time 
    processing_time_total = sum(processing_time_worker.values())

    # average processing time (all workers)
    processing_time_avg = processing_time_total / sum([len(results[worker]) for worker in
                                                       results.keys()])


    print('Results \n \
          ------------------ \n \
          workers: {} \n \
          processing time workers: {} \n \
          avg processing  time workers: {} \n \
          total processing time: {} \n \
          avg processing time: {}'.format(workers,
                                          processing_time_worker,
                                          processing_time_worker_avg,
                                          processing_time_total,
                                          processing_time_avg)
         )


def on_recv(conn, ident, message):
    """ method receives the calculation results of a node, also means that one worker is now idling """

    result = pickle.loads(message.data)
    print(ident, result)

    # add tuple (ident, result) to results
    results[ident].append(result)
    free_workers.append(ident)

    if len(free_workers) >= 2:
        distribute()


def on_connect(conn, ident):
    """ register to worker pool """
    print('connected', ident, conn)
    free_workers.append(ident)

    # if an minimal amount of workers is available start it off 
    if len(free_workers) >= 4:
        distribute()

def on_disconnect(conn, ident):
    """ unregister from worker pool """
    print('disconnected', ident, conn)
    free_workers.remove(ident)


def distribute():
    """ simple work distribution method """

    # make global state muteable
    global index
    global done

    # get the number of returned elements
    #results_len = len(list(chain.from_iterable(results.values())))
    # slightly more memory efficient
    results_len = sum([len(results[worker]) for worker in results.keys()])


    # if the end of the task queu has been reached, do the analytics
    #if index == len(PRIMES_SHORT) and results_len == len(PRIMES_SHORT) and not done:
    if index == N and results_len == N and not done:
        eval_results()
        done = True
    elif done:
       return

    # iteration over subset
    for i in range(index, N):

        # i = 0 -> index + 0
        for worker in list(set(free_workers)):
            send(worker, next(tasks))
            free_workers.remove(worker)
            index += 1

        break


def send(ident, number):
    """ method sends prime numbers to the worker nodes """
    message = snakemq.message.Message(pickle.dumps(number), ttl=600)
    my_messaging.send_message(ident, message)


def main():
    global my_messaging
    global tasks

    # init the tasks generator
    tasks = get_tasks(N, PRIMES_SHORT)

    # setup the listener
    my_link = snakemq.link.Link()
    my_packeter = snakemq.packeter.Packeter(my_link)
    my_messaging = snakemq.messaging.Messaging('control_node', "", my_packeter)

    my_link.add_listener(("", 4000))

    # register the callback functions
    my_messaging.on_message_recv.add(on_recv)
    my_messaging.on_connect.add(on_connect)
    my_messaging.on_disconnect(on_disconnect)

    print('Single-Threaded ControlServer initialized')
    print('-------------------------')
    my_link.loop()


if __name__ == '__main__':
    main()

