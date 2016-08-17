import aiozmq
import asyncio
import zmq
import json
from collections import defaultdict
from tqdm import trange


class Publisher:
    BIND_ADDRESS = 'tcp://*:10000'

    def __init__(self, n_tasks):
        self.stream = None
        self.parameter = None
        self.tasks = None
        self.workers = []
        self.free_workers = []
        self.n_tasks = n_tasks
        self.results = defaultdict(list)

        # list of sample tasks
        self.PRIMES_SHORT = [
            112272535095293,
            112582705942171,
            112272535095293,
            115280095190773,
            115797848077099,
            1099726899285419
        ]

    def get_tasks(self, n, source):
        """ return a generator for n elements """
        # map the asked number of elements on the given array of numbers
        for i in trange(0, n):
            index = i % len(source)
            yield self.PRIMES_SHORT[index]

    def eval_results(self):
        """ if all results have been collected evaluate the results """

        # number of workers
        workers = len(self.results)

        # processing time per worker
        processing_time_worker = {
            worker: sum([elem['duration'] for elem in self.results[worker]])
            for worker in self.results.keys()
            }


        # average processing time per worker
        processing_time_worker_avg = {
            worker: processing_time_worker[worker] / len(self.results[worker])
            for worker in self.results.keys()
            }

        # total processing time
        processing_time_total = sum(processing_time_worker.values())

        # average processing time (all workers)
        processing_time_avg = processing_time_total / sum([len(self.results[worker]) for worker in
                                                           self.results.keys()])

        print('\n \
              Results \n \
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


    @asyncio.coroutine
    def run(self):
        self.tasks = self.get_tasks(self.n_tasks, self.PRIMES_SHORT)
        self.stream = yield from aiozmq.create_zmq_stream(zmq.XPUB, bind=Publisher.BIND_ADDRESS)
        tasks = [
            asyncio.async(self.subscriptions()),
            asyncio.async(self.publish()),
        ]

        print('Control Server initialized')
        yield from asyncio.gather(*tasks)

    @asyncio.coroutine
    def subscriptions(self):
        print("Listening for workers now")
        while True:

            received = yield from self.stream.read()

            # if first byte is 0 or 1 - it's a (un) subscription event
            first_byte = received[0][0]
            if first_byte is 1:

                worker = received[0][-len(received[0])+1:].decode("utf-8")

                # maintain global worker list
                self.workers.append(worker)
                self.free_workers.append(worker)

            elif first_byte == 0:

                worker = received[0][-len(received[0]) + 1:].decode("utf-8")
                self.workers.remove(worker)

            # else it's a result, at least if everything is in the pre-defined state
            else:
                received = json.loads(received[0].decode("utf-8"))

                worker = list(received.keys())[0]
                result = received[worker]

                self.results[worker].append(result)
                self.free_workers.append(worker)

    @asyncio.coroutine
    def publish(self):
        while True:

            if bool(self.free_workers):

                results_len = sum([len(self.results[worker]) for worker in self.results.keys()])

                # do the evaluation as soon as all results are back
                # free_workers == workers - as long as there is one
                if results_len == self.n_tasks:
                    self.eval_results()
                    break

                for worker in self.free_workers:

                    # Publish fetched values to subscribers / topic - in this case the worker name
                    # has to be first
                    try:
                        message = bytearray(worker + ":" + str(next(self.tasks)), "utf-8")

                    except StopIteration:
                        print('All tasks have been sent - start evaluation now, it takes a while until all results'
                              'are available')

                    pack = [message]

                    self.stream.write(pack)
                    self.free_workers.remove(worker)
                    yield from self.stream.drain()

            yield from asyncio.sleep(0.5)


def main():
    server = Publisher(5)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.run())


if __name__ == '__main__':
    main()
