import zmq
import math
import random
import time


class Worker:
    XSUB_CONNECT = 'tcp://localhost:10000'

    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.XSUB)
        self.socket.connect(Worker.XSUB_CONNECT)
        self.name = self.get_name()

    @staticmethod
    def get_name():
        return 'worker-{}'.format(str(random.randint(1, 99999)))

    def loop(self):
        print(self.socket.recv())
        self.socket.close()

    def subscribe(self, parameter):
        self.socket.send_string('\x01'+parameter)
        print("Subscribed to work queue "+parameter)

    def send(self, result):
        # send the worker id and the calculation result
        self.socket.send_string(result)

    @staticmethod
    def isprime(n):
        """ figure out if the given number is a prime number """

        if n % 2 == 0:
            return False

        # else take square root and iterate over all uneven (step 2) numbers
        sqrt_n = int(math.floor(math.sqrt(n)))
        for i in range(3, sqrt_n + 1, 2):
            if n % i == 0:
                return False

        return True

    def isprimetime(self, n):
        """ measure calculation time to execute isprime method on given number """
        start = time.time()
        prime = self.isprime(n)
        end = time.time()

        return {'number': n, 'is_prime': prime, 'duration': end - start}

    def run(self):
        print('worker started:', self.name)

        # subscribe to its task channel
        self.subscribe(self.name)

        while True:
            task = int(self.socket.recv().decode("utf-8")[len(self.name) + 1:])
            result = self.isprimetime(task)

            print(result)

            self.socket.send_json({self.name: result})


def main():
    test = Worker()
    test.run()


if __name__ == '__main__':
    main()
