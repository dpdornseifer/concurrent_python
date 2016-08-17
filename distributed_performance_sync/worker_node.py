import math
import random
import time
import pickle
#from memory_profiler import profile


import snakemq.link
import snakemq.packeter
import snakemq.messaging
import snakemq.message


# global reference to messaging stack 
my_messaging = None


def isprime(n):
    """ figure out if the given number is a prime number """

    # if n modulo 2 equals 0 - number can be devided by 2 without rest, so no prime
    if n % 2 == 0:
        return False

    # else take square root and iterate over all uneven (step 2) numbers
    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return False

    return True


def timeisprime(n):
    """ measure calculation time to execute isprime method on given number """
    start = time.time()
    prime = isprime(n)
    end = time.time()

    return {'number': n, 'is_prime': prime, 'duration': end - start}





# is prime is blocking right now and not running in its own thread
def on_recv(conn, ident, message):
    """ receives the messages and convertes the payload to and integer """ 
    number = pickle.loads(message.data)
    result = timeisprime(number)
    print(result)
    send(ident, result)


def send(ident, msg):
    """ pickle the message and send it to the destination node """ 
    message = pickle.dumps(msg)
    message = snakemq.message.Message(message, ttl=600)
    my_messaging.send_message(ident, message)


def main():
    global my_messaging
    worker = get_name()

    my_link = snakemq.link.Link()
    my_packeter = snakemq.packeter.Packeter(my_link)
    my_messaging = snakemq.messaging.Messaging(worker, "", my_packeter)

    # connect to the listener
    my_link.add_connector(("localhost", 4000))

    # register the callback function 
    my_messaging.on_message_recv.add(on_recv)

    #send("control_node", 'ping from {}'.format(worker))

    print(worker, 'started ')
    print('-----------')

    my_link.loop()


if __name__ == '__main__':
    main()
