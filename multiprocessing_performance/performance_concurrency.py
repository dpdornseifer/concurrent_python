import math
import time
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from memory_profiler import profile


PRIMES_SHORT = [
    112272535095293,
    112582705942171,
    112272535095293,
    115280095190773,
    115797848077099,
    1099726899285419
    ]


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


def runwithexecutor(executortype, primes):
    """
    run the 'isprime' method in a executon pool for concurrency and measure and
    return the time it takes to finish the calculation
    """

    start = time.time()
    with executortype as executor:
        for number, prime in zip(primes, executor.map(isprime, primes)):
            print("{} is prime: {}".format(number, prime))
    end = time.time()
    return end -start


def runwithprocesspoolexecutor(primes):
    """ run the process with a process pool executor """

    time = runwithexecutor(ProcessPoolExecutor(), primes)
    n = len(primes)
    print('ProcessPoolExecutor', n, time)
    return(time)


def runwiththreadpoolexecutor(primes):
    """ run the process with a process pool executor """

    time = runwithexecutor(ThreadPoolExecutor(20), primes)
    n = len(primes)
    print('ThreadPoolExecutor', n, time)
    return(time)


def queuesize(primes, exponents):
    """ multiply the number of elements by a factor  2^x """

    num = []
    thread = []
    process = []
    count = len(primes)

    for exponent in range(0, exponents + 1):
        # count of primes for x axis
        multiplier = int(math.pow(2, exponent))
        num.append(multiplier * count)

        # times for y axis
        thread.append(runwiththreadpoolexecutor(PRIMES_SHORT * multiplier))
        process.append(runwithprocesspoolexecutor(PRIMES_SHORT * multiplier))

    # return the measures for each queue size as a tripple
    return num, thread, process


def getslope(input_sizes, calculation_times):
    """
    calculate the gradient of the data points to see for what factor the 
    time keeps increasing when the input is doubled 
    """
    gradients = []


    for i in range(1, len(input_sizes)):

        delta_x = input_sizes[i]  - input_sizes[i - 1]
        delta_y = calculation_times[i] - calculation_times[i - 1]
        gradients.append(delta_y / delta_x)

    return gradients


def main():

    # run the executions and get the calculation times 
    num, thread, process = queuesize(PRIMES_SHORT, 4)


    # for testing purpose
    #data = list(zip(*[(6, 4.078994989395142, 0.9684610366821289), (12, 8.20539379119873, 2.1957099437713623),
    #       (24, 16.26440978050232, 4.1612560749053955)]))

    #num = list(data[0])
    #thread = list(data[1])
    #process = list(data[2])

    print('Gradients Threads', getslope(num, thread))
    print('Gradients Processes', getslope(num, process))

    # plot the results 
    plt.plot(num, thread, 'r-', num, process, 'g--')
    plt.xlabel('number of elements (n)')
    plt.ylabel('processing time (s)')
    plt.grid(True)
    plt.show()


if __name__ == '__main__':
    main()
