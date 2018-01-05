''' Testing bash apps
'''
import parsl
from parsl import *

import os
import time
import shutil
import argparse
from nose.tools import nottest

#parsl.set_stream_logger()
workers = IPyParallelExecutor()
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def import_echo(x, string, sleep=0, stdout=None):
    import time
    time.sleep(sleep)
    print(string)
    return x*5


def test_parallel_for (n=10):

    d = {}
    start = time.time()
    for i in range(0,n):
        d[i] = import_echo(2, "hello", sleep=20)
        #time.sleep(0.01)

    assert len(d.keys())   == n , "Only {0}/{1} keys in dict".format(len(d.keys()), n)

    [d[i].result() for i in d]
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d


if __name__ == '__main__' :

    parser   = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10", help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args   = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_parallel_for()
    #x = test_parallel_for(int(args.count))

    #x = test_stdout()
    #raise_error(0)