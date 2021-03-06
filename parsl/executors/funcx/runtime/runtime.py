import os
import argparse
import pickle
from ipyparallel.serialize import unpack_apply_message
import logging
import sys
import platform
import threading
import time
import queue
import uuid
import zmq
import math
import json
import subprocess


parser = argparse.ArgumentParser(description='Get Parsl buffer information for FuncX runtime in Singularity.')

parser.add_argument('--buffer_file')

args = parser.parse_args()

buffer_path = args.buffer_file

bufs = pickle.load(open(buffer_path, "rb"))

user_ns = locals()
user_ns.update({'__builtins__': __builtins__})

f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)


# We might need to look into callability of the function from itself
# since we change it's name in the new namespace
prefix = "parsl_"
fname = prefix + "f"
argname = prefix + "args"
kwargname = prefix + "kwargs"
resultname = prefix + "result"

user_ns.update({fname: f,
                argname: args,
                kwargname: kwargs,
                resultname: resultname})

code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                       argname, kwargname)


with open("test_file2.py", "w") as f:
    f.write(code)

try:
    exec(code, user_ns, user_ns)
    print("HERE")
except Exception as e:
    print("HELLO")
    print("Caught exception; will raise it: {}".format(e))
    raise e

else:
    result_filepath = 'function_result.pkl'
    # TODO: Write this to file named 'function_result.pkl'
    print(user_ns.get(resultname))
    print("BANANA")
    pickle.dump(user_ns.get(resultname), open(result_filepath,"wb"))
    print("YO.")
