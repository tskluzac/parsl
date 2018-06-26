import os
import pytest

import parsl
from parsl.app.app import App
from parsl.data_provider.files import File
from parsl.tests.configs.local_ipp import config
working_dir = os.path.join(os.getcwd(), 'working_dir')
config.executors[0].working_dir = working_dir

parsl.set_stream_logger()

parsl.clear()
parsl.load(config)

unsorted_file = File(os.path.abspath(__file__))

@App('python', auxiliary_files=[unsorted_file])
def sort_strings(path, inputs=[], outputs=[]):
    with open(path, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.local
def test_implicit_staging_local():
    """Test implicit staging for local auxiliary file"""


    # Create a local file for output data
    sorted_file = File('sorted.txt')

    path = os.path.join(working_dir, os.path.basename(__file__))
    f = sort_strings(path, outputs=[sorted_file])
    f.result()


if __name__ == "__main__":
    test_implicit_staging_local()
