.. _codebases

Modularizing Parsl workflows
----------------------------

Parsl workflows can be developed in many ways. When developing a simple workflow it is
often convenient to include the app definitions and control logic in a single script.
However, as a workflow inevitably grows and changes, like any code, there are significant
benefits to be obtained by modularizing the workflow, including:

   1. Better readability
   2. Logical separation of components (e.g., apps, config, and control logic)
   3. Ease of reuse of components


.. caution::
   Support for isolating configuration loading and app definition is available since 0.6.0.
   Refer: `Issue#50 <https://github.com/Parsl/parsl/issues/50>`_


The following example illustrates how a Parsl project can be organized into modules.

The configuration(s) can be defined in a module or file (e.g., ``config.py``)
which can be imported into the control script depending on which execution resources
should be used.

.. literalinclude:: examples/config.py

Parsl apps can be defined in separate file(s) or module(s) (e.g., ``library.py``)
grouped by functionality.


.. literalinclude:: examples/library.py

Finally, the control logic for the Parsl application can then be implemented in a
separate file (e.g., ``run_increment.py``). This file must the import the
configuration from ``config.py`` before calling the ``increment`` app from
``library.py``:

.. literalinclude:: examples/run_increment.py

Which produces the following output::

    0 + 1 = 1
    1 + 1 = 2
    2 + 1 = 3
    3 + 1 = 4
    4 + 1 = 5
