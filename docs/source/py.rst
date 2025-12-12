Python package
==============

Installation
------------

PyPI (recommended)
..................

.. code-block:: console

   $ pip install symscan

From source
...........

.. important:: 

   You must have ``rustc`` installed on your system to be able to compile the
   underlying Rust code.

.. note:: 

   Python bindings are only available from versions 0.3 and onwards.

From your Python environment, run the following replacing ``<VERSION_TAG>``
with the appropriate version specifier (e.g. ``v0.6.0``). The latest release
tags can be found by checking the 'releases' section on the github repository
page.

.. code-block:: console

	$ pip install git+https://github.com/yutanagano/symscan.git@<VERSION_TAG>

You can also clone the repository, and from within your Python environment,
navigate to the project root directory and run:

.. code-block:: console

	$ pip install .

Usage
-----

The Python package exposes two ways to use the symdel algorithm. The first is
the :py:func:`~symscan.symdel` function, which is optimised for one-off
computations. In addition, the package provides a memoized implementation
in the :py:class:`~symscan.CachedSymdel` class, which is useful for cases where
you know you will be repeatedly querying against a large reference set.

Like with the :doc:`CLI </usage_cli>`, the default setting is to detect string
pairs within one (Levenshtein) edit distance away from one another. The API
reference for the function is shown below. Unlike the :doc:`CLI </usage_cli>`,
the Python bindings report string pairs using 0-based indexing by default,
since the Python language uses 0-based indexing in general. Please see below
for more detail.

Functional API
--------------

.. autofunction:: symscan.symdel

Class-based (memoized) API
--------------------------

.. autoclass:: symscan.CachedSymdel
   :members:
