Python package
==============

Installation
------------

.. code-block:: console

   $ pip install symscan

.. admonition:: Getting the latest development version

   You must have ``rustc`` installed on your system to be able to compile the
   underlying Rust code.

   .. code-block:: console

        $ pip install git+https://github.com/yutanagano/symscan.git

   You can also clone the repository, and from within your Python environment,
   navigate to the project root directory and run:

   .. code-block:: console

        $ pip install .

Usage
-----

The Python package exposes two ways to use the symdel algorithm. The first are
the pure functins :py:func:`~symscan.get_neighbors_within` and
:py:func:`~symscan.get_neighbors_across`, which is optimised for one-off
computations. In addition, the package provides a memoized implementation in
the :py:class:`~symscan.CachedRef` class, which is useful for cases where you
know you will be repeatedly querying against a large reference set.

Functional API
--------------

.. autofunction:: symscan.get_neighbors_within
.. autofunction:: symscan.get_neighbors_across

Class-based (memoized) API
--------------------------

.. autoclass:: symscan.CachedRef
   :members:
