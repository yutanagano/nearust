Library (Python bindings)
=========================

Installation
------------

.. code-block:: console

   $ pip install symscan

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
