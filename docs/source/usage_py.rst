Usage (Python package)
======================

The Python package exposes two ways to use the symdel algorithm. The first is
the :py:func:`~nearust.symdel` function, which is optimised for one-off
computations. In addition, the package also provides a memoized implementation
in the :py:class:`~nearust.CachedSymdel` class, which is useful for cases where
you know you will be repeatedly querying against a large reference set.

Like with the :doc:`CLI </usage_cli>`, the default setting is to detect string
pairs within one (Levenshtein) edit distance away from one another. The API
reference for the function is shown below. Unlike the :doc:`CLI </usage_cli>`,
the Python bindings report string pairs using 0-based indexing by default,
since the Python language uses 0-based indexing in general. Please see below
for more detail.

Functional API
--------------

.. autofunction:: nearust.symdel


Class-based (memoized) API
--------------------------

.. autoclass:: nearust.CachedSymdel
   :members:
