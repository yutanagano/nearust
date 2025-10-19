Usage (Python package)
======================

nearust exposes a single function called :py:func:`~nearust.symdel`. This
function can be used to either look for similar strings within a given set of
strings, or across two sets of strings. Like with the :doc:`CLI </usage_cli>`,
the default setting is to detect string pairs within one (Levenshtein) edit
distance away from one another. See :ref:`api` for details on options.

Detecting similar strings within a set
--------------------------------------

To look for pairs of similar strings within one set, you can run
:py:func:`~nearust.symdel` with one iterable over strings.

>>> import nearust
>>> nearust.symdel(["fizz", "fuzz", "buzz"])
[(0, 1, 1), (1, 2, 1)]

nearust returns a list of integer triplets, where each triplet represents a
pair of similar strings that have been detected. The first two integers
represent the indices of the strings comprising the pair, and the third integer
represents the edit distance between them. Unlike with the :doc:`CLI
</usage_cli>`, the default behaviour in Python is to return 0-based indices for
the strings, since Python uses 0-based indexing.

Detecting similar strings across two sets
-----------------------------------------

If you run :py:func:`~nearust.symdel` with two iterables over strings, the
function will strictly look for strings in the first set that are similar to
strings in the second set.

>>> nearust.symdel(["fizz", "fuzz", "buzz"], ["fooo", "barr", "bazz", "buzz"])
[(1, 3, 1), (2, 2, 1), (2, 3, 0)]

Note that in the returned triplets, the first index always corresponds to a
string from the first iterable (the `query`), and the second index always
corresponds to a string from the second iterable (the `reference`).

.. _api:

API reference
-------------

.. autofunction:: nearust.symdel
