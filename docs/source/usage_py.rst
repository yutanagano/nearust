Usage (Python package)
======================

nearust exposes a single function called :py:func:`~nearust.symdel`. This
function can be used to either look for similar strings within a given set of
strings, or across two sets of strings. Like with the :doc:`CLI </usage_cli>`,
the default setting is to detect string pairs within one (Levenshtein) edit
distance away from one another. The API reference for the function is shown
below.

.. autofunction:: nearust.symdel
