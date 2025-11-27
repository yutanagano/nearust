import numpy as np
from numpy.typing import NDArray
from typing import Sequence, Optional

def symdel(
    query: Sequence[str],
    reference: Optional[Sequence[str]] = None,
    max_distance: int = 1,
    zero_index: bool = True,
) -> tuple[NDArray[np.uint32], NDArray[np.uint32], NDArray[np.uint8]]:
    """
    Quickly detects pairs of similar strings.

    Parameters
    ----------
    query : iterable of str
    reference : iterable of str, optional
    max_distance : int, default=1
        The maximum edit distance at which strings are considered neighbors.
    zero_index : bool, default=True
        If set to True, reports the indices of strings of interest using
        0-based indexing. Otherwise uses 1-based indexing.

    Returns
    -------
    i : ndarray of shape (N,), dtype=uint32
        Indices of strings in the query that have neighbors.

    j : ndarray of shape (N,), dtype=uint32
        Indices of neighbor strings. If only the `query` parameter was set,
        then ``query[i[k]]`` and ``query[j[k]]`` are neighbors. If both `query`
        and `reference` parameters were set, then ``query[i[k]]`` and
        ``reference[j[k]]`` are neighbors.

    dists : ndarray of shape (N,), dtype=uint8
        Edit distances between neighbors. If only the `query` parameter was
        set, then ``Levenshtein(query[i[k]], query[j[k]]) = dists[k]``. If both
        `query` and `reference` parameters were set, then
        ``Levenshtein(query[i[k]], reference[j[k]]) = dists[k]``.

    Examples
    --------
    Provide one iterable over strings to look for pairs of similar strings
    within it.

    >>> import nearust
    >>> (i, j, dists) = nearust.symdel(["fizz", "fuzz", "buzz"])
    >>> i
    array([0, 1], dtype=uint32)
    >>> j
    array([1, 2], dtype=uint32)
    >>> dists
    array([1, 1], dtype=uint8)

    To increase the threshold at which string pairs are considered similar, set
    `max_distance`.

    >>> (i, j, dists) = nearust.symdel(["fizz", "fuzz", "buzz"], max_distance=2)
    >>> i
    array([0, 0, 1], dtype=uint32)
    >>> j
    array([1, 2, 2], dtype=uint32)
    >>> dists
    array([1, 2, 1], dtype=uint8)

    To look for pairs of similar strings across two sets, provide two iterables
    over strings (`query` and `reference`).

    >>> (i, j, dists) = nearust.symdel(["fizz", "fuzz", "buzz"], ["fooo", "barr", "bazz", "buzz"])
    >>> i
    array([1, 2, 2], dtype=uint32)
    >>> j
    array([3, 2, 3], dtype=uint32)
    >>> dists
    array([1, 1, 0], dtype=uint8)

    If you would like the string indices returned to be 1-based instead of
    0-based (in a manner similar to the default behaviour of the CLI), you can
    set `zero_index` to False.

    >>> (i, j, dists) = nearust.symdel(["fizz", "fuzz", "buzz"], zero_index=False)
    >>> i
    array([1, 2], dtype=uint32)
    >>> j
    array([2, 3], dtype=uint32)
    >>> dists
    array([1, 1], dtype=uint8)
    """
    ...

class CachedSymdel:
    """
    A memoized implementation of symdel.

    An implementation of symdel where the deletion variant computations for the
    reference string set is memoized and stored in memory as a hashmap. This is
    useful for use-cases where you want to repeatedly query the same reference,
    especially if the reference is very large.

    Parameters
    ----------
    reference : iterable of str
    max_distance : int, default=1
        The maximum edit distance that this CachedSymdel instance will be able
        to support when detecting similar strings. That is, if `max_distance`
        is set to X at construction, later calls to this instance's symdel
        method will only be accept `max_distance` less than or equal to X.
    """

    def __init__(self, reference: Sequence[str], max_distance: int = 1) -> None: ...
    def symdel(
        self,
        query: Sequence[str] | "CachedSymdel" | None = None,
        max_distance: int = 1,
        zero_index: bool = True,
    ) -> tuple[NDArray[np.uint32], NDArray[np.uint32], NDArray[np.uint8]]:
        """
        Quickly detects pairs of similar strings.

        Parameters
        ----------
        query : iterable of str or CachedSymdel or None, default=None
        max_distance : int, default=1
            The maximum edit distance at which strings are considered
            neighbours. This must be less than or equal to the `max_distance`
            specified when constructing the caller instance (as well as that
            when constructing `query`, if `query` is set to a CachedSymdel
            instance).
        zero_index : bool, default=True
            If set to True, reports the indices of strings of interest using
            0-based indexing. Otherwise uses 1-based indexing.

        Returns
        -------
        .. tip::
            If `query` is set to another CachedSymdel instance constructed
            using some set of strings X, then the resulting computation is
            equivalent to setting `query` to X directly. This is useful in
            cases where you want to memoize deletion variant computations for
            both query and reference sets.

        i : ndarray of shape (N,), dtype=uint32
            Indices of strings in the query that have neighbors.

        j : ndarray of shape (N,), dtype=uint32
            Indices of neighbor strings. If only the `query` parameter was set,
            then ``query[i[k]]`` and ``query[j[k]]`` are neighbors. If both
            `query` and `reference` parameters were set, then ``query[i[k]]``
            and ``reference[j[k]]`` are neighbors.

        dists : ndarray of shape (N,), dtype=uint8
            Edit distances between neighbors. If only the `query` parameter was
            set, then ``Levenshtein(query[i[k]], query[j[k]]) = dists[k]``. If
            both `query` and `reference` parameters were set, then
            ``Levenshtein(query[i[k]], reference[j[k]]) = dists[k]``.


        Examples
        --------
        Construct a CachedSymdel instance with an iterable over reference
        strings. This pre-computes the deletion variats for the strings in the
        reference and stores the results in a hashmap held internally by the
        instance.

        >>> import nearust
        >>> cached = nearust.CachedSymdel(["fooo", "barr", "bazz", "buzz"])

        Then, call the symdel method with `query` set to an iterable over query
        strings to find similar strings across it and the reference set.

        >>> (i, j, dists) = cached.symdel(["fizz", "fuzz", "buzz"])
        >>> i
        array([1, 2, 2], dtype=uint32)
        >>> j
        array([3, 2, 3], dtype=uint32)
        >>> dists
        array([1, 1, 0], dtype=uint8)

        If you also want to memoize deletion variant computations on the query
        set as well, you can do so.

        >>> cached_query = nearust.CachedSymdel(["fizz", "fuzz", "buzz"])
        >>> (i, j, dists) = cached.symdel(cached_query)
        >>> i
        array([1, 2, 2], dtype=uint32)
        >>> j
        array([3, 2, 3], dtype=uint32)
        >>> dists
        array([1, 1, 0], dtype=uint8)

        If you call symdel without specifying `query`, the function will look
        for pairs of similar strings within the reference set.

        >>> (i, j, dists) = cached.symdel()
        >>> i
        array([2], dtype=uint32)
        >>> j
        array([3], dtype=uint32)
        >>> dists
        array([1], dtype=uint8)

        For a CachedSymdel instance to support calls to symdel with
        `max_distance` equal to X, it `max_distance` must be set to X or
        greater at construction time.

        >>> cached_maxd2 = nearust.CachedSymdel(["fooo", "barr", "bazz", "buzz"], max_distance=2)
        >>> (i, j, dists) = cached_maxd2.symdel(["fizz", "fuzz", "buzz"])
        >>> i
        array([1, 2, 2], dtype=uint32)
        >>> j
        array([3, 2, 3], dtype=uint32)
        >>> dists
        array([1, 1, 0], dtype=uint8)
        >>> (i, j, dists) = cached_maxd2.symdel(["fizz", "fuzz", "buzz"], max_distance=2)
        >>> i
        array([0, 0, 1, 1, 2, 2], dtype=uint32)
        >>> j
        array([2, 3, 2, 3, 2, 3], dtype=uint32)
        >>> dists
        array([2, 2, 2, 1, 1, 0], dtype=uint8)
        >>> # max_distance > 2 will throw an error!: cached_maxd2.symdel(["fizz", "fuzz", "buzz"], max_distance=3)
        """
        ...
