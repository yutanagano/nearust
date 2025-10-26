import nearust._lib as rustlib
from typing import Iterable, Optional


def symdel(
    query: Iterable[str],
    reference: Optional[Iterable[str]] = None,
    max_distance: int = 1,
    zero_index: bool = True,
) -> list[tuple[int, int, int]]:
    """
    Quickly detects pairs of similar strings.

    Parameters
    ----------
    query : iterable of str
    reference : iterable of str, optional
    max_distance : int, default=1
        The maximum edit distance at which strings are considered neighbours.
    zero_index : bool, default=True
        If set to True, reports the indices of strings of interest using
        0-based indexing. Otherwise uses 1-based indexing.

    Returns
    -------
    list of tuple of int, int, int
        A list of integer triplets, where each triplet describes a pair of
        strings that were detected as being neighbours. The first two ints
        describe the indices of the two strings in the pair, and the third
        int describes the Levenshtein edit distance between them.

        If symdel was only given `query` as input, then it returns pairs of
        strings from within `query` that are neighbours. If symdel was given
        both `query` and `reference`, then it returns string pairs across the
        two sets. In this case, the first index always corresponds to a string
        from `query`, and the second index always corresponds to a string from
        `reference`.

    Examples
    --------
    Provide one iterable over strings to look for pairs of similar strings
    within it.

    >>> import nearust
    >>> nearust.symdel(["fizz", "fuzz", "buzz"])
    [(0, 1, 1), (1, 2, 1)]

    To increase the threshold at which string pairs are considered similar, set
    `max_distance`.

    >>> nearust.symdel(["fizz", "fuzz", "buzz"], max_distance=2)
    [(0, 1, 1), (0, 2, 2), (1, 2, 1)]

    To look for pairs of similar strings across two sets, set both provide two
    iterables over strings (`query` and `reference`).

    >>> nearust.symdel(["fizz", "fuzz", "buzz"], ["fooo", "barr", "bazz", "buzz"])
    [(1, 3, 1), (2, 2, 1), (2, 3, 0)]

    If you would like the string indices returned to be 1-based instead of
    0-based (in a manner similar to the default behaviour of the CLI), you can
    set `zero_index` to False.

    >>> nearust.symdel(["fizz", "fuzz", "buzz"], zero_index=False)
    [(1, 2, 1), (2, 3, 1)]
    """
    if reference is not None:
        return rustlib.symdel_cross(query, reference, max_distance, zero_index)

    return rustlib.symdel_within(query, max_distance, zero_index)


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

    def __init__(self, reference: Iterable[str], max_distance: int = 1) -> None:
        self._internal = rustlib.CachedSymdel(reference, max_distance)

    def symdel(
        self,
        query: Iterable[str] | "CachedSymdel" | None = None,
        max_distance: int = 1,
        zero_index: bool = True,
    ) -> list[tuple[int, int, int]]:
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
        list of tuple of int, int, int
            A list of integer triplets, where each triplet describes a pair of
            strings that were detected as being neighbours. The first two ints
            describe the indices of the two strings in the pair, and the third
            int describes the Levenshtein edit distance between them.

            If `query` is not set (or set to None), the function computes
            symdel for pairs of strings within `reference` (specified when
            constructing the class). If `query` is set, the function computes
            symdel for pairs of strings between the query and reference. In
            this case, the first index in the triplet always corresponds to a
            string from `query`, and the second index always corresponds to a
            string from `reference`.

            Note that if `query` is set to another CachedSymdel instance
            constructed using some set of strings X, then the resulting
            computation is equivalent to setting `query` to X directly. This is
            useful in cases where you want to memoize deletion variant
            computations for both query and reference sets.

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

        >>> cached.symdel(["fizz", "fuzz", "buzz"])
        [(1, 3, 1), (2, 2, 1), (2, 3, 0)]

        If you also want to memoize deletion variant computations on the query
        set as well, you can do so.

        >>> cached_query = nearust.CachedSymdel(["fizz", "fuzz", "buzz"])
        >>> cached.symdel(cached_query)
        [(1, 3, 1), (2, 2, 1), (2, 3, 0)]

        If you call symdel without specifying `query`, the function will look
        for pairs of similar strings within the reference set.

        >>> cached.symdel()
        [(2, 3, 1)]

        For a CachedSymdel instance to support calls to symdel with
        `max_distance` equal to X, it `max_distance` must be set to X or
        greater at construction time.

        >>> cached = nearust.CachedSymdel(["fooo", "barr", "bazz", "buzz"], max_distance=2)
        >>> cached.symdel(["fizz", "fuzz", "buzz"], max_distance=2)
        [(0, 2, 2), (0, 3, 2), (1, 2, 2), (1, 3, 1), (2, 2, 1), (2, 3, 0)]
        """
        if query is None:
            return self._internal.symdel_within(max_distance, zero_index)

        if isinstance(query, CachedSymdel):
            return self._internal.symdel_cross_against_cached(
                query._internal, max_distance, zero_index
            )

        return self._internal.symdel_cross(query, max_distance, zero_index)
