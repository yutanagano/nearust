import nearust._lib as rustlib
from typing import Iterable, Optional


def symdel(
    query: Iterable[str],
    reference: Optional[Iterable[str]] = None,
    max_distance: int = 1,
    zero_index: bool = True,
) -> list[tuple[int, int, int]]:
    """
    Fast, multi-threaded implementation of Chotisorayuth and Mayer's symdel
    algorithm.

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
        strings that were detected as being neighbours, where the first two
        ints describe the indices of the two strings in the pair, and the third
        int describes the Levenshtein edit distance between them. If symdel was
        only given `query` as input, then it returns pairs of strings from
        within `query` that are neighbours. If symdel was given both `query`
        and `reference`, then it returns string pairs across the two sets,
        where the first index always corresponds to a string from `query`, and
        the second index always corresponds to a string from `reference`.

    Raises
    ------
    ValueError
        If any of the input strings contain non-ASCII characters.

    Examples
    --------
    Provide one iterable over strings to look for pairs of similar strings
    within it.

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
        return rustlib.symdel_across_sets(query, reference, max_distance, zero_index)

    return rustlib.symdel_within_set(query, max_distance, zero_index)
