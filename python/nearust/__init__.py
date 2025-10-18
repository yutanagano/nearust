import nearust._lib as rustlib
from typing import Iterable, Optional


__all__ = ["symdel"]


def symdel(
    query: Iterable[str],
    reference: Optional[Iterable[str]] = None,
    max_distance: int = 1,
):
    """
    Fast, multi-threaded implementation of Chotisorayuth and Mayer's symdel
    algorithm.

    Parameters
    ----------
    query : iterable of str
    reference : iterable of str, optional
    max_distance : int, default=1
        The maximum edit distance at which strings are considered neighbours.

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
    """
    if reference is not None:
        return rustlib.symdel_across_sets(query, reference, max_distance, True)

    return rustlib.symdel_within_set(query, max_distance, True)
