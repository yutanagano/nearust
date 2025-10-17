import nearust._lib as rustlib
from typing import Iterable


__all__ = ["symdel"]


def symdel(
    query: Iterable[str], reference: Iterable[str] = None, max_distance: int = 1
):
    if reference is not None:
        return rustlib.symdel_across_sets(query, reference, max_distance, True)

    return rustlib.symdel_within_set(query, max_distance, True)
