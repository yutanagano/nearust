# SymScan

### Check out the [documentation page](https://symscan.readthedocs.io).

**SymScan** enables extremely fast discovery of pairs of similar strings within
and across large collections.

SymScan is a variation on the [symmetric deletion
](https://seekstorm.com/blog/1000x-spelling-correction/) algorithm that is
optimised for bulk-searching similar strings within one or across two large
string collections at once (e.g. searching for similar protein sequences among
a collection of 10M). The key algorithmic difference between SymScan and
traditional symmetric deletion is the use of a [sort-merge
join](https://en.wikipedia.org/wiki/Sort-merge_join) approach in place of hash
maps to discover input strings that share common deletion variants. This
sort-and-scan approach trades off an additional factor of O(log N) (with N the
total number of strings being compared) in expected time complexity for
improved cache locality and effective parallelization, and ends up being much
faster for the above use case.

## Installing

### CLI

```sh
brew install yutanagano/tap/symscan
```

### Rust library

```sh
cargo add symscan
```

### Python package

```sh
pip install symscan
```

## Licensing

SymScan is dual-licensed under the MIT and Apache 2.0 licenses. Unless
explicitly stated otherwise, any contribution submitted by you, as defined in
the Apache license, shall be dual-licensed as above, without any additional
terms and conditions.
