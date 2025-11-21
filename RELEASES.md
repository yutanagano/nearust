## Version 0.6.0

- Change in return types of Python API:
  - Previously returned Python list of tuples of integers
  - Now returns three Numpy arrays
- Greatly improved runtime and memory usage
- More reliable freeing of OS memory after finishing computations

## Version 0.5.1

- Performance improvements
- Fixed bug that prevented users from specifying a `max_distance` greater than
  the length of the shortest string in the input sets

## Version 0.5.0

- Implement a memoized implementation of symdel for use in the Python package

## Version 0.4.0

- Add Python bindings
- Add documentation page
- Remove previous restriction where only input strings of length < 255
  characters were allowed
- Build CLI and Python package for all major platforms (ARM platforms newly
  added)

## Version 0.3.0

- Add option to make output 0-indexed instead of the default 1-indexed
- Support new installation methods:
  - homebrew
  - shell scripts

## Version 0.2.0

- Implement cross-search feature, nearust can now look for pairs of similar
  strings across two inputs
- Further performance optimisations

## Version 0.1.0

- First working prototype
- Fast detection of similar strings within one input, which is read from
  standard input
