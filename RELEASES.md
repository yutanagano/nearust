## Version 0.4.0

- Add Python bindings
- Add documentation page
- Remove previous restriction where only input strings of length < 255
  characters were allowed
- Build CLI and Python package for all major platforms (ARM platforms + MUSL
  Linux newly added)

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
