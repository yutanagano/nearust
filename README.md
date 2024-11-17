# nearust
`nearust` is a minimal CLI utility for fast detection of nearest neighbour strings that fall within a threshold edit distance.
It is a fast, multi-threaded, CPU-based implementation of Chotisorayuth and Mayer's [symdel algorithm](https://arxiv.org/abs/2403.09010v1).

## Installing
Check out the latest release to download pre-built binaries for your system.

## Features
If you provide nearust with a path to a `[FILE_PRIAMRY]`, it will read its contents for input.
If no path is supplied, nearust will read from the standard input until it receives an EOF signal.
Nearust will then look for pairs of similar strings within its input, where each line of text is treated as an individual string.
You can also supply nearust with two paths -- a `[FILE_PRIMARY]` and `[FILE_COMPARISON]`, in which case the program will look for pairs of similar strings across the contents of the two files.
Currently, only valid ASCII input is supported.

By default, the threshold (Levenshtein) edit distance at or below which a pair of strings are considered similar is set at 1.
This can be changed by setting the `--max-distance` option.

Nearust's output is plain text, where each line encodes a detected pair of similar input strings.
Each line is comprised of three integers separated by commas, which represent:

| Column        | Value                                                                                                                                                |
| ---           | ---                                                                                                                                                  |
| First column  | (1-indexed) line number of the string from the primary input (i.e. `stdin` or `[FILE_PRIMARY]`)                                                      |
| Second column | (1-indexed) line number of the string from the secondary input (i.e. `stdin` or `[FILE_PRIMARY]` if one input, or `[FILE_COMPARISON]` if two inputs) |
| Third column  | (Levenshtein) edit distance between the similar strings                                                                                              |

For more help on the CLI options, try `nearust --help`.

### Examples

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust
1,2,1
2,3,1
```

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust -d 2
1,2,1
1,3,2
2,3,1
```

## Future plans / extensions
- Fancier release infrastructure:
    * Working installer scripts (shell + powershell)
    * Distribute via homebrew
- Support for wider unicode encodings outside of simple ASCII
