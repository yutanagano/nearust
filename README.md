# nearust
`nearust` is a minimal CLI utility for fast detection of nearest neighbour strings that fall within a threshold edit distance.
It is a fast, multi-threaded, CPU-based implementation of Chotisorayuth and Mayer's [symdel algorithm](https://arxiv.org/abs/2403.09010v1).

## Installing
Check out the latest release to download pre-built binaries for your system.
The installer script currently does not work since the repository is private.

## Features
The program reads from the standard input until an EOF signal is reached, where each new line is considered to represent a distinct input string. 
All input must be valid ASCII.
The program detects all pairs of input strings that are at most <MAX_DISTANCE> (default=1) edits away from one another, and prints them out in plain text to standard output.
Each line in the program's output contains three integers separated with a comma, where the first two integers represent the (1-indexed) line numbers in the input data corresponding to the two neighbour strings, and the third number corresponds to the number of edits (Levenshtein distance) between them.

Examples:

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust
1,2,1
2,3,1
```

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust
1,2,1
1,3,2
2,3,1
```

For more help on the CLI options, try `nearust --help`.

## Future plans / extensions
- Add ability to look for similar strings *across* two different string multisets, not within one. This can be done in a couple of ways:
    * Specify some special delimiter line that can be put inside the `stdin` input to nearust (e.g. `---`) such that the program will treat everything before it as one multiset and everything after as the second
    * Add an option to read from files instead of `stdin` e.g. `nearust --anchors file1.txt --comparisons file2.txt`
- Fancier release infrastructure:
    * Working installer scripts (shell + powershell)
    * Distribute via homebrew
