# nearust
`nearust` is a minimal CLI utility for fast detection of nearest neighbour strings that fall within a threshold edit distance.
It is a fast, multi-threaded, CPU-based implementation of Chotisorayuth and Mayer's [symdel algorithm](https://arxiv.org/abs/2403.09010v1).

## Features
The program reads from the standard input until an EOF signal is reached, where each new line is considered to represent a distinct input string. 
All input must be valid ASCII.
The program detects all pairs of input strings that are at most <MAX_DISTANCE> (default=1) edits away from one another, and prints them out in plain text to standard output.
Each line in the program's output contains three eintegers separated with a comma, where the first two integers represent the (0-indexed) line numbers in the input data corresponding to the two neighbour strings, and the third number corresponds to the number of edits (Levenshtein distance) between them.

Examples:

```bash
$ echo $'foo\nbar\nbaz' | nearust
1,2,1
```

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust
0,1,1
1,2,1
```

## Future plans / extensions
- Read from files instead of `stdin` -- this will allow two different files / buffers as input, allowing `nearust` to also handle cases where we want to look for pairs *across* two different multisets e.g. `nearust --anchors file1.txt --comparisons file2.txt`
