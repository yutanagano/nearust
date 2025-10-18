# nearust

`nearust` is a tool for fast detection of nearest neighbour strings that fall within a threshold edit distance.
It is a fast, multi-threaded, CPU-based implementation of Chotisorayuth and Mayer's [symdel algorithm](https://arxiv.org/abs/2403.09010v1).

## Installing

### Homebrew

```bash
brew install yutanagano/tap/nearust
```

### Shell script

Replace `<LATEST-VERSION>` below to the latest release version tag:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/yutanagano/nearust/releases/download/<LATEST-VERSION>/nearust-installer.sh | sh
```

### Alternative methods

For alternative installation methods including a direct binary download, check out the latest release.

## Quickstart

Use the examples below to get started.
You can also view nearust's inline help text with `nearust --help`.

### Basic usage

Give nearust a list of strings, and it will tell you which ones are similar.
By default, it will detect which strings are within one (Levenshtein) edit distance away from one another.
Nearust reads its standard input stream and considers each line (delineated by `newline` characters) a separate string.
A minimal example is below:

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust
1,2,1
2,3,1
```

As you can see, nearust outputs its result in plaintext to standard output.
Each line in its output corresponds to a pair of similar strings that is detected.
The first two numbers in each line is the (1-indexed) line numbers corresponding to the two similar input strings.
The third and final number is the number of edits separating the two strings.

### Options

To look for string pairs that are at most `<k>` edits away from each other, pass the option `-d <k>`:

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust -d 2
1,2,1
1,3,2
2,3,1
```

If you want the output to have 0-indexed line numbers as opposed to 1-indexed, pass the option `-z`:

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust -d 2 -z
0,1,1
0,2,2
1,2,1
```

### Read from and write to files

To read input from `input.txt` and write to `output.txt`:

```bash
$ cat input.txt | nearust > output.txt
```

or

```bash
$ nearust input.txt > output.txt
```

### Look for pairs across two string sets

To look strictly for strings in `set_a.txt` that are similar to strings in `set_b.txt` (and ignore pairs within the sets that are similar to each other):

```bash
$ nearust set_a.txt set_b.txt > output.txt
```
