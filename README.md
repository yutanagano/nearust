# nearust

`nearust` is a minimal CLI utility for finding pairs of nearest neighbour strings that fall within 1 or 2 edit distance using Chotisorayuth and Mayer's [symdel algorithm](https://arxiv.org/abs/2403.09010v1).

## Planned features / proposed specs
### Read from `stdin`
- This should be the most basic behaviour and should be implemented first
- We can start by having the threshold distance set at 1 edit, where you cannot change this
- `stdin` is read where each new line represents a UTF-8 string to be considered
- `stdin` is read until EOF signal is reached
- The output is also in plain text and directed to `stdout`
- Every line in the output represents a hit (pair of strings within threshold distance)
- A hit is encoded in terms of the 0-indexed indices of the strings involved, separated by a comma, where the lower index is always first

Examples:

```bash
$ echo $'foo\nbar\nbaz' | nearust
1,2
```

```bash
$ echo $'fizz\nfuzz\nbuzz' | nearust
0,1
1,2
```

### Future plans / extensions
- Allow user to set threshold edit distance (but maybe realistically only up to 2?) e.g. `nearust --distance 2`
- Read from files instead of `stdin` -- this will allow two different files / buffers as input, allowing `nearust` to also handle cases where we want to look for pairs *across* two different multisets e.g. `nearust --anchors file1.txt --comparisons file2.txt`
- Concurrency and parallelism -- use multiple CPU cores for blazingly fast performance while also not requiring a GPU
