use clap::{ArgAction, Parser};
use itertools::Itertools;
use rapidfuzz::distance::levenshtein;
use rayon::{prelude::*, ThreadPoolBuilder};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Error, ErrorKind, Write};
use std::sync::mpsc;

/// Minimal CLI utility for fast detection of nearest neighbour strings that fall within a
/// threshold edit distance.
///
/// If you provide nearust with a path to a [FILE_PRIAMRY], it will read its contents for input.
/// If no path is supplied, nearust will read from the standard input until it receives an EOF signal.
/// Nearust will then look for pairs of similar strings within its input, where each line of text is treated as an individual string.
/// You can also supply nearust with two paths -- a [FILE_PRIMARY] and [FILE_COMPARISON], in which case the program will look for pairs of similar strings across the contents of the two files.
/// Currently, only valid ASCII input is supported.
///
/// By default, the threshold (Levenshtein) edit distance at or below which a pair of strings are considered similar is set at 1.
/// This can be changed by setting the --max-distance option.
///
/// Nearust's output is plain text, where each line encodes a detected pair of similar input strings.
/// Each line is comprised of three integers separated by commas, which represent, in respective order:
/// the (1-indexed) line number of the string from the primary input (i.e. stdin or [FILE_PRIMARY]),
/// the (1-indexed) line number of the string from the secondary input (i.e. stdin or [FILE_PRIMARY] if one input, or [FILE_COMPARISON] if two inputs), and
/// the (Levenshtein) edit distance between the similar strings.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// The maximum (Levenshtein) edit distance away to check for neighbours.
    #[arg(short = 'd', long, default_value_t = 1)]
    max_distance: usize,

    /// The number of OS threads the program spawns (if 0 spawns one thread per CPU core).
    #[arg(short, long, default_value_t = 0)]
    num_threads: usize,

    /// 0-index line numbers in the output.
    #[arg(short, long, action = ArgAction::SetTrue)]
    zero_index: bool,

    /// Primary input file (if absent program reads from stdin until EOF).
    file_primary: Option<String>,

    /// If provided, searches for pairs of similar strings between the primary input file and the
    /// comparison input file.
    file_comparison: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum CrossComparisonIndex {
    Primary(usize),
    Comparison(usize),
}

/// Reads (blocking) all lines from in_stream until EOF, and converts the data into a vector of
/// Strings where each String is a line from in_stream. Performs symdel to look for String
/// pairs within <MAX_DISTANCE> (as read from the CLI arguments, defaults to 1) edit distance.
/// Outputs the detected pairs from symdel into out_stream, where each new line written encodes a
/// detected pair as a pair of 1-indexed line numbers of the input strings involved separated by a
/// comma, and the lower line number is always first.
fn main() {
    let mut stdout = BufWriter::new(io::stdout().lock());
    let args = Args::parse();

    ThreadPoolBuilder::new()
        .num_threads(args.num_threads)
        .build_global()
        .unwrap_or_else(|_| panic!("global thread pool cannot be initialised more than once"));

    let primary_input = match args.file_primary {
        Some(path) => {
            let reader = get_file_bufreader(&path);
            get_input_lines_as_ascii(reader)
                .unwrap_or_else(|e| panic!("(from {}) {}", &path, e.to_string()))
        }
        None => {
            let stdin = io::stdin().lock();
            get_input_lines_as_ascii(stdin)
                .unwrap_or_else(|e| panic!("(from stdin) {}", e.to_string()))
        }
    };

    match args.file_comparison {
        Some(path) => {
            let comparison_reader = get_file_bufreader(&path);
            let comparison_input = get_input_lines_as_ascii(comparison_reader)
                .unwrap_or_else(|e| panic!("(from {}) {}", &path, e.to_string()));

            run_symdel_across_sets(
                &primary_input,
                &comparison_input,
                args.max_distance,
                args.zero_index,
                &mut stdout,
            );
        }
        None => run_symdel_within_set(
            &primary_input,
            args.max_distance,
            args.zero_index,
            &mut stdout,
        ),
    }
}

/// Get a buffered reader to a file at path.
fn get_file_bufreader(path: &str) -> BufReader<File> {
    let file =
        File::open(&path).unwrap_or_else(|e| panic!("failed to open {}: {}", &path, e.to_string()));
    BufReader::new(file)
}

/// Read lines from in_stream until EOF and collect into vector of byte vectors. Return any
/// errors if trouble reading, or if the input text contains non-ASCII data. The returned vector
/// is guaranteed to only contain ASCII bytes.
fn get_input_lines_as_ascii(in_stream: impl BufRead) -> Result<Vec<String>, Error> {
    let mut strings = Vec::new();

    for (idx, line) in in_stream.lines().enumerate() {
        let line_unwrapped = line?;

        if !line_unwrapped.is_ascii() {
            let err_msg = format!("input line {}: contains non-ASCII data", idx + 1);
            return Err(Error::new(ErrorKind::InvalidData, err_msg));
        }

        if line_unwrapped.len() > 255 {
            let err_msg = format!("input line {}: input strings longer than 255 characters are currently not supported", idx+1);
            return Err(Error::new(ErrorKind::InvalidData, err_msg));
        }

        strings.push(line_unwrapped);
    }

    Ok(strings)
}

fn run_symdel_within_set(
    strings: &[String],
    max_edits: usize,
    zero_indexed: bool,
    out_stream: &mut impl Write,
) {
    let num_vi_pairs = get_num_vi_pairs(strings, max_edits);
    let mut variant_index_pairs = Vec::with_capacity(num_vi_pairs);
    let (tx, rx) = mpsc::channel();
    strings
        .par_iter()
        .enumerate()
        .for_each_with(tx, |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_edits);
            transmitter.send((idx, variants)).unwrap();
        });

    for (idx, mut variants) in rx {
        for variant in variants.drain(..) {
            variant_index_pairs.push((variant, idx));
        }
    }

    variant_index_pairs.par_sort_unstable();

    let mut convergent_indices = Vec::new();
    variant_index_pairs
        .chunk_by(|(v1, _), (v2, _)| v1 == v2)
        .for_each(|group| {
            if group.len() == 1 {
                return;
            }
            let indices = group.iter().map(|(_, idx)| *idx).collect_vec();
            convergent_indices.push(indices);
        });

    let num_hit_candidates = get_num_hit_candidates(&convergent_indices);
    let mut hit_candidates = Vec::with_capacity(num_hit_candidates);
    let (tx, rx) = mpsc::channel();
    convergent_indices
        .par_iter()
        .for_each_with(tx, |tx, indices| {
            let pair_tuples = indices
                .iter()
                .combinations(2)
                .map(|v| (*v[0], *v[1]))
                .collect_vec();
            tx.send(pair_tuples).unwrap();
        });

    for pair_tuples in rx {
        for pair in pair_tuples {
            hit_candidates.push(pair);
        }
    }

    hit_candidates.par_sort_unstable();
    hit_candidates.dedup();

    write_true_hits(
        &hit_candidates,
        strings,
        strings,
        max_edits,
        zero_indexed,
        out_stream,
    );
}

fn run_symdel_across_sets(
    strings_primary: &[String],
    strings_comparison: &[String],
    max_edits: usize,
    zero_indexed: bool,
    out_stream: &mut impl Write,
) {
    let num_vi_primary = get_num_vi_pairs(strings_primary, max_edits);
    let num_vi_comparison = get_num_vi_pairs(strings_comparison, max_edits);
    let mut variant_index_pairs = Vec::with_capacity(num_vi_primary + num_vi_comparison);
    let (transmitter, receiver) = mpsc::channel();
    strings_primary.par_iter().enumerate().for_each_with(
        transmitter.clone(),
        |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_edits);
            transmitter
                .send((CrossComparisonIndex::Primary(idx), variants))
                .unwrap();
        },
    );
    strings_comparison.par_iter().enumerate().for_each_with(
        transmitter,
        |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_edits);
            transmitter
                .send((CrossComparisonIndex::Comparison(idx), variants))
                .unwrap();
        },
    );

    for (idx, mut variants) in receiver {
        for variant in variants.drain(..) {
            variant_index_pairs.push((variant, idx));
        }
    }

    variant_index_pairs.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut convergent_indices = Vec::new();
    let mut total_num_index_pairs = 0;
    variant_index_pairs
        .chunk_by(|(v1, _), (v2, _)| v1 == v2)
        .for_each(|group| {
            if group.len() == 1 {
                return;
            }

            let mut indices_primary = Vec::new();
            let mut indices_comparison = Vec::new();

            group.iter().for_each(|(_, idx)| match idx {
                CrossComparisonIndex::Primary(v) => indices_primary.push(*v),
                CrossComparisonIndex::Comparison(v) => indices_comparison.push(*v),
            });

            let num_index_pairs = indices_primary.len() * indices_comparison.len();
            if num_index_pairs == 0 {
                return;
            }

            total_num_index_pairs += num_index_pairs;
            convergent_indices.push((indices_primary, indices_comparison));
        });

    let mut hit_candidates = Vec::with_capacity(total_num_index_pairs);
    let (tx, rx) = mpsc::channel();
    convergent_indices
        .par_iter()
        .for_each_with(tx, |tx, (indices_primary, indices_comparison)| {
            let pair_tuples = indices_primary
                .into_iter()
                .cartesian_product(indices_comparison)
                .map(|v| (*v.0, *v.1))
                .collect_vec();
            tx.send(pair_tuples).unwrap();
        });

    for pair_tuples in rx {
        for pair in pair_tuples {
            hit_candidates.push(pair);
        }
    }

    hit_candidates.par_sort_unstable();
    hit_candidates.dedup();

    write_true_hits(
        &hit_candidates,
        strings_primary,
        strings_comparison,
        max_edits,
        zero_indexed,
        out_stream,
    );
}

fn get_num_vi_pairs(strings: &[String], max_edits: usize) -> usize {
    strings
        .iter()
        .map(|s| {
            (0..max_edits)
                .map(|k| get_num_k_combs(s.len(), k))
                .sum::<usize>()
        })
        .sum()
}

fn get_num_k_combs(n: usize, k: usize) -> usize {
    assert!(n > 0);
    assert!(n >= k);

    if k == 0 {
        return 1;
    }

    let num_subsamples: usize = (n - k + 1..=n).product();
    let subsample_perms: usize = (1..=k).product();

    return num_subsamples / subsample_perms;
}

fn get_num_hit_candidates(convergent_indices: &[Vec<usize>]) -> usize {
    convergent_indices
        .iter()
        .map(|indices| get_num_k_combs(indices.len(), 2))
        .sum()
}

/// Given an input string, generate all possible strings after making at most max_deletions
/// single-character deletions.
fn get_deletion_variants(input: &str, max_deletions: usize) -> Vec<String> {
    let input_length = input.len();

    let mut deletion_variants = Vec::new();
    deletion_variants.push(input.to_string());

    for num_deletions in 1..=max_deletions {
        if num_deletions > input_length {
            deletion_variants.push("".to_string());
            break;
        }

        for deletion_indices in (0..input_length).combinations(num_deletions) {
            let mut variant = String::with_capacity(input_length - num_deletions);
            let mut offset = 0;

            for idx in deletion_indices.iter() {
                variant.push_str(&input[offset..*idx]);
                offset = idx + 1;
            }
            variant.push_str(&input[offset..input_length]);

            deletion_variants.push(variant);
        }
    }

    deletion_variants.sort_unstable();
    deletion_variants.dedup();

    deletion_variants
}

/// Examine and double check hits to see if they are real
fn write_true_hits(
    hit_candidates: &[(usize, usize)],
    strings_primary: &[String],
    strings_comparison: &[String],
    max_edits: usize,
    zero_indexed: bool,
    writer: &mut impl Write,
) {
    let candidates_with_dist: Vec<(usize, usize, usize)> = hit_candidates
        .par_iter()
        .map(|(idx_primary, idx_comparison)| {
            let anchor = &strings_primary[*idx_primary];
            let comparison = &strings_comparison[*idx_comparison];
            let dist = if (anchor.len() > comparison.len()
                && anchor.len() - comparison.len() == max_edits)
                || (anchor.len() < comparison.len() && comparison.len() - anchor.len() == max_edits)
            {
                max_edits
            } else {
                levenshtein::distance(anchor.chars(), comparison.chars())
            };

            (*idx_primary, *idx_comparison, dist)
        })
        .collect();

    for (a_idx, c_idx, dist) in candidates_with_dist {
        if dist > max_edits {
            continue;
        }

        let (a_idx_to_write, c_idx_to_write) = if zero_indexed {
            (a_idx, c_idx)
        } else {
            (a_idx + 1, c_idx + 1)
        };

        write!(writer, "{},{},{}\n", a_idx_to_write, c_idx_to_write, dist).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::Read;

    #[test]
    fn test_get_input_lines_as_ascii() {
        let strings = get_input_lines_as_ascii(&mut "foo\nbar\nbaz\n".as_bytes()).unwrap();
        let expected: Vec<String> = vec!["foo".into(), "bar".into(), "baz".into()];
        assert_eq!(strings, expected);
    }

    #[test]
    fn test_get_num_k_combinations() {
        let result = get_num_k_combs(5, 2);
        assert_eq!(result, 10);

        let result = get_num_k_combs(5, 0);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_get_deletion_variants() {
        let variants = get_deletion_variants("foo", 1);
        let mut expected: Vec<String> = Vec::new();
        expected.push("fo".into());
        expected.push("foo".into());
        expected.push("oo".into());
        assert_eq!(variants, expected);

        let variants = get_deletion_variants("foo", 2);
        let mut expected: Vec<String> = Vec::new();
        expected.push("f".into());
        expected.push("fo".into());
        expected.push("foo".into());
        expected.push("o".into());
        expected.push("oo".into());
        assert_eq!(variants, expected);
    }

    #[test]
    fn test_get_num_hit_candidates() {
        let convergent_indices = &[vec![1, 2, 3], vec![1, 2, 3, 4], vec![1, 2]];
        let result = get_num_hit_candidates(convergent_indices);
        assert_eq!(result, 10);
    }

    /// Run this test from the project home directory so that the test CDR3 text files can be found
    /// at the expected paths
    #[test]
    fn test_within() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let test_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_a.txt").unwrap());
        let mut expected_output = Vec::new();
        let _ = f.read_to_end(&mut expected_output);

        let mut test_output_stream = Vec::new();

        run_symdel_within_set(&test_input, 1, false, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);
    }

    /// Run this test from the project home directory so that the test CDR3 text files can be found
    /// at the expected paths
    #[test]
    fn test_cross() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let primary_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/cdr3b_10k_b.txt").unwrap());
        let comparison_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_cross.txt").unwrap());
        let mut expected_output = Vec::new();
        let _ = f.read_to_end(&mut expected_output);

        let mut test_output_stream = Vec::new();

        run_symdel_across_sets(
            &primary_input,
            &comparison_input,
            1,
            false,
            &mut test_output_stream,
        );

        assert_eq!(test_output_stream, expected_output);
    }
}
