use clap::Parser;
use itertools::Itertools;
use rapidfuzz::distance::levenshtein;
use rayon::{prelude::*, ThreadPoolBuilder};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Error, ErrorKind, Write};
use std::sync::mpsc;

/// Minimal CLI utility for fast detection of nearest neighbour strings that fall within a
/// threshold edit distance.
///
/// The program reads from the standard input until an EOF signal is reached, where each new line
/// is considered to represent a distinct input string. All input must be valid ASCII. The program
/// detects all pairs of input strings that are at most <MAX_DISTANCE> (default=1) edits away from
/// one another, and prints them out to standard output. Each line in the program's output contains
/// three integers separated with a comma, where the first two integers represent the (1-indexed)
/// line numbers in the input data corresponding to the two neighbour strings, and the third number
/// corresponds to the number of edits (Levenshtein distance) between them.
///
/// Example: echo $'fizz\nfuzz\nbuzz' | nearust -d 2 > results.txt
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// The maximum (Levenshtein) edit distance away to check for neighbours.
    #[arg(short='d', long, default_value_t = 1)]
    max_distance: usize,

    /// The number of OS threads the program spawns (if 0 spawns one thread per CPU core).
    #[arg(short, long, default_value_t = 0)]
    num_threads: usize,

    /// Primary input file (if absent program reads from stdin until EOF).
    file_primary: Option<String>,

    /// If provided, searches for pairs of similar strings between the primary input file and the
    /// comparison input file.
    file_comparison: Option<String>
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
    let stdout = io::stdout().lock();
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
        },
        None => {
            let stdin = io::stdin().lock();
            get_input_lines_as_ascii(stdin)
                .unwrap_or_else(|e| panic!("(from stdin) {}", e.to_string()))
        }
    };

    if let Some(path) = args.file_comparison {
        let comparison_reader = get_file_bufreader(&path);
        let comparison_input = get_input_lines_as_ascii(comparison_reader)
            .unwrap_or_else(|e| panic!("(from {}) {}", &path, e.to_string()));
        let hit_candidates = get_hit_candidates_cross(&primary_input, &comparison_input, args.max_distance);
        write_true_hits_cross(&hit_candidates, &primary_input, &comparison_input, args.max_distance, stdout);
    } else {
        let hit_candidates = get_hit_candidates(&primary_input, args.max_distance);
        write_true_hits(&hit_candidates, &primary_input, args.max_distance, stdout);
    }
}

/// Get a buffered reader to a file at path.
fn get_file_bufreader(path: &str) -> BufReader<File> {
    let file = File::open(&path)
        .unwrap_or_else(|e| panic!("failed to open {}: {}", &path, e.to_string()));
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
            let err_msg = format!("input line {}: contains non-ASCII data", idx+1);
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

fn get_hit_candidates(strings: &[String], max_edits: usize) -> Vec<(usize, usize)> {
    let mut variant_index_pairs = Vec::new();
    let (transmitter, receiver) = mpsc::channel();

    strings
        .par_iter()
        .enumerate()
        .for_each_with(transmitter, |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_edits);
            transmitter.send((idx, variants)).unwrap();
        });

    for (idx, mut variants) in receiver {
        for variant in variants.drain(..) {
            variant_index_pairs.push((variant, idx));
        }
    }

    variant_index_pairs.par_sort_unstable();

    let mut hit_candidates = Vec::new();
    for indices in variant_index_pairs.chunk_by(|(v1, _), (v2, _)| v1 == v2) {
        if indices.len() == 1 {
            continue
        }
        indices
            .iter()
            .map(|(_, idx)| *idx)
            .combinations(2)
            .for_each(|v| hit_candidates.push((v[0], v[1])));
    }

    hit_candidates.par_sort_unstable();
    hit_candidates.dedup();
    hit_candidates
}

fn get_hit_candidates_cross(strings_primary: &[String], strings_comparison: &[String], max_edits: usize) -> Vec<(usize, usize)> {
    let mut variant_index_pairs = Vec::new();
    let (transmitter, receiver) = mpsc::channel();

    strings_primary
        .par_iter()
        .enumerate()
        .for_each_with(transmitter.clone(), |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_edits);
            transmitter
                .send((CrossComparisonIndex::Primary(idx), variants))
                .unwrap();
        });

    strings_comparison
        .par_iter()
        .enumerate()
        .for_each_with(transmitter, |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_edits);
            transmitter
                .send((CrossComparisonIndex::Comparison(idx), variants))
                .unwrap();
        });

    for (idx, mut variants) in receiver {
        for variant in variants.drain(..) {
            variant_index_pairs.push((variant, idx));
        }
    }

    variant_index_pairs.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut index_pairs = Vec::new();
    for indices in variant_index_pairs.chunk_by(|(v1, _), (v2, _)| v1 == v2) {
        let mut primary_indices = Vec::new();
        let mut comparison_indices = Vec::new();
        
        indices.iter().for_each(|(_, idx)| {
            match idx {
                CrossComparisonIndex::Primary(v) => primary_indices.push(*v),
                CrossComparisonIndex::Comparison(v) => comparison_indices.push(*v),
            }
        });

        for pair in primary_indices.into_iter().cartesian_product(comparison_indices) {
            index_pairs.push(pair);
        }
    }

    index_pairs.par_sort_unstable();
    index_pairs.dedup();
    index_pairs
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
            break
        }
        
        for deletion_indices in (0..input_length).combinations(num_deletions) {
            let mut variant = String::with_capacity(input_length-num_deletions);
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

/// Examine and double check hits to see if they are real (this will require an 
/// implementation of Levenshtein distance)
fn write_true_hits(hit_candidates: &[(usize, usize)], strings: &[String], max_edits: usize, out_stream: impl Write) {
    let mut writer = BufWriter::new(out_stream);

    let true_hits: Vec<(usize, usize, usize)> = hit_candidates.par_iter().map(|(anchor_idx, comparison_idx)| {
        let anchor = &strings[*anchor_idx];
        let comparison = &strings[*comparison_idx];
        let dist = if (anchor.len() > comparison.len() && anchor.len() - comparison.len() == max_edits) ||
                      (anchor.len() < comparison.len() && comparison.len() - anchor.len() == max_edits) {
            max_edits
        } else {
            levenshtein::distance(anchor.chars(), comparison.chars())
        };

        (*anchor_idx, *comparison_idx, dist)
    }).collect();

    for (a_idx, c_idx, dist) in true_hits.iter().filter(|(_,_,d)| *d <= max_edits) {
        // Add one to both anchor and comparison indices as line numbers are 1-indexed, not
        // 0-indexed
        write!(&mut writer, "{},{},{}\n", a_idx+1, c_idx+1, dist).unwrap();
    }
}

fn write_true_hits_cross(hit_candidates: &[(usize, usize)], strings_primary: &[String], strings_comparison: &[String], max_edits: usize, out_stream: impl Write) {
    let mut writer = BufWriter::new(out_stream);

    let candidates_with_dist: Vec<(usize, usize, usize)> = hit_candidates
        .par_iter()
        .map(|(idx_primary, idx_comparison)| {
            let anchor = &strings_primary[*idx_primary];
            let comparison = &strings_comparison[*idx_comparison];
            let dist = if (anchor.len() > comparison.len() && anchor.len() - comparison.len() == max_edits) ||
                          (anchor.len() < comparison.len() && comparison.len() - anchor.len() == max_edits) {
                max_edits
            } else {
                levenshtein::distance(anchor.chars(), comparison.chars())
            };

            (*idx_primary, *idx_comparison, dist)
        })
        .collect();

    for (a_idx, c_idx, dist) in candidates_with_dist {
        if dist > max_edits {
            continue
        }
        // Add one to both anchor and comparison indices as line numbers are 1-indexed, not
        // 0-indexed
        write!(&mut writer, "{},{},{}\n", a_idx+1, c_idx+1, dist).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_input_lines_as_ascii() {
        let strings = get_input_lines_as_ascii(&mut "foo\nbar\nbaz\n".as_bytes()).unwrap();
        let expected: Vec<String> = vec!["foo".into(), "bar".into(), "baz".into()];
        assert_eq!(strings, expected);
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
}
