use clap::Parser;
use rapidfuzz::distance::levenshtein;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Error, ErrorKind, Read, Write};
use std::sync::mpsc;

/// Minimal CLI utility for fast detection of similar strings using the symdel algorithm.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// The maximum number of edits away to check for neighbours.
    #[arg(short, long, default_value_t = 1)]
    max_edits: usize,
}

/// Reads (blocking) all lines from in_stream until EOF, and converts the data into a vector of
/// Strings where each String is a line from in_stream. Performs symdel to look for String
/// pairs within 1 edit distance. Outputs the detected pairs from symdel into out_stream, where
/// each new line written encodes a detected pair as a pair of 0-indexed indices of the Strings
/// involved separated by a comma, and the lower index is always first.
///
/// Any unrecoverable errors should be written out to err_stream, before the program exits.
///
/// The function accepts the three aforementioned streams as parameters instead of having them
/// directly bound to stdin, stdout and stderr respectively. This is so that the streams can be
/// easily bound to other buffers for the purposes of testing.
fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let args = Args::parse();

    let input_strings = get_input_lines_as_ascii(stdin).unwrap();
    let variant_lookup_table = get_variant_lookup_table(&input_strings, args.max_edits);
    let hit_candidates = get_hit_candidates(&variant_lookup_table);
    write_true_hits(&hit_candidates, &input_strings, args.max_edits, stdout);
}

/// Read lines from in_stream until EOF and collect into vector of byte vectors. Return any
/// errors if trouble reading, or if the input text contains non-ASCII data. The returned vector
/// is guaranteed to only contain ASCII bytes.
fn get_input_lines_as_ascii(in_stream: impl Read) -> Result<Vec<Vec<u8>>, Error> {
    let reader = BufReader::new(in_stream);
    let mut strings = Vec::new();

    for line in reader.lines() {
        let line_as_bytes = line?.into_bytes();

        if !line_as_bytes.is_ascii() {
            return Err(Error::new(ErrorKind::InvalidData, "Input must be valid ASCII"));
        }

        strings.push(line_as_bytes);
    }

    Ok(strings)
}

/// Make hash map of all possible substrings that can be generated from input strings via making
/// deletions up to the threshold level, where the keys are the substrings and the values are
/// vectors of indices corresponding to the input strings from which the substrings can be
/// generated.
fn get_variant_lookup_table(strings: &[Vec<u8>], max_edits: usize) -> FxHashMap<Vec<u8>, Vec<usize>> {
    let mut variant_dict: FxHashMap<Vec<u8>, Vec<usize>> = FxHashMap::default();

    for (idx, s) in strings.iter().enumerate() {
        let variants = get_deletion_variants(s, max_edits).unwrap();
        for variant in variants.iter() {
            let entry = variant_dict.entry(variant.clone()).or_default();
            entry.push(idx);
        }
    };

    variant_dict
}

/// Given an input string, generate all possible strings after making at most max_deletions
/// single-character deletions.
fn get_deletion_variants(input: &[u8], max_deletions: usize) -> Result<Vec<Vec<u8>>, Error> {

    let input_length = input.len();
    if input_length > 255 {
        return Err(Error::new(ErrorKind::InvalidInput, "Input strings longer than 255 characters are unsupported"))
    }

    let mut deletion_variants = Vec::new();
    deletion_variants.push(input.to_vec());

    for num_deletions in 1..=max_deletions {
        if num_deletions > input_length {
            deletion_variants.push(Vec::new());
            break
        }

        for deletion_indices in get_k_combinations(input_length, num_deletions)? {
            let mut variant = Vec::new();
            let mut offset = 0;

            for idx in deletion_indices.iter() {
                variant.extend(&input[offset..*idx]);
                offset = idx + 1;
            }
            variant.extend(&input[offset..input_length]);

            deletion_variants.push(variant);
        }
    }

    deletion_variants.sort_unstable();
    deletion_variants.dedup();

    Ok(deletion_variants)
}

/// Return a vector containing all k-combinations of the integers in the range 0..n.
fn get_k_combinations(n: usize, k: usize) -> Result<Vec<Vec<usize>>, Error> {
    if k > n {
        return Err(Error::new(ErrorKind::InvalidInput, "k cannot be larger than n"))
    }

    let mut combinations: Vec<Vec<usize>> = Vec::new();
    let mut current_combination: Vec<usize> = Vec::new();

    combination_search(n, k, 0, &mut current_combination, &mut combinations);

    Ok(combinations)
}

/// Recursive function used in computing k-combinations.
fn combination_search(n: usize, k: usize, start: usize, current_combination: &mut Vec<usize>, combinations: &mut Vec<Vec<usize>>) {
    if current_combination.len() == k {
        combinations.push(current_combination.clone());
        return
    };

    for idx in start..n {
        current_combination.push(idx);
        combination_search(n, k, idx+1, current_combination, combinations);
        current_combination.pop();
    };
}

/// iterate through the hashmap generated above and collect all candidates for hits
fn get_hit_candidates(variant_lookup_table: &FxHashMap<Vec<u8>, Vec<usize>>) -> Vec<(usize, usize)> {
    let mut hit_candidates = Vec::new();
    let (transmitter, receiver) = mpsc::channel();

    variant_lookup_table.par_iter().for_each_with(transmitter, |transmitter, (_, indices)| {
        let combs = match get_k_combinations(indices.len(), 2) {
            Ok(v) => v,
            Err(_) => return
        };
        let pairs: Vec<_> = combs.iter().map(|comb| {
            (indices[comb[0]], indices[comb[1]])
        }).collect();
        transmitter.send(pairs).unwrap();
    });

    for pairs in receiver {
        for pair in pairs {
            hit_candidates.push(pair);
        }
    }

    hit_candidates.par_sort_unstable();
    hit_candidates.dedup();

    hit_candidates
}

/// Examine and double check hits to see if they are real (this will require an 
/// implementation of Levenshtein distance)
fn write_true_hits(hit_candidates: &[(usize, usize)], strings: &[Vec<u8>], max_edits: usize, out_stream: impl Write) {
    let mut writer = BufWriter::new(out_stream);

    let true_hits: Vec<(usize, usize, usize)> = hit_candidates.par_iter().map(|(anchor_idx, comparison_idx)| {
        let anchor = &strings[*anchor_idx];
        let comparison = &strings[*comparison_idx];
        let dist = if (anchor.len() > comparison.len() && anchor.len() - comparison.len() == max_edits) ||
                      (anchor.len() < comparison.len() && comparison.len() - anchor.len() == max_edits) {
            max_edits
        } else {
            levenshtein::distance(anchor, comparison)
        };

        (*anchor_idx, *comparison_idx, dist)
    }).collect();

    for (a_idx, c_idx, dist) in true_hits.iter().filter(|(_,_,d)| *d <= max_edits) {
        write!(&mut writer, "{a_idx},{c_idx},{dist}\n").unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_input_lines_as_ascii() {
        let strings = get_input_lines_as_ascii(&mut "foo\nbar\nbaz\n".as_bytes()).unwrap();
        let expected: Vec<Vec<u8>> = vec!["foo".into(), "bar".into(), "baz".into()];
        assert_eq!(strings, expected);
    }

    #[test]
    fn test_get_k_combinations() {
        let combinations = get_k_combinations(3, 2).unwrap();
        let expected = vec![
            vec![0,1],
            vec![0,2],
            vec![1,2]
        ];
        assert_eq!(combinations, expected);

        let error = get_k_combinations(2, 3);
        assert!(matches!(error, Err(_)))
    }

    #[test]
    fn test_get_deletion_variants() {
        let variants = get_deletion_variants(b"foo", 1).unwrap();
        let mut expected: Vec<Vec<u8>> = Vec::new();
        expected.push("fo".into());
        expected.push("foo".into());
        expected.push("oo".into());
        assert_eq!(variants, expected);

        let variants = get_deletion_variants(b"foo", 2).unwrap();
        let mut expected: Vec<Vec<u8>> = Vec::new();
        expected.push("f".into());
        expected.push("fo".into());
        expected.push("foo".into());
        expected.push("o".into());
        expected.push("oo".into());
        assert_eq!(variants, expected);
    }
}
