use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::io::{stderr, stdin, stdout, BufReader, BufRead, BufWriter, Error, ErrorKind, Read, Write};

fn main() -> Result<(), Error> {
    nearust(stdin(), stdout(), stderr())
}

fn nearust(
    in_stream: impl Read, 
    out_stream: impl Write, 
    mut _err_stream: impl Write
) -> Result<(), Error> {
    // Reads (blocking) all lines from in_stream until EOF, and converts the data into a vector of
    // Strings where each String is a line from in_stream. Performs symdel to look for String
    // pairs within 1 edit distance. Outputs the detected pairs from symdel into out_stream, where
    // each new line written encodes a detected pair as a pair of 0-indexed indices of the Strings
    // involved separated by a comma, and the lower index is always first.
    //
    // Any unrecoverable errors should be written out to err_stream, before the program exits.
    //
    // The function accepts the three aforementioned streams as parameters instead of having them
    // directly bound to stdin, stdout and stderr respectively. This is so that the streams can be
    // easily bound to other buffers for the purposes of testing.
    let threshold: u8 = 1;

    let input_strings = get_input_lines_as_ascii(in_stream).unwrap();
    let max_num_vars: usize = input_strings.iter().map(|s| get_num_deletion_variants(s.len() as u8, 1)).sum();

    // Make hash map of all possible substrings that can be generated from input strings via making
    // deletions up to the threshold level, where the keys are the substrings and the values are
    // vectors of indices corresponding to the input strings from which the substrings can be
    // generated.
    let mut variant_dict: HashMap<Vec<u8>, Vec<usize>> = HashMap::with_capacity(max_num_vars);
    for (idx, s) in input_strings.iter().enumerate() {
        let variants = get_deletion_variants(s, 1).unwrap();
        for v in variants.iter() {
            let entry = variant_dict.entry(v.clone()).or_default();
            entry.push(idx);
        }
    }

    // iterate through the hashmap generated above and collect all candidates for hits
    let max_num_hit_candidates: usize = variant_dict.iter().map(|(_, inds)| inds.len() * (inds.len() - 1) / 2).sum();
    let mut hit_candidates: HashSet<(usize, usize)> = HashSet::with_capacity(max_num_hit_candidates);
    for (_, indices) in variant_dict.iter() {
        let combs = match get_k_combinations(indices.len(), 2) {
            Ok(v) => v,
            Err(_) => continue
        };
        for pair in combs.iter().map(|comb| {
            (indices[comb[0]], indices[comb[1]])
        }) {
            hit_candidates.insert(pair);
        }
    }

    // Examine and double check hits to see if they are real (this will require an 
    // implementation of Levenshtein distance)
    let mut writer = BufWriter::new(out_stream);
    for hit_candidate in hit_candidates.iter() {
        let anchor_idx = hit_candidate.0;
        let comparison_idx = hit_candidate.1;

        let anchor = &input_strings[hit_candidate.0];
        let comparison = &input_strings[hit_candidate.1];

        if anchor.len() > comparison.len() && anchor.len() - comparison.len() < threshold as usize && levenshtein(anchor, comparison) > threshold {
            continue
        }

        if anchor.len() < comparison.len() && comparison.len() - anchor.len() < threshold as usize && levenshtein(anchor, comparison) > threshold {
            continue
        }

        if anchor.len() == comparison.len() && levenshtein(anchor, comparison) > threshold {
            continue
        }

        write!(&mut writer, "{anchor_idx},{comparison_idx}\n").unwrap();
    }

    Ok(())
}

fn get_input_lines_as_ascii(in_stream: impl Read) -> Result<Vec<Vec<u8>>, Error> {
    // Read lines from in_stream until EOF and collect into vector of byte vectors. Return any
    // errors if trouble reading, or if the input text contains non-ASCII data. The returned vector
    // is guaranteed to only contain ASCII bytes.

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

fn get_num_deletion_variants(input_len: u8, max_deletions: u8) -> usize {
    let input_len = input_len as usize;
    let max_deletions = max_deletions as usize;
    let mut total_num_variants = 0;

    for num_dels in 0..=max_deletions {
        if num_dels == 0 {
            total_num_variants += 1;
            continue
        }

        let num_perms: usize = (input_len-num_dels+1..=input_len).product();
        let num_orderings: usize = (1..=num_dels).product();
        let num_combinations = num_perms / num_orderings;
        total_num_variants += num_combinations;
    }

    total_num_variants
}

fn get_deletion_variants(input: &[u8], max_deletions: u8) -> Result<HashSet<Vec<u8>>, Error> {
    // Given an input string, generate all possible strings after making at most max_deletions
    // single-character deletions.

    if max_deletions > 2 {
        return Err(Error::new(ErrorKind::InvalidInput, "Thresholds above 2 edit distance are unsupported"))
    }

    let input_length = input.len();
    if input_length > 255 {
        return Err(Error::new(ErrorKind::InvalidInput, "Input strings longer than 255 characters are unsupported"))
    }

    let mut deletion_variants = HashSet::new();
    deletion_variants.insert(input.to_vec());

    for num_deletions in 1..=max_deletions {
        if num_deletions > input_length as u8 {
            deletion_variants.insert(Vec::new());
            break
        }

        for deletion_indices in get_k_combinations(input_length, num_deletions as usize)? {
            let mut variant = Vec::new();
            let mut offset = 0;

            for idx in deletion_indices.iter() {
                variant.extend(&input[offset..*idx]);
                offset = idx + 1;
            }
            variant.extend(&input[offset..input_length]);

            deletion_variants.insert(variant);
        }
    }

    Ok(deletion_variants)
}

fn get_k_combinations(n: usize, k: usize) -> Result<Vec<Vec<usize>>, Error> {
    // Return a vector containing all k-combinations of the integers in the range 0..n.

    if k > n {
        return Err(Error::new(ErrorKind::InvalidInput, "k cannot be larger than n"))
    }

    let mut combinations: Vec<Vec<usize>> = Vec::new();
    let mut current_combination: Vec<usize> = Vec::new();

    combination_search(n, k, 0, &mut current_combination, &mut combinations);

    Ok(combinations)
}

fn combination_search(n: usize, k: usize, start: usize, current_combination: &mut Vec<usize>, combinations: &mut Vec<Vec<usize>>) {
    // Recursive function used in computing k-combinations.

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

fn levenshtein(anchor: &[u8], comparison: &[u8]) -> u8 {
    let mut dist_row_prev = [0u8; 255];
    let mut dist_row = [0u8; 255];

    for i in 0..=anchor.len() {
        dist_row_prev[i] = i as u8;
    }

    for j in 1..=comparison.len() {
        dist_row[0] = j as u8;

        for i in 1..=anchor.len() {
            if anchor[i-1] == comparison[j-1] {
                dist_row[i] = dist_row_prev[i-1];
                continue
            }

            let insertion_cost = dist_row_prev[i] + 1;
            let deletion_cost = dist_row[i-1] + 1;
            let substitution_cost = dist_row_prev[i-1] + 1;

            dist_row[i] = min(min(insertion_cost, deletion_cost), substitution_cost);
        }

        for i in 0..=anchor.len() {
            dist_row_prev[i] = dist_row[i];
        }
    }

    return dist_row[anchor.len()];
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;

    #[test]
    fn test_nearust() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        nearust(&mut "foo\nbar\nbaz".as_bytes(), &mut out, &mut err).unwrap();
        assert_eq!(out, b"1,2\n");

        let mut out = Vec::new();
        let mut err = Vec::new();
        nearust(&mut "fizz\nfuzz\nbuzz".as_bytes(), &mut out, &mut err).unwrap();
        assert_eq!(str::from_utf8(&out).unwrap(), "0,1\n1,2\n");
    }

    #[test]
    fn test_get_string_vector() {
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
        let mut expected = HashSet::new();
        expected.insert("foo".into());
        expected.insert("fo".into());
        expected.insert("oo".into());
        assert_eq!(variants, expected);

        let variants = get_deletion_variants(b"foo", 2).unwrap();
        let mut expected = HashSet::new();
        expected.insert("foo".into());
        expected.insert("fo".into());
        expected.insert("oo".into());
        expected.insert("f".into());
        expected.insert("o".into());
        assert_eq!(variants, expected);
    }

    #[test]
    fn test_levenshtein() {
        let result = levenshtein(b"foo", b"bar");
        assert_eq!(result, 3);

        let result = levenshtein(b"kitten", b"sitting");
        assert_eq!(result, 3);
    }

    #[test]
    fn test_get_num_deletion_variatns() {
        let result = get_num_deletion_variants(5, 2);
        assert_eq!(result, 1 + 5 + 10);
    }
}
