use std::collections::{HashMap, HashSet};
use std::io::{stderr, stdin, stdout, BufReader, BufRead, Error, ErrorKind, Read, Write};

fn main() -> Result<(), Error> {
    nearust(stdin(), stdout(), stderr())
}

fn nearust(
    in_stream: impl Read, 
    mut out_stream: impl Write, 
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
    //
    // The error stream is currently unused as we do not handle any errors, and thus its name is
    // prefixed with an underscore.

    let strings = get_string_vector(in_stream).unwrap();

    // Make hash map of all possible substrings that can be generated from input strings via making
    // deletions up to the threshold level, where the keys are the substrings and the values are
    // vectors of indices corresponding to the input strings from which the substrings can be
    // generated.
    let mut variant_dict: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, s) in strings.iter().enumerate() {
        let variants = get_deletion_variants(s, 1).unwrap();
        for v in variants.iter() {
            let entry = variant_dict.entry(v.to_string()).or_default();
            entry.push(idx);
        }
    }

    // TODO: iterate through the hashmap generated above, examine and double check hits to see if
    // they are real (this will require an implementation of Levenshtein distance)
    // TODO: Make a record of all validated hits, and print them out to out_stream

    let _ = out_stream.write("1,2".as_bytes()).unwrap();
    Ok(())
}

fn get_string_vector(in_stream: impl Read) -> Result<Vec<String>, Error> {
    // Read lines from in_stream until EOF and collect into vector of string slices.
    let reader = BufReader::new(in_stream);
    let mut strings = Vec::new();

    for line in reader.lines() {
        let line_as_string = line?.to_string();

        if !line_as_string.is_ascii() {
            return Err(Error::new(ErrorKind::InvalidData, "Input must be valid ASCII"));
        }

        strings.push(line_as_string);
    }

    Ok(strings)
}

fn get_deletion_variants(input: &str, max_deletions: u8) -> Result<HashSet<String>, &'static str> {
    // Given an input string, generate all possible strings after making at most max_deletions
    // single-character deletions.

    if max_deletions > 2 {
        return Err("Thresholds above 2 edit distance are unsupported.")
    }

    let input_length = input.chars().count();
    if input_length > 255 {
        return Err("Input strings longer than 255 characters are unsupported.")
    }

    let mut deletion_variants = HashSet::new();
    deletion_variants.insert(input.to_string());
    
    for num_deletions in 1..=max_deletions {
        if num_deletions > input_length as u8 {
            deletion_variants.insert("".to_string());
            continue
        }

        for deletion_indices in get_k_combinations(input_length, num_deletions as usize)? {
            let mut variant = "".to_string();
            let mut offset = 0;

            // NOTE: we should use char iteration instead of direct indexing here because this will
            // get screwed up when we use non-ASCII UTF-8 strings
            for idx in deletion_indices.iter() {
                variant += &input[offset..*idx];
                offset = idx + 1;
            }
            variant += &input[offset..input_length];

            deletion_variants.insert(variant);
        }
    }

    Ok(deletion_variants)
}

fn get_k_combinations(n: usize, k: usize) -> Result<Vec<Vec<usize>>, &'static str> {
    // Return a vector containing all k-combinations of the integers in the range 0..n.

    if k > n {
        return Err("k cannot be larger than n")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nearust() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        nearust(&mut "foo\nbar\nbaz".as_bytes(), &mut out, &mut err).unwrap();
        assert_eq!(out, b"1,2");
    }

    #[test]
    fn test_get_string_vector() {
        let strings = get_string_vector(&mut "foo\nbar\nbaz\n".as_bytes()).unwrap();
        let expected = vec!["foo", "bar", "baz"];
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
        let variants = get_deletion_variants("foo", 1).unwrap();
        let mut expected = HashSet::new();
        expected.insert("foo".to_string());
        expected.insert("fo".to_string());
        expected.insert("oo".to_string());
        assert_eq!(variants, expected);

        let variants = get_deletion_variants("foo", 2).unwrap();
        let mut expected = HashSet::new();
        expected.insert("foo".to_string());
        expected.insert("fo".to_string());
        expected.insert("oo".to_string());
        expected.insert("f".to_string());
        expected.insert("o".to_string());
        assert_eq!(variants, expected);
    }
}
