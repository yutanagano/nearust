use core::hash::{BuildHasher, Hasher};
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use itertools::Itertools;
use rapidfuzz::distance::levenshtein;
use rayon::prelude::*;
use std::io;
use std::io::{BufRead, Error, ErrorKind::InvalidData, Write};
use std::usize;
use std::{hash::Hash, sync::mpsc};

mod pymod;

#[derive(Debug, Clone, Copy)]
enum CrossComparisonIndex {
    Query(usize),
    Reference(usize),
}

/// Class for assymetric cross-set symdel where the reference is known beforehand, and a variant
/// hashmap (mapping deletion variants to all the original strings that could have produced that
/// variant) can be computed beforehand to expedite multiple future queries against that same
/// reference.
pub struct CachedSymdel {
    reference: Vec<String>,
    variant_map: HashMap<String, Vec<usize>>,
    max_distance: usize,
}

impl CachedSymdel {
    pub fn new(reference: Vec<String>, max_distance: usize) -> Self {
        let mut variant_map = HashMap::new();
        let hash_builder = variant_map.hasher();
        let (tx, rx) = mpsc::channel();

        reference
            .par_iter()
            .enumerate()
            .for_each_with(tx, |transmitter, (idx, s)| {
                let variants_and_hashes = get_deletion_variants(s, max_distance)
                    .into_iter()
                    .map(|v| {
                        let mut state = hash_builder.build_hasher();
                        v.hash(&mut state);
                        (v, state.finish())
                    })
                    .collect_vec();
                transmitter.send((idx, variants_and_hashes)).unwrap();
            });

        for (idx, mut variants_and_hashes) in rx {
            for (variant, precomputed_hash) in variants_and_hashes.drain(..) {
                match variant_map
                    .raw_entry_mut()
                    .from_key_hashed_nocheck(precomputed_hash, &variant)
                {
                    RawEntryMut::Vacant(view) => {
                        view.insert_hashed_nocheck(precomputed_hash, variant, vec![idx]);
                    }
                    RawEntryMut::Occupied(mut view) => {
                        let v = view.get_mut();
                        v.push(idx);
                        v.sort_unstable();
                    }
                }
            }
        }

        CachedSymdel {
            reference,
            variant_map,
            max_distance,
        }
    }

    pub fn symdel_within(
        &self,
        max_distance: usize,
        zero_index: bool,
    ) -> io::Result<Vec<(usize, usize, usize)>> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance, self.max_distance)));
        }

        let mut convergent_indices = Vec::new();
        let (tx, rx) = mpsc::channel();
        self.variant_map
            .par_iter()
            .for_each_with(tx, |transmitter, (_, indices)| {
                if indices.len() == 1 {
                    return;
                }
                transmitter.send(indices).unwrap();
            });

        for indices in rx {
            convergent_indices.push(indices);
        }

        let num_hit_candidates = get_num_hit_candidates(&convergent_indices);
        let mut hit_candidates = Vec::with_capacity(num_hit_candidates);
        let (tx, rx) = mpsc::channel();
        convergent_indices
            .par_iter()
            .for_each_with(tx, |transmitter, indices| {
                let pair_tuples = indices
                    .iter()
                    .combinations(2)
                    .map(|v| (*v[0], *v[1]))
                    .collect_vec();
                transmitter.send(pair_tuples).unwrap();
            });

        for pair_tuples in rx {
            for pair in pair_tuples {
                hit_candidates.push(pair);
            }
        }

        hit_candidates.par_sort_unstable();
        hit_candidates.dedup();

        Ok(get_true_hits(
            hit_candidates,
            &self.reference,
            &self.reference,
            max_distance,
            zero_index,
        ))
    }

    pub fn symdel_cross(
        &self,
        query: &[String],
        max_distance: usize,
        zero_index: bool,
    ) -> io::Result<Vec<(usize, usize, usize)>> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance, self.max_distance)));
        }

        let num_vi_pairs = get_num_vi_pairs(query, max_distance);
        let mut variant_index_pairs = Vec::with_capacity(num_vi_pairs);
        let (tx, rx) = mpsc::channel();
        query
            .par_iter()
            .enumerate()
            .for_each_with(tx, |transmitter, (idx, s)| {
                let variants = get_deletion_variants(s, max_distance);
                transmitter.send((idx, variants)).unwrap();
            });

        for (idx, mut variants) in rx {
            for variant in variants.drain(..) {
                variant_index_pairs.push((variant, idx));
            }
        }

        variant_index_pairs.par_sort_unstable();

        let mut convergent_indices = Vec::new();
        let mut total_num_index_pairs = 0;
        variant_index_pairs
            .chunk_by(|(v1, _), (v2, _)| v1 == v2)
            .for_each(|group| {
                let variant = &group[0].0;
                match self.variant_map.get(variant) {
                    None => return,
                    Some(indices_ref) => {
                        let indices_query = group.iter().map(|(_, idx)| *idx).collect_vec();
                        total_num_index_pairs += indices_query.len() * indices_ref.len();
                        convergent_indices.push((indices_query, indices_ref));
                    }
                }
            });

        let mut hit_candidates = Vec::with_capacity(total_num_index_pairs);
        let (tx, rx) = mpsc::channel();
        convergent_indices
            .par_iter()
            .for_each_with(tx, |tx, (indices_query, indices_ref)| {
                let pair_tuples = indices_query
                    .into_iter()
                    .cartesian_product(*indices_ref)
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

        Ok(get_true_hits(
            hit_candidates,
            query,
            &self.reference,
            max_distance,
            zero_index,
        ))
    }

    pub fn symdel_cross_against_cached(
        &self,
        query: &Self,
        max_distance: usize,
        zero_index: bool,
    ) -> io::Result<Vec<(usize, usize, usize)>> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance, self.max_distance)));
        }

        if max_distance > query.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the query ({})", max_distance, query.max_distance)));
        }

        let mut convergent_indices = Vec::new();
        let mut total_num_index_pairs = 0;
        let (tx, rx) = mpsc::channel();
        if query.variant_map.len() < self.variant_map.len() {
            query.variant_map.par_iter().for_each_with(
                tx,
                |transmitter, (variant, indices_query)| match self.variant_map.get(variant) {
                    None => return,
                    Some(indices_ref) => {
                        let num_index_pairs = indices_query.len() * indices_ref.len();
                        transmitter
                            .send((num_index_pairs, (indices_query, indices_ref)))
                            .unwrap();
                    }
                },
            );
        } else {
            self.variant_map
                .par_iter()
                .for_each_with(tx, |transmitter, (variant, indices_ref)| {
                    match query.variant_map.get(variant) {
                        None => return,
                        Some(indices_query) => {
                            let num_index_pairs = indices_query.len() * indices_ref.len();
                            transmitter
                                .send((num_index_pairs, (indices_query, indices_ref)))
                                .unwrap();
                        }
                    }
                });
        }

        for (num_index_pairs, indices) in rx {
            total_num_index_pairs += num_index_pairs;
            convergent_indices.push(indices);
        }

        let mut hit_candidates = Vec::with_capacity(total_num_index_pairs);
        let (tx, rx) = mpsc::channel();
        convergent_indices
            .par_iter()
            .for_each_with(tx, |tx, (indices_query, indices_ref)| {
                let pair_tuples = indices_query
                    .into_iter()
                    .cartesian_product(*indices_ref)
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

        Ok(get_true_hits(
            hit_candidates,
            &query.reference,
            &self.reference,
            max_distance,
            zero_index,
        ))
    }
}

pub fn symdel_within(
    query: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let num_vi_pairs = get_num_vi_pairs(query, max_distance);
    let mut variant_index_pairs = Vec::with_capacity(num_vi_pairs);
    let (tx, rx) = mpsc::channel();
    query
        .par_iter()
        .enumerate()
        .for_each_with(tx, |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_distance);
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

    get_true_hits(hit_candidates, query, query, max_distance, zero_index)
}

pub fn symdel_cross(
    query: &[String],
    reference: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let num_vi_query = get_num_vi_pairs(query, max_distance);
    let num_vi_reference = get_num_vi_pairs(reference, max_distance);
    let mut variant_index_pairs = Vec::with_capacity(num_vi_query + num_vi_reference);
    let (tx, rx) = mpsc::channel();
    query
        .par_iter()
        .enumerate()
        .for_each_with(tx.clone(), |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_distance);
            transmitter
                .send((CrossComparisonIndex::Query(idx), variants))
                .unwrap();
        });
    reference
        .par_iter()
        .enumerate()
        .for_each_with(tx, |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_distance);
            transmitter
                .send((CrossComparisonIndex::Reference(idx), variants))
                .unwrap();
        });

    for (idx, mut variants) in rx {
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

            let mut indices_query = Vec::new();
            let mut indices_reference = Vec::new();

            group.iter().for_each(|(_, idx)| match idx {
                CrossComparisonIndex::Query(v) => indices_query.push(*v),
                CrossComparisonIndex::Reference(v) => indices_reference.push(*v),
            });

            let num_index_pairs = indices_query.len() * indices_reference.len();
            if num_index_pairs == 0 {
                return;
            }

            total_num_index_pairs += num_index_pairs;
            convergent_indices.push((indices_query, indices_reference));
        });

    let mut hit_candidates = Vec::with_capacity(total_num_index_pairs);
    let (tx, rx) = mpsc::channel();
    convergent_indices
        .par_iter()
        .for_each_with(tx, |tx, (indices_query, indices_reference)| {
            let pair_tuples = indices_query
                .into_iter()
                .cartesian_product(indices_reference)
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

    get_true_hits(hit_candidates, query, reference, max_distance, zero_index)
}

fn get_num_vi_pairs(strings: &[String], max_distance: usize) -> usize {
    strings
        .iter()
        .map(|s| {
            let mut num_vi_pairs = 0;
            for k in 0..=max_distance {
                if k > s.len() {
                    break;
                }
                num_vi_pairs += get_num_k_combs(s.len(), k);
            }
            num_vi_pairs
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

fn get_num_hit_candidates<T>(convergent_indices: &[T]) -> usize
where
    T: AsRef<[usize]>,
{
    convergent_indices
        .iter()
        .map(|indices| get_num_k_combs(indices.as_ref().len(), 2))
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
fn get_true_hits(
    hit_candidates: Vec<(usize, usize)>,
    query: &[String],
    reference: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let candidates_with_dist: Vec<(usize, usize, usize)> = hit_candidates
        .into_par_iter()
        .map(|(idx_query, idx_reference)| {
            let string_query = &query[idx_query];
            let string_reference = &reference[idx_reference];
            let dist = if (string_query.len() > string_reference.len()
                && string_query.len() - string_reference.len() == max_distance)
                || (string_query.len() < string_reference.len()
                    && string_reference.len() - string_query.len() == max_distance)
            {
                max_distance
            } else {
                levenshtein::distance(string_query.chars(), string_reference.chars())
            };

            (idx_query, idx_reference, dist)
        })
        .collect();

    candidates_with_dist
        .into_iter()
        .filter(|(_, _, dist)| *dist <= max_distance)
        .map(|(qidx, refidx, dist)| {
            if zero_index {
                (qidx, refidx, dist)
            } else {
                (qidx + 1, refidx + 1, dist)
            }
        })
        .collect_vec()
}

/// Read lines from in_stream until EOF and collect into vector of byte vectors. Return any
/// errors if trouble reading, or if the input text contains non-ASCII data. The returned vector
/// is guaranteed to only contain ASCII bytes.
pub fn get_input_lines_as_ascii(in_stream: impl BufRead) -> Result<Vec<String>, Error> {
    let mut strings = Vec::new();

    for (idx, line) in in_stream.lines().enumerate() {
        let line_unwrapped = line?;

        if !line_unwrapped.is_ascii() {
            let err_msg = format!(
                "non-ASCII data is currently unsupported (\"{}\" from input line {})",
                line_unwrapped,
                idx + 1
            );
            return Err(Error::new(InvalidData, err_msg));
        }

        strings.push(line_unwrapped);
    }

    Ok(strings)
}

/// Write to stdout
pub fn write_results(results: Vec<(usize, usize, usize)>, writer: &mut impl Write) {
    for (a_idx_to_write, c_idx_to_write, dist) in results.iter() {
        write!(writer, "{},{},{}\n", a_idx_to_write, c_idx_to_write, dist).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{BufReader, Read};

    #[test]
    fn test_get_num_k_combinations() {
        let result = get_num_k_combs(5, 2);
        assert_eq!(result, 10);

        let result = get_num_k_combs(5, 0);
        assert_eq!(result, 1);
    }

    #[test]
    fn test_get_num_vi_pairs() {
        let strings = ["foo".to_string(), "bar".to_string(), "baz".to_string()];
        let result = get_num_vi_pairs(&strings, 1);
        assert_eq!(result, 12);
    }

    #[test]
    fn test_get_deletion_variants() {
        let variants = get_deletion_variants("foo", 1);
        let expected = vec!["fo".to_string(), "foo".to_string(), "oo".to_string()];
        assert_eq!(variants, expected);

        let variants = get_deletion_variants("foo", 2);
        let expected = vec![
            "f".to_string(),
            "fo".to_string(),
            "foo".to_string(),
            "o".to_string(),
            "oo".to_string(),
        ];
        assert_eq!(variants, expected);

        let variants = get_deletion_variants("foo", 10);
        let expected = vec![
            "".to_string(),
            "f".to_string(),
            "fo".to_string(),
            "foo".to_string(),
            "o".to_string(),
            "oo".to_string(),
        ];
        assert_eq!(variants, expected);
    }

    #[test]
    fn test_get_num_hit_candidates() {
        let convergent_indices = &[vec![1, 2, 3], vec![1, 2, 3, 4], vec![1, 2]];
        let result = get_num_hit_candidates(convergent_indices);
        assert_eq!(result, 10);
    }

    #[test]
    fn test_get_input_lines_as_ascii() {
        let strings = get_input_lines_as_ascii(&mut "foo\nbar\nbaz\n".as_bytes()).unwrap();
        let expected: Vec<String> = vec!["foo".into(), "bar".into(), "baz".into()];
        assert_eq!(strings, expected);
    }

    /// Run the following tests from the project home directory so that the test CDR3 text files
    /// can be found at the expected paths
    #[test]
    fn test_within() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let test_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_a.txt").unwrap());
        let mut expected_output = Vec::new();
        f.read_to_end(&mut expected_output).unwrap();

        let mut test_output_stream = Vec::new();
        let results = symdel_within(&test_input, 1, false);
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);

        expected_output.clear();
        test_output_stream.clear();

        let mut f = BufReader::new(File::open("test_files/results_10k_a_d2.txt").unwrap());
        f.read_to_end(&mut expected_output).unwrap();

        let results = symdel_within(&test_input, 2, false);
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output)
    }

    #[test]
    fn test_cross() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let primary_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/cdr3b_10k_b.txt").unwrap());
        let comparison_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_cross.txt").unwrap());
        let mut expected_output = Vec::new();
        f.read_to_end(&mut expected_output).unwrap();

        let mut test_output_stream = Vec::new();
        let results = symdel_cross(&primary_input, &comparison_input, 1, false);
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);

        expected_output.clear();
        test_output_stream.clear();

        let mut f = BufReader::new(File::open("test_files/results_10k_cross_d2.txt").unwrap());
        f.read_to_end(&mut expected_output).unwrap();

        let results = symdel_cross(&primary_input, &comparison_input, 2, false);
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);
    }

    #[test]
    fn test_within_cached() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let test_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_a.txt").unwrap());
        let mut expected_output = Vec::new();
        let _ = f.read_to_end(&mut expected_output);

        let mut test_output_stream = Vec::new();

        let cached = CachedSymdel::new(test_input, 1);
        let results = cached.symdel_within(1, false).unwrap();
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);
    }

    #[test]
    fn test_cross_cached() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let primary_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/cdr3b_10k_b.txt").unwrap());
        let comparison_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_cross.txt").unwrap());
        let mut expected_output = Vec::new();
        let _ = f.read_to_end(&mut expected_output);

        let mut test_output_stream = Vec::new();

        let ccsd = CachedSymdel::new(comparison_input, 1);
        let results = ccsd.symdel_cross(&primary_input, 1, false).unwrap();
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);
    }

    #[test]
    fn test_cross_cached_against_cached() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let primary_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/cdr3b_10k_b.txt").unwrap());
        let comparison_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_cross.txt").unwrap());
        let mut expected_output = Vec::new();
        let _ = f.read_to_end(&mut expected_output);

        let mut test_output_stream = Vec::new();

        let cached_query = CachedSymdel::new(primary_input, 1);
        let cached_reference = CachedSymdel::new(comparison_input, 1);
        let results = cached_reference
            .symdel_cross_against_cached(&cached_query, 1, false)
            .unwrap();
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);
    }
}
