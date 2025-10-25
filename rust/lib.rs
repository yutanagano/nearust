use itertools::Itertools;
use rapidfuzz::distance::levenshtein;
use rayon::prelude::*;
use std::sync::mpsc;
use std::usize;

pub mod pymod;

#[derive(Debug, Clone, Copy)]
enum CrossComparisonIndex {
    Query(usize),
    Reference(usize),
}

pub fn symdel_within_set(
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

    get_true_hits(&hit_candidates, query, query, max_distance, zero_index)
}

pub fn symdel_across_sets(
    query: &[String],
    reference: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let num_vi_query = get_num_vi_pairs(query, max_distance);
    let num_vi_reference = get_num_vi_pairs(reference, max_distance);
    let mut variant_index_pairs = Vec::with_capacity(num_vi_query + num_vi_reference);
    let (transmitter, receiver) = mpsc::channel();
    query
        .par_iter()
        .enumerate()
        .for_each_with(transmitter.clone(), |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_distance);
            transmitter
                .send((CrossComparisonIndex::Query(idx), variants))
                .unwrap();
        });
    reference
        .par_iter()
        .enumerate()
        .for_each_with(transmitter, |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_distance);
            transmitter
                .send((CrossComparisonIndex::Reference(idx), variants))
                .unwrap();
        });

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

    get_true_hits(&hit_candidates, query, reference, max_distance, zero_index)
}

fn get_num_vi_pairs(strings: &[String], max_distance: usize) -> usize {
    strings
        .iter()
        .map(|s| {
            (0..max_distance)
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
fn get_true_hits(
    hit_candidates: &[(usize, usize)],
    query: &[String],
    reference: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let candidates_with_dist: Vec<(usize, usize, usize)> = hit_candidates
        .par_iter()
        .map(|(idx_query, idx_reference)| {
            let string_query = &query[*idx_query];
            let string_reference = &reference[*idx_reference];
            let dist = if (string_query.len() > string_reference.len()
                && string_query.len() - string_reference.len() == max_distance)
                || (string_query.len() < string_reference.len()
                    && string_reference.len() - string_query.len() == max_distance)
            {
                max_distance
            } else {
                levenshtein::distance(string_query.chars(), string_reference.chars())
            };

            (*idx_query, *idx_reference, dist)
        })
        .collect();

    let mut results = Vec::new();
    for (a_idx, c_idx, dist) in candidates_with_dist {
        if dist > max_distance {
            continue;
        }

        let (a_idx_to_write, c_idx_to_write) = if zero_index {
            (a_idx, c_idx)
        } else {
            (a_idx + 1, c_idx + 1)
        };

        results.push((a_idx_to_write, c_idx_to_write, dist));
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
