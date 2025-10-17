use itertools::Itertools;
use rapidfuzz::distance::levenshtein;
use rayon::prelude::*;
use std::sync::mpsc;
use std::usize;

pub mod pymod;

#[derive(Debug, Clone, Copy)]
enum CrossComparisonIndex {
    Primary(usize),
    Comparison(usize),
}

pub fn symdel_within_set(
    strings: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let num_vi_pairs = get_num_vi_pairs(strings, max_distance);
    let mut variant_index_pairs = Vec::with_capacity(num_vi_pairs);
    let (tx, rx) = mpsc::channel();
    strings
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

    get_true_hits(&hit_candidates, strings, strings, max_distance, zero_index)
}

pub fn symdel_across_sets(
    strings_primary: &[String],
    strings_comparison: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let num_vi_primary = get_num_vi_pairs(strings_primary, max_distance);
    let num_vi_comparison = get_num_vi_pairs(strings_comparison, max_distance);
    let mut variant_index_pairs = Vec::with_capacity(num_vi_primary + num_vi_comparison);
    let (transmitter, receiver) = mpsc::channel();
    strings_primary.par_iter().enumerate().for_each_with(
        transmitter.clone(),
        |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_distance);
            transmitter
                .send((CrossComparisonIndex::Primary(idx), variants))
                .unwrap();
        },
    );
    strings_comparison.par_iter().enumerate().for_each_with(
        transmitter,
        |transmitter, (idx, s)| {
            let variants = get_deletion_variants(s, max_distance);
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

    get_true_hits(
        &hit_candidates,
        strings_primary,
        strings_comparison,
        max_distance,
        zero_index,
    )
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
    strings_primary: &[String],
    strings_comparison: &[String],
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    let candidates_with_dist: Vec<(usize, usize, usize)> = hit_candidates
        .par_iter()
        .map(|(idx_primary, idx_comparison)| {
            let anchor = &strings_primary[*idx_primary];
            let comparison = &strings_comparison[*idx_comparison];
            let dist = if (anchor.len() > comparison.len()
                && anchor.len() - comparison.len() == max_distance)
                || (anchor.len() < comparison.len()
                    && comparison.len() - anchor.len() == max_distance)
            {
                max_distance
            } else {
                levenshtein::distance(anchor.chars(), comparison.chars())
            };

            (*idx_primary, *idx_comparison, dist)
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
