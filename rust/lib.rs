use core::hash::{BuildHasher, Hasher};
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use itertools::Itertools;
use rapidfuzz::distance::levenshtein;
use rayon::prelude::*;
use std::io::{BufRead, Error, ErrorKind::InvalidData, Write};
use std::usize;
use std::{hash::Hash, sync::mpsc};
use std::{io, mem, u8};

mod pymod;

#[derive(Debug, Clone, Copy, PartialEq)]
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
    variant_map: HashMap<Box<str>, Vec<usize>>,
    max_distance: u8,
}

impl CachedSymdel {
    pub fn new(reference: Vec<String>, max_distance: u8) -> io::Result<Self> {
        if max_distance == u8::MAX {
            return Err(Error::new(
                InvalidData,
                format!(
                    "max_distance must be less than {} (got {})",
                    u8::MAX,
                    max_distance
                ),
            ));
        }

        let mut variant_map = HashMap::new();
        let hash_builder = variant_map.hasher();
        let (tx, rx) = mpsc::channel();

        reference
            .par_iter()
            .with_min_len(100000)
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

        for (idx, variants_and_hashes) in rx {
            for (variant, precomputed_hash) in variants_and_hashes.into_iter() {
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

        Ok(CachedSymdel {
            reference,
            variant_map,
            max_distance,
        })
    }

    pub fn symdel_within(
        &self,
        max_distance: u8,
        zero_index: bool,
    ) -> io::Result<(Vec<usize>, Vec<usize>, Vec<u8>)> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance, self.max_distance)));
        }

        let mut convergent_indices = Vec::new();
        self.variant_map.iter().for_each(|(_, indices)| {
            if indices.len() == 1 {
                return;
            }
            convergent_indices.push(indices);
        });

        let hit_candidates = get_hit_candidates_from_cis_within(&convergent_indices);

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
        max_distance: u8,
        zero_index: bool,
    ) -> io::Result<(Vec<usize>, Vec<usize>, Vec<u8>)> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance, self.max_distance)));
        }

        let num_del_variants = get_num_deletion_variants(query, max_distance);

        let total_capacity = num_del_variants.iter().sum();
        let mut variant_index_pairs = Vec::with_capacity(total_capacity);
        unsafe { variant_index_pairs.set_len(total_capacity) };

        let mut vip_chunks: Vec<&mut [(Box<str>, usize)]> = Vec::with_capacity(query.len());
        let mut remaining = &mut variant_index_pairs[..];
        for n in num_del_variants {
            let (chunk, rest) = remaining.split_at_mut(n);
            vip_chunks.push(chunk);
            remaining = rest;
        }

        query
            .par_iter()
            .enumerate()
            .zip(vip_chunks.into_par_iter())
            .with_min_len(100000)
            .for_each(|((idx, s), chunk)| {
                write_deletion_variants_rawidx(s, idx, max_distance, chunk);
            });

        variant_index_pairs.par_sort_unstable();
        variant_index_pairs.dedup();

        let mut convergent_indices = Vec::new();
        variant_index_pairs
            .chunk_by(|(v1, _), (v2, _)| v1 == v2)
            .for_each(|group| {
                let variant = &group[0].0;
                match self.variant_map.get(variant) {
                    None => return,
                    Some(indices_ref) => {
                        let indices_query = group.iter().map(|&(_, idx)| idx).collect_vec();
                        convergent_indices.push((indices_query, indices_ref));
                    }
                }
            });

        mem::drop(variant_index_pairs);

        let hit_candidates = get_hit_candidates_from_cis_cross(&convergent_indices);

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
        max_distance: u8,
        zero_index: bool,
    ) -> io::Result<(Vec<usize>, Vec<usize>, Vec<u8>)> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance, self.max_distance)));
        }

        if max_distance > query.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the query ({})", max_distance, query.max_distance)));
        }

        let mut convergent_indices = Vec::new();
        if query.variant_map.len() < self.variant_map.len() {
            query.variant_map.iter().for_each(|(variant, indices_q)| {
                match self.variant_map.get(variant) {
                    None => return,
                    Some(indices_ref) => {
                        convergent_indices.push((indices_q, indices_ref));
                    }
                }
            });
        } else {
            self.variant_map.iter().for_each(|(variant, indices_r)| {
                match query.variant_map.get(variant) {
                    None => return,
                    Some(indices_query) => {
                        convergent_indices.push((indices_query, indices_r));
                    }
                }
            });
        }

        let hit_candidates = get_hit_candidates_from_cis_cross(&convergent_indices);

        Ok(get_true_hits(
            hit_candidates,
            &query.reference,
            &self.reference,
            max_distance,
            zero_index,
        ))
    }
}

pub fn get_candidates_within(
    query: &[String],
    max_distance: u8,
) -> io::Result<Vec<(usize, usize)>> {
    if max_distance == u8::MAX {
        return Err(Error::new(
            InvalidData,
            format!(
                "max_distance must be less than {} (got {})",
                u8::MAX,
                max_distance
            ),
        ));
    }

    let num_del_variants = get_num_deletion_variants(query, max_distance);

    let total_capacity = num_del_variants.iter().sum();
    let mut variant_index_pairs = Vec::with_capacity(total_capacity);
    unsafe { variant_index_pairs.set_len(total_capacity) };

    let mut vip_chunks: Vec<&mut [(Box<str>, usize)]> = Vec::with_capacity(query.len());
    let mut remaining = &mut variant_index_pairs[..];
    for n in num_del_variants {
        let (chunk, rest) = remaining.split_at_mut(n);
        vip_chunks.push(chunk);
        remaining = rest;
    }

    query
        .par_iter()
        .enumerate()
        .zip(vip_chunks.into_par_iter())
        .with_min_len(100000)
        .for_each(|((idx, s), chunk)| {
            write_deletion_variants_rawidx(s, idx, max_distance, chunk);
        });

    variant_index_pairs.par_sort_unstable();
    variant_index_pairs.dedup();

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

    mem::drop(variant_index_pairs);

    let hit_candidates = get_hit_candidates_from_cis_within(&convergent_indices);

    Ok(hit_candidates)
}

pub fn get_candidates_cross(
    query: &[String],
    reference: &[String],
    max_distance: u8,
) -> io::Result<Vec<(usize, usize)>> {
    if max_distance == u8::MAX {
        return Err(Error::new(
            InvalidData,
            format!(
                "max_distance must be less than {} (got {})",
                u8::MAX,
                max_distance
            ),
        ));
    }

    let num_del_variants_q = get_num_deletion_variants(query, max_distance);
    let num_del_variants_r = get_num_deletion_variants(reference, max_distance);

    let total_capacity =
        num_del_variants_q.iter().sum::<usize>() + num_del_variants_r.iter().sum::<usize>();
    let mut variant_index_pairs = Vec::with_capacity(total_capacity);
    unsafe { variant_index_pairs.set_len(total_capacity) };

    let mut vip_chunks_q: Vec<&mut [(Box<str>, CrossComparisonIndex)]> =
        Vec::with_capacity(query.len());
    let mut remaining = &mut variant_index_pairs[..];
    for n in num_del_variants_q {
        let (chunk, rest) = remaining.split_at_mut(n);
        vip_chunks_q.push(chunk);
        remaining = rest;
    }

    let mut vip_chunks_r: Vec<&mut [(Box<str>, CrossComparisonIndex)]> =
        Vec::with_capacity(query.len());
    for n in num_del_variants_r {
        let (chunk, rest) = remaining.split_at_mut(n);
        vip_chunks_r.push(chunk);
        remaining = rest;
    }

    query
        .par_iter()
        .enumerate()
        .zip(vip_chunks_q.into_par_iter())
        .with_min_len(100000)
        .for_each(|((idx, s), chunk)| {
            write_deletion_variants_cci(s, idx, max_distance, false, chunk);
        });
    reference
        .par_iter()
        .enumerate()
        .zip(vip_chunks_r.into_par_iter())
        .with_min_len(100000)
        .for_each(|((idx, s), chunk)| {
            write_deletion_variants_cci(s, idx, max_distance, true, chunk);
        });

    variant_index_pairs.par_sort_unstable_by(|(variant1, _), (variant2, _)| variant1.cmp(variant2));
    variant_index_pairs.dedup();

    let mut convergent_indices = Vec::new();
    variant_index_pairs
        .chunk_by(|(v1, _), (v2, _)| v1 == v2)
        .for_each(|group| {
            if group.len() == 1 {
                return;
            }

            let mut indices_query = Vec::new();
            let mut indices_reference = Vec::new();

            group.iter().for_each(|&(_, idx)| match idx {
                CrossComparisonIndex::Query(v) => indices_query.push(v),
                CrossComparisonIndex::Reference(v) => indices_reference.push(v),
            });

            let num_index_pairs = indices_query.len() * indices_reference.len();
            if num_index_pairs == 0 {
                return;
            }

            convergent_indices.push((indices_query, indices_reference));
        });

    mem::drop(variant_index_pairs);

    let hit_candidates = get_hit_candidates_from_cis_cross(&convergent_indices);

    Ok(hit_candidates)
}

fn get_num_deletion_variants(strings: &[String], max_distance: u8) -> Vec<usize> {
    strings
        .iter()
        .map(|s| {
            let mut num_vars = 0;
            for k in 0..=max_distance {
                if k as usize > s.len() {
                    break;
                }
                num_vars += get_num_k_combs(s.len(), k);
            }
            num_vars
        })
        .collect_vec()
}

fn get_num_k_combs(n: usize, k: u8) -> usize {
    assert!(n > 0);
    assert!(n >= k as usize);

    if k == 0 {
        return 1;
    }

    let num_subsamples: usize = (n - k as usize + 1..=n).product();
    let subsample_perms: usize = (1..=k as usize).product();

    return num_subsamples / subsample_perms;
}

/// Given an input string and its index in the original input vector, generate all possible strings
/// after making at most max_deletions single-character deletions and write them into the slots in
/// the provided chunk, as 2-tuples (variant, input_idx).
fn write_deletion_variants_rawidx(
    input: &str,
    input_idx: usize,
    max_deletions: u8,
    chunk: &mut [(Box<str>, usize)],
) {
    let input_length = input.len();

    chunk[0] = (input.into(), input_idx);

    let mut variant_idx = 1;
    for num_deletions in 1..=max_deletions {
        if num_deletions as usize > input_length {
            break;
        }

        for deletion_indices in (0..input_length).combinations(num_deletions as usize) {
            let mut variant = String::with_capacity(input_length - num_deletions as usize);
            let mut offset = 0;

            for idx in deletion_indices {
                variant.push_str(&input[offset..idx]);
                offset = idx + 1;
            }
            variant.push_str(&input[offset..input_length]);

            chunk[variant_idx] = (variant.into(), input_idx);
            variant_idx += 1;
        }
    }
}

/// Similar to write_deletion_variants_rawidx but with the indices wrapped in CrossComparisonIndex.
fn write_deletion_variants_cci(
    input: &str,
    input_idx: usize,
    max_deletions: u8,
    is_ref: bool,
    chunk: &mut [(Box<str>, CrossComparisonIndex)],
) {
    let input_length = input.len();

    chunk[0] = if is_ref {
        (input.into(), CrossComparisonIndex::Reference(input_idx))
    } else {
        (input.into(), CrossComparisonIndex::Query(input_idx))
    };

    let mut variant_idx = 1;
    for num_deletions in 1..=max_deletions {
        if num_deletions as usize > input_length {
            break;
        }

        for deletion_indices in (0..input_length).combinations(num_deletions as usize) {
            let mut variant = String::with_capacity(input_length - num_deletions as usize);
            let mut offset = 0;

            for idx in deletion_indices {
                variant.push_str(&input[offset..idx]);
                offset = idx + 1;
            }
            variant.push_str(&input[offset..input_length]);

            chunk[variant_idx] = if is_ref {
                (variant.into(), CrossComparisonIndex::Reference(input_idx))
            } else {
                (variant.into(), CrossComparisonIndex::Query(input_idx))
            };
            variant_idx += 1;
        }
    }
}

/// Similar to the write_deletion_variants functions but instead of writing to slots in a slice,
/// returns a vector containing the variants.
fn get_deletion_variants(input: &str, max_deletions: u8) -> Vec<Box<str>> {
    let input_length = input.len();

    let mut deletion_variants = Vec::new();
    deletion_variants.push(input.to_string().into_boxed_str());

    for num_deletions in 1..=max_deletions {
        if num_deletions as usize > input_length {
            deletion_variants.push("".to_string().into_boxed_str());
            break;
        }

        for deletion_indices in (0..input_length).combinations(num_deletions as usize) {
            let mut variant = String::with_capacity(input_length - num_deletions as usize);
            let mut offset = 0;

            for idx in deletion_indices.iter() {
                variant.push_str(&input[offset..*idx]);
                offset = idx + 1;
            }
            variant.push_str(&input[offset..input_length]);

            deletion_variants.push(variant.into_boxed_str());
        }
    }

    deletion_variants.sort_unstable();
    deletion_variants.dedup();

    deletion_variants
}

fn get_hit_candidates_from_cis_within<T>(convergent_indices: &[T]) -> Vec<(usize, usize)>
where
    T: AsRef<[usize]> + Sync,
{
    let num_hit_candidates = convergent_indices
        .iter()
        .map(|indices| get_num_k_combs(indices.as_ref().len(), 2))
        .collect_vec();

    let total_capacity = num_hit_candidates.iter().sum();
    let mut hit_candidates = Vec::with_capacity(total_capacity);
    unsafe { hit_candidates.set_len(total_capacity) };

    let mut hc_chunks: Vec<&mut [(usize, usize)]> = Vec::with_capacity(convergent_indices.len());
    let mut remaining = &mut hit_candidates[..];
    for n in num_hit_candidates {
        let (chunk, rest) = remaining.split_at_mut(n);
        hc_chunks.push(chunk);
        remaining = rest;
    }

    convergent_indices
        .par_iter()
        .zip(hc_chunks.into_par_iter())
        .with_min_len(100000)
        .for_each(|(indices, chunk)| {
            for (i, candidate) in indices
                .as_ref()
                .iter()
                .map(|&v| v)
                .tuple_combinations()
                .enumerate()
            {
                chunk[i] = candidate;
            }
        });

    hit_candidates.par_sort_unstable();
    hit_candidates.dedup();

    hit_candidates
}

fn get_hit_candidates_from_cis_cross<T, U>(convergent_indices: &[(T, U)]) -> Vec<(usize, usize)>
where
    (T, U): Sync,
    T: AsRef<[usize]>,
    U: AsRef<[usize]>,
{
    let num_hit_candidates = convergent_indices
        .iter()
        .map(|(qi, ri)| qi.as_ref().len() * ri.as_ref().len())
        .collect_vec();

    let total_capacity = num_hit_candidates.iter().sum();
    let mut hit_candidates = Vec::with_capacity(total_capacity);
    unsafe { hit_candidates.set_len(total_capacity) };

    let mut hc_chunks: Vec<&mut [(usize, usize)]> = Vec::with_capacity(convergent_indices.len());
    let mut remaining = &mut hit_candidates[..];
    for n in num_hit_candidates {
        let (chunk, rest) = remaining.split_at_mut(n);
        hc_chunks.push(chunk);
        remaining = rest;
    }

    convergent_indices
        .par_iter()
        .zip(hc_chunks.into_par_iter())
        .with_min_len(100000)
        .for_each(|((indices_q, indices_r), chunk)| {
            for (i, candidate) in indices_q
                .as_ref()
                .iter()
                .map(|&v| v)
                .cartesian_product(indices_r.as_ref().iter().map(|&v| v))
                .enumerate()
            {
                chunk[i] = candidate;
            }
        });

    hit_candidates.par_sort_unstable();
    hit_candidates.dedup();

    hit_candidates
}

/// Examine and double check hits to see if they are real
fn get_true_hits(
    hit_candidates: Vec<(usize, usize)>,
    query: &[String],
    reference: &[String],
    max_distance: u8,
    zero_index: bool,
) -> (Vec<usize>, Vec<usize>, Vec<u8>) {
    let candidates_with_dist = compute_dists(hit_candidates, query, reference, max_distance);

    let mut q_indices = Vec::with_capacity(candidates_with_dist.len());
    let mut ref_indices = Vec::with_capacity(candidates_with_dist.len());
    let mut dists = Vec::with_capacity(candidates_with_dist.len());

    for (qi, ri, d) in candidates_with_dist.into_iter() {
        if d > max_distance {
            continue;
        }
        if zero_index {
            q_indices.push(qi);
            ref_indices.push(ri);
            dists.push(d);
        } else {
            q_indices.push(qi + 1);
            ref_indices.push(ri + 1);
            dists.push(d);
        }
    }

    (q_indices, ref_indices, dists)
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
pub fn write_true_results(
    hit_candidates: Vec<(usize, usize)>,
    query: &[String],
    reference: &[String],
    max_distance: u8,
    zero_index: bool,
    writer: &mut impl Write,
) {
    let candidates_with_dists = compute_dists(hit_candidates, query, reference, max_distance);
    for (q_idx, ref_idx, dist) in candidates_with_dists.iter() {
        if *dist > max_distance {
            continue;
        }

        if zero_index {
            write!(writer, "{},{},{}\n", q_idx, ref_idx, dist).unwrap();
        } else {
            write!(writer, "{},{},{}\n", q_idx + 1, ref_idx + 1, dist).unwrap();
        }
    }
}

fn compute_dists(
    hit_candidates: Vec<(usize, usize)>,
    query: &[String],
    reference: &[String],
    max_distance: u8,
) -> Vec<(usize, usize, u8)> {
    hit_candidates
        .into_par_iter()
        .with_min_len(100000)
        .map(|(idx_query, idx_reference)| {
            let string_query = &query[idx_query];
            let string_reference = &reference[idx_reference];
            let dist = if (string_query.len() > string_reference.len()
                && string_query.len() - string_reference.len() == max_distance as usize)
                || (string_query.len() < string_reference.len()
                    && string_reference.len() - string_query.len() == max_distance as usize)
            {
                max_distance
            } else {
                let full_dist =
                    levenshtein::distance(string_query.chars(), string_reference.chars());
                if full_dist > max_distance as usize {
                    u8::MAX
                } else {
                    full_dist as u8
                }
            };

            (idx_query, idx_reference, dist)
        })
        .collect()
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
        let result = get_num_deletion_variants(&strings, 1);
        assert_eq!(result, vec![4, 4, 4]);
    }

    #[test]
    fn test_get_deletion_variants() {
        let variants = get_deletion_variants("foo", 1);
        let expected = vec!["fo".into(), "foo".into(), "oo".into()];
        assert_eq!(variants, expected);

        let variants = get_deletion_variants("foo", 2);
        let expected = vec![
            "f".into(),
            "fo".into(),
            "foo".into(),
            "o".into(),
            "oo".into(),
        ];
        assert_eq!(variants, expected);

        let variants = get_deletion_variants("foo", 10);
        let expected = vec![
            "".into(),
            "f".into(),
            "fo".into(),
            "foo".into(),
            "o".into(),
            "oo".into(),
        ];
        assert_eq!(variants, expected);
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
        let results = get_candidates_within(&test_input, 1).unwrap();
        write_true_results(
            results,
            &test_input,
            &test_input,
            1,
            false,
            &mut test_output_stream,
        );

        assert_eq!(test_output_stream, expected_output);

        expected_output.clear();
        test_output_stream.clear();

        let mut f = BufReader::new(File::open("test_files/results_10k_a_d2.txt").unwrap());
        f.read_to_end(&mut expected_output).unwrap();

        let results = get_candidates_within(&test_input, 2).unwrap();
        write_true_results(
            results,
            &test_input,
            &test_input,
            2,
            false,
            &mut test_output_stream,
        );

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
        let results = get_candidates_cross(&primary_input, &comparison_input, 1).unwrap();
        write_true_results(
            results,
            &primary_input,
            &comparison_input,
            1,
            false,
            &mut test_output_stream,
        );

        assert_eq!(test_output_stream, expected_output);

        expected_output.clear();
        test_output_stream.clear();

        let mut f = BufReader::new(File::open("test_files/results_10k_cross_d2.txt").unwrap());
        f.read_to_end(&mut expected_output).unwrap();

        let results = get_candidates_cross(&primary_input, &comparison_input, 2).unwrap();
        write_true_results(
            results,
            &primary_input,
            &comparison_input,
            2,
            false,
            &mut test_output_stream,
        );

        assert_eq!(test_output_stream, expected_output);
    }

    fn written_to_coo(in_stream: impl BufRead) -> (Vec<usize>, Vec<usize>, Vec<u8>) {
        let mut q_indices = Vec::new();
        let mut ref_indices = Vec::new();
        let mut dists = Vec::new();

        for line_res in in_stream.lines() {
            let line = line_res.unwrap();
            let mut parts = line.split(",");

            let qi = parts.next().unwrap().parse::<usize>().unwrap();
            let ri = parts.next().unwrap().parse::<usize>().unwrap();
            let d = parts.next().unwrap().parse::<usize>().unwrap();

            q_indices.push(qi);
            ref_indices.push(ri);
            dists.push(d as u8);
        }

        (q_indices, ref_indices, dists)
    }

    #[test]
    fn test_within_cached() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let test_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/results_10k_a.txt").unwrap());
        let expected_output = written_to_coo(f);

        let cached = CachedSymdel::new(test_input, 1).unwrap();
        let results = cached.symdel_within(1, false).unwrap();

        assert_eq!(results, expected_output);
    }

    #[test]
    fn test_cross_cached() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let primary_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/cdr3b_10k_b.txt").unwrap());
        let comparison_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/results_10k_cross.txt").unwrap());
        let expected_output = written_to_coo(f);

        let cached = CachedSymdel::new(comparison_input, 1).unwrap();
        let results = cached.symdel_cross(&primary_input, 1, false).unwrap();

        assert_eq!(results, expected_output);
    }

    #[test]
    fn test_cross_cached_against_cached() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let primary_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/cdr3b_10k_b.txt").unwrap());
        let comparison_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/results_10k_cross.txt").unwrap());
        let expected_output = written_to_coo(f);

        let cached_query = CachedSymdel::new(primary_input, 1).unwrap();
        let cached_reference = CachedSymdel::new(comparison_input, 1).unwrap();
        let results = cached_reference
            .symdel_cross_against_cached(&cached_query, 1, false)
            .unwrap();

        assert_eq!(results, expected_output);
    }
}
