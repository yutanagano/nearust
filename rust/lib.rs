use foldhash::fast::FixedState;
use hashbrown::HashMap;
use itertools::Itertools;
use rapidfuzz::distance::levenshtein;
use rayon::prelude::*;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hasher};
use std::io::{self, BufRead, Error, ErrorKind::InvalidData};
use std::mem::MaybeUninit;
use std::ops::{BitAnd, BitOr, Range};
use std::{ptr, u8, usize};

mod pymod;

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct MaxDistance(u8);

impl MaxDistance {
    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for MaxDistance {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value == u8::MAX {
            Err(Error::new(
                InvalidData,
                format!("max_distance must be less than {} (got {})", u8::MAX, value),
            ))
        } else {
            Ok(Self(value))
        }
    }
}

trait CrossComparable: Copy + BitAnd<Output = Self> + BitOr<Output = Self> + PartialEq + Debug {
    const TYPE_MASK: Self;
    const VALUE_MASK: Self;
}

impl CrossComparable for usize {
    const TYPE_MASK: Self = 1 << (usize::BITS - 1);
    const VALUE_MASK: Self = !Self::TYPE_MASK;
}

impl CrossComparable for u32 {
    const TYPE_MASK: Self = 1 << 31;
    const VALUE_MASK: Self = !Self::TYPE_MASK;
}

impl CrossComparable for u64 {
    const TYPE_MASK: Self = 1 << 63;
    const VALUE_MASK: Self = !Self::TYPE_MASK;
}

#[derive(Clone, Copy, PartialEq)]
struct CrossIndex<T: CrossComparable>(T);

impl<T: CrossComparable> CrossIndex<T> {
    fn from(value: T, is_ref: bool) -> Self {
        debug_assert_ne!(value & T::TYPE_MASK, T::TYPE_MASK);

        if is_ref {
            Self(value | T::TYPE_MASK)
        } else {
            Self(value)
        }
    }

    fn is_ref(&self) -> bool {
        self.0 & T::TYPE_MASK == T::TYPE_MASK
    }

    fn get_value(&self) -> T {
        self.0 & T::VALUE_MASK
    }
}

#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn write(&mut self, bytes: &[u8]) {
        unreachable!("hasher only designed for u64, got {bytes:?}");
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[derive(Default)]
struct IdentityHasherBuilder;

impl BuildHasher for IdentityHasherBuilder {
    type Hasher = IdentityHasher;

    fn build_hasher(&self) -> Self::Hasher {
        IdentityHasher::default()
    }
}

struct Span {
    start: usize,
    len: usize,
}

impl Span {
    fn new(start: usize, len: usize) -> Self {
        Span { start, len }
    }

    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn as_range(&self) -> Range<usize> {
        self.start..self.start + self.len
    }
}

/// Class for assymetric cross-set symdel where the reference is known beforehand, and a variant
/// hashmap (mapping deletion variants to all the original strings that could have produced that
/// variant) can be computed beforehand to expedite multiple future queries against that same
/// reference.
pub struct CachedSymdel {
    str_store: Vec<u8>,
    str_spans: Vec<Span>,
    index_store: Vec<usize>,
    variant_map: HashMap<u64, Span, IdentityHasherBuilder>,
    max_distance: MaxDistance,
}

impl CachedSymdel {
    pub fn new(reference: &[String], max_distance: MaxDistance) -> Self {
        let (str_store, str_spans) = {
            let strlens = reference.iter().map(|s| s.len()).collect_vec();
            let total_capacity = strlens.iter().sum();

            let mut str_store_uninit: Vec<MaybeUninit<u8>> = Vec::with_capacity(total_capacity);
            unsafe { str_store_uninit.set_len(total_capacity) };

            let mut str_spans = Vec::with_capacity(reference.len());
            let mut cursor = 0;
            for &n in strlens.iter() {
                str_spans.push(Span::new(cursor, n));
                cursor += n;
            }

            debug_assert_eq!(cursor, str_store_uninit.len());
            debug_assert_eq!(str_spans.len(), reference.len());

            let mut str_store_chunks = Vec::with_capacity(reference.len());
            let mut remaining = &mut str_store_uninit[..];
            for n in strlens {
                let (chunk, rest) = remaining.split_at_mut(n);
                str_store_chunks.push(chunk);
                remaining = rest;
            }

            debug_assert_eq!(remaining.len(), 0);
            debug_assert_eq!(str_store_chunks.len(), reference.len());

            reference
                .par_iter()
                .zip(str_store_chunks.into_par_iter())
                .with_min_len(100000)
                .for_each(|(s, chunk)| {
                    debug_assert_eq!(s.len(), chunk.len());
                    unsafe {
                        ptr::copy_nonoverlapping(s.as_ptr(), chunk.as_mut_ptr() as *mut u8, s.len())
                    };
                });

            let str_store = unsafe { cast_to_initialised_vec(str_store_uninit) };

            (str_store, str_spans)
        };

        let hash_builder = FixedState::with_seed(42);

        let (index_store, convergence_groups) = {
            let num_del_variants = get_num_deletion_variants(reference, max_distance);

            let total_capacity = num_del_variants.iter().sum();
            let mut variant_index_pairs_uninit: Vec<MaybeUninit<(u64, usize)>> =
                Vec::with_capacity(total_capacity);
            unsafe { variant_index_pairs_uninit.set_len(total_capacity) };

            let mut vip_chunks: Vec<&mut [MaybeUninit<(u64, usize)>]> =
                Vec::with_capacity(reference.len());
            let mut remaining = &mut variant_index_pairs_uninit[..];
            for n in num_del_variants {
                let (chunk, rest) = remaining.split_at_mut(n);
                vip_chunks.push(chunk);
                remaining = rest;
            }

            debug_assert_eq!(remaining.len(), 0);
            debug_assert_eq!(vip_chunks.len(), reference.len());

            reference
                .par_iter()
                .zip(vip_chunks.into_par_iter())
                .enumerate()
                .with_min_len(100000)
                .for_each(|(idx, (s, chunk))| {
                    write_vi_pairs_rawidx(s, idx, max_distance, chunk, &hash_builder);
                });

            let mut variant_index_pairs =
                unsafe { cast_to_initialised_vec(variant_index_pairs_uninit) };

            variant_index_pairs.par_sort_unstable();
            variant_index_pairs.dedup();

            let mut total_num_convergent_indices = 0;
            let mut num_convergence_groups = 0;

            variant_index_pairs
                .chunk_by(|(v1, _), (v2, _)| v1 == v2)
                .for_each(|chunk| {
                    total_num_convergent_indices += chunk.len();
                    num_convergence_groups += 1;
                });

            let mut convergent_indices = Vec::with_capacity(total_num_convergent_indices);
            let mut convergence_groups = Vec::with_capacity(num_convergence_groups);
            let mut cursor = 0;

            variant_index_pairs
                .chunk_by(|(v1, _), (v2, _)| v1 == v2)
                .for_each(|chunk| {
                    convergent_indices.extend(chunk.iter().map(|&(_, i)| i));
                    convergence_groups.push((chunk[0].0, Span::new(cursor, chunk.len())));
                    cursor += chunk.len();
                });

            debug_assert_eq!(cursor, convergent_indices.len());

            (convergent_indices, convergence_groups)
        };

        let mut variant_map = HashMap::with_hasher(IdentityHasherBuilder::default());

        for (v_hash, index_range) in convergence_groups {
            variant_map.entry(v_hash).insert(index_range);
        }

        CachedSymdel {
            str_store,
            str_spans,
            index_store,
            variant_map,
            max_distance,
        }
    }

    pub fn symdel_within(
        &self,
        max_distance: MaxDistance,
        zero_index: bool,
    ) -> io::Result<(Vec<usize>, Vec<usize>, Vec<u8>)> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance.as_u8(), self.max_distance.as_u8())));
        }

        let mut convergent_indices = Vec::with_capacity(self.variant_map.len());
        self.variant_map.iter().for_each(|(_, span)| {
            if span.len() == 1 {
                return;
            }
            convergent_indices.push(self.get_convergent_indices_from_span(span));
        });

        let hit_candidates = get_hit_candidates_from_cis_within(&convergent_indices);

        Ok(self.get_true_hits_fully_cached(hit_candidates, self, max_distance, zero_index))
    }

    pub fn symdel_cross(
        &self,
        query: &[String],
        max_distance: MaxDistance,
        zero_index: bool,
    ) -> io::Result<(Vec<usize>, Vec<usize>, Vec<u8>)> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance.as_u8(), self.max_distance.as_u8())));
        }

        let convergent_indices = {
            let num_del_variants = get_num_deletion_variants(query, max_distance);

            let total_capacity = num_del_variants.iter().sum();
            let mut variant_index_pairs_uninit: Vec<MaybeUninit<(u64, usize)>> =
                Vec::with_capacity(total_capacity);
            unsafe { variant_index_pairs_uninit.set_len(total_capacity) };

            let mut vip_chunks: Vec<&mut [MaybeUninit<(u64, usize)>]> =
                Vec::with_capacity(query.len());
            let mut remaining = &mut variant_index_pairs_uninit[..];
            for n in num_del_variants {
                let (chunk, rest) = remaining.split_at_mut(n);
                vip_chunks.push(chunk);
                remaining = rest;
            }

            let hash_builder = FixedState::with_seed(42);

            query
                .par_iter()
                .enumerate()
                .zip(vip_chunks.into_par_iter())
                .with_min_len(100000)
                .for_each(|((idx, s), chunk)| {
                    write_vi_pairs_rawidx(s, idx, max_distance, chunk, &hash_builder);
                });

            let mut variant_index_pairs =
                unsafe { cast_to_initialised_vec(variant_index_pairs_uninit) };

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
                            convergent_indices.push((
                                indices_query,
                                self.get_convergent_indices_from_span(indices_ref),
                            ));
                        }
                    }
                });

            convergent_indices
        };

        let hit_candidates = get_hit_candidates_from_cis_cross(&convergent_indices);

        Ok(self.get_true_hits_partially_cached(hit_candidates, query, max_distance, zero_index))
    }

    pub fn symdel_cross_against_cached(
        &self,
        query: &Self,
        max_distance: MaxDistance,
        zero_index: bool,
    ) -> io::Result<(Vec<usize>, Vec<usize>, Vec<u8>)> {
        if max_distance > self.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the caller ({})", max_distance.as_u8(), self.max_distance.as_u8())));
        }

        if max_distance > query.max_distance {
            return Err(Error::new(InvalidData, format!("the max_distance supplied to this method ({}) must not be greater than the max_distance specified when constructing the query ({})", max_distance.as_u8(), query.max_distance.as_u8())));
        }

        let mut convergent_indices = Vec::new();
        if query.variant_map.len() < self.variant_map.len() {
            query.variant_map.iter().for_each(|(variant, indices_q)| {
                match self.variant_map.get(variant) {
                    None => return,
                    Some(indices_ref) => {
                        convergent_indices.push((
                            query.get_convergent_indices_from_span(indices_q),
                            self.get_convergent_indices_from_span(indices_ref),
                        ));
                    }
                }
            });
        } else {
            self.variant_map.iter().for_each(|(variant, indices_r)| {
                match query.variant_map.get(variant) {
                    None => return,
                    Some(indices_query) => {
                        convergent_indices.push((
                            query.get_convergent_indices_from_span(indices_query),
                            self.get_convergent_indices_from_span(indices_r),
                        ));
                    }
                }
            });
        }

        let hit_candidates = get_hit_candidates_from_cis_cross(&convergent_indices);

        Ok(self.get_true_hits_fully_cached(hit_candidates, &query, max_distance, zero_index))
    }

    #[inline(always)]
    fn get_convergent_indices_from_span(&self, span: &Span) -> &[usize] {
        &self.index_store[span.as_range()]
    }

    #[inline(always)]
    fn get_str_at_index(&self, i: usize) -> &str {
        unsafe { str::from_utf8_unchecked(&self.str_store[self.str_spans[i].as_range()]) }
    }

    fn get_true_hits_partially_cached(
        &self,
        hit_candidates: Vec<(usize, usize)>,
        query: &[String],
        max_distance: MaxDistance,
        zero_index: bool,
    ) -> (Vec<usize>, Vec<usize>, Vec<u8>) {
        let candidates_with_dist: Vec<(usize, usize, u8)> = hit_candidates
            .into_par_iter()
            .with_min_len(100000)
            .map(|(idx_query, idx_reference)| {
                let string_query = &query[idx_query];
                let string_reference = self.get_str_at_index(idx_reference);
                let dist = {
                    let full_dist =
                        levenshtein::distance(string_query.chars(), string_reference.chars());
                    if full_dist > max_distance.as_u8() as usize {
                        u8::MAX
                    } else {
                        full_dist as u8
                    }
                };

                (idx_query, idx_reference, dist)
            })
            .collect();

        let mut q_indices = Vec::with_capacity(candidates_with_dist.len());
        let mut ref_indices = Vec::with_capacity(candidates_with_dist.len());
        let mut dists = Vec::with_capacity(candidates_with_dist.len());

        for (qi, ri, d) in candidates_with_dist.into_iter() {
            if d > max_distance.as_u8() {
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

    fn get_true_hits_fully_cached(
        &self,
        hit_candidates: Vec<(usize, usize)>,
        query: &Self,
        max_distance: MaxDistance,
        zero_index: bool,
    ) -> (Vec<usize>, Vec<usize>, Vec<u8>) {
        let candidates_with_dist: Vec<(usize, usize, u8)> = hit_candidates
            .into_par_iter()
            .with_min_len(100000)
            .map(|(idx_query, idx_reference)| {
                let string_query = query.get_str_at_index(idx_query);
                let string_reference = self.get_str_at_index(idx_reference);
                let dist = {
                    let full_dist =
                        levenshtein::distance(string_query.chars(), string_reference.chars());
                    if full_dist > max_distance.as_u8() as usize {
                        u8::MAX
                    } else {
                        full_dist as u8
                    }
                };

                (idx_query, idx_reference, dist)
            })
            .collect();

        let mut q_indices = Vec::with_capacity(candidates_with_dist.len());
        let mut ref_indices = Vec::with_capacity(candidates_with_dist.len());
        let mut dists = Vec::with_capacity(candidates_with_dist.len());

        for (qi, ri, d) in candidates_with_dist.into_iter() {
            if d > max_distance.as_u8() {
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
}

pub fn get_candidates_within(query: &[String], max_distance: MaxDistance) -> Vec<(usize, usize)> {
    let (convergent_indices, group_sizes) = {
        let num_del_variants = get_num_deletion_variants(query, max_distance);

        let total_capacity = num_del_variants.iter().sum();
        let mut variant_index_pairs_uninit: Vec<MaybeUninit<(u64, usize)>> =
            Vec::with_capacity(total_capacity);
        unsafe { variant_index_pairs_uninit.set_len(total_capacity) };

        let mut vip_chunks: Vec<&mut [MaybeUninit<(u64, usize)>]> = Vec::with_capacity(query.len());
        let mut remaining = &mut variant_index_pairs_uninit[..];
        for n in num_del_variants {
            let (chunk, rest) = remaining.split_at_mut(n);
            vip_chunks.push(chunk);
            remaining = rest;
        }

        debug_assert_eq!(remaining.len(), 0);
        debug_assert_eq!(vip_chunks.len(), query.len());

        let hash_builder = FixedState::with_seed(42);

        query
            .par_iter()
            .zip(vip_chunks.into_par_iter())
            .enumerate()
            .with_min_len(100000)
            .for_each(|(idx, (s, chunk))| {
                write_vi_pairs_rawidx(s, idx, max_distance, chunk, &hash_builder);
            });

        let mut variant_index_pairs =
            unsafe { cast_to_initialised_vec(variant_index_pairs_uninit) };

        variant_index_pairs.par_sort_unstable();
        variant_index_pairs.dedup();

        let mut total_num_convergent_indices = 0;
        let mut num_convergence_groups = 0;

        variant_index_pairs
            .chunk_by(|(v1, _), (v2, _)| v1 == v2)
            .filter(|chunk| chunk.len() > 1)
            .for_each(|chunk| {
                total_num_convergent_indices += chunk.len();
                num_convergence_groups += 1;
            });

        let mut convergent_indices = Vec::with_capacity(total_num_convergent_indices);
        let mut convergence_group_sizes = Vec::with_capacity(num_convergence_groups);

        variant_index_pairs
            .chunk_by(|(v1, _), (v2, _)| v1 == v2)
            .filter(|chunk| chunk.len() > 1)
            .for_each(|chunk| {
                convergent_indices.extend(chunk.iter().map(|&(_, i)| i));
                convergence_group_sizes.push(chunk.len());
            });

        (convergent_indices, convergence_group_sizes)
    };

    let mut convergent_chunks = Vec::with_capacity(group_sizes.len());
    let mut remaining = &convergent_indices[..];
    for n in group_sizes {
        let (chunk, rest) = remaining.split_at(n);
        convergent_chunks.push(chunk);
        remaining = rest;
    }

    debug_assert_eq!(remaining.len(), 0);

    get_hit_candidates_from_cis_within(&convergent_chunks)
}

pub fn get_candidates_cross(
    query: &[String],
    reference: &[String],
    max_distance: MaxDistance,
) -> Vec<(usize, usize)> {
    let (convergent_indices, group_sizes) = {
        let num_del_variants_q = get_num_deletion_variants(query, max_distance);
        let num_del_variants_r = get_num_deletion_variants(reference, max_distance);

        let total_capacity =
            num_del_variants_q.iter().sum::<usize>() + num_del_variants_r.iter().sum::<usize>();
        let mut variant_index_pairs_uninit: Vec<MaybeUninit<(u64, CrossIndex<usize>)>> =
            Vec::with_capacity(total_capacity);
        unsafe { variant_index_pairs_uninit.set_len(total_capacity) };

        let mut vip_chunks_q: Vec<&mut [MaybeUninit<(u64, CrossIndex<usize>)>]> =
            Vec::with_capacity(query.len());
        let mut remaining = &mut variant_index_pairs_uninit[..];
        for n in num_del_variants_q {
            let (chunk, rest) = remaining.split_at_mut(n);
            vip_chunks_q.push(chunk);
            remaining = rest;
        }

        let mut vip_chunks_r: Vec<&mut [MaybeUninit<(u64, CrossIndex<usize>)>]> =
            Vec::with_capacity(reference.len());
        for n in num_del_variants_r {
            let (chunk, rest) = remaining.split_at_mut(n);
            vip_chunks_r.push(chunk);
            remaining = rest;
        }

        debug_assert_eq!(remaining.len(), 0);
        debug_assert_eq!(vip_chunks_q.len(), query.len());
        debug_assert_eq!(vip_chunks_r.len(), reference.len());

        let hash_builder = FixedState::with_seed(42);

        query
            .par_iter()
            .zip(vip_chunks_q.into_par_iter())
            .enumerate()
            .with_min_len(100000)
            .for_each(|(idx, (s, chunk))| {
                write_vi_pairs_ci(s, idx, max_distance, false, chunk, &hash_builder);
            });
        reference
            .par_iter()
            .zip(vip_chunks_r.into_par_iter())
            .enumerate()
            .with_min_len(100000)
            .for_each(|(idx, (s, chunk))| {
                write_vi_pairs_ci(s, idx, max_distance, true, chunk, &hash_builder);
            });

        let mut variant_index_pairs =
            unsafe { cast_to_initialised_vec(variant_index_pairs_uninit) };

        variant_index_pairs
            .par_sort_unstable_by(|(variant1, _), (variant2, _)| variant1.cmp(variant2));
        variant_index_pairs.dedup();

        let mut total_num_convergent_indices = 0;
        let mut num_convergence_groups = 0;

        variant_index_pairs
            .chunk_by(|(v1, _), (v2, _)| v1 == v2)
            .filter(|chunk| chunk.len() > 1)
            .for_each(|chunk| {
                total_num_convergent_indices += chunk.len();
                num_convergence_groups += 1;
            });

        let mut convergent_indices = Vec::with_capacity(total_num_convergent_indices);
        let mut convergence_group_sizes = Vec::with_capacity(num_convergence_groups);

        variant_index_pairs
            .chunk_by(|(v1, _), (v2, _)| v1 == v2)
            .filter(|chunk| chunk.len() > 1)
            .map(|chunk| {
                let len_q = chunk.iter().filter(|(_, ci)| !ci.is_ref()).count();

                let len_r = chunk.iter().filter(|(_, ci)| ci.is_ref()).count();

                (chunk, len_q, len_r)
            })
            .filter(|(_, len_q, len_r)| len_q * len_r > 0)
            .for_each(|(chunk, len_q, len_r)| {
                convergent_indices.extend(
                    chunk
                        .iter()
                        .filter(|(_, ci)| !ci.is_ref())
                        .map(|&(_, ci)| ci.get_value()),
                );
                convergent_indices.extend(
                    chunk
                        .iter()
                        .filter(|(_, ci)| ci.is_ref())
                        .map(|&(_, ci)| ci.get_value()),
                );

                convergence_group_sizes.push((len_q, len_r));
            });

        (convergent_indices, convergence_group_sizes)
    };

    let mut convergent_chunks = Vec::with_capacity(group_sizes.len());
    let mut remaining = &convergent_indices[..];
    for (n_q, n_r) in group_sizes {
        let (chunk_q, rest) = remaining.split_at(n_q);
        let (chunk_r, rest) = rest.split_at(n_r);
        convergent_chunks.push((chunk_q, chunk_r));
        remaining = rest;
    }

    debug_assert_eq!(remaining.len(), 0);

    get_hit_candidates_from_cis_cross(&convergent_chunks)
}

fn get_num_deletion_variants(strings: &[String], max_distance: MaxDistance) -> Vec<usize> {
    strings
        .iter()
        .map(|s| {
            let mut num_vars = 0;
            for k in 0..=max_distance.as_u8() {
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
/// after making at most max_deletions single-character deletions, compute their hash, and write
/// them into the slots in the provided chunk, as 2-tuples (hash, input_idx).
fn write_vi_pairs_rawidx(
    input: &str,
    input_idx: usize,
    max_deletions: MaxDistance,
    chunk: &mut [MaybeUninit<(u64, usize)>],
    hash_builder: &impl BuildHasher,
) {
    let input_length = input.len();

    chunk[0].write((hash_string(input, hash_builder), input_idx));

    let mut variant_idx = 1;
    for num_deletions in 1..=max_deletions.as_u8() {
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

            chunk[variant_idx].write((hash_string(variant, hash_builder), input_idx));
            variant_idx += 1;
        }
    }
}

/// Similar to write_deletion_variants_rawidx but with the indices wrapped in CrossIndex.
fn write_vi_pairs_ci(
    input: &str,
    input_idx: usize,
    max_deletions: MaxDistance,
    is_ref: bool,
    chunk: &mut [MaybeUninit<(u64, CrossIndex<usize>)>],
    hash_builder: &impl BuildHasher,
) {
    let input_length = input.len();

    chunk[0].write(if is_ref {
        (
            hash_string(input, hash_builder),
            CrossIndex::from(input_idx, true),
        )
    } else {
        (
            hash_string(input, hash_builder),
            CrossIndex::from(input_idx, false),
        )
    });

    let mut variant_idx = 1;
    for num_deletions in 1..=max_deletions.as_u8() {
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

            chunk[variant_idx].write(if is_ref {
                (
                    hash_string(variant, hash_builder),
                    CrossIndex::from(input_idx, true),
                )
            } else {
                (
                    hash_string(variant, hash_builder),
                    CrossIndex::from(input_idx, false),
                )
            });
            variant_idx += 1;
        }
    }
}

/// Similar to the write_deletion_variants functions but instead of writing to slots in a slice,
/// returns a vector containing the variants.
fn get_del_var_hashes(
    input: &str,
    max_deletions: MaxDistance,
    hash_builder: &impl BuildHasher,
) -> Vec<u64> {
    let input_length = input.len();

    let mut deletion_variants = Vec::new();
    deletion_variants.push(hash_string(input, hash_builder));

    for num_deletions in 1..=max_deletions.as_u8() {
        if num_deletions as usize > input_length {
            deletion_variants.push(hash_string("", hash_builder));
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

            deletion_variants.push(hash_string(variant, hash_builder));
        }
    }

    deletion_variants.sort_unstable();
    deletion_variants.dedup();

    deletion_variants
}

fn hash_string(s: impl AsRef<[u8]>, hash_builder: &impl BuildHasher) -> u64 {
    let mut hasher = hash_builder.build_hasher();
    hasher.write(s.as_ref());
    hasher.finish()
}

unsafe fn cast_to_initialised_vec<T>(mut input: Vec<MaybeUninit<T>>) -> Vec<T> {
    let ptr = input.as_mut_ptr() as *mut T;
    let len = input.len();
    let cap = input.capacity();
    std::mem::forget(input);
    Vec::from_raw_parts(ptr, len, cap)
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
    let mut hit_candidates_uninit: Vec<MaybeUninit<(usize, usize)>> =
        Vec::with_capacity(total_capacity);
    unsafe { hit_candidates_uninit.set_len(total_capacity) };

    let mut hc_chunks: Vec<&mut [MaybeUninit<(usize, usize)>]> =
        Vec::with_capacity(convergent_indices.len());
    let mut remaining = &mut hit_candidates_uninit[..];
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
                chunk[i].write(candidate);
            }
        });

    let mut hit_candidates = unsafe { cast_to_initialised_vec(hit_candidates_uninit) };

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
    let mut hit_candidates_uninit: Vec<MaybeUninit<(usize, usize)>> =
        Vec::with_capacity(total_capacity);
    unsafe { hit_candidates_uninit.set_len(total_capacity) };

    let mut hc_chunks: Vec<&mut [MaybeUninit<(usize, usize)>]> =
        Vec::with_capacity(convergent_indices.len());
    let mut remaining = &mut hit_candidates_uninit[..];
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
                chunk[i].write(candidate);
            }
        });

    let mut hit_candidates = unsafe { cast_to_initialised_vec(hit_candidates_uninit) };

    hit_candidates.par_sort_unstable();
    hit_candidates.dedup();

    hit_candidates
}

/// Examine and double check hits to see if they are real
pub fn get_true_hits(
    hit_candidates: Vec<(usize, usize)>,
    query: &[String],
    reference: &[String],
    max_distance: MaxDistance,
    zero_index: bool,
) -> (Vec<usize>, Vec<usize>, Vec<u8>) {
    let candidates_with_dist = compute_dists(hit_candidates, query, reference, max_distance);

    let mut q_indices = Vec::with_capacity(candidates_with_dist.len());
    let mut ref_indices = Vec::with_capacity(candidates_with_dist.len());
    let mut dists = Vec::with_capacity(candidates_with_dist.len());

    for (qi, ri, d) in candidates_with_dist.into_iter() {
        if d > max_distance.as_u8() {
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

pub fn compute_dists(
    hit_candidates: Vec<(usize, usize)>,
    query: &[String],
    reference: &[String],
    max_distance: MaxDistance,
) -> Vec<(usize, usize, u8)> {
    hit_candidates
        .into_par_iter()
        .with_min_len(100000)
        .map(|(idx_query, idx_reference)| {
            let string_query = &query[idx_query];
            let string_reference = &reference[idx_reference];
            let dist = {
                let full_dist =
                    levenshtein::distance(string_query.chars(), string_reference.chars());
                if full_dist > max_distance.as_u8() as usize {
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
    use std::io::Cursor;

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
        let result = get_num_deletion_variants(&strings, MaxDistance(1));
        assert_eq!(result, vec![4, 4, 4]);
    }

    #[test]
    fn test_get_deletion_variants() {
        let hash_builder = FixedState::with_seed(42);

        let variants = get_del_var_hashes("foo", MaxDistance(1), &hash_builder);
        let mut expected = vec![
            hash_string("fo", &hash_builder),
            hash_string("foo", &hash_builder),
            hash_string("oo", &hash_builder),
        ];
        expected.sort_unstable();
        assert_eq!(variants, expected);

        let variants = get_del_var_hashes("foo", MaxDistance(2), &hash_builder);
        let mut expected = vec![
            hash_string("f", &hash_builder),
            hash_string("fo", &hash_builder),
            hash_string("foo", &hash_builder),
            hash_string("o", &hash_builder),
            hash_string("oo", &hash_builder),
        ];
        expected.sort_unstable();
        assert_eq!(variants, expected);

        let variants = get_del_var_hashes("foo", MaxDistance(10), &hash_builder);
        let mut expected = vec![
            hash_string("", &hash_builder),
            hash_string("f", &hash_builder),
            hash_string("fo", &hash_builder),
            hash_string("foo", &hash_builder),
            hash_string("o", &hash_builder),
            hash_string("oo", &hash_builder),
        ];
        expected.sort_unstable();
        assert_eq!(variants, expected);
    }

    #[test]
    fn test_get_input_lines_as_ascii() {
        let strings = get_input_lines_as_ascii(&mut "foo\nbar\nbaz\n".as_bytes()).unwrap();
        let expected: Vec<String> = vec!["foo".into(), "bar".into(), "baz".into()];
        assert_eq!(strings, expected);
    }

    static QUERY_BYTES: &[u8] = include_bytes!("../test_files/cdr3b_10k_a.txt");
    static REFERENCE_BYTES: &[u8] = include_bytes!("../test_files/cdr3b_10k_b.txt");
    static EXPECTED_BYTES_WITHIN_1: &[u8] = include_bytes!("../test_files/results_10k_a.txt");
    static EXPECTED_BYTES_WITHIN_2: &[u8] = include_bytes!("../test_files/results_10k_a_d2.txt");
    static EXPECTED_BYTES_CROSS_1: &[u8] = include_bytes!("../test_files/results_10k_cross.txt");
    static EXPECTED_BYTES_CROSS_2: &[u8] = include_bytes!("../test_files/results_10k_cross_d2.txt");

    fn bytes_as_ascii_lines(bytes: &[u8]) -> Vec<String> {
        get_input_lines_as_ascii(Cursor::new(bytes)).expect("test files should be valid ASCII")
    }

    fn bytes_as_coo(bytes: &[u8]) -> (Vec<usize>, Vec<usize>, Vec<u8>) {
        let mut q_indices = Vec::new();
        let mut ref_indices = Vec::new();
        let mut dists = Vec::new();

        for line_res in Cursor::new(bytes).lines() {
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
    fn test_within() {
        let query = bytes_as_ascii_lines(QUERY_BYTES);

        let candidates = get_candidates_within(&query, MaxDistance(1));
        let results = get_true_hits(candidates, &query, &query, MaxDistance(1), false);
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_WITHIN_1));

        let candidates = get_candidates_within(&query, MaxDistance(2));
        let results = get_true_hits(candidates, &query, &query, MaxDistance(2), false);
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_WITHIN_2))
    }

    #[test]
    fn test_cross() {
        let query = bytes_as_ascii_lines(QUERY_BYTES);
        let reference = bytes_as_ascii_lines(REFERENCE_BYTES);

        let candidates = get_candidates_cross(&query, &reference, MaxDistance(1));
        let results = get_true_hits(candidates, &query, &reference, MaxDistance(1), false);
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_CROSS_1));

        let candidates = get_candidates_cross(&query, &reference, MaxDistance(2));
        let results = get_true_hits(candidates, &query, &reference, MaxDistance(2), false);
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_CROSS_2))
    }

    #[test]
    fn test_within_cached() {
        let query = bytes_as_ascii_lines(QUERY_BYTES);

        let cached = CachedSymdel::new(&query, MaxDistance(2));
        let results = cached.symdel_within(MaxDistance(1), false).unwrap();
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_WITHIN_1));

        let results = cached.symdel_within(MaxDistance(2), false).unwrap();
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_WITHIN_2));
    }

    #[test]
    fn test_cross_cached() {
        let query = bytes_as_ascii_lines(QUERY_BYTES);
        let reference = bytes_as_ascii_lines(REFERENCE_BYTES);

        let cached = CachedSymdel::new(&reference, MaxDistance(2));
        let results = cached.symdel_cross(&query, MaxDistance(1), false).unwrap();
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_CROSS_1));

        let results = cached.symdel_cross(&query, MaxDistance(2), false).unwrap();
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_CROSS_2));
    }

    #[test]
    fn test_cross_cached_against_cached() {
        let query = bytes_as_ascii_lines(QUERY_BYTES);
        let reference = bytes_as_ascii_lines(REFERENCE_BYTES);

        let cached_query = CachedSymdel::new(&query, MaxDistance(2));
        let cached_reference = CachedSymdel::new(&reference, MaxDistance(2));
        let results = cached_reference
            .symdel_cross_against_cached(&cached_query, MaxDistance(1), false)
            .unwrap();
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_CROSS_1));

        let results = cached_reference
            .symdel_cross_against_cached(&cached_query, MaxDistance(2), false)
            .unwrap();
        assert_eq!(results, bytes_as_coo(EXPECTED_BYTES_CROSS_2));
    }
}
