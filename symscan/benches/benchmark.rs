use criterion::{criterion_group, criterion_main, Criterion};
use std::io::{self, BufRead, Cursor};
use symscan::{symdel_cross, symdel_within, CachedSymdel};

static QUERY_BYTES: &[u8] = include_bytes!("../../test_files/cdr3b_10k_a.txt");
static REFERENCE_BYTES: &[u8] = include_bytes!("../../test_files/cdr3b_10k_b.txt");

fn bytes_as_ascii_lines(bytes: &[u8]) -> Vec<String> {
    Cursor::new(bytes)
        .lines()
        .collect::<io::Result<Vec<String>>>()
        .expect("test files have valid lines")
}

fn setup_benchmarks(c: &mut Criterion) {
    let query = bytes_as_ascii_lines(QUERY_BYTES);
    let reference = bytes_as_ascii_lines(REFERENCE_BYTES);
    let cached_query = CachedSymdel::new(&query, 1).expect("short input");
    let cached_reference = CachedSymdel::new(&reference, 1).expect("short input");

    c.bench_function("get_candidates_within", |b| {
        b.iter(|| {
            let _ = symdel_within(&query, 1);
        })
    });

    c.bench_function("get_candidates_cross", |b| {
        b.iter(|| {
            let _ = symdel_cross(&query, &reference, 1);
        })
    });

    c.bench_function("get_candidates_within (cached)", |b| {
        b.iter(|| {
            let _ = cached_reference.symdel_within(1);
        })
    });

    c.bench_function("get_candidates_cross (partially cached)", |b| {
        b.iter(|| {
            let _ = cached_reference.symdel_cross(&query, 1);
        })
    });

    c.bench_function("get_candidates_cross (fully cached)", |b| {
        b.iter(|| {
            let _ = cached_reference.symdel_cross_against_cached(&cached_query, 1);
        })
    });

    c.bench_function("cached instantiation", |b| {
        b.iter(|| {
            let _ = CachedSymdel::new(&reference, 1);
        })
    });
}

criterion_group!(bench, setup_benchmarks);
criterion_main!(bench);
