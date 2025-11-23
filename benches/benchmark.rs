use _lib::{
    collect_true_hits, compute_dists, get_candidates_cross, get_candidates_within,
    get_input_lines_as_ascii, CachedSymdel, MaxDistance,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::io::Cursor;

static QUERY_BYTES: &[u8] = include_bytes!("../test_files/cdr3b_10k_a.txt");
static REFERENCE_BYTES: &[u8] = include_bytes!("../test_files/cdr3b_10k_b.txt");

fn bytes_as_ascii_lines(bytes: &[u8]) -> Vec<String> {
    get_input_lines_as_ascii(Cursor::new(bytes)).expect("test files should be valid ASCII")
}

fn setup_benchmarks(c: &mut Criterion) {
    let query = bytes_as_ascii_lines(QUERY_BYTES);
    let reference = bytes_as_ascii_lines(REFERENCE_BYTES);
    let mdist = MaxDistance::try_from(1).expect("1 is a valid MaxDistance");
    let cached_query = CachedSymdel::new(&query, mdist);
    let cached_reference = CachedSymdel::new(&reference, mdist);

    c.bench_function("get_candidates_within", |b| {
        b.iter(|| {
            let _ = get_candidates_within::<usize>(&query, mdist);
        })
    });

    c.bench_function("get_candidates_cross", |b| {
        b.iter(|| {
            let _ = get_candidates_cross(&query, &reference, mdist);
        })
    });

    c.bench_function("get_candidates_within (cached)", |b| {
        b.iter(|| {
            let _ = cached_reference.get_candidates_within(mdist);
        })
    });

    c.bench_function("get_candidates_cross (cached)", |b| {
        b.iter(|| {
            let _ = cached_reference.get_candidates_cross(&query, mdist);
        })
    });

    c.bench_function("get_candidates_cross (cached-on-cached)", |b| {
        b.iter(|| {
            let _ = cached_reference.get_candidates_cross_against_cached(&cached_query, mdist);
        })
    });

    c.bench_function("cached instantiation", |b| {
        b.iter(|| {
            let _ = CachedSymdel::new(&reference, mdist);
        })
    });

    c.bench_function("compute_dists", |b| {
        let candidates = get_candidates_cross(&query, &reference, mdist).expect("valid input");
        b.iter(|| {
            let _ = compute_dists(&candidates, &query, &reference, mdist);
        })
    });

    c.bench_function("get_true_hits", |b| {
        let candidates = get_candidates_cross(&query, &reference, mdist).expect("valid input");
        let dists = compute_dists(&candidates, &query, &reference, mdist);
        b.iter(|| {
            let _ = collect_true_hits(&candidates, &dists, mdist, true);
        })
    });
}

criterion_group!(bench, setup_benchmarks);
criterion_main!(bench);
