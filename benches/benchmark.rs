use _lib::{get_candidates_cross, get_candidates_within, get_input_lines_as_ascii};
use criterion::{criterion_group, criterion_main, Criterion};
use std::fs::File;
use std::io::BufReader;

fn setup_benchmarks(c: &mut Criterion) {
    let f =
        BufReader::new(File::open("test_files/cdr3b_10k_a.txt").expect("can't read test query"));
    let query = get_input_lines_as_ascii(f).expect("can't process test query");

    let f = BufReader::new(
        File::open("test_files/cdr3b_10k_b.txt").expect("can't read test reference"),
    );
    let reference = get_input_lines_as_ascii(f).expect("can't process test reference");

    c.bench_function("within", |b| {
        b.iter(|| {
            let _ = get_candidates_within(&query, 1);
        })
    });

    c.bench_function("cross", |b| {
        b.iter(|| {
            let _ = get_candidates_cross(&query, &reference, 1);
        })
    });
}

criterion_group!(bench, setup_benchmarks);
criterion_main!(bench);
