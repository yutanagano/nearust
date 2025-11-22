use _lib::{get_candidates_within, get_input_lines_as_ascii};
use criterion::{criterion_group, criterion_main, Criterion};
use std::fs::File;
use std::io::BufReader;

pub fn within_bench(c: &mut Criterion) {
    let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
    let test_input = get_input_lines_as_ascii(f).expect("can't read test input file");

    c.bench_function("within", |b| {
        b.iter(|| {
            let _ = get_candidates_within(&test_input, 1);
        })
    });
}

criterion_group!(bench, within_bench);
criterion_main!(bench);
