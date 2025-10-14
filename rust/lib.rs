use pyo3::prelude::*;

// pub mod so that symdel accessible as a rust library
pub mod symdel;

#[pyfunction]
#[pyo3(signature = (strings, max_edits = 2, zero_indexed = false))]
fn symdel_within_set(
    strings: Vec<String>,
    max_edits: usize,
    zero_indexed: bool,
) -> PyResult<Vec<[usize; 3]>> {
    Ok(symdel::run_symdel_within_set(
        &strings,
        max_edits,
        zero_indexed,
    ))
}

#[pyfunction]
#[pyo3(signature = (strings_primary, strings_comparison, max_edits = 2, zero_indexed = false))]
fn symdel_across_sets(
    strings_primary: Vec<String>,
    strings_comparison: Vec<String>,
    max_edits: usize,
    zero_indexed: bool,
) -> PyResult<Vec<[usize; 3]>> {
    Ok(symdel::run_symdel_across_sets(
        &strings_primary,
        &strings_comparison,
        max_edits,
        zero_indexed,
    ))
}

/// A Python module implemented in Rust.
#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(symdel_within_set, m)?)?;
    m.add_function(wrap_pyfunction!(symdel_across_sets, m)?)?;
    Ok(())
}
