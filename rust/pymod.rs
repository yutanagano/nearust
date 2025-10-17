use pyo3::prelude::*;

#[pyfunction]
fn symdel_within_set(
    strings: Vec<String>,
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    super::symdel_within_set(&strings, max_distance, zero_index)
}

#[pyfunction]
fn symdel_across_sets(
    strings_primary: Vec<String>,
    strings_comparison: Vec<String>,
    max_distance: usize,
    zero_index: bool,
) -> Vec<(usize, usize, usize)> {
    super::symdel_across_sets(
        &strings_primary,
        &strings_comparison,
        max_distance,
        zero_index,
    )
}

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(symdel_within_set, m)?)?;
    m.add_function(wrap_pyfunction!(symdel_across_sets, m)?)?;
    Ok(())
}
