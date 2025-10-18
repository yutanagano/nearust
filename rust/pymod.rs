use pyo3::{exceptions::PyValueError, prelude::*};

#[pyfunction]
fn symdel_within_set(
    strings: Vec<String>,
    max_distance: usize,
    zero_index: bool,
) -> PyResult<Vec<(usize, usize, usize)>> {
    check_strings_ascii(&strings)?;
    Ok(super::symdel_within_set(&strings, max_distance, zero_index))
}

#[pyfunction]
fn symdel_across_sets(
    strings_primary: Vec<String>,
    strings_comparison: Vec<String>,
    max_distance: usize,
    zero_index: bool,
) -> PyResult<Vec<(usize, usize, usize)>> {
    check_strings_ascii(&strings_primary)?;
    check_strings_ascii(&strings_comparison)?;
    Ok(super::symdel_across_sets(
        &strings_primary,
        &strings_comparison,
        max_distance,
        zero_index,
    ))
}

fn check_strings_ascii(strings: &[String]) -> Result<(), PyErr> {
    for (idx, s) in strings.iter().enumerate() {
        if !s.is_ascii() {
            let err_msg =
                format!("non-ASCII strings are currently unsupported (\"{s}\" at index {idx})");
            return Err(PyValueError::new_err(err_msg));
        }
    }
    Ok(())
}

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(symdel_within_set, m)?)?;
    m.add_function(wrap_pyfunction!(symdel_across_sets, m)?)?;
    Ok(())
}
