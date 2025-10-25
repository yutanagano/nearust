use pyo3::{exceptions::PyValueError, prelude::*};
use std::usize;

#[pyclass]
struct CachedCrossSymdel {
    internal: super::CachedCrossSymdel,
}

#[pymethods]
impl CachedCrossSymdel {
    #[new]
    fn new(reference: Vec<String>, max_distance: usize) -> PyResult<Self> {
        check_strings_ascii(&reference)?;
        let internal = super::CachedCrossSymdel::new(reference, max_distance);
        Ok(CachedCrossSymdel { internal })
    }

    fn symdel(
        &self,
        query: Vec<String>,
        max_distance: usize,
        zero_index: bool,
    ) -> PyResult<Vec<(usize, usize, usize)>> {
        check_strings_ascii(&query)?;
        self.internal
            .symdel(&query, max_distance, zero_index)
            .map_err(PyValueError::new_err)
    }
}

#[pyfunction]
fn symdel_within_set(
    query: Vec<String>,
    max_distance: usize,
    zero_index: bool,
) -> PyResult<Vec<(usize, usize, usize)>> {
    check_strings_ascii(&query)?;
    Ok(super::symdel_within_set(&query, max_distance, zero_index))
}

#[pyfunction]
fn symdel_across_sets(
    query: Vec<String>,
    reference: Vec<String>,
    max_distance: usize,
    zero_index: bool,
) -> PyResult<Vec<(usize, usize, usize)>> {
    check_strings_ascii(&query)?;
    check_strings_ascii(&reference)?;
    Ok(super::symdel_across_sets(
        &query,
        &reference,
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
    m.add_class::<CachedCrossSymdel>()?;
    Ok(())
}
