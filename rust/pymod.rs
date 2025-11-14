use pyo3::{exceptions::PyValueError, prelude::*};
use std::usize;

#[pyclass]
struct CachedSymdel {
    internal: super::CachedSymdel,
}

#[pymethods]
impl CachedSymdel {
    #[new]
    fn new(reference: Vec<String>, max_distance: usize) -> PyResult<Self> {
        check_strings_ascii(&reference)?;
        let internal = super::CachedSymdel::new(reference, max_distance);
        Ok(CachedSymdel { internal })
    }

    fn symdel_within(
        &self,
        max_distance: usize,
        zero_index: bool,
    ) -> PyResult<Vec<(usize, usize, usize)>> {
        self.internal
            .symdel_within(max_distance, zero_index)
            .map_err(PyValueError::new_err)
    }

    fn symdel_cross(
        &self,
        query: Vec<String>,
        max_distance: usize,
        zero_index: bool,
    ) -> PyResult<Vec<(usize, usize, usize)>> {
        check_strings_ascii(&query)?;
        self.internal
            .symdel_cross(&query, max_distance, zero_index)
            .map_err(PyValueError::new_err)
    }

    fn symdel_cross_against_cached(
        &self,
        query: PyRef<Self>,
        max_distance: usize,
        zero_index: bool,
    ) -> PyResult<Vec<(usize, usize, usize)>> {
        self.internal
            .symdel_cross_against_cached(&query.internal, max_distance, zero_index)
            .map_err(PyValueError::new_err)
    }
}

// #[pyfunction]
// fn symdel_within(
//     query: Vec<String>,
//     max_distance: usize,
//     zero_index: bool,
// ) -> PyResult<Vec<(usize, usize, usize)>> {
//     check_strings_ascii(&query)?;
//     Ok(super::get_candidates_within(
//         &query,
//         max_distance,
//         zero_index,
//     ))
// }
//
// #[pyfunction]
// fn symdel_cross(
//     query: Vec<String>,
//     reference: Vec<String>,
//     max_distance: usize,
//     zero_index: bool,
// ) -> PyResult<Vec<(usize, usize, usize)>> {
//     check_strings_ascii(&query)?;
//     check_strings_ascii(&reference)?;
//     Ok(super::get_candidates_cross(
//         &query,
//         &reference,
//         max_distance,
//         zero_index,
//     ))
// }

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
    // m.add_function(wrap_pyfunction!(symdel_within, m)?)?;
    // m.add_function(wrap_pyfunction!(symdel_cross, m)?)?;
    m.add_class::<CachedSymdel>()?;
    Ok(())
}
