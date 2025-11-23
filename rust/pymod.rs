use super::{
    collect_true_hits, compute_dists, get_candidates_cross, get_candidates_within, MaxDistance,
};
use numpy::IntoPyArray;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyTuple};
use std::usize;

#[pyclass]
struct CachedSymdel {
    internal: super::CachedSymdel,
}

#[pymethods]
impl CachedSymdel {
    #[new]
    fn new(reference: Vec<String>, max_distance: u8) -> PyResult<Self> {
        check_strings_ascii(&reference)?;
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;
        let internal = super::CachedSymdel::new(&reference, max_distance);
        Ok(CachedSymdel { internal })
    }

    fn symdel_within<'py>(
        &self,
        py: Python<'py>,
        max_distance: u8,
        zero_index: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;
        let (candidates, dists) = self
            .internal
            .get_candidates_within(max_distance)
            .map_err(PyValueError::new_err)?;
        let (qi, ri, filtered_dists) =
            collect_true_hits(&candidates, &dists, max_distance, zero_index);

        PyTuple::new(
            py,
            &[
                qi.into_pyarray(py).as_any(),
                ri.into_pyarray(py).as_any(),
                filtered_dists.into_pyarray(py).as_any(),
            ],
        )
    }

    fn symdel_cross<'py>(
        &self,
        py: Python<'py>,
        query: Vec<String>,
        max_distance: u8,
        zero_index: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        check_strings_ascii(&query)?;
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;
        let (candidates, dists) = self
            .internal
            .get_candidates_cross(&query, max_distance)
            .map_err(PyValueError::new_err)?;
        let (qi, ri, filtered_dists) =
            collect_true_hits(&candidates, &dists, max_distance, zero_index);

        PyTuple::new(
            py,
            &[
                qi.into_pyarray(py).as_any(),
                ri.into_pyarray(py).as_any(),
                filtered_dists.into_pyarray(py).as_any(),
            ],
        )
    }

    fn symdel_cross_against_cached<'py>(
        &self,
        py: Python<'py>,
        query: PyRef<Self>,
        max_distance: u8,
        zero_index: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;
        let (candidates, dists) = self
            .internal
            .get_candidates_cross_against_cached(&query.internal, max_distance)
            .map_err(PyValueError::new_err)?;
        let (qi, ri, filtered_dists) =
            collect_true_hits(&candidates, &dists, max_distance, zero_index);

        PyTuple::new(
            py,
            &[
                qi.into_pyarray(py).as_any(),
                ri.into_pyarray(py).as_any(),
                filtered_dists.into_pyarray(py).as_any(),
            ],
        )
    }
}

#[pyfunction]
fn symdel_within<'py>(
    py: Python<'py>,
    query: Vec<String>,
    max_distance: u8,
    zero_index: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    check_strings_ascii(&query)?;
    let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

    let candidates = get_candidates_within(&query, max_distance).map_err(PyValueError::new_err)?;
    let dists = compute_dists(&candidates, &query, &query, max_distance);
    let (q_indices, ref_indices, dists) =
        collect_true_hits(&candidates, &dists, max_distance, zero_index);

    PyTuple::new(
        py,
        &[
            q_indices.into_pyarray(py).as_any(),
            ref_indices.into_pyarray(py).as_any(),
            dists.into_pyarray(py).as_any(),
        ],
    )
}

#[pyfunction]
fn symdel_cross<'py>(
    py: Python<'py>,
    query: Vec<String>,
    reference: Vec<String>,
    max_distance: u8,
    zero_index: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    check_strings_ascii(&query)?;
    check_strings_ascii(&reference)?;
    let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

    let candidates =
        get_candidates_cross(&query, &reference, max_distance).map_err(PyValueError::new_err)?;
    let dists = compute_dists(&candidates, &query, &reference, max_distance);
    let (q_indices, ref_indices, dists) =
        collect_true_hits(&candidates, &dists, max_distance, zero_index);

    PyTuple::new(
        py,
        &[
            q_indices.into_pyarray(py).as_any(),
            ref_indices.into_pyarray(py).as_any(),
            dists.into_pyarray(py).as_any(),
        ],
    )
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
    m.add_function(wrap_pyfunction!(symdel_within, m)?)?;
    m.add_function(wrap_pyfunction!(symdel_cross, m)?)?;
    m.add_class::<CachedSymdel>()?;
    Ok(())
}
