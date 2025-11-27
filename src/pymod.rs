use crate::{
    collect_true_hits, compute_dists, get_candidates_cross, get_candidates_within,
    CachedSymdel as CSInternal, MaxDistance,
};
use numpy::IntoPyArray;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyTuple};

#[pyclass]
struct CachedSymdel {
    internal: CSInternal,
}

#[pymethods]
impl CachedSymdel {
    #[new]
    #[pyo3(signature = (reference, max_distance = 1))]
    fn new(reference: Vec<String>, max_distance: u8) -> PyResult<Self> {
        check_strings_ascii(&reference)?;
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;
        let internal = CSInternal::new(&reference, max_distance).map_err(PyValueError::new_err)?;
        Ok(CachedSymdel { internal })
    }

    #[pyo3(signature = (query = None, max_distance = 1, zero_index = true))]
    fn symdel<'py>(
        &self,
        py: Python<'py>,
        query: Option<&Bound<'py, PyAny>>,
        max_distance: u8,
        zero_index: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

        let (candidates, dists) = match query {
            Some(q_given) => {
                if let Ok(cached) = q_given.cast::<CachedSymdel>() {
                    self.internal
                        .get_candidates_cross_against_cached(
                            &cached.borrow().internal,
                            max_distance,
                        )
                        .map_err(PyValueError::new_err)?
                } else if let Ok(seq) = q_given.extract::<Vec<String>>() {
                    self.internal
                        .get_candidates_cross(&seq, max_distance)
                        .map_err(PyValueError::new_err)?
                } else {
                    let type_name = q_given
                        .get_type()
                        .name()
                        .map(|pys| pys.to_string())
                        .unwrap_or("UNKNOWN".to_string());
                    return Err(PyValueError::new_err(format!(
                        "query must be either a sequence of str or CachedSymdel or None, got '{type_name}'",
                    )));
                }
            }
            None => self
                .internal
                .get_candidates_within(max_distance)
                .map_err(PyValueError::new_err)?,
        };

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
#[pyo3(signature = (query, reference = None, max_distance = 1, zero_index = true))]
fn symdel<'py>(
    py: Python<'py>,
    query: Vec<String>,
    reference: Option<Vec<String>>,
    max_distance: u8,
    zero_index: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    check_strings_ascii(&query)?;
    let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

    let (candidates, dists) = match reference {
        Some(ref_given) => {
            check_strings_ascii(&ref_given)?;
            let candidates = get_candidates_cross(&query, &ref_given, max_distance)
                .map_err(PyValueError::new_err)?;
            let dists = compute_dists(&candidates, &query, &ref_given, max_distance);
            (candidates, dists)
        }
        None => {
            let candidates =
                get_candidates_within(&query, max_distance).map_err(PyValueError::new_err)?;
            let dists = compute_dists(&candidates, &query, &query, max_distance);
            (candidates, dists)
        }
    };

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
fn nearust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(symdel, m)?)?;
    m.add_class::<CachedSymdel>()?;
    Ok(())
}
