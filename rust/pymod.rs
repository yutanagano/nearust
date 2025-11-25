use super::{
    collect_true_hits, compute_dists, get_candidates_cross, get_candidates_within, Integer,
    MaxDistance,
};
use numpy::IntoPyArray;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyTuple};
use std::usize;

#[pyclass]
struct CachedSymdel {
    internal: CSTyped,
}

enum CSTyped {
    U32(super::CachedSymdel<u32>),
    U64(super::CachedSymdel<u64>),
}

#[pymethods]
impl CachedSymdel {
    #[new]
    fn new(reference: Vec<String>, max_distance: u8) -> PyResult<Self> {
        check_strings_ascii(&reference)?;
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

        if reference.len() < u32::MAX_INDEXABLE_LEN_RAW {
            let internal = CSTyped::U32(
                super::CachedSymdel::new(&reference, max_distance).expect("short input"),
            );
            Ok(CachedSymdel { internal })
        } else {
            let internal = CSTyped::U64(
                super::CachedSymdel::new(&reference, max_distance).expect("short input"),
            );
            Ok(CachedSymdel { internal })
        }
    }

    fn symdel_within<'py>(
        &self,
        py: Python<'py>,
        max_distance: u8,
        zero_index: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

        match &self.internal {
            CSTyped::U32(cached) => {
                let (candidates, dists) = cached
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
            CSTyped::U64(cached) => {
                let (candidates, dists) = cached
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
        }
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

        if query.len() < u32::MAX_INDEXABLE_LEN_RAW {
            match &self.internal {
                CSTyped::U32(cached) => {
                    let (candidates, dists) = cached
                        .get_candidates_cross::<u32>(&query, max_distance)
                        .map_err(PyValueError::new_err)?;
                    let (qi, ri, filtered_dists) =
                        collect_true_hits(&candidates, &dists, max_distance, zero_index);

                    return PyTuple::new(
                        py,
                        &[
                            qi.into_pyarray(py).as_any(),
                            ri.into_pyarray(py).as_any(),
                            filtered_dists.into_pyarray(py).as_any(),
                        ],
                    );
                }
                _ => (),
            }
        }

        let (candidates, dists) = match &self.internal {
            CSTyped::U32(cached) => cached
                .get_candidates_cross::<u64>(&query, max_distance)
                .map_err(PyValueError::new_err)?,
            CSTyped::U64(cached) => cached
                .get_candidates_cross::<u64>(&query, max_distance)
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

    fn symdel_cross_against_cached<'py>(
        &self,
        py: Python<'py>,
        query: PyRef<Self>,
        max_distance: u8,
        zero_index: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

        match (&query.internal, &self.internal) {
            (CSTyped::U32(cached_q), CSTyped::U32(cached_r)) => {
                let (candidates, dists) = cached_r
                    .get_candidates_cross_against_cached::<u32, u32>(cached_q, max_distance)
                    .map_err(PyValueError::new_err)?;
                let (qi, ri, filtered_dists) =
                    collect_true_hits(&candidates, &dists, max_distance, zero_index);

                return PyTuple::new(
                    py,
                    &[
                        qi.into_pyarray(py).as_any(),
                        ri.into_pyarray(py).as_any(),
                        filtered_dists.into_pyarray(py).as_any(),
                    ],
                );
            }
            _ => (),
        };

        let (candidates, dists) = match (&query.internal, &self.internal) {
            (CSTyped::U32(cached_q), CSTyped::U64(cached_r)) => cached_r
                .get_candidates_cross_against_cached::<u64, u32>(cached_q, max_distance)
                .map_err(PyValueError::new_err)?,
            (CSTyped::U64(cached_q), CSTyped::U32(cached_r)) => cached_r
                .get_candidates_cross_against_cached::<u64, u64>(cached_q, max_distance)
                .map_err(PyValueError::new_err)?,
            (CSTyped::U64(cached_q), CSTyped::U64(cached_r)) => cached_r
                .get_candidates_cross_against_cached::<u64, u64>(cached_q, max_distance)
                .map_err(PyValueError::new_err)?,
            _ => unreachable!("this condition is checked in the block above"),
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
fn symdel_within<'py>(
    py: Python<'py>,
    query: Vec<String>,
    max_distance: u8,
    zero_index: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    check_strings_ascii(&query)?;
    let max_distance = MaxDistance::try_from(max_distance).map_err(PyValueError::new_err)?;

    if query.len() <= u32::MAX_INDEXABLE_LEN_RAW {
        let candidates =
            get_candidates_within::<u32>(&query, max_distance).map_err(PyValueError::new_err)?;
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
    } else {
        let candidates =
            get_candidates_within::<u64>(&query, max_distance).map_err(PyValueError::new_err)?;
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

    if query.len() <= u32::MAX_INDEXABLE_LEN_CROSS
        && reference.len() <= u32::MAX_INDEXABLE_LEN_CROSS
    {
        let candidates = get_candidates_cross::<u32>(&query, &reference, max_distance)
            .map_err(PyValueError::new_err)?;
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
    } else {
        let candidates = get_candidates_cross::<u64>(&query, &reference, max_distance)
            .map_err(PyValueError::new_err)?;
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
