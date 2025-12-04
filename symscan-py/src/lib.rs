use numpy::IntoPyArray;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyString, PyTuple},
};
use symscan::{symdel_cross, symdel_within, CachedSymdel as CSInternal, NeighbourPairs};

/// A memoized implementation of symdel.
///
/// An implementation of symdel where the deletion variant computations for the
/// reference string set is memoized and stored in memory as a hashmap. This is
/// useful for use-cases where you want to repeatedly query the same reference,
/// especially if the reference is very large.
///
/// Parameters
/// ----------
/// reference : iterable of str
/// max_distance : int, default=1
///     The maximum edit distance that this CachedSymdel instance will be able
///     to support when detecting similar strings. That is, if `max_distance`
///     is set to X at construction, later calls to this instance's symdel
///     method will only be accept `max_distance` less than or equal to X.
#[pyclass]
struct CachedSymdel {
    internal: CSInternal,
}

#[pymethods]
impl CachedSymdel {
    #[new]
    #[pyo3(signature = (reference, max_distance = 1))]
    fn new(reference: &Bound<PyAny>, max_distance: u8) -> PyResult<Self> {
        let ref_handles = get_pystring_handles(&reference)?;
        let ref_views = get_str_refs(&ref_handles)?;
        check_strings_ascii(&ref_views)?;

        let internal = CSInternal::new(&ref_views, max_distance).map_err(PyValueError::new_err)?;

        Ok(CachedSymdel { internal })
    }

    /// Detect pairs of similar strings.
    ///
    /// Parameters
    /// ----------
    /// query : iterable of str or CachedSymdel or None, default=None
    /// max_distance : int, default=1
    ///     The maximum edit distance at which strings are considered
    ///     neighbours. This must be less than or equal to the `max_distance`
    ///     specified when constructing the caller instance (as well as that
    ///     when constructing `query`, if `query` is set to a CachedSymdel
    ///     instance).
    /// zero_index : bool, default=True
    ///     If set to True, reports the indices of strings of interest using
    ///     0-based indexing. Otherwise uses 1-based indexing.
    ///
    /// Returns
    /// -------
    /// .. tip::
    ///     If `query` is set to another CachedSymdel instance constructed
    ///     using some set of strings X, then the resulting computation is
    ///     equivalent to setting `query` to X directly. This is useful in
    ///     cases where you want to memoize deletion variant computations for
    ///     both query and reference sets.
    ///
    /// i : ndarray of shape (N,), dtype=uint32
    ///     Indices of strings in the query that have neighbors.
    ///
    /// j : ndarray of shape (N,), dtype=uint32
    ///     Indices of neighbor strings. If only the `query` parameter was set,
    ///     then ``query[i[k]]`` and ``query[j[k]]`` are neighbors. If both
    ///     `query` and `reference` parameters were set, then ``query[i[k]]``
    ///     and ``reference[j[k]]`` are neighbors.
    ///
    /// dists : ndarray of shape (N,), dtype=uint8
    ///     Edit distances between neighbors. If only the `query` parameter was
    ///     set, then ``Levenshtein(query[i[k]], query[j[k]]) = dists[k]``. If
    ///     both `query` and `reference` parameters were set, then
    ///     ``Levenshtein(query[i[k]], reference[j[k]]) = dists[k]``.
    ///
    ///
    /// Examples
    /// --------
    /// Construct a CachedSymdel instance with an iterable over reference
    /// strings. This pre-computes the deletion variants for the strings in the
    /// reference and stores the results in a hashmap held internally by the
    /// instance.
    ///
    /// >>> import symscan
    /// >>> cached = symscan.CachedSymdel(["fooo", "barr", "bazz", "buzz"])
    ///
    /// Then, call the symdel method with `query` set to an iterable over query
    /// strings to find similar strings across it and the reference set.
    ///
    /// >>> (i, j, dists) = cached.symdel(["fizz", "fuzz", "buzz"])
    /// >>> i
    /// array([1, 2, 2], dtype=uint32)
    /// >>> j
    /// array([3, 2, 3], dtype=uint32)
    /// >>> dists
    /// array([1, 1, 0], dtype=uint8)
    ///
    /// If you also want to memoize deletion variant computations on the query
    /// set as well, you can do so.
    ///
    /// >>> cached_query = symscan.CachedSymdel(["fizz", "fuzz", "buzz"])
    /// >>> (i, j, dists) = cached.symdel(cached_query)
    /// >>> i
    /// array([1, 2, 2], dtype=uint32)
    /// >>> j
    /// array([3, 2, 3], dtype=uint32)
    /// >>> dists
    /// array([1, 1, 0], dtype=uint8)
    ///
    /// If you call symdel without specifying `query`, the function will look
    /// for pairs of similar strings within the reference set.
    ///
    /// >>> (i, j, dists) = cached.symdel()
    /// >>> i
    /// array([2], dtype=uint32)
    /// >>> j
    /// array([3], dtype=uint32)
    /// >>> dists
    /// array([1], dtype=uint8)
    ///
    /// For a CachedSymdel instance to support calls to symdel with
    /// `max_distance` equal to X, it `max_distance` must be set to X or
    /// greater at construction time.
    ///
    /// >>> cached_maxd2 = symscan.CachedSymdel(["fooo", "barr", "bazz", "buzz"], max_distance=2)
    /// >>> (i, j, dists) = cached_maxd2.symdel(["fizz", "fuzz", "buzz"])
    /// >>> i
    /// array([1, 2, 2], dtype=uint32)
    /// >>> j
    /// array([3, 2, 3], dtype=uint32)
    /// >>> dists
    /// array([1, 1, 0], dtype=uint8)
    /// >>> (i, j, dists) = cached_maxd2.symdel(["fizz", "fuzz", "buzz"], max_distance=2)
    /// >>> i
    /// array([0, 0, 1, 1, 2, 2], dtype=uint32)
    /// >>> j
    /// array([2, 3, 2, 3, 2, 3], dtype=uint32)
    /// >>> dists
    /// array([2, 2, 2, 1, 1, 0], dtype=uint8)
    /// >>> # max_distance > 2 will throw an error!: cached_maxd2.symdel(["fizz", "fuzz", "buzz"], max_distance=3)
    #[pyo3(signature = (query = None, max_distance = 1, zero_index = true))]
    fn symdel<'py>(
        &self,
        py: Python<'py>,
        query: Option<&Bound<'py, PyAny>>,
        max_distance: u8,
        zero_index: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let NeighbourPairs { row, col, dists } = match query {
            Some(q_given) => {
                if let Ok(cached) = q_given.cast::<CachedSymdel>() {
                    self.internal
                        .symdel_cross_against_cached(&cached.borrow().internal, max_distance)
                        .map_err(PyValueError::new_err)?
                } else if let Ok(iterable) = q_given.try_iter() {
                    let query_handles = get_pystring_handles(&iterable)?;
                    let query_views = get_str_refs(&query_handles)?;
                    self.internal
                        .symdel_cross(&query_views, max_distance)
                        .map_err(PyValueError::new_err)?
                } else {
                    let type_name = q_given
                        .get_type()
                        .name()
                        .map(|pys| pys.to_string())
                        .unwrap_or("UNKNOWN".to_string());
                    return Err(PyValueError::new_err(format!(
                        "query must be either an iterable of str or CachedSymdel or None, got '{type_name}'",
                    )));
                }
            }
            None => self
                .internal
                .symdel_within(max_distance)
                .map_err(PyValueError::new_err)?,
        };

        PyTuple::new(
            py,
            &[
                row.into_pyarray(py).as_any(),
                col.into_pyarray(py).as_any(),
                dists.into_pyarray(py).as_any(),
            ],
        )
    }
}

/// Detect pairs of similar strings.
///
/// Parameters
/// ----------
/// query : iterable of str
/// reference : iterable of str, optional
/// max_distance : int, default=1
///     The maximum edit distance at which strings are considered neighbors.
/// zero_index : bool, default=True
///     If set to True, reports the indices of strings of interest using
///     0-based indexing. Otherwise uses 1-based indexing.
///
/// Returns
/// -------
/// i : ndarray of shape (N,), dtype=uint32
///     Indices of strings in the query that have neighbors.
///
/// j : ndarray of shape (N,), dtype=uint32
///     Indices of neighbor strings. If only the `query` parameter was set,
///     then ``query[i[k]]`` and ``query[j[k]]`` are neighbors. If both `query`
///     and `reference` parameters were set, then ``query[i[k]]`` and
///     ``reference[j[k]]`` are neighbors.
///
/// dists : ndarray of shape (N,), dtype=uint8
///     Edit distances between neighbors. If only the `query` parameter was
///     set, then ``Levenshtein(query[i[k]], query[j[k]]) = dists[k]``. If both
///     `query` and `reference` parameters were set, then
///     ``Levenshtein(query[i[k]], reference[j[k]]) = dists[k]``.
///
/// Examples
/// --------
/// Provide one iterable over strings to look for pairs of similar strings
/// within it.
///
/// >>> import symscan
/// >>> (i, j, dists) = symscan.symdel(["fizz", "fuzz", "buzz"])
/// >>> i
/// array([0, 1], dtype=uint32)
/// >>> j
/// array([1, 2], dtype=uint32)
/// >>> dists
/// array([1, 1], dtype=uint8)
///
/// To increase the threshold at which string pairs are considered similar, set
/// `max_distance`.
///
/// >>> (i, j, dists) = symscan.symdel(["fizz", "fuzz", "buzz"], max_distance=2)
/// >>> i
/// array([0, 0, 1], dtype=uint32)
/// >>> j
/// array([1, 2, 2], dtype=uint32)
/// >>> dists
/// array([1, 2, 1], dtype=uint8)
///
/// To look for pairs of similar strings across two sets, provide two iterables
/// over strings (`query` and `reference`).
///
/// >>> (i, j, dists) = symscan.symdel(["fizz", "fuzz", "buzz"], ["fooo", "barr", "bazz", "buzz"])
/// >>> i
/// array([1, 2, 2], dtype=uint32)
/// >>> j
/// array([3, 2, 3], dtype=uint32)
/// >>> dists
/// array([1, 1, 0], dtype=uint8)
///
/// If you would like the string indices returned to be 1-based instead of
/// 0-based (in a manner similar to the default behaviour of the CLI), you can
/// set `zero_index` to False.
///
/// >>> (i, j, dists) = symscan.symdel(["fizz", "fuzz", "buzz"], zero_index=False)
/// >>> i
/// array([1, 2], dtype=uint32)
/// >>> j
/// array([2, 3], dtype=uint32)
/// >>> dists
/// array([1, 1], dtype=uint8)
#[pyfunction]
#[pyo3(signature = (query, reference = None, max_distance = 1, zero_index = true))]
fn symdel<'py>(
    py: Python<'py>,
    query: &Bound<'py, PyAny>,
    reference: Option<&Bound<'py, PyAny>>,
    max_distance: u8,
    zero_index: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let query_handles = get_pystring_handles(&query)?;
    let query_views = get_str_refs(&query_handles)?;
    check_strings_ascii(&query_views)?;

    let NeighbourPairs { row, col, dists } = match reference {
        Some(ref_given) => {
            let ref_handles = get_pystring_handles(&ref_given)?;
            let ref_views = get_str_refs(&ref_handles)?;
            check_strings_ascii(&ref_views)?;
            symdel_cross(&query_views, &ref_views, max_distance).map_err(PyValueError::new_err)?
        }
        None => symdel_within(&query_views, max_distance).map_err(PyValueError::new_err)?,
    };

    PyTuple::new(
        py,
        &[
            row.into_pyarray(py).as_any(),
            col.into_pyarray(py).as_any(),
            dists.into_pyarray(py).as_any(),
        ],
    )
}

fn get_pystring_handles<'py>(input: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyString>>> {
    if let Ok(_) = input.cast::<PyString>() {
        Err(PyValueError::new_err("expected iterable of str, got str"))
    } else {
        input
            .try_iter()?
            .map(|v| v?.cast_into::<PyString>().map_err(PyErr::from))
            .collect::<PyResult<Vec<_>>>()
    }
}

fn get_str_refs<'py>(input: &'py [Bound<'py, PyString>]) -> PyResult<Vec<&'py str>> {
    input
        .iter()
        .map(|v| v.to_str())
        .collect::<PyResult<Vec<_>>>()
}

fn check_strings_ascii(strings: &[impl AsRef<str>]) -> Result<(), PyErr> {
    for (idx, s) in strings.iter().enumerate() {
        if !s.as_ref().is_ascii() {
            let err_msg = format!(
                "non-ASCII strings are currently unsupported ('{}' at index {idx})",
                s.as_ref()
            );
            return Err(PyValueError::new_err(err_msg));
        }
    }
    Ok(())
}

/// Fast detection of similar strings
#[pymodule(name = "symscan")]
fn symscan_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(symdel, m)?)?;
    m.add_class::<CachedSymdel>()?;
    Ok(())
}
