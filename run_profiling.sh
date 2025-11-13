#! /bin/sh
echo "NOTE: make sure you are running this shell script from outside the Python venv or else rustc will complain about some linking issues"

echo "Building CLI binary..."
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release

echo "Building Python package..."
source ./.venv/bin/activate
pip install .[dev]

CURRENT_COMMIT=$(python ./profiling/scripts/get_version.py)
CURRENT_TIME=$(date -Iseconds)

if [ ! -d "profiling/results" ]; then
  mkdir "profiling/results"
fi

echo "Running perf on CLI..."
perf record -g -o "profiling/results/perf.data.within.${CURRENT_COMMIT}" ./target/release/nearust test_files/cdr3b_1m_a.txt > /dev/null
perf record -g -o "profiling/results/perf.data.cross.${CURRENT_COMMIT}" ./target/release/nearust test_files/cdr3b_1m_a.txt test_files/cdr3b_1m_b.txt > /dev/null

echo "Running memray on Python package..."
memray run -o "/tmp/memray_within_${CURRENT_TIME}.bin" --aggregate ./profiling/scripts/profiling_within.py
memray run -o "/tmp/memray_cross_${CURRENT_TIME}.bin" --aggregate ./profiling/scripts/profiling_cross.py
memray run -o "/tmp/memray_memoized_${CURRENT_TIME}.bin" --aggregate ./profiling/scripts/profiling_memoized.py

memray flamegraph -o "profiling/results/memray.within.${CURRENT_COMMIT}.html" "/tmp/memray_within_${CURRENT_TIME}.bin"
memray flamegraph -o "profiling/results/memray.cross.${CURRENT_COMMIT}.html" "/tmp/memray_cross_${CURRENT_TIME}.bin"
memray flamegraph -o "profiling/results/memray.memoized.${CURRENT_COMMIT}.html" "/tmp/memray_memoized_${CURRENT_TIME}.bin"
