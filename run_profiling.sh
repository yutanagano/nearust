#! /bin/sh
CURRENT_COMMIT=$(git rev-parse --short HEAD)
CURRENT_TIME=$(date -Iseconds)

echo "Building binary..."
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release

if [ ! -d "profiling" ]; then
  mkdir "profiling"
fi

echo "Profiling..."
perf record -g -o "profiling/${CURRENT_COMMIT}_${CURRENT_TIME}_1m_within.data" ./target/release/nearust test_files/cdr3b_1m_a.txt > /dev/null
perf record -g -o "profiling/${CURRENT_COMMIT}_${CURRENT_TIME}_1m_cross.data" ./target/release/nearust test_files/cdr3b_1m_a.txt test_files/cdr3b_1m_b.txt > /dev/null
