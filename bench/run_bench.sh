#!/bin/bash
#
# Run barrel_docdb benchmarks
#
# Usage:
#   ./run_bench.sh              # Run with defaults (10000 docs, 10000 iterations)
#   ./run_bench.sh 1000 1000    # Run with custom num_docs and iterations
#

set -e

cd "$(dirname "$0")"

NUM_DOCS=${1:-10000}
ITERATIONS=${2:-10000}

echo "Building benchmark..."
rebar3 compile

echo "Running benchmark with num_docs=$NUM_DOCS, iterations=$ITERATIONS..."
erl -pa _build/default/lib/*/ebin \
    -pa ../_build/default/lib/*/ebin \
    -noshell \
    -eval "barrel_bench:run(#{num_docs => $NUM_DOCS, iterations => $ITERATIONS}), halt()."
