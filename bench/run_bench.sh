#!/bin/bash
#
# Run barrel_docdb benchmarks
#
# Usage:
#   ./run_bench.sh                     # Run legacy benchmark (10000 docs, 10000 iterations)
#   ./run_bench.sh 1000 1000           # Run with custom num_docs and iterations
#   ./run_bench.sh doc_types           # Run document type comparison benchmark
#   ./run_bench.sh doc_types 500 200   # Doc types with custom num_docs and iterations
#   ./run_bench.sh vdb                 # Run VDB vs non-sharded comparison
#   ./run_bench.sh vdb 5000 100        # VDB with custom num_docs and iterations
#   ./run_bench.sh http                # Run HTTP API vs Direct API comparison
#   ./run_bench.sh http 1000 100       # HTTP with custom num_docs and iterations
#

set -e

cd "$(dirname "$0")"

echo "Building benchmark..."
rebar3 compile

case "$1" in
    doc_types)
        NUM_DOCS=${2:-1000}
        ITERATIONS=${3:-500}
        echo "Running document type benchmark with num_docs=$NUM_DOCS, iterations=$ITERATIONS..."
        erl -pa _build/default/lib/*/ebin \
            -pa ../_build/default/lib/*/ebin \
            -noshell \
            -eval "barrel_bench:run_doc_types(#{num_docs => $NUM_DOCS, iterations => $ITERATIONS}), halt()."
        ;;
    vdb)
        NUM_DOCS=${2:-5000}
        ITERATIONS=${3:-5000}
        echo "Running VDB benchmark with num_docs=$NUM_DOCS, iterations=$ITERATIONS..."
        erl -pa _build/default/lib/*/ebin \
            -pa ../_build/default/lib/*/ebin \
            -noshell \
            -eval "barrel_bench:run_vdb(#{num_docs => $NUM_DOCS, iterations => $ITERATIONS}), halt()."
        ;;
    http)
        NUM_DOCS=${2:-1000}
        ITERATIONS=${3:-1000}
        echo "Running HTTP API benchmark with num_docs=$NUM_DOCS, iterations=$ITERATIONS..."
        erl -pa _build/default/lib/*/ebin \
            -pa ../_build/default/lib/*/ebin \
            -noshell \
            -eval "barrel_bench:run_http(#{num_docs => $NUM_DOCS, iterations => $ITERATIONS}), halt()."
        ;;
    *)
        NUM_DOCS=${1:-10000}
        ITERATIONS=${2:-10000}
        echo "Running benchmark with num_docs=$NUM_DOCS, iterations=$ITERATIONS..."
        erl -pa _build/default/lib/*/ebin \
            -pa ../_build/default/lib/*/ebin \
            -noshell \
            -eval "barrel_bench:run(#{num_docs => $NUM_DOCS, iterations => $ITERATIONS}), halt()."
        ;;
esac
