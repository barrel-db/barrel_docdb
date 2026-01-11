#!/bin/bash
# Main test runner for barrel_docdb Docker tests
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

source "$SCRIPT_DIR/lib/common.sh"

# Parse arguments
BUILD=true
CLEANUP=false
TESTS="all"

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-build)
            BUILD=false
            shift
            ;;
        --cleanup)
            CLEANUP=true
            shift
            ;;
        --test)
            TESTS="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --no-build    Skip Docker build"
            echo "  --cleanup     Remove containers and volumes after tests"
            echo "  --test NAME   Run specific test (http_api, replication, federation, policies, tiered)"
            echo ""
            echo "Note: VDB multi-region tests use a separate cluster. Run them with:"
            echo "  ./run_vdb_tests.sh"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "  barrel_docdb Docker Test Suite"
echo "=========================================="
echo ""

cd "$DOCKER_DIR"

# Build and start cluster
if [ "$BUILD" = true ]; then
    echo "Building Docker images..."
    docker-compose build
fi

echo "Starting 5-node cluster..."
docker-compose up -d

# Wait for cluster to be healthy
wait_for_cluster 5

# Run test suites
run_test() {
    local name=$1
    local script=$2

    if [ "$TESTS" = "all" ] || [ "$TESTS" = "$name" ]; then
        "$SCRIPT_DIR/$script"
        echo ""
    fi
}

run_test "http_api" "test_http_api.sh"
run_test "replication" "test_replication.sh"
run_test "federation" "test_federation.sh"
run_test "policies" "test_policies.sh"
run_test "tiered" "test_tiered.sh"

echo "=========================================="
echo -e "  ${GREEN}All Tests Completed${NC}"
echo "=========================================="

# Cleanup if requested
if [ "$CLEANUP" = true ]; then
    echo ""
    echo "Cleaning up..."
    docker-compose down -v
    echo "Done."
fi
