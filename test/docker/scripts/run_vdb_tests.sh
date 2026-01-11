#!/bin/bash
# Run VDB Multi-Region Tests
# Usage: ./run_vdb_tests.sh [--keep]
#   --keep: Keep containers running after tests

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
KEEP_CONTAINERS=false

# Parse arguments
for arg in "$@"; do
    case $arg in
        --keep)
            KEEP_CONTAINERS=true
            shift
            ;;
    esac
done

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== VDB Multi-Region Test Suite ===${NC}"
echo ""

# Cleanup function
cleanup() {
    if [ "$KEEP_CONTAINERS" = false ]; then
        echo ""
        echo -e "${YELLOW}Cleaning up...${NC}"
        cd "$DOCKER_DIR"
        docker compose -f docker-compose-vdb.yml down -v 2>/dev/null || true
    else
        echo ""
        echo -e "${YELLOW}Keeping containers running. To stop:${NC}"
        echo "  cd $DOCKER_DIR && docker compose -f docker-compose-vdb.yml down -v"
    fi
}

# Set trap for cleanup on exit
trap cleanup EXIT

# Start the VDB cluster
echo -e "${YELLOW}Starting 4-node VDB cluster (2 zones)...${NC}"
cd "$DOCKER_DIR"

# Build and start
docker compose -f docker-compose-vdb.yml build
docker compose -f docker-compose-vdb.yml up -d

# Wait for health checks
echo ""
echo -e "${YELLOW}Waiting for all nodes to be healthy...${NC}"
max_wait=180
waited=0

while true; do
    healthy=0
    for container in barrel-east1 barrel-east2 barrel-west1 barrel-west2; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "not_found")
        if [ "$status" = "healthy" ]; then
            healthy=$((healthy + 1))
        fi
    done

    if [ $healthy -eq 4 ]; then
        echo -e "${GREEN}All 4 nodes healthy!${NC}"
        break
    fi

    waited=$((waited + 5))
    if [ $waited -ge $max_wait ]; then
        echo -e "${RED}Timeout waiting for nodes to be healthy${NC}"
        echo "Node status:"
        for container in barrel-east1 barrel-east2 barrel-west1 barrel-west2; do
            status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "not_found")
            echo "  $container: $status"
        done
        exit 1
    fi

    echo "  $healthy/4 nodes healthy, waiting... ($waited/$max_wait seconds)"
    sleep 5
done

# Run the tests
echo ""
echo -e "${YELLOW}Running VDB tests...${NC}"
echo ""

"$SCRIPT_DIR/test_vdb.sh"

echo ""
echo -e "${GREEN}=== All VDB Tests Passed! ===${NC}"
