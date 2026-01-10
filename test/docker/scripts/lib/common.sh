#!/bin/bash
# Common functions for barrel_docdb Docker tests

# Node URLs (use 127.0.0.1 instead of localhost to avoid IPv6 issues)
BARREL1="http://127.0.0.1:8091"
BARREL2="http://127.0.0.1:8092"
BARREL3="http://127.0.0.1:8093"
BARREL4="http://127.0.0.1:8094"
BARREL5="http://127.0.0.1:8095"

# API key for authentication
API_KEY="test_admin_key_for_docker_tests"
AUTH_HEADER="Authorization: Bearer $API_KEY"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Wait for all nodes to be healthy
wait_for_cluster() {
    local nodes=$1
    local max_wait=120
    local waited=0

    for i in $(seq 1 $nodes); do
        local port=$((8090 + i))
        echo -n "Waiting for barrel$i..."
        while ! curl -sf "http://127.0.0.1:$port/health" > /dev/null 2>&1; do
            sleep 1
            waited=$((waited + 1))
            if [ $waited -ge $max_wait ]; then
                echo -e " ${RED}TIMEOUT${NC}"
                exit 1
            fi
        done
        echo -e " ${GREEN}OK${NC}"
    done
}

# Create database
create_db() {
    local url=$1
    local db=$2
    curl -sf -X PUT "$url/db/$db" -H "Content-Type: application/json" -H "$AUTH_HEADER"
}

# Delete database
delete_db() {
    local url=$1
    local db=$2
    curl -sf -X DELETE "$url/db/$db" -H "$AUTH_HEADER" 2>/dev/null || true
}

# Put document (extracts _id from doc JSON and uses PUT)
put_doc() {
    local url=$1
    local db=$2
    local doc=$3
    # Extract _id from doc if present
    local doc_id=$(echo "$doc" | jq -r '._id // empty')
    if [ -z "$doc_id" ]; then
        # Generate UUID for doc without _id
        doc_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
    fi
    curl -sf -X PUT "$url/db/$db/$doc_id" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d "$doc"
}

# Get document
get_doc() {
    local url=$1
    local db=$2
    local id=$3
    curl -sf "$url/db/$db/$id" -H "$AUTH_HEADER"
}

# Get changes
get_changes() {
    local url=$1
    local db=$2
    local since=${3:-first}
    curl -sf "$url/db/$db/_changes?since=$since" -H "$AUTH_HEADER"
}

# Query documents
find_docs() {
    local url=$1
    local db=$2
    local query=$3
    curl -sf -X POST "$url/db/$db/_find" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d "$query"
}

# Add peer
add_peer() {
    local url=$1
    local peer_url=$2
    curl -sf -X POST "$url/_peers" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d "{\"url\": \"$peer_url\"}"
}

# Assert equals
assert_eq() {
    local expected=$1
    local actual=$2
    local msg=$3
    if [ "$expected" != "$actual" ]; then
        echo -e "${RED}FAIL${NC}: $msg (expected: $expected, got: $actual)"
        exit 1
    fi
    echo -e "${GREEN}PASS${NC}: $msg"
}

# Assert contains
assert_contains() {
    local haystack=$1
    local needle=$2
    local msg=$3
    if [[ "$haystack" != *"$needle"* ]]; then
        echo -e "${RED}FAIL${NC}: $msg (expected to contain: $needle)"
        echo "Actual: $haystack"
        exit 1
    fi
    echo -e "${GREEN}PASS${NC}: $msg"
}

# Assert not contains
assert_not_contains() {
    local haystack=$1
    local needle=$2
    local msg=$3
    if [[ "$haystack" == *"$needle"* ]]; then
        echo -e "${RED}FAIL${NC}: $msg (should not contain: $needle)"
        exit 1
    fi
    echo -e "${GREEN}PASS${NC}: $msg"
}

# Print section header
section() {
    echo ""
    echo -e "${YELLOW}=== $1 ===${NC}"
    echo ""
}

# Print test name
test_start() {
    echo -e "Test: $1"
}
