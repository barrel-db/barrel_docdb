#!/bin/bash
# Common functions for barrel_docdb Docker tests

# Node URLs (use 127.0.0.1 instead of localhost to avoid IPv6 issues)
BARREL1="http://127.0.0.1:8091"
BARREL2="http://127.0.0.1:8092"
BARREL3="http://127.0.0.1:8093"
BARREL4="http://127.0.0.1:8094"
BARREL5="http://127.0.0.1:8095"

# API key for authentication (must start with ak_ prefix)
API_KEY="ak_test_admin_key_for_docker"
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

# Add peer with synchronous option (waits for peer to become active)
add_peer_sync() {
    local url=$1
    local peer_url=$2
    curl -sf -X POST "$url/_peers" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d "{\"url\": \"$peer_url\", \"sync\": true}"
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

# Wait for a document to appear (polling instead of hardcoded sleep)
# Usage: wait_for_doc URL DB DOC_ID [MAX_WAIT_SECS] [EXPECTED_CONTENT]
wait_for_doc() {
    local url=$1
    local db=$2
    local doc_id=$3
    local max_wait=${4:-30}
    local expected=${5:-}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        local result=$(curl -sf "$url/db/$db/$doc_id" -H "$AUTH_HEADER" 2>/dev/null || echo "not_found")
        if [[ "$result" != "not_found" ]] && [[ "$result" != *"error"* ]]; then
            if [ -z "$expected" ] || [[ "$result" == *"$expected"* ]]; then
                echo "$result"
                return 0
            fi
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "timeout"
    return 1
}

# Count documents in a database using _find
# Usage: count_docs URL DB
count_docs() {
    local url=$1
    local db=$2
    local result=$(curl -sf -X POST "$url/db/$db/_find" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d '{"where": {}, "limit": 10000}' 2>/dev/null)
    # _find returns results in 'results' field
    echo "$result" | jq -r '.results | length' 2>/dev/null || echo "0"
}

# Wait for document count to reach expected value
# Usage: wait_for_doc_count URL DB EXPECTED_COUNT [MAX_WAIT_SECS]
wait_for_doc_count() {
    local url=$1
    local db=$2
    local expected=$3
    local max_wait=${4:-60}
    local waited=0

    while [ $waited -lt $max_wait ]; do
        local count=$(count_docs "$url" "$db")
        if [ "$count" -ge "$expected" ] 2>/dev/null; then
            echo "$count"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "$(count_docs "$url" "$db")"
    return 1
}

# Get changes feed and return count of results
# Usage: get_changes_count URL DB [SINCE]
get_changes_count() {
    local url=$1
    local db=$2
    local since=${3:-first}
    local result=$(curl -sf "$url/db/$db/_changes?since=$since" -H "$AUTH_HEADER" 2>/dev/null)
    echo "$result" | jq -r '.results | length' 2>/dev/null || echo "0"
}

# Verify a batch of documents are replicated
# Usage: verify_batch_replicated URL DB PREFIX START END [FIELD] [MAX_WAIT]
# Returns: number of docs found
verify_batch_replicated() {
    local url=$1
    local db=$2
    local prefix=$3
    local start=$4
    local end=$5
    local field=${6:-value}
    local max_wait=${7:-30}
    local found=0
    local total=$((end - start + 1))

    echo -n "  Verifying $total docs ($prefix$start to $prefix$end)... "

    for i in $(seq $start $end); do
        local doc_id="${prefix}${i}"
        local result=$(wait_for_doc "$url" "$db" "$doc_id" "$max_wait")
        if [[ "$result" != "timeout" ]] && [[ "$result" != *"error"* ]]; then
            found=$((found + 1))
        fi
    done

    if [ $found -eq $total ]; then
        echo -e "${GREEN}$found/$total${NC}"
        return 0
    else
        echo -e "${RED}$found/$total${NC}"
        return 1
    fi
}

# Assert document count equals expected
# Usage: assert_doc_count URL DB EXPECTED MSG
assert_doc_count() {
    local url=$1
    local db=$2
    local expected=$3
    local msg=$4
    local actual=$(count_docs "$url" "$db")
    if [ "$actual" -eq "$expected" ] 2>/dev/null; then
        echo -e "${GREEN}PASS${NC}: $msg ($actual docs)"
    else
        echo -e "${RED}FAIL${NC}: $msg (expected $expected, got $actual)"
        exit 1
    fi
}

# Create batch of documents
# Usage: create_batch URL DB PREFIX START END [EXTRA_FIELDS]
create_batch() {
    local url=$1
    local db=$2
    local prefix=$3
    local start=$4
    local end=$5
    local extra=${6:-}

    for i in $(seq $start $end); do
        local doc="{\"_id\": \"${prefix}${i}\", \"value\": $i${extra:+, $extra}}"
        put_doc "$url" "$db" "$doc" > /dev/null
    done
}

# Compare two documents and verify all properties match (ignoring _rev)
# Usage: compare_docs DOC1 DOC2 MSG
# Returns: 0 if match, 1 if mismatch
compare_docs() {
    local doc1=$1
    local doc2=$2
    local msg=$3

    # Normalize: remove _rev field and sort keys for comparison
    local norm1=$(echo "$doc1" | jq -S 'del(._rev)')
    local norm2=$(echo "$doc2" | jq -S 'del(._rev)')

    if [ "$norm1" = "$norm2" ]; then
        echo -e "${GREEN}PASS${NC}: $msg"
        return 0
    else
        echo -e "${RED}FAIL${NC}: $msg"
        echo "  Source: $norm1"
        echo "  Target: $norm2"
        return 1
    fi
}

# Verify documents match between source and target databases
# Usage: verify_doc_matches SOURCE_URL SOURCE_DB TARGET_URL TARGET_DB DOC_ID MSG
verify_doc_matches() {
    local source_url=$1
    local source_db=$2
    local target_url=$3
    local target_db=$4
    local doc_id=$5
    local msg=$6

    local source_doc=$(get_doc "$source_url" "$source_db" "$doc_id")
    local target_doc=$(get_doc "$target_url" "$target_db" "$doc_id")

    compare_docs "$source_doc" "$target_doc" "$msg"
}

# Verify a batch of documents match between source and target
# Usage: verify_batch_matches SOURCE_URL SOURCE_DB TARGET_URL TARGET_DB PREFIX START END [MAX_ERRORS]
# Returns: number of mismatches
verify_batch_matches() {
    local source_url=$1
    local source_db=$2
    local target_url=$3
    local target_db=$4
    local prefix=$5
    local start=$6
    local end=$7
    local max_errors=${8:-3}
    local mismatches=0
    local total=$((end - start + 1))

    echo -n "  Comparing $total docs ($prefix$start to $prefix$end)... "

    for i in $(seq $start $end); do
        local doc_id="${prefix}${i}"
        local source_doc=$(get_doc "$source_url" "$source_db" "$doc_id")
        local target_doc=$(get_doc "$target_url" "$target_db" "$doc_id")

        # Normalize: remove _rev field and sort keys
        local norm1=$(echo "$source_doc" | jq -S 'del(._rev)' 2>/dev/null)
        local norm2=$(echo "$target_doc" | jq -S 'del(._rev)' 2>/dev/null)

        if [ "$norm1" != "$norm2" ]; then
            mismatches=$((mismatches + 1))
            if [ $mismatches -le $max_errors ]; then
                echo ""
                echo -e "    ${RED}Mismatch${NC}: $doc_id"
                echo "      Source: $norm1"
                echo "      Target: $norm2"
            fi
        fi
    done

    if [ $mismatches -eq 0 ]; then
        echo -e "${GREEN}ALL MATCH${NC}"
        return 0
    else
        echo -e "${RED}$mismatches/$total MISMATCHED${NC}"
        return 1
    fi
}
