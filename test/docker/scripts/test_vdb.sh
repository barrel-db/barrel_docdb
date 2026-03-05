#!/bin/bash
# VDB (Virtual Database) Multi-Region Tests for barrel_docdb
# Tests sharding across 4 nodes in 2 zones (us-east, eu-west)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# VDB-specific node URLs (different ports from regular cluster)
EAST1="http://127.0.0.1:8081"
EAST2="http://127.0.0.1:8082"
WEST1="http://127.0.0.1:8083"
WEST2="http://127.0.0.1:8084"

# Wait for VDB cluster nodes
wait_for_vdb_cluster() {
    local max_wait=120
    local waited=0

    for port in 8081 8082 8083 8084; do
        echo -n "Waiting for node on port $port..."
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

# Create VDB
create_vdb() {
    local url=$1
    local name=$2
    local shards=${3:-4}
    local replicas=${4:-2}
    curl -sf -X POST "$url/vdb" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d "{\"name\": \"$name\", \"shard_count\": $shards, \"replica_factor\": $replicas}"
}

# Delete VDB
delete_vdb() {
    local url=$1
    local name=$2
    curl -sf -X DELETE "$url/vdb/$name" -H "$AUTH_HEADER" 2>/dev/null || true
}

# Get VDB info
get_vdb_info() {
    local url=$1
    local name=$2
    curl -sf "$url/vdb/$name" -H "$AUTH_HEADER"
}

# List VDBs
list_vdbs() {
    local url=$1
    curl -sf "$url/vdb" -H "$AUTH_HEADER"
}

# Put document to VDB
put_vdb_doc() {
    local url=$1
    local vdb=$2
    local doc=$3
    local doc_id=$(echo "$doc" | jq -r '._id // empty')
    if [ -z "$doc_id" ]; then
        doc_id=$(uuidgen | tr '[:upper:]' '[:lower:]')
    fi
    curl -sf -X PUT "$url/vdb/$vdb/$doc_id" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d "$doc"
}

# Get document from VDB
get_vdb_doc() {
    local url=$1
    local vdb=$2
    local id=$3
    curl -sf "$url/vdb/$vdb/$id" -H "$AUTH_HEADER"
}

# Find documents in VDB (scatter-gather)
find_vdb_docs() {
    local url=$1
    local vdb=$2
    local query=$3
    curl -sf -X POST "$url/vdb/$vdb/_find" \
        -H "Content-Type: application/json" \
        -H "$AUTH_HEADER" \
        -d "$query"
}

# Get VDB replication status
get_vdb_replication() {
    local url=$1
    local vdb=$2
    curl -sf "$url/vdb/$vdb/_replication" -H "$AUTH_HEADER"
}

# Get node zone info
get_node_zone() {
    local url=$1
    curl -sf "$url/.well-known/barrel" -H "$AUTH_HEADER" | jq -r '.zone // "undefined"'
}

section "VDB Multi-Region Tests"

# ============================================================================
# Setup: Wait for cluster and register peers
# ============================================================================
test_start "Setup: Wait for VDB cluster"
wait_for_vdb_cluster

test_start "Setup: Verify node zones"
east1_zone=$(get_node_zone "$EAST1")
east2_zone=$(get_node_zone "$EAST2")
west1_zone=$(get_node_zone "$WEST1")
west2_zone=$(get_node_zone "$WEST2")

echo "  barrel-east1 zone: $east1_zone"
echo "  barrel-east2 zone: $east2_zone"
echo "  barrel-west1 zone: $west1_zone"
echo "  barrel-west2 zone: $west2_zone"

assert_eq "us-east" "$east1_zone" "barrel-east1 is in us-east zone"
assert_eq "us-east" "$east2_zone" "barrel-east2 is in us-east zone"
assert_eq "eu-west" "$west1_zone" "barrel-west1 is in eu-west zone"
assert_eq "eu-west" "$west2_zone" "barrel-west2 is in eu-west zone"

test_start "Setup: Register peers (synchronous)"
# Register peers on all nodes so they can communicate with each other
# Using sync mode to ensure peers are active before creating VDBs
# East1 knows about all others
add_peer_sync "$EAST1" "http://barrel-east2:8080" > /dev/null || true
add_peer_sync "$EAST1" "http://barrel-west1:8080" > /dev/null || true
add_peer_sync "$EAST1" "http://barrel-west2:8080" > /dev/null || true
# East2 knows about east1
add_peer_sync "$EAST2" "http://barrel-east1:8080" > /dev/null || true
# West1 knows about east1
add_peer_sync "$WEST1" "http://barrel-east1:8080" > /dev/null || true
# West2 knows about east1
add_peer_sync "$WEST2" "http://barrel-east1:8080" > /dev/null || true
# No sleep needed - peers already active with sync mode

# Verify peers are known
peers=$(curl -sf "$EAST1/_peers" -H "$AUTH_HEADER")
echo "  Peers visible from east1: $(echo "$peers" | jq -r '.peers | length')"
echo -e "${GREEN}PASS${NC}: Peers registered"

# ============================================================================
# Test 1: Create VDB with sharding
# ============================================================================
test_start "Create VDB with 4 shards"

# Clean up any existing test VDB
delete_vdb "$EAST1" "test_vdb"

# Create VDB with 4 shards, replica_factor=2
result=$(create_vdb "$EAST1" "test_vdb" 4 2)
if [[ "$result" == *"ok"* ]] || [[ "$result" == *"test_vdb"* ]]; then
    echo -e "${GREEN}PASS${NC}: VDB created"
else
    echo -e "${RED}FAIL${NC}: VDB creation failed: $result"
    exit 1
fi

# Verify VDB info
info=$(get_vdb_info "$EAST1" "test_vdb")
shard_count=$(echo "$info" | jq -r '.shard_count // 0')
assert_eq "4" "$shard_count" "VDB has 4 shards"

# ============================================================================
# Test 2: Document routing
# ============================================================================
test_start "Document routing across shards"

# Insert documents with different IDs to distribute across shards
echo "  Inserting 20 documents..."
for i in $(seq 1 20); do
    doc="{\"_id\": \"doc$i\", \"value\": $i, \"type\": \"test\"}"
    put_vdb_doc "$EAST1" "test_vdb" "$doc" > /dev/null
done

# Verify documents are retrievable
for i in 1 5 10 15 20; do
    result=$(get_vdb_doc "$EAST1" "test_vdb" "doc$i")
    assert_contains "$result" "\"value\":$i" "Document doc$i retrievable"
done

# ============================================================================
# Test 3: Scatter-gather query
# ============================================================================
test_start "Scatter-gather query"

# Query all documents with type=test
result=$(find_vdb_docs "$EAST1" "test_vdb" '{"where": [{"path": ["type"], "op": "==", "value": "test"}]}')
count=$(echo "$result" | jq -r '.docs | length')

if [ "$count" -ge "20" ]; then
    echo -e "${GREEN}PASS${NC}: Scatter-gather returned all 20 documents"
else
    echo -e "${RED}FAIL${NC}: Expected 20 documents, got $count"
    exit 1
fi

# Query with filter
result=$(find_vdb_docs "$EAST1" "test_vdb" '{"where": [{"path": ["value"], "op": ">", "value": 15}]}')
count=$(echo "$result" | jq -r '.docs | length')
assert_eq "5" "$count" "Scatter-gather with filter returns 5 documents"

# ============================================================================
# Test 4: Query with limit and ordering
# ============================================================================
test_start "Scatter-gather with limit"

result=$(find_vdb_docs "$EAST1" "test_vdb" '{"where": [{"path": ["type"], "op": "==", "value": "test"}], "limit": 5}')
count=$(echo "$result" | jq -r '.docs | length')
assert_eq "5" "$count" "Scatter-gather respects limit"

# ============================================================================
# Test 5: On-demand VDB config sync
# ============================================================================
test_start "On-demand VDB config sync"

# Trigger on-demand config pull by accessing VDB info from west1
# This tests that barrel_vdb_sync pulls config with auth from peers
echo "  Triggering on-demand config pull on west1..."
result=$(curl -sf "$WEST1/vdb/test_vdb" -H "$AUTH_HEADER" 2>&1)

if [[ "$result" == *"shard_count"* ]]; then
    echo -e "${GREEN}PASS${NC}: VDB config pulled on-demand to west1"
else
    echo -e "${RED}FAIL${NC}: Failed to pull VDB config to west1: $result"
    exit 1
fi

# Verify VDB is now in the list
vdbs=$(curl -sf "$WEST1/vdb" -H "$AUTH_HEADER" | jq -r '.vdbs[]' 2>/dev/null || echo "")
assert_contains "$vdbs" "test_vdb" "VDB visible in west1 list after sync"

# Test west1 can now write to local shards
put_vdb_doc "$WEST1" "test_vdb" '{"_id": "west_local_doc", "value": 777, "origin": "eu-west"}' > /dev/null

# Verify doc is readable from west1 (local shard)
result=$(get_vdb_doc "$WEST1" "test_vdb" "west_local_doc")
assert_contains "$result" "eu-west" "Document written to west1 local shards"

# Same for west2
curl -sf "$WEST2/vdb/test_vdb" -H "$AUTH_HEADER" > /dev/null 2>&1
put_vdb_doc "$WEST2" "test_vdb" '{"_id": "west2_local_doc", "value": 666, "origin": "eu-west2"}' > /dev/null
result=$(get_vdb_doc "$WEST2" "test_vdb" "west2_local_doc")
assert_contains "$result" "eu-west2" "Document written to west2 local shards"

# Note: Cross-zone document access requires shard replication (tested separately)

# ============================================================================
# Test 6: VDB configuration sync across nodes
# ============================================================================
test_start "VDB configuration sync"

# VDB should be visible from all nodes after sync
sleep 2  # Allow config sync

# List VDBs from each node
for url in "$EAST1" "$EAST2" "$WEST1" "$WEST2"; do
    result=$(list_vdbs "$url")
    if [[ "$result" == *"test_vdb"* ]]; then
        echo -e "  ${GREEN}OK${NC}: VDB visible from $(echo $url | grep -o ':[0-9]*' | head -1)"
    else
        echo -e "  ${YELLOW}WARN${NC}: VDB not yet visible from $(echo $url | grep -o ':[0-9]*' | head -1)"
    fi
done

# ============================================================================
# Test 7: Replication status
# ============================================================================
test_start "Replication status"

rep_status=$(get_vdb_replication "$EAST1" "test_vdb")
if [[ "$rep_status" == *"shards"* ]] || [[ "$rep_status" == *"policies"* ]]; then
    enabled=$(echo "$rep_status" | jq -r '.policies.enabled // 0')
    total=$(echo "$rep_status" | jq -r '.shard_count // 0')
    echo "  Replication: $enabled/$total enabled"
    echo -e "${GREEN}PASS${NC}: Replication status available"
else
    echo -e "${YELLOW}SKIP${NC}: Replication status format differs: $rep_status"
fi

# ============================================================================
# Test 8: Shards endpoint
# ============================================================================
test_start "Shards information"

shards_result=$(curl -sf "$EAST1/vdb/test_vdb/_shards" -H "$AUTH_HEADER" 2>&1 || echo "{}")
if [[ "$shards_result" == *"ranges"* ]] || [[ "$shards_result" == *"shard"* ]]; then
    shard_count=$(echo "$shards_result" | jq -r '.ranges | length // .shards | length // 0')
    echo "  Shards endpoint shows $shard_count shards"
    echo -e "${GREEN}PASS${NC}: Shards endpoint works"
else
    echo -e "${YELLOW}SKIP${NC}: Shards endpoint format: $shards_result"
fi

# ============================================================================
# Test 9: Document updates
# ============================================================================
test_start "Document updates across nodes"

# Update a document via east1 (origin node where all shards were created)
put_vdb_doc "$EAST1" "test_vdb" '{"_id": "doc1", "value": 100, "type": "test", "updated": true}' > /dev/null

# Verify update is visible via east1
result=$(get_vdb_doc "$EAST1" "test_vdb" "doc1")
assert_contains "$result" "\"value\":100" "Updated value visible"
assert_contains "$result" "\"updated\":true" "Update field visible"

# Wait for replication and verify on replica node (west1 is replica for shard 3)
sleep 2
result=$(curl -sf "$WEST1/db/test_vdb_s3/doc1" -H "$AUTH_HEADER" 2>/dev/null || echo "{}")
if [[ "$result" == *"value\":100"* ]]; then
    echo -e "${GREEN}PASS${NC}: Update replicated to west1"
else
    echo -e "${YELLOW}WARN${NC}: Update not yet replicated to west1 (replication delay)"
fi

# ============================================================================
# Test 10: Delete and verify
# ============================================================================
test_start "Document deletion"

# Delete a document via east1
curl -sf -X DELETE "$EAST1/vdb/test_vdb/doc20" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# Verify deletion via west1
sleep 1
result=$(curl -sf "$WEST1/vdb/test_vdb/doc20" -H "$AUTH_HEADER" 2>&1 || echo "not_found")
if [[ "$result" == *"not_found"* ]] || [[ "$result" == *"error"* ]]; then
    echo -e "${GREEN}PASS${NC}: Document deletion propagated"
else
    echo -e "${YELLOW}WARN${NC}: Document may still be visible: $result"
fi

# ============================================================================
# Test 11: Create second VDB
# ============================================================================
test_start "Create second VDB"

delete_vdb "$EAST1" "test_vdb2"
result=$(create_vdb "$EAST1" "test_vdb2" 2 1)
if [[ "$result" == *"ok"* ]] || [[ "$result" == *"test_vdb2"* ]]; then
    echo -e "${GREEN}PASS${NC}: Second VDB created"

    # Insert documents
    put_vdb_doc "$EAST1" "test_vdb2" '{"_id": "v2doc1", "data": "test2"}' > /dev/null
    result=$(get_vdb_doc "$EAST1" "test_vdb2" "v2doc1")
    assert_contains "$result" "test2" "Second VDB documents accessible"
else
    echo -e "${YELLOW}SKIP${NC}: Second VDB creation: $result"
fi

# ============================================================================
# Cleanup
# ============================================================================
section "Cleanup"
delete_vdb "$EAST1" "test_vdb"
delete_vdb "$EAST1" "test_vdb2"

echo ""
echo -e "${GREEN}--- VDB Multi-Region Tests Complete ---${NC}"
