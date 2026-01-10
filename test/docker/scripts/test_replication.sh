#!/bin/bash
# Replication Tests for barrel_docdb
# Tests: one-shot, incremental, filtered, bidirectional, batch verification, changes feed
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

section "Replication Tests"

# ============================================================================
# Test 1: One-shot replication with batch verification
# ============================================================================
test_start "One-shot replication (50 docs batch)"

delete_db "$BARREL1" "rep_source"
delete_db "$BARREL2" "rep_target"
create_db "$BARREL1" "rep_source"
create_db "$BARREL2" "rep_target"

# Create 50 documents on source
echo "  Creating 50 documents on source..."
create_batch "$BARREL1" "rep_source" "doc" 1 50

# Verify source count
source_count=$(count_docs "$BARREL1" "rep_source")
assert_eq "50" "$source_count" "Source has 50 documents"

# Get changes count before replication
source_changes=$(get_changes_count "$BARREL1" "rep_source")
echo "  Source changes feed has $source_changes entries"

# Trigger replication via HTTP
result=$(curl -sf -X POST "$BARREL1/db/rep_source/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel2:8080/db/rep_target"}')
assert_contains "$result" "ok" "Replication initiated"

# Wait for replication using polling (not hardcoded sleep)
echo "  Waiting for replication to complete..."
target_count=$(wait_for_doc_count "$BARREL2" "rep_target" 50 60)
assert_eq "50" "$target_count" "Target has all 50 documents"

# Verify changes feed on target matches source
target_changes=$(get_changes_count "$BARREL2" "rep_target")
echo "  Target changes feed has $target_changes entries"
if [ "$target_changes" -ge 50 ]; then
    echo -e "${GREEN}PASS${NC}: Changes feed shows 50+ entries on target"
else
    echo -e "${RED}FAIL${NC}: Changes feed incomplete (expected 50+, got $target_changes)"
    exit 1
fi

# Verify a sample of documents have correct content
echo "  Verifying document content..."
for i in 1 10 25 50; do
    result=$(get_doc "$BARREL2" "rep_target" "doc$i")
    assert_contains "$result" "\"value\":$i" "Document doc$i has correct content"
done

# Verify ALL properties match between source and target
verify_batch_matches "$BARREL1" "rep_source" "$BARREL2" "rep_target" "doc" 1 50 || exit 1

# ============================================================================
# Test 2: Incremental replication
# ============================================================================
test_start "Incremental replication (add 25 more docs)"

# Add 25 more documents to source
echo "  Adding 25 more documents..."
create_batch "$BARREL1" "rep_source" "doc" 51 75

# Verify source now has 75
source_count=$(count_docs "$BARREL1" "rep_source")
assert_eq "75" "$source_count" "Source now has 75 documents"

# Trigger incremental replication
result=$(curl -sf -X POST "$BARREL1/db/rep_source/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel2:8080/db/rep_target"}')
assert_contains "$result" "ok" "Incremental replication initiated"

# Wait for all 75 docs
echo "  Waiting for incremental replication..."
target_count=$(wait_for_doc_count "$BARREL2" "rep_target" 75 60)
assert_eq "75" "$target_count" "Target has all 75 documents after incremental"

# Verify new documents
for i in 51 60 75; do
    result=$(get_doc "$BARREL2" "rep_target" "doc$i")
    assert_contains "$result" "\"value\":$i" "New document doc$i replicated"
done

# Verify ALL properties match for incremental docs
verify_batch_matches "$BARREL1" "rep_source" "$BARREL2" "rep_target" "doc" 51 75 || exit 1

# Cleanup
delete_db "$BARREL1" "rep_source"
delete_db "$BARREL2" "rep_target"

# ============================================================================
# Test 3: Filtered replication (by path)
# ============================================================================
test_start "Filtered replication (by path pattern)"

delete_db "$BARREL1" "rep_filtered_source"
delete_db "$BARREL3" "rep_filtered_target"
create_db "$BARREL1" "rep_filtered_source"
create_db "$BARREL3" "rep_filtered_target"

# Create mixed documents - 10 users, 10 orders
echo "  Creating mixed documents (10 users, 10 orders)..."
for i in $(seq 1 10); do
    put_doc "$BARREL1" "rep_filtered_source" "{\"_id\": \"user$i\", \"type\": \"user\", \"name\": \"User$i\"}" > /dev/null
    put_doc "$BARREL1" "rep_filtered_source" "{\"_id\": \"order$i\", \"type\": \"order\", \"total\": $((i * 100))}" > /dev/null
done

source_count=$(count_docs "$BARREL1" "rep_filtered_source")
assert_eq "20" "$source_count" "Source has 20 documents (10 users + 10 orders)"

# Replicate only users (filter by type/user path)
result=$(curl -sf -X POST "$BARREL1/db/rep_filtered_source/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "target": "http://barrel3:8080/db/rep_filtered_target",
        "filter": {"paths": ["type/user"]}
    }')
assert_contains "$result" "ok" "Filtered replication initiated"

# Wait for exactly 10 user documents
echo "  Waiting for filtered replication..."
target_count=$(wait_for_doc_count "$BARREL3" "rep_filtered_target" 10 30)
assert_eq "10" "$target_count" "Target has exactly 10 user documents"

# Verify users are replicated
for i in 1 5 10; do
    result=$(get_doc "$BARREL3" "rep_filtered_target" "user$i")
    assert_contains "$result" "User$i" "User$i replicated"
done

# Verify orders are NOT replicated
for i in 1 5 10; do
    result=$(curl -sf "$BARREL3/db/rep_filtered_target/order$i" -H "$AUTH_HEADER" 2>&1 || echo "not_found")
    assert_contains "$result" "not_found" "Order$i filtered out"
done

# Cleanup
delete_db "$BARREL1" "rep_filtered_source"
delete_db "$BARREL3" "rep_filtered_target"

# ============================================================================
# Test 4: Bidirectional replication
# ============================================================================
test_start "Bidirectional replication (15 docs each direction)"

delete_db "$BARREL4" "bidir_db"
delete_db "$BARREL5" "bidir_db"
create_db "$BARREL4" "bidir_db"
create_db "$BARREL5" "bidir_db"

# Add 15 documents to barrel4
echo "  Creating 15 docs on barrel4..."
for i in $(seq 1 15); do
    put_doc "$BARREL4" "bidir_db" "{\"_id\": \"from4_$i\", \"source\": \"barrel4\", \"index\": $i}" > /dev/null
done

# Add 15 different documents to barrel5
echo "  Creating 15 docs on barrel5..."
for i in $(seq 1 15); do
    put_doc "$BARREL5" "bidir_db" "{\"_id\": \"from5_$i\", \"source\": \"barrel5\", \"index\": $i}" > /dev/null
done

# Verify initial counts
count4=$(count_docs "$BARREL4" "bidir_db")
count5=$(count_docs "$BARREL5" "bidir_db")
assert_eq "15" "$count4" "Barrel4 has 15 documents"
assert_eq "15" "$count5" "Barrel5 has 15 documents"

# Replicate barrel4 -> barrel5
curl -sf -X POST "$BARREL4/db/bidir_db/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel5:8080/db/bidir_db"}' > /dev/null
echo -e "${GREEN}PASS${NC}: Barrel4 -> Barrel5 replication initiated"

# Replicate barrel5 -> barrel4
curl -sf -X POST "$BARREL5/db/bidir_db/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel4:8080/db/bidir_db"}' > /dev/null
echo -e "${GREEN}PASS${NC}: Barrel5 -> Barrel4 replication initiated"

# Wait for both to have 30 documents
echo "  Waiting for bidirectional sync..."
count4=$(wait_for_doc_count "$BARREL4" "bidir_db" 30 60)
count5=$(wait_for_doc_count "$BARREL5" "bidir_db" 30 60)

assert_eq "30" "$count4" "Barrel4 has all 30 documents"
assert_eq "30" "$count5" "Barrel5 has all 30 documents"

# Verify cross-replication
result=$(get_doc "$BARREL5" "bidir_db" "from4_10")
assert_contains "$result" "barrel4" "Document from barrel4 reached barrel5"

result=$(get_doc "$BARREL4" "bidir_db" "from5_10")
assert_contains "$result" "barrel5" "Document from barrel5 reached barrel4"

# Verify changes feeds show all documents
changes4=$(get_changes_count "$BARREL4" "bidir_db")
changes5=$(get_changes_count "$BARREL5" "bidir_db")
if [ "$changes4" -ge 30 ] && [ "$changes5" -ge 30 ]; then
    echo -e "${GREEN}PASS${NC}: Changes feeds show 30+ entries on both nodes"
else
    echo -e "${YELLOW}WARN${NC}: Changes feed counts: barrel4=$changes4, barrel5=$changes5"
fi

# Cleanup
delete_db "$BARREL4" "bidir_db"
delete_db "$BARREL5" "bidir_db"

# ============================================================================
# Test 5: Large batch replication (100 docs)
# ============================================================================
test_start "Large batch replication (100 docs)"

delete_db "$BARREL1" "batch_source"
delete_db "$BARREL2" "batch_target"
create_db "$BARREL1" "batch_source"
create_db "$BARREL2" "batch_target"

# Create 100 documents
echo "  Creating 100 documents on source..."
create_batch "$BARREL1" "batch_source" "batch" 1 100

source_count=$(count_docs "$BARREL1" "batch_source")
assert_eq "100" "$source_count" "Source has 100 documents"

# Trigger replication
result=$(curl -sf -X POST "$BARREL1/db/batch_source/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel2:8080/db/batch_target"}')
assert_contains "$result" "ok" "Batch replication initiated"

# Wait for all 100 docs
echo "  Waiting for 100 docs to replicate..."
target_count=$(wait_for_doc_count "$BARREL2" "batch_target" 100 90)
assert_eq "100" "$target_count" "Target has all 100 documents"

# Verify first, middle, and last documents
for i in 1 50 100; do
    result=$(get_doc "$BARREL2" "batch_target" "batch$i")
    assert_contains "$result" "\"value\":$i" "Batch document $i correct"
done

# Verify ALL 100 documents have matching properties
verify_batch_matches "$BARREL1" "batch_source" "$BARREL2" "batch_target" "batch" 1 100 || exit 1

# Cleanup
delete_db "$BARREL1" "batch_source"
delete_db "$BARREL2" "batch_target"

echo ""
echo -e "${GREEN}--- Replication Tests Complete ---${NC}"
