#!/bin/bash
# Replication Tests for barrel_docdb
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

section "Replication Tests"

# Setup
delete_db "$BARREL1" "rep_source"
delete_db "$BARREL2" "rep_target"
create_db "$BARREL1" "rep_source"
create_db "$BARREL2" "rep_target"

# Test 1: One-shot replication
test_start "One-shot replication"

# Create documents on source
for i in $(seq 1 10); do
    put_doc "$BARREL1" "rep_source" "{\"_id\": \"doc$i\", \"value\": $i}" > /dev/null
done

# Trigger replication via HTTP
result=$(curl -sf -X POST "$BARREL1/db/rep_source/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel2:8080/db/rep_target"}')
assert_contains "$result" "ok" "Replication initiated"

# Wait for replication to complete
sleep 3

# Verify documents replicated
for i in $(seq 1 10); do
    result=$(get_doc "$BARREL2" "rep_target" "doc$i")
    assert_contains "$result" "\"value\":$i" "Document doc$i replicated"
done

# Test 2: Incremental replication
test_start "Incremental replication"

# Add more documents to source
for i in $(seq 11 15); do
    put_doc "$BARREL1" "rep_source" "{\"_id\": \"doc$i\", \"value\": $i}" > /dev/null
done

# Trigger another replication (should only replicate new docs)
result=$(curl -sf -X POST "$BARREL1/db/rep_source/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel2:8080/db/rep_target"}')
assert_contains "$result" "ok" "Incremental replication initiated"

sleep 2

# Verify new documents replicated
for i in $(seq 11 15); do
    result=$(get_doc "$BARREL2" "rep_target" "doc$i")
    assert_contains "$result" "\"value\":$i" "Document doc$i replicated incrementally"
done

# Cleanup first test
delete_db "$BARREL1" "rep_source"
delete_db "$BARREL2" "rep_target"

# Test 3: Filtered replication (by path)
test_start "Filtered replication (by path pattern)"

delete_db "$BARREL1" "rep_filtered_source"
delete_db "$BARREL3" "rep_filtered_target"
create_db "$BARREL1" "rep_filtered_source"
create_db "$BARREL3" "rep_filtered_target"

# Create mixed documents
put_doc "$BARREL1" "rep_filtered_source" '{"_id": "user1", "type": "user", "name": "Bob"}' > /dev/null
put_doc "$BARREL1" "rep_filtered_source" '{"_id": "order1", "type": "order", "total": 100}' > /dev/null
put_doc "$BARREL1" "rep_filtered_source" '{"_id": "user2", "type": "user", "name": "Carol"}' > /dev/null
put_doc "$BARREL1" "rep_filtered_source" '{"_id": "order2", "type": "order", "total": 200}' > /dev/null

# Replicate only users (filter by type/user path)
result=$(curl -sf -X POST "$BARREL1/db/rep_filtered_source/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "target": "http://barrel3:8080/db/rep_filtered_target",
        "filter": {"paths": ["type/user"]}
    }')
assert_contains "$result" "ok" "Filtered replication initiated"

sleep 2

# Verify only users replicated
result=$(get_doc "$BARREL3" "rep_filtered_target" "user1")
assert_contains "$result" "Bob" "User1 replicated"

result=$(get_doc "$BARREL3" "rep_filtered_target" "user2")
assert_contains "$result" "Carol" "User2 replicated"

# Orders should NOT be replicated
result=$(curl -sf "$BARREL3/db/rep_filtered_target/order1" -H "$AUTH_HEADER" 2>&1 || echo "not_found")
assert_contains "$result" "not_found" "Order1 not replicated (filtered out)"

result=$(curl -sf "$BARREL3/db/rep_filtered_target/order2" -H "$AUTH_HEADER" 2>&1 || echo "not_found")
assert_contains "$result" "not_found" "Order2 not replicated (filtered out)"

# Test 4: Bidirectional replication
test_start "Bidirectional replication"

delete_db "$BARREL4" "bidir_db"
delete_db "$BARREL5" "bidir_db"
create_db "$BARREL4" "bidir_db"
create_db "$BARREL5" "bidir_db"

# Add document to barrel4
put_doc "$BARREL4" "bidir_db" '{"_id": "from4", "source": "barrel4"}' > /dev/null

# Add document to barrel5
put_doc "$BARREL5" "bidir_db" '{"_id": "from5", "source": "barrel5"}' > /dev/null

# Replicate barrel4 -> barrel5
curl -sf -X POST "$BARREL4/db/bidir_db/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel5:8080/db/bidir_db"}' > /dev/null

# Replicate barrel5 -> barrel4
curl -sf -X POST "$BARREL5/db/bidir_db/_replicate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"target": "http://barrel4:8080/db/bidir_db"}' > /dev/null

sleep 2

# Verify both nodes have both documents
result=$(get_doc "$BARREL5" "bidir_db" "from4")
assert_contains "$result" "barrel4" "Document from barrel4 reached barrel5"

result=$(get_doc "$BARREL4" "bidir_db" "from5")
assert_contains "$result" "barrel5" "Document from barrel5 reached barrel4"

# Cleanup
delete_db "$BARREL1" "rep_filtered_source"
delete_db "$BARREL3" "rep_filtered_target"
delete_db "$BARREL4" "bidir_db"
delete_db "$BARREL5" "bidir_db"

echo ""
echo -e "${GREEN}--- Replication Tests Complete ---${NC}"
