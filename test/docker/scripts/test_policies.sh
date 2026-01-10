#!/bin/bash
# Replication Policy Tests for barrel_docdb
# Tests: chain, group, fanout patterns with batch verification and hard assertions
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

section "Replication Policy Tests"

# ============================================================================
# Test 1: Chain replication policy (barrel1 -> barrel2 -> barrel3)
# ============================================================================
test_start "Chain replication (barrel1 -> barrel2 -> barrel3)"

# Setup
delete_db "$BARREL1" "chain_db"
delete_db "$BARREL2" "chain_db"
delete_db "$BARREL3" "chain_db"
create_db "$BARREL1" "chain_db"
create_db "$BARREL2" "chain_db"
create_db "$BARREL3" "chain_db"

# Delete existing policy if any
curl -sf -X DELETE "$BARREL1/_policies/test_chain" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# Add 20 documents to source FIRST (before enabling policy)
echo "  Creating 20 documents on barrel1..."
create_batch "$BARREL1" "chain_db" "chain_doc" 1 20

# Verify source has all docs
source_count=$(count_docs "$BARREL1" "chain_db")
assert_eq "20" "$source_count" "Source (barrel1) has 20 documents"

# Create chain policy with auth for inter-node communication
result=$(curl -sf -X POST "$BARREL1/_policies" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "name": "test_chain",
        "pattern": "chain",
        "nodes": [
            "http://barrel1:8080",
            "http://barrel2:8080",
            "http://barrel3:8080"
        ],
        "database": "chain_db",
        "mode": "one_shot",
        "auth": {"bearer_token": "test_admin_key_for_docker_tests"}
    }')
assert_contains "$result" "ok" "Chain policy created"

# List policies
test_start "List policies"
result=$(curl -sf "$BARREL1/_policies" -H "$AUTH_HEADER")
assert_contains "$result" "test_chain" "Policy listed"

# Get policy
test_start "Get policy info"
result=$(curl -sf "$BARREL1/_policies/test_chain" -H "$AUTH_HEADER")
assert_contains "$result" "chain" "Policy info returned"

# Enable policy (this triggers one_shot replication with the 20 docs already present)
test_start "Enable chain policy"
result=$(curl -sf -X POST "$BARREL1/_policies/test_chain/_enable" -H "$AUTH_HEADER")
assert_contains "$result" "ok" "Policy enabled"

# Check status
test_start "Check policy status"
result=$(curl -sf "$BARREL1/_policies/test_chain/_status" -H "$AUTH_HEADER")
assert_contains "$result" "enabled" "Policy status shows enabled"

# Wait for propagation to barrel2 using polling
echo "  Waiting for chain propagation to barrel2..."
count2=$(wait_for_doc_count "$BARREL2" "chain_db" 20 60)
assert_eq "20" "$count2" "Barrel2 has all 20 documents"

# Wait for propagation to barrel3 (end of chain)
echo "  Waiting for chain propagation to barrel3..."
count3=$(wait_for_doc_count "$BARREL3" "chain_db" 20 60)
assert_eq "20" "$count3" "Barrel3 (end of chain) has all 20 documents"

# Verify specific documents reached end of chain
for i in 1 10 20; do
    result=$(get_doc "$BARREL3" "chain_db" "chain_doc$i")
    assert_contains "$result" "\"value\":$i" "chain_doc$i reached barrel3"
done

# Verify ALL document properties match through the chain
echo "  Verifying chain replication preserves all properties..."
verify_batch_matches "$BARREL1" "chain_db" "$BARREL2" "chain_db" "chain_doc" 1 20 || exit 1
verify_batch_matches "$BARREL2" "chain_db" "$BARREL3" "chain_db" "chain_doc" 1 20 || exit 1

# Verify changes feeds
changes1=$(get_changes_count "$BARREL1" "chain_db")
changes2=$(get_changes_count "$BARREL2" "chain_db")
changes3=$(get_changes_count "$BARREL3" "chain_db")
echo "  Changes feed counts: barrel1=$changes1, barrel2=$changes2, barrel3=$changes3"
if [ "$changes3" -ge 20 ]; then
    echo -e "${GREEN}PASS${NC}: Changes feed on barrel3 shows 20+ entries"
else
    echo -e "${RED}FAIL${NC}: Changes feed incomplete on barrel3"
    exit 1
fi

# Disable policy
test_start "Disable chain policy"
result=$(curl -sf -X POST "$BARREL1/_policies/test_chain/_disable" -H "$AUTH_HEADER")
assert_contains "$result" "ok" "Policy disabled"

# Delete policy
curl -sf -X DELETE "$BARREL1/_policies/test_chain" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# ============================================================================
# Test 2: Group replication (bidirectional barrel4 <-> barrel5)
# ============================================================================
test_start "Group replication (barrel4 <-> barrel5)"

delete_db "$BARREL4" "group_db"
delete_db "$BARREL5" "group_db"
create_db "$BARREL4" "group_db"
create_db "$BARREL5" "group_db"

# Delete existing policy if any
curl -sf -X DELETE "$BARREL1/_policies/test_group" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# Add 15 documents to each node FIRST (before enabling policy)
echo "  Creating 15 documents on barrel4..."
for i in $(seq 1 15); do
    put_doc "$BARREL4" "group_db" "{\"_id\": \"from4_$i\", \"source\": \"barrel4\", \"index\": $i}" > /dev/null
done

echo "  Creating 15 documents on barrel5..."
for i in $(seq 1 15); do
    put_doc "$BARREL5" "group_db" "{\"_id\": \"from5_$i\", \"source\": \"barrel5\", \"index\": $i}" > /dev/null
done

# Verify initial counts
count4=$(count_docs "$BARREL4" "group_db")
count5=$(count_docs "$BARREL5" "group_db")
assert_eq "15" "$count4" "Barrel4 initially has 15 documents"
assert_eq "15" "$count5" "Barrel5 initially has 15 documents"

# Create group policy with auth
result=$(curl -sf -X POST "$BARREL1/_policies" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "name": "test_group",
        "pattern": "group",
        "members": [
            "http://barrel4:8080/db/group_db",
            "http://barrel5:8080/db/group_db"
        ],
        "mode": "one_shot",
        "auth": {"bearer_token": "test_admin_key_for_docker_tests"}
    }')
assert_contains "$result" "ok" "Group policy created"

# Enable policy (this triggers one_shot replication with docs already present)
result=$(curl -sf -X POST "$BARREL1/_policies/test_group/_enable" -H "$AUTH_HEADER")
assert_contains "$result" "ok" "Group policy enabled"

# Wait for bidirectional sync - both should have 30 docs
echo "  Waiting for bidirectional group sync..."
count4=$(wait_for_doc_count "$BARREL4" "group_db" 30 60)
count5=$(wait_for_doc_count "$BARREL5" "group_db" 30 60)

assert_eq "30" "$count4" "Barrel4 has all 30 documents after sync"
assert_eq "30" "$count5" "Barrel5 has all 30 documents after sync"

# Verify cross-replication
result=$(get_doc "$BARREL5" "group_db" "from4_10")
assert_contains "$result" "barrel4" "Document from barrel4 reached barrel5"

result=$(get_doc "$BARREL4" "group_db" "from5_10")
assert_contains "$result" "barrel5" "Document from barrel5 reached barrel4"

# Verify ALL document properties match in both directions
echo "  Verifying bidirectional sync preserves all properties..."
# Documents from barrel4 should match on barrel5
for i in $(seq 1 15); do
    source_doc=$(get_doc "$BARREL4" "group_db" "from4_$i")
    target_doc=$(get_doc "$BARREL5" "group_db" "from4_$i")
    norm1=$(echo "$source_doc" | jq -S 'del(._rev)')
    norm2=$(echo "$target_doc" | jq -S 'del(._rev)')
    if [ "$norm1" != "$norm2" ]; then
        echo -e "${RED}FAIL${NC}: from4_$i properties mismatch"
        exit 1
    fi
done
echo -e "${GREEN}PASS${NC}: barrel4->barrel5 documents match"

# Documents from barrel5 should match on barrel4
for i in $(seq 1 15); do
    source_doc=$(get_doc "$BARREL5" "group_db" "from5_$i")
    target_doc=$(get_doc "$BARREL4" "group_db" "from5_$i")
    norm1=$(echo "$source_doc" | jq -S 'del(._rev)')
    norm2=$(echo "$target_doc" | jq -S 'del(._rev)')
    if [ "$norm1" != "$norm2" ]; then
        echo -e "${RED}FAIL${NC}: from5_$i properties mismatch"
        exit 1
    fi
done
echo -e "${GREEN}PASS${NC}: barrel5->barrel4 documents match"

# Cleanup
curl -sf -X POST "$BARREL1/_policies/test_group/_disable" -H "$AUTH_HEADER" > /dev/null 2>&1 || true
curl -sf -X DELETE "$BARREL1/_policies/test_group" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# ============================================================================
# Test 3: Fanout policy (barrel1 -> barrel2, barrel3, barrel4)
# ============================================================================
test_start "Fanout replication (barrel1 -> barrel2, barrel3, barrel4)"

delete_db "$BARREL1" "fanout_source"
delete_db "$BARREL2" "fanout_target"
delete_db "$BARREL3" "fanout_target"
delete_db "$BARREL4" "fanout_target"
create_db "$BARREL1" "fanout_source"
create_db "$BARREL2" "fanout_target"
create_db "$BARREL3" "fanout_target"
create_db "$BARREL4" "fanout_target"

# Delete existing policy if any
curl -sf -X DELETE "$BARREL1/_policies/test_fanout" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# Add 25 documents to source FIRST (before enabling policy)
echo "  Creating 25 documents on source..."
create_batch "$BARREL1" "fanout_source" "fanout_doc" 1 25

source_count=$(count_docs "$BARREL1" "fanout_source")
assert_eq "25" "$source_count" "Source has 25 documents"

# Create fanout policy with auth
result=$(curl -sf -X POST "$BARREL1/_policies" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "name": "test_fanout",
        "pattern": "fanout",
        "source": "http://barrel1:8080/db/fanout_source",
        "targets": [
            "http://barrel2:8080/db/fanout_target",
            "http://barrel3:8080/db/fanout_target",
            "http://barrel4:8080/db/fanout_target"
        ],
        "mode": "one_shot",
        "auth": {"bearer_token": "test_admin_key_for_docker_tests"}
    }')
assert_contains "$result" "ok" "Fanout policy created"

# Enable policy (triggers one_shot with 25 docs already present)
curl -sf -X POST "$BARREL1/_policies/test_fanout/_enable" -H "$AUTH_HEADER" > /dev/null
echo -e "${GREEN}PASS${NC}: Fanout policy enabled"

# Wait for fanout to all targets
echo "  Waiting for fanout to barrel2..."
count2=$(wait_for_doc_count "$BARREL2" "fanout_target" 25 60)
assert_eq "25" "$count2" "Barrel2 has all 25 documents"

echo "  Waiting for fanout to barrel3..."
count3=$(wait_for_doc_count "$BARREL3" "fanout_target" 25 60)
assert_eq "25" "$count3" "Barrel3 has all 25 documents"

echo "  Waiting for fanout to barrel4..."
count4=$(wait_for_doc_count "$BARREL4" "fanout_target" 25 60)
assert_eq "25" "$count4" "Barrel4 has all 25 documents"

# Verify specific documents on each target
for target_num in 2 3 4; do
    port=$((8090 + target_num))
    result=$(curl -sf "http://127.0.0.1:$port/db/fanout_target/fanout_doc15" -H "$AUTH_HEADER")
    assert_contains "$result" "\"value\":15" "fanout_doc15 reached barrel$target_num"
done

# Verify ALL document properties match on each target
echo "  Verifying fanout preserves all properties..."
verify_batch_matches "$BARREL1" "fanout_source" "$BARREL2" "fanout_target" "fanout_doc" 1 25 || exit 1
verify_batch_matches "$BARREL1" "fanout_source" "$BARREL3" "fanout_target" "fanout_doc" 1 25 || exit 1
verify_batch_matches "$BARREL1" "fanout_source" "$BARREL4" "fanout_target" "fanout_doc" 1 25 || exit 1

# Verify changes feed on all targets
for target_num in 2 3 4; do
    port=$((8090 + target_num))
    changes=$(curl -sf "http://127.0.0.1:$port/db/fanout_target/_changes?since=first" -H "$AUTH_HEADER" | jq -r '.results | length')
    if [ "$changes" -ge 25 ]; then
        echo -e "${GREEN}PASS${NC}: Changes feed on barrel$target_num shows 25+ entries"
    else
        echo -e "${RED}FAIL${NC}: Changes feed incomplete on barrel$target_num (got $changes)"
        exit 1
    fi
done

# Cleanup
curl -sf -X POST "$BARREL1/_policies/test_fanout/_disable" -H "$AUTH_HEADER" > /dev/null 2>&1 || true
curl -sf -X DELETE "$BARREL1/_policies/test_fanout" -H "$AUTH_HEADER" > /dev/null 2>&1 || true
delete_db "$BARREL1" "chain_db"
delete_db "$BARREL2" "chain_db"
delete_db "$BARREL3" "chain_db"
delete_db "$BARREL4" "group_db"
delete_db "$BARREL5" "group_db"
delete_db "$BARREL1" "fanout_source"
delete_db "$BARREL2" "fanout_target"
delete_db "$BARREL3" "fanout_target"
delete_db "$BARREL4" "fanout_target"

echo ""
echo -e "${GREEN}--- Replication Policy Tests Complete ---${NC}"
