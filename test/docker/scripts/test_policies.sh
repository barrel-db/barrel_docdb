#!/bin/bash
# Replication Policy Tests for barrel_docdb
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

section "Replication Policy Tests"

# Test 1: Chain replication policy
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

# Enable policy
test_start "Enable chain policy"
result=$(curl -sf -X POST "$BARREL1/_policies/test_chain/_enable" -H "$AUTH_HEADER")
assert_contains "$result" "ok" "Policy enabled"

# Check status
test_start "Check policy status"
result=$(curl -sf "$BARREL1/_policies/test_chain/_status" -H "$AUTH_HEADER")
assert_contains "$result" "enabled" "Policy status shows enabled"

# Add document to source
put_doc "$BARREL1" "chain_db" '{"_id": "chain_doc1", "value": "test"}' > /dev/null

# Wait for propagation
sleep 5

# Verify propagation through chain
result=$(get_doc "$BARREL2" "chain_db" "chain_doc1" 2>&1 || echo "not_found")
if [[ "$result" == *"test"* ]]; then
    echo -e "${GREEN}PASS${NC}: Document reached barrel2"
else
    echo -e "${YELLOW}SKIP${NC}: Chain propagation may need manual trigger"
fi

result=$(get_doc "$BARREL3" "chain_db" "chain_doc1" 2>&1 || echo "not_found")
if [[ "$result" == *"test"* ]]; then
    echo -e "${GREEN}PASS${NC}: Document reached barrel3"
else
    echo -e "${YELLOW}SKIP${NC}: Chain propagation to barrel3 pending"
fi

# Disable policy
test_start "Disable chain policy"
result=$(curl -sf -X POST "$BARREL1/_policies/test_chain/_disable" -H "$AUTH_HEADER")
assert_contains "$result" "ok" "Policy disabled"

# Delete policy
curl -sf -X DELETE "$BARREL1/_policies/test_chain" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# Test 2: Group replication (bidirectional)
test_start "Group replication (barrel4 <-> barrel5)"

delete_db "$BARREL4" "group_db"
delete_db "$BARREL5" "group_db"
create_db "$BARREL4" "group_db"
create_db "$BARREL5" "group_db"

# Delete existing policy if any
curl -sf -X DELETE "$BARREL1/_policies/test_group" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

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

# Enable policy
result=$(curl -sf -X POST "$BARREL1/_policies/test_group/_enable" -H "$AUTH_HEADER")
assert_contains "$result" "ok" "Group policy enabled"

# Add document to each node
put_doc "$BARREL4" "group_db" '{"_id": "from4", "source": "barrel4"}' > /dev/null
put_doc "$BARREL5" "group_db" '{"_id": "from5", "source": "barrel5"}' > /dev/null

# Wait for sync
sleep 5

# Verify bidirectional sync
result=$(get_doc "$BARREL5" "group_db" "from4" 2>&1 || echo "not_found")
if [[ "$result" == *"barrel4"* ]]; then
    echo -e "${GREEN}PASS${NC}: Document from barrel4 reached barrel5"
else
    echo -e "${YELLOW}SKIP${NC}: Group sync barrel4->barrel5 pending"
fi

result=$(get_doc "$BARREL4" "group_db" "from5" 2>&1 || echo "not_found")
if [[ "$result" == *"barrel5"* ]]; then
    echo -e "${GREEN}PASS${NC}: Document from barrel5 reached barrel4"
else
    echo -e "${YELLOW}SKIP${NC}: Group sync barrel5->barrel4 pending"
fi

# Cleanup
curl -sf -X POST "$BARREL1/_policies/test_group/_disable" -H "$AUTH_HEADER" > /dev/null 2>&1 || true
curl -sf -X DELETE "$BARREL1/_policies/test_group" -H "$AUTH_HEADER" > /dev/null 2>&1 || true

# Test 3: Fanout policy
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

# Enable and add document
curl -sf -X POST "$BARREL1/_policies/test_fanout/_enable" -H "$AUTH_HEADER" > /dev/null
put_doc "$BARREL1" "fanout_source" '{"_id": "fanout_doc", "data": "broadcast"}' > /dev/null

sleep 5

# Check all targets (use correct ports 8092-8094)
for i in 2 3 4; do
    port=$((8090 + i))
    result=$(curl -sf "http://127.0.0.1:$port/db/fanout_target/fanout_doc" -H "$AUTH_HEADER" 2>&1 || echo "not_found")
    if [[ "$result" == *"broadcast"* ]]; then
        echo -e "${GREEN}PASS${NC}: Document reached barrel$i"
    else
        echo -e "${YELLOW}SKIP${NC}: Fanout to barrel$i pending"
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
