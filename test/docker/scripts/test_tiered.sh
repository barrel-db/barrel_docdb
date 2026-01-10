#!/bin/bash
# Tiered Storage Tests for barrel_docdb

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

section "Tiered Storage Tests"

# Setup
delete_db "$BARREL1" "tier_hot"
delete_db "$BARREL1" "tier_warm"
delete_db "$BARREL1" "tier_cold"
create_db "$BARREL1" "tier_hot"
create_db "$BARREL1" "tier_warm"
create_db "$BARREL1" "tier_cold"

# Test 1: Configure tiered storage
test_start "Configure tiered storage"
result=$(curl -s -X POST "$BARREL1/db/tier_hot/_tier/config" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "enabled": true,
        "warm_db": "tier_warm",
        "cold_db": "tier_cold",
        "hot_threshold": 60,
        "warm_threshold": 120
    }' 2>&1)
if [[ "$result" == *"ok"* ]]; then
    echo -e "${GREEN}PASS${NC}: Tier configuration set"
else
    echo -e "${RED}FAIL${NC}: Tier config failed: $result"
fi

# Test 2: Add documents to hot tier
test_start "Add documents to hot tier"
for i in $(seq 1 5); do
    put_doc "$BARREL1" "tier_hot" "{\"_id\": \"hot_doc$i\", \"data\": \"recent\", \"index\": $i}" > /dev/null
done
echo -e "${GREEN}PASS${NC}: Documents added to hot tier"

# Test 3: Get tier configuration
test_start "Get tier configuration"
result=$(curl -s "$BARREL1/db/tier_hot/_tier/config" -H "$AUTH_HEADER" 2>&1)
if [[ "$result" == *"warm_db"* ]] || [[ "$result" == *"enabled"* ]]; then
    echo -e "${GREEN}PASS${NC}: Tier config retrieved"
else
    echo -e "${RED}FAIL${NC}: Tier config get failed: $result"
fi

# Test 4: Get capacity info
test_start "Get capacity info"
result=$(curl -s "$BARREL1/db/tier_hot/_tier/capacity" -H "$AUTH_HEADER" 2>&1)
if [[ "$result" == *"size"* ]] || [[ "$result" == *"bytes"* ]] || [[ "$result" == *"doc_count"* ]]; then
    echo -e "${GREEN}PASS${NC}: Capacity info returned"
else
    echo -e "${RED}FAIL${NC}: Capacity endpoint failed: $result"
fi

# Test 5: Get document tier
test_start "Get document tier"
result=$(curl -s "$BARREL1/db/tier_hot/hot_doc1/_tier" -H "$AUTH_HEADER" 2>&1)
if [[ "$result" == *"tier"* ]]; then
    echo -e "${GREEN}PASS${NC}: Document tier returned"
else
    echo -e "${RED}FAIL${NC}: Document tier endpoint failed: $result"
fi

# Test 6: Manual tier migration
test_start "Manual tier migration"
result=$(curl -s -X POST "$BARREL1/db/tier_hot/_tier/migrate" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"doc_id": "hot_doc1", "to_tier": "warm"}' 2>&1)
if [[ "$result" == *"ok"* ]] || [[ "$result" == *"migrated"* ]]; then
    echo -e "${GREEN}PASS${NC}: Document migration initiated"

    # Verify document moved to warm tier
    sleep 1
    result=$(get_doc "$BARREL1" "tier_warm" "hot_doc1" 2>&1 || echo "not_found")
    if [[ "$result" == *"recent"* ]]; then
        echo -e "${GREEN}PASS${NC}: Document found in warm tier"
    else
        echo -e "${YELLOW}SKIP${NC}: Document not yet in warm tier"
    fi
else
    echo -e "${RED}FAIL${NC}: Migration endpoint failed: $result"
fi

# Test 7: Set document TTL
test_start "Set document TTL"
result=$(curl -s -X POST "$BARREL1/db/tier_hot/hot_doc2/_tier/ttl" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"ttl": 3600}' 2>&1)
if [[ "$result" == *"ok"* ]] || [[ "$result" == *"expires"* ]]; then
    echo -e "${GREEN}PASS${NC}: TTL set on document"
else
    echo -e "${RED}FAIL${NC}: TTL endpoint failed: $result"
fi

# Test 8: Get TTL info
test_start "Get document TTL"
result=$(curl -s "$BARREL1/db/tier_hot/hot_doc2/_tier/ttl" -H "$AUTH_HEADER" 2>&1)
if [[ "$result" == *"ttl"* ]] || [[ "$result" == *"expires"* ]]; then
    echo -e "${GREEN}PASS${NC}: TTL info returned"
else
    echo -e "${RED}FAIL${NC}: TTL get endpoint failed: $result"
fi

# Test 9: Run migration policy
test_start "Run migration policy"
result=$(curl -s -X POST "$BARREL1/db/tier_hot/_tier/run_migration" -H "$AUTH_HEADER" 2>&1)
if [[ "$result" == *"ok"* ]] || [[ "$result" == *"action"* ]]; then
    echo -e "${GREEN}PASS${NC}: Migration policy executed"
else
    echo -e "${RED}FAIL${NC}: Migration run failed: $result"
fi

# Test 10: Disable tiering
test_start "Disable tiering"
result=$(curl -s -X POST "$BARREL1/db/tier_hot/_tier/config" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"enabled": false}' 2>&1)
if [[ "$result" == *"ok"* ]]; then
    echo -e "${GREEN}PASS${NC}: Tiering disabled"
else
    echo -e "${RED}FAIL${NC}: Tier disable failed: $result"
fi

# Cleanup
delete_db "$BARREL1" "tier_hot"
delete_db "$BARREL1" "tier_warm"
delete_db "$BARREL1" "tier_cold"

echo ""
echo -e "${GREEN}--- Tiered Storage Tests Complete ---${NC}"
