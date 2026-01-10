#!/bin/bash
# HTTP API Tests for barrel_docdb
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

section "HTTP API Tests"

# Test 1: Health endpoint (no auth needed)
test_start "Health endpoint"
health=$(curl -sf "$BARREL1/health")
assert_contains "$health" "ok" "Health check returns ok"

# Test 2: Metrics endpoint (no auth needed)
test_start "Metrics endpoint"
metrics=$(curl -sf "$BARREL1/metrics")
assert_contains "$metrics" "barrel" "Metrics endpoint returns data"

# Test 3: Create database
test_start "Create database"
delete_db "$BARREL1" "test_api"
result=$(create_db "$BARREL1" "test_api")
assert_contains "$result" "ok" "Database created"

# Test 4: Database info
test_start "Database info"
result=$(curl -sf "$BARREL1/db/test_api" -H "$AUTH_HEADER")
assert_contains "$result" "test_api" "Database info returned"

# Test 5: Put document with ID
test_start "Put document with ID"
doc='{"_id": "doc1", "type": "user", "name": "Alice", "age": 30}'
result=$(put_doc "$BARREL1" "test_api" "$doc")
assert_contains "$result" "doc1" "Document created with ID"

# Test 6: Put document without ID (auto-generated)
test_start "Put document without ID"
doc='{"type": "user", "name": "Bob", "age": 25}'
result=$(put_doc "$BARREL1" "test_api" "$doc")
assert_contains "$result" "id" "Document created with auto-generated ID"

# Test 7: Get document
test_start "Get document"
result=$(get_doc "$BARREL1" "test_api" "doc1")
assert_contains "$result" "Alice" "Document retrieved"
assert_contains "$result" "_rev" "Document has revision"

# Test 8: Update document
test_start "Update document"
rev=$(echo "$result" | jq -r '._rev')
doc="{\"_id\": \"doc1\", \"_rev\": \"$rev\", \"type\": \"user\", \"name\": \"Alice Updated\", \"age\": 31}"
result=$(put_doc "$BARREL1" "test_api" "$doc")
assert_contains "$result" "doc1" "Document updated"

# Test 9: Query documents (equality)
test_start "Query documents (equality)"
query='{"where": [{"path": ["type"], "op": "==", "value": "user"}]}'
result=$(find_docs "$BARREL1" "test_api" "$query")
assert_contains "$result" "Alice" "Query returns Alice"
assert_contains "$result" "Bob" "Query returns Bob"

# Test 10: Query with limit
test_start "Query with limit"
query='{"where": [{"path": ["type"], "op": "==", "value": "user"}], "limit": 1}'
result=$(find_docs "$BARREL1" "test_api" "$query")
count=$(echo "$result" | jq '.results | length')
assert_eq "1" "$count" "Query returns 1 document with limit"

# Test 11: Changes feed
test_start "Changes feed"
result=$(get_changes "$BARREL1" "test_api")
assert_contains "$result" "doc1" "Changes include document"
assert_contains "$result" "hlc" "Changes include HLC"

# Test 12: Changes with limit
test_start "Changes with limit"
result=$(curl -sf "$BARREL1/db/test_api/_changes?limit=1" -H "$AUTH_HEADER")
count=$(echo "$result" | jq '.results | length')
assert_eq "1" "$count" "Changes returns 1 with limit"

# Test 13: Delete document
test_start "Delete document"
current=$(get_doc "$BARREL1" "test_api" "doc1")
rev=$(echo "$current" | jq -r '._rev')
result=$(curl -sf -X DELETE "$BARREL1/db/test_api/doc1?rev=$rev" -H "$AUTH_HEADER")
assert_contains "$result" "ok" "Document deleted"

# Test 14: Get deleted document (should fail)
test_start "Get deleted document"
result=$(curl -sf "$BARREL1/db/test_api/doc1" -H "$AUTH_HEADER" 2>&1 || echo "not_found")
assert_contains "$result" "not_found" "Deleted document returns not_found"

# Test 15: Node info (discovery - no auth needed)
test_start "Node info endpoint"
result=$(curl -sf "$BARREL1/.well-known/barrel")
assert_contains "$result" "node_id" "Node info returns node_id"
assert_contains "$result" "version" "Node info returns version"

# Cleanup
delete_db "$BARREL1" "test_api"

echo ""
echo -e "${GREEN}--- HTTP API Tests Complete ---${NC}"
