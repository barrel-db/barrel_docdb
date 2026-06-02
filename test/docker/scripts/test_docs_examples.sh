#!/bin/bash
# Drive every curl example from the docs against a running barrel_docdb
# container. The goal is to catch documentation drift (endpoints that no
# longer exist, payloads that the server rejects, query syntax that changed).
#
# Usage: BARREL_URL=http://127.0.0.1:18080 API_KEY=ak_test ./test_docs_examples.sh

set -u

BARREL=${BARREL_URL:-http://127.0.0.1:18080}
KEY=${API_KEY:-ak_test_admin_key_for_docker}

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
FAILED_TESTS=()

run() {
    local name="$1"; shift
    local expected_status="$1"; shift
    local out
    out=$("$@" -s -o /tmp/barrel_resp -w "%{http_code}" 2>/dev/null || echo "000")
    if [[ "$out" =~ ^${expected_status} ]]; then
        printf "  ${GREEN}PASS${NC} %s (HTTP %s)\n" "$name" "$out"
        PASS=$((PASS + 1))
    else
        printf "  ${RED}FAIL${NC} %s (HTTP %s, expected %s)\n" "$name" "$out" "$expected_status"
        printf "       body: %s\n" "$(head -c 200 /tmp/barrel_resp 2>/dev/null)"
        FAIL=$((FAIL + 1))
        FAILED_TESTS+=("$name")
    fi
}

H_AUTH=(-H "Authorization: Bearer $KEY")
H_JSON=(-H "Content-Type: application/json")

section() { echo; echo -e "${YELLOW}=== $1 ===${NC}"; }

# Reset state if a previous run left it behind.
curl -s -X DELETE "${H_AUTH[@]}" "$BARREL/db/mydb" >/dev/null 2>&1 || true
curl -s -X DELETE "${H_AUTH[@]}" "$BARREL/db/example" >/dev/null 2>&1 || true

section "README — public endpoints"
run "GET /health"  "2"   curl  "$BARREL/health"
run "GET /metrics" "2"   curl  "$BARREL/metrics"
run "GET /.well-known/barrel" "2" curl  "$BARREL/.well-known/barrel"

section "README — database lifecycle"
run "PUT /db/mydb" "2" curl -X PUT     "${H_AUTH[@]}" "$BARREL/db/mydb"
run "POST /db/mydb (auto id)" "2" \
    curl -X POST "${H_AUTH[@]}" "${H_JSON[@]}" -d '{"type":"user","name":"Alice"}' "$BARREL/db/mydb"
run "PUT /db/mydb/doc1" "2" \
    curl -X PUT "${H_AUTH[@]}" "${H_JSON[@]}" -d '{"type":"user","name":"Alice"}' "$BARREL/db/mydb/doc1"
run "GET /db/mydb/doc1" "2" curl "${H_AUTH[@]}" "$BARREL/db/mydb/doc1"
run "POST /db/mydb/_find" "2" \
    curl -X POST "${H_AUTH[@]}" "${H_JSON[@]}" \
        -d '{"where":[{"path":["type"],"value":"user"}]}' \
        "$BARREL/db/mydb/_find"

section "README — changes feed"
run "GET /db/mydb/_changes?since=first" "2" \
    curl "${H_AUTH[@]}" "$BARREL/db/mydb/_changes?since=first"
# longpoll with 1s timeout — should return promptly with an empty result
run "GET /db/mydb/_changes?feed=longpoll" "2" \
    curl --max-time 5 "${H_AUTH[@]}" "$BARREL/db/mydb/_changes?feed=longpoll&timeout=1000"

section "README — attachments"
# Create a small file to upload
echo "hello attachment" >/tmp/barrel_doc_attachment.txt
run "PUT  /db/mydb/doc1/_attachments/note.txt" "2" \
    curl -X PUT "${H_AUTH[@]}" -H "Content-Type: text/plain" \
        --data-binary @/tmp/barrel_doc_attachment.txt \
        "$BARREL/db/mydb/doc1/_attachments/note.txt"
run "GET  /db/mydb/doc1/_attachments/note.txt" "2" \
    curl "${H_AUTH[@]}" "$BARREL/db/mydb/doc1/_attachments/note.txt"
run "GET  /db/mydb/doc1/_attachments (list)" "2" \
    curl "${H_AUTH[@]}" "$BARREL/db/mydb/doc1/_attachments"
run "DEL  /db/mydb/doc1/_attachments/note.txt" "2" \
    curl -X DELETE "${H_AUTH[@]}" "$BARREL/db/mydb/doc1/_attachments/note.txt"

section "advanced-features.md — replication primitives"
run "PUT /db/example" "2" curl -X PUT "${H_AUTH[@]}" "$BARREL/db/example"
run "POST /db/example/_replicate (one-shot)" "2" \
    curl -X POST "${H_AUTH[@]}" "${H_JSON[@]}" \
        -d '{"source":"mydb","target":"example"}' \
        "$BARREL/db/example/_replicate"
run "POST /db/mydb/_revsdiff" "2" \
    curl -X POST "${H_AUTH[@]}" "${H_JSON[@]}" \
        -d '{"id":"doc1","revs":["1-doesnotexist"]}' \
        "$BARREL/db/mydb/_revsdiff"

section "api/http.md — bulk and local docs"
run "POST /db/mydb/_bulk_docs" "2" \
    curl -X POST "${H_AUTH[@]}" "${H_JSON[@]}" \
        -d '{"docs":[{"_id":"bulk1","v":1},{"_id":"bulk2","v":2}]}' \
        "$BARREL/db/mydb/_bulk_docs"
run "PUT /db/mydb/_local/cp1" "2" \
    curl -X PUT "${H_AUTH[@]}" "${H_JSON[@]}" -d '{"seq":1}' \
        "$BARREL/db/mydb/_local/cp1"
run "GET /db/mydb/_local/cp1" "2" \
    curl "${H_AUTH[@]}" "$BARREL/db/mydb/_local/cp1"
run "DEL /db/mydb/_local/cp1" "2" \
    curl -X DELETE "${H_AUTH[@]}" "$BARREL/db/mydb/_local/cp1"

section "admin endpoints (api/http.md)"
run "GET /admin/usage" "2" curl "${H_AUTH[@]}" "$BARREL/admin/usage"
run "GET /admin/databases/mydb/usage" "2" curl "${H_AUTH[@]}" "$BARREL/admin/databases/mydb/usage"

section "cleanup"
run "DELETE /db/mydb" "2" curl -X DELETE "${H_AUTH[@]}" "$BARREL/db/mydb"
run "DELETE /db/example" "2" curl -X DELETE "${H_AUTH[@]}" "$BARREL/db/example"

echo
echo -e "${YELLOW}=== Summary ===${NC}"
echo -e "PASS: ${GREEN}$PASS${NC}    FAIL: ${RED}$FAIL${NC}"
if [ "$FAIL" -gt 0 ]; then
    echo "Failed tests:"
    for t in "${FAILED_TESTS[@]}"; do echo "  - $t"; done
    exit 1
fi
exit 0
