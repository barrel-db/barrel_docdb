# Docker Testing Checkpoint - 2026-01-10 (Session 2)

## Current Status

| Test Suite | Status | Notes |
|------------|--------|-------|
| HTTP API tests | PASSED (17/17) | All document CRUD, queries, changes |
| Replication tests | PASSED | One-shot, incremental, filtered, bidirectional |
| Federation tests | PASSED | Local queries work, remote needs auth support |
| Policies tests | READY | Auth support added, Docker rebuilt |
| Tiered tests | PENDING | Blocked until policies pass |

## Key Fixes This Session

### 1. Auth Support for Inter-Node Replication (barrel_rep_policy.erl)
**Problem**: Policies crashed with 401 when trying to replicate between nodes because no auth token was passed.

**Fix Applied**:
- Added `auth` field to policy type (line 88): `auth => map()`
- Modified pattern functions to pass auth to replication tasks
- Added `apply_auth_to_url/2` to convert URLs to endpoint maps with auth
- Added `is_remote_url/1` and `apply_auth_config/2` helpers
- Updated `decode_policy/1` to parse auth field from JSON

### 2. Error Handling in Checkpoint (barrel_rep_checkpoint.erl)
**Problem**: `read_last_seq/3` crashed on `{error, {http_error, 401, ...}}` - case_clause error.

**Fix Applied**:
- Added handlers for `{error, {http_error, Status, Msg}}` and `{error, {connection_error, Reason}}`
- Returns `first` on errors (safe fallback) and logs warning
- Prevents gen_server crash loop

### 3. Test Scripts Updated (test_policies.sh)
- Added `"auth": {"bearer_token": "test_admin_key_for_docker_tests"}` to all policy configs

## Docker Cluster Info

- **Ports**: 8091-8095 (barrel1-barrel5)
- **API Key**: `test_admin_key_for_docker_tests`
- **Auth Header**: `Authorization: Bearer test_admin_key_for_docker_tests`
- **Images**: REBUILT with fixes (2026-01-10)

## How to Continue

```bash
# 1. Navigate to test directory
cd /Users/benoitc/Projects/barrel_docdb/test/docker

# 2. Start the cluster
docker compose up -d

# 3. Wait for all nodes to be healthy
for i in 1 2 3 4 5; do
  port=$((8090 + i))
  while ! curl -sf "http://127.0.0.1:$port/health" > /dev/null 2>&1; do sleep 1; done
  echo "barrel$i ready"
done

# 4. Run policies tests
./scripts/test_policies.sh

# 5. Run tiered tests (may need auth headers added)
./scripts/test_tiered.sh
```

## Files Modified This Session

1. **src/barrel_rep_policy.erl**
   - Lines 68-89: Added auth field to type
   - Lines 346-372: apply_chain_pattern passes auth
   - Lines 374-397: apply_group_pattern passes auth
   - Lines 399-420: apply_fanout_pattern passes auth
   - Lines 533-609: start_replication_task uses auth, plus helper functions
   - Line 747-748: decode_policy handles auth field

2. **src/barrel_rep_checkpoint.erl**
   - Lines 189-228: read_last_seq handles HTTP/connection errors

3. **test/docker/scripts/test_policies.sh**
   - Lines 24-39: Chain policy with auth
   - Lines 102-115: Group policy with auth
   - Lines 163-178: Fanout policy with auth
