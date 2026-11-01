# AI Column Phase 4B-1: conditional task lease and staging SQL primitives

This is **not** the finished AI Column feature, a worker loop, or a DML hook.
It adds four native catalog SQL methods to `ObAiServiceProxy`:

- `try_claim_ai_column_task_in_trans`: conditional UPDATE from WAITING / due RETRY_WAIT to RUNNING, increments attempts, installs a unique lease token and deadline. Two simultaneous workers cannot claim the same task through a read/modify/write race.
- `stage_ai_column_success_in_trans`: stores an AI result only while the same worker owner/token still holds an unexpired RUNNING lease, without exposing the result to user SQL.
- `stage_ai_column_failure_in_trans`: handles a model failure through one leased conditional UPDATE, rescheduling if retryable and under the attempt cap, otherwise staging failure without changing the base row.
- `recover_expired_ai_column_task_in_trans`: moves a dead worker's expired RUNNING task to retry wait or terminal staged failure based on attempt budget, preserving the old token.

All 4 methods require a caller-owned started `ObMySQLTransaction`. Each executes one conditional UPDATE and treats a zero-row match as an expected contention/fencing miss (`bool = false`). Validation prevents empty identity/lease tokens, invalid IDs, and invalid deadlines. User strings are emitted only through SQL hex escaping. These are state transitions on the native `__all_ai_column_task` table; they do not create users' row outbox entries or publish results.

## Fast regression tests

```bash
python3 tools/competition_2026/ai_column/test_ai_column_catalog_writer_mock.py
python3 tools/competition_2026/ai_column/test_ai_column_worker_sql_cas.py
python3 tools/competition_2026/ai_column/test_ai_column_catalog_schema.py src/share/inner_table/ob_inner_table_schema_def.py
```

`test_ai_column_worker_sql_cas.py` compiles the *actual C++ method bodies* against minimal mocks, checks transaction guards/return contracts, and executes their emitted UPDATE SQL in isolated SQLite databases for duplicate claims, delayed retry, lease expiry, stale staging, attempt cap and version fences. This is not a seekdb native compilation, SQL integration test, or proof of storage-level transaction atomicity.

## Work still required

1. Serialize AI metadata through schema/RPC and insert definitions **within the same Rootserver DDL transaction**; keep DDL fail-closed until all functionality exists.
2. Add durable Outbox rows, invalidation, row incarnation and dependency revision into the **same business storage transaction** as each INSERT/UPDATE/DELETE; `ObMySQLTransaction` is not a substitute for the user DML tx.
3. Implement a long-running restart-safe scheduler and model execution, failure/retry staging, group publication in one real user-row transaction, live row fence checks, strict-read and index-status semantics.
4. Compile/link native seekdb and verify real SQL under rollback, concurrent updates, crash/restart, NULLs, DDL changes, PK changes and 1135/1136 upstream ID reservation.
