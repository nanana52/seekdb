# AI Column Phase 4C: Retry Semantics & Lease Heartbeat (WIP)

This patch follows the verified Phase 4B state (`49f0e21`) and changes only
`src/observer/ai_service/ob_ai_service_proxy.{h,cpp}` and the CAS mock test.

## Behavior

1. `max_retries` means *additional attempts*. An initial waiting task can
   always run once when `max_retries=0`; `max_retries=2` permits up to three
   claims. The persisted `attempt_count` is the count of *started attempts*.
2. `renew_ai_column_task_lease_in_trans(...)` extends an active RUNNING task
   only under a single SQL CAS that checks task identity, definition version,
   dependency revision, row incarnation, owner, last token, unexpired deadline,
   and strictly increasing expiration. `affected_rows=0` is an expected loss
   of ownership, and MUST stop the worker from publishing.
3. Expiry recovery resets `next_due_us` for terminal failures and preserves
   the last lease token so late workers cannot reuse it.
4. Callers must supply an already-started transaction and database-derived
   timestamps (microseconds); the functions neither start nor commit it.

## Required runtime integration (NOT IMPLEMENTED)

* DDL: validate and persist AI definition in the same committed schema DDL tx,
  including schema serialization, rename/drop/alter and ID allocation.
* DML: enqueue outbox tasks in the **same storage tx** as base-row changes.
  Never substitute a separate `ObMySQLTransaction` for the base-row tx.
* Worker: poll/claim/renew/recover, execute AI functions with timeout/backoff,
  atomically publish row values and group status with row-version fencing,
  and handle restart, delete/reinsert, PK changes and cancellation.
* Native rebuild and real seekdb SQL E2E must pass before submission.

Do not remove the resolver's `OB_NOT_SUPPORTED` fail-closed gate until the
complete end-to-end pipeline is installed and verified.

## Tests run on the supplied source snapshot

- `test_ai_column_catalog_schema.py` PASS
- `test_ai_column_catalog_writer_mock.py` PASS
- `test_ai_column_worker_sql_cas.py` PASS (33 SQLite/C++ CAS checks)
- `test_ai_column_task_state.cpp` PASS (77 state-core assertions)

Not a native seekdb compile of this new patch. A previous user report shows
`AI_COLUMN_NATIVE_BUILD_EXIT=0` for Phase 4B, not for this 4C change.
