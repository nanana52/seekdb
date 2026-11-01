# AI Column Phase 4A: native catalog write primitives (NOT the final feature)

**Scope:** Adds two write methods to the *existing* `ObAiServiceProxy` C++
translation unit. The methods target `__all_ai_column_definition` and
`__all_ai_column_task`, whose physical schemas were generated and verified in
commit `9391a62`.

- `insert_ai_column_definition_in_trans(ObMySQLTransaction&, row)` records the
  expression, column dependencies, model, type, version, sync/retry policy and
  active status with a caller-owned **already-started** SQL transaction. The
  intended future caller is the rootserver DDL schema transaction.
- `enqueue_ai_column_task_in_trans(ObMySQLTransaction&, row)` constructs an
  immutable durable task record with request, row-incarnation, dependency and
  definition fences; database defaults initialize status, attempts and lease.
  This is only a primitive for callers *actually sharing that same SQL
  transaction*. **Do not call it from regular user DML via a second internal
  SQL transaction.** Ordinary user DML uses DAS/ObTxDesc, which has a separate
  transaction API; such use would break rollback atomicity.

Both methods reject invalid keys/versions and inactive transactions, escape all
variable-length SQL values, check exactly one inserted row, and do **not**
start/commit/end a transaction. Duplicate-key failure is left as a database
error for the caller to handle.

## What is still missing

The AI COLUMN resolver still returns `OB_NOT_SUPPORTED`. Phase 4A does **not**
pass its parsed expression to the rootserver, does **not** wire DDL to the
writer, does **not** register AI metadata on user columns, and does **not**
connect DAS row mutations, transactional invalidation, lease-based workers,
result publication, strict reads, vector index updates, or crash recovery.
Only a full end-to-end implementation can remove the fail-closed guard.

## Fast isolated checks (no database)

```bash
cd /workspaces/seekdb-core
python3 tools/competition_2026/ai_column/test_ai_column_catalog_writer_mock.py
python3 tools/competition_2026/ai_column/test_ai_column_catalog_schema.py src/share/inner_table/ob_inner_table_schema_def.py
g++ -std=c++14 -Wall -Wextra -Werror -pedantic -I . tools/competition_2026/ai_column/test_ai_column_task_state.cpp -o /tmp/test_ai_column_task_state
/tmp/test_ai_column_task_state
```

The writer mock test compiles the **real method bodies** with tiny C++ stand-ins
and checks the emitted SQL columns, string-escaping calls, transaction guard,
return-value handling and invalid-input behavior. It does **not** replace a
native seekdb build or actual SQL transaction verification.

## Required integration before any competition submission of AI Column

1. Add a durable Schema/RPC representation of AI metadata, with versioned
   catalog INSERT/DELETE inside rootserver's DDL transaction. Validate the
   AST expression, model/type/dependency consistency, and ALTER/DROP semantics.
2. Integrate the outbox *inside the same user business transaction* as changed
   rows via the seekdb DAS/ObTxDesc path; rollback must remove both. Also
   track row incarnation, dependency revision, and mark stale AI values.
3. Start a restart-safe worker that conditionally claims task leases and
   observes attempt limits; stage model results, reject stale leases/row
   versions, and atomically publish entire groups with status/index changes.
4. Verify error handling, strict read, DDL changes, delete/reinsert, UPDATE,
   rollback, concurrent modifications and observer restart using a native
   seekdb build and real SQL; confirm upstream reservation of table IDs 1135/1136.
