# AI Column phase 1 - task state and fencing core

This is an additive, dependency-free C++14 foundation for the seekdb 2026 AI
Column exercise. **It is not a completed AI COLUMN SQL feature.** No DDL,
Schema storage, SQL hooks, DML changes, durable outbox, background worker,
strict-read planner gate, or HNSW updates are installed by this patch.

## What this code checks

- Version fencing across tenant/table/row-incarnation/dependency-change/DDL
  generation/request/group; rejects late responses after changed/deleted rows.
- Single-lease claims, attempt budgets, time-limited reclaims after worker crash.
- Exponential/other retry delay is a caller policy; deadlines saturate on overflow.
- Staged outcomes remain invisible until every expected column in the group
  reaches a terminal state; ready and permanently failed members may publish
  together after a *database* transaction verifies the fence.
- `strict_read_rejects()` encodes column-level readiness; the real SQL planner
  must apply it independently of predicates on the returned rows.

## Safe local test (no database needed)

```bash
cd /workspaces/seekdb-core
g++ -std=c++14 -Wall -Wextra -Werror -pedantic -I . \
  tools/competition_2026/ai_column/test_ai_column_task_state.cpp \
  -o /tmp/test_ai_column_task_state
/tmp/test_ai_column_task_state
```

## Non-negotiable integration requirements

1. In `src/sql/resolver/ddl/ob_ddl_resolver.cpp`, add dedicated AI COLUMN
   definition validation rather than treating AI expressions as ordinary
   synchronous generated expressions. Persistent metadata must round-trip
   through `src/share/schema/ob_column_schema.{h,cpp}` and `__all_column`.
2. In `src/sql/engine/dml/ob_dml_service.cpp`, write AI result invalidation,
   dependency revision and outbox rows in the *same* business transaction.
   On ROLLBACK these changes must all disappear. Never issue HTTP in DML.
3. Workers claim committed outbox rows via a conditional, transactional update;
   the lease token and attempt counters must be persisted and reaped after crash.
4. Never publish an HTTP completion merely because its primary key matches.
   Verify the entire version fence and the active lease in a conditional UPDATE.
   Delete/reinsert of a primary key must change `row_generation`.
5. All terminal outputs of one change group must be published in one SQL
   transaction, including index invalidation/rebuild and per-column status.
   The state helper does not provide that transaction; the integration must.
6. Manual retry of READY or FAILED rows must create a new request/group ID.
   Original tasks must not be mutated to make old HTTP responses valid again.
7. SQL strict read must inspect all live rows for referenced AI columns, even
   if the query's WHERE clause filters away unready rows.
8. Do not enable the feature until native seekdb build, SQL test and crash /
   rollback recovery checks pass.

The uploaded `src/sql/parser/sql_parser_mysql_mode.y` currently exposes the
ordinary `T_GENERATED_COLUMN` grammar and ordinary generated column storage.
The uploaded `src/share/schema/ob_column_schema.h` exposes stored generated
column flags, which are **not** an async task state or persisted job definition.
The commit callback in `src/storage/tx/ob_trans_end_trans_callback.*` cannot
replace an atomic, persisted outbox.

No existing file is overwritten. This patch only adds:
- `src/share/ai_service/ob_ai_column_task_state.h`
- this README and its local C++ unit test.
