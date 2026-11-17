Native asynchronous AI columns in seekdb

CREATE and ALTER declarations persist typed AI definitions and hidden state/request/version columns. Native row triggers create the outbox in the business transaction. Background workers claim persisted request groups, issue model calls after commit, stage results, and publish the group with request/version fences. Indexed vector invalidation uses a fresh generated VID when a BEFORE trigger changes the vector through its dependencies. Publication failures release ownership without discarding staged model output.

The unchanged official mysqltest suites passed: ai_column_ddl, ai_column_t1_dml, ai_column_worker, ai_column_vector_index, ai_column_retry_strict. The same binary passed the original AI function enhancement suite. The file SQL suite kept its original SQL body and expected results, with only the SSH fixture installer replaced by local fixture setup.

Validation used a clean, isolated seekdb runtime on Linux x86_64. Development paths in the attached driver are specific to the validation workspace; official suites and their expected results remain at the repository standard paths. Platform evaluation is pending. No full-score claim is made from local tests.
