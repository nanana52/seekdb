# Bounded small-server DDL build concurrency

Native DDL event timestamps showed the independent FTS inverted-index and document-word auxiliary builds queued behind one DdlBuild worker in mini mode. This change permits two workers only when effective system/cgroup memory is at least2GiB, logical memory budget is at least1GiB, and configured/detected CPU count is at least2. Explicit cpu_count1 and actual cgroup1.5GiB retain one worker. The larger-server16-worker path is unchanged.

Actual Docker constraints, process task names and ordinary/unique/fulltext/rollback checks are in actual_container_guards.json. The full unmodified20K official benchmark ran in four alternate before/after/after/before containers, each2CPU2GiB512PID; summaries include all measured metrics and SQL score/filter/group/NULL/DML semantic comparisons. Performance improvements are local comparisons, not formal contest points.

The deprecated memory_limit parameter does not constrain current seekdb memory. Resource claims here come from Docker cgroup limits and inspect evidence. Input data, test SQL, timing and scoring scripts are unchanged. Prior passing contest submissions are preserved.
