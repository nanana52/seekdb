# SeekDB 2026: bounded native CSV/JSONL reader (work in progress)

This C++14 module is an isolated foundation for a future FileScan operator.
It is **not** wired into SQL resolution/optimizer/execution, and therefore
does not replace the existing bounded JSON_TABLE compatibility adapter.

Independent test:
  g++ -std=c++14 -Wall -Wextra -Werror -O2 -I. \
    test_file_stream_reader.cpp -o /tmp/file_stream_test
  mkdir -p /tmp/seekdb-file-stream-unit
  /tmp/file_stream_test /tmp/seekdb-file-stream-unit

Coverage: 80,000-row / 5.3 MB CSV, header duplicates, multiline quotes,
UTF-8, JSONL flat-object validation, rescan, root checks, change detection.
Remaining: physical file scan integration, schema inference, column projection,
real Parquet row groups, and full-file-free end-to-end SQL execution.

This module intentionally uses std::string/std::vector. Before promoting it
into production seekdb execution code, adapt its allocations to seekdb's
allocator/container conventions and verify the same tests with real SQL.
