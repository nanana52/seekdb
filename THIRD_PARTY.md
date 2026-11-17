# Dependency provenance

Only nlohmann/json 3.12.0 is vendored, under the MIT License in vendor/nlohmann/LICENSE.MIT. Its original single header is from https://github.com/nlohmann/json/tree/v3.12.0/single_include/nlohmann/json.hpp . SHA256: aaf127c04cb31c406e5b04a63f1ae89369fccde6d8fa7cdda1ed4f32dfc5de63 . It handles the JSONL transport, vector literal JSON and compact schema serialization. It does not implement database storage, SQL execution, transactions, recovery or vector search.

Optional validation/benchmark.py uses NumPy only as an independent external ground-truth oracle. The database binary and its Docker runtime do not depend on NumPy. Official public driver and SQL test files are unchanged from oceanbase/seekdb commit f4d4e26653af823a7601668802f80e374ad6643c.
