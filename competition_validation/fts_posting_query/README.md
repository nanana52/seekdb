# Exact posting merge query hot path

CPU sampling attributed much of membership query time to fill_merge_heap, indirect token-iterator calls and generic binary-collation ID comparisons. ObTextDaaTIter owns final ObTextRetrievalDaaTTokenIter objects, as used by its existing reset/reuse path. For the existing bounded simple merger, make those calls direct and obtain contiguous dimension arrays once. Larger fan-ins retain the generic implementation. The merge algorithm, dimension collector and minimum-match semantics stay intact. Score multiplication retains original associativity.

For Varchar IDs with CS_TYPE_BINARY, comparison uses the same full-byte memcmp and length order as ob_strnncollsp_binary, preserving embedded zero bytes and trailing spaces. NULL and other metadata types retain the selected original comparator. Numeric/vector IDs are unaffected.

1074 token-output comparisons,96 independent Python-token-set DML oracle checks,28large-fanin exact-score/boolean/offset/group comparisons, and score/threshold/NOT/group/NULL/duplicate-MATCH/rollback comparisons passed. Four full official benchmark phases ran in actual2CPU2GiB512PID containers, alternating40.661parent/candidate/candidate/parent. All metrics, including regressions, are disclosed in the reports. This is local validation, not a formal full-score claim; test data, SQL, timing and scoring are unchanged.
