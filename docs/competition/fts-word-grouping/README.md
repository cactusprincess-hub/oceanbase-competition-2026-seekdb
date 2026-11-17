# Full-text word aggregation optimization

The per-document and per-query word maps belong to a single parser worker. They now use the existing no-lock hash map policy. Word counts insert or increment in one hash traversal. The process-wide stopword set is populated before server startup and remains immutable until shutdown; its lookups are const and use the same no-lock policy. Hashing, collation comparison, stopword contents and first-occurrence counting remain unchanged.

Baseline: `b854f62df49bfead09d8a24f90ee55ed80da402e` (already includes parser reuse). Candidate: this commit's source changes. Both binaries were built as macOS arm64 RelWithDebInfo (`-O2`) and alternated on the same isolated database instance using four configured CPUs. The official benchmark scripts and configuration were not modified: 20,000 rows, 500-row batches, 3,000 tokenize rounds, 200 query rounds, 3 samples, 30 warmups. A temporary date wrapper supplied nanosecond timestamps on macOS; Homebrew Bash handled associative arrays.

Four raw reports are included: before, after, after-repeat, before-repeat. Their timestamps identify the actual execution order. Candidate reports show the baseline Git HEAD with dirty=1 because they ran before the changes were committed. No build, sampling or other database benchmark ran concurrently with these four measurements.

Across the mean of two reports per binary, build/tokenize/query category improvements were 1.54%, 4.34% and 4.65%, respectively; their equal-weight mean was 3.51%. This is a small local improvement, with visible noise: one candidate multi-column index build regressed from 5.382s to 6.605s, then measured 4.521s on repetition. One repeated baseline Chinese query sample was also noisy. These measurements do not establish a 50% reduction or an official score. The fixed CI baseline uses different hardware and must not be interpreted as an expected platform score.

All four reports return the same query hit counts: 8001, 11000, 7332 and 20. Local Release target `seekdb` built successfully. Existing `tokenize_function`, `parser_properties` and `cn_word` mysqltests pass on both binaries. Their SQL and expected results are unchanged; temporary wrappers provide the standalone host/port, and an admin account is confined to the isolated test instance. The new `unittest/sql/fts/t/word_grouping.test` is checked against output recorded with the baseline binary. It verifies English/Chinese frequencies, case/accent and binary collations, empty input and multi-document index inserts/updates/deletes. Linux compilation of this change has not been run.

Run the added regression against a standalone instance:

```sh
deps/3rd/u01/obclient/bin/mysqltest --host=127.0.0.1 --port=29882 --user=root --database=test --default-character-set=utf8mb4 --test-file=unittest/sql/fts/t/word_grouping.test --result-file=unittest/sql/fts/r/mysql/word_grouping.result
```
