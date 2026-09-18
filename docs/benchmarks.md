# Benchmarks

Every claim below is measured, not asserted — each number comes from an actual `go test -bench`
run on the machine this doc was last updated on (Apple M3 Max, `GOMAXPROCS=16`), not an estimate.
Numbers are illustrative of relative shape and tradeoffs, not a performance guarantee: rerun the
commands below before relying on any of this for capacity planning, since absolute throughput is
machine- and Go-version-dependent even when the relative shape holds.

| Benchmark | File | What it answers |
|---|---|---|
| `BenchmarkProcessFile_ParquetFormats*` | `format_bench_test.go` | What raw passthrough buys over the generic JSON-decode route, across schema shapes, row-group sizes, and worker counts. |
| `BenchmarkNDJSONProcess*` | `internal/jsonio/ndjson_bench_test.go` | NDJSON throughput across document sizes and buffer sizes. |
| `BenchmarkProcess_Dispatch` | `streamio_bench_test.go` | `ProcessFile`'s own format-detection/routing overhead, isolated from decode/encode cost. |
| `BenchmarkEncoder_BatchVsStreaming` | `internal/parquetio/encoder_bench_test.go` | Whether the CLI's single-continuous-file Parquet output (`streamingEncoder`) costs anything over the library's default one-file-per-batch encoder. |
| `BenchmarkStreamingEncoder_MemoryBoundedness` | `internal/parquetio/encoder_bench_test.go` | Whether streaming output's per-flush memory actually stays bounded as total output size grows — and what does scale instead. |
| `BenchmarkTransformRecord_RenameAndDrop` | `internal/options/transform_bench_test.go` | Per-record cost of `WithTransforms`' rename/drop hook, across record widths. |
| `BenchmarkRunConvert` | `cmd/convert_bench_test.go` | The CLI's actual end-to-end throughput (flag wiring + real file I/O), for every one of the 5×5 = 25 in→out pairs across every format the CLI supports (NDJSON, Parquet, CSV, TSV, Arrow), at 20,000 rows. |
| `BenchmarkRunConvert_LargeScale` | `cmd/convert_bench_test.go` | The same 25 pairs, at 10,000,000 rows — confirming steady-state throughput holds, and that this scale doesn't blow past parquet-go's row-group cap when `--batch-size` is chosen sensibly. Not run by default; see below. |

## Batch-per-file vs. single continuous file (Parquet output)

```
go test ./internal/parquetio/... -bench=BenchmarkEncoder_BatchVsStreaming -benchmem -run '^$'
```

| batch size | encoder | ns/row | B/op | allocs/op |
|---|---|---|---|---|
| 100 | default (one file per batch) | 967 | 133,458 | 269 |
| 100 | streaming (single file) | 533 | 119,201 | 264 |
| 1,000 | default | 207 | 415,720 | 274 |
| 1,000 | streaming | 166 | 313,440 | 271 |
| 10,000 | default | 160 | 4,639,659 | 317 |
| 10,000 | streaming | 163 | 4,617,913 | 317 |

Streaming output is never slower than the default per-batch-file encoder here, and noticeably
cheaper at small batch sizes (it skips writing a fresh magic-bytes-plus-footer for every batch); the
two converge as batch size grows, since per-batch framing overhead becomes negligible relative to
row-encoding cost either way. There's no throughput reason to prefer the default encoder over
streaming output at any batch size measured.

## Streaming output's memory boundedness

```
go test ./internal/parquetio/... -bench=BenchmarkStreamingEncoder_MemoryBoundedness -run '^$'
```

| batch size | total rows | row groups | max single flush | `Finalize` trailer |
|---|---|---|---|---|
| 100 | 10,000 | 100 | 32,768 B | 84,674 B |
| 100 | 100,000 | 1,000 | 32,768 B | 631,939 B |
| 100 | 1,000,000 | 10,000 | 32,768 B | 6,334,203 B |
| 100,000 | 100,000,000 | 1,000 | 5,931,008 B | 2,141,220 B |

This is the more interesting — and more honest — result than a blanket "memory is bounded" claim
would be. Two separate, both true, stories:

- **`max-flush-bytes` never scales with total row count at a fixed batch size** — flat at 32,768 B
  across the first three rows (a 100× growth in total rows, batch size held at 100). But it is
  **not** a universal constant: the fourth row, at a 1,000× larger batch size (100,000 rows/batch),
  shows a ~5.9 MB max flush instead. That's expected — a single large batch can produce more encoded
  bytes than the writer's small internal buffer holds before that call's own `Flush` forces them
  out — so the precise claim is peak memory per call is bounded by `max(one batch's encoded size,
  the writer's internal buffer)`, not by a fixed number, and never by total row/batch count.
- **`finalize-bytes` scales with row-group count** (one per batch) — the Parquet footer lists
  per-row-group column metadata for every row group in the file, which is inherent to the format,
  not something this encoder could avoid. Note how row 3 (10,000 row groups) has a *larger* footer
  than row 4 (1,000 row groups, 100× more total rows): footer size tracks row-group count, not row
  count.

**A hard ceiling this benchmark deliberately stays under:** parquet-go caps a file at 32,767 row
groups (`math.MaxInt16`) — past that, `EncodeBatch` fails the whole conversion outright with a
wrapped `ErrTooManyRowGroups`, rather than merely producing a larger footer (pinned by
`TestStreamingEncoder_TooManyRowGroupsFailsRatherThanSilentlyGrowing` in
`internal/parquetio/streaming_encoder_test.go`). This is a real, previously undocumented constraint
this benchmark pass surfaced while scaling up to 100,000,000 rows: the library's default
`--batch-size` (512) would produce ~195,000 row groups at that scale — six times over the cap,
failing outright. **The CLI's `--help` now documents this**, and `runConvert` (`cmd/convert.go`)
catches `errors.Is(err, parquetgo.ErrTooManyRowGroups)` specifically to add an actionable
`--batch-size` hint instead of surfacing parquet-go's bare error text — see
`TestRunConvert_TooSmallBatchSizeGetsAnActionableError` in `cmd/convert_test.go`. The practical
takeaway for any caller converting a very large input to Parquet: choose `--batch-size` so total
rows ÷ batch size stays well under 32,767.

## Record-transform overhead

```
go test ./internal/options/... -bench=BenchmarkTransformRecord_RenameAndDrop -benchmem -run '^$'
```

| record width | ns/op | B/op |
|---|---|---|
| 4 fields | 342 | 0 |
| 16 fields | 687 | 0 |
| 64 fields | 1,715 | 0 |

Zero allocations at every width, confirming `transformRecord`'s in-place filter (`rec[:0]`, see
`internal/options/transform.go`) does what it's documented to do. Cost scales roughly linearly with
field count, as expected for a single linear scan per record.

## CLI end-to-end

```
go test ./cmd/... -bench=BenchmarkRunConvert -benchmem -run '^$'
```

Covers all 5×5 = 25 in→out pairs across every format the CLI supports (`cmd/convert_bench_test.go`'s
`benchAllPairCases`), the same matrix `TestConvertMatrix_AllFormatPairs`
(`cmd/convert_matrix_test.go`) verifies for correctness.

**20,000 rows:**

| in → out | ns/row | B/op | allocs/op |
|---|---|---|---|
| json→json | 156.3 | 35,042,328 | 40,106 |
| json→parquet | 337.5 | 35,766,808 | 82,628 |
| json→csv | 323.0 | 35,384,360 | 81,194 |
| json→tsv | 332.8 | 35,386,360 | 81,203 |
| json→arrow | 294.9 | 36,781,248 | 83,422 |
| parquet→json | 281.4 | 3,982,648 | 83,317 |
| parquet→parquet | 221.9 | 3,486,944 | 44,200 |
| parquet→csv | 192.5 | 2,999,856 | 42,745 |
| parquet→tsv | 192.3 | 3,023,544 | 42,738 |
| parquet→arrow | 167.5 | 4,469,608 | 45,005 |
| csv→json | 329.2 | 36,086,368 | 101,165 |
| csv→parquet | 209.7 | 35,388,616 | 62,557 |
| csv→csv | 179.9 | 35,067,776 | 61,202 |
| csv→tsv | 177.9 | 35,067,264 | 61,198 |
| csv→arrow | 177.2 | 36,579,152 | 63,779 |
| tsv→json | 249.2 | 36,086,368 | 101,165 |
| tsv→parquet | 205.3 | 35,390,536 | 62,564 |
| tsv→csv | 178.2 | 35,069,776 | 61,212 |
| tsv→tsv | 175.5 | 35,069,776 | 61,212 |
| tsv→arrow | 160.3 | 36,579,728 | 63,783 |
| arrow→json | 227.9 | 2,381,560 | 62,249 |
| arrow→parquet | 176.1 | 1,902,080 | 23,710 |
| arrow→csv | 165.6 | 1,521,112 | 22,280 |
| arrow→tsv | 164.1 | 1,521,640 | 22,284 |
| arrow→arrow | 125.7 | 2,919,816 | 24,522 |

At 10,000,000 rows (`BenchmarkRunConvert_LargeScale`, not run by default —
`go test ./cmd/... -bench=BenchmarkRunConvert_LargeScale -benchmem -run '^$' -timeout=30m`):

| in → out | ns/row | B/op | allocs/op |
|---|---|---|---|
| json→json | 45.0 | 1,122,880,296 | 20,019,668 |
| json→parquet | 316.5 | 1,321,332,016 | 40,345,223 |
| json→csv | 331.6 | 1,286,628,248 | 40,040,242 |
| json→tsv | 332.5 | 1,286,627,608 | 40,040,236 |
| json→arrow | 279.6 | 2,026,530,088 | 40,622,222 |
| parquet→json | 253.2 | 1,926,175,632 | 41,033,419 |
| parquet→parquet | 187.4 | 1,257,725,800 | 20,876,320 |
| parquet→csv | 203.7 | 1,396,538,120 | 21,052,446 |
| parquet→tsv | 201.8 | 1,396,388,472 | 21,052,324 |
| parquet→arrow | 147.8 | 1,963,416,328 | 21,153,902 |
| csv→json | 241.7 | 1,428,549,464 | 50,020,717 |
| csv→parquet | 193.9 | 895,966,944 | 30,324,501 |
| csv→csv | 183.4 | 872,458,056 | 30,040,261 |
| csv→tsv | 183.7 | 872,458,008 | 30,040,260 |
| csv→arrow | 142.3 | 1,737,589,880 | 30,722,241 |
| tsv→json | 241.4 | 1,428,549,208 | 50,020,713 |
| tsv→parquet | 194.1 | 895,503,304 | 30,324,350 |
| tsv→csv | 185.0 | 872,458,712 | 30,040,260 |
| tsv→tsv | 183.3 | 872,457,736 | 30,040,258 |
| tsv→arrow | 144.3 | 1,737,590,072 | 30,722,244 |
| arrow→json | 234.3 | 1,176,534,880 | 30,290,728 |
| arrow→parquet | 161.2 | 663,929,072 | 10,617,681 |
| arrow→csv | 175.7 | 621,880,032 | 10,310,231 |
| arrow→tsv | 176.6 | 621,878,384 | 10,310,214 |
| arrow→arrow | 133.3 | 1,361,788,664 | 10,892,279 |
