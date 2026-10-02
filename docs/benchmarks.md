# Benchmarks

Measured on Apple M3 Max, `GOMAXPROCS=16`. Rerun before using for capacity planning.

| Benchmark | File | What it answers |
|---|---|---|
| `BenchmarkProcessFile_ParquetFormats*` | `format_bench_test.go` | What raw passthrough buys over the generic JSON-decode route, across schema shapes, row-group sizes, and worker counts. |
| `BenchmarkNDJSONProcess*` | `internal/jsonio/ndjson_bench_test.go` | NDJSON throughput across document sizes and buffer sizes. |
| `BenchmarkProcess_Dispatch` | `streamio_bench_test.go` | `ProcessFile`'s own format-detection/routing overhead, isolated from decode/encode cost. |
| `BenchmarkEncoder_BatchVsStreaming` | `internal/parquetio/encoder_bench_test.go` | Whether the CLI's single-continuous-file Parquet output (`streamingEncoder`) costs anything over the library's default one-file-per-batch encoder. |
| `BenchmarkStreamingEncoder_MemoryBoundedness` | `internal/parquetio/encoder_bench_test.go` | Whether streaming output's per-flush memory actually stays bounded as total output size grows. |
| `BenchmarkTransformRecord_RenameAndDrop` | `internal/options/transform_bench_test.go` | Per-record cost of `WithTransforms`' rename/drop hook, across record widths. |
| `BenchmarkRunConvert` | `cmd/streamio/convert_bench_test.go` | CLI end-to-end throughput, all 5×5 = 25 in→out pairs, 20,000 rows. |
| `BenchmarkRunConvert_LargeScale` | `cmd/streamio/convert_bench_test.go` | Same 25 pairs, 10,000,000 rows. Not run by default. |

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

parquet-go caps a file at 32,767 row groups (`math.MaxInt16`); choose `--batch-size` so total rows ÷
batch size stays well under that.

## Record-transform overhead

```
go test ./internal/options/... -bench=BenchmarkTransformRecord_RenameAndDrop -benchmem -run '^$'
```

| record width | ns/op | B/op |
|---|---|---|
| 4 fields | 342 | 0 |
| 16 fields | 687 | 0 |
| 64 fields | 1,715 | 0 |

## CLI end-to-end

```
go test ./cmd/streamio/... -bench=BenchmarkRunConvert -benchmem -run '^$'
```

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

**10,000,000 rows** (`BenchmarkRunConvert_LargeScale`, not run by default —
`go test ./cmd/streamio/... -bench=BenchmarkRunConvert_LargeScale -benchmem -run '^$' -timeout=30m`):

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
