# Package layout

```
streamio/
├── api.go                public types/options: Format, Result, Stats, DocumentHandler, transforms
├── streamio.go    ProcessFile: compares requested vs. native format, picks a route
├── cmd/                  standalone `streamio convert` CLI, its own `package main`; drives
│                         ProcessFile directly through the public api.go surface
└── internal/
    ├── options/
    │   ├── opts.go        Config (RunConfig/NDJSONConfig/ParquetConfig/CSVConfig sub-structs),
    │   │                  Option, every WithX constructor
    │   ├── factory.go     Config → validated, defaulted runtime settings
    │   ├── format.go      Format/OutputFormat constants
    │   ├── logger.go      Logger interface
    │   └── transform.go   PathTransformRule, RenamePath/DropPath, NewPathTransformer
    ├── formatio/          RawSource, RecordDecoder, RecordEncoder, DecodeStats — what a format supplies
    ├── record/            Record, Field, Value, Kind, Semantic — the canonical cross-format row
    ├── pool/
    │   ├── pool.go        RunRaw + the decode/dispatch worker pool skeleton both routes share
    │   └── records.go     RunRecords: the generic decode→encode path over that same skeleton
    ├── jsonio/
    │   ├── decoder.go        ObjectDecoder: one JSON object → record
    │   ├── encoder.go        JSON RecordEncoder: record → one JSON object
    │   ├── format.go         float/date/timestamp rendering rules
    │   ├── ndjson.go         sharedState/openShared: NDJSON state shared by the raw and record routes
    │   ├── raw.go            NewRawSource (formatio.RawSource impl): NDJSON raw route
    │   ├── decode.go         chunkDecoder: per-worker chunk claiming and line batching
    │   ├── lines.go          chunk-boundary line resolution
    │   ├── batch.go          per-worker line-accumulation scratch
    │   ├── record_decoder.go NDJSON file → records, delegating line JSON to ObjectDecoder
    │   └── convert_test.go   convertFile: test/benchmark-only NewRawSource+pool.RunRaw wrapper
    ├── parquetio/
    │   ├── parquet.go         openShared, sharedState: shared schema walk and row-group queue
    │   ├── schema.go          schema classification/naming
    │   ├── raw.go             NewRawSource + raw row-group extraction
    │   ├── record_decoder.go  Parquet row → record
    │   ├── encoder.go         record batch → one complete Parquet file (formatio.RecordEncoder)
    │   ├── streaming_encoder.go  record batch → one row group of a continuously-growing single
    │   │                        file (formatio.FinalizableRecordEncoder), for bounded-memory
    │   │                        multi-gigabyte single-file output
    │   └── buf.go             cloneBuf: shared reused-buffer-to-owned-slice copy helper
    ├── csvio/                 CSV and TSV in one package, parameterized by delimiter
    │   ├── csvio.go           openShared, sharedState, delimiterFor: shared file/delimiter state
    │   ├── lines.go           chunk-boundary line resolution, mirroring jsonio's
    │   ├── row.go             parseRow/rowWriter: thin encoding/csv wrappers
    │   ├── raw.go             NewRawSource: CSV/TSV raw route, one row per document
    │   ├── record_decoder.go  NewDecoder: CSV/TSV row → record, every field record.KindBytes
    │   ├── encoder.go         record batch → one complete CSV/TSV document (formatio.RecordEncoder)
    │   ├── value.go           record.Value → CSV field text, reusing jsonio's date/timestamp rendering
    │   └── streaming_encoder.go  record batch → rows of a single CSV/TSV document spanning the run
    └── arrowio/               Apache Arrow IPC (file format) support, full decode+encode
        ├── arrow.go           sharedState/openShared: shared schema walk and record-batch queue
        ├── value.go           Arrow type → record.Kind/Semantic classification, scalar reads
        ├── record_decoder.go  Arrow record batch → record, including nested Struct/List
        ├── encoder.go         record batch → one complete Arrow IPC file (formatio.RecordEncoder)
        └── streaming_encoder.go  record batch → one record batch of a continuously-growing single
                                 file, via growingSink (ipc.FileWriter has no Reset/Flush)
```
