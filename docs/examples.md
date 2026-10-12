# Examples

Worked examples for every command and what the tool does in each case. All of the output below was
produced by running the commands against the generated files described next, so you can reproduce
it; counts depend only on the data.

## The data

`users.ndjson` has 1,000 rows:

```
{"id":0,"name":"","country":"US","age":18,"score":0.0,"active":true}
{"id":1,"name":"user1","country":"GR","age":19,"score":0.37,"active":false}
```

`name` is empty on every 25th row; `country` cycles through US, GR, DE, JP; `age` runs 18 to 67.
The other files in the examples are derived from it by the commands shown.

Errors are printed once, on one line, as `Error: <message>`.

## 1. Converting between formats

```
$ streamio convert --in users.ndjson --out users.parquet --out-format parquet
wrote users.parquet: 1000 rows read, 2 documents dispatched

$ streamio convert --in users.parquet --out users.csv --out-format csv --csv-has-header
wrote users.csv: 1000 rows read, 2 documents dispatched

$ head -3 users.csv
active,age,country,id,name,score
true,18,US,0,,0
false,19,GR,1,user1,0.37

$ streamio convert --in users.csv --csv-has-header --out users.arrow --out-format arrow
wrote users.arrow: 1000 rows read, 3 documents dispatched

$ streamio convert --in users.arrow --out back.ndjson
wrote back.ndjson: 1000 rows read, 1000 documents dispatched
```

What to notice:

- **`documents dispatched` is one per batch for columnar and tabular outputs.** Parquet, CSV, TSV and
  Arrow encode a whole batch (512 rows by default) as one document, so 1,000 rows made 2 or 3
  documents. NDJSON output is one document per row.
- **Column order follows the encoder's schema**, not the input's: Parquet from NDJSON came out
  alphabetical, and CSV kept that order.
- **CSV carries no types.** Going on from the CSV, every value is text:

  ```
  $ head -2 back.ndjson
  {"active":"true","age":"18","country":"US","id":"0","name":"","score":"0"}
  {"active":"false","age":"19","country":"GR","id":"1","name":"user1","score":"0.37"}
  ```

  Parquet and Arrow written from NDJSON keep real types (`"age":18`); anything that passes through
  CSV or TSV loses them.
- **`--csv-has-header`** says the first row names the columns. Without it, a header row is read as
  data (see example 9).
- **A same-format conversion streams the bytes through unchanged** (NDJSON to NDJSON, CSV to CSV,
  Parquet to Parquet), which is why it is fast. Configuring a transform, filter or sampling makes it
  decode and re-encode instead.

## 2. How the input format is detected

The format comes from the file's **contents**; the extension is only a tiebreaker.

```
$ cp users.csv data.json                         # CSV under a .json name
$ streamio convert --in data.json --csv-has-header --out d1.ndjson
wrote d1.ndjson: 1000 rows read, 1000 documents dispatched

$ cp users.parquet noext                         # no extension at all
$ streamio convert --in noext --out d2.ndjson
wrote d2.ndjson: 1000 rows read, 1000 documents dispatched

$ cp users.ndjson mislabeled.csv                 # NDJSON under a .csv name
$ streamio convert --in mislabeled.csv --out d3.ndjson
wrote d3.ndjson: 1000 rows read, 1000 documents dispatched

$ cp users.arrow thing.bin
$ streamio convert --in thing.bin --out d4.ndjson
wrote d4.ndjson: 1000 rows read, 1000 documents dispatched

$ printf 'a\tb\n1\t2\n3\t4\n' > tabs.txt          # tab-separated text
$ streamio convert --in tabs.txt --csv-has-header --out d8.ndjson
$ cat d8.ndjson
{"a":"1","b":"2"}
{"a":"3","b":"4"}
```

| Content | Detected as |
|---|---|
| starts `PAR1` | Parquet (`PARE`, an encrypted footer, is rejected with a clear error) |
| starts `ARROW1` | Arrow IPC file |
| first non-blank byte is `{` | NDJSON |
| text with a consistent comma or tab count per line, outside quotes | CSV or TSV |
| empty file, or text with no delimiter | the extension's guess, else NDJSON when empty |
| anything else | an error |

When nothing matches and the extension doesn't help, you get an error instead of a guess:

```
$ printf 'just some prose\nwith no structure\n' > prose
$ streamio convert --in prose --out d5.ndjson
Error: prose: streamio: cannot determine input format

$ streamio convert --in prose --in-format json --out d6.ndjson     # you decide
wrote d6.ndjson: 2 rows read, 2 documents dispatched
```

An empty file is a valid empty input, and its extension decides:

```
$ : > empty.csv
$ streamio convert --in empty.csv --out d7.ndjson
wrote d7.ndjson: 0 rows read, 0 documents dispatched
```

`--in-format json|parquet|csv|tsv|arrow` overrides detection. A single-column CSV or TSV (no
delimiter to count) needs its extension or `--in-format`, since its content is just lines.

## 3. Compression

### Reading

gzip, zstd, bzip2, xz, LZ4 and Snappy streams are recognised by their leading bytes and
decompressed transparently. Brotli has no magic number, so it is recognised by a `.br` extension.

```
$ for e in gz zst xz lz4 sz br bz2; do
>   streamio convert --in users.ndjson.$e --out rt_$e.ndjson
> done
wrote rt_gz.ndjson:  1000 rows read, 1000 documents dispatched
wrote rt_zst.ndjson: 1000 rows read, 1000 documents dispatched
wrote rt_xz.ndjson:  1000 rows read, 1000 documents dispatched
wrote rt_lz4.ndjson: 1000 rows read, 1000 documents dispatched
wrote rt_sz.ndjson:  1000 rows read, 1000 documents dispatched
wrote rt_br.ndjson:  1000 rows read, 1000 documents dispatched
wrote rt_bz2.ndjson: 1000 rows read, 1000 documents dispatched

$ cmp rt_gz.ndjson users.ndjson && echo identical
identical
```

The contents win over the name, in both directions:

```
$ cp users.ndjson.gz renamed_blob                 # gzip, no extension
$ streamio convert --in renamed_blob --out rb.ndjson
wrote rb.ndjson: 1000 rows read, 1000 documents dispatched

$ cp users.ndjson.zst data.csv.gz                 # zstd, named like gzip, and like CSV
$ streamio convert --in data.csv.gz --out rb2.ndjson
wrote rb2.ndjson: 1000 rows read, 1000 documents dispatched

$ cp users.ndjson plain.ndjson.gz                 # plain text, named like gzip
$ streamio convert --in plain.ndjson.gz --out rb3.ndjson
wrote rb3.ndjson: 1000 rows read, 1000 documents dispatched
```

Brotli is the exception, because nothing in its bytes says "Brotli":

```
$ cp users.ndjson.br users_br_noext
$ streamio convert --in users_br_noext --out rb4.ndjson
Error: users_br_noext: streamio: cannot determine input format

$ streamio convert --in users_br_noext --in-compression brotli --in-format json --out rb5.ndjson
wrote rb5.ndjson: 1000 rows read, 1000 documents dispatched
```

A name alone never overrides plain contents for the codecs that have a magic number. A plain file
named `*.br`, which would have to be Brotli, fails with a Brotli error.

Naming the wrong codec fails clearly:

```
$ streamio convert --in users.ndjson.gz --in-compression zstd --out rb6.ndjson
Error: converting users.ndjson.gz to rb6.ndjson: decompressing users.ndjson.gz: invalid input: magic number mismatch
```

**How it is read:** a compressed input is decompressed once, up front, as a single sequential
stream, into memory (up to 64 MiB) or a temp file that is removed when the run ends. That keeps
random access, so NDJSON chunking and Parquet row groups still decode in parallel afterwards.
`WithMaxDecompressedSize` (library) caps how far an input may expand.

### Writing

The output codec comes from `--out`'s extension, or `--out-compression`:

```
$ for e in gz zst xz lz4 sz br; do streamio convert --in users.ndjson --out users.ndjson.$e; done

$ ls -l users.ndjson*
91772  users.ndjson
10180  users.ndjson.gz
 7542  users.ndjson.zst
 6752  users.ndjson.xz
16710  users.ndjson.lz4
18175  users.ndjson.sz
 5433  users.ndjson.br

$ streamio convert --in users.ndjson --out other.out --out-compression zstd
$ head -c4 other.out | xxd -p
28b52ffd
```

bzip2 can be read but not written:

```
$ streamio convert --in users.ndjson --out users.ndjson.bz2
Error: bzip2 output is not supported (bzip2 input is)
```

| Codec | Detected by | Write |
|---|---|---|
| gzip | `1f 8b` | `.gz` |
| zstd | `28 b5 2f fd` | `.zst` |
| bzip2 | `BZh` + block magic | not supported |
| xz | `fd 37 7a 58 5a 00` | `.xz` |
| LZ4 (frame) | `04 22 4d 18` | `.lz4` |
| Snappy (framed) | `ff 06 00 00 sNaPpY` | `.sz` |
| Brotli | `.br` extension or `--in-compression` | `.br` |

Compression is independent of the format: `--in logs.csv.zst --out logs.parquet`, or
`--out data.json.gz`, both work, and the format is detected from the decompressed contents.

## 4. Profiling with `stats`

One pass over any input: the record count, how often each attribute is non-empty, how many distinct
values it has, and the most frequent values.

```
$ streamio stats --in users.ndjson --top 3
file:        users.ndjson
records:     1000
attributes:  6

ATTRIBUTE  NON-EMPTY  %       DISTINCT
id         1000       100.0%  1000
name       960        96.0%   960
country    1000       100.0%  4
age        1000       100.0%  50
score      1000       100.0%  1000
active     1000       100.0%  2

top values of id:
  0   1  0.1%
  1   1  0.1%
  10  1  0.1%

top values of country:
  DE  250  25.0%
  GR  250  25.0%
  JP  250  25.0%

top values of active:
  false  666  66.6%
  true   334  33.4%
```

What to notice:

- **`name` is 96.0% non-empty**: 40 of the 1,000 names are empty strings, and empty (like missing
  and null) does not count as filled. Whitespace does count.
- **Ties** are listed by value, so output is stable: US, DE, GR and JP all have 250, and the first
  three alphabetically are shown.
- **`top values of id` is all ones**: when every value is distinct, "top" carries no information; the
  `DISTINCT` column already told you.

Restrict it to some attributes, or get JSON:

```
$ streamio stats --in users.ndjson --columns country,name --top 2
file:        users.ndjson
records:     1000
attributes:  2

ATTRIBUTE  NON-EMPTY  %       DISTINCT
name       960        96.0%   960
country    1000       100.0%  4

top values of name:
  user1   1  0.1%
  user10  1  0.1%

top values of country:
  DE  250  25.0%
  GR  250  25.0%

$ streamio stats --in users.ndjson --columns country --top 2 --output json
{
  "columns": [
    {
      "name": "country",
      "top_values": [
        {"value": "DE", "count": 250, "pct": 25},
        {"value": "GR", "count": 250, "pct": 25}
      ],
      "non_empty": 1000,
      "pct": 100,
      "distinct": 4,
      "distinct_exact": true
    }
  ],
  "records": 1000,
  "rows_skipped": 0,
  "columns_truncated": false
}
```

(The JSON is shown compacted for the `top_values` entries.) `--columns` also selects nested entries:
`--columns attributes` covers everything under `attributes.`. A name that matches nothing warns:

```
$ streamio stats --in users.ndjson --columns nope
warning: --columns "nope" matched no attribute
file:        users.ndjson
records:     1000
attributes:  0
```

### Same data, any format

```
$ streamio stats --in users.parquet --columns age --top 2
file:        users.parquet
records:     1000
attributes:  1

ATTRIBUTE  NON-EMPTY  %       DISTINCT
age        1000       100.0%  50

top values of age:
  18  20  2.0%
  19  20  2.0%
```

The same numbers come from the NDJSON, CSV, TSV, Parquet and Arrow copies of a dataset, because a
value is counted by the text the converters would write for it: `1`, `1.0` and `"1"` are one value,
and dates and timestamps render as ISO-8601.

### A CSV without a header

```
$ streamio stats --in users.csv --top 1
file:        users.csv
records:     1001
attributes:  6

ATTRIBUTE  NON-EMPTY  %       DISTINCT
col0       1001       100.0%  3
col1       1001       100.0%  51
col2       1001       100.0%  5
col3       1001       100.0%  1001
col4       961        96.0%   961
col5       1001       100.0%  1001
```

Without `--csv-has-header`, the header row is data (1,001 records) and columns are `col0`, `col1`,
and so on. Add `--csv-has-header` and the real names and 1,000 records appear.

### High-cardinality columns and `--max-distinct`

Counting every distinct value costs memory proportional to how many there are, so each decode
worker tracks at most `--max-distinct` values per attribute exactly (100,000 by default). Past
that, counts become estimates and the attribute is marked:

```
$ streamio stats --in users.ndjson --columns id,country --max-distinct 50 --top 2
ATTRIBUTE  NON-EMPTY  %       DISTINCT
id         1000       100.0%  50+
country    1000       100.0%  4

top values of id (approximate: distinct limit reached):
  950  ~20  2.0%
  951  ~20  2.0%
```

`+` means "at least"; `~` means the count may be off (here it is far off: every id occurs once).
Non-empty counts and percentages stay exact. The approximation suits *skewed* columns, where a few
values dominate and survive the limit; a column of unique ids has nothing meaningful to rank, as the
output shows. `country` is below the limit and stays exact.

### Awkward values are made safe to print

```
$ printf '{"v":"a\\tb\\nc"}\n{"v":"   "}\n{"v":""}\n{"v":null}\n{"v":"plain"}\n' > odd.ndjson
$ streamio stats --in odd.ndjson
file:        odd.ndjson
records:     5
attributes:  1

ATTRIBUTE  NON-EMPTY  %      DISTINCT
v          3          60.0%  3

top values of v:
  "   "      1  20.0%
  "a\tb\nc"  1  20.0%
  plain      1  20.0%
```

The empty string and `null` are not counted; whitespace-only and control characters are, and are
shown quoted so a value can never break a table row.

### Compressed input and several workers

```
$ streamio stats --in users.ndjson.zst --workers 4 --chunk-size 20000 --columns country --top 1
file:        users.ndjson.zst
records:     1000
attributes:  1

ATTRIBUTE  NON-EMPTY  %       DISTINCT
country    1000       100.0%  4

top values of country:
  DE  250  25.0%
```

A file smaller than one chunk (32 MiB by default) is read by a single worker whatever `--workers`
says; lower `--chunk-size` to spread a smaller file across workers.

## 5. Bad rows

By default the first malformed row fails the run. `--max-row-errors N` skips up to N and carries on.

```
$ cat bad.ndjson
{"a":1}
not json
{"a":3}
{"a":
{"a":5}

$ streamio stats --in bad.ndjson
row error 1/1: ndjson bad.ndjson: jsonio: document is not a JSON object: ...
Error: profiling bad.ndjson: streamio: too many row errors: limit 0, got 1 errors (first: ...)

$ streamio stats --in bad.ndjson --max-row-errors 5
skipping row: ndjson bad.ndjson: jsonio: document is not a JSON object: ...
skipping row: ndjson bad.ndjson: field "a": jsontext: unexpected EOF within "/a" after offset 5
file:        bad.ndjson
records:     3
skipped:     2
attributes:  1

$ streamio convert --in bad.ndjson --out bad.out --out-format csv --csv-has-header --max-row-errors 5
skipping row: ...
skipping row: ...
wrote bad.out: 3 rows read, 2 rows skipped, 1 documents dispatched
```

Skipped rows are not in `records`, and are reported in `skipped`. A CSV or TSV *syntax* error (an
unterminated quote) and any Parquet or Arrow decode error always fail the run, since no smaller
unit is recoverable.

## 6. From Go

```go
ctx := context.Background()

// What is this file?
f, _ := os.Open(path)
st, _ := f.Stat()
format, err := streamio.DetectFormat(streamio.Source{Reader: f, Size: st.Size(), Name: path})
// format == streamio.FormatJSON, seeing through any compression.

// Process every document, filtering, rewriting and renaming on the way.
res, err := streamio.ProcessFile(ctx, path, handler,
    streamio.WithRowFilter(func() (streamio.RowFilter, error) { return adults{}, nil }),
    streamio.WithRecordTransform(func() (streamio.RecordTransform, error) { return lowerCountry{}, nil }),
    streamio.WithTransforms(streamio.RenamePath("score", "points"), streamio.DropPath("active")),
)
// res.Stats: RowsRead 1000, RowsFiltered 840, DocumentsDispatched 160.
// A kept document: {"id":42,"name":"user42","country":"de","age":60,"points":15.54}

// Read records with no output, one consumer per worker.
total := &ageSum{}
res, err = streamio.ConsumeFile(ctx, path,
    func() (streamio.RecordConsumer, error) { return total, nil },
    streamio.WithParallelWorkers(4), streamio.WithChunkSize(20_000))
// total saw 1000 rows, ages summing to 42500; res.Stats.RowsRead == 1000.

// Profile.
p, err := streamio.ProfileFile(ctx, path,
    streamio.WithProfileTopN(2), streamio.WithProfileColumns("country"))
// p.Records == 1000; p.Columns[0].TopValues == [{DE 250 0 25} {GR 250 0 25}]

// Pin the codec and format instead of detecting them.
_, err = streamio.ProcessFile(ctx, path+".gz", handler,
    streamio.WithInputCompression(streamio.CompressionGzip),
    streamio.WithInputFormat(streamio.FormatJSON))
```

Where the stages are:

```go
type adults struct{}

func (adults) Keep(rec record.Record) (bool, error) {
    v, ok := rec.Lookup("age")
    return ok && v.Kind == record.KindInt64 && v.I64 >= 60, nil
}

type lowerCountry struct{}

// Output bytes go in the batch arena: a buffer reused between records would be overwritten by the
// next record, and every record in the batch would end up with the last one's value.
func (lowerCountry) Apply(rec record.Record, arena *record.Arena) (record.Record, error) {
    for i := range rec {
        if rec[i].Name == "country" && rec[i].Value.Kind == record.KindBytes {
            out := arena.Copy(rec[i].Value.Str)
            for j, b := range out {
                if b >= 'A' && b <= 'Z' {
                    out[j] = b + 32
                }
            }
            rec[i].Value = record.Bytes(out)
        }
    }
    return rec, nil
}
```

The order is fixed: decode, row filters, record transforms, `WithTransforms` rules, encode. Each
option takes a factory that is called once per decode worker, so a stage may keep scratch without
locking. `stages.md` has the full contract.

## 7. Error messages you may see

| Situation | Message |
|---|---|
| Nothing matches and the extension doesn't help | `<file>: streamio: cannot determine input format` |
| A Parquet file with an encrypted footer | `streamio: encrypted Parquet footers are not supported` |
| `--in-compression` names the wrong codec | `decompressing <file>: invalid input: magic number mismatch` |
| Output asked in bzip2 | `bzip2 output is not supported (bzip2 input is)` |
| Unknown compression name | `unknown compression: want auto, none, gzip, zstd, bzip2, xz, lz4, snappy, or brotli` |
| A compressed input expands past `WithMaxDecompressedSize` | `streamio: decompressed input exceeds the configured size limit` |
| `--csv-delimiter` is not one character | `--csv-delimiter must be exactly one character` |
| `--max-row-errors` is negative | `--max-row-errors must not be negative` |
| `--top` below 1 | `--top must be at least 1: got 0` |
| A malformed row with no `--max-row-errors` | `streamio: too many row errors: limit 0, got 1 errors (first: ...)` |
