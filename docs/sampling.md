# Deterministic sampling

`streamio convert` and `streamio stats` can keep a reproducible subset of the input. The choice is a
pure function of a seed and a *key*, so the same data gives the same sample whatever the worker
count, row order or file format, and a sample taken at a lower rate is a subset of one taken at a
higher rate.

```
streamio convert --in users.parquet --out users.sample.parquet --out-format parquet \
    --sample 1% --sample-key user_id --sample-seed 2026-q4
```

## How it works

Every record gets a 64-bit hash of `(seed, key)`, read as a fraction `h` in `[0, 1)`. A record is
kept when `h` falls in the selected interval:

| Flag | Keeps |
|---|---|
| `--sample 1%` (or `0.01`) | `h < 0.01` |
| `--sample-range 0.8:1.0` | `0.8 <= h < 1.0` |
| `--stratify col --stratum-rate A=100%,default=1%` | `h <` that stratum's rate |

Nothing is shared between workers, so there is no coordination and no dependence on order. The key
is what makes a sample *useful*:

- **`--sample-key a,b`** hashes those columns' values (as text, the way the converters render them,
  so `1`, `1.0` and `"1"` are one value). Every record with the same key is kept or dropped
  together. Sample `orders` by `user_id` and `users` by `id` with the same seed and you get the same
  users in both files: a miniature, still-joinable database.
- **No key** hashes the whole record, independent of column order, with null, empty and missing
  columns treated alike. Identical rows are kept or dropped together.

## Splits

Disjoint ranges of one key partition the data and stay stable as it grows:

```
streamio convert --in events.ndjson --out train.ndjson --sample-range 0:0.8 --sample-key user_id --sample-seed s
streamio convert --in events.ndjson --out test.ndjson  --sample-range 0.8:1 --sample-key user_id --sample-seed s
```

## Unequal rates and weights

`--stratify` gives each value of a column its own rate, which is how to keep every error line and
thin out the noise:

```
streamio convert --in logs.ndjson.gz --out logs.sample.ndjson \
    --stratify severity --stratum-rate ERROR=100%,WARN=10%,default=1% \
    --sample-key trace_id --sample-seed s --sample-weight
```

`--sample-weight` adds a `_sample_weight` column holding `1/rate` for the rate each record was kept
at. Summing it over a time window estimates the original count there. It is exact in expectation,
not per bucket.

Keying by `trace_id` keeps whole traces together: every span and log line of a kept trace is kept.
A record with no `trace_id` hashes by its content and is sampled independently.

## Things to know

- **Rates are Bernoulli per key.** A 1% sample of 1,000,000 distinct keys keeps about 10,000, within
  a few hundred, not exactly 10,000.
- **Few keys, few rows.** A key with 50 distinct values can only be sampled in 50 groups; use a
  finer key or accept the coarseness.
- **Set `--sample-seed` explicitly.** The default seed is the empty string, which is deterministic
  but the same for everyone.
- **Sampling runs on the decoded input record**, before any transform. Key and stratum names refer to
  source columns. It makes a same-format conversion decode and re-encode instead of streaming raw
  bytes.
- **The hash is not secret.** Sampling needs speed and uniformity, not secrecy; do not rely on a seed
  to hide which records were chosen.

## Examples

Every example below was run against generated data, and the outputs shown are what the tool
printed. The fixtures:

| File | Contents |
|---|---|
| `users.ndjson` | 2,000 rows: `id`, `name`, `country` (cycling US, GR, DE, JP), `age` |
| `orders.ndjson` | 6,000 rows: `order_id`, `user_id` (three orders per user), `total` |
| `logs.ndjson` | 6,000 lines: `ts`, `level` (80% INFO, 10% WARN, 10% ERROR), `trace_id` (four lines per trace), `msg` |

Counts will differ with other data or another seed, but for the same input, seed and key they are
identical on every run and on every machine.

### 1. A reproducible sample

```
$ streamio convert --in users.ndjson --out a.json --sample 10% --sample-key id --sample-seed s1
wrote a.json: 2000 rows read, 226 documents dispatched
$ streamio convert --in users.ndjson --out b.json --sample 10% --sample-key id --sample-seed s1
wrote b.json: 2000 rows read, 226 documents dispatched
$ cmp a.json b.json && echo identical
identical
```

**What happens:** each row's `id` is hashed with the seed to a fraction in `[0, 1)`, and rows below
0.10 are kept. Nothing depends on row order or worker count, so the two runs agree byte for byte.
`rows read` is every decoded row; `documents dispatched` is how many survived.

**Why 226 and not 200:** a rate is a coin flip per key, so the count is binomial around
`rate × N`. Here the standard deviation is about 13, so 226 is about two deviations high, which is
ordinary. Use `--sample-n` (see below) when you need an exact count.

### 2. A different seed picks different rows

```
$ streamio convert --in users.ndjson --out c.json --sample 10% --sample-key id --sample-seed OTHER
wrote c.json: 2000 rows read, 195 documents dispatched
```

Only 24 of the first run's 226 rows also appear here, about what chance predicts for two independent
10% samples. **Use a new seed for an independent sample, and keep the seed to reproduce one.**

### 3. A lower rate is a subset of a higher one

```
$ streamio convert --in users.ndjson --out r5.json  --sample 5%  --sample-key id --sample-seed s1
$ streamio convert --in users.ndjson --out r25.json --sample 25% --sample-key id --sample-seed s1
# 115 rows at 5%, 530 rows at 25%, and every 5% row is also in the 25% sample
```

**What happens:** the hash fraction does not change with the rate, only the threshold does. You can
grow a sample later (1% to 5%) and keep every row you already looked at.

### 4. An 80/20 train/test split

```
$ streamio convert --in users.ndjson --out train.json --sample-range 0:0.8 --sample-key id --sample-seed split
$ streamio convert --in users.ndjson --out test.json  --sample-range 0.8:1 --sample-key id --sample-seed split
# train: 1619 rows, test: 381 rows, overlap: 0, together: all 2000
```

**What happens:** ranges are half-open, `[lo, hi)`, over the same hash fraction, so ranges that
tile `[0, 1)` partition the data exactly once. The split is by key, so every row of one `id` lands
on one side, and it stays stable as new rows arrive: an id never moves from test to train.

### 5. Samples of related files that still join

```
$ streamio convert --in users.ndjson  --out su.json --sample 5% --sample-key id      --sample-seed j
$ streamio convert --in orders.ndjson --out so.json --sample 5% --sample-key user_id --sample-seed j
# users kept: 117, orders kept: 351 (117 distinct users)
# every kept order belongs to a kept user; every kept user has all 3 of their orders
```

**What happens:** `users.id` and `orders.user_id` hold the same values, and the same seed and rate
apply, so both files pick the same entities. The result is a miniature but referentially intact
dataset. The key columns may have different names; only the *values* and the seed must match.

### 6. A sample with no key hashes the whole row

```
$ streamio convert --in users.ndjson --out w.json --sample 10% --sample-seed r
wrote w.json: 2000 rows read, 217 documents dispatched
```

**What happens:** with no `--sample-key`, every non-empty field's name and text are hashed and
summed, so the row's fate does not depend on column order, and `null`, empty and missing columns
count the same. Reversing every object's key order in the input gives the same 94 of 300 rows.
Identical rows are kept or dropped together: 200 ids repeated five times at 30% kept 50 ids with
exactly five copies each.

### 7. Keep every error, thin out the rest, and weight the result

```
$ streamio convert --in logs.ndjson --out lg.json \
    --stratify level --stratum-rate ERROR=100%,WARN=25%,default=5% \
    --sample-key trace_id --sample-seed L --sample-weight
wrote lg.json: 6000 rows read, 965 documents dispatched
```

| level | in input | kept | weight | `sum(weight)` |
|---|---|---|---|---|
| ERROR | 600 | 600 | 1 | 600 |
| WARN | 600 | 145 | 4 | 580 |
| INFO | 4,800 | 220 | 20 | 4,400 |

A kept line looks like:

```
{"ts":1700000009,"level":"ERROR","trace_id":"t2","msg":"event 9","_sample_weight":1}
```

**What happens:** each row's stratum is its `level`. A listed stratum uses its own rate; every
other value (INFO here) uses `default`. `_sample_weight` is `1/rate` for the rate the row was kept
at, so `sum(weight)` estimates the original count. It is exact in expectation, not per run: 5,580
of 6,000 here, because the key is the trace, so rows are kept in groups of four and the variance is
per trace, not per row.

**Caveat:** with `--stratify` the strata thin independently. Every ERROR line is kept, but the
other three lines of its trace follow *their* levels' rates, so you do not get whole traces for
error traces. Whole-trace retention for error traces is a separate, tail-based feature.

### 8. Keep whole traces

```
$ streamio convert --in logs.ndjson --out tr.json --sample 10% --sample-key trace_id --sample-seed T
wrote tr.json: 6000 rows read, 612 documents dispatched
# 153 traces kept, every one with all 4 of its lines
```

**What happens:** the key is `trace_id`, so every line of a trace shares one hash and is kept or
dropped together. Run the same command on a spans file with the same seed and its `trace_id`
column, and the kept spans belong to exactly these traces.

### 9. Sampling while converting formats

```
$ streamio convert --in users.csv --csv-has-header --out s.parquet --out-format parquet \
    --sample 10% --sample-key id --sample-seed s1
wrote s.parquet: 2000 rows read, 1 documents dispatched
$ streamio convert --in s.parquet --out s.json
wrote s.json: 226 rows read, 226 documents dispatched
```

**What happens:** the same 226 entities as example 1, from a CSV source into Parquet. Keys are
hashed by their text rendering, so the CSV string `"5"` and the JSON number `5` are the same key.
Note the Parquet column came from CSV text, so `id` is a string there: the same entities, a
different type.

### 10. Sampling does not stream raw bytes

```
$ cat spaced.ndjson
{ "id" : 1 ,   "name":"x" }
{ "id" : 2 , "name":"y" }
$ streamio convert --in spaced.ndjson --out raw.json          # no sampling: raw bytes
{ "id" : 1 ,   "name":"x" }
{ "id" : 2 , "name":"y" }
$ streamio convert --in spaced.ndjson --out samp.json --sample 100% --sample-key id
{"id":1,"name":"x"}
{"id":2,"name":"y"}
```

**What happens:** a same-format conversion normally copies bytes through untouched. Sampling needs
decoded records, so the rows are decoded and written again, in canonical form. `--sample 100%`
keeps everything but still takes the decode path.

### 11. A profile of a sample

```
$ streamio stats --in users.ndjson --sample 10% --sample-key id --sample-seed s1 --columns country --top 4
records:     226
attributes:  1

ATTRIBUTE  NON-EMPTY  %       DISTINCT
country    226        100.0%  4

top values of country:
  JP  60  26.5%
  GR  59  26.1%
  DE  55  24.3%
  US  52  23.0%
```

**What happens:** `stats` takes the same flags. It profiles only the sampled rows, so `records` is
226, not 2,000.

### 12. A nested key

```
$ streamio convert --in nested.ndjson --out n.json --sample 20% --sample-key user.id --sample-seed n
wrote n.json: 1000 rows read, 230 documents dispatched
# 23 distinct user.id values kept, 10 rows each
```

**What happens:** `user.id` reaches into the `user` map. The input had 100 distinct ids across 1,000
rows, so a 20% sample keeps about 20 ids, with all of each id's 10 rows.

### 13. Rows that have no key

`sparse.ndjson` has 1,000 rows; the 500 odd-numbered ones have no `id`.

```
$ streamio convert --in sparse.ndjson --out sp.json --sample 50% --sample-key id --sample-seed b
warning: 500 rows had no value for --sample-key (id) and were sampled independently by their content
wrote sp.json: 1000 rows read, 530 documents dispatched
# 530 kept: 283 of the 500 rows without an id
```

**What happens:** a row whose key columns are all empty has nothing to hash, so it is sampled on
its own content instead, as if no key had been given. Independence is safe because a row with no
key has nothing to join on. Across six seeds the unkeyed rows kept were 249, 283, 257, 261, 253 and
258 of 500, which is the 50% rate and not an all-or-nothing swing.

**Older behavior, for contrast:** hashing "no key" gave every such row the same hash, so under one
seed all 500 were kept and under another none were. That is what this replaces.

`--sample-key-missing` chooses what happens to those rows:

| Policy | Result on the same input and `--sample 50%` |
|---|---|
| `row` (default) | 530 kept, 283 of them without an `id`; the rate applies to them like any other row |
| `keep` | 747 kept: every unkeyed row, plus the 247 keyed rows the rate selected |
| `drop` | 247 kept: unkeyed rows are removed |
| `error` | the run fails: `sampling key is missing: every key column is empty (id)` |

With `keep`, `--sample-weight` gives those rows weight 1, since they were not sampled at a rate.
`stats` prints the same count as a `no sample key:` line.

## What happens in the awkward cases

| Case | What the tool does |
|---|---|
| **Every** key column is missing, null or empty on a row | The row is sampled **independently**, by hashing its whole content, and counted in `Stats.RowsMissingKey` (the CLI warns). Change it with `--sample-key-missing` (example 13) |
| **Some** of several key columns are missing | The row is still keyed by the ones that are present |
| The **stratum** column is missing, or its value is not listed | The row uses the `default` rate. A missing `--stratify` column gave 205 of 2,000 rows at `default=10%` |
| `--sample-key` with several columns | The values are hashed together in the order given, so `a,b` and `b,a` give different samples |
| Rate `100%` | Keeps every row, but still decodes and re-encodes |
| Same seed, different **key columns** | Different rows: the key is part of what is hashed |
| `--sample-weight` and the file already has `_sample_weight` | The run fails: `sampling weight column already exists in the record: "_sample_weight"` |
| `--sample-weight` with a plain rate | Every row gets the same weight, `1/rate` |
| Empty `--sample-seed` | Allowed and deterministic, but the same for everyone; set one explicitly |
| `--where` and a sampling flag together | `--where` runs first, so the sample is taken from the rows that pass it (`--where "country = 'US'" --sample 20%` kept a subset of the US rows only) |
| Input is compressed or mislabeled | Handled before sampling, exactly as for `convert`: the sample is of the decoded records |

## Flag errors

Each of these fails before any file is read or written:

```
$ streamio convert ... --sample 150%
Error: --sample: want a fraction in (0, 1] such as 0.01, or a percentage such as 1%: got "150%"

$ streamio convert ... --sample 0
Error: --sample: want a fraction in (0, 1] such as 0.01, or a percentage such as 1%: got "0"

$ streamio convert ... --sample abc
Error: --sample: want a fraction in (0, 1] such as 0.01, or a percentage such as 1%: got "abc"

$ streamio convert ... --sample-range 0.9:0.1
Error: ... streamio: invalid sampling: sampling range must satisfy 0 <= lo < hi <= 1: got 0.9:0.1

$ streamio convert ... --sample-range 0.5
Error: --sample-range wants lo:hi, each a fraction or percentage: got "0.5"

$ streamio convert ... --sample 10% --sample-range 0:0.5
Error: --sample, --sample-range and --stratify select one sampling mode

$ streamio convert ... --sample-key id            # a key without a mode
Error: --sample-key, --sample-seed and --sample-weight need --sample, --sample-range or --stratify

$ streamio convert ... --stratum-rate A=1         # rates without --stratify
Error: --stratum-rate needs --stratify

$ streamio convert ... --stratify level           # strata without any rate
Error: ... streamio: invalid sampling: strata sampling needs stratum rates or a default rate

$ streamio convert ... --stratify level --stratum-rate ERROR
Error: --stratum-rate wants VALUE=RATE pairs such as ERROR=100%,default=1%: got "ERROR"

$ streamio convert ... --sample-key id --sample 10% --sample-key-missing sometimes
Error: ... streamio: invalid sampling: unknown missing-key policy: want row, keep, drop, or error: got "sometimes"

$ streamio convert ... --stratify level --stratum-rate ERROR=2
Error: ... streamio: invalid sampling: sampling rate must be in (0, 1]: stratum "ERROR" has 2
```

A bare number is a fraction (`0.1` is 10%, `1` is 100%); a `%` suffix is a percentage.

## From the library

```go
// 1. A reproducible 10% sample keyed by id.
streamio.ProcessFile(ctx, path, handler, streamio.WithSampling(streamio.SamplingSpec{
    Seed: "2026-q4", Key: []string{"id"}, Rate: 0.10,
}))

// 2. The test side of an 80/20 split (the train side is RangeLo: 0, RangeHi: 0.8).
streamio.ProcessFile(ctx, path, handler, streamio.WithSampling(streamio.SamplingSpec{
    Seed: "split", Key: []string{"id"}, RangeLo: 0.8, RangeHi: 1.0,
}))

// 3. Per-stratum rates with a weight column.
res, err := streamio.ProcessFile(ctx, path, handler, streamio.WithSampling(streamio.SamplingSpec{
    Seed: "s", Key: []string{"id"}, StratifyBy: "country",
    StratumRates: map[string]float64{"US": 1, "GR": 0.5}, DefaultRate: 0.1,
    WeightColumn: "w",
}))
// res.Stats.RowsRead is every decoded row; res.Stats.RowsFiltered is how many were dropped.

// 4. Drop records that have no key instead of sampling them independently.
streamio.ProcessFile(ctx, path, handler, streamio.WithSampling(streamio.SamplingSpec{
    Seed: "s", Key: []string{"id"}, Rate: 0.1, OnMissingKey: streamio.MissingKeyDrop,
}))
// res.Stats.RowsMissingKey says how many there were.

// 5. Profile a sample.
p, err := streamio.ProfileFile(ctx, path, streamio.WithSampling(streamio.SamplingSpec{
    Seed: "s", Key: []string{"id"}, Rate: 0.1,
}))
```

On the 2,000-row `users.ndjson`, those give 218, 381 and 833 documents (with 1,167 filtered), and
a profile of 169 records. An invalid spec is reported as an error before any file is opened:

```
streamio: invalid sampling: sampling rate must be in (0, 1]: got 2
```

`SamplingSpec` selects exactly one of `Rate`, `RangeHi` (with `RangeLo`), or `StratifyBy`. A `Rate`
of 0 cannot be told from "not set", so it is reported as a missing mode; the CLI checks the range
itself and says so more precisely.
