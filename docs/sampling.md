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

## From the library

```go
streamio.ProcessFile(ctx, path, handler,
    streamio.WithSampling(streamio.SamplingSpec{
        Seed: "2026-q4", Key: []string{"user_id"}, Rate: 0.01,
    }),
)
```

`SamplingSpec` selects exactly one of `Rate`, `RangeHi` (with `RangeLo`), or `StratifyBy`. An invalid
spec is reported as an option error before any file is opened.
