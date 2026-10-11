# Filtering rows with `--where`

`streamio convert` and `streamio stats` keep only the records that satisfy a predicate:

```
streamio convert --in events.ndjson.gz --out errors.parquet --out-format parquet \
    --where "level in ('ERROR', 'FATAL') and not trace_id is empty"
```

## Grammar

```
expr     = or
or       = and { "or" and }
and      = not { "and" not }
not      = "not" not | primary
primary  = "(" expr ")" | path op literal | path ["not"] "in" "(" literal {"," literal} ")"
         | path "is" ["not"] "empty" | path ("contains" | "starts_with") 'string'
op       = "=" | "!=" | "<" | "<=" | ">" | ">="
path     = name { "." name }          a name is an identifier or a `quoted` segment
literal  = number | 'string' | "string" | true | false
```

Keywords are case-insensitive; column names are case-sensitive. A dotted path reaches into nested
map entries (`attrs.env`). A column whose own name contains a dot is written in backticks:
``attrs.`a.b` ``.

## How a comparison is decided

The **literal's type** decides how a value is compared:

| Literal | Compares | Notes |
|---|---|---|
| `'text'` | the value's text, byte-wise | `age = '30'` matches the number 30; `<`, `>` are lexicographic |
| `30`, `4.5` | numerically | a text value is parsed (so a CSV column works); integers compare exactly |
| `true`, `false` | booleans | a text value `true`/`false`, in any case, also works |

The text of a value is the converters' own rendering, so a date column compares as
`'2024-01-01'`, and a number compares the same whichever format it came from.

### Empty and missing values

A **missing, null or empty value matches no comparison**, including `!=`. Test for it with
`is empty` / `is not empty`:

```
email is not empty and email contains '@'
```

So `not (x = 1)` is true for a record with no `x`. This is a deliberate, simple two-valued logic: a
predicate answers "does this record match?", and a record that doesn't have the value doesn't match.

### Wrong types

A value that cannot be compared with its literal (the string `unknown` against `age > 18`, or a
list against a number) does **not match** and is counted in `Stats.PredicateMisses`; the CLI prints
the count as a warning. With `--where-strict` (`WithWhereStrict`) the run fails instead, naming the
column.

## Examples

Every example was run, and the numbers are what the tool reported. `people.ndjson` has 1,000 rows:
`id`, `name` (empty on every 10th row), `country` (US, GR, DE, JP in turn), `age` (10 to 79),
`active` (every third row), `email` (null on every 7th row) and `score`, a *string* of a number,
`"n/a"` on every 50th row. `logs.ndjson` has 2,000 lines with `level`, `msg`, `latency_ms` and
`trace_id`.

### Comparing numbers

```
$ streamio convert --in people.ndjson --out out.json --where "age >= 18"
wrote out.json: 1000 rows read, 880 documents dispatched

$ streamio convert --in people.ndjson --out out.json --where "id = 999"
wrote out.json: 1000 rows read, 1 documents dispatched
```

`rows read` is every row decoded; `documents dispatched` is what matched. In the library the
difference is `Stats.RowsFiltered`.

### Several conditions

```
$ --where "age >= 18 and country in ('US','GR')"
wrote out.json: 1000 rows read, 440 documents dispatched

$ --where "country = 'JP' or age < 15"
wrote out.json: 1000 rows read, 310 documents dispatched

$ --where "not (country = 'US')"
wrote out.json: 1000 rows read, 750 documents dispatched

$ --where "country not in ('US','GR')"
wrote out.json: 1000 rows read, 500 documents dispatched
```

`and` binds tighter than `or`; use parentheses when you want the other grouping.

### Text

```
$ --where "name starts_with 'user9'"
wrote out.json: 1000 rows read, 100 documents dispatched

$ --where "email is not empty and email contains '@x.org'"
wrote out.json: 1000 rows read, 857 documents dispatched

$ streamio convert --in logs.ndjson --out out.json --where "msg contains 'timeout'"
wrote out.json: 2000 rows read, 118 documents dispatched

$ streamio convert --in logs.ndjson --out out.json --where "level in ('ERROR','WARN') and latency_ms > 500"
wrote out.json: 2000 rows read, 171 documents dispatched
```

`contains` and `starts_with` are case-sensitive and take a quoted string.

### Empty, null and missing

```
$ --where "name is empty"
wrote out.json: 1000 rows read, 100 documents dispatched

$ --where "missing_col = 1"
wrote out.json: 1000 rows read, 0 documents dispatched

$ --where "not missing_col = 1"
wrote out.json: 1000 rows read, 1000 documents dispatched
```

`is empty` is true for a missing column, a `null`, an empty string, and an empty list or map. A
comparison with a missing value is simply false, so its negation is true: `not missing_col = 1`
matches every row. If you want "has a value and it isn't 1", say so: `missing_col is not empty and
missing_col != 1`.

### A column that is text but holds numbers

`score` is a string column, as it would be from a CSV:

```
$ --where "score > 50"
warning: 20 comparisons in --where "score > 50" found a value of the wrong type and did not match (use --where-strict to fail instead)
wrote out.json: 1000 rows read, 490 documents dispatched
```

The number literal makes the comparison numeric, so `"51"` to `"99"` are compared as numbers: 490 rows.
The 20 rows holding `"n/a"` cannot be read as a number, so they do not match, and the tool says so
instead of dropping them silently. To fail on them:

```
$ --where "score > 50" --where-strict
Error: converting people.ndjson to out.json: value cannot be compared: column "score" against 50
```

Quote the literal to compare text instead: `score > '5'` is a lexicographic comparison and would
match `"9"`.

### `stats` takes the same predicate

```
$ streamio stats --in people.ndjson --where "age >= 70" --columns country --top 4
file:        people.ndjson
records:     140
attributes:  1

ATTRIBUTE  NON-EMPTY  %       DISTINCT
country    140        100.0%  4

top values of country:
  DE  35  25.0%
  GR  35  25.0%
  JP  35  25.0%
  US  35  25.0%
```

`records` is the matching rows. Type misses appear as a `where type misses:` line, and as
`predicate_misses` in JSON output.

### Mistakes are reported with a position

```
$ --where "age >"
Error: ... invalid where expression "age >": syntax error at column 6: expected a value (number, quoted string, true or false), found "end of expression"

$ --where "(age = 1"
Error: ... syntax error at column 9: expected ) but found "end of expression"

$ --where "age ~ 1"
Error: ... syntax error at column 5: unexpected character "~"

$ --where "name contains 5"
Error: ... syntax error at column 15: expected a quoted string but found "5"

$ --where "active < true"
Error: ... syntax error at column 8: < cannot compare true or false: use = or !=

$ --where "age in ()"
Error: ... syntax error at column 9: expected a value (number, quoted string, true or false), found ")"
```

An expression is validated before any file is opened, so a typo costs nothing.

## Interaction with other options

- Row filters run in the order they are given, before transforms. A `--where` therefore narrows what
  sampling sees, so `--sample` and `--sample-n` count among the records that pass it.
- It forces the record path, so a same-format conversion decodes and re-encodes.
- `stats --where` profiles only the matching records; `Stats.RowsFiltered` counts the rest.

## From the library

```go
streamio.ProcessFile(ctx, path, handler,
    streamio.WithWhere(`country in ('US', 'GR') and age >= 18`),
)
```

An expression that does not parse is reported as an option error (wrapping `ErrInvalidWhere`, with
the column of the problem) before any file is opened.

## Not supported

Arithmetic, functions, regular expressions, and comparing two columns. The right-hand side of a
comparison is always a literal.
