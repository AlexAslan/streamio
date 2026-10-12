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

Keywords are case-insensitive; column names are case-sensitive and may contain any letters, digits,
`_` and `-`, including non-ASCII ones (`größe > 5`). A dotted path reaches into nested map entries
(`attrs.env`). A column whose own name contains a dot or other punctuation is written in backticks:
``attrs.`a.b` ``.

Details worth knowing:

- **Reserved words.** `not` is the one word you cannot use as a bare column name, because it begins
  an expression; write `` `not` = 1 ``. Words like `and`, `in`, `is` and `contains` are fine as column
  names where they cannot be misread.
- **Strings** use `'...'` or `"..."` with the escapes `\n`, `\t`, `\\`, `\'` and `\"`. Any other
  backslash is an error, so a Windows path is written `'C:\\path'`.
- **`=` and `==`** both mean equals.
- **Numbers** are decimal, optionally signed, with an optional fraction and exponent: `-5`, `4.5`,
  `1e3`. A leading `+` or `.` is not accepted.
- **Limits.** Parentheses and `not` may nest 64 levels in total (each `not (` level uses two), and an
  expression may be at most 65,536 bytes.
- Syntax errors name the column of the problem, counted in characters.

## How a comparison is decided

The **literal's type** decides how a value is compared:

| Literal | Compares | Notes |
|---|---|---|
| `'text'` | the value's text, byte-wise | `age = '30'` matches the number 30; `<`, `>` are lexicographic |
| `30`, `4.5` | numerically | a text value is parsed (so a CSV column works); integers compare exactly |
| `true`, `false` | booleans | a text value `true`/`false`, in any case, also works |

The text of a value is the converters' own rendering, so a date column compares as
`'2024-01-01'`, and a number compares the same whichever format it came from.

### Numbers in detail

- **Whole numbers compare exactly**, as 64-bit integers, whether the value is a number or text:
  `id = 9007199254740993` does not match the text `"9007199254740992"`, which a float comparison
  would conflate. Values and literals up to 18446744073709551615 are exact, and an unsigned Parquet
  column compares as unsigned.
- **Decimals compare as 64-bit floats.** A `float32` column compares at float32 precision, so
  `ratio = 0.1` matches a stored `0.1`.
- **Text must look like a decimal number** to be read as one: digits with an optional sign, point
  and exponent. `inf`, `NaN`, hex, `1_000` and padded text (`" 42"`) are not numbers; they are type
  misses, so a column of identifiers cannot accidentally satisfy `> 0`.
- **A number does not compare with a date or timestamp column.** The stored value is a day or tick
  count whose unit would silently change the answer, so `ts > 1700000000` is a type miss. Compare with
  a quoted ISO-8601 string instead: `ts >= '2024-01-01' and ts < '2024-02-01'`.
- **NaN** is unordered: it matches only `!=`.

### Empty and missing values

A **missing, null or empty value matches no comparison**, including `!=`. Test for it with
`is empty` / `is not empty`:

```
email is not empty and email contains '@'
```

An empty string, `null`, an empty list and an empty map are all "empty". So `not (x = 1)` is true for
a record with no `x`. This is a deliberate, simple two-valued logic: a predicate answers "does this
record match?", and a record that doesn't have the value doesn't match.

### Wrong types

A value that cannot be compared with its literal (the string `unknown` against `age > 18`, a
non-empty list against a number, a boolean against a number) does **not match** and is counted in
`Stats.PredicateMisses`; the CLI prints the count as a warning. With `--where-strict`
(`WithWhereStrict`) the run fails instead, naming the column and why:

```
Error: ... value cannot be compared: column "age" is not a number, and the expression compares it with 18
```

The error does not carry a row number, since a record knows nothing of its position.

Things that follow from "a miss does not match":

- **A miss is counted where it is evaluated.** `and` and `or` stop as soon as the answer is known,
  so `a > 1 or b > 1` never looks at `b` when `a > 1` holds. The count is deterministic for an
  expression and data, and is in the order written.
- **`not` turns a miss into a match.** `not (age > 18)` is true for the text `"unknown"`: the
  comparison did not match, and its negation does. Under `--where-strict` that row is an error
  instead.
- **`in` is one test per row.** `x in (1, 'abc')` is true for `"abc"` whatever order the literals
  are in. If nothing matches and a literal was the wrong type, it is a single miss, and then neither
  `in` nor `not in` holds. Strict mode fails only in that case, never when a literal matched.

### A column that doesn't exist

A name no record has, almost always a typo, matches nothing and is reported:

```
$ streamio convert --in people.ndjson --out out.json --where "nmae = 'x'"
warning: --where "nmae = 'x'" compares column "nmae", which no record had
wrote out.json: 1000 rows read, 0 documents dispatched
```

`x is empty` is exempt, since asking about a column that may be absent is its purpose.

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

```
$ --where ""
Error: --where is empty: give a predicate, or omit the flag
```

An expression is validated before any file is opened, so a typo costs nothing. An *empty*
`--where` is an error rather than "no filter", so an unset shell variable cannot silently disable a
filter.

## Interaction with other options

- Row filters run in the order they are given, before transforms, so a later filter sees only the
  records that passed the earlier ones.
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
