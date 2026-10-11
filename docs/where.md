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
list against a number) does **not match** and is counted in `Stats.PredicateMisses`. With
`--where-strict` (`WithWhereStrict`) the run fails instead, naming the column.

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
