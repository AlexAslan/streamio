# AGENTS.md

## What this is

`streamio` is a Go library (module `streamio`, Go 1.27) that reads NDJSON and Parquet files and
pushes documents to a caller-provided handler, with an accompanying CLI (`cmd/streamio/`) for format
conversion. See `README.md` for a detailed architecture writeup (raw-passthrough vs. generic
record path, the decode/dispatch worker pool, NDJSON chunking, Parquet row groups) before making
non-trivial changes — it documents *why* the code is shaped the way it is, not just what it does.

## Layout

- `streamio.go`, `api.go` — public entry point (`ProcessFile`, option functions).
- `internal/pool` — the shared fan-out/fan-in decode/dispatch worker pool.
- `internal/jsonio` — NDJSON decode/encode, raw source.
- `internal/parquetio` — Parquet decode/encode, raw source, row-group extraction.
- `internal/csvio` — CSV and TSV decode/encode, raw source (one package, delimiter-parameterized).
- `internal/record` — canonical format-neutral row type (`Record`, `Field`, `Value`).
- `internal/formatio` — the generic cross-format seam (`RecordDecoder`/`RecordEncoder` interfaces).
- `internal/options` — functional options, config, transforms.
- `cmd/streamio/` — the `streamio` CLI (cobra-based).

## Build / test / lint

```bash
make build              # go build -o build/streamio ./cmd/streamio
make test               # go test -race -covermode=atomic -shuffle=on ./...
make lint                # golangci-lint run
make lint-fix            # golangci-lint run --fix
make fmt                 # golangci-lint fmt (goimports + gofumpt)
make lint-new-issues      # lint only issues new vs. base branch
```

Run `make fmt` and `make lint` before considering a change done — `.golangci.yml` enables a large,
strict linter set (including `gocritic`, `revive`, `err113`, `wrapcheck`-adjacent checks via
`std-error-handling`, `mnd`, `ireturn`, etc.). Test files are exempted from a few linters
(`bodyclose`, `dupl`, `errcheck`, `funlen`, `goconst`, `gosec`, `noctx`) but not others.

## Conventions to preserve

- **Allocation-free decoding.** `record.Value` is a tagged union, not `map[string]any` — decoders
  only classify values (`Kind` + `Semantic`), they never pre-render them. Don't reintroduce boxing
  or per-field heap allocation on the decode hot path.
- **One implementation per conversion.** A hand-written Parquet→JSON fast path was deliberately
  deleted in favor of the generic `record.Record` path despite being faster, to avoid maintaining
  two implementations of the same conversion. Don't add format-pair-specific shortcuts without a
  strong reason and buy-in — the generic path is intentionally the only path.
- **`formatio.SplittableRecordDecoder`** lets a decoder hand out per-worker scratch and share a
  work queue; implement it for any new decoder that can be parallelized, but respect that the
  natural claim unit (chunk, row group) caps worker count.
- **Errors over silent coercion.** `parquetio`'s encoder rejects schema drift, unsupported kinds
  (`KindMap`), etc. rather than coercing or dropping — keep that pattern for new format support.
- **Bypass a stdlib API only when it's the measured allocation ceiling, and prove equivalence.**
  `internal/csvio/row.go`'s `rowWriter` reimplements `encoding/csv.Writer`'s RFC 4180
  quoting/escaping directly over a reused `[]byte` buffer because `csv.Writer.Write([]string)`
  forces a per-field string allocation that a byte-native path avoids. Don't reach for this
  pattern speculatively — confirm the stdlib call is the actual bottleneck via benchmark first —
  and when you do reach for it, pin the reimplementation against the real stdlib behavior with a
  fuzz-style test (see `internal/csvio/quoting_test.go`), not just hand-picked cases.
- Tests use the standard `_test.go` + testify convention; benchmark files are named
  `*_bench_test.go`.

## Doc comments

Every exported identifier's godoc comment starts with the identifier's own name (already the
existing convention throughout this codebase) and is **hard-capped at 1–2 sentences, plain
human-friendly language** — never dense technical prose, never a multi-paragraph block. This
applies to every godoc comment in the repo, not just new ones — the codebase was swept to this
style as of the CSV/TSV addition, and lint-clean, tested code proved the cap holds even for
subtle concurrency/allocation-heavy types.

A godoc comment explains *what* an identifier is/does. Nothing else belongs in it:

- A non-obvious **why** (a hidden constraint, a workaround, a subtle invariant) goes in a regular
  `//` comment placed after the short godoc block, never folded into the godoc sentences
  themselves — a reader who only wants "what is this" should never have to skim past "why" to find
  it, and `go doc`/pkg.go.dev output should stay skimmable.
- If a "why" comment itself grows past a short paragraph, that's a signal it belongs in
  `README.md`'s architecture writeup instead, with the code comment reduced to a pointer at it —
  don't let source comments become the primary place a design decision is explained.
- Prefer trimming content over relocating it: when a rationale is inferable from the code itself
  (a variable name, a type's own field comments, an adjacent test name), cut it rather than write
  it down twice.

This rule governs godoc comments (the comment block immediately above a package clause or an
exported `func`/`type`/`const`/`var`). It does not touch `//nolint:x // reason` directives (a
different, lint-enforced contract) or ordinary comments on unexported identifiers, which may stay
as long as they need to be.

## PR / commit notes

- Never include the Claude Code session link in PR descriptions (per global user preference).
- This repo has no GitHub remote-derived defaults assumed here — check `git remote` before
  assuming a base branch; `Makefile`'s `BASE_REF` autodetects it for `lint-new-issues`.
