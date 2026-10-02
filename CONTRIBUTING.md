# Contributing to streamio

Thanks for your interest! Bug reports, fixes, docs and well-scoped features are all welcome.

## Before you start

- For anything beyond a small fix, please open an issue first so we can agree on the approach.
- Read the [architecture docs](docs/architecture.md) — they explain *why* the code is shaped the
  way it is (raw passthrough vs. generic record path, the worker pool, chunking).
- Security issues: do **not** open a public issue; see [SECURITY.md](SECURITY.md).

## Development

Requires Go (see `go.mod`) and [golangci-lint](https://golangci-lint.run).

```bash
make build   # build the CLI into build/streamio
make test    # race detector, shuffled
make fmt     # goimports + gofumpt
make lint    # strict golangci-lint set
```

Run `make fmt lint test` before opening a PR; CI runs the same checks.

## Conventions

- **Allocation-free decoding:** `record.Value` is a tagged union; don't box values or add per-field
  heap allocations on the decode hot path.
- **One implementation per conversion:** no format-pair-specific shortcuts without prior discussion.
- **Errors over silent coercion:** reject unsupported or drifting input instead of dropping data.
- **Doc comments:** every exported identifier's godoc starts with its name and is 1–2 plain
  sentences. Put non-obvious *why* in a regular `//` comment, or in the README/docs.
- **Imports:** stdlib, third-party, then this module, last.
- **Tests:** `_test.go` with testify; benchmarks in `*_bench_test.go`. Include a test for every
  behavior change, and a benchmark comparison for hot-path changes.

## Pull requests

1. Fork and branch from `master`.
2. Keep PRs focused; one logical change each.
3. Fill in the PR template and make sure CI is green.
4. A maintainer will review; please be patient and responsive to feedback.

Commit messages: short imperative summary line, details in the body if needed.

## License

By contributing you agree that your contributions are licensed under the [MIT License](LICENSE).
