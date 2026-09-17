package jsonio_test

import (
	"context"
	"streamio/internal/jsonio"
	"streamio/internal/options"
	"streamio/internal/pool"
)

// convertFile decodes the newline-delimited JSON file at path and calls sink once per line. It is
// the same NewRawSource+pool.RunRaw composition production code reaches through
// streamio.ProcessFile, kept here — rather than exported from the package under test — so
// this package's own tests and benchmarks can exercise it without needing an export that no
// production code calls.
func convertFile(
	ctx context.Context,
	path string,
	sink options.DocumentHandler,
	cfg options.Config,
) (options.Result, error) {
	src, err := jsonio.NewRawSource(cfg, path)
	if err != nil {
		return options.Result{}, err
	}
	defer src.Close()

	return pool.RunRaw(ctx, cfg, src, sink)
}
