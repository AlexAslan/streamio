package jsonio_test

import (
	"context"
	"os"

	"github.com/AlexAslan/streamio/internal/jsonio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/pool"
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
	f, err := os.Open(path)
	if err != nil {
		return options.Result{}, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return options.Result{}, err
	}

	raw, err := jsonio.NewRawSource(cfg, options.Source{Reader: f, Size: stat.Size(), Name: path})
	if err != nil {
		return options.Result{}, err
	}
	defer raw.Close()

	return pool.RunRaw(ctx, cfg, raw, sink)
}
