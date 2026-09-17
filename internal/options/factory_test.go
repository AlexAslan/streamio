package options_test

import (
	"streamio/internal/options"
	"testing"
)

func TestNew_Defaults(t *testing.T) {
	cfg := options.New()

	if cfg.Run.Workers != 1 {
		t.Errorf("Workers = %d, want 1", cfg.Run.Workers)
	}
	if cfg.Run.BatchSize != 512 {
		t.Errorf("BatchSize = %d, want 512", cfg.Run.BatchSize)
	}
	if cfg.Run.ReadBufferSize != 32*1024*1024 {
		t.Errorf("ReadBufferSize = %d, want 32MiB", cfg.Run.ReadBufferSize)
	}
	if cfg.NDJSON.ChunkSize != 32*1024*1024 {
		t.Errorf("ChunkSize = %d, want 32MiB", cfg.NDJSON.ChunkSize)
	}
	if cfg.Parquet.MaxOpenReaders != cfg.Run.Workers {
		t.Errorf("MaxOpenReaders = %d, want %d (Workers)", cfg.Parquet.MaxOpenReaders, cfg.Run.Workers)
	}
}

// TestNew_ZeroOrNegativeOptionsFallBackToDefault verifies that WithXxx(n) for n<=0 is treated the
// same as omitting the option, not as an explicit override to zero.
func TestNew_ZeroOrNegativeOptionsFallBackToDefault(t *testing.T) {
	cases := []struct {
		opt  options.Option
		get  func(options.Config) int
		name string
		want int
	}{
		{
			name: "zero workers", opt: options.WithParallelWorkers(0),
			get: func(c options.Config) int { return c.Run.Workers }, want: 1,
		},
		{
			name: "negative workers", opt: options.WithParallelWorkers(-1),
			get: func(c options.Config) int { return c.Run.Workers }, want: 1,
		},
		{
			name: "zero batch size", opt: options.WithBatchSize(0),
			get: func(c options.Config) int { return c.Run.BatchSize }, want: 512,
		},
		{
			name: "negative batch size", opt: options.WithBatchSize(-5),
			get: func(c options.Config) int { return c.Run.BatchSize }, want: 512,
		},
		{
			name: "zero chunk size", opt: options.WithChunkSize(0),
			get: func(c options.Config) int { return c.NDJSON.ChunkSize }, want: 32 * 1024 * 1024,
		},
		{
			name: "zero read buffer size", opt: options.WithReadBufferSize(0),
			get: func(c options.Config) int { return c.Run.ReadBufferSize }, want: 32 * 1024 * 1024,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := options.New(tc.opt)
			if got := tc.get(cfg); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestNew_MaxOpenReadersDefaultsToWorkers verifies MaxOpenReaders tracks an explicit Workers
// count when not itself set, rather than silently serializing decoding below the requested
// parallelism.
func TestNew_MaxOpenReadersDefaultsToWorkers(t *testing.T) {
	cfg := options.New(options.WithParallelWorkers(6))
	if cfg.Parquet.MaxOpenReaders != 6 {
		t.Errorf("MaxOpenReaders = %d, want 6 (Workers)", cfg.Parquet.MaxOpenReaders)
	}
}

// TestNew_MaxOpenReadersExplicitOverride verifies an explicit WithMaxOpenReaders survives
// regardless of Workers.
func TestNew_MaxOpenReadersExplicitOverride(t *testing.T) {
	cfg := options.New(options.WithParallelWorkers(6), options.WithMaxOpenReaders(2))
	if cfg.Parquet.MaxOpenReaders != 2 {
		t.Errorf("MaxOpenReaders = %d, want 2", cfg.Parquet.MaxOpenReaders)
	}
}

// TestNew_ReadBufferSizeCappedToChunkSize verifies a ReadBufferSize larger than ChunkSize is
// capped, since a full-size buffer per worker would be wasted on small chunks.
func TestNew_ReadBufferSizeCappedToChunkSize(t *testing.T) {
	cfg := options.New(options.WithChunkSize(1024), options.WithReadBufferSize(1024*1024))
	if cfg.Run.ReadBufferSize != 1024 {
		t.Errorf("ReadBufferSize = %d, want 1024 (capped to ChunkSize)", cfg.Run.ReadBufferSize)
	}
}

// TestNew_ReadBufferSizeUnderChunkSizeUnchanged verifies the cap doesn't kick in when
// ReadBufferSize is already <= ChunkSize.
func TestNew_ReadBufferSizeUnderChunkSizeUnchanged(t *testing.T) {
	cfg := options.New(options.WithChunkSize(1024*1024), options.WithReadBufferSize(4096))
	if cfg.Run.ReadBufferSize != 4096 {
		t.Errorf("ReadBufferSize = %d, want 4096 (unchanged)", cfg.Run.ReadBufferSize)
	}
}

func TestNew_LoggerOption(t *testing.T) {
	logger := &fakeLogger{}
	cfg := options.New(options.WithLogger(logger))
	if cfg.Logger != logger {
		t.Errorf("Logger not set to the provided instance")
	}
}

type fakeLogger struct{}

func (*fakeLogger) Printf(string, ...any) {}
