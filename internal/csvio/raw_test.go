package csvio_test

import (
	"context"
	"streamio/internal/csvio"
	"streamio/internal/formatio"
	"streamio/internal/options"
	"testing"
)

// drainRawSource claims chunks until src reports it is exhausted, returning the row bytes it
// dispatched.
func drainRawSource(tb testing.TB, src formatio.RawSource) [][]byte {
	tb.Helper()
	out := make(chan [][]byte, 8)
	var stats formatio.DecodeStats

	done := make(chan error, 1)
	go func() { done <- src.DecodeRaw(context.Background(), out, &stats) }()

	var rows [][]byte
	for {
		select {
		case batch, ok := <-out:
			if !ok {
				continue
			}
			rows = append(rows, batch...)
		case err := <-done:
			if err != nil {
				tb.Fatalf("DecodeRaw: %v", err)
			}
			for {
				select {
				case batch := <-out:
					rows = append(rows, batch...)
				default:
					return rows
				}
			}
		}
	}
}

// TestNewRawSource_SkipsHeaderDispatchesDataRows checks the header line is not itself dispatched
// as a document when cfg.CSV.HasHeader is set.
func TestNewRawSource_SkipsHeaderDispatchesDataRows(t *testing.T) {
	path := writeCSVFile(t, "id,name\n1,Alice\n2,Bob\n")
	cfg := newConfig(1, 0, ',', true)

	src, err := csvio.NewRawSource(cfg, openSource(t, path))
	if err != nil {
		t.Fatalf("NewRawSource: %v", err)
	}
	t.Cleanup(func() {
		if cerr := src.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	rows := drainRawSource(t, src)
	// Raw passthrough preserves each line's original terminator exactly, so dispatched rows can be
	// concatenated back to back with no separator and still reproduce the input's own bytes.
	want := []string{"1,Alice\n", "2,Bob\n"}
	if len(rows) != len(want) {
		t.Fatalf("dispatched %d rows, want %d: %v", len(rows), len(want), rows)
	}
	for i := range want {
		if string(rows[i]) != want[i] {
			t.Errorf("row %d = %q, want %q", i, rows[i], want[i])
		}
	}
}

// TestNewRawSource_NoHeaderDispatchesEveryLine checks every line is dispatched when the file has
// no header row to skip.
func TestNewRawSource_NoHeaderDispatchesEveryLine(t *testing.T) {
	path := writeCSVFile(t, "1,Alice\n2,Bob\n")
	cfg := newConfig(1, 0, ',', false)

	src, err := csvio.NewRawSource(cfg, openSource(t, path))
	if err != nil {
		t.Fatalf("NewRawSource: %v", err)
	}
	t.Cleanup(func() {
		if cerr := src.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	rows := drainRawSource(t, src)
	if len(rows) != 2 {
		t.Fatalf("dispatched %d rows, want 2: %v", len(rows), rows)
	}
}

// TestNewRawSource_EmptyFile checks an empty input dispatches nothing rather than erroring.
func TestNewRawSource_EmptyFile(t *testing.T) {
	path := writeCSVFile(t, "")
	cfg := options.New(options.WithCSVHasHeader(false))

	src, err := csvio.NewRawSource(cfg, openSource(t, path))
	if err != nil {
		t.Fatalf("NewRawSource: %v", err)
	}
	t.Cleanup(func() {
		if cerr := src.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	var stats formatio.DecodeStats
	out := make(chan [][]byte, 1)
	if err = src.DecodeRaw(context.Background(), out, &stats); err != nil {
		t.Fatalf("DecodeRaw: %v", err)
	}
	select {
	case batch := <-out:
		t.Fatalf("dispatched %d rows on an empty file, want 0", len(batch))
	default:
	}
}
