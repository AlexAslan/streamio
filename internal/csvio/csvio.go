// Package csvio owns streamio's CSV and TSV support: one package for both, since they differ only
// in field delimiter.
//
// A CSV field has no type of its own, so every decoded field is record.KindBytes rather than
// sniffed as a number or bool.
package csvio

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"streamio/internal/options"
	"sync"
	"sync/atomic"
)

// defaultDelimiter is used when cfg.CSV.Delimiter is unset and the format itself doesn't say
// otherwise (comma, for plain CSV).
const defaultDelimiter = ','

// tsvDelimiter is TSV's own default, used when the caller asked for FormatTSV and didn't set an
// explicit delimiter.
const tsvDelimiter = '\t'

// defaultChunkSize is used when cfg.NDJSON.ChunkSize is unset. csvio reuses NDJSON's chunk-size
// setting rather than adding a separate one, since the two formats split files the same way.
const defaultChunkSize = 32 * 1024 * 1024

// delimiterFor resolves the rune a decoder or encoder should split or join fields on: whatever the
// caller set explicitly, or the format's own default otherwise.
func delimiterFor(cfg options.Config, format options.OutputFormat) rune {
	if cfg.CSV.Delimiter != 0 {
		return cfg.CSV.Delimiter
	}
	if format == options.FormatTSV {
		return tsvDelimiter
	}
	return defaultDelimiter
}

// sharedState is read-only once built (aside from its atomics) and shared by every decode worker,
// mirroring jsonio's sharedState — the two packages split a file into byte-range chunks the same
// way, differing only in what a chunk's rows decode into.
type sharedState struct {
	f              *os.File
	path           string
	header         []string
	size           int64
	nextChunk      atomic.Int64
	bodyStart      int64
	readBufferSize int
	batchSize      int
	chunkSize      int
	headerOnce     sync.Once
	delimiter      rune
	hasHeader      bool
}

// openShared opens the CSV/TSV file at path, reads its header row when cfg.CSV.HasHeader says
// there is one, and builds the state every decode worker shares.
func openShared(cfg options.Config, path string, format options.OutputFormat) (*os.File, *sharedState, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	s := &sharedState{
		f:              f,
		path:           path,
		size:           stat.Size(),
		delimiter:      delimiterFor(cfg, format),
		hasHeader:      cfg.CSV.HasHeader,
		readBufferSize: cfg.Run.ReadBufferSize,
		batchSize:      cfg.Run.BatchSize,
		chunkSize:      cfg.NDJSON.ChunkSize,
	}
	if s.chunkSize <= 0 {
		s.chunkSize = defaultChunkSize
	}

	if cfg.CSV.HasHeader {
		header, headerEnd, headerErr := readHeaderLine(f, s.delimiter)
		if headerErr != nil {
			f.Close()
			return nil, nil, headerErr
		}
		s.header = header
		s.bodyStart = headerEnd
	}

	if cfg.Logger != nil {
		cfg.Logger.Printf("csv file %s, size: %d bytes, delimiter: %q, header: %v",
			path, stat.Size(), s.delimiter, cfg.CSV.HasHeader)
	}

	return f, s, nil
}

// claimChunk claims the next unclaimed chunk from the shared queue, returning its index and start
// offset relative to the file's data region (past any header line). The bool is false once the
// file is exhausted.
func (s *sharedState) claimChunk() (int64, int64, bool) {
	idx := s.nextChunk.Add(1) - 1
	start := s.bodyStart + idx*int64(s.chunkSize)
	return idx, start, start < s.size
}

// openChunkReader returns a reader over the shared file positioned at a resolved line boundary for
// chunk chunkIdx, plus the number of bytes read past chunkStart while resolving that boundary. The
// reader extends all the way to EOF, not just to this chunk's nominal chunkSize: the caller (a
// csv.Reader wrapping this) reads exactly one full row past chunkSize when the last row in range
// straddles the boundary, rather than truncating it — see recordDecoder's own doc for why that
// overrun can never collide with the next chunk's own boundary resolution.
//
// Mirrors jsonio's own boundary resolution: chunk 0 starts exactly at the data region's start
// (chunkStart == s.bodyStart, never mid-line), and every later chunk starts wherever claimChunk put
// it, which can land mid-row, so it skips forward to the next physical line.
//
// This scan is by physical newline, not by row: it cannot tell a literal newline inside a quoted
// field apart from a real row terminator, so a chunk boundary landing inside a multi-line quoted
// field is misresolved — the same limitation record_decoder.go's predecessor had (see git history),
// carried forward rather than fixed by this change. Fixing it correctly requires knowing whether a
// given byte offset falls inside an open quote, which cannot be answered by scanning locally around
// that offset (an arbitrarily large quoted field earlier in the chunk can make the local answer
// wrong): it needs quote parity tracked from a known-safe start (the header, or file start), i.e.
// effectively sequential work proportional to file size — which would defeat chunked parallelism
// for exactly the files that need it most. This is called out here as a known, unresolved
// correctness gap for CSV/TSV files containing quoted newlines run with more than one decode
// worker, not silently accepted: WithParallelWorkers(1) reads the whole file through one
// csv.Reader and has no such limitation, since there is only ever one boundary (the start of the
// file, never mid-quote).
//
// Verified (TestNewDecoder_QuotedNewlineAtChunkBoundaryErrors, internal/csvio/record_decoder_test.go)
// that landing a boundary inside a quoted newline fails safely: the chunk that starts mid-quote
// hands encoding/csv a stream that looks like an unterminated quoted field, which csv.Reader
// rejects with a parse error rather than silently returning misaligned or corrupted rows. This gap
// is a "run WithParallelWorkers(1) for such files, or expect a decode error" limitation, not a
// silent-data-corruption risk.
func (s *sharedState) openChunkReader(chunkIdx, chunkStart int64) (*bufio.Reader, int64, error) {
	if chunkIdx == 0 {
		sr := io.NewSectionReader(s.f, chunkStart, s.size-chunkStart)
		return bufio.NewReaderSize(sr, s.readBufferSize), 0, nil
	}

	sr := io.NewSectionReader(s.f, chunkStart-1, s.size-(chunkStart-1))
	br := bufio.NewReaderSize(sr, s.readBufferSize)

	first, err := br.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	if first == '\n' {
		return br, 0, nil
	}

	discarded, err := br.ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}

	return br, int64(len(discarded)), nil
}

// readHeaderLine reads the file's first physical line and parses it as one CSV row of column
// names, returning the names and the byte offset just past the line's terminator. This is a
// one-shot, once-per-file read, not the per-row hot path record_decoder.go optimizes, so a
// throwaway csv.Reader over the known-length header bytes costs nothing worth avoiding.
func readHeaderLine(f *os.File, delimiter rune) ([]string, int64, error) {
	sr := io.NewSectionReader(f, 0, math.MaxInt64)
	br := bufio.NewReader(sr)

	raw, err := br.ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}

	r := csv.NewReader(bytes.NewReader(raw))
	r.Comma = delimiter
	r.FieldsPerRecord = -1
	names, parseErr := r.Read()
	if parseErr != nil {
		return nil, 0, fmt.Errorf("csv header: %w", parseErr)
	}
	return names, int64(len(raw)), nil
}
