// Package parquetio supplies streamio's Parquet capabilities: raw row-group passthrough, decoding
// into canonical records, and encoding records back into Parquet documents.
package parquetio

import (
	"os"
	"streamio/internal/options"
	"sync/atomic"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

type columnMeta struct {
	logicalType *format.LogicalType
}

// sharedState is read-only once built and shared by every decode worker.
type sharedState struct {
	readerSlots  chan struct{}
	leafPaths    [][]string
	columnMeta   []columnMeta
	rowGroups    []parquetgo.RowGroup
	mapOrder     []string
	nextRowGroup atomic.Int64

	batchSize int
}

// openShared opens the Parquet file at path and walks its schema once to build the state every
// decode worker shares. The returned file is owned by the caller and must stay open for as long as
// the state is used: the row groups read their column chunks straight from it.
func openShared(cfg options.Config, path string) (*os.File, *sharedState, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	pf, err := parquetgo.OpenFile(f, stat.Size())
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	schema := pf.Schema()
	leafPaths := schema.Columns()
	s := &sharedState{
		leafPaths:   leafPaths,
		columnMeta:  buildColumnMeta(schema, leafPaths),
		mapOrder:    mapFieldOrder(leafPaths),
		readerSlots: make(chan struct{}, cfg.Parquet.MaxOpenReaders),
		rowGroups:   pf.RowGroups(),
		batchSize:   cfg.Run.BatchSize,
	}

	if cfg.Logger != nil {
		msg := "parquet file %s: %d row groups, %d rows total, size: %d bytes"
		cfg.Logger.Printf(msg, path, len(s.rowGroups), pf.NumRows(), stat.Size())
	}

	return f, s, nil
}
