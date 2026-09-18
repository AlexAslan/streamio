// Package parquetio supplies streamio's Parquet capabilities: raw row-group passthrough, decoding
// into canonical records, and encoding records back into Parquet documents.
package parquetio

import (
	"sync/atomic"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/AlexAslan/streamio/internal/options"
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

// openShared walks src's schema once to build the state every decode worker shares. src.Reader must
// stay open for as long as the state is used: the row groups read their column chunks straight
// from it.
func openShared(cfg options.Config, src options.Source) (*sharedState, error) {
	pf, err := parquetgo.OpenFile(src.Reader, src.Size)
	if err != nil {
		return nil, err
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
		cfg.Logger.Printf(msg, src.Name, len(s.rowGroups), pf.NumRows(), src.Size)
	}

	return s, nil
}
