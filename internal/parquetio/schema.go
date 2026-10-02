package parquetio

import (
	parquetgo "github.com/AlexAslan/parquet-go"
)

// buildColumnMeta walks the schema once and returns a columnMeta slice indexed by leaf column index.
func buildColumnMeta(schema *parquetgo.Schema, leafPaths [][]string) []columnMeta {
	meta := make([]columnMeta, len(leafPaths))
	for i, path := range leafPaths {
		if leaf, ok := schema.Lookup(path...); ok {
			meta[i].logicalType = leaf.Node.Type().LogicalType()
		}
	}
	return meta
}

// isMapLeaf reports whether path is a leaf of a Map(String,String) column, which parquet encodes
// as the three-segment path [field, "key_value", "key"|"value"].
func isMapLeaf(path []string) bool {
	return len(path) == 3 && path[1] == "key_value"
}

// mapFieldOrder returns the top-level field names of every Map(String,String) column in
// leafPaths, in first-occurrence order, so map-column output order is stable across rows without
// having to detect it per row.
func mapFieldOrder(leafPaths [][]string) []string {
	var fields []string
	seen := make(map[string]bool)

	for _, path := range leafPaths {
		if isMapLeaf(path) && !seen[path[0]] {
			seen[path[0]] = true
			fields = append(fields, path[0])
		}
	}

	return fields
}
