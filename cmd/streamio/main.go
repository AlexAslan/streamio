// Command streamio-cli converts a document file between the formats streamio
// supports (NDJSON, Parquet, CSV, TSV, and Arrow IPC).
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "streamio",
		Short: "Convert document files between NDJSON, Parquet, CSV, TSV, and Arrow IPC",
	}
	root.AddCommand(getConvertCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
