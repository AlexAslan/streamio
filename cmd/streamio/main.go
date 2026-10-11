// Command streamio-cli converts a document file between the formats streamio
// supports (NDJSON, Parquet, CSV, TSV, and Arrow IPC), and profiles them.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "streamio",
		Short: "Convert and profile document files (NDJSON, Parquet, CSV, TSV, Arrow IPC)",
	}
	root.AddCommand(getConvertCmd())
	root.AddCommand(getStatsCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
