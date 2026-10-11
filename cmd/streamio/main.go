// Command streamio-cli converts a document file between the formats streamio
// supports (NDJSON, Parquet, CSV, TSV, and Arrow IPC), and profiles them.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// newRootCmd builds the streamio command tree.
//
// Errors are silenced here and printed once by main: cobra would otherwise print "Error: ..." and
// main would print the same message again.
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "streamio",
		Short:         "Convert and profile document files (NDJSON, Parquet, CSV, TSV, Arrow IPC)",
		SilenceErrors: true,
	}
	root.AddCommand(getConvertCmd())
	root.AddCommand(getStatsCmd())
	return root
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
