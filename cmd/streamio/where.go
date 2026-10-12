package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/AlexAslan/streamio"
)

// whereFlags holds the row-predicate flags convert and stats share.
type whereFlags struct {
	expr string
	// set is whether --where was given at all, so an explicitly empty expression is an error
	// instead of being mistaken for "no filter".
	set    bool
	strict bool
}

func registerWhereFlags(cmd *cobra.Command, f *whereFlags) {
	cmd.Flags().StringVar(&f.expr, "where", "",
		"keep only records matching a predicate, e.g. \"age >= 18 and country in ('US','GR')\"")
	cmd.Flags().BoolVar(&f.strict, "where-strict", false,
		"fail when a value has the wrong type for its literal, instead of treating it as not matching")
}

// markWhereSet records whether --where was given on cmd's command line.
func markWhereSet(cmd *cobra.Command, f *whereFlags) {
	f.set = cmd.Flags().Changed("where")
}

var (
	errWhereStrictNeedsWhere = errors.New("--where-strict needs --where")
	errEmptyWhere            = errors.New("--where is empty: give a predicate, or omit the flag")
)

// whereOptions turns the flags into library options, or nil when --where was not given.
func whereOptions(f whereFlags) ([]streamio.Option, error) {
	if strings.TrimSpace(f.expr) == "" {
		switch {
		case f.set:
			return nil, errEmptyWhere
		case f.strict:
			return nil, errWhereStrictNeedsWhere
		default:
			return nil, nil
		}
	}
	if f.strict {
		return []streamio.Option{streamio.WithWhereStrict(f.expr)}, nil
	}
	return []streamio.Option{streamio.WithWhere(f.expr)}, nil
}

// warnWhere tells the user about what quietly changed a --where's meaning: comparisons that met a
// value of the wrong type and did not match, and columns no record had.
func warnWhere(w io.Writer, expr string, misses int64, unseen []string) {
	if misses > 0 {
		fmt.Fprintf(w, "warning: %d comparisons in --where %q found a value of the wrong type and did not match "+
			"(use --where-strict to fail instead)\n", misses, expr)
	}
	for _, col := range unseen {
		fmt.Fprintf(w, "warning: --where %q compares column %q, which no record had\n", expr, col)
	}
}
