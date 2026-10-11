package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/AlexAslan/streamio"
)

// whereFlags holds the row-predicate flags convert and stats share.
type whereFlags struct {
	expr   string
	strict bool
}

func registerWhereFlags(cmd *cobra.Command, f *whereFlags) {
	cmd.Flags().StringVar(&f.expr, "where", "",
		"keep only records matching a predicate, e.g. \"age >= 18 and country in ('US','GR')\"")
	cmd.Flags().BoolVar(&f.strict, "where-strict", false,
		"fail when a value has the wrong type for its literal, instead of treating it as not matching")
}

// whereOptions turns the flags into library options, or nil when --where was not given.
func whereOptions(f whereFlags) ([]streamio.Option, error) {
	if f.expr == "" {
		if f.strict {
			return nil, errWhereStrictNeedsWhere
		}
		return nil, nil
	}
	if f.strict {
		return []streamio.Option{streamio.WithWhereStrict(f.expr)}, nil
	}
	return []streamio.Option{streamio.WithWhere(f.expr)}, nil
}

var errWhereStrictNeedsWhere = errors.New("--where-strict needs --where")

// warnPredicateMisses tells the user when a --where comparison met values of the wrong type for its
// literal, since those rows quietly did not match.
func warnPredicateMisses(w io.Writer, n int64, expr string) {
	if n == 0 {
		return
	}
	fmt.Fprintf(w, "warning: %d comparisons in --where %q found a value of the wrong type and did not match "+
		"(use --where-strict to fail instead)\n", n, expr)
}
