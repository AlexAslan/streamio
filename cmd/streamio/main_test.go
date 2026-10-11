package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// TestRootCmd_DoesNotPrintErrorsItself pins that cobra stays quiet about a command error, since
// main prints it: both printing it showed every error twice.
func TestRootCmd_DoesNotPrintErrorsItself(t *testing.T) {
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{
		"convert", "--in", filepath.Join(t.TempDir(), "missing.ndjson"), "--out", filepath.Join(t.TempDir(), "o.json"),
	})

	err := root.Execute()
	if err == nil {
		t.Fatal("converting a missing file returned no error")
	}
	if got := errOut.String() + out.String(); strings.Contains(got, "missing.ndjson") {
		t.Errorf("cobra printed the error itself (main does): %q", got)
	}
}

func TestRootCmd_HasBothCommands(t *testing.T) {
	root := newRootCmd()
	for _, name := range []string{"convert", "stats"} {
		if _, _, err := root.Find([]string{name}); err != nil {
			t.Errorf("command %q: %v", name, err)
		}
	}
}
