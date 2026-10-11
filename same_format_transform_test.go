package streamio_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/AlexAslan/streamio"
)

// stringSink collects a copy of every dispatched document as a string.
func stringSink() (streamio.DocumentHandler, func() []string) {
	var (
		mu   sync.Mutex
		docs []string
	)
	sink := func(_ context.Context, doc []byte) error {
		mu.Lock()
		defer mu.Unlock()
		docs = append(docs, string(doc))
		return nil
	}
	return sink, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), docs...)
	}
}

// TestProcessFile_TransformsApplyToSameFormatConversions pins that a rename/drop rule is honored
// when the output format matches the input's: those runs used to take raw passthrough, which never
// decodes records, so the rules silently did nothing.
func TestProcessFile_TransformsApplyToSameFormatConversions(t *testing.T) {
	rules := []streamio.Option{
		streamio.WithTransforms(streamio.RenamePath("name", "full_name"), streamio.DropPath("secret")),
	}

	t.Run("ndjson", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "in.ndjson")
		data := "{\"id\":1,\"name\":\"ann\",\"secret\":\"x\"}\n{\"id\":2,\"name\":\"bob\",\"secret\":\"y\"}\n"
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}

		sink, collected := stringSink()
		opts := append([]streamio.Option{streamio.WithOutputFormat(streamio.FormatJSON)}, rules...)
		if _, err := streamio.ProcessFile(context.Background(), path, sink, opts...); err != nil {
			t.Fatalf("ProcessFile: %v", err)
		}

		docs := collected()
		if len(docs) != 2 {
			t.Fatalf("dispatched %d documents, want 2", len(docs))
		}
		for _, doc := range docs {
			if !strings.Contains(doc, `"full_name"`) || strings.Contains(doc, `"name"`) || strings.Contains(doc, "secret") {
				t.Errorf("transform not applied to same-format output: %s", doc)
			}
		}
	})

	t.Run("csv", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "in.csv")
		if err := os.WriteFile(path, []byte("id,name,secret\n1,ann,x\n2,bob,y\n"), 0o600); err != nil {
			t.Fatal(err)
		}

		sink, collected := stringSink()
		opts := append([]streamio.Option{
			streamio.WithOutputFormat(streamio.FormatCSV),
			streamio.WithCSVHasHeader(true),
			streamio.WithParallelWorkers(1),
		}, rules...)
		if _, err := streamio.ProcessFile(context.Background(), path, sink, opts...); err != nil {
			t.Fatalf("ProcessFile: %v", err)
		}

		out := strings.Join(collected(), "")
		if !strings.Contains(out, "full_name") || strings.Contains(out, "secret") {
			t.Errorf("transform not applied to same-format output:\n%s", out)
		}
	})
}

// TestProcessFile_SameFormatWithoutTransformsStaysRaw guards the other side: with no transform the
// bytes still stream through untouched.
func TestProcessFile_SameFormatWithoutTransformsStaysRaw(t *testing.T) {
	path := filepath.Join(t.TempDir(), "in.ndjson")
	// Odd spacing a decode/encode round trip would normalize away.
	line := "{ \"id\" : 1 ,   \"name\":\"ann\" }"
	if err := os.WriteFile(path, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	sink, collected := stringSink()
	if _, err := streamio.ProcessFile(context.Background(), path, sink); err != nil {
		t.Fatalf("ProcessFile: %v", err)
	}
	docs := collected()
	if len(docs) != 1 || strings.TrimSpace(docs[0]) != line {
		t.Errorf("raw passthrough altered the line: %q", docs)
	}
}
