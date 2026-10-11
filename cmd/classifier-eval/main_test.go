package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadDataset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cases.jsonl")
	content := strings.Join([]string{
		`{"input":"talk","candidates":["a","b"],"expected":{"mode":"broadcast"}}`,
		`{"input":"spam","candidates":["a","b"],"expected":{"mode":"drop"}}`,
		`{"input":"help","candidates":["a","b"],"expected":{"mode":"selective","targets":["a"]}}`,
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cases, err := readDataset(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 3 || len(cases[0].Expected.Targets) != 2 || !cases[1].Expected.Drop || len(cases[2].Expected.Targets) != 1 {
		t.Fatalf("cases = %#v", cases)
	}
}

func TestReadDatasetRejectsUnknownAndInvalidPlans(t *testing.T) {
	for _, content := range []string{
		`{"input":"x","candidates":["a"],"expected":{"mode":"invoke"}}`,
		`{"input":"x","candidates":["a"],"expected":{"mode":"selective"}}`,
		`{"input":"x","candidates":["a"],"expected":{"mode":"broadcast"},"extra":true}`,
	} {
		t.Run(content, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cases.jsonl")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readDataset(path); err == nil {
				t.Fatal("expected invalid dataset error")
			}
		})
	}
}
