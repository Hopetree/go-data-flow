package json

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

func TestSink_New(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
}

func TestSink_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "lines format",
			config: Config{
				FilePath: "/tmp/test.jsonl",
				Format:   "lines",
			},
			wantErr: false,
		},
		{
			name: "array format",
			config: Config{
				FilePath: "/tmp/test.json",
				Format:   "array",
			},
			wantErr: false,
		},
		{
			name: "array with indent",
			config: Config{
				FilePath: "/tmp/test-pretty.json",
				Format:   "array",
				Indent:   "  ",
			},
			wantErr: false,
		},
		{
			name: "append mode",
			config: Config{
				FilePath: "/tmp/test-append.jsonl",
				Append:   true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			configBytes, _ := json.Marshal(tt.config)
			err := s.Init(configBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSink_ConsumeLines(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.jsonl")

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: outputPath,
		Format:   "lines",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	records := []types.Record{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}

	in := make(chan types.Record, len(records))
	for _, r := range records {
		in <- r
	}
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify file exists
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// Count lines (should be 3)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines, got %d", len(lines))
	}

	// Verify each line is valid JSON
	for i, line := range lines {
		var result types.Record
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", i+1, err)
		}
	}

	// Verify count
	if s.Count() != len(records) {
		t.Errorf("Count() = %d, want %d", s.Count(), len(records))
	}
}

func TestSink_ConsumeArray(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.json")

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: outputPath,
		Format:   "array",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	records := []types.Record{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	in := make(chan types.Record, len(records))
	for _, r := range records {
		in <- r
	}
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify file exists and is valid JSON array
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var result []types.Record
	if err := json.Unmarshal(content, &result); err != nil {
		t.Fatalf("Output is not valid JSON array: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 records, got %d", len(result))
	}

	// Verify it starts with [ and ends with ]
	if !strings.HasPrefix(string(content), "[") {
		t.Error("Array output should start with [")
	}
	if !strings.HasSuffix(strings.TrimSpace(string(content)), "]") {
		t.Error("Array output should end with ]")
	}
}

func TestSink_ArrayWithIndent(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "pretty.json")

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: outputPath,
		Format:   "array",
		Indent:   "  ",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	records := []types.Record{
		{"id": 1},
	}

	in := make(chan types.Record, 1)
	in <- records[0]
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	// Should contain indentation (spaces)
	if !strings.Contains(string(content), "  ") {
		t.Error("Pretty output should contain indentation")
	}
}

func TestSink_Count(t *testing.T) {
	s := New()

	// Initial count
	if s.Count() != 0 {
		t.Errorf("Initial Count() = %d, want 0", s.Count())
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "count.jsonl")

	configBytes, _ := json.Marshal(Config{FilePath: outputPath})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	in := make(chan types.Record, 5)
	for i := 0; i < 5; i++ {
		in <- types.Record{"id": i}
	}
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	if s.Count() != 5 {
		t.Errorf("Count() = %d, want 5", s.Count())
	}
}
