package csv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
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
			name: "basic config",
			config: Config{
				FilePath: "/tmp/test.csv",
			},
			wantErr: false,
		},
		{
			name: "with separator",
			config: Config{
				FilePath: "/tmp/test-tab.csv",
				Separator: "\t",
			},
			wantErr: false,
		},
		{
			name: "with headers",
			config: Config{
				FilePath: "/tmp/test-headers.csv",
				Headers:  []string{"id", "name", "value"},
			},
			wantErr: false,
		},
		{
			name: "append mode",
			config: Config{
				FilePath: "/tmp/test-append.csv",
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

func TestSink_Consume(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.csv")

	s := New()
	configBytes, _ := json.Marshal(Config{FilePath: outputPath})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	records := []types.Record{
		{"id": "1", "name": "Alice", "value": "100"},
		{"id": "2", "name": "Bob", "value": "200"},
		{"id": "3", "name": "Charlie", "value": "300"},
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

	// Close to flush
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Fatal("Output file was not created")
	}

	// Read and verify CSV content
	file, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Should have header + 3 data rows
	if len(rows) != 4 {
		t.Errorf("Expected 4 rows (1 header + 3 data), got %d", len(rows))
	}

	// Verify count
	if s.Count() != len(records) {
		t.Errorf("Count() = %d, want %d", s.Count(), len(records))
	}
}

func TestSink_WithHeaders(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "headers.csv")

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: outputPath,
		Headers:  []string{"id", "name"},
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	records := []types.Record{
		{"id": "1", "name": "Alice", "extra": "ignored"},
		{"id": "2", "name": "Bob", "extra": "ignored"},
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

	// Read and verify headers
	file, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Verify header matches specified order
	if len(rows) > 0 {
		if rows[0][0] != "id" || rows[0][1] != "name" {
			t.Errorf("Header = %v, want [id name]", rows[0])
		}
	}
}

func TestSink_CustomSeparator(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "tab.tsv")

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: outputPath,
		Separator: "\t",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	records := []types.Record{
		{"a": "1", "b": "2"},
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

	// Verify tab separator
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if !strings.Contains(string(content), "\t") {
		t.Error("Output does not contain tab separator")
	}
}

func TestSink_Count(t *testing.T) {
	s := New()

	// Initial count
	if s.Count() != 0 {
		t.Errorf("Initial Count() = %d, want 0", s.Count())
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "count.csv")

	configBytes, _ := json.Marshal(Config{FilePath: outputPath})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	in := make(chan types.Record, 5)
	for i := 0; i < 5; i++ {
		in <- types.Record{"id": string(rune('0' + i))}
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
