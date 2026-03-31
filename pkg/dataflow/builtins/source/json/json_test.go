package json

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestSource_New(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
}

func TestSource_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "default config",
			config: Config{
				FilePath: "/tmp/test.json",
			},
			wantErr: false,
		},
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
			name: "with batch size",
			config: Config{
				FilePath:  "/tmp/test.json",
				Format:    "lines",
				BatchSize: 100,
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

func TestSource_ReadJSONLines(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	jsonlPath := filepath.Join(tmpDir, "test.jsonl")

	// Write JSON Lines content
	content := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
{"id": 3, "name": "Charlie"}
`
	if err := os.WriteFile(jsonlPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: jsonlPath,
		Format:   "lines",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	out := make(chan types.Record, 10)
	ctx := context.Background()

	count, err := s.Read(ctx, out)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if count != 3 {
		t.Errorf("Read() count = %d, want 3", count)
	}

	close(out)

	// Verify records
	records := make([]types.Record, 0, 3)
	for r := range out {
		records = append(records, r)
	}

	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Check first record
	if records[0]["id"].(float64) != 1 {
		t.Errorf("First record id = %v, want 1", records[0]["id"])
	}
	if records[0]["name"] != "Alice" {
		t.Errorf("First record name = %v, want Alice", records[0]["name"])
	}
}

func TestSource_ReadJSONArray(t *testing.T) {
	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "test.json")

	// Write JSON array content
	records := []types.Record{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}
	content, _ := json.Marshal(records)
	if err := os.WriteFile(jsonPath, content, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: jsonPath,
		Format:   "array",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	out := make(chan types.Record, 10)
	ctx := context.Background()

	count, err := s.Read(ctx, out)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if count != 2 {
		t.Errorf("Read() count = %d, want 2", count)
	}
}

func TestSource_ReadWithBatchSize(t *testing.T) {
	tmpDir := t.TempDir()
	jsonlPath := filepath.Join(tmpDir, "batch.jsonl")

	// Create file with 5 records
	var content string
	for i := 1; i <= 5; i++ {
		content += `{"id": ` + string(rune('0'+i)) + `}
`
	}
	if err := os.WriteFile(jsonlPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath:  jsonlPath,
		Format:    "lines",
		BatchSize: 3,
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	out := make(chan types.Record, 10)
	ctx := context.Background()

	count, err := s.Read(ctx, out)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should only read batch size (3)
	if count != 3 {
		t.Errorf("Read() count = %d, want 3 (batch size)", count)
	}
}

func TestSource_FileNotFound(t *testing.T) {
	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: "/nonexistent/path/file.json",
		Format:   "lines",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	out := make(chan types.Record, 10)
	ctx := context.Background()

	_, err := s.Read(ctx, out)
	if err == nil {
		t.Error("Read() should return error for non-existent file")
	}
}

func TestSource_GlobPattern(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple JSON files
	for i := 1; i <= 2; i++ {
		path := filepath.Join(tmpDir, "file"+string(rune('0'+i))+".jsonl")
		content := `{"id": ` + string(rune('0'+i)) + `}
`
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: filepath.Join(tmpDir, "*.jsonl"),
		Format:   "lines",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	out := make(chan types.Record, 10)
	ctx := context.Background()

	count, err := s.Read(ctx, out)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should read from both files
	if count != 2 {
		t.Errorf("Read() count = %d, want 2", count)
	}
}

func TestSource_EmptyLines(t *testing.T) {
	tmpDir := t.TempDir()
	jsonlPath := filepath.Join(tmpDir, "empty.jsonl")

	// Include empty lines
	content := `{"id": 1}

{"id": 2}

`
	if err := os.WriteFile(jsonlPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	s := New()
	configBytes, _ := json.Marshal(Config{
		FilePath: jsonlPath,
		Format:   "lines",
	})
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	out := make(chan types.Record, 10)
	ctx := context.Background()

	count, err := s.Read(ctx, out)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should skip empty lines
	if count != 2 {
		t.Errorf("Read() count = %d, want 2", count)
	}
}

// SliceSource tests

func TestSliceSource_New(t *testing.T) {
	s := NewSliceSource()
	if s == nil {
		t.Fatal("NewSliceSource() returned nil")
	}
}

func TestSliceSource_Init(t *testing.T) {
	records := []types.Record{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}
	configBytes, _ := json.Marshal(map[string]interface{}{
		"records": records,
	})

	s := NewSliceSource()
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if len(s.records) != 2 {
		t.Errorf("Init() records count = %d, want 2", len(s.records))
	}
}

func TestSliceSource_Read(t *testing.T) {
	records := []types.Record{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}
	configBytes, _ := json.Marshal(map[string]interface{}{
		"records": records,
	})

	s := NewSliceSource()
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	out := make(chan types.Record, 10)
	ctx := context.Background()

	count, err := s.Read(ctx, out)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if count != 3 {
		t.Errorf("Read() count = %d, want 3", count)
	}

	close(out)

	var result []types.Record
	for r := range out {
		result = append(result, r)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 records, got %d", len(result))
	}
}
