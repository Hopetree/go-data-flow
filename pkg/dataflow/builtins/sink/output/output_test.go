package output

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
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
			name:    "default config",
			config:  Config{},
			wantErr: false,
		},
		{
			name: "json format",
			config: Config{
				Format: "json",
			},
			wantErr: false,
		},
		{
			name: "with limit",
			config: Config{
				Format: "json",
				Limit:  10,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			configBytes, err := json.Marshal(tt.config)
			if err != nil {
				t.Fatalf("json.Marshal 失败: %v", err)
			}
			err = s.Init(configBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSink_Consume(t *testing.T) {
	tests := []struct {
		name       string
		config     Config
		input      []types.Record
		expectLast int
	}{
		{
			name:   "consume all records",
			config: Config{Format: "json"},
			input: []types.Record{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
			expectLast: 2,
		},
		{
			name:   "with limit",
			config: Config{Format: "json", Limit: 2},
			input: []types.Record{
				{"id": 1},
				{"id": 2},
				{"id": 3},
				{"id": 4},
			},
			expectLast: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Redirect stdout
			old := os.Stdout
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe() 失败: %v", err)
			}
			os.Stdout = w

			s := New()
			configBytes, err := json.Marshal(tt.config)
			if err != nil {
				t.Fatalf("json.Marshal 失败: %v", err)
			}
			if err := s.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, len(tt.input))
			for _, record := range tt.input {
				in <- record
			}
			close(in)

			ctx := context.Background()
			if err := s.Consume(ctx, in); err != nil {
				t.Fatalf("Consume() error = %v", err)
			}

			// Restore stdout and read output
			w.Close()
			os.Stdout = old
			if _, err := io.Copy(io.Discard, r); err != nil {
				t.Logf("io.Copy 失败: %v", err)
			}

			if s.Count() != tt.expectLast {
				t.Errorf("Count() = %d, want %d", s.Count(), tt.expectLast)
			}
		})
	}
}

func TestSink_JSONOutput(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() 失败: %v", err)
	}
	os.Stdout = w

	s := New()
	configBytes, err := json.Marshal(Config{Format: "json"})
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	in := make(chan types.Record, 1)
	in <- types.Record{"id": 1, "name": "test"}
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	// Restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy 失败: %v", err)
	}

	// Verify output is valid JSON
	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Errorf("Output is not valid JSON: %v", err)
	}
}

func TestSink_Count(t *testing.T) {
	s := New()

	// Initial count should be 0
	if s.Count() != 0 {
		t.Errorf("Initial Count() = %d, want 0", s.Count())
	}

	// Redirect stdout
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() 失败: %v", err)
	}
	os.Stdout = w

	configBytes, err := json.Marshal(Config{Format: "json"})
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	if err := s.Init(configBytes); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	in := make(chan types.Record, 3)
	for i := 0; i < 3; i++ {
		in <- types.Record{"id": i}
	}
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	// Restore stdout
	w.Close()
	os.Stdout = old
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Logf("io.Copy 失败: %v", err)
	}

	if s.Count() != 3 {
		t.Errorf("Count() = %d, want 3", s.Count())
	}
}
