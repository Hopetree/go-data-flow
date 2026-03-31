package collect

import (
	"context"
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
	s := New()
	err := s.Init([]byte("{}"))
	if err != nil {
		t.Errorf("Init() error = %v", err)
	}
}

func TestSink_Consume(t *testing.T) {
	s := New()

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

	data := s.Data()
	if len(data) != len(records) {
		t.Errorf("Data() returned %d records, want %d", len(data), len(records))
	}

	for i, r := range data {
		if r["id"] != records[i]["id"] {
			t.Errorf("Data()[%d][\"id\"] = %v, want %v", i, r["id"], records[i]["id"])
		}
	}
}

func TestSink_Count(t *testing.T) {
	s := New()

	// Initial count should be 0
	if s.Count() != 0 {
		t.Errorf("Initial Count() = %d, want 0", s.Count())
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

func TestSink_Reset(t *testing.T) {
	s := New()

	in := make(chan types.Record, 3)
	for i := 0; i < 3; i++ {
		in <- types.Record{"id": i}
	}
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	if s.Count() != 3 {
		t.Errorf("Count() before reset = %d, want 3", s.Count())
	}

	s.Reset()

	if s.Count() != 0 {
		t.Errorf("Count() after reset = %d, want 0", s.Count())
	}

	data := s.Data()
	if len(data) != 0 {
		t.Errorf("Data() after reset returned %d records, want 0", len(data))
	}
}
