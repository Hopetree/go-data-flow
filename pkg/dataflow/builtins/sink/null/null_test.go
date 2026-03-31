package null

import (
	"context"
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

	// Null sink should consume all records without error
	// No data is stored, so nothing to verify
}

func TestSink_ConsumeEmpty(t *testing.T) {
	s := New()

	in := make(chan types.Record)
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() with empty channel error = %v", err)
	}
}

func TestSink_ConsumeLargeBatch(t *testing.T) {
	s := New()

	count := 1000
	in := make(chan types.Record, count)
	for i := 0; i < count; i++ {
		in <- types.Record{"id": i, "data": "test data"}
	}
	close(in)

	ctx := context.Background()
	if err := s.Consume(ctx, in); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
}
