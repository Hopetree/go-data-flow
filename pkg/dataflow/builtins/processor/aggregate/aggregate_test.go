package aggregate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestProcessor_New(t *testing.T) {
	p := New()
	if p == nil {
		t.Fatal("New() returned nil")
	}
}

func TestProcessor_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "count aggregation",
			config: Config{
				GroupBy:    []string{"category"},
				Aggregates: map[string]string{"value": "count"},
			},
			wantErr: false,
		},
		{
			name: "sum aggregation",
			config: Config{
				GroupBy:    []string{"type"},
				Aggregates: map[string]string{"amount": "sum"},
			},
			wantErr: false,
		},
		{
			name: "multiple aggregations",
			config: Config{
				GroupBy:    []string{"region"},
				Aggregates: map[string]string{"sales": "sum", "orders": "count"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			err := p.Init(configBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessor_Process(t *testing.T) {
	tests := []struct {
		name          string
		config        Config
		input         []types.Record
		expectedCount int
	}{
		{
			name: "count by single field",
			config: Config{
				GroupBy:    []string{"category"},
				Aggregates: map[string]string{"id": "count"},
			},
			input: []types.Record{
				{"category": "A", "id": 1},
				{"category": "B", "id": 2},
				{"category": "A", "id": 3},
			},
			expectedCount: 2, // 2 groups: A and B
		},
		{
			name: "count by multiple fields",
			config: Config{
				GroupBy:    []string{"region", "type"},
				Aggregates: map[string]string{"id": "count"},
			},
			input: []types.Record{
				{"region": "US", "type": "A", "id": 1},
				{"region": "US", "type": "B", "id": 2},
				{"region": "EU", "type": "A", "id": 3},
			},
			expectedCount: 3, // 3 groups
		},
		{
			name: "sum aggregation",
			config: Config{
				GroupBy:    []string{"category"},
				Aggregates: map[string]string{"value": "sum"},
			},
			input: []types.Record{
				{"category": "A", "value": 10},
				{"category": "A", "value": 20},
				{"category": "B", "value": 30},
			},
			expectedCount: 2, // 2 groups
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			if err := p.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, len(tt.input))
			out := make(chan types.Record, len(tt.input))

			for _, record := range tt.input {
				in <- record
			}
			close(in)

			ctx := context.Background()
			if err := p.Process(ctx, in, out); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			close(out)

			var count int
			for result := range out {
				// Check that aggregated results have expected fields
				if _, ok := result["_key"]; !ok {
					t.Error("Process() result missing _key field")
				}
				if _, ok := result["_count"]; !ok {
					t.Error("Process() result missing _count field")
				}
				count++
			}

			if count != tt.expectedCount {
				t.Errorf("Process() got %d groups, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestProcessor_ConcurrencyCap(t *testing.T) {
	p := New()
	cap := p.ConcurrencyCap()

	if cap.Supported {
		t.Error("ConcurrencyCap().Supported should be false for stateful processor")
	}
	if cap.SuggestedMax != 1 {
		t.Error("ConcurrencyCap().SuggestedMax should be 1 for stateful processor")
	}
	if !cap.IsStateful {
		t.Error("ConcurrencyCap().IsStateful should be true")
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{int(42), 42.0, true},
		{int64(42), 42.0, true},
		{float32(42.5), 42.5, true},
		{float64(42.5), 42.5, true},
		{"42", 0, false},
		{nil, 0, false},
	}

	for _, tt := range tests {
		result, ok := toFloat64(tt.input)
		if ok != tt.ok {
			t.Errorf("toFloat64(%v) ok = %v, want %v", tt.input, ok, tt.ok)
		}
		if ok && result != tt.expected {
			t.Errorf("toFloat64(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}
