package condition

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

func TestProcessor_Process_Gt(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "gt integers",
			config: Config{
				Field: "age",
				Op:    "gt",
				Value: 18,
			},
			input: []types.Record{
				{"age": 15, "name": "young"},
				{"age": 18, "name": "exact"},
				{"age": 25, "name": "adult"},
				{"age": 30, "name": "older"},
			},
			expected: 2,
		},
		{
			name: "gt float64",
			config: Config{
				Field: "price",
				Op:    "gt",
				Value: 99.5,
			},
			input: []types.Record{
				{"price": 50.0},
				{"price": 99.5},
				{"price": 100.0},
				{"price": 199.9},
			},
			expected: 2,
		},
		{
			name: "gt int vs float64 data",
			config: Config{
				Field: "score",
				Op:    "gt",
				Value: 50, // int
			},
			input: []types.Record{
				{"score": 50.0},  // float64, not > 50
				{"score": 50},    // int, not > 50
				{"score": 60.0},  // float64, > 50
				{"score": 40.0},  // float64, not > 50
			},
			expected: 1,
		},
		{
			name: "gt strings",
			config: Config{
				Field: "name",
				Op:    "gt",
				Value: "m",
			},
			input: []types.Record{
				{"name": "alice"},
				{"name": "bob"},
				{"name": "mike"},  // "mike" > "m" (prefix comparison)
				{"name": "zack"},
			},
			expected: 2, // "mike" and "zack" > "m"
		},
		{
			name: "gt negative numbers",
			config: Config{
				Field: "temp",
				Op:    "gt",
				Value: -10,
			},
			input: []types.Record{
				{"temp": -20},
				{"temp": -10},
				{"temp": 0},
				{"temp": 10},
			},
			expected: 2,
		},
		{
			name: "gt field not exists",
			config: Config{
				Field: "missing",
				Op:    "gt",
				Value: 0,
			},
			input: []types.Record{
				{"other": 1},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, err := json.Marshal(tt.config)
			if err != nil {
				t.Fatalf("json.Marshal 失败: %v", err)
			}
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
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_Gte(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "gte integers",
			config: Config{
				Field: "age",
				Op:    "gte",
				Value: 18,
			},
			input: []types.Record{
				{"age": 15},
				{"age": 18},
				{"age": 25},
			},
			expected: 2,
		},
		{
			name: "gte with equal value",
			config: Config{
				Field: "count",
				Op:    "gte",
				Value: 100,
			},
			input: []types.Record{
				{"count": 99},
				{"count": 100},
				{"count": 101},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, err := json.Marshal(tt.config)
			if err != nil {
				t.Fatalf("json.Marshal 失败: %v", err)
			}
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
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_Lt(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "lt integers",
			config: Config{
				Field: "age",
				Op:    "lt",
				Value: 18,
			},
			input: []types.Record{
				{"age": 15},
				{"age": 18},
				{"age": 25},
			},
			expected: 1,
		},
		{
			name: "lt negative numbers",
			config: Config{
				Field: "temp",
				Op:    "lt",
				Value: 0,
			},
			input: []types.Record{
				{"temp": -10},
				{"temp": 0},
				{"temp": 10},
			},
			expected: 1,
		},
		{
			name: "lt strings",
			config: Config{
				Field: "name",
				Op:    "lt",
				Value: "m",
			},
			input: []types.Record{
				{"name": "alice"},
				{"name": "bob"},
				{"name": "mike"},
				{"name": "zack"},
			},
			expected: 2, // "alice" and "bob" < "m"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, err := json.Marshal(tt.config)
			if err != nil {
				t.Fatalf("json.Marshal 失败: %v", err)
			}
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
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_Lte(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "lte integers",
			config: Config{
				Field: "age",
				Op:    "lte",
				Value: 18,
			},
			input: []types.Record{
				{"age": 15},
				{"age": 18},
				{"age": 25},
			},
			expected: 2,
		},
		{
			name: "lte with equal value",
			config: Config{
				Field: "count",
				Op:    "lte",
				Value: 100,
			},
			input: []types.Record{
				{"count": 99},
				{"count": 100},
				{"count": 101},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, err := json.Marshal(tt.config)
			if err != nil {
				t.Fatalf("json.Marshal 失败: %v", err)
			}
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
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}
