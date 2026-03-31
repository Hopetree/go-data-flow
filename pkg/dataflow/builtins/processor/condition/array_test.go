package condition

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestProcessor_Process_In(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "in with integers",
			config: Config{
				Field: "id",
				Op:    "in",
				Value: []interface{}{1, 3, 5},
			},
			input: []types.Record{
				{"id": 1, "name": "one"},
				{"id": 2, "name": "two"},
				{"id": 3, "name": "three"},
				{"id": 4, "name": "four"},
				{"id": 5, "name": "five"},
			},
			expected: 3,
		},
		{
			name: "in with strings",
			config: Config{
				Field: "status",
				Op:    "in",
				Value: []interface{}{"active", "pending"},
			},
			input: []types.Record{
				{"status": "active"},
				{"status": "inactive"},
				{"status": "pending"},
				{"status": "deleted"},
			},
			expected: 2,
		},
		{
			name: "in with mixed types",
			config: Config{
				Field: "value",
				Op:    "in",
				Value: []interface{}{1, 2.0, "three"},
			},
			input: []types.Record{
				{"value": 1},   // int, matches
				{"value": 2},   // int, should match 2.0
				{"value": 2.0}, // float64, matches
				{"value": "three"},
				{"value": 4},
			},
			expected: 4,
		},
		{
			name: "in with empty array",
			config: Config{
				Field: "id",
				Op:    "in",
				Value: []interface{}{},
			},
			input: []types.Record{
				{"id": 1},
				{"id": 2},
			},
			expected: 0,
		},
		{
			name: "in field not exists",
			config: Config{
				Field: "missing",
				Op:    "in",
				Value: []interface{}{1, 2, 3},
			},
			input: []types.Record{
				{"id": 1},
			},
			expected: 0,
		},
		{
			name: "in with float config matching int data",
			config: Config{
				Field: "count",
				Op:    "in",
				Value: []interface{}{1.0, 2.0, 3.0},
			},
			input: []types.Record{
				{"count": 1}, // int, should match 1.0
				{"count": 2}, // int, should match 2.0
				{"count": 4}, // int, no match
			},
			expected: 2,
		},
		{
			name: "in with single value (not array)",
			config: Config{
				Field: "status",
				Op:    "in",
				Value: "active", // 单个值，不是数组
			},
			input: []types.Record{
				{"status": "active"},
				{"status": "inactive"},
			},
			expected: 1,
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
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_Nin(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		{
			name: "nin with integers",
			config: Config{
				Field: "id",
				Op:    "nin",
				Value: []interface{}{1, 3, 5},
			},
			input: []types.Record{
				{"id": 1, "name": "one"},
				{"id": 2, "name": "two"},
				{"id": 3, "name": "three"},
				{"id": 4, "name": "four"},
				{"id": 5, "name": "five"},
			},
			expected: 2, // only 2 and 4 not in [1, 3, 5]
		},
		{
			name: "nin field not exists - should pass",
			config: Config{
				Field: "missing",
				Op:    "nin",
				Value: []interface{}{1, 2, 3},
			},
			input: []types.Record{
				{"id": 1},
			},
			expected: 1, // 字段不存在时，nin 返回 true
		},
		{
			name: "nin with empty array - all pass",
			config: Config{
				Field: "id",
				Op:    "nin",
				Value: []interface{}{},
			},
			input: []types.Record{
				{"id": 1},
				{"id": 2},
			},
			expected: 2,
		},
		{
			name: "nin with strings",
			config: Config{
				Field: "status",
				Op:    "nin",
				Value: []interface{}{"deleted", "archived"},
			},
			input: []types.Record{
				{"status": "active"},
				{"status": "deleted"},
				{"status": "pending"},
				{"status": "archived"},
			},
			expected: 2,
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
			for range out {
				count++
			}

			if count != tt.expected {
				t.Errorf("Process() got %d records, want %d", count, tt.expected)
			}
		})
	}
}
