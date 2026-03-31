package transform

import (
	"context"
	"encoding/json"
	"reflect"
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
			name: "mapping only",
			config: Config{
				Mapping: map[string]string{"old": "new"},
			},
			wantErr: false,
		},
		{
			name: "add only",
			config: Config{
				Add: map[string]interface{}{"new_field": "value"},
			},
			wantErr: false,
		},
		{
			name: "remove only",
			config: Config{
				Remove: []string{"temp"},
			},
			wantErr: false,
		},
		{
			name: "all operations",
			config: Config{
				Mapping: map[string]string{"a": "b"},
				Add:     map[string]interface{}{"c": 1},
				Remove:  []string{"d"},
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
		name     string
		config   Config
		input    types.Record
		expected types.Record
	}{
		{
			name: "field mapping",
			config: Config{
				Mapping: map[string]string{
					"old_name": "new_name",
				},
			},
			input:    types.Record{"old_name": "value", "keep": "me"},
			expected: types.Record{"new_name": "value", "keep": "me"},
		},
		{
			name: "add fields",
			config: Config{
				Add: map[string]interface{}{
					"processed": true,
					"version":   "1.0",
				},
			},
			input:    types.Record{"id": 1},
			expected: types.Record{"id": 1, "processed": true, "version": "1.0"},
		},
		{
			name: "remove fields",
			config: Config{
				Remove: []string{"secret", "internal"},
			},
			input:    types.Record{"id": 1, "secret": "password", "internal": "data", "public": "ok"},
			expected: types.Record{"id": 1, "public": "ok"},
		},
		{
			name: "mapping and add",
			config: Config{
				Mapping: map[string]string{"value": "score"},
				Add:     map[string]interface{}{"processed": true},
			},
			input:    types.Record{"value": 100, "name": "test"},
			expected: types.Record{"score": 100, "name": "test", "processed": true},
		},
		{
			name: "all operations combined",
			config: Config{
				Mapping: map[string]string{"old": "new"},
				Add:     map[string]interface{}{"added": "value"},
				Remove:  []string{"temp"},
			},
			input:    types.Record{"old": "data", "keep": "this", "temp": "delete"},
			expected: types.Record{"new": "data", "keep": "this", "added": "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			if err := p.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, 1)
			out := make(chan types.Record, 1)

			in <- tt.input
			close(in)

			ctx := context.Background()
			if err := p.Process(ctx, in, out); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			close(out)

			result := <-out

			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("Process() result[%s] = %v, want %v", k, result[k], v)
				}
			}

			for k := range tt.config.Remove {
				if _, exists := result[tt.config.Remove[k]]; exists {
					t.Errorf("Process() should have removed field %s", tt.config.Remove[k])
				}
			}
		})
	}
}

func TestProcessor_ConcurrencyCap(t *testing.T) {
	p := New()
	cap := p.ConcurrencyCap()

	if !cap.Supported {
		t.Error("ConcurrencyCap().Supported should be true")
	}
	if cap.SuggestedMax < 1 {
		t.Error("ConcurrencyCap().SuggestedMax should be at least 1")
	}
	if cap.IsStateful {
		t.Error("ConcurrencyCap().IsStateful should be false")
	}
}

func TestProcessor_Process_Extract(t *testing.T) {
	tests := []struct {
		name          string
		config        Config
		input         types.Record
		expectedCount int
		expected      []types.Record
	}{
		{
			name: "extract simple field",
			config: Config{
				Extract: "data",
			},
			input: types.Record{
				"key":  "243434",
				"data": types.Record{"name": "xxx", "list": []interface{}{1, 2}},
			},
			expectedCount: 1,
			expected: []types.Record{
				{"name": "xxx", "list": []interface{}{1, 2}},
			},
		},
		{
			name: "extract with keep",
			config: Config{
				Extract:     "data",
				ExtractKeep: []string{"trace_id", "timestamp"},
			},
			input: types.Record{
				"trace_id":  "abc",
				"timestamp": "2024-01-01",
				"other":     "ignored",
				"data":      types.Record{"name": "xxx", "value": 100},
			},
			expectedCount: 1,
			expected: []types.Record{
				{"trace_id": "abc", "timestamp": "2024-01-01", "name": "xxx", "value": 100},
			},
		},
		{
			name: "extract nested field",
			config: Config{
				Extract: "payload.user.profile",
			},
			input: types.Record{
				"event": "login",
				"payload": types.Record{
					"user": types.Record{
						"profile": types.Record{"name": "Alice", "age": 30},
					},
				},
			},
			expectedCount: 1,
			expected: []types.Record{
				{"name": "Alice", "age": 30},
			},
		},
		{
			name: "extract non-existent field returns empty",
			config: Config{
				Extract: "missing",
			},
			input: types.Record{
				"key":  "value",
				"data": types.Record{"name": "test"},
			},
			expectedCount: 1,
			expected: []types.Record{
				{},
			},
		},
		{
			name: "extract array without flatten",
			config: Config{
				Extract: "items",
			},
			input: types.Record{
				"order_id": "O001",
				"items":    []interface{}{types.Record{"sku": "A1"}, types.Record{"sku": "B2"}},
			},
			expectedCount: 1,
			expected: []types.Record{
				{"_value": []interface{}{types.Record{"sku": "A1"}, types.Record{"sku": "B2"}}},
			},
		},
		{
			name: "extract array with flatten",
			config: Config{
				Extract:        "items",
				ExtractFlatten: true,
			},
			input: types.Record{
				"order_id": "O001",
				"items":    []interface{}{types.Record{"sku": "A1", "qty": 2}, types.Record{"sku": "B2", "qty": 1}},
			},
			expectedCount: 2,
			expected: []types.Record{
				{"sku": "A1", "qty": 2},
				{"sku": "B2", "qty": 1},
			},
		},
		{
			name: "extract array with flatten and keep",
			config: Config{
				Extract:        "items",
				ExtractFlatten: true,
				ExtractKeep:    []string{"order_id"},
			},
			input: types.Record{
				"order_id": "O001",
				"items":    []interface{}{types.Record{"sku": "A1"}, types.Record{"sku": "B2"}},
			},
			expectedCount: 2,
			expected: []types.Record{
				{"order_id": "O001", "sku": "A1"},
				{"order_id": "O001", "sku": "B2"},
			},
		},
		{
			name: "extract array flatten with non-object elements",
			config: Config{
				Extract:        "values",
				ExtractFlatten: true,
			},
			input: types.Record{
				"id":     1,
				"values": []interface{}{"a", "b", "c"},
			},
			expectedCount: 3,
			expected: []types.Record{
				{"_value": "a"},
				{"_value": "b"},
				{"_value": "c"},
			},
		},
		{
			name: "extract empty array returns empty record",
			config: Config{
				Extract:        "items",
				ExtractFlatten: true,
			},
			input: types.Record{
				"order_id": "O001",
				"items":    []interface{}{},
			},
			expectedCount: 1,
			expected: []types.Record{
				{},
			},
		},
		{
			name: "extract with subsequent mapping and add",
			config: Config{
				Extract: "data",
				Mapping: map[string]string{"old_name": "new_name"},
				Add:     map[string]interface{}{"processed": true},
			},
			input: types.Record{
				"key": "123",
				"data": types.Record{
					"old_name": "value",
					"keep":     "me",
				},
			},
			expectedCount: 1,
			expected: []types.Record{
				{"new_name": "value", "keep": "me", "processed": true},
			},
		},
		{
			name: "extract primitive type returns empty",
			config: Config{
				Extract: "value",
			},
			input: types.Record{
				"value": "hello",
			},
			expectedCount: 1,
			expected: []types.Record{
				{},
			},
		},
		{
			name: "extract nested path not exists",
			config: Config{
				Extract: "a.b.c",
			},
			input: types.Record{
				"a": types.Record{"b": types.Record{}},
			},
			expectedCount: 1,
			expected: []types.Record{
				{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			configBytes, _ := json.Marshal(tt.config)
			if err := p.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, 1)
			out := make(chan types.Record, 10)

			in <- tt.input
			close(in)

			ctx := context.Background()
			if err := p.Process(ctx, in, out); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			close(out)

			var results []types.Record
			for result := range out {
				results = append(results, result)
			}

			if len(results) != tt.expectedCount {
				t.Errorf("Process() got %d records, want %d", len(results), tt.expectedCount)
				return
			}

			for i, expected := range tt.expected {
				for k, v := range expected {
					if !reflect.DeepEqual(results[i][k], v) {
						t.Errorf("Process() result[%d][%s] = %v, want %v", i, k, results[i][k], v)
					}
				}
			}
		})
	}
}
