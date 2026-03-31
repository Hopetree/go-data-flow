package condition

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

func TestCompareContains(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"contains", "hello world", "world", true},
		{"not contains", "hello world", "foo", false},
		{"empty substring", "hello", "", true},
		{"empty string", "", "hello", false},
		{"case sensitive", "Hello", "hello", false},
		{"prefix", "hello world", "hello", true},
		{"suffix", "hello world", "world", true},
		{"a is not string", 123, "23", false},
		{"b is not string", "hello", 123, false},
		{"both not string", 123, 23, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareContains(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareContains(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestComparePrefix(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"has prefix", "hello world", "hello", true},
		{"not prefix", "hello world", "world", false},
		{"empty prefix", "hello", "", true},
		{"exact match", "hello", "hello", true},
		{"case sensitive", "Hello", "hello", false},
		{"a is not string", 123, "12", false},
		{"b is not string", "hello", 123, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := comparePrefix(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("comparePrefix(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareSuffix(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
	}{
		{"has suffix", "hello world", "world", true},
		{"not suffix", "hello world", "hello", false},
		{"empty suffix", "hello", "", true},
		{"exact match", "hello", "hello", true},
		{"case sensitive", "Hello", "hello", false},
		{"a is not string", 123, "23", false},
		{"b is not string", "hello", 123, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareSuffix(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareSuffix(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestProcessor_Process_StringOps(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		input    []types.Record
		expected int
	}{
		// contains
		{
			name: "contains match",
			config: Config{
				Field: "message",
				Op:    "contains",
				Value: "error",
			},
			input: []types.Record{
				{"message": "error: connection failed"},
				{"message": "success"},
				{"message": "ERROR: timeout"}, // case sensitive, not matched
			},
			expected: 1,
		},
		{
			name: "contains empty string",
			config: Config{
				Field: "message",
				Op:    "contains",
				Value: "",
			},
			input: []types.Record{
				{"message": "hello"},
				{"message": ""},
			},
			expected: 2,
		},
		// prefix
		{
			name: "prefix match",
			config: Config{
				Field: "path",
				Op:    "prefix",
				Value: "/api/",
			},
			input: []types.Record{
				{"path": "/api/users"},
				{"path": "/api/products"},
				{"path": "/web/index"},
			},
			expected: 2,
		},
		{
			name: "prefix user-",
			config: Config{
				Field: "id",
				Op:    "prefix",
				Value: "user-",
			},
			input: []types.Record{
				{"id": "user-123"},
				{"id": "user-456"},
				{"id": "admin-1"},
				{"id": "guest"},
			},
			expected: 2,
		},
		// suffix
		{
			name: "suffix match",
			config: Config{
				Field: "email",
				Op:    "suffix",
				Value: "@example.com",
			},
			input: []types.Record{
				{"email": "user1@example.com"},
				{"email": "user2@example.com"},
				{"email": "user3@other.org"},
			},
			expected: 2,
		},
		{
			name: "suffix .json",
			config: Config{
				Field: "filename",
				Op:    "suffix",
				Value: ".json",
			},
			input: []types.Record{
				{"filename": "config.json"},
				{"filename": "data.yaml"},
				{"filename": "settings.json"},
			},
			expected: 2,
		},
		// field not exists
		{
			name: "contains field not exists",
			config: Config{
				Field: "missing",
				Op:    "contains",
				Value: "test",
			},
			input: []types.Record{
				{"other": "value"},
			},
			expected: 0,
		},
		{
			name: "prefix field not exists",
			config: Config{
				Field: "missing",
				Op:    "prefix",
				Value: "test",
			},
			input: []types.Record{
				{"other": "value"},
			},
			expected: 0,
		},
		{
			name: "suffix field not exists",
			config: Config{
				Field: "missing",
				Op:    "suffix",
				Value: "test",
			},
			input: []types.Record{
				{"other": "value"},
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
