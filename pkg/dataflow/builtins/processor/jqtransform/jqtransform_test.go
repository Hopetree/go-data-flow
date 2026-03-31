package jqtransform

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
		query   string
		wantErr bool
	}{
		{
			name:    "simple object construction",
			query:   "{id: .id, name: .name}",
			wantErr: false,
		},
		{
			name:    "select fields",
			query:   "{user_id: .id, user_name: .name}",
			wantErr: false,
		},
		{
			name:    "array map",
			query:   "{total: (.items | map(.price) | add)}",
			wantErr: false,
		},
		{
			name:    "array flatten",
			query:   ".items[]",
			wantErr: false,
		},
		{
			name:    "conditional",
			query:   `if .score >= 60 then {status: "pass"} else {status: "fail"} end`,
			wantErr: false,
		},
		{
			name:    "empty query",
			query:   "",
			wantErr: true,
		},
		{
			name:    "invalid syntax",
			query:   "{id: .id,,}",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			config := Config{Query: tt.query}
			configBytes, _ := json.Marshal(config)
			err := p.Init(configBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// assertEqualValue 比较 interface{} 值，处理 int/float64 类型差异
func assertEqualValue(t *testing.T, got, want interface{}, fieldName string) bool {
	// 处理数值类型比较
	switch g := got.(type) {
	case float64:
		switch w := want.(type) {
		case float64:
			if g != w {
				t.Errorf("%s = %v, want %v", fieldName, got, want)
				return false
			}
			return true
		case int:
			if g != float64(w) {
				t.Errorf("%s = %v, want %v", fieldName, got, want)
				return false
			}
			return true
		}
	case int:
		switch w := want.(type) {
		case float64:
			if float64(g) != w {
				t.Errorf("%s = %v, want %v", fieldName, got, want)
				return false
			}
			return true
		case int:
			if g != w {
				t.Errorf("%s = %v, want %v", fieldName, got, want)
				return false
			}
			return true
		}
	}

	// 其他类型直接比较
	if got != want {
		t.Errorf("%s = %v, want %v", fieldName, got, want)
		return false
	}
	return true
}

func TestProcessor_Process(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		input         types.Record
		expectedCount int
		checkFunc     func(t *testing.T, results []types.Record)
	}{
		{
			name:  "select and rename fields",
			query: "{user_id: .id, user_name: .name, email: .contact.email}",
			input: types.Record{
				"id":      1,
				"name":    "Alice",
				"contact": map[string]interface{}{"email": "alice@example.com"},
				"secret":  "should be dropped",
			},
			expectedCount: 1,
			checkFunc: func(t *testing.T, results []types.Record) {
				assertEqualValue(t, results[0]["user_id"], 1, "user_id")
				if results[0]["user_name"] != "Alice" {
					t.Errorf("user_name = %v, want Alice", results[0]["user_name"])
				}
				if results[0]["email"] != "alice@example.com" {
					t.Errorf("email = %v, want alice@example.com", results[0]["email"])
				}
				if _, exists := results[0]["secret"]; exists {
					t.Error("secret should not exist in result")
				}
			},
		},
		{
			name:  "flatten array",
			query: ".items[]",
			input: types.Record{
				"order_id": "O001",
				"items": []interface{}{
					map[string]interface{}{"sku": "A1", "price": 100},
					map[string]interface{}{"sku": "B2", "price": 200},
				},
			},
			expectedCount: 2,
			checkFunc: func(t *testing.T, results []types.Record) {
				if len(results) != 2 {
					return
				}
				if results[0]["sku"] != "A1" {
					t.Errorf("results[0].sku = %v, want A1", results[0]["sku"])
				}
				if results[1]["sku"] != "B2" {
					t.Errorf("results[1].sku = %v, want B2", results[1]["sku"])
				}
			},
		},
		{
			name:  "calculate total from array",
			query: "{order_id, total: (.items | map(.price) | add), item_count: (.items | length)}",
			input: types.Record{
				"order_id": "O001",
				"items": []interface{}{
					map[string]interface{}{"sku": "A1", "price": float64(100)},
					map[string]interface{}{"sku": "B2", "price": float64(200)},
				},
			},
			expectedCount: 1,
			checkFunc: func(t *testing.T, results []types.Record) {
				if results[0]["order_id"] != "O001" {
					t.Errorf("order_id = %v, want O001", results[0]["order_id"])
				}
				assertEqualValue(t, results[0]["total"], float64(300), "total")
				assertEqualValue(t, results[0]["item_count"], 2, "item_count")
			},
		},
		{
			name:  "conditional transform",
			query: `if .score >= 60 then {name, status: "pass", grade: "A"} else {name, status: "fail", grade: "F"} end`,
			input: types.Record{
				"name":  "Alice",
				"score": 85,
			},
			expectedCount: 1,
			checkFunc: func(t *testing.T, results []types.Record) {
				if results[0]["status"] != "pass" {
					t.Errorf("status = %v, want pass", results[0]["status"])
				}
				if results[0]["grade"] != "A" {
					t.Errorf("grade = %v, want A", results[0]["grade"])
				}
			},
		},
		{
			name:  "filter with empty",
			query: "if .active then {id, name} else empty end",
			input: types.Record{
				"id":     1,
				"name":   "Alice",
				"active": false,
			},
			expectedCount: 0,
		},
		{
			name:  "extract nested field",
			query: "{user: .data.user, meta: .data.metadata}",
			input: types.Record{
				"data": map[string]interface{}{
					"user":     "alice",
					"metadata": map[string]interface{}{"version": "1.0"},
					"internal": "secret",
				},
			},
			expectedCount: 1,
			checkFunc: func(t *testing.T, results []types.Record) {
				if results[0]["user"] != "alice" {
					t.Errorf("user = %v, want alice", results[0]["user"])
				}
			},
		},
		{
			name:  "add computed field",
			query: `. + {processed: true, timestamp: "2024-01-15"}`,
			input: types.Record{
				"id":   1,
				"name": "Alice",
			},
			expectedCount: 1,
			checkFunc: func(t *testing.T, results []types.Record) {
				assertEqualValue(t, results[0]["id"], 1, "id")
				if results[0]["processed"] != true {
					t.Errorf("processed = %v, want true", results[0]["processed"])
				}
				if results[0]["timestamp"] != "2024-01-15" {
					t.Errorf("timestamp = %v, want 2024-01-15", results[0]["timestamp"])
				}
			},
		},
		{
			name:  "select with condition",
			query: `{name, category: (if .price > 100 then "premium" else "standard" end)}`,
			input: types.Record{
				"name":  "Product A",
				"price": float64(150),
			},
			expectedCount: 1,
			checkFunc: func(t *testing.T, results []types.Record) {
				if results[0]["category"] != "premium" {
					t.Errorf("category = %v, want premium", results[0]["category"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			config := Config{Query: tt.query}
			configBytes, _ := json.Marshal(config)
			if err := p.Init(configBytes); err != nil {
				t.Fatalf("Init() error = %v", err)
			}

			in := make(chan types.Record, 1)
			out := make(chan types.Record, 10)

			in <- tt.input
			close(in)

			ctx := context.Background()

			// 在 goroutine 中运行 Process
			done := make(chan error, 1)
			go func() {
				done <- p.Process(ctx, in, out)
			}()

			// 等待 Process 完成
			err := <-done
			if err != nil {
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

			if tt.checkFunc != nil && len(results) > 0 {
				tt.checkFunc(t, results)
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
