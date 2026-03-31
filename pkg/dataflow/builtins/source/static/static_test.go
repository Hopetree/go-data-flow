package static

import (
	"context"
	"testing"

	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

func TestSource_Init(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "valid config",
			config: `{
				"data": [
					{"id": 1, "name": "Alice"},
					{"id": 2, "name": "Bob"}
				]
			}`,
			wantErr: false,
		},
		{
			name: "empty data",
			config: `{
				"data": []
			}`,
			wantErr: false,
		},
		{
			name:    "invalid json",
			config:  `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			err := s.Init([]byte(tt.config))
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSource_Read(t *testing.T) {
	s := New()
	config := `{
		"data": [
			{"id": 1, "name": "Alice", "score": 95},
			{"id": 2, "name": "Bob", "score": 72},
			{"id": 3, "name": "Charlie", "score": 88}
		]
	}`

	if err := s.Init([]byte(config)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	out := make(chan types.Record, 10)
	count, err := s.Read(context.Background(), out)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Read() count = %d, want 3", count)
	}

	// 验证读取的数据
	close(out)
	var records []types.Record
	for r := range out {
		records = append(records, r)
	}

	if len(records) != 3 {
		t.Errorf("got %d records, want 3", len(records))
	}

	// 验证第一条记录
	if records[0]["name"] != "Alice" {
		t.Errorf("first record name = %v, want Alice", records[0]["name"])
	}
}

func TestSource_ReadWithCancel(t *testing.T) {
	s := New()
	config := `{
		"data": [
			{"id": 1},
			{"id": 2},
			{"id": 3}
		]
	}`

	if err := s.Init([]byte(config)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	out := make(chan types.Record, 10)
	_, err := s.Read(ctx, out)
	if err != context.Canceled {
		t.Errorf("Read() error = %v, want context.Canceled", err)
	}
}
