package generator

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

func TestSource_Init(t *testing.T) {
	tests := []struct {
		name       string
		config     string
		wantCount  int
		wantInterv int
		wantErr    bool
	}{
		{
			name:       "default values",
			config:     `{}`,
			wantCount:  0,
			wantInterv: 0,
			wantErr:    false,
		},
		{
			name: "with count",
			config: `{
				"count": 100
			}`,
			wantCount:  100,
			wantInterv: 0,
			wantErr:    false,
		},
		{
			name: "with interval",
			config: `{
				"count": 10,
				"interval_ms": 100
			}`,
			wantCount:  10,
			wantInterv: 100,
			wantErr:    false,
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
				return
			}
			if !tt.wantErr {
				if s.count != tt.wantCount {
					t.Errorf("count = %d, want %d", s.count, tt.wantCount)
				}
				if s.intervalMs != tt.wantInterv {
					t.Errorf("intervalMs = %d, want %d", s.intervalMs, tt.wantInterv)
				}
			}
		})
	}
}

func TestSource_Read(t *testing.T) {
	s := New()
	config := `{
		"count": 5
	}`

	if err := s.Init([]byte(config)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	out := make(chan types.Record, 10)
	count, err := s.Read(context.Background(), out)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if count != 5 {
		t.Errorf("Read() count = %d, want 5", count)
	}

	// 验证读取的数据
	close(out)
	var records []types.Record
	for r := range out {
		records = append(records, r)
	}

	if len(records) != 5 {
		t.Errorf("got %d records, want 5", len(records))
	}

	// 验证 ID 序列
	for i, r := range records {
		if r["id"] != i {
			t.Errorf("record[%d].id = %v, want %d", i, r["id"], i)
		}
	}
}

func TestSource_ReadWithInterval(t *testing.T) {
	s := New()
	config := `{
		"count": 3,
		"interval_ms": 10
	}`

	if err := s.Init([]byte(config)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	start := time.Now()
	out := make(chan types.Record, 10)
	count, err := s.Read(context.Background(), out)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Read() count = %d, want 3", count)
	}

	// 验证间隔时间 (3条记录 = 2个间隔 = 至少 20ms)
	if elapsed < 20*time.Millisecond {
		t.Errorf("elapsed = %v, expected at least 20ms", elapsed)
	}
}

func TestSource_ReadWithCancel(t *testing.T) {
	s := New()
	// count = 0 表示无限生成
	config := `{
		"count": 0,
		"interval_ms": 100
	}`

	if err := s.Init([]byte(config)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	out := make(chan types.Record, 100)
	_, err := s.Read(ctx, out)
	if err != context.DeadlineExceeded {
		t.Errorf("Read() error = %v, want context.DeadlineExceeded", err)
	}
}
