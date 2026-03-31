// Package csv_test 提供 CSV Source 的单元测试
package csv_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	csv "github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/source/csv"
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
				"file_path": "./test.csv",
				"separator": ",",
				"has_header": true
			}`,
			wantErr: false,
		},
		{
			name: "with column types",
			config: `{
				"file_path": "./test.csv",
				"column_types": {
					"id": "int",
					"price": "float"
				}
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
			s := csv.New()
			err := s.Init([]byte(tt.config))
			if (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSource_Read(t *testing.T) {
	// 创建临时 CSV 文件
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.csv")

	content := `id,name,score
1,Alice,95
2,Bob,72
3,Charlie,88
`
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	s := csv.New()
	config := `{
		"file_path": "` + tmpFile + `",
		"separator": ",",
		"has_header": true
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

func TestSource_ReadWithTypeConversion(t *testing.T) {
	// 创建临时 CSV 文件
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.csv")

	content := `id,price,active
1,19.99,true
2,29.50,false
`
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	s := csv.New()
	config := `{
		"file_path": "` + tmpFile + `",
		"separator": ",",
		"has_header": true,
		"column_types": {
			"id": "int",
			"price": "float",
			"active": "bool"
		}
	}`

	if err := s.Init([]byte(config)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	out := make(chan types.Record, 10)
	count, err := s.Read(context.Background(), out)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if count != 2 {
		t.Errorf("Read() count = %d, want 2", count)
	}

	// 验证类型转换（只检查第一条记录）
	r, ok := <-out
	if !ok {
		t.Fatal("通道中无记录")
	}
	// id 应该是 int 类型
	if _, ok := r["id"].(int); !ok {
		t.Errorf("id type = %T, want int", r["id"])
	}
	// price 应该是 float64 类型
	if _, ok := r["price"].(float64); !ok {
		t.Errorf("price type = %T, want float64", r["price"])
	}
	// active 应该是 bool 类型
	if _, ok := r["active"].(bool); !ok {
		t.Errorf("active type = %T, want bool", r["active"])
	}
}

func TestSource_FileNotFound(t *testing.T) {
	s := csv.New()
	config := `{
		"file_path": "/nonexistent/path/file.csv"
	}`

	if err := s.Init([]byte(config)); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	out := make(chan types.Record, 10)
	_, err := s.Read(context.Background(), out)
	if err == nil {
		t.Error("Read() should return error for non-existent file")
	}
}

func TestSource_ReadWithCancel(t *testing.T) {
	// 创建临时 CSV 文件
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.csv")

	content := `id,name
1,Alice
2,Bob
3,Charlie
`
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	s := csv.New()
	config := `{
		"file_path": "` + tmpFile + `"
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
