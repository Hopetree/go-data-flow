// Package csv 提供从 CSV 文件读取数据的 Source
package csv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Source 从 CSV 文件读取数据
type Source struct {
	filePath    string
	separator   rune
	hasHeader   bool
	skipRows    int
	columnTypes map[string]string // 列名 -> 类型: string, int, float, bool
}

// Config Source 的配置
type Config struct {
	// FilePath CSV 文件路径，支持 glob 模式
	FilePath string `json:"file_path"`
	// Separator 字段分隔符，默认为逗号
	Separator string `json:"separator"`
	// HasHeader 是否有标题行，默认 true
	HasHeader bool `json:"has_header"`
	// SkipRows 跳过的行数（在标题行之前）
	SkipRows int `json:"skip_rows"`
	// ColumnTypes 列类型映射，如 {"id": "int", "price": "float"}
	ColumnTypes map[string]string `json:"column_types"`
}

// New 创建新的 Source
func New() *Source {
	return &Source{
		separator: ',',
		hasHeader: true,
	}
}

// Init 初始化 Source
func (s *Source) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	s.filePath = cfg.FilePath
	s.hasHeader = cfg.HasHeader
	if !s.hasHeader {
		s.hasHeader = true // 默认有标题
	}
	s.skipRows = cfg.SkipRows
	s.columnTypes = cfg.ColumnTypes

	// 解析分隔符
	if cfg.Separator != "" {
		sep := []rune(cfg.Separator)
		if len(sep) > 0 {
			s.separator = sep[0]
		}
	}

	return nil
}

// Read 从 CSV 文件读取数据
func (s *Source) Read(ctx context.Context, out chan<- types.Record) (int64, error) {
	// 获取匹配的文件列表
	files, err := filepath.Glob(s.filePath)
	if err != nil {
		return 0, fmt.Errorf("匹配文件失败: %w", err)
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("没有找到匹配的文件: %s", s.filePath)
	}

	var totalCount int64
	var headers []string

	for _, file := range files {
		count, h, err := s.readFile(ctx, file, headers, out)
		if err != nil {
			return totalCount, err
		}
		if len(headers) == 0 {
			headers = h
		}
		totalCount += count
	}

	return totalCount, nil
}

// readFile 读取单个 CSV 文件
func (s *Source) readFile(ctx context.Context, filePath string, existingHeaders []string, out chan<- types.Record) (int64, []string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = s.separator

	var count int64
	var headers []string

	// 跳过指定行数
	for i := 0; i < s.skipRows; i++ {
		_, err := reader.Read()
		if err != nil {
			return 0, nil, err
		}
	}

	// 读取标题行
	if s.hasHeader {
		row, err := reader.Read()
		if err != nil {
			return 0, nil, fmt.Errorf("读取标题行失败: %w", err)
		}
		headers = row
	} else if len(existingHeaders) > 0 {
		headers = existingHeaders
	}

	// 读取数据行
	for {
		select {
		case <-ctx.Done():
			return count, headers, ctx.Err()
		default:
		}

		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, headers, fmt.Errorf("读取数据行失败: %w", err)
		}

		// 构建记录
		record := make(types.Record)
		for i, value := range row {
			var key string
			if i < len(headers) {
				key = headers[i]
			} else {
				key = fmt.Sprintf("column_%d", i)
			}

			// 类型转换
			record[key] = s.convertValue(key, value)
		}

		select {
		case <-ctx.Done():
			return count, headers, ctx.Err()
		case out <- record:
			count++
		}
	}

	return count, headers, nil
}

// convertValue 根据 columnTypes 配置转换值类型
func (s *Source) convertValue(column, value string) interface{} {
	if s.columnTypes == nil {
		return value
	}

	typ, ok := s.columnTypes[column]
	if !ok {
		return value
	}

	switch typ {
	case "int":
		var v int
		fmt.Sscanf(value, "%d", &v)
		return v
	case "float":
		var v float64
		fmt.Sscanf(value, "%f", &v)
		return v
	case "bool":
		lower := strings.ToLower(value)
		return lower == "true" || lower == "1" || lower == "yes"
	default:
		return value
	}
}

// 确保 Source 实现了 Source 接口
var _ dataflow.Source[types.Record] = (*Source)(nil)
