// Package csv 提供写入 CSV 文件的 Sink
package csv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Sink 将数据写入 CSV 文件
type Sink struct {
	filePath    string
	separator   rune
	headers     []string
	append      bool
	flushEvery  int
	file        *os.File
	writer      *csv.Writer
	mu          sync.Mutex
	count       int
}

// Config Sink 的配置
type Config struct {
	// FilePath 输出文件路径
	FilePath string `json:"file_path"`
	// Separator 字段分隔符
	Separator string `json:"separator"`
	// Headers 指定列名顺序
	Headers []string `json:"headers"`
	// Append 是否追加模式
	Append bool `json:"append"`
	// FlushEvery 每写入多少条刷新一次
	FlushEvery int `json:"flush_every"`
}

// New 创建新的 Sink
func New() *Sink {
	return &Sink{
		separator:  ',',
		flushEvery: 100,
	}
}

// Init 初始化 Sink
func (s *Sink) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	s.filePath = cfg.FilePath
	s.headers = cfg.Headers
	s.append = cfg.Append
	if cfg.FlushEvery > 0 {
		s.flushEvery = cfg.FlushEvery
	}

	if cfg.Separator != "" {
		sep := []rune(cfg.Separator)
		if len(sep) > 0 {
			s.separator = sep[0]
		}
	}

	return nil
}

// Consume 消费数据并写入 CSV 文件
func (s *Sink) Consume(ctx context.Context, in <-chan types.Record) error {
	flag := os.O_CREATE | os.O_WRONLY
	if s.append {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	file, err := os.OpenFile(s.filePath, flag, 0644)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	s.file = file
	s.writer = csv.NewWriter(file)
	s.writer.Comma = s.separator

	defer s.writer.Flush()

	// 如果预先指定了 headers，先写入标题行
	if len(s.headers) > 0 {
		if err := s.writer.Write(s.headers); err != nil {
			return fmt.Errorf("写入标题行失败: %w", err)
		}
	}

	for record := range in {
		s.mu.Lock()

		if len(s.headers) == 0 {
			s.headers = s.extractHeaders(record)
			if err := s.writer.Write(s.headers); err != nil {
				s.mu.Unlock()
				return fmt.Errorf("写入标题行失败: %w", err)
			}
		}

		row := s.recordToRow(record)
		if err := s.writer.Write(row); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("写入数据行失败: %w", err)
		}

		s.count++

		if s.count%s.flushEvery == 0 {
			s.writer.Flush()
		}

		s.mu.Unlock()
	}

	return nil
}

// Close 关闭文件
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer != nil {
		s.writer.Flush()
	}
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}

// Count 返回已写入的记录数
func (s *Sink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// extractHeaders 从记录中提取标题
func (s *Sink) extractHeaders(record types.Record) []string {
	headers := make([]string, 0, len(record))
	for k := range record {
		headers = append(headers, k)
	}
	sort.Strings(headers)
	return headers
}

// recordToRow 将记录转换为 CSV 行
func (s *Sink) recordToRow(record types.Record) []string {
	row := make([]string, len(s.headers))
	for i, h := range s.headers {
		if v, ok := record[h]; ok {
			row[i] = fmt.Sprintf("%v", v)
		}
	}
	return row
}

var _ dataflow.Sink[types.Record] = (*Sink)(nil)
var _ dataflow.Closer = (*Sink)(nil)
