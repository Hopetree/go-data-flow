// Package json 提供写入 JSON 文件的 Sink
package json

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// Sink 将数据写入 JSON 文件，支持 JSON Lines 和 JSON 数组格式
type Sink struct {
	filePath   string
	format     string // "lines" 或 "array"
	indent     string
	append     bool
	file       *os.File
	encoder    *json.Encoder
	arrayFirst bool
	mu         sync.Mutex
	count      int
}

// Config Sink 的配置
type Config struct {
	// FilePath 输出文件路径
	FilePath string `json:"file_path"`
	// Format 格式: "lines" 或 "array"
	Format string `json:"format"`
	// Indent 缩进字符串
	Indent string `json:"indent"`
	// Append 是否追加模式
	Append bool `json:"append"`
}

// New 创建新的 Sink
func New() *Sink {
	return &Sink{
		format:     "lines",
		arrayFirst: true,
	}
}

// Init 初始化 Sink
func (s *Sink) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	s.filePath = cfg.FilePath
	if cfg.Format != "" {
		s.format = cfg.Format
	}
	s.indent = cfg.Indent
	s.append = cfg.Append

	return nil
}

// Consume 消费数据并写入 JSON 文件
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

	s.encoder = json.NewEncoder(file)
	if s.indent != "" {
		s.encoder.SetIndent("", s.indent)
	}

	if s.format == "array" {
		if _, err := file.WriteString("[\n"); err != nil {
			return fmt.Errorf("写入数组开始失败: %w", err)
		}
		s.arrayFirst = true
	}

	defer s.closeFile()

	for record := range in {
		s.mu.Lock()

		if s.format == "array" {
			if !s.arrayFirst {
				if _, err := file.WriteString(",\n"); err != nil {
					s.mu.Unlock()
					return fmt.Errorf("写入逗号失败: %w", err)
				}
			}
			s.arrayFirst = false

			if s.indent == "" {
				if _, err := file.WriteString("  "); err != nil {
					s.mu.Unlock()
					return fmt.Errorf("写入缩进失败: %w", err)
				}
			}
		}

		if err := s.encoder.Encode(record); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("写入记录失败: %w", err)
		}

		s.count++
		s.mu.Unlock()
	}

	return nil
}

// closeFile 关闭文件并完成数组格式
func (s *Sink) closeFile() {
	if s.file == nil {
		return
	}

	if s.format == "array" {
		if _, err := s.file.WriteString("\n]"); err != nil {
			logger.Warn("[json-sink] 写入数组结束失败: %v", err)
		}
	}

	s.file.Close()
	s.file = nil
}

// Close 关闭 Sink
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closeFile()
	return nil
}

// Count 返回已写入的记录数
func (s *Sink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// WriterSink 通用的 JSON Writer Sink
type WriterSink struct {
	writer  io.Writer
	encoder *json.Encoder
	mu      sync.Mutex
	count   int
}

// NewWriterSink 创建新的 WriterSink
func NewWriterSink(writer io.Writer) *WriterSink {
	return &WriterSink{
		writer:  writer,
		encoder: json.NewEncoder(writer),
	}
}

// Init 初始化 Sink
func (s *WriterSink) Init(config []byte) error {
	return nil
}

// Consume 消费数据
func (s *WriterSink) Consume(ctx context.Context, in <-chan types.Record) error {
	for record := range in {
		s.mu.Lock()
		if err := s.encoder.Encode(record); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("写入记录失败: %w", err)
		}
		s.count++
		s.mu.Unlock()
	}
	return nil
}

// Count 返回已写入的记录数
func (s *WriterSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

var _ dataflow.Sink[types.Record] = (*Sink)(nil)
var _ dataflow.Sink[types.Record] = (*WriterSink)(nil)
var _ dataflow.Closer = (*Sink)(nil)
