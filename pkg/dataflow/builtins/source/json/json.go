// Package json 提供从 JSON 文件读取数据的 Source
package json

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
)

// Source 从 JSON 文件读取数据，支持 JSON Lines 和 JSON 数组格式
type Source struct {
	filePath  string
	format    string // "lines" 或 "array"
	batchSize int    // 每次读取的批次大小，0 表示无限制
}

// Config Source 的配置
type Config struct {
	// FilePath JSON 文件路径，支持 glob 模式
	FilePath string `json:"file_path"`
	// Format 格式: "lines" (JSON Lines) 或 "array" (JSON 数组)，默认 "lines"
	Format string `json:"format"`
	// BatchSize 每次读取的批次大小，0 表示无限制
	BatchSize int `json:"batch_size"`
}

// New 创建新的 Source
func New() *Source {
	return &Source{
		format: "lines",
	}
}

// Init 初始化 Source
func (s *Source) Init(config []byte) error {
	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	s.filePath = cfg.FilePath
	if cfg.Format != "" {
		s.format = cfg.Format
	}
	s.batchSize = cfg.BatchSize

	return nil
}

// Read 从 JSON 文件读取数据
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

	for _, file := range files {
		count, err := s.readFile(ctx, file, out)
		if err != nil {
			return totalCount, err
		}
		totalCount += count
	}

	return totalCount, nil
}

// readFile 读取单个 JSON 文件
func (s *Source) readFile(ctx context.Context, filePath string, out chan<- types.Record) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	switch s.format {
	case "array":
		return s.readJSONArray(ctx, file, out)
	case "lines":
		fallthrough
	default:
		return s.readJSONLines(ctx, file, out)
	}
}

// readJSONLines 读取 JSON Lines 格式文件
func (s *Source) readJSONLines(ctx context.Context, file *os.File, out chan<- types.Record) (int64, error) {
	scanner := bufio.NewScanner(file)
	var count int64
	batchCount := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record types.Record
		if err := json.Unmarshal(line, &record); err != nil {
			// 跳过无效的 JSON 行
			continue
		}

		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- record:
			count++
			batchCount++

			// 检查批次大小
			if s.batchSize > 0 && batchCount >= s.batchSize {
				return count, nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("读取文件失败: %w", err)
	}

	return count, nil
}

// readJSONArray 读取 JSON 数组格式文件
func (s *Source) readJSONArray(ctx context.Context, file *os.File, out chan<- types.Record) (int64, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return 0, fmt.Errorf("读取文件失败: %w", err)
	}

	var records []types.Record
	if err := json.Unmarshal(data, &records); err != nil {
		return 0, fmt.Errorf("解析 JSON 数组失败: %w", err)
	}

	var count int64
	limit := len(records)
	if s.batchSize > 0 && s.batchSize < limit {
		limit = s.batchSize
	}

	for i := 0; i < limit; i++ {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- records[i]:
			count++
		}
	}

	return count, nil
}

// 确保 Source 实现了 Source 接口
var _ dataflow.Source[types.Record] = (*Source)(nil)

// SliceSource 从内存中的 JSON 数据读取，用于测试
type SliceSource struct {
	records []types.Record
}

// NewSliceSource 创建新的 SliceSource
func NewSliceSource() *SliceSource {
	return &SliceSource{}
}

// Init 初始化 Source
func (s *SliceSource) Init(config []byte) error {
	var cfg struct {
		Records []types.Record `json:"records"`
	}
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}
	s.records = cfg.Records
	return nil
}

// Read 将内存中的 JSON 数据写入输出通道
func (s *SliceSource) Read(ctx context.Context, out chan<- types.Record) (int64, error) {
	var count int64
	for _, record := range s.records {
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case out <- record:
			count++
		}
	}
	return count, nil
}

var _ dataflow.Source[types.Record] = (*SliceSource)(nil)
