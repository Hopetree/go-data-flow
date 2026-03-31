// Package clickhouse 提供 ClickHouse 批量写入 Sink
// 支持批量写入、定时刷新和可选的字段映射
package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Hopetree/go-data-flow/pkg/dataflow"
	"github.com/Hopetree/go-data-flow/pkg/dataflow/builtins/types"
	"github.com/Hopetree/go-data-flow/pkg/logger"
)

// 组件注册名称
const TypeName = "sink-clickhouse"

// 默认配置值
const (
	defaultBatchSize       = 1000
	defaultFlushIntervalMs = 1000
	defaultDialTimeoutSec  = 60
	defaultMaxOpenConns    = 10
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 1800 // 30 分钟
)

// Config ClickHouse Sink 配置
type Config struct {
	// ClickHouse 连接配置
	CKAddress  []string `json:"ck_address"`  // ClickHouse 服务器地址列表
	CKUser     string   `json:"ck_user"`     // 用户名
	CKPassword string   `json:"ck_password"` // 密码
	CKDatabase string   `json:"ck_database"` // 数据库名

	// 目标表
	Table string `json:"table"` // 目标表名（必填）

	// 批量写入配置
	BatchSize       int `json:"batch_size"`        // 批量写入大小（默认 1000）
	FlushIntervalMs int `json:"flush_interval_ms"` // 刷新间隔毫秒（默认 1000）

	// 字段映射（可选）
	// key: Record 中的字段名, value: ClickHouse 表中的列名
	// 为空时直接使用 Record 的 key 作为列名
	FieldMapping map[string]string `json:"field_mapping"`

	// 连接调优（可选）
	DialTimeoutSec    int `json:"dial_timeout_sec"`     // 连接超时秒数（默认 60）
	MaxOpenConns      int `json:"max_open_conns"`       // 最大连接数（默认 10）
	MaxIdleConns      int `json:"max_idle_conns"`       // 最大空闲连接数（默认 5）
	ConnMaxLifetimeSec int `json:"conn_max_lifetime_sec"` // 连接最大生命周期秒数（默认 1800）
}

// Sink ClickHouse 批量写入 Sink
type Sink struct {
	// 连接配置
	ckAddress       []string
	ckUser          string
	ckPassword      string
	ckDatabase      string
	dialTimeout     time.Duration
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration

	// 目标表
	table string

	// 批量写入配置
	batchSize     int
	flushInterval time.Duration

	// 字段映射
	fieldMapping   map[string]string
	columnToSource map[string]string // 反向映射: 目标列名 -> 源字段名
	columns        []string          // 最终确定的列顺序

	// 运行时状态
	conn  clickhouse.Conn
	mu    sync.Mutex
	count int64
}

// New 创建新的 ClickHouse Sink
func New() *Sink {
	return &Sink{}
}

// Init 初始化 Sink，解析配置并建立 ClickHouse 连接
func (s *Sink) Init(config []byte) error {
	logger.Debug("[ck-sink] 开始解析配置")

	var cfg Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		logger.Error("[ck-sink] 解析配置失败: %v", err)
		return fmt.Errorf("解析配置失败: %w", err)
	}

	// 验证必填字段
	if len(cfg.CKAddress) == 0 {
		logger.Error("[ck-sink] 配置验证失败: ck_address 不能为空")
		return fmt.Errorf("ck_address 不能为空")
	}
	if cfg.Table == "" {
		logger.Error("[ck-sink] 配置验证失败: table 不能为空")
		return fmt.Errorf("table 不能为空")
	}

	// 应用默认值
	if cfg.CKUser == "" {
		cfg.CKUser = "default"
	}
	if cfg.CKDatabase == "" {
		cfg.CKDatabase = "default"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushIntervalMs <= 0 {
		cfg.FlushIntervalMs = defaultFlushIntervalMs
	}
	if cfg.DialTimeoutSec <= 0 {
		cfg.DialTimeoutSec = defaultDialTimeoutSec
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = defaultMaxOpenConns
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = defaultMaxIdleConns
	}
	if cfg.ConnMaxLifetimeSec <= 0 {
		cfg.ConnMaxLifetimeSec = defaultConnMaxLifetime
	}

	// 保存配置
	s.ckAddress = cfg.CKAddress
	s.ckUser = cfg.CKUser
	s.ckPassword = cfg.CKPassword
	s.ckDatabase = cfg.CKDatabase
	s.table = cfg.Table
	s.batchSize = cfg.BatchSize
	s.flushInterval = time.Duration(cfg.FlushIntervalMs) * time.Millisecond
	s.fieldMapping = cfg.FieldMapping
	s.dialTimeout = time.Duration(cfg.DialTimeoutSec) * time.Second
	s.maxOpenConns = cfg.MaxOpenConns
	s.maxIdleConns = cfg.MaxIdleConns
	s.connMaxLifetime = time.Duration(cfg.ConnMaxLifetimeSec) * time.Second

	// 如果配置了 field_mapping，预计算列顺序和反向映射
	if len(s.fieldMapping) > 0 {
		s.columnToSource = make(map[string]string, len(s.fieldMapping))
		s.columns = make([]string, 0, len(s.fieldMapping))
		for srcKey, targetCol := range s.fieldMapping {
			s.columnToSource[targetCol] = srcKey
			s.columns = append(s.columns, targetCol)
		}
	}

	// 建立 ClickHouse 连接
	conn, err := s.connect()
	if err != nil {
		return fmt.Errorf("连接 ClickHouse 失败: %w", err)
	}
	s.conn = conn

	// 记录配置信息（隐藏密码）
	maskedPassword := "***"
	if s.ckPassword == "" {
		maskedPassword = "(空)"
	}
	logger.Info("[ck-sink] 配置初始化完成:")
	logger.Info("[ck-sink]   ClickHouse 地址: %v", s.ckAddress)
	logger.Info("[ck-sink]   数据库: %s, 用户: %s, 密码: %s", s.ckDatabase, s.ckUser, maskedPassword)
	logger.Info("[ck-sink]   目标表: %s", s.table)
	logger.Info("[ck-sink]   批量大小: %d, 刷新间隔: %v", s.batchSize, s.flushInterval)
	if len(s.fieldMapping) > 0 {
		logger.Info("[ck-sink]   字段映射: %v", s.fieldMapping)
	}

	return nil
}

// connect 建立 ClickHouse 连接
func (s *Sink) connect() (clickhouse.Conn, error) {
	logger.Debug("[ck-sink] 正在连接 ClickHouse: %v, 数据库: %s, 用户: %s",
		s.ckAddress, s.ckDatabase, s.ckUser)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: s.ckAddress,
		Auth: clickhouse.Auth{
			Database: s.ckDatabase,
			Username: s.ckUser,
			Password: s.ckPassword,
		},
		MaxOpenConns:     s.maxOpenConns,
		MaxIdleConns:     s.maxIdleConns,
		ConnMaxLifetime:  s.connMaxLifetime,
		DialTimeout:      s.dialTimeout,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		BlockBufferSize:  10,
	})
	if err != nil {
		logger.Error("[ck-sink] 创建 ClickHouse 连接失败: %v", err)
		return nil, err
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		logger.Error("[ck-sink] Ping ClickHouse 失败: %v", err)
		return nil, fmt.Errorf("ping 失败: %w", err)
	}

	logger.Debug("[ck-sink] ClickHouse 连接测试成功")
	return conn, nil
}

// Consume 消费数据并批量写入 ClickHouse
func (s *Sink) Consume(ctx context.Context, in <-chan types.Record) error {
	batch := make([]types.Record, 0, s.batchSize)

	// 定时刷新计时器
	timer := time.NewTimer(s.flushInterval)
	defer timer.Stop()

	// 重置计时器（正确处理已触发状态）
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(s.flushInterval)
	}

	// flush 将当前批次写入 ClickHouse
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		logger.Debug("[ck-sink] 刷新批次: %d 条记录", len(batch))
		if err := s.writeBatch(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		resetTimer()
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// 上下文取消，尝试刷新剩余数据
			logger.Warn("[ck-sink] 上下文取消，尝试刷新剩余 %d 条记录", len(batch))
			if err := flush(); err != nil {
				return err
			}
			return ctx.Err()

		case <-timer.C:
			// 定时刷新
			if err := flush(); err != nil {
				return err
			}

		case record, ok := <-in:
			if !ok {
				// 通道关闭，刷新剩余数据后退出
				if len(batch) > 0 {
					logger.Info("[ck-sink] 输入通道关闭，刷新剩余 %d 条记录", len(batch))
					if err := flush(); err != nil {
						return err
					}
				}
				logger.Info("[ck-sink] 消费完成，共写入 %d 条记录", s.count)
				return nil
			}

			// 从首条记录推断列顺序（仅当未配置 field_mapping 时）
			if s.columns == nil {
				s.columns = s.extractColumns(record)
				logger.Info("[ck-sink] 从首条记录推断列顺序: %v", s.columns)
			}

			batch = append(batch, record)

			// 达到批量大小，触发刷新
			if len(batch) >= s.batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// writeBatch 将一批记录写入 ClickHouse
func (s *Sink) writeBatch(ctx context.Context, batch []types.Record) error {
	query := s.buildInsertQuery()

	// 准备批量插入
	ckBatch, err := s.conn.PrepareBatch(ctx, query)
	if err != nil {
		logger.Error("[ck-sink] 准备批次失败: %v", err)
		return fmt.Errorf("准备批次失败: %w", err)
	}

	// 逐条追加
	for _, record := range batch {
		values := s.mapRecord(record)
		if err := ckBatch.Append(values...); err != nil {
			logger.Error("[ck-sink] 追加记录失败: %v", err)
			return fmt.Errorf("追加记录失败: %w", err)
		}
	}

	// 发送批次
	if err := ckBatch.Send(); err != nil {
		logger.Error("[ck-sink] 发送批次失败: %v", err)
		return fmt.Errorf("发送批次失败: %w", err)
	}

	// 更新计数
	s.mu.Lock()
	s.count += int64(len(batch))
	s.mu.Unlock()

	logger.Info("[ck-sink] 写入 %d 条记录到 %s.%s", len(batch), s.ckDatabase, s.table)
	logger.Debug("[ck-sink] 累计已写入 %d 条记录", s.count)
	return nil
}

// Close 关闭 ClickHouse 连接
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		if err != nil {
			logger.Error("[ck-sink] 关闭连接失败: %v", err)
			return err
		}
		logger.Debug("[ck-sink] ClickHouse 连接已关闭")
	}
	return nil
}

// Count 返回已写入的记录数
func (s *Sink) Count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// extractColumns 从 Record 中提取列名（按字母排序）
func (s *Sink) extractColumns(record types.Record) []string {
	columns := make([]string, 0, len(record))
	for k := range record {
		columns = append(columns, k)
	}
	sort.Strings(columns)
	return columns
}

// buildInsertQuery 构建 INSERT 语句
func (s *Sink) buildInsertQuery() string {
	if len(s.columns) == 0 {
		return fmt.Sprintf("INSERT INTO %s.%s", s.ckDatabase, s.table)
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s)",
		s.ckDatabase, s.table, strings.Join(s.columns, ", "))
}

// mapRecord 将 Record 映射为按列顺序排列的值切片
func (s *Sink) mapRecord(record types.Record) []interface{} {
	values := make([]interface{}, len(s.columns))
	for i, col := range s.columns {
		if s.columnToSource != nil {
			values[i] = record[s.columnToSource[col]]
		} else {
			values[i] = record[col]
		}
	}
	return values
}

// 编译期接口检查
var _ dataflow.Sink[types.Record] = (*Sink)(nil)
var _ dataflow.Closer = (*Sink)(nil)
