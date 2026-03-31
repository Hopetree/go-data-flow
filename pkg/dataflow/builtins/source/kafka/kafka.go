// Package kafka 提供从 Kafka Topic 消费消息的 Source
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"

	"github.com/yourorg/go-data-flow/pkg/dataflow"
	"github.com/yourorg/go-data-flow/pkg/dataflow/builtins/types"
)

// Source 从 Kafka Topic 消费消息
type Source struct {
	config   *Config
	consumer sarama.ConsumerGroup
	handler  *consumerHandler
	mu       sync.Mutex
}

// Config Source 的配置
type Config struct {
	// Brokers Kafka 集群地址列表
	Brokers []string `json:"brokers"`
	// Topic 消费的 Topic
	Topic string `json:"topic"`
	// GroupID 消费者组 ID
	GroupID string `json:"group_id"`
	// Offset 起始位置: newest (最新) 或 oldest (最早)
	Offset string `json:"offset"`
}

// consumerHandler 实现 sarama.ConsumerGroupHandler
type consumerHandler struct {
	messages chan *sarama.ConsumerMessage
	ready    chan struct{}
}

func newConsumerHandler() *consumerHandler {
	return &consumerHandler{
		messages: make(chan *sarama.ConsumerMessage, 100),
		ready:    make(chan struct{}),
	}
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
		case h.messages <- msg:
			session.MarkMessage(msg, "") // 标记消息已处理
		case <-session.Context().Done():
			return nil
		}
	}
	return nil
}

// New 创建新的 Source
func New() *Source {
	return &Source{}
}

// Init 初始化 Source
func (s *Source) Init(config []byte) error {
	cfg := &Config{
		Offset: "newest", // 默认从最新位置开始
	}

	if err := json.Unmarshal(config, cfg); err != nil {
		return fmt.Errorf("解析配置失败: %w", err)
	}

	if len(cfg.Brokers) == 0 {
		return fmt.Errorf("brokers 不能为空")
	}
	if cfg.Topic == "" {
		return fmt.Errorf("topic 不能为空")
	}
	if cfg.GroupID == "" {
		return fmt.Errorf("group_id 不能为空")
	}

	s.config = cfg

	// 创建 Sarama 配置
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0 // Kafka 版本

	// 设置偏移量
	switch cfg.Offset {
	case "oldest":
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// 创建消费者组
	consumer, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaConfig)
	if err != nil {
		return fmt.Errorf("创建 Kafka 消费者失败: %w", err)
	}

	s.consumer = consumer
	s.handler = newConsumerHandler()

	return nil
}

// Read 读取 Kafka 消息
func (s *Source) Read(ctx context.Context, out chan<- types.Record) (int64, error) {
	var count int64

	// 启动消费者组
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Consume 会阻塞直到发生重平衡或出错
			if err := s.consumer.Consume(ctx, []string{s.config.Topic}, s.handler); err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				select {
				case errCh <- err:
				default:
				}
				return
			}

			// 检查上下文是否已取消
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 等待消费者就绪
	<-s.handler.ready
	log.Printf("[kafka] 已连接到 %v, topic=%s, group=%s", s.config.Brokers, s.config.Topic, s.config.GroupID)

	// 处理消息
	for {
		select {
		case <-ctx.Done():
			// 优雅关闭
			s.Close()
			wg.Wait()
			return count, ctx.Err()

		case err := <-errCh:
			s.Close()
			wg.Wait()
			return count, err

		case msg, ok := <-s.handler.messages:
			if !ok {
				wg.Wait()
				return count, nil
			}

			record := types.Record{
				"key":       string(msg.Key),
				"value":     string(msg.Value),
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"timestamp": msg.Timestamp.Unix(),
			}

			select {
			case out <- record:
				count++
			case <-ctx.Done():
				s.Close()
				wg.Wait()
				return count, ctx.Err()
			}
		}
	}
}

// Close 关闭连接
func (s *Source) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.consumer != nil {
		return s.consumer.Close()
	}
	return nil
}

// 确保 Source 实现了 Source 接口
var _ dataflow.Source[types.Record] = (*Source)(nil)
