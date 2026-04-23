package dataflow

// FlowConfig defines the configuration for a Flow.
type FlowConfig struct {
	// Name is the flow name used for logging and monitoring.
	Name string `yaml:"name" json:"name"`
	// BufferSize is the channel buffer size between components.
	BufferSize int `yaml:"buffer_size" json:"buffer_size"`
	// ShutdownTimeout 优雅关闭超时时间（秒）。
	// 收到 SIGINT/SIGTERM 后，停止 source 并等待 channel 中剩余数据被消费完毕。
	// 默认 30 秒。设置为 0 可禁用优雅关闭。
	ShutdownTimeout int `yaml:"shutdown_timeout" json:"shutdown_timeout"`
	// Source is the source component configuration.
	Source ComponentSpec `yaml:"source" json:"source"`
	// Processors is the list of processor configurations (executed in order).
	Processors []ComponentSpec `yaml:"processors" json:"processors"`
	// Sink is the sink component configuration.
	Sink ComponentSpec `yaml:"sink" json:"sink"`
	// ErrorHandling 错误处理配置
	ErrorHandling ErrorHandling `yaml:"error_handling" json:"error_handling"`
}

// ErrorHandling 错误处理配置
type ErrorHandling struct {
	// DLQ 死信队列配置
	DLQ DLQConfig `yaml:"dlq" json:"dlq"`
}

// DLQConfig 死信队列配置
// 启用后，processor/sink 的错误不会中断 flow，而是路由到指定的 sink
type DLQConfig struct {
	// Enabled 是否启用死信队列
	Enabled bool `yaml:"enabled" json:"enabled"`
	// Sink 用于接收错误记录的 sink 组件配置
	Sink ComponentSpec `yaml:"sink" json:"sink"`
}

// ComponentSpec defines a component specification.
type ComponentSpec struct {
	// Name is the component name used for Registry lookup.
	Name string `yaml:"name" json:"name"`
	// Concurrency is the optional concurrency override for processors.
	// Only applies to processors that declare concurrency support.
	//   - nil or not set: use the component's declared default
	//   - 1: force single goroutine (disable concurrency)
	//   - N > 1: use N workers (no upper limit, user decides)
	// Recommended: set based on CPU cores and whether the component is I/O bound.
	// Ignored for source and sink.
	Concurrency *int `yaml:"concurrency" json:"concurrency"`
	// Config is the component-specific configuration passed to Init.
	Config map[string]interface{} `yaml:"config" json:"config"`
}

// SetDefaults sets default values for the configuration.
func (c *FlowConfig) SetDefaults() {
	if c.BufferSize <= 0 {
		c.BufferSize = 100
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30
	}
}

// Validate validates the flow configuration.
func (c *FlowConfig) Validate() error {
	if c.Name == "" {
		return ErrFlowNameRequired
	}
	if c.Source.Name == "" {
		return ErrSourceRequired
	}
	if c.Sink.Name == "" {
		return ErrSinkRequired
	}
	return nil
}

// ValidateBuild validates the configuration during Build.
// This is called internally by Flow.Build().
func (c *FlowConfig) ValidateBuild() error {
	if err := c.Validate(); err != nil {
		return err
	}
	c.SetDefaults()
	return nil
}
