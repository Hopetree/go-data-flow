package dataflow

// FlowConfig defines the configuration for a Flow.
type FlowConfig struct {
	// Name is the flow name used for logging and monitoring.
	Name string `yaml:"name" json:"name"`
	// BufferSize is the channel buffer size between components.
	BufferSize int `yaml:"buffer_size" json:"buffer_size"`
	// Source is the source component configuration.
	Source ComponentSpec `yaml:"source" json:"source"`
	// Processors is the list of processor configurations (executed in order).
	Processors []ComponentSpec `yaml:"processors" json:"processors"`
	// Sink is the sink component configuration.
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
