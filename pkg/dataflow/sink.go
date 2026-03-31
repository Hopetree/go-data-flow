package dataflow

import "context"

// Sink is the data sink interface.
// Sinks consume data from input channel and write to external systems.
type Sink[T any] interface {
	// Init initializes the component with JSON configuration.
	// Returns an error if config parsing or validation fails.
	Init(config []byte) error

	// Consume reads data from input channel and writes to external system.
	// ctx: context for cancellation and timeout
	// in: input channel to read from
	// Returns: error if any
	//
	// Implementation guidelines:
	// - Use for data := range in { ... } to iterate input channel
	// - Implement batch writing for better performance
	// - Implement periodic flush to avoid data accumulation
	// - Return when in channel is closed and all data is written
	// - Handle write failures (retry or log error)
	Consume(ctx context.Context, in <-chan T) error
}
