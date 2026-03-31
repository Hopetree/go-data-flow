// Package dataflow provides a lightweight data pipeline framework for ETL tasks.
// It implements a Source → Processor → Sink pattern using Go channels.
package dataflow

import "context"

// Source is the data source interface.
// Sources read data from external systems and send to the output channel.
type Source[T any] interface {
	// Init initializes the component with JSON configuration.
	// This method only parses config, does NOT establish connections.
	// Returns an error if config parsing or validation fails.
	Init(config []byte) error

	// Read reads data from external system and sends to the output channel.
	// ctx: context for cancellation and timeout
	// out: output channel to send data
	// Returns: number of records sent, error if any
	//
	// Implementation guidelines:
	// - Establish connections in this method, not in Init
	// - Close resources before returning
	// - Check ctx.Done() regularly to support cancellation
	// - Return the count of successfully sent records
	Read(ctx context.Context, out chan<- T) (int64, error)
}
