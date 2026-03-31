package dataflow

import "errors"

// Sentinel errors for common failure cases.
var (
	// ErrComponentNotFound is returned when a component is not registered.
	ErrComponentNotFound = errors.New("component not found")

	// ErrInvalidConfig is returned when configuration is invalid.
	ErrInvalidConfig = errors.New("invalid config")

	// ErrFlowNotBuilt is returned when Run() is called before Build().
	ErrFlowNotBuilt = errors.New("flow not built, call Build() first")

	// ErrFlowAlreadyRunning is returned when Run() is called while flow is running.
	ErrFlowAlreadyRunning = errors.New("flow already running")

	// ErrSourceRequired is returned when source is not configured.
	ErrSourceRequired = errors.New("source is required")

	// ErrSinkRequired is returned when sink is not configured.
	ErrSinkRequired = errors.New("sink is required")

	// ErrFlowNameRequired is returned when flow name is empty.
	ErrFlowNameRequired = errors.New("flow name is required")
)
