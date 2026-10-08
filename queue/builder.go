package queue

import (
	"context"
	"slices"
)

// WorkerBuilder configures a worker and the queues it handles. Methods return
// independent copies; Build validates everything and touches no database.
//
//	worker, err := client.Worker("emails").
//		Pickup(queue.OnEnqueue()).
//		Handle(emails, sendEmail).
//		Handle(receipts, sendReceipt).
//		Build()
type WorkerBuilder struct {
	client        *Client
	config        WorkerConfig
	registrations []func(*HandlerRegistry) error
}

// Worker starts a builder whose worker claims jobs as id.
func (c *Client) Worker(id string) WorkerBuilder {
	return WorkerBuilder{client: c, config: WorkerConfig{WorkerID: id}}
}

// Pickup sets when the worker looks for jobs; see OnEnqueue and PollEvery.
func (b WorkerBuilder) Pickup(pickup Pickup) WorkerBuilder {
	b.config.Pickup = pickup
	return b
}

// Tune sets the remaining worker settings (reaping, visibility, batch size,
// backoff, callbacks). The builder's id and Pickup take precedence over the same
// fields in config.
func (b WorkerBuilder) Tune(config WorkerConfig) WorkerBuilder {
	config.WorkerID, config.Pickup = b.config.WorkerID, b.config.Pickup
	b.config = config
	return b
}

// Handle routes jobs of a typed queue to handler. Malformed JSON fails the job
// without retry.
func (b WorkerBuilder) Handle[P any](queue TypedQueue[P], handler func(context.Context, P, Job) error) WorkerBuilder {
	return b.with(func(registry *HandlerRegistry) error {
		return queue.Register(registry, handler)
	})
}

// HandleRaw routes jobs of queueName to handler with their payload undecoded.
func (b WorkerBuilder) HandleRaw(queueName string, handler Handler) WorkerBuilder {
	return b.with(func(registry *HandlerRegistry) error {
		return registry.Register(queueName, handler)
	})
}

// Build validates the configuration and handlers without starting the worker.
func (b WorkerBuilder) Build() (*Worker, error) {
	registry := NewHandlerRegistry()
	for _, register := range b.registrations {
		if err := register(registry); err != nil {
			return nil, err
		}
	}
	return NewWorker(b.client, registry, b.config)
}

func (b WorkerBuilder) with(register func(*HandlerRegistry) error) WorkerBuilder {
	b.registrations = slices.Concat(b.registrations, []func(*HandlerRegistry) error{register})
	return b
}
