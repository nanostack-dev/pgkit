package workflow

import "context"

// Logger is a minimal adapter for workflow runtime logging.
type Logger interface {
	Debug(ctx context.Context, msg string, fields map[string]any)
	Info(ctx context.Context, msg string, fields map[string]any)
	Warn(ctx context.Context, msg string, fields map[string]any)
	Error(ctx context.Context, msg string, fields map[string]any)
}

type noopLogger struct{}

func (noopLogger) Debug(context.Context, string, map[string]any) {}
func (noopLogger) Info(context.Context, string, map[string]any)  {}
func (noopLogger) Warn(context.Context, string, map[string]any)  {}
func (noopLogger) Error(context.Context, string, map[string]any) {}
