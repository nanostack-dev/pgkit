package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/nanostack-dev/pgkit/queue"

const (
	jobOutcomeDone    = "done"
	jobOutcomeHandled = "handled"
	jobOutcomeRetry   = "retry"
	jobOutcomeFailed  = "failed"
)

const (
	attrMessagingSystem    = attribute.Key("messaging.system")
	attrMessagingOperation = attribute.Key("messaging.operation.type")
	attrMessagingQueue     = attribute.Key("messaging.destination.name")
	attrMessagingMessageID = attribute.Key("messaging.message.id")
	attrJobAttempt         = attribute.Key("pgkit.queue.job.attempt")
	attrJobMaxAttempts     = attribute.Key("pgkit.queue.job.max_attempts")
	attrJobOutcome         = attribute.Key("pgkit.queue.job.outcome")
)

func injectTraceContext(ctx context.Context) sql.NullString {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return sql.NullString{}
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return sql.NullString{}
	}
	encoded, err := json.Marshal(carrier)
	if err != nil {
		return sql.NullString{}
	}
	return sql.NullString{String: string(encoded), Valid: true}
}

func extractTraceContext(ctx context.Context, stored sql.NullString) context.Context {
	if !stored.Valid {
		return ctx
	}
	var carrier propagation.MapCarrier
	if err := json.Unmarshal([]byte(stored.String), &carrier); err != nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func startJobSpan(ctx context.Context, job *Job) (context.Context, trace.Span) {
	ctx, span := otel.Tracer(tracerName).Start(
		extractTraceContext(ctx, job.traceContext),
		"pgkit.queue "+job.QueueName,
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	if span.IsRecording() {
		span.SetAttributes(
			attrMessagingSystem.String("pgkit"),
			attrMessagingOperation.String("process"),
			attrMessagingQueue.String(job.QueueName),
			attrMessagingMessageID.String(strconv.FormatInt(job.ID, 10)),
			attrJobAttempt.Int(job.Attempts),
			attrJobMaxAttempts.Int(job.MaxAttempts),
		)
	}
	return ctx, span
}

func recordJobOutcome(span trace.Span, outcome string, handlerErr error) {
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attrJobOutcome.String(outcome))
	if handlerErr == nil || outcome == jobOutcomeHandled {
		return
	}
	span.RecordError(handlerErr)
	span.SetStatus(codes.Error, handlerErr.Error())
}
