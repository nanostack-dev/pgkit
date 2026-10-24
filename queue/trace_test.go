package queue

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func installTracing(t *testing.T) (*tracetest.SpanRecorder, trace.Tracer) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return recorder, provider.Tracer("test")
}

func consumerSpans(recorder *tracetest.SpanRecorder, queueName string) []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "pgkit.queue "+queueName {
			spans = append(spans, span)
		}
	}
	return spans
}

func spanAttribute(span sdktrace.ReadOnlySpan, key attribute.Key) attribute.Value {
	for _, kv := range span.Attributes() {
		if kv.Key == key {
			return kv.Value
		}
	}
	return attribute.Value{}
}

type handlerSpans struct {
	mu       sync.Mutex
	contexts []trace.SpanContext
}

func (h *handlerSpans) record(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.contexts = append(h.contexts, trace.SpanContextFromContext(ctx))
}

func (h *handlerSpans) all() []trace.SpanContext {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]trace.SpanContext(nil), h.contexts...)
}

func runTracedWorker(t *testing.T, q *Client, queueName string, handler Handler) {
	t.Helper()
	worker, err := q.Worker(t.Name()).
		Pickup(PollEvery(20*time.Millisecond)).
		Tune(WorkerConfig{BackoffBase: time.Millisecond, BackoffMax: time.Millisecond, RetryDelay: func(Job, error) time.Duration { return 0 }}).
		HandleRaw(queueName, handler).
		Build()
	if err != nil {
		t.Fatalf("build worker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = worker.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func TestWorkerContinuesTheEnqueuersTrace(t *testing.T) {
	recorder, tracer := installTracing(t)
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)

	requestCtx, request := tracer.Start(context.Background(), "POST /orders")
	id, err := q.Enqueue(requestCtx, EnqueueParams{QueueName: queueName, Payload: []byte(`{}`)})
	request.End()
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var seen handlerSpans
	runTracedWorker(t, q, queueName, func(ctx context.Context, _ Job) error {
		seen.record(ctx)
		return nil
	})
	requireEventually(t, 5*time.Second, 20*time.Millisecond, func() bool { return len(consumerSpans(recorder, queueName)) == 1 })

	span := consumerSpans(recorder, queueName)[0]
	parent := request.SpanContext()
	if span.SpanContext().TraceID() != parent.TraceID() || span.Parent().SpanID() != parent.SpanID() {
		t.Fatalf("consumer span %v has parent %v, want child of %v", span.SpanContext(), span.Parent(), parent)
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("span kind = %v, want consumer", span.SpanKind())
	}
	if got := seen.all(); len(got) != 1 || got[0].SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("handler ran under %v, want the consumer span %v", got, span.SpanContext())
	}
	want := map[attribute.Key]attribute.Value{
		attrMessagingSystem:    attribute.StringValue("pgkit"),
		attrMessagingOperation: attribute.StringValue("process"),
		attrMessagingQueue:     attribute.StringValue(queueName),
		attrMessagingMessageID: attribute.StringValue(strconv.FormatInt(id, 10)),
		attrJobAttempt:         attribute.IntValue(1),
		attrJobMaxAttempts:     attribute.IntValue(defaultMaxAttempts),
		attrJobOutcome:         attribute.StringValue(jobOutcomeDone),
	}
	for key, value := range want {
		if got := spanAttribute(span, key); got != value {
			t.Errorf("%s = %v, want %v", key, got.String(), value.String())
		}
	}
	if span.Status().Code != codes.Unset {
		t.Fatalf("status = %v, want unset", span.Status())
	}
}

func TestEveryAttemptJoinsTheEnqueuersTrace(t *testing.T) {
	recorder, tracer := installTracing(t)
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)

	requestCtx, request := tracer.Start(context.Background(), "POST /orders")
	_, err := q.Enqueue(requestCtx, EnqueueParams{QueueName: queueName, Payload: []byte(`{}`), MaxAttempts: 2})
	request.End()
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runTracedWorker(t, q, queueName, func(context.Context, Job) error { return errors.New("upstream timeout") })
	requireEventually(t, 5*time.Second, 20*time.Millisecond, func() bool { return len(consumerSpans(recorder, queueName)) == 2 })

	spans := consumerSpans(recorder, queueName)
	for i, wantOutcome := range []string{jobOutcomeRetry, jobOutcomeFailed} {
		span := spans[i]
		if span.Parent().SpanID() != request.SpanContext().SpanID() {
			t.Fatalf("attempt %d parent = %v, want the request span", i+1, span.Parent())
		}
		if got := spanAttribute(span, attrJobAttempt).AsInt64(); got != int64(i+1) {
			t.Fatalf("attempt attribute = %d, want %d", got, i+1)
		}
		if got := spanAttribute(span, attrJobOutcome).AsString(); got != wantOutcome {
			t.Fatalf("attempt %d outcome = %q, want %q", i+1, got, wantOutcome)
		}
		if span.Status().Code != codes.Error || span.Status().Description != "upstream timeout" {
			t.Fatalf("attempt %d status = %v, want error", i+1, span.Status())
		}
		if len(span.Events()) != 1 || span.Events()[0].Name != "exception" {
			t.Fatalf("attempt %d events = %v, want the recorded error", i+1, span.Events())
		}
	}
}

func TestNonRetryableFailureMarksTheSpanFailed(t *testing.T) {
	recorder, tracer := installTracing(t)
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)

	requestCtx, request := tracer.Start(context.Background(), "POST /orders")
	_, err := q.Enqueue(requestCtx, EnqueueParams{QueueName: queueName, Payload: []byte(`{}`)})
	request.End()
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runTracedWorker(t, q, queueName, func(context.Context, Job) error { return NonRetryable(errors.New("invalid order")) })
	requireEventually(t, 5*time.Second, 20*time.Millisecond, func() bool { return len(consumerSpans(recorder, queueName)) == 1 })

	span := consumerSpans(recorder, queueName)[0]
	if got := spanAttribute(span, attrJobOutcome).AsString(); got != jobOutcomeFailed {
		t.Fatalf("outcome = %q, want failed", got)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", span.Status())
	}
}

func TestJobWithoutTraceContextStartsANewTrace(t *testing.T) {
	recorder, _ := installTracing(t)
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)

	id, err := q.Enqueue(context.Background(), EnqueueParams{QueueName: queueName, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	var stored *string
	if err := q.DB().QueryRowContext(context.Background(), `SELECT trace_context::text FROM pgqueue_jobs WHERE id = $1`, id).Scan(&stored); err != nil {
		t.Fatalf("read trace context: %v", err)
	}
	if stored != nil {
		t.Fatalf("trace context = %q, want NULL without a span", *stored)
	}

	var seen handlerSpans
	runTracedWorker(t, q, queueName, func(ctx context.Context, _ Job) error {
		seen.record(ctx)
		return nil
	})
	requireEventually(t, 5*time.Second, 20*time.Millisecond, func() bool { return len(consumerSpans(recorder, queueName)) == 1 })

	span := consumerSpans(recorder, queueName)[0]
	if span.Parent().IsValid() {
		t.Fatalf("parent = %v, want a root span", span.Parent())
	}
	if got := seen.all(); len(got) != 1 || !got[0].IsValid() {
		t.Fatalf("handler span contexts = %v, want one valid span", got)
	}
}

func TestEnqueueStoresTheW3CTraceContext(t *testing.T) {
	_, tracer := installTracing(t)
	q := sharedQueue(t)

	ctx, span := tracer.Start(context.Background(), "POST /orders")
	defer span.End()
	id, err := q.Enqueue(ctx, EnqueueParams{QueueName: uniqueQueueName(t), Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var traceparent string
	if err := q.DB().QueryRowContext(ctx, `SELECT trace_context->>'traceparent' FROM pgqueue_jobs WHERE id = $1`, id).Scan(&traceparent); err != nil {
		t.Fatalf("read traceparent: %v", err)
	}
	want := "00-" + span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String() + "-01"
	if traceparent != want {
		t.Fatalf("traceparent = %q, want %q", traceparent, want)
	}
}

func TestStartJobSpanWithoutTracerProviderIsNoop(t *testing.T) {
	job := &Job{ID: 1, QueueName: "noop", Attempts: 1, MaxAttempts: 5}
	allocations := testing.AllocsPerRun(100, func() {
		ctx, span := startJobSpan(context.Background(), job)
		recordJobOutcome(span, jobOutcomeDone, nil)
		span.End()
		_ = ctx
	})
	if allocations > 8 {
		t.Fatalf("startJobSpan allocates %.0f times per job without a provider", allocations)
	}
	if injectTraceContext(context.Background()).Valid {
		t.Fatal("injectTraceContext stored a context without a span")
	}
}
