package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var tracing struct {
	once     sync.Once
	recorder *tracetest.SpanRecorder
	tracer   trace.Tracer
}

func installTracing() (*tracetest.SpanRecorder, trace.Tracer) {
	tracing.once.Do(func() {
		tracing.recorder = tracetest.NewSpanRecorder()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(tracing.recorder))
		otel.SetTracerProvider(provider)
		otel.SetTextMapPropagator(propagation.TraceContext{})
		tracing.tracer = provider.Tracer("test")
	})
	return tracing.recorder, tracing.tracer
}

func TestRunActivationsAndChildrenJoinTheStartersTrace(t *testing.T) {
	recorder, tracer := installTracing()
	h := newHarness(t)

	var mu sync.Mutex
	var stepTraces []trace.TraceID
	recordStep := func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		stepTraces = append(stepTraces, trace.SpanContextFromContext(ctx).TraceID())
	}
	child := Define("traced-child", func(wf *Context, _ struct{}) (string, error) {
		return wf.Step("child-step", func(ctx context.Context) (string, error) {
			recordStep(ctx)
			return "child", nil
		})
	})
	parent := Define("traced-parent", func(wf *Context, _ struct{}) (string, error) {
		if _, err := wf.Step("before", func(ctx context.Context) (int, error) {
			recordStep(ctx)
			return 1, nil
		}); err != nil {
			return "", err
		}
		if err := wf.Sleep("nap", 50*time.Millisecond); err != nil {
			return "", err
		}
		return wf.Call("call-child", child, struct{}{})
	})
	h.startWorker(parent, child)

	requestCtx, request := tracer.Start(h.ctx, "POST /checkout")
	run, err := h.client.Start(requestCtx, parent, struct{}{})
	request.End()
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	mustResult(h, run)

	traceID := request.SpanContext().TraceID()
	mu.Lock()
	defer mu.Unlock()
	if len(stepTraces) != 2 {
		t.Fatalf("recorded %d steps, want 2", len(stepTraces))
	}
	for i, got := range stepTraces {
		if got != traceID {
			t.Fatalf("step %d ran in trace %s, want %s", i, got, traceID)
		}
	}

	activations := map[string]int{}
	for _, span := range recorder.Ended() {
		if span.SpanContext().TraceID() == traceID && span.SpanKind() == trace.SpanKindConsumer {
			activations[span.Name()]++
		}
	}
	if activations["pgkit.queue pgworkflow:traced-parent"] < 2 || activations["pgkit.queue pgworkflow:traced-child"] < 1 {
		t.Fatalf("consumer spans in the starter's trace = %v, want parent activations around the sleep and the child", activations)
	}
}
