package workflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type order struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

type receipt struct {
	OrderID string `json:"order_id"`
	Charged int    `json:"charged"`
	Note    string `json:"note"`
}

func TestRunReturnsTheWorkflowOutput(t *testing.T) {
	h := newHarness(t)
	checkout := Define("checkout", func(wf *Context, in order) (receipt, error) {
		charged, err := wf.Step("charge", func(context.Context) (int, error) { return in.Total, nil })
		if err != nil {
			return receipt{}, err
		}
		note, err := wf.Step("note", func(context.Context) (string, error) { return "thanks " + in.ID, nil })
		if err != nil {
			return receipt{}, err
		}
		return receipt{OrderID: in.ID, Charged: charged, Note: note}, nil
	})
	h.startWorker(checkout)

	got := mustResult(h, mustStart(h, checkout, order{ID: "o-1", Total: 42}))

	if got != (receipt{OrderID: "o-1", Charged: 42, Note: "thanks o-1"}) {
		t.Fatalf("receipt = %+v", got)
	}
}

func TestStepsAreRecordedInTheOrderTheRunReachedThem(t *testing.T) {
	h := newHarness(t)
	flow := Define("recorded", func(wf *Context, _ struct{}) (string, error) {
		for _, name := range []string{"first", "second", "third"} {
			if _, err := wf.Step(name, func(context.Context) (string, error) { return name + "-done", nil }); err != nil {
				return "", err
			}
		}
		return "ok", nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	mustResult(h, run)

	steps := h.steps(run.ID)
	var names []string
	for _, step := range steps {
		names = append(names, step.Name)
		if step.Kind != KindStep || step.Status != StepSucceeded || step.Attempts != 1 {
			t.Fatalf("step %+v", step)
		}
		if string(step.Output) != `"`+step.Name+`-done"` {
			t.Fatalf("step %s output = %s", step.Name, step.Output)
		}
	}
	if strings.Join(names, ",") != "first,second,third" {
		t.Fatalf("steps = %v", names)
	}
	info := h.run(run.ID)
	if info.Status != RunSucceeded || string(info.Output) != `"ok"` || info.StartedAt.IsZero() || info.CompletedAt.IsZero() {
		t.Fatalf("run = %+v", info)
	}
}

func TestStartWithAKeyReturnsTheExistingRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("keyed", func(wf *Context, in int) (int, error) { return in, nil })

	first := mustStart(h, flow, 1, Key("customer-7"))
	second := mustStart(h, flow, 2, Key("customer-7"))

	if first.ID != second.ID {
		t.Fatalf("keyed starts created two runs: %s and %s", first.ID, second.ID)
	}
	if got := h.queryInt(`SELECT count(*) FROM pgworkflow_runs`); got != 1 {
		t.Fatalf("runs = %d", got)
	}
	if got := h.queryInt(`SELECT count(*) FROM pgqueue_jobs`); got != 1 {
		t.Fatalf("jobs = %d", got)
	}
	h.startWorker(flow)
	if got := mustResult(h, second); got != 1 {
		t.Fatalf("result = %d, want the first input", got)
	}
}

func TestStartWithTheKeyOfAFinishedRunReturnsThatRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("keyed-finished", func(wf *Context, in int) (int, error) { return in * 10, nil })
	h.startWorker(flow)
	first := mustStart(h, flow, 1, Key("once"))
	mustResult(h, first)

	again := mustStart(h, flow, 5, Key("once"))

	if again.ID != first.ID {
		t.Fatalf("a finished keyed run was started again")
	}
	if got := mustResult(h, again); got != 10 {
		t.Fatalf("result = %d", got)
	}
}

func TestKeysAreScopedToTheirWorkflow(t *testing.T) {
	h := newHarness(t)
	a := Define("scoped-a", func(wf *Context, in int) (int, error) { return in, nil })
	b := Define("scoped-b", func(wf *Context, in int) (int, error) { return in, nil })

	runA := mustStart(h, a, 1, Key("shared"))
	runB := mustStart(h, b, 2, Key("shared"))

	if runA.ID == runB.ID {
		t.Fatal("two workflows shared a run through the same key")
	}
}

func TestStartTxStartsTheRunOnlyWhenTheTransactionCommits(t *testing.T) {
	h := newHarness(t)
	flow := Define("transactional-start", func(wf *Context, in string) (string, error) { return in, nil })
	h.startWorker(flow)

	tx, err := h.db.BeginTx(h.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.StartTx(h.ctx, tx, flow, "rolled back"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := h.queryInt(`SELECT count(*) FROM pgworkflow_runs`); got != 0 {
		t.Fatalf("a rolled-back start left %d runs", got)
	}

	tx, err = h.db.BeginTx(h.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	run, err := h.client.StartTx(h.ctx, tx, flow, "committed")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := mustResult(h, run); got != "committed" {
		t.Fatalf("result = %q", got)
	}
}

func TestRunByKeyFindsTheRunStartedWithTheKey(t *testing.T) {
	h := newHarness(t)
	flow := Define("by-key", func(wf *Context, in int) (int, error) { return in, nil })
	started := mustStart(h, flow, 3, Key("lookup"))

	found, err := h.client.RunByKey(h.ctx, flow, "lookup")
	if err != nil {
		t.Fatal(err)
	}
	if found.ID != started.ID {
		t.Fatalf("found %s, want %s", found.ID, started.ID)
	}
	_, err = h.client.RunByKey(h.ctx, flow, "missing")
	requireErrorIs(t, err, ErrRunNotFound)
}

func TestClientRunIsATypedHandleOnAnExistingRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("handle", func(wf *Context, in int) (int, error) { return in + 1, nil })
	h.startWorker(flow)
	started := mustStart(h, flow, 41)

	if got := mustResult(h, h.client.Run(flow, started.ID)); got != 42 {
		t.Fatalf("result = %d", got)
	}
}

func TestResultOfAnUnknownRunIsErrRunNotFound(t *testing.T) {
	h := newHarness(t)
	flow := Define("unknown", func(wf *Context, in int) (int, error) { return in, nil })
	_, err := result(h, h.client.Run(flow, "no-such-run"))
	requireErrorIs(t, err, ErrRunNotFound)
}

func TestResultStopsWithItsContext(t *testing.T) {
	h := newHarness(t)
	flow := Define("never-picked-up", func(wf *Context, in int) (int, error) { return in, nil })
	run := mustStart(h, flow, 1)

	ctx, cancel := context.WithTimeout(h.ctx, 200*time.Millisecond)
	defer cancel()
	_, err := run.Result(ctx)
	requireErrorIs(t, err, context.DeadlineExceeded)
}

func TestAnErrorFromTheWorkflowFailsTheRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("refusing", func(wf *Context, in order) (receipt, error) {
		return receipt{}, fmt.Errorf("order %s is empty", in.ID)
	})
	failed := make(chan RunInfo, 1)
	h.startWorkerWith(workerOptions{config: WorkerConfig{OnRunFailed: func(_ context.Context, run RunInfo) {
		failed <- run
	}}}, flow)
	run := mustStart(h, flow, order{ID: "o-9"})

	_, err := result(h, run)

	runErr := requireRunError(t, err)
	if runErr.Status != RunFailed || runErr.Message != "order o-9 is empty" || runErr.Workflow != "refusing" || runErr.RunID != run.ID {
		t.Fatalf("run error = %+v", runErr)
	}
	if errors.Is(err, ErrCancelled) || errors.Is(err, ErrTimeout) {
		t.Fatalf("a plain failure matched cancellation or timeout: %v", err)
	}
	select {
	case info := <-failed:
		if info.ID != run.ID || info.Status != RunFailed {
			t.Fatalf("OnRunFailed got %+v", info)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnRunFailed was not called")
	}
}

func TestAPanicInTheWorkflowFailsTheRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("panicking", func(wf *Context, _ struct{}) (int, error) {
		panic("boom")
	})
	h.startWorker(flow)

	_, err := result(h, mustStart(h, flow, struct{}{}))

	if runErr := requireRunError(t, err); runErr.Message != "workflow: panic: boom" {
		t.Fatalf("message = %q", runErr.Message)
	}
}

func TestAnInputThatNoLongerDecodesFailsTheRun(t *testing.T) {
	h := newHarness(t)
	producer := Define("reshaped", func(wf *Context, in string) (string, error) { return in, nil })
	consumer := Define("reshaped", func(wf *Context, in struct{ Count int }) (int, error) { return in.Count, nil })
	run := mustStart(h, producer, "not an object")
	h.startWorker(consumer)

	_, err := result(h, h.client.Run(consumer, run.ID))

	if runErr := requireRunError(t, err); !strings.Contains(runErr.Message, ErrNonDeterministic.Error()) {
		t.Fatalf("message = %q", runErr.Message)
	}
}

func TestAnOutputThatCannotBeEncodedFailsTheRun(t *testing.T) {
	h := newHarness(t)
	flow := Define("unencodable", func(wf *Context, _ struct{}) (chan int, error) { return make(chan int), nil })
	h.startWorker(flow)

	_, err := result(h, mustStart(h, flow, struct{}{}))

	if runErr := requireRunError(t, err); !strings.Contains(runErr.Message, "encode chan int output") {
		t.Fatalf("message = %q", runErr.Message)
	}
}

func TestRunIDIsAvailableToTheWorkflow(t *testing.T) {
	h := newHarness(t)
	flow := Define("self-aware", func(wf *Context, _ struct{}) (string, error) { return wf.RunID(), nil })
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if got := mustResult(h, run); got != run.ID {
		t.Fatalf("RunID = %q, want %q", got, run.ID)
	}
}

func TestTheWorkflowContextIsAContext(t *testing.T) {
	h := newHarness(t)
	type keyType struct{}
	flow := Define("as-context", func(wf *Context, _ struct{}) (bool, error) {
		var ctx context.Context = wf
		return ctx.Value(keyType{}) == nil && ctx.Err() == nil, nil
	})
	h.startWorker(flow)

	if !mustResult(h, mustStart(h, flow, struct{}{})) {
		t.Fatal("the workflow context is not a live context")
	}
}

func TestStartRefusesAnInputThatCannotBeEncoded(t *testing.T) {
	h := newHarness(t)
	flow := Define("bad-input", func(wf *Context, in chan int) (int, error) { return 0, nil })
	_, err := h.client.Start(h.ctx, flow, make(chan int))
	if err == nil || !strings.Contains(err.Error(), "encode chan int input") {
		t.Fatalf("err = %v", err)
	}
}

var _ = sql.ErrNoRows
