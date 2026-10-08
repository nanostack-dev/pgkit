package queue

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

type builderEmail struct {
	To string `json:"to"`
}

type builderReceipt struct {
	Amount int `json:"amount"`
}

func ignoreEmail(context.Context, builderEmail, Job) error { return nil }

func TestWorkerBuilderHandlesSeveralTypedQueues(t *testing.T) {
	h := newPickupHarness(t)
	emails := h.q.Queue[builderEmail]("builder_emails")
	receipts := h.q.Queue[builderReceipt]("builder_receipts")
	var sentTo atomic.Value
	var charged atomic.Int64
	worker, err := h.q.Worker("builder").
		Pickup(OnEnqueue().RescanEvery(noRescan)).
		Handle(emails, func(_ context.Context, email builderEmail, _ Job) error {
			sentTo.Store(email.To)
			return nil
		}).
		Handle(receipts, func(_ context.Context, receipt builderReceipt, _ Job) error {
			charged.Store(int64(receipt.Amount))
			return nil
		}).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	h.start(&countingWorker{Worker: worker})

	if _, err := emails.Enqueue(h.ctx, builderEmail{To: "rowan@example.com"}); err != nil {
		t.Fatalf("enqueue email: %v", err)
	}
	if _, err := receipts.Enqueue(h.ctx, builderReceipt{Amount: 42}); err != nil {
		t.Fatalf("enqueue receipt: %v", err)
	}
	requireEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return sentTo.Load() == "rowan@example.com" && charged.Load() == 42
	})
}

func TestWorkerBuilderHandleRawReceivesTheStoredPayload(t *testing.T) {
	h := newPickupHarness(t)
	var payload atomic.Value
	worker, err := h.q.Worker("builder_raw").
		Pickup(OnEnqueue().RescanEvery(noRescan)).
		HandleRaw("builder_raw", func(_ context.Context, job Job) error {
			payload.Store(string(job.Payload))
			return nil
		}).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	h.start(&countingWorker{Worker: worker})
	if _, err := h.q.Enqueue(h.ctx, EnqueueParams{QueueName: "builder_raw", Payload: []byte(`{"raw":true}`)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	requireEventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return payload.Load() == `{"raw":true}`
	})
}

func TestWorkerBuilderCopiesDoNotShareHandlers(t *testing.T) {
	q, err := New(openWithoutConnecting(t))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	base := q.Worker("copies").Pickup(PollEvery(time.Second)).Handle(q.Queue[builderEmail]("first"), ignoreEmail)
	extended := base.Handle(q.Queue[builderEmail]("second"), ignoreEmail)
	sibling := base.Handle(q.Queue[builderEmail]("third"), ignoreEmail)

	for builder, want := range map[*WorkerBuilder][]string{
		&base:     {"first"},
		&extended: {"first", "second"},
		&sibling:  {"first", "third"},
	} {
		worker, err := builder.Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := worker.registry.queueNames(); !slices.Equal(got, want) {
			t.Fatalf("expected queues %v, got %v", want, got)
		}
	}
}

func TestWorkerBuilderRefusesTheSameQueueTwice(t *testing.T) {
	q, err := New(openWithoutConnecting(t))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	emails := q.Queue[builderEmail]("twice")
	_, err = q.Worker("twice").Handle(emails, ignoreEmail).Handle(emails, ignoreEmail).Build()
	if !errors.Is(err, ErrHandlerAlreadySet) {
		t.Fatalf("expected ErrHandlerAlreadySet, got %v", err)
	}
}

func TestWorkerBuilderTuneKeepsTheIdAndPickup(t *testing.T) {
	q, err := New(openWithoutConnecting(t))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	worker, err := q.Worker("tuned").
		Pickup(PollEvery(3 * time.Second)).
		Tune(WorkerConfig{WorkerID: "ignored", BatchSizePerQueue: 7}).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if worker.cfg.WorkerID != "tuned" || worker.cfg.Pickup != PollEvery(3*time.Second) || worker.cfg.BatchSizePerQueue != 7 {
		t.Fatalf("unexpected config: %+v", worker.cfg)
	}
}
