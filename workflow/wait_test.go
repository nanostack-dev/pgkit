package workflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSleepParksTheRunAndFreesTheWorker(t *testing.T) {
	h := newHarness(t)
	sleeper := Define("sleeper", func(wf *Context, _ struct{}) (string, error) {
		return "awake", wf.Sleep("nap", time.Hour)
	})
	quick := Define("quick", func(wf *Context, _ struct{}) (string, error) { return "quick", nil })
	h.startWorkerWith(workerOptions{config: WorkerConfig{Concurrency: 1}}, sleeper, quick)
	sleeping := mustStart(h, sleeper, struct{}{})

	waiting := h.waitForStatus(sleeping.ID, RunWaiting)

	if got := mustResult(h, mustStart(h, quick, struct{}{})); got != "quick" {
		t.Fatalf("the single worker slot did not run another run while one slept: %q", got)
	}
	nap := h.step(sleeping.ID, "nap")
	if nap.Kind != KindSleep || nap.Status != StepWaiting || !waiting.WakeAt.Equal(nap.WakeAt) {
		t.Fatalf("run = %+v, nap = %+v", waiting, nap)
	}
	if delay := time.Until(nap.WakeAt); delay < 59*time.Minute || delay > 61*time.Minute {
		t.Fatalf("wakes in %v", delay)
	}
	jobDue := h.queryInt(`SELECT count(*) FROM pgqueue_jobs j JOIN pgworkflow_runs r ON r.job_id = j.id
		WHERE r.id = $1 AND j.status = 'pending' AND j.available_at = r.wake_at`, sleeping.ID)
	if jobDue != 1 {
		t.Fatal("the run's job is not parked until the run wakes")
	}
}

func TestSleepResumesTheRunWhenItIsDue(t *testing.T) {
	h := newHarness(t)
	flow := Define("short-nap", func(wf *Context, _ struct{}) (time.Duration, error) {
		started, err := wf.Step("started", func(context.Context) (time.Time, error) { return time.Now(), nil })
		if err != nil {
			return 0, err
		}
		if err := wf.Sleep("nap", 300*time.Millisecond); err != nil {
			return 0, err
		}
		return time.Since(started), nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if slept := mustResult(h, run); slept < 300*time.Millisecond {
		t.Fatalf("slept %v", slept)
	}
	if nap := h.step(run.ID, "nap"); nap.Status != StepSucceeded {
		t.Fatalf("nap = %+v", nap)
	}
}

func TestASleepIsMeasuredFromTheFirstTimeTheRunReachesIt(t *testing.T) {
	h := newHarness(t)
	flow := Define("steady-nap", func(wf *Context, _ struct{}) (string, error) {
		return "", wf.Sleep("nap", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)
	firstWake := h.step(run.ID, "nap").WakeAt
	lease := h.lease(run.ID)

	h.wakeJob(run.ID)
	h.eventually("the run replays and parks again", func() bool {
		return h.lease(run.ID) > lease && h.run(run.ID).Status == RunWaiting
	})

	if wake := h.step(run.ID, "nap").WakeAt; !wake.Equal(firstWake) {
		t.Fatalf("replaying moved the wake time from %v to %v", firstWake, wake)
	}
}

func TestSleepUntilAPastTimeDoesNotPark(t *testing.T) {
	h := newHarness(t)
	flow := Define("overdue", func(wf *Context, _ struct{}) (string, error) {
		return "on time", wf.SleepUntil("deadline", time.Now().Add(-time.Hour))
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	mustResult(h, run)

	if h.lease(run.ID) != 1 {
		t.Fatal("a past SleepUntil parked the run")
	}
}

func TestSleepUntilParksUntilTheGivenTime(t *testing.T) {
	h := newHarness(t)
	wakeAt := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Millisecond)
	flow := Define("appointment", func(wf *Context, _ struct{}) (string, error) {
		return "", wf.SleepUntil("appointment", wakeAt)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	h.waitForStatus(run.ID, RunWaiting)

	if got := h.step(run.ID, "appointment").WakeAt; !got.Equal(wakeAt) {
		t.Fatalf("wakes at %v, want %v", got, wakeAt)
	}
}

func TestAZeroSleepDoesNotPark(t *testing.T) {
	h := newHarness(t)
	flow := Define("no-nap", func(wf *Context, _ struct{}) (string, error) {
		return "done", wf.Sleep("nap", 0)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	mustResult(h, run)

	if h.lease(run.ID) != 1 || h.step(run.ID, "nap").Status != StepSucceeded {
		t.Fatal("a zero sleep parked the run")
	}
}

func TestOperationsAfterASuspensionReturnErrSuspended(t *testing.T) {
	h := newHarness(t)
	calls := newCounter()
	flow := Define("careless", func(wf *Context, _ struct{}) (string, error) {
		_ = wf.Sleep("ignored", time.Hour)
		_, err := wf.Step("must-not-run", func(context.Context) (int, error) { return calls.add("step"), nil })
		if !errors.Is(err, ErrSuspended) {
			return "", fmt.Errorf("step returned %v", err)
		}
		return "returned success", nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	h.waitForStatus(run.ID, RunWaiting)

	if calls.get("step") != 0 {
		t.Fatal("a step ran after the run suspended")
	}
}

type approval struct {
	By string `json:"by"`
}

var approved = NewSignal[approval]("approved")

func approvalFlow(name string, timeout time.Duration) *Workflow[struct{}, string] {
	return Define(name, func(wf *Context, _ struct{}) (string, error) {
		decision, err := wf.Receive(approved, timeout)
		if err != nil {
			return "", err
		}
		return "approved by " + decision.By, nil
	})
}

func TestReceiveReturnsASignalSentWhileTheRunWaits(t *testing.T) {
	h := newHarness(t)
	flow := approvalFlow("approval", Forever)
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	if err := h.client.Signal(h.ctx, run.ID, approved, approval{By: "lea"}); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "approved by lea" {
		t.Fatalf("result = %q", got)
	}
	step := h.step(run.ID, "approved")
	if step.Kind != KindSignal || step.Signal != "approved" || step.Status != StepSucceeded || string(step.Output) != `{"by": "lea"}` {
		t.Fatalf("step = %+v", step)
	}
}

func TestASignalSentBeforeTheRunWaitsIsKept(t *testing.T) {
	h := newHarness(t)
	flow := approvalFlow("early-approval", Forever)
	run := mustStart(h, flow, struct{}{})
	if err := h.client.Signal(h.ctx, run.ID, approved, approval{By: "sam"}); err != nil {
		t.Fatal(err)
	}
	h.startWorker(flow)

	if got := mustResult(h, run); got != "approved by sam" {
		t.Fatalf("result = %q", got)
	}
	if h.lease(run.ID) != 1 {
		t.Fatal("the run parked although its signal was already there")
	}
}

func TestSignalsAreReceivedOnceInTheOrderTheyWereSent(t *testing.T) {
	h := newHarness(t)
	votes := NewSignal[int]("vote")
	flow := Define("tally", func(wf *Context, _ struct{}) ([]int, error) {
		var received []int
		for {
			vote, err := wf.Receive(votes, 0)
			if errors.Is(err, ErrTimeout) {
				return received, nil
			}
			if err != nil {
				return nil, err
			}
			received = append(received, vote)
		}
	})
	run := mustStart(h, flow, struct{}{})
	for _, vote := range []int{3, 1, 2} {
		if err := h.client.Signal(h.ctx, run.ID, votes, vote); err != nil {
			t.Fatal(err)
		}
	}
	h.startWorker(flow)

	got := mustResult(h, run)

	if fmt.Sprint(got) != "[3 1 2]" {
		t.Fatalf("received %v", got)
	}
	if h.queryInt(`SELECT count(*) FROM pgworkflow_signals WHERE received_by IS NULL`) != 0 {
		t.Fatal("a signal was left unreceived")
	}
}

func TestReceiveTimesOut(t *testing.T) {
	h := newHarness(t)
	flow := Define("expiring", func(wf *Context, _ struct{}) (string, error) {
		_, err := wf.Receive(approved, 200*time.Millisecond)
		if errors.Is(err, ErrTimeout) {
			return "expired", nil
		}
		return "approved", err
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if got := mustResult(h, run); got != "expired" {
		t.Fatalf("result = %q", got)
	}
	if step := h.step(run.ID, "approved"); step.Status != StepTimedOut {
		t.Fatalf("step = %+v", step)
	}
}

func TestATimedOutReceiveStaysTimedOutOnReplay(t *testing.T) {
	h := newHarness(t)
	flow := Define("expired-for-good", func(wf *Context, _ struct{}) (string, error) {
		_, err := wf.Receive(approved, time.Hour)
		outcome := "approved"
		if errors.Is(err, ErrTimeout) {
			outcome = "expired"
		} else if err != nil {
			return "", err
		}
		return outcome, wf.Sleep("after", time.Hour)
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)
	h.fastForward(run.ID)
	h.waitForStep(run.ID, "approved", StepTimedOut)
	h.waitForStatus(run.ID, RunWaiting)

	if err := h.client.Signal(h.ctx, run.ID, approved, approval{By: "late"}); err != nil {
		t.Fatal(err)
	}
	h.fastForward(run.ID)

	if got := mustResult(h, run); got != "expired" {
		t.Fatalf("a late signal changed a recorded timeout: %q", got)
	}
}

func TestReceiveWithAZeroTimeoutChecksOnce(t *testing.T) {
	h := newHarness(t)
	flow := Define("poll-once", func(wf *Context, _ struct{}) (bool, error) {
		_, err := wf.Receive(approved, 0)
		return errors.Is(err, ErrTimeout), nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	if !mustResult(h, run) || h.lease(run.ID) != 1 {
		t.Fatal("a zero-timeout receive waited")
	}
}

func TestReceiveForeverParksWithoutAWakeTime(t *testing.T) {
	h := newHarness(t)
	flow := approvalFlow("indefinite", Forever)
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	waiting := h.waitForStatus(run.ID, RunWaiting)

	if !waiting.WakeAt.IsZero() || !h.step(run.ID, "approved").WakeAt.IsZero() {
		t.Fatalf("a receive without timeout set a wake time: %+v", waiting)
	}
	if h.queryInt(`SELECT count(*) FROM pgqueue_jobs WHERE available_at = 'infinity'`) != 1 {
		t.Fatal("the run's job is not parked until woken")
	}
}

func TestASignalWakesAParkedRunThroughNotifications(t *testing.T) {
	h := newHarness(t)
	flow := approvalFlow("notified", Forever)
	h.startWorkerWith(workerOptions{pickup: notifiedOnly}, flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	if err := h.client.Signal(h.ctx, run.ID, approved, approval{By: "ana"}); err != nil {
		t.Fatal(err)
	}

	if got := mustResult(h, run); got != "approved by ana" {
		t.Fatalf("result = %q", got)
	}
}

func TestASignalOfAnotherNameDoesNotWakeTheRun(t *testing.T) {
	h := newHarness(t)
	flow := approvalFlow("ignoring", Forever)
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)
	lease := h.lease(run.ID)

	if err := h.client.Signal(h.ctx, run.ID, NewSignal[string]("unrelated"), "hello"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	if h.lease(run.ID) != lease || h.run(run.ID).Status != RunWaiting {
		t.Fatal("an unrelated signal woke the run")
	}
}

func TestSignalingAFinishedRunFails(t *testing.T) {
	h := newHarness(t)
	flow := Define("finished-early", func(wf *Context, _ struct{}) (int, error) { return 1, nil })
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	mustResult(h, run)

	err := h.client.Signal(h.ctx, run.ID, approved, approval{})

	requireErrorIs(t, err, ErrRunFinished)
}

func TestSignalingAnUnknownRunFails(t *testing.T) {
	h := newHarness(t)
	requireErrorIs(t, h.client.Signal(h.ctx, "missing", approved, approval{}), ErrRunNotFound)
}

func TestSignalTxDeliversOnlyWhenTheTransactionCommits(t *testing.T) {
	h := newHarness(t)
	flow := approvalFlow("transactional-approval", Forever)
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})
	h.waitForStatus(run.ID, RunWaiting)

	signalIn := func(by string, commit bool) {
		tx, err := h.db.BeginTx(h.ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.client.SignalTx(h.ctx, tx, run.ID, approved, approval{By: by}); err != nil {
			t.Fatal(err)
		}
		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	signalIn("rolled back", false)
	signalIn("committed", true)

	if got := mustResult(h, run); got != "approved by committed" {
		t.Fatalf("result = %q", got)
	}
}

func TestSignalingWithAnInvalidSignalFails(t *testing.T) {
	h := newHarness(t)
	err := h.client.Signal(h.ctx, "any", Signal[int]{}, 1)
	requireErrorIs(t, err, ErrInvalidName)
}

func TestNoSignalIsLostWhileTheRunParksAndWakes(t *testing.T) {
	h := newHarness(t)
	const total = 40
	increments := NewSignal[int]("increment")
	flow := Define("accumulator", func(wf *Context, _ struct{}) (int, error) {
		sum := 0
		for range total {
			value, err := wf.Receive(increments, Forever)
			if err != nil {
				return 0, err
			}
			sum += value
		}
		return sum, nil
	})
	h.startWorker(flow)
	run := mustStart(h, flow, struct{}{})

	var senders sync.WaitGroup
	for sender := range 4 {
		senders.Go(func() {
			for i := range total / 4 {
				if err := h.client.Signal(h.ctx, run.ID, increments, sender*100+i); err != nil {
					t.Errorf("signal: %v", err)
				}
				time.Sleep(time.Duration(i%3) * 5 * time.Millisecond)
			}
		})
	}
	senders.Wait()

	want := 0
	for sender := range 4 {
		for i := range total / 4 {
			want += sender*100 + i
		}
	}
	if got := mustResult(h, run); got != want {
		t.Fatalf("sum = %d, want %d", got, want)
	}
}

func TestASignalPayloadThatNoLongerDecodesFailsTheRun(t *testing.T) {
	h := newHarness(t)
	asText := NewSignal[string]("reshaped")
	asNumber := NewSignal[int]("reshaped")
	flow := Define("reshaped-signal", func(wf *Context, _ struct{}) (int, error) {
		return wf.Receive(asNumber, Forever)
	})
	run := mustStart(h, flow, struct{}{})
	if err := h.client.Signal(h.ctx, run.ID, asText, "not a number"); err != nil {
		t.Fatal(err)
	}
	h.startWorker(flow)

	_, err := result(h, run)

	if runErr := requireRunError(t, err); !strings.Contains(runErr.Message, ErrNonDeterministic.Error()) {
		t.Fatalf("message = %q", runErr.Message)
	}
}

var _ = sql.ErrNoRows
