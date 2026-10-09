package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var double = Define("double", func(wf *Context, n int) (int, error) {
	return wf.Step("multiply", func(context.Context) (int, error) { return n * 2, nil })
})

func TestCallReturnsTheChildOutput(t *testing.T) {
	h := newHarness(t)
	parent := Define("caller", func(wf *Context, n int) (int, error) {
		doubled, err := wf.Call("double", double, n)
		if err != nil {
			return 0, err
		}
		return doubled + 1, nil
	})
	h.startWorker(parent, double)

	if got := mustResult(h, mustStart(h, parent, 20)); got != 41 {
		t.Fatalf("result = %d", got)
	}
}

func TestAChildRunIsLinkedToItsParent(t *testing.T) {
	h := newHarness(t)
	parent := Define("linking", func(wf *Context, n int) (int, error) {
		return wf.Call("double", double, n)
	})
	h.startWorker(parent, double)
	run := mustStart(h, parent, 2)
	mustResult(h, run)

	step := h.step(run.ID, "double")
	child := h.run(step.ChildRunID)
	if step.Kind != KindChild || step.Status != StepSucceeded || string(step.Output) != "4" {
		t.Fatalf("step = %+v", step)
	}
	if child.ParentRunID != run.ID || child.Key != run.ID+"/double" || child.Workflow != "double" || child.Status != RunSucceeded {
		t.Fatalf("child = %+v", child)
	}
	children, err := h.client.ListRuns(h.ctx, ListRunsParams{ParentRunID: run.ID})
	if err != nil || len(children) != 1 || children[0].ID != child.ID {
		t.Fatalf("children = %+v, err = %v", children, err)
	}
}

func TestStartFansOutToChildrenThatRunConcurrently(t *testing.T) {
	h := newHarness(t)
	const children = 4
	var entered sync.WaitGroup
	entered.Add(children)
	allEntered := make(chan struct{})
	go func() { entered.Wait(); close(allEntered) }()
	rendezvous := Define("rendezvous-child", func(wf *Context, n int) (int, error) {
		return wf.Step("meet", func(ctx context.Context) (int, error) {
			entered.Done()
			select {
			case <-allEntered:
				return n * 2, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}, NoRetry, Timeout(20*time.Second))
	})
	parent := Define("fan-out", func(wf *Context, numbers []int) (int, error) {
		var futures []*Future[int]
		for _, n := range numbers {
			futures = append(futures, wf.Start("double", rendezvous, n))
		}
		sum := 0
		for _, future := range futures {
			doubled, err := future.Wait()
			if err != nil {
				return 0, err
			}
			sum += doubled
		}
		return sum, nil
	})
	h.startWorkerWith(workerOptions{config: WorkerConfig{Concurrency: children + 1}}, parent, rendezvous)
	run := mustStart(h, parent, []int{1, 2, 3, 4})

	if got := mustResult(h, run); got != 20 {
		t.Fatalf("sum = %d", got)
	}
	if got := h.queryInt(`SELECT count(*) FROM pgworkflow_runs WHERE parent_run_id = $1`, run.ID); got != children {
		t.Fatalf("children = %d", got)
	}
}

func TestAChildIsStartedOnceAcrossReplays(t *testing.T) {
	h := newHarness(t)
	parent := Define("replaying-parent", func(wf *Context, n int) (int, error) {
		doubled, err := wf.Call("double", double, n)
		if err != nil {
			return 0, err
		}
		return doubled, wf.Sleep("pause", time.Hour)
	})
	h.startWorker(parent, double)
	run := mustStart(h, parent, 5)
	h.waitForStep(run.ID, "pause", StepWaiting)
	h.waitForStatus(run.ID, RunWaiting)

	h.fastForward(run.ID)

	if got := mustResult(h, run); got != 10 {
		t.Fatalf("result = %d", got)
	}
	if got := h.queryInt(`SELECT count(*) FROM pgworkflow_runs WHERE parent_run_id = $1`, run.ID); got != 1 {
		t.Fatalf("children = %d", got)
	}
}

func TestAFailedChildIsARunErrorInItsParent(t *testing.T) {
	h := newHarness(t)
	failing := Define("failing-child", func(wf *Context, _ struct{}) (int, error) {
		return 0, errors.New("out of stock")
	})
	parent := Define("handling-parent", func(wf *Context, _ struct{}) (string, error) {
		_, err := wf.Call("reserve", failing, struct{}{})
		var runErr *RunError
		if !errors.As(err, &runErr) {
			return "", fmt.Errorf("unexpected %T: %v", err, err)
		}
		if err := wf.Sleep("replay-me", 10*time.Millisecond); err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s: %s", runErr.Workflow, runErr.Status, runErr.Message), nil
	})
	h.startWorker(parent, failing)

	got := mustResult(h, mustStart(h, parent, struct{}{}))

	if got != "failing-child failed: out of stock" {
		t.Fatalf("result = %q", got)
	}
}

func TestAParentWaitsForAChildOnAnotherWorker(t *testing.T) {
	h := newHarness(t)
	parent := Define("distributed", func(wf *Context, n int) (int, error) {
		return wf.Call("double", double, n)
	})
	h.startWorkerWith(workerOptions{id: "parents"}, parent)
	run := mustStart(h, parent, 7)
	h.waitForStatus(run.ID, RunWaiting)

	h.startWorkerWith(workerOptions{id: "children", pickup: notifiedOnly}, double)

	if got := mustResult(h, run); got != 14 {
		t.Fatalf("result = %d", got)
	}
}

func TestCancellingAParentCancelsItsChildren(t *testing.T) {
	h := newHarness(t)
	sleepy := Define("sleepy-child", func(wf *Context, _ struct{}) (int, error) {
		return 0, wf.Sleep("forever", time.Hour)
	})
	parent := Define("cancelled-parent", func(wf *Context, _ struct{}) (int, error) {
		return wf.Call("child", sleepy, struct{}{})
	})
	h.startWorker(parent, sleepy)
	run := mustStart(h, parent, struct{}{})
	childStep := h.waitForStep(run.ID, "child", StepWaiting)
	h.waitForStatus(childStep.ChildRunID, RunWaiting)

	if err := h.client.Cancel(h.ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	_, err := result(h, run)
	requireErrorIs(t, err, ErrCancelled)
	_, err = result(h, h.client.Run(sleepy, childStep.ChildRunID))
	requireErrorIs(t, err, ErrCancelled)
	h.eventually("both jobs are settled", func() bool {
		return h.queryInt(`SELECT count(*) FROM pgqueue_jobs WHERE status <> 'done'`) == 0
	})
}

func TestAFailingParentCancelsItsUnfinishedChildren(t *testing.T) {
	h := newHarness(t)
	sleepy := Define("abandoned-child", func(wf *Context, _ struct{}) (int, error) {
		return 0, wf.Sleep("forever", time.Hour)
	})
	parent := Define("failing-parent", func(wf *Context, _ struct{}) (int, error) {
		wf.Start("child", sleepy, struct{}{})
		return 0, errors.New("gave up")
	})
	h.startWorker(parent, sleepy)
	run := mustStart(h, parent, struct{}{})

	_, err := result(h, run)
	requireFailed(t, err)
	childID := h.step(run.ID, "child").ChildRunID

	h.waitForStatus(childID, RunCancelled)
}

func TestACancelledChildIsReportedToItsParent(t *testing.T) {
	h := newHarness(t)
	sleepy := Define("cancellable-child", func(wf *Context, _ struct{}) (int, error) {
		return 0, wf.Sleep("forever", time.Hour)
	})
	parent := Define("observing-parent", func(wf *Context, _ struct{}) (bool, error) {
		_, err := wf.Call("child", sleepy, struct{}{})
		return errors.Is(err, ErrCancelled), nil
	})
	h.startWorker(parent, sleepy)
	run := mustStart(h, parent, struct{}{})
	childStep := h.waitForStep(run.ID, "child", StepWaiting)
	h.waitForStatus(childStep.ChildRunID, RunWaiting)
	h.waitForStatus(run.ID, RunWaiting)

	if err := h.client.Cancel(h.ctx, childStep.ChildRunID); err != nil {
		t.Fatal(err)
	}

	if !mustResult(h, run) {
		t.Fatal("the parent did not see its child cancelled")
	}
}

func TestATimedOutChildIsATimeoutInItsParent(t *testing.T) {
	h := newHarness(t)
	parent := Define("deadline-parent", func(wf *Context, _ struct{}) (bool, error) {
		_, err := wf.Call("child", slowChild, struct{}{}, Timeout(200*time.Millisecond))
		return errors.Is(err, ErrTimeout), nil
	})
	h.startWorker(parent, slowChild)
	run := mustStart(h, parent, struct{}{})

	if !mustResult(h, run) {
		t.Fatal("the parent did not see its child time out")
	}
}

var slowChild = Define("slow-child", func(wf *Context, _ struct{}) (int, error) {
	return wf.Receive(NewSignal[int]("never"), Forever)
})

func TestAChildRefusesAKey(t *testing.T) {
	h := newHarness(t)
	parent := Define("keyed-child", func(wf *Context, _ struct{}) (string, error) {
		_, err := wf.Call("child", double, 1, Key("mine"))
		return fmt.Sprint(err), nil
	})
	h.startWorker(parent, double)

	if got := mustResult(h, mustStart(h, parent, struct{}{})); got != `workflow: child "child" is keyed by its step name and takes no Key` {
		t.Fatalf("result = %q", got)
	}
}

func TestAChildWithAnInvalidNameFailsItsFuture(t *testing.T) {
	h := newHarness(t)
	parent := Define("bad-child-name", func(wf *Context, _ struct{}) (bool, error) {
		_, err := wf.Call("", double, 1)
		return errors.Is(err, ErrInvalidName), nil
	})
	h.startWorker(parent, double)

	if !mustResult(h, mustStart(h, parent, struct{}{})) {
		t.Fatal("an invalid child name was accepted")
	}
}
