package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

// Context gives a workflow function its durable operations. It is also the
// context.Context of the run, cancelled when the run is cancelled, times out or
// moves to another worker.
//
// Every operation takes a name that identifies its checkpoint within the run. A name
// repeated in one execution, typically in a loop, is numbered: charge, charge#2...
// Names must not contain '#'. Keep the order of repeated names deterministic.
type Context struct {
	context.Context
	act *activation
}

// RunID is the ID of the run being executed.
func (wf *Context) RunID() string {
	return wf.act.runID
}

// Step runs fn once and records its result. Replays return the recorded result
// without calling fn. A failed attempt is retried as Retry allows; after the last
// one, Step returns a *StepError. fn receives a context bounded by Timeout and
// carrying IdempotencyKey.
func (wf *Context) Step[T any](name string, fn func(ctx context.Context) (T, error), options ...StepOption) (T, error) {
	key, err := wf.act.stepKey(name)
	if err != nil {
		var zero T
		return zero, err
	}
	return runStep(wf.act, key, options, false, func(ctx context.Context, _ *sql.Tx) (T, error) {
		return fn(ctx)
	})
}

// TxStep runs fn in a transaction that also records its result, so its database
// writes and the checkpoint commit together: they happen exactly once. A failed
// attempt rolls back before it is retried.
func (wf *Context) TxStep[T any](name string, fn func(ctx context.Context, tx *sql.Tx) (T, error), options ...StepOption) (T, error) {
	key, err := wf.act.stepKey(name)
	if err != nil {
		var zero T
		return zero, err
	}
	return runStep(wf.act, key, options, true, fn)
}

// Async runs a step like Step, concurrently with the rest of the function. Wait for
// its result with Future.Wait; the run does not finish before its async steps do.
func (wf *Context) Async[T any](name string, fn func(ctx context.Context) (T, error), options ...StepOption) *Future[T] {
	key, err := wf.act.stepKey(name)
	if err != nil {
		return failedFuture[T](err)
	}
	done := make(chan struct{})
	var value T
	var stepErr error
	wf.act.async.Go(func() {
		defer close(done)
		value, stepErr = runStep(wf.act, key, options, false, func(ctx context.Context, _ *sql.Tx) (T, error) {
			return fn(ctx)
		})
	})
	return newFuture(func() (T, error) {
		<-done
		return value, stepErr
	})
}

// Sleep pauses the run for duration, counted from the first time the run reaches
// this sleep. The worker is free meanwhile.
func (wf *Context) Sleep(name string, duration time.Duration) error {
	return wf.sleep(name, duration, time.Time{})
}

// SleepUntil pauses the run until wakeAt. The worker is free meanwhile.
func (wf *Context) SleepUntil(name string, wakeAt time.Time) error {
	return wf.sleep(name, 0, wakeAt)
}

func (wf *Context) sleep(name string, duration time.Duration, wakeAt time.Time) error {
	act := wf.act
	key, err := act.stepKey(name)
	if err != nil {
		return err
	}
	if err := act.stopped(); err != nil {
		return err
	}
	cp, recorded, err := act.recorded(key, KindSleep)
	if err != nil {
		return err
	}
	if !recorded {
		var absolute any
		if !wakeAt.IsZero() {
			absolute = wakeAt
		}
		cp, err = act.insertCheckpoint(key, `
INSERT INTO pgworkflow_steps (run_id, name, kind, status, wake_at)
VALUES ($1, $2, 'sleep', 'waiting', COALESCE($3::timestamptz, NOW() + make_interval(secs => $4)))
ON CONFLICT (run_id, name) DO UPDATE SET updated_at = NOW()
RETURNING `+checkpointColumns, absolute, duration.Seconds())
		if err != nil {
			return err
		}
	}
	if cp.status == StepSucceeded {
		return nil
	}
	if cp.wake.passed() {
		return act.completeCheckpoint(key, cp, StepSucceeded, nil)
	}
	return act.suspendOn(wakeCondition{wake: cp.wake})
}

// Receive returns the next signal of its kind sent to the run, waiting at most
// timeout from the first time the run reaches this receive. It returns ErrTimeout
// when none arrives in time; pass Forever to wait without a timeout. Signals sent
// before the run waits are kept in order, each received once.
func (wf *Context) Receive[T any](signal Signal[T], timeout time.Duration) (T, error) {
	var zero T
	act := wf.act
	key, err := act.stepKey(signal.name)
	if err != nil {
		return zero, err
	}
	if err := act.stopped(); err != nil {
		return zero, err
	}
	cp, recorded, err := act.recorded(key, KindSignal)
	if err != nil {
		return zero, err
	}
	if !recorded {
		var waitSeconds any
		if timeout != Forever {
			waitSeconds = max(timeout, 0).Seconds()
		}
		cp, err = act.insertCheckpoint(key, `
INSERT INTO pgworkflow_steps (run_id, name, kind, status, signal, wake_at)
VALUES ($1, $2, 'signal', 'waiting', $3, NOW() + make_interval(secs => $4::float8))
ON CONFLICT (run_id, name) DO UPDATE SET updated_at = NOW()
RETURNING `+checkpointColumns, signal.name, waitSeconds)
		if err != nil {
			return zero, err
		}
	}
	switch cp.status {
	case StepSucceeded:
		return decodeRecorded[T](act, key, cp.output)
	case StepTimedOut:
		return zero, fmt.Errorf("%w: no %q signal arrived", ErrTimeout, signal.name)
	}
	payload, received, err := act.receiveSignal(key, cp)
	if err != nil {
		return zero, err
	}
	if received {
		return decodeRecorded[T](act, key, payload)
	}
	if cp.wake.passed() {
		if err := act.completeCheckpoint(key, cp, StepTimedOut, nil); err != nil {
			return zero, err
		}
		return zero, fmt.Errorf("%w: no %q signal arrived", ErrTimeout, signal.name)
	}
	return zero, act.suspendOn(wakeCondition{signal: signal.name, wake: cp.wake})
}

// Start starts child as a child run and returns its future result. The child runs on
// whichever worker lists its workflow; Future.Wait pauses the run until it finishes.
// A child that fails, times out or is cancelled makes Wait return a *RunError.
// Cancelling or failing the parent cancels its unfinished children. Timeout bounds
// the child run; a child is keyed by its step name, so Key is refused.
func (wf *Context) Start[In, Out any](name string, child *Workflow[In, Out], input In, options ...StartOption) *Future[Out] {
	act := wf.act
	key, err := act.stepKey(name)
	if err != nil {
		return failedFuture[Out](err)
	}
	config := startConfig{}
	for _, option := range options {
		option.applyToStart(&config)
	}
	if config.key != "" {
		return failedFuture[Out](fmt.Errorf("workflow: child %q is keyed by its step name and takes no Key", key))
	}
	config.key = act.runID + "/" + key
	if err := act.startChild(key, child.definition, input, config); err != nil {
		return failedFuture[Out](err)
	}
	return newFuture(func() (Out, error) {
		return awaitChild[Out](act, key)
	})
}

// Call starts child as a child run and waits for its result.
func (wf *Context) Call[In, Out any](name string, child *Workflow[In, Out], input In, options ...StartOption) (Out, error) {
	return wf.Start(name, child, input, options...).Wait()
}

type stepInfoKey struct{}

type stepInfo struct {
	runID string
	key   string
}

// IdempotencyKey identifies the step running with ctx across its attempts and
// replays, as "<run id>/<step name>". Pass it to external systems so a repeated
// attempt does not repeat their side effect. It is empty outside a step.
func IdempotencyKey(ctx context.Context) string {
	info, ok := ctx.Value(stepInfoKey{}).(stepInfo)
	if !ok {
		return ""
	}
	return info.runID + "/" + info.key
}

type attempt[T any] struct {
	value   T
	failure error
	stop    error
}

func runStep[T any](act *activation, key string, options []StepOption, transactional bool, fn func(context.Context, *sql.Tx) (T, error)) (T, error) {
	var zero T
	if err := act.stopped(); err != nil {
		return zero, err
	}
	config := act.stepConfig(options)
	cp, recorded, err := act.recorded(key, KindStep)
	if err != nil {
		return zero, err
	}
	if recorded {
		switch cp.status {
		case StepSucceeded:
			return decodeRecorded[T](act, key, cp.output)
		case StepFailed:
			return zero, &StepError{Step: key, Attempts: cp.attempts, Message: cp.errMessage}
		case StepRetrying:
			if err := act.waitForRetry(cp.wake); err != nil {
				return zero, err
			}
		case StepRunning:
			retry, err := act.recordFailedAttempt(key, cp.attempts, errAttemptLost, config.retry)
			if err != nil {
				return zero, err
			}
			if retry.final != nil {
				return zero, retry.final
			}
			if err := act.waitForRetry(retry.wake); err != nil {
				return zero, err
			}
		}
	}
	for {
		if err := act.stopped(); err != nil {
			return zero, err
		}
		number, err := act.markAttemptStarted(key)
		if err != nil {
			return zero, err
		}
		result := runAttempt(act, key, config, transactional, fn)
		if result.stop != nil {
			return zero, result.stop
		}
		if result.failure == nil {
			return result.value, nil
		}
		retry, err := act.recordFailedAttempt(key, number, result.failure, config.retry)
		if err != nil {
			return zero, err
		}
		if retry.final != nil {
			return zero, retry.final
		}
		if err := act.waitForRetry(retry.wake); err != nil {
			return zero, err
		}
	}
}

func runAttempt[T any](act *activation, key string, config stepConfig, transactional bool, fn func(context.Context, *sql.Tx) (T, error)) attempt[T] {
	stepCtx := context.WithValue(act.runCtx, stepInfoKey{}, stepInfo{runID: act.runID, key: key})
	if config.timeout > 0 {
		var cancel context.CancelFunc
		stepCtx, cancel = context.WithTimeout(stepCtx, config.timeout)
		defer cancel()
	}
	if !transactional {
		value, err := callStep(act, stepCtx, fn, nil)
		if err != nil && act.runCtx.Err() != nil {
			return attempt[T]{stop: act.interruptAttempt(key)}
		}
		if err != nil {
			return attempt[T]{failure: err}
		}
		output, err := json.Marshal(value)
		if err != nil {
			return attempt[T]{failure: NonRetryable(fmt.Errorf("encode %T result: %w", value, err))}
		}
		if err := act.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
			return act.recordSucceededStepTx(ctx, tx, key, output)
		}); err != nil {
			return attempt[T]{stop: err}
		}
		return attempt[T]{value: value}
	}

	bookkeepingCtx, cancel := act.bookkeepingContext()
	defer cancel()
	tx, err := act.fencedTx(bookkeepingCtx)
	if err != nil {
		return attempt[T]{stop: err}
	}
	defer func() { _ = tx.Rollback() }()
	value, err := callStep(act, stepCtx, fn, tx)
	if err != nil && act.runCtx.Err() != nil {
		_ = tx.Rollback()
		return attempt[T]{stop: act.interruptAttempt(key)}
	}
	if err != nil {
		return attempt[T]{failure: err}
	}
	output, err := json.Marshal(value)
	if err != nil {
		return attempt[T]{failure: NonRetryable(fmt.Errorf("encode %T result: %w", value, err))}
	}
	if err := act.recordSucceededStepTx(bookkeepingCtx, tx, key, output); err != nil {
		return attempt[T]{stop: act.infrastructureFailure(err)}
	}
	if err := tx.Commit(); err != nil {
		return attempt[T]{failure: fmt.Errorf("commit: %w", err)}
	}
	return attempt[T]{value: value}
}

func callStep[T any](act *activation, ctx context.Context, fn func(context.Context, *sql.Tx) (T, error), tx *sql.Tx) (value T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			act.client.log.Error(ctx, "workflow step panicked", map[string]any{
				"run_id": act.runID, "workflow": act.def.name, "panic": fmt.Sprint(recovered), "stack": stackTrace(),
			})
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return fn(ctx, tx)
}

func stackTrace() string {
	return string(debug.Stack())
}

func (a *activation) stepConfig(options []StepOption) stepConfig {
	config := stepConfig{}
	for _, option := range options {
		option.applyToStep(&config)
	}
	if config.retry == nil {
		defaults := a.def.retry
		config.retry = &defaults
	}
	withDefaults := config.retry.withDefaults()
	config.retry = &withDefaults
	return config
}

func (a *activation) insertCheckpoint(key, query string, args ...any) (checkpoint, error) {
	var cp checkpoint
	err := a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
		_, scanned, err := scanCheckpoint(tx.QueryRowContext(ctx, query, append([]any{a.runID, key}, args...)...))
		if err != nil {
			return fmt.Errorf("workflow: record %q: %w", key, err)
		}
		cp = scanned
		return nil
	})
	if err != nil {
		return checkpoint{}, err
	}
	a.remember(key, cp)
	return cp, nil
}

func (a *activation) completeCheckpoint(key string, cp checkpoint, status StepStatus, output json.RawMessage) error {
	var outputText any
	if output != nil {
		outputText = string(output)
	}
	err := a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_steps
SET status = $3, output = $4::jsonb, completed_at = NOW(), updated_at = NOW()
WHERE run_id = $1 AND name = $2`, a.runID, key, status, outputText)
		return err
	})
	if err != nil {
		return err
	}
	cp.status, cp.output = status, output
	a.remember(key, cp)
	return nil
}

func (a *activation) markAttemptStarted(key string) (int, error) {
	var number int
	err := a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `
INSERT INTO pgworkflow_steps (run_id, name, kind, status, attempts)
VALUES ($1, $2, 'step', 'running', 1)
ON CONFLICT (run_id, name) DO UPDATE
SET status = 'running', attempts = pgworkflow_steps.attempts + 1, wake_at = NULL, updated_at = NOW()
RETURNING attempts`, a.runID, key).Scan(&number)
	})
	if err != nil {
		return 0, err
	}
	a.remember(key, checkpoint{kind: KindStep, status: StepRunning, attempts: number})
	return number, nil
}

func (a *activation) recordSucceededStepTx(ctx context.Context, tx *sql.Tx, key string, output json.RawMessage) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_steps
SET status = 'succeeded', output = $3::jsonb, error = NULL, wake_at = NULL, completed_at = NOW(), updated_at = NOW()
WHERE run_id = $1 AND name = $2`, a.runID, key, string(output)); err != nil {
		return fmt.Errorf("workflow: record %q result: %w", key, err)
	}
	a.mu.Lock()
	cp := a.checkpoints[key]
	cp.status, cp.output = StepSucceeded, output
	a.checkpoints[key] = cp
	a.mu.Unlock()
	return nil
}

type retryPlan struct {
	final *StepError
	wake  deadline
}

// recordFailedAttempt records the failure of attempt number and decides whether the
// step fails for good or retries, and when.
func (a *activation) recordFailedAttempt(key string, number int, failure error, retry *Retry) (retryPlan, error) {
	message := failure.Error()
	if isNonRetryable(failure) || number >= retry.MaxAttempts {
		err := a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_steps
SET status = 'failed', error = $3, wake_at = NULL, completed_at = NOW(), updated_at = NOW()
WHERE run_id = $1 AND name = $2`, a.runID, key, message)
			return err
		})
		if err != nil {
			return retryPlan{}, err
		}
		a.remember(key, checkpoint{kind: KindStep, status: StepFailed, attempts: number, errMessage: message})
		return retryPlan{final: &StepError{Step: key, Attempts: number, Message: message}}, nil
	}
	var cp checkpoint
	err := a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
		var err error
		_, cp, err = scanCheckpoint(tx.QueryRowContext(ctx, `
UPDATE pgworkflow_steps
SET status = 'retrying', error = $3, wake_at = NOW() + make_interval(secs => $4), updated_at = NOW()
WHERE run_id = $1 AND name = $2
RETURNING `+checkpointColumns, a.runID, key, message, retry.delayAfter(number).Seconds()))
		return err
	})
	if err != nil {
		return retryPlan{}, err
	}
	a.remember(key, cp)
	return retryPlan{wake: cp.wake}, nil
}

// waitForRetry waits in process for a retry due within a second, and suspends the
// run for a later one.
func (a *activation) waitForRetry(wake deadline) error {
	remaining := time.Until(wake.dueAt)
	if remaining <= 0 {
		return nil
	}
	if remaining > inProcessRetryLimit || a.stopped() != nil {
		return a.suspendOn(wakeCondition{wake: wake})
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-a.runCtx.Done():
		return a.interruption()
	}
}

// interruptAttempt stops the activation after the run context ended mid-attempt.
// On shutdown the attempt is handed back uncounted, as it was not the step's fault.
func (a *activation) interruptAttempt(key string) error {
	err := a.interruption()
	if reason, _ := a.stopState(); reason == shuttingDown {
		_ = a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_steps
SET status = 'retrying', attempts = attempts - 1, wake_at = NOW(), updated_at = NOW()
WHERE run_id = $1 AND name = $2 AND status = 'running'`, a.runID, key)
			return err
		})
	}
	return err
}

// receiveSignal takes the oldest signal of the receive's kind not yet received and
// records it as the receive's result.
func (a *activation) receiveSignal(key string, cp checkpoint) (json.RawMessage, bool, error) {
	var payload []byte
	err := a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `
WITH next AS (
    SELECT id, payload FROM pgworkflow_signals
    WHERE run_id = $1 AND name = $3 AND received_by IS NULL
    ORDER BY id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
), received AS (
    UPDATE pgworkflow_signals SET received_by = $2
    FROM next WHERE pgworkflow_signals.id = next.id
    RETURNING next.payload
)
UPDATE pgworkflow_steps
SET status = 'succeeded', output = received.payload, completed_at = NOW(), updated_at = NOW()
FROM received
WHERE run_id = $1 AND name = $2
RETURNING received.payload`, a.runID, key, cp.signal).Scan(&payload)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	if err != nil || payload == nil {
		return nil, false, err
	}
	cp.status, cp.output = StepSucceeded, payload
	a.remember(key, cp)
	return payload, true, nil
}

func (a *activation) startChild(key string, child *definition, input any, config startConfig) error {
	if err := a.stopped(); err != nil {
		return err
	}
	cp, recorded, err := a.recorded(key, KindChild)
	if err != nil || recorded {
		return err
	}
	encodedInput, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("workflow: encode %T input of child %q: %w", input, key, err)
	}
	err = a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
		childRunID, err := a.client.insertRun(ctx, tx, child, encodedInput, config, a.runID, key)
		if err != nil {
			return err
		}
		_, cp, err = scanCheckpoint(tx.QueryRowContext(ctx, `
INSERT INTO pgworkflow_steps (run_id, name, kind, status, child_run_id)
VALUES ($1, $2, 'child', 'waiting', $3)
ON CONFLICT (run_id, name) DO UPDATE SET updated_at = NOW()
RETURNING `+checkpointColumns, a.runID, key, childRunID))
		return err
	})
	if err != nil {
		return err
	}
	a.remember(key, cp)
	return nil
}

func awaitChild[Out any](act *activation, key string) (Out, error) {
	var zero Out
	if err := act.stopped(); err != nil {
		return zero, err
	}
	cp, recorded, err := act.recorded(key, KindChild)
	if err != nil {
		return zero, err
	}
	if !recorded {
		return zero, fmt.Errorf("workflow: child %q was not started", key)
	}
	if cp.status == StepWaiting {
		ctx, cancel := act.bookkeepingContext()
		child, err := act.client.GetRun(ctx, cp.childRunID)
		cancel()
		if err != nil {
			return zero, act.infrastructureFailure(err)
		}
		if !child.Status.Finished() {
			return zero, act.suspendOn(wakeCondition{childRunID: cp.childRunID})
		}
		cp, err = act.recordChildOutcome(key, cp, child)
		if err != nil {
			return zero, err
		}
	}
	if cp.status == StepFailed {
		return zero, childRunError(cp)
	}
	return decodeRecorded[Out](act, key, cp.output)
}

func (a *activation) recordChildOutcome(key string, cp checkpoint, child RunInfo) (checkpoint, error) {
	outcome := childOutcome{Status: child.Status, TimedOut: child.TimedOut, Message: child.Error, Workflow: child.Workflow}
	status, output := StepSucceeded, child.Output
	if child.Status != RunSucceeded {
		status = StepFailed
		output, _ = json.Marshal(outcome)
	}
	var outputText any
	if output != nil {
		outputText = string(output)
	}
	err := a.checkpointWrite(func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_steps
SET status = $3, output = $4::jsonb, error = NULLIF($5, ''), completed_at = NOW(), updated_at = NOW()
WHERE run_id = $1 AND name = $2`, a.runID, key, status, outputText, child.Error)
		return err
	})
	if err != nil {
		return checkpoint{}, err
	}
	cp.status, cp.output, cp.errMessage = status, output, child.Error
	a.remember(key, cp)
	return cp, nil
}

// childOutcome is what a failed child step records, so the parent rebuilds the same
// RunError on every replay.
type childOutcome struct {
	Workflow string    `json:"workflow"`
	Status   RunStatus `json:"status"`
	TimedOut bool      `json:"timed_out,omitempty"`
	Message  string    `json:"message"`
}

func childRunError(cp checkpoint) error {
	var outcome childOutcome
	_ = json.Unmarshal(cp.output, &outcome)
	return &RunError{
		RunID:    cp.childRunID,
		Workflow: outcome.Workflow,
		Status:   outcome.Status,
		TimedOut: outcome.TimedOut,
		Message:  outcome.Message,
	}
}
