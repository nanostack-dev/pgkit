package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/nanostack-dev/pgkit/queue"
)

// An activation is one execution of a run by a worker, from claiming the run's job
// to finishing, parking or releasing it. The workflow function may execute several
// times in one activation when a condition it waits for is already met.
type activation struct {
	client      *Client
	def         *definition
	config      WorkerConfig
	job         queue.Job
	runID       string
	lease       int64
	input       json.RawMessage
	parentRunID string
	deadline    deadline

	mu          sync.Mutex
	checkpoints map[string]checkpoint
	occurrences map[string]int
	stop        stopReason
	stopErr     error
	conditions  []wakeCondition
	async       sync.WaitGroup
	runCtx      context.Context
}

// deadline is a database time and the local instant it falls due, so waiting on it
// uses the database clock once and the monotonic clock afterwards.
type deadline struct {
	at    time.Time
	dueAt time.Time
}

func (d deadline) isSet() bool  { return !d.at.IsZero() }
func (d deadline) passed() bool { return d.isSet() && !time.Now().Before(d.dueAt) }

type checkpoint struct {
	kind       StepKind
	status     StepStatus
	attempts   int
	output     json.RawMessage
	errMessage string
	wake       deadline
	childRunID string
	signal     string
}

// wakeCondition is something a suspended run waits for. The park transaction checks
// all of them before parking, and parks only when none is met.
type wakeCondition struct {
	signal     string
	childRunID string
	wake       deadline
}

// stopReason ranks why an activation stops starting new work; a stronger reason
// replaces a weaker one.
type stopReason int

const (
	running stopReason = iota
	suspended
	infrastructureFailed
	nonDeterministic
	timedOut
	cancelled
	shuttingDown
	leaseLost
)

var (
	errLeaseLost   = errors.New("workflow: another worker took over the run")
	errRunTimedOut = fmt.Errorf("%w: the run outlived its timeout", ErrTimeout)
	errAttemptLost = errors.New("the attempt did not report back: its worker stopped")
)

// dueNow is a past time: SnoozeTx makes a job snoozed until then due immediately.
var dueNow = time.Unix(0, 0).UTC()

const (
	bookkeepingTimeout  = 30 * time.Second
	inProcessRetryLimit = time.Second
	maxReplays          = 100
)

func (a *activation) stopWith(reason stopReason, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if reason > a.stop {
		a.stop, a.stopErr = reason, err
	}
	return a.stopErr
}

// stopped reports why the activation must not start new work, if it must: a
// suspension, a failure, or the end of the run context.
func (a *activation) stopped() error {
	a.mu.Lock()
	stopErr := a.stopErr
	a.mu.Unlock()
	if stopErr == nil && a.runCtx.Err() != nil {
		return a.interruption()
	}
	return stopErr
}

func (a *activation) stopState() (stopReason, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stop, a.stopErr
}

func (a *activation) infrastructureFailure(err error) error {
	return a.stopWith(infrastructureFailed, err)
}

func (a *activation) nonDeterminism(err error) error {
	return a.stopWith(nonDeterministic, err)
}

func (a *activation) suspendOn(condition wakeCondition) error {
	a.mu.Lock()
	a.conditions = append(a.conditions, condition)
	a.mu.Unlock()
	return a.stopWith(suspended, ErrSuspended)
}

// interruption turns the cause of a cancelled run context into the reason the
// activation stops.
func (a *activation) interruption() error {
	switch cause := context.Cause(a.runCtx); {
	case errors.Is(cause, ErrCancelled):
		return a.stopWith(cancelled, ErrCancelled)
	case errors.Is(cause, errRunTimedOut):
		return a.stopWith(timedOut, errRunTimedOut)
	case errors.Is(cause, errLeaseLost):
		return a.stopWith(leaseLost, ErrSuspended)
	default:
		return a.stopWith(shuttingDown, ErrSuspended)
	}
}

func (a *activation) resetForReplay() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.occurrences = map[string]int{}
	a.conditions = nil
	a.stop, a.stopErr = running, nil
}

// stepKey names the checkpoint of the next operation called name. Repeating a name
// in one execution, typically in a loop, numbers it: name, name#2, name#3.
func (a *activation) stepKey(name string) (string, error) {
	if !validName(name) {
		return "", fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.occurrences[name]++
	if n := a.occurrences[name]; n > 1 {
		return fmt.Sprintf("%s#%d", name, n), nil
	}
	return name, nil
}

// recorded returns the checkpoint of key, failing the run when it records another
// kind of operation than the code now runs under that name.
func (a *activation) recorded(key string, kind StepKind) (checkpoint, bool, error) {
	a.mu.Lock()
	cp, ok := a.checkpoints[key]
	a.mu.Unlock()
	if ok && cp.kind != kind {
		err := fmt.Errorf("%w: %q was recorded as a %s, the code now runs it as a %s", ErrNonDeterministic, key, cp.kind, kind)
		return checkpoint{}, false, a.nonDeterminism(err)
	}
	return cp, ok, nil
}

func (a *activation) remember(key string, cp checkpoint) {
	a.mu.Lock()
	a.checkpoints[key] = cp
	a.mu.Unlock()
}

func decodeRecorded[T any](a *activation, key string, raw json.RawMessage) (T, error) {
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		err = fmt.Errorf("%w: the value recorded for %q does not decode into %T: %v", ErrNonDeterministic, key, value, err)
		return value, a.nonDeterminism(err)
	}
	return value, nil
}

func (a *activation) bookkeepingContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(a.runCtx), bookkeepingTimeout)
}

// fencedTx begins the transaction of a checkpoint write. It share-locks the run row
// after checking that this activation still owns the run, so a worker that took
// over, or a cancellation, waits for the write and then wins.
func (a *activation) fencedTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := a.client.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, a.infrastructureFailure(fmt.Errorf("workflow: begin checkpoint: %w", err))
	}
	var status RunStatus
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM pgworkflow_runs WHERE id = $1 AND lease = $2 FOR SHARE`,
		a.runID, a.lease,
	).Scan(&status)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_ = tx.Rollback()
		return nil, a.stopWith(leaseLost, ErrSuspended)
	case err != nil:
		_ = tx.Rollback()
		return nil, a.infrastructureFailure(fmt.Errorf("workflow: lock run: %w", err))
	case status == RunCancelled:
		_ = tx.Rollback()
		return nil, a.stopWith(cancelled, ErrCancelled)
	case status != RunRunning:
		_ = tx.Rollback()
		return nil, a.stopWith(leaseLost, ErrSuspended)
	}
	return tx, nil
}

// checkpointWrite runs write in a fenced transaction and commits it.
func (a *activation) checkpointWrite(write func(ctx context.Context, tx *sql.Tx) error) error {
	ctx, cancel := a.bookkeepingContext()
	defer cancel()
	tx, err := a.fencedTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := write(ctx, tx); err != nil {
		return a.infrastructureFailure(err)
	}
	if err := tx.Commit(); err != nil {
		return a.infrastructureFailure(fmt.Errorf("workflow: commit checkpoint: %w", err))
	}
	return nil
}

const checkpointColumns = `name, kind, status, attempts, output, COALESCE(error, ''), wake_at,
	EXTRACT(EPOCH FROM wake_at - NOW())::float8, COALESCE(child_run_id, ''), COALESCE(signal, '')`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanCheckpoint(row rowScanner) (string, checkpoint, error) {
	var (
		key    string
		cp     checkpoint
		output []byte
		wakeAt sql.NullTime
		wakeIn sql.NullFloat64
	)
	err := row.Scan(&key, &cp.kind, &cp.status, &cp.attempts, &output, &cp.errMessage, &wakeAt, &wakeIn, &cp.childRunID, &cp.signal)
	if err != nil {
		return "", checkpoint{}, err
	}
	cp.output = output
	cp.wake = deadlineFrom(wakeAt, wakeIn)
	return key, cp, nil
}

func deadlineFrom(at sql.NullTime, secondsLeft sql.NullFloat64) deadline {
	if !at.Valid {
		return deadline{}
	}
	return deadline{at: at.Time, dueAt: time.Now().Add(time.Duration(secondsLeft.Float64 * float64(time.Second)))}
}

func (a *activation) loadCheckpoints(ctx context.Context) error {
	rows, err := a.client.db.QueryContext(ctx,
		`SELECT `+checkpointColumns+` FROM pgworkflow_steps WHERE run_id = $1`, a.runID)
	if err != nil {
		return fmt.Errorf("workflow: load checkpoints: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		key, cp, err := scanCheckpoint(rows)
		if err != nil {
			return fmt.Errorf("workflow: scan checkpoint: %w", err)
		}
		a.checkpoints[key] = cp
	}
	return rows.Err()
}

// execute drives the run until it finishes, parks, is released or stops. It returns
// queue.Handled for every outcome it settled itself and an error for the queue to
// retry the job after an infrastructure failure.
func (a *activation) execute(workerCtx context.Context) error {
	if a.deadline.passed() {
		return a.finishFailed(errRunTimedOut.Error(), true)
	}
	runCtx, cancel := context.WithCancelCause(workerCtx)
	defer cancel(nil)
	if a.deadline.isSet() {
		var cancelDeadline context.CancelFunc
		runCtx, cancelDeadline = context.WithDeadlineCause(runCtx, a.deadline.dueAt, errRunTimedOut)
		defer cancelDeadline()
	}
	a.runCtx = runCtx
	stopWatching := a.watch(cancel)
	defer stopWatching()

	for replay := 0; ; replay++ {
		var output json.RawMessage
		var err error
		if a.runCtx.Err() != nil {
			a.interruption()
		} else {
			output, err = a.executeFunction()
		}
		if reason, _ := a.stopState(); reason == infrastructureFailed && a.runCtx.Err() != nil {
			a.interruption()
		}
		reason, stopErr := a.stopState()
		switch reason {
		case leaseLost:
			return queue.Handled()
		case shuttingDown:
			return a.release()
		case cancelled:
			return a.acknowledgeCancellation()
		case timedOut:
			return a.finishFailed(errRunTimedOut.Error(), true)
		case nonDeterministic:
			return a.finishFailed(stopErr.Error(), false)
		case infrastructureFailed:
			return a.infrastructureOutcome(stopErr)
		case suspended:
			parked, err := a.park(replay)
			if err != nil {
				return a.infrastructureOutcome(err)
			}
			if parked {
				return queue.Handled()
			}
			a.resetForReplay()
			continue
		}
		if err != nil {
			return a.finishFailed(err.Error(), false)
		}
		return a.finishSucceeded(output)
	}
}

func (a *activation) executeFunction() (output json.RawMessage, err error) {
	wf := &Context{Context: a.runCtx, act: a}
	defer a.async.Wait()
	defer func() {
		if recovered := recover(); recovered != nil {
			a.client.log.Error(a.runCtx, "workflow function panicked", map[string]any{
				"run_id": a.runID, "workflow": a.def.name, "panic": fmt.Sprint(recovered), "stack": stackTrace(),
			})
			output, err = nil, fmt.Errorf("workflow: panic: %v", recovered)
		}
	}()
	return a.def.execute(wf, a.input)
}

// watch heartbeats the job until the activation ends, also after the run context
// ended while a step finishes, and cancels the run context when the run is
// cancelled or another worker takes it over.
func (a *activation) watch(cancel context.CancelCauseFunc) (stop func()) {
	done := make(chan struct{})
	var stopped sync.WaitGroup
	var wake <-chan struct{}
	subscription, err := a.client.queue.Subscribe(runNotifyKey(a.runID))
	if err == nil {
		wake = subscription.Wake()
	}
	heartbeat := time.NewTicker(max(a.config.VisibilityTimeout/3, 10*time.Millisecond))
	stopped.Go(func() {
		defer heartbeat.Stop()
		for {
			select {
			case <-done:
				return
			case <-heartbeat.C:
				ctx, cancelBeat := a.bookkeepingContext()
				err := a.client.queue.Heartbeat(ctx, a.job)
				cancelBeat()
				if errors.Is(err, queue.ErrJobNotFound) {
					cancel(errLeaseLost)
					return
				}
				a.checkOwnership(cancel)
			case <-wake:
				a.checkOwnership(cancel)
			}
		}
	})
	return func() {
		close(done)
		stopped.Wait()
		if subscription != nil {
			subscription.Close()
		}
	}
}

func (a *activation) checkOwnership(cancel context.CancelCauseFunc) {
	ctx, cancelCheck := a.bookkeepingContext()
	defer cancelCheck()
	var status RunStatus
	var lease int64
	err := a.client.db.QueryRowContext(ctx,
		`SELECT status, lease FROM pgworkflow_runs WHERE id = $1`, a.runID).Scan(&status, &lease)
	switch {
	case err != nil:
		return
	case status == RunCancelled:
		cancel(ErrCancelled)
	case lease != a.lease:
		cancel(errLeaseLost)
	}
}

// park records that the run waits, unless one of its wake conditions is already
// met, in which case it reports parked false and the function executes again.
func (a *activation) park(replay int) (parked bool, err error) {
	a.mu.Lock()
	conditions := slices.Clone(a.conditions)
	a.mu.Unlock()

	ctx, cancel := a.bookkeepingContext()
	defer cancel()
	tx, err := a.client.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("workflow: begin park: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status RunStatus
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM pgworkflow_runs WHERE id = $1 AND lease = $2 FOR UPDATE`, a.runID, a.lease,
	).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && status != RunRunning && status != RunCancelled) {
		a.stopWith(leaseLost, ErrSuspended)
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("workflow: lock run to park: %w", err)
	}
	if status == RunCancelled {
		return true, a.acknowledgeCancellationTx(ctx, tx)
	}

	ready, err := anyConditionMet(ctx, tx, a.runID, conditions)
	if err != nil {
		return false, err
	}
	if ready && replay < maxReplays {
		return false, nil
	}
	next := earliestWake(conditions, a.deadline)
	if !ready && next.isSet() && time.Until(next.dueAt) <= inProcessRetryLimit && replay < maxReplays {
		_ = tx.Rollback()
		a.waitUntil(next)
		return false, nil
	}

	wake := next.at
	if ready {
		wake = dueNow
	}
	var wakeAt any
	if !wake.IsZero() {
		wakeAt = wake
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE pgworkflow_runs SET status = 'waiting', wake_at = $2, updated_at = NOW() WHERE id = $1`,
		a.runID, wakeAt,
	); err != nil {
		return false, fmt.Errorf("workflow: park run: %w", err)
	}
	if err := a.client.queue.SnoozeTx(ctx, tx, a.job, wake); err != nil {
		return false, fmt.Errorf("workflow: snooze run job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("workflow: commit park: %w", err)
	}
	return true, nil
}

func anyConditionMet(ctx context.Context, tx *sql.Tx, runID string, conditions []wakeCondition) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}
	signals, children := []string{}, []string{}
	var earliest time.Time
	for _, condition := range conditions {
		switch {
		case condition.signal != "":
			signals = append(signals, condition.signal)
		case condition.childRunID != "":
			children = append(children, condition.childRunID)
		}
		if condition.wake.isSet() && (earliest.IsZero() || condition.wake.at.Before(earliest)) {
			earliest = condition.wake.at
		}
	}
	var earliestAt any
	if !earliest.IsZero() {
		earliestAt = earliest
	}
	signalsJSON, _ := json.Marshal(signals)
	childrenJSON, _ := json.Marshal(children)
	var met bool
	err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
           SELECT 1 FROM pgworkflow_signals
           WHERE run_id = $1 AND received_by IS NULL
             AND name IN (SELECT jsonb_array_elements_text($2::jsonb)))
    OR EXISTS (
           SELECT 1 FROM pgworkflow_runs
           WHERE id IN (SELECT jsonb_array_elements_text($3::jsonb))
             AND status IN ('succeeded', 'failed', 'cancelled'))
    OR COALESCE($4::timestamptz <= NOW(), FALSE)`,
		runID, string(signalsJSON), string(childrenJSON), earliestAt,
	).Scan(&met)
	if err != nil {
		return false, fmt.Errorf("workflow: check wake conditions: %w", err)
	}
	return met, nil
}

func earliestWake(conditions []wakeCondition, runDeadline deadline) deadline {
	earliest := runDeadline
	for _, condition := range conditions {
		if condition.wake.isSet() && (!earliest.isSet() || condition.wake.at.Before(earliest.at)) {
			earliest = condition.wake
		}
	}
	return earliest
}

// waitUntil waits in process for a wake time close enough that parking the run
// would cost more than waiting, unless the run context ends first.
func (a *activation) waitUntil(wake deadline) {
	timer := time.NewTimer(time.Until(wake.dueAt))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-a.runCtx.Done():
	}
}

// release hands the run back to the queue when the worker shuts down, so another
// worker resumes it now instead of after the visibility timeout.
func (a *activation) release() error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.runCtx), bookkeepingTimeout)
	defer cancel()
	tx, err := a.client.db.BeginTx(ctx, nil)
	if err != nil {
		return queue.Handled()
	}
	defer func() { _ = tx.Rollback() }()
	var owned bool
	if err := tx.QueryRowContext(ctx,
		`SELECT TRUE FROM pgworkflow_runs WHERE id = $1 AND lease = $2 FOR UPDATE`, a.runID, a.lease,
	).Scan(&owned); err != nil {
		return queue.Handled()
	}
	if err := a.client.queue.SnoozeTx(ctx, tx, a.job, dueNow); err != nil {
		return queue.Handled()
	}
	_ = tx.Commit()
	return queue.Handled()
}

func (a *activation) acknowledgeCancellation() error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.runCtx), bookkeepingTimeout)
	defer cancel()
	if err := a.client.queue.Ack(ctx, a.job.ID); err != nil && !errors.Is(err, queue.ErrJobNotFound) {
		return err
	}
	return queue.Handled()
}

func (a *activation) acknowledgeCancellationTx(ctx context.Context, tx *sql.Tx) error {
	if err := a.client.queue.AckTx(ctx, tx, a.job.ID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workflow: commit cancelled run: %w", err)
	}
	a.stopWith(cancelled, ErrCancelled)
	return nil
}

// infrastructureOutcome lets the queue retry the job, unless the job is no longer
// this activation's, or it was the last attempt, in which case the run fails.
func (a *activation) infrastructureOutcome(cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.runCtx), bookkeepingTimeout)
	defer cancel()
	if err := a.client.queue.Heartbeat(ctx, a.job); errors.Is(err, queue.ErrJobNotFound) {
		return queue.Handled()
	}
	a.client.log.Warn(ctx, "workflow activation failed, the queue retries it", map[string]any{
		"run_id": a.runID, "workflow": a.def.name, "attempt": a.job.Attempts, "error": cause.Error(),
	})
	if a.job.Attempts >= a.job.MaxAttempts {
		message := fmt.Sprintf("gave up after %d failed activations: %v", a.job.Attempts, cause)
		if err := a.finishFailed(message, false); err == nil || queue.IsHandled(err) {
			return queue.Handled()
		}
	}
	return cause
}

func (a *activation) finishSucceeded(output json.RawMessage) error {
	return a.finish(func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_runs
SET status = 'succeeded', output = $2, completed_at = NOW(), wake_at = NULL, updated_at = NOW()
WHERE id = $1`, a.runID, string(output))
		return err
	}, nil)
}

func (a *activation) finishFailed(message string, timedOut bool) error {
	return a.finish(func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
UPDATE pgworkflow_runs
SET status = 'failed', error = $2, timed_out = $3, completed_at = NOW(), wake_at = NULL, updated_at = NOW()
WHERE id = $1`, a.runID, message, timedOut); err != nil {
			return err
		}
		return a.client.cancelDescendantsTx(ctx, tx, a.runID)
	}, func(ctx context.Context) {
		if a.config.OnRunFailed == nil {
			return
		}
		if info, err := a.client.GetRun(ctx, a.runID); err == nil {
			a.config.OnRunFailed(ctx, info)
		}
	})
}

// finish moves the run to a final status, wakes its parent and settles its job in
// one transaction. It locks the parent before the run, the order every transaction
// touching two runs follows.
func (a *activation) finish(update func(ctx context.Context, tx *sql.Tx) error, afterCommit func(ctx context.Context)) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.runCtx), bookkeepingTimeout)
	defer cancel()
	tx, err := a.client.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workflow: begin finish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if a.parentRunID != "" {
		if err := a.client.wakeParentTx(ctx, tx, a.parentRunID); err != nil {
			return err
		}
	}
	var status RunStatus
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM pgworkflow_runs WHERE id = $1 AND lease = $2 FOR UPDATE`, a.runID, a.lease,
	).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return queue.Handled()
	}
	if err != nil {
		return fmt.Errorf("workflow: lock run to finish: %w", err)
	}
	if status == RunCancelled {
		if err := a.acknowledgeCancellationTx(ctx, tx); err != nil {
			return err
		}
		return queue.Handled()
	}
	if status != RunRunning {
		return queue.Handled()
	}
	if err := update(ctx, tx); err != nil {
		return fmt.Errorf("workflow: finish run: %w", err)
	}
	if err := a.client.queue.AckTx(ctx, tx, a.job.ID); err != nil {
		if errors.Is(err, queue.ErrJobNotFound) {
			return queue.Handled()
		}
		return err
	}
	if err := a.client.queue.NotifyTx(ctx, tx, runNotifyKey(a.runID)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workflow: commit finish: %w", err)
	}
	if afterCommit != nil {
		afterCommit(ctx)
	}
	return queue.Handled()
}

func runNotifyKey(runID string) string {
	return "pgworkflow.run:" + runID
}
