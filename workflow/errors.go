package workflow

import (
	"errors"
	"fmt"
)

var (
	ErrNilQueue            = errors.New("workflow: queue client is nil")
	ErrRunNotFound         = errors.New("workflow: run not found")
	ErrRunFinished         = errors.New("workflow: run already finished")
	ErrRunNotRetryable     = errors.New("workflow: only a failed or cancelled run can be retried")
	ErrInvalidName         = errors.New("workflow: names must be non-empty and must not contain '#'")
	ErrNoWorkflows         = errors.New("workflow: worker has no workflows")
	ErrDuplicateWorkflow   = errors.New("workflow: worker lists a workflow version twice")
	ErrInvalidWorkerConfig = errors.New("workflow: invalid worker config")

	// ErrSuspended is returned by durable operations when the run has to pause:
	// to sleep, to wait for a signal or a child run, or to back off before retrying
	// a step. Return it from the workflow function; the run resumes later from its
	// checkpoints. Once returned, every later durable operation returns it too.
	ErrSuspended = errors.New("workflow: run suspended")

	// ErrCancelled reports a cancelled run, from durable operations of the run itself
	// and from errors.Is on the RunError of Run.Result or Future.Wait.
	ErrCancelled = errors.New("workflow: run cancelled")

	// ErrTimeout reports a Receive that timed out, and through errors.Is a run that
	// outlived its Timeout.
	ErrTimeout = errors.New("workflow: timed out")

	// ErrNonDeterministic reports a run whose recorded checkpoints no longer match
	// the code: a step name now used for another kind of operation, or a recorded
	// value that no longer decodes into the type the code expects. The run fails;
	// fix the code, then Client.Retry it.
	ErrNonDeterministic = errors.New("workflow: run no longer matches its recorded steps")
)

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string { return e.err.Error() }
func (e nonRetryableError) Unwrap() error { return e.err }

// NonRetryable marks a step error as final: the step fails without further attempts.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

func isNonRetryable(err error) bool {
	var target nonRetryableError
	return errors.As(err, &target)
}

// StepError reports a step that failed all its attempts. It is rebuilt from the
// database when the run replays, so it carries the message of the last error, not
// the error itself: code that branches on it behaves the same on every replay.
type StepError struct {
	Step     string
	Attempts int
	Message  string
}

func (e *StepError) Error() string {
	return fmt.Sprintf("workflow: step %q failed after %d attempt(s): %s", e.Step, e.Attempts, e.Message)
}

// RunError reports a run that failed or was cancelled, from Run.Result and from
// Future.Wait on a child run. errors.Is(err, ErrCancelled) and
// errors.Is(err, ErrTimeout) tell those two cases apart from other failures.
type RunError struct {
	RunID    string
	Workflow string
	Status   RunStatus
	TimedOut bool
	Message  string
}

func (e *RunError) Error() string {
	return fmt.Sprintf("workflow: %s run %s %s: %s", e.Workflow, e.RunID, e.Status, e.Message)
}

func (e *RunError) Is(target error) bool {
	switch target {
	case ErrCancelled:
		return e.Status == RunCancelled
	case ErrTimeout:
		return e.TimedOut
	}
	return false
}
