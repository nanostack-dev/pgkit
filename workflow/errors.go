package workflow

import "errors"

var (
	ErrNilDB                     = errors.New("workflow: db is nil")
	ErrNilQueue                  = errors.New("workflow: queue is nil")
	ErrEmptyWorkflowName         = errors.New("workflow: workflow name is required")
	ErrEmptyStepName             = errors.New("workflow: step name is required")
	ErrDuplicateStepName         = errors.New("workflow: duplicate step name")
	ErrUnknownDependency         = errors.New("workflow: step dependency not found")
	ErrDefinitionCycle           = errors.New("workflow: definition contains cycle")
	ErrNoRegisteredDefinitions   = errors.New("workflow: no registered workflow definitions")
	ErrDefinitionNotFound        = errors.New("workflow: definition not found")
	ErrDefinitionNotRegistered   = errors.New("workflow: definition runtime not registered")
	ErrActiveDefinitionNotFound  = errors.New("workflow: active definition not found")
	ErrInvalidWorkerConfig       = errors.New("workflow: invalid worker config")
	ErrRunNotFound               = errors.New("workflow: run not found")
	ErrStepNotFound              = errors.New("workflow: step not found")
	ErrRunNotRetryable           = errors.New("workflow: run is not retryable")
	ErrStepNotRetryable          = errors.New("workflow: step is not retryable")
	ErrStepNotRunnable           = errors.New("workflow: step is not runnable")
	ErrStepOutputNotFound        = errors.New("workflow: step output not found")
	ErrRunDefinitionMismatch     = errors.New("workflow: run definition graph mismatch")
	ErrStepAlreadyScheduled      = errors.New("workflow: step already scheduled")
	ErrInvalidJobPayload         = errors.New("workflow: invalid workflow job payload")
	ErrUnsupportedDefinitionType = errors.New("workflow: unsupported definition type")
)

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string {
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

// NonRetryable marks an error as terminal for a workflow step.
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
