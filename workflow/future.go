package workflow

import "sync"

// Future is the result of an Async step or of a child run started with Start.
type Future[T any] struct {
	once  sync.Once
	wait  func() (T, error)
	value T
	err   error
}

func newFuture[T any](wait func() (T, error)) *Future[T] {
	return &Future[T]{wait: wait}
}

func failedFuture[T any](err error) *Future[T] {
	return newFuture(func() (T, error) {
		var zero T
		return zero, err
	})
}

// Wait returns the result. For a child run that has not finished, it returns
// ErrSuspended: return it from the workflow function, which resumes when the child
// finishes.
func (f *Future[T]) Wait() (T, error) {
	f.once.Do(func() { f.value, f.err = f.wait() })
	return f.value, f.err
}
