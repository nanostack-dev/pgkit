package workflow

import "strings"

// Signal is a typed message a run receives with Context.Receive and anyone sends
// with Client.Signal. Declare it once so both sides share the name and the type.
// Signals sent before the run waits for them are kept until it does.
type Signal[T any] struct {
	name string
}

// NewSignal declares a signal. It panics on an invalid name, as that is a
// programming error.
func NewSignal[T any](name string) Signal[T] {
	if !validName(name) {
		panic("workflow: NewSignal(" + name + "): " + ErrInvalidName.Error())
	}
	return Signal[T]{name: name}
}

// Name is the name given to NewSignal.
func (s Signal[T]) Name() string {
	return s.name
}

func validName(name string) bool {
	return strings.TrimSpace(name) != "" && !strings.Contains(name, "#")
}
