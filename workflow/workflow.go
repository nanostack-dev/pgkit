// Package workflow runs durable Go functions on PostgreSQL.
//
// A workflow is an ordinary function. Each step it runs through its Context is
// checkpointed by name, so after a crash, a deploy, a sleep or a wait for a signal,
// the function runs again from the top and completed steps return their recorded
// results instead of running twice. See README.md for the full guide.
package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Workflow is a durable function from In to Out, created with Define. Start runs
// with Client.Start and execute them on a worker that lists the workflow.
type Workflow[In, Out any] struct {
	definition *definition
}

// Definition is a workflow of any input and output type, as WorkerBuilder.Workflows
// accepts.
type Definition interface {
	Name() string
	definitionOf() *definition
}

type definition struct {
	name    string
	version int
	retry   Retry
	execute func(wf *Context, input json.RawMessage) (json.RawMessage, error)
}

// Define names a workflow function. The name identifies its runs in the database:
// renaming a workflow strands its unfinished runs. Names must not contain '@', which
// separates a name from its Version in queue names. Define panics on an invalid name
// or a nil function, as both are programming errors.
func Define[In, Out any](name string, run func(wf *Context, input In) (Out, error), options ...DefineOption) *Workflow[In, Out] {
	if strings.TrimSpace(name) == "" || strings.Contains(name, "@") {
		panic(fmt.Sprintf("workflow: Define(%q) needs a non-empty name without '@'", name))
	}
	if run == nil {
		panic(fmt.Sprintf("workflow: Define(%q) needs a function", name))
	}
	def := &definition{name: name}
	for _, option := range options {
		option.applyToDefinition(def)
	}
	def.execute = func(wf *Context, rawInput json.RawMessage) (json.RawMessage, error) {
		var input In
		if err := json.Unmarshal(rawInput, &input); err != nil {
			return nil, fmt.Errorf("%w: run input does not decode into %T: %v", ErrNonDeterministic, input, err)
		}
		output, err := run(wf, input)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(output)
		if err == nil {
			err = checkStorable(encoded)
		}
		if err != nil {
			return nil, fmt.Errorf("workflow: encode %T output: %w", output, err)
		}
		return encoded, nil
	}
	return &Workflow[In, Out]{definition: def}
}

// Name is the name given to Define.
func (w *Workflow[In, Out]) Name() string {
	return w.definition.name
}

func (w *Workflow[In, Out]) definitionOf() *definition {
	return w.definition
}

func (d *definition) queueName() string {
	if d.version == 0 {
		return "pgworkflow:" + d.name
	}
	return fmt.Sprintf("pgworkflow:%s@%d", d.name, d.version)
}
