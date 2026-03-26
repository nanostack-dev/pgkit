package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

func Define(name string, build func(*Builder)) (*Definition, error) {
	if name == "" {
		return nil, ErrEmptyWorkflowName
	}
	b := &Builder{definition: &Definition{Name: name}}
	if build != nil {
		build(b)
	}
	if err := b.definition.validate(); err != nil {
		return nil, err
	}
	return b.definition, nil
}

type Builder struct {
	definition *Definition
}

func (b *Builder) Metadata(metadata map[string]any) {
	b.definition.Metadata = cloneMap(metadata)
}

func (b *Builder) Title(title string) {
	b.definition.Title = title
}

func (b *Builder) Description(description string) {
	b.definition.Description = description
}

func (b *Builder) Step(name string, handler StepHandler, opts StepOptions) {
	b.definition.steps = append(b.definition.steps, makeStepSpec(name, handler, nil, opts))
}

func (b *Builder) TxStep(name string, handler TxStepHandler, opts StepOptions) {
	b.definition.steps = append(b.definition.steps, makeStepSpec(name, nil, handler, opts))
}

func (b *Builder) ForEach(name string, resolver ForEachResolver, handler StepHandler, opts StepOptions) {
	b.definition.steps = append(b.definition.steps, makeForEachSpec(name, resolver, handler, nil, opts))
}

func (b *Builder) TxForEach(name string, resolver ForEachResolver, handler TxStepHandler, opts StepOptions) {
	b.definition.steps = append(b.definition.steps, makeForEachSpec(name, resolver, nil, handler, opts))
}

func makeStepSpec(name string, handler StepHandler, txHandler TxStepHandler, opts StepOptions) StepSpec {
	retry := opts.RetryPolicy.withDefaults()
	queueName := opts.QueueName
	if queueName == "" {
		queueName = name
	}
	label := opts.Label
	if label == "" {
		label = name
	}
	return StepSpec{
		Name:        name,
		Kind:        StepKindStep,
		Label:       label,
		QueueName:   queueName,
		DependsOn:   append([]string(nil), opts.DependsOn...),
		RetryPolicy: retry,
		Handler:     handler,
		TxHandler:   txHandler,
		Metadata:    cloneMap(opts.Metadata),
	}
}

func makeForEachSpec(name string, resolver ForEachResolver, handler StepHandler, txHandler TxStepHandler, opts StepOptions) StepSpec {
	spec := makeStepSpec(name, handler, txHandler, opts)
	spec.Kind = StepKindForEach
	spec.Resolver = resolver
	return spec
}

func (d *Definition) validate() error {
	if d == nil {
		return ErrDefinitionNotFound
	}
	if d.Name == "" {
		return ErrEmptyWorkflowName
	}
	seen := make(map[string]struct{}, len(d.steps))
	for _, step := range d.steps {
		if step.Name == "" {
			return ErrEmptyStepName
		}
		if _, ok := seen[step.Name]; ok {
			return fmt.Errorf("%w: %s", ErrDuplicateStepName, step.Name)
		}
		seen[step.Name] = struct{}{}
		if step.Handler == nil && step.TxHandler == nil {
			return fmt.Errorf("workflow: step %s has no handler", step.Name)
		}
		if step.Kind == StepKindForEach && step.Resolver == nil {
			return fmt.Errorf("workflow: foreach step %s has no resolver", step.Name)
		}
		for _, dep := range step.DependsOn {
			if dep == "" {
				return fmt.Errorf("workflow: step %s contains empty dependency", step.Name)
			}
		}
	}
	for _, step := range d.steps {
		for _, dep := range step.DependsOn {
			if _, ok := seen[dep]; !ok {
				return fmt.Errorf("%w: step=%s dependency=%s", ErrUnknownDependency, step.Name, dep)
			}
		}
	}
	if hasCycle(d.steps) {
		return ErrDefinitionCycle
	}
	return nil
}

func (d *Definition) Step(name string) (StepSpec, bool) {
	for _, step := range d.steps {
		if step.Name == name {
			return step, true
		}
	}
	return StepSpec{}, false
}

func (d *Definition) Steps() []StepSpec {
	out := make([]StepSpec, len(d.steps))
	copy(out, d.steps)
	return out
}

func (d *Definition) Graph(version int) Graph {
	nodes := make([]GraphNode, 0, len(d.steps))
	edges := make([]GraphEdge, 0)
	for _, step := range d.steps {
		nodes = append(nodes, GraphNode{
			ID:          step.Name,
			Kind:        string(step.Kind),
			Label:       step.Label,
			Queue:       step.QueueName,
			MaxAttempts: step.RetryPolicy.MaxAttempts,
			DependsOn:   append([]string(nil), step.DependsOn...),
			Metadata:    cloneMap(step.Metadata),
		})
		for _, dep := range step.DependsOn {
			edges = append(edges, GraphEdge{From: dep, To: step.Name})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})
	return Graph{
		Name:        d.Name,
		Version:     version,
		Title:       d.Title,
		Description: d.Description,
		Nodes:       nodes,
		Edges:       edges,
		Metadata:    cloneMap(d.Metadata),
	}
}

func (d *Definition) ContentHash() (string, []byte, error) {
	graph := d.Graph(0)
	payload, err := marshalCanonical(graph)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), payload, nil
}

func hasCycle(steps []StepSpec) bool {
	deps := make(map[string][]string, len(steps))
	for _, step := range steps {
		deps[step.Name] = append([]string(nil), step.DependsOn...)
	}
	state := make(map[string]int, len(steps))
	var visit func(string) bool
	visit = func(node string) bool {
		if state[node] == 1 {
			return true
		}
		if state[node] == 2 {
			return false
		}
		state[node] = 1
		for _, dep := range deps[node] {
			if visit(dep) {
				return true
			}
		}
		state[node] = 2
		return false
	}
	for _, step := range steps {
		if visit(step.Name) {
			return true
		}
	}
	return false
}

func marshalCanonical(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	return json.Marshal(decoded)
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
