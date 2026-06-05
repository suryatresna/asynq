package asynq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/heimdalr/dag"
)

// FlowParams holds key-value pairs produced by a step and consumed by downstream steps.
type FlowParams map[string]interface{}

// StepFunc is a job function that receives merged output params from all parent steps
// and returns its own output params for downstream steps.
type StepFunc func(ctx context.Context, t *Task, in FlowParams) (FlowParams, error)

// handlerEntry holds either a legacy job function or a step function.
type handlerEntry struct {
	fn     func(ctx context.Context, t *Task) error
	stepFn StepFunc
}

type Workflow struct {
	mu sync.RWMutex
	// flow define the flow
	flows map[string]FlowInterface

	// options []WorkflowOptionInterface
	opts []WorkflowOptionInterface

	// routes define the routes
	routes     map[string]Handler
	stepRoutes map[string]StepFunc
}

type WorkflowOption struct {
	group string
	flows []preFlow
}

type WorkflowOptionInterface interface {
	SetFlows(name string, from, to string)
	GetFlows() []preFlow
	GetNameGroup() string
}

type preFlow struct {
	name     string
	from, to string
}

func RegisterFlow(group string) WorkflowOptionInterface {
	return &WorkflowOption{
		group: group,
		flows: []preFlow{},
	}
}

func (o *WorkflowOption) SetFlows(name string, from, to string) {
	o.flows = append(o.flows, preFlow{
		name: name,
		from: from,
		to:   to,
	})
}

func (o *WorkflowOption) GetNameGroup() string {
	return o.group
}

func (o *WorkflowOption) GetFlows() []preFlow {
	return o.flows
}

type FlowInterface interface {
	Node(name string) (string, error)
	Edge(name string, from, to string) error
	ListFlow() string

	Job(name string, fn func(ctx context.Context, t *Task) error) (string, error)
	// Step registers a job that receives merged output params from all parent steps
	// and returns its own output params for downstream steps to consume or override.
	Step(name string, fn StepFunc) (string, error)
	GetName() string
	ProcessSequence(ctx context.Context, t *Task) error
	DescribeFlow() string
}

type WorkflowInterface interface {
	NewFlow(flowname string) *FlowInterface
}

func NewWorkflow(opts ...WorkflowOptionInterface) *Workflow {
	return &Workflow{
		opts: opts,
	}
}

func (a *Workflow) RegisterRoutes(mux *ServeMux) {
	a.routes = mux.GetAllRoutes()
	a.stepRoutes = mux.GetAllStepRoutes()
}

func (a *Workflow) InitiateAllFlows() {
	a.mu.RLock()
	defer a.mu.RUnlock()
	flows := make(map[string]FlowInterface)
	for _, opt := range a.opts {
		optFlows := opt.GetFlows()
		flow := a.NewFlow(opt.GetNameGroup())

		mapVertex := make(map[string]string)

		for name, fn := range a.stepRoutes {
			vertexID, err := flow.Step(name, fn)
			if err != nil {
				fmt.Printf("[ERR] error adding step: %s\n", err)
			}
			mapVertex[name] = vertexID
		}

		for name, hdl := range a.routes {
			if _, isStep := a.stepRoutes[name]; isStep {
				continue // already registered via Step
			}
			vertexID, err := flow.Job(name, hdl.ProcessTask)
			if err != nil {
				fmt.Printf("[ERR] error adding job: %s\n", err)
			}
			mapVertex[name] = vertexID
		}

		for _, flowItems := range optFlows {
			if err := flow.Edge(flowItems.name, mapVertex[flowItems.from], mapVertex[flowItems.to]); err != nil {
				fmt.Printf("[ERR] error adding edge: %s\n", err)
			}
		}
		fmt.Printf("[INFO] flow %s: %s\n", opt.GetNameGroup(), flow.DescribeFlow())
		flows[opt.GetNameGroup()] = flow
	}
	a.flows = flows
}

func (a *Workflow) GetAllFlows() map[string]FlowInterface {
	return a.flows
}

func (a *Workflow) NewFlow(flowname string) FlowInterface {
	return &Flow{
		name:       flowname,
		dag:        dag.NewDAG(),
		mapHandler: make(map[string]handlerEntry),
		nodes:      []string{},
		jobs:       []string{},
	}
}

type Flow struct {
	mu          sync.RWMutex
	name        string
	dag         *dag.DAG
	mapHandler  map[string]handlerEntry
	firstVertex string
	nodes       []string
	jobs        []string
	task        *Task
}

func (f *Flow) Node(name string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	res, err := f.dag.AddVertex(name)
	if err != nil {
		return "", err
	}
	f.nodes = append(f.nodes, res)
	return res, nil
}

func (f *Flow) Edge(name string, from, to string) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.firstVertex == "" {
		f.firstVertex = from
	}

	return f.dag.AddEdge(from, to)
}

func (f *Flow) ListFlow() string {
	return f.dag.String()
}

func (f *Flow) Job(name string, fn func(ctx context.Context, t *Task) error) (string, error) {
	adVal, err := f.dag.AddVertex(name)
	if err != nil {
		return "", err
	}
	f.mapHandler[adVal] = handlerEntry{fn: fn}
	f.jobs = append(f.jobs, adVal)
	return adVal, nil
}

// Step registers a job that can read params from parent steps and pass params to child steps.
// Use Job instead if the step does not need to exchange params with adjacent steps.
func (f *Flow) Step(name string, fn StepFunc) (string, error) {
	adVal, err := f.dag.AddVertex(name)
	if err != nil {
		return "", err
	}
	f.mapHandler[adVal] = handlerEntry{stepFn: fn}
	f.jobs = append(f.jobs, adVal)
	return adVal, nil
}

func (f *Flow) GetName() string {
	return f.name
}

func (f *Flow) ProcessSequence(ctx context.Context, t *Task) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.dag == nil {
		return fmt.Errorf("dag is not initialized")
	}

	// Check if the flow is empty.
	if len(f.dag.GetVertices()) == 0 {
		return fmt.Errorf("flow is empty")
	}

	// set task
	f.task = t

	// Run the sequence of tasks in the flow.
	if err := f.runSequence(); err != nil {
		return fmt.Errorf("error running sequence: %w", err)
	}

	return nil
}

func (f *Flow) DescribeFlow() string {
	return f.dag.String()
}

func (f *Flow) runSequence() error {
	res, err := f.dag.DescendantsFlow(f.firstVertex, nil, f.flowCallback)
	if err != nil {
		return errors.New("error processing flow, detail " + err.Error())
	}

	for _, v := range res {
		if v.Error != nil {
			return fmt.Errorf("error processing vertex %s, detail %v", v.ID, v.Error)
		}
	}

	return nil
}

func (f *Flow) flowCallback(d *dag.DAG, id string, parentResults []dag.FlowResult) (interface{}, error) {
	v, _ := d.GetVertex(id)

	// Merge output params from all parent steps; later parents overwrite earlier ones on key collision.
	merged := FlowParams{}
	for _, r := range parentResults {
		if params, ok := r.Result.(FlowParams); ok {
			for k, val := range params {
				merged[k] = val
			}
		}
	}

	ctx := context.Background()
	entry, err := f.getHandlerEntry(id)
	if err != nil {
		return nil, errors.New("no function registered for job, detail " + err.Error())
	}

	// Shallow-copy the task so concurrent steps (e.g. fan-out siblings) each
	// get an independent typename field without racing on f.task.
	stepTask := *f.task
	if val, ok := v.(string); ok {
		stepTask.typename = val
	}

	if entry.stepFn != nil {
		out, err := entry.stepFn(ctx, &stepTask, merged)
		if err != nil {
			return nil, errors.New("error processing job, detail " + err.Error())
		}
		return out, nil
	}

	if err := entry.fn(ctx, &stepTask); err != nil {
		return nil, errors.New("error processing job, detail " + err.Error())
	}
	return nil, nil
}

func (f *Flow) getHandlerEntry(id string) (handlerEntry, error) {
	entry, ok := f.mapHandler[id]
	if !ok {
		return handlerEntry{}, fmt.Errorf("no function registered for job: %s", id)
	}
	return entry, nil
}
