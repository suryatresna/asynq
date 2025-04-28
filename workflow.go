package asynq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/heimdalr/dag"
)

type Workflow struct {
	mu sync.RWMutex
	// flow define the flow
	flows map[string]FlowInterface

	// options []WorkflowOptionInterface
	opts []WorkflowOptionInterface

	// routes define the routes
	routes map[string]Handler
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
}

func (a *Workflow) InitiateAllFlows() {
	a.mu.RLock()
	defer a.mu.RUnlock()
	flows := make(map[string]FlowInterface)
	for _, opt := range a.opts {
		optFlows := opt.GetFlows()
		flow := a.NewFlow(opt.GetNameGroup())

		mapVertex := make(map[string]string)

		for name, hdl := range a.routes {
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
		mapHandler: make(map[string]func(ctx context.Context, t *Task) error),
		nodes:      []string{},
		jobs:       []string{},
	}
}

type Flow struct {
	mu          sync.RWMutex
	name        string
	dag         *dag.DAG
	mapHandler  map[string]func(ctx context.Context, t *Task) error
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
	f.mapHandler[adVal] = fn
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
	// Get the first vertex in the flow.
	// firstVertex := ""

	// for id, name := range f.dag.GetVertices() {
	// 	if val, ok := name.(string); ok {
	// 		if id == f.firstVertex {
	// 			firstVertex = id
	// 		}
	// 	}
	// }

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
	// var parents []interface{}
	// for _, r := range parentResults {
	// 	p, _ := d.GetVertex(r.ID)
	// 	parents = append(parents, p)
	// }

	ctx := context.Background()
	fn, err := f.getJobFunction(id)
	if err != nil {
		return nil, errors.New("no function registered for job, detail " + err.Error())
	}
	// fmt.Printf("%v based on: %+v\n", v, parents)
	if val, ok := v.(string); ok {
		f.task.typename = val
	}
	if err := fn(ctx, f.task); err != nil {
		return nil, errors.New("error processing job, detail " + err.Error())
	}

	return v, nil
}

func (f *Flow) getJobFunction(name string) (func(ctx context.Context, t *Task) error, error) {
	fn, ok := f.mapHandler[name]
	if !ok {
		return nil, fmt.Errorf("no function registered for job: %s", name)
	}
	return fn, nil
}
