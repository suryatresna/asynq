// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/heimdalr/dag"
)

func TestRegisterFlow(t *testing.T) {
	opt := RegisterFlow("group-a")

	if got := opt.GetNameGroup(); got != "group-a" {
		t.Errorf("GetNameGroup() = %q, want %q", got, "group-a")
	}
	if got := opt.GetFlows(); len(got) != 0 {
		t.Errorf("GetFlows() len = %d, want 0", len(got))
	}
}

func TestWorkflowOption_SetFlows(t *testing.T) {
	opt := RegisterFlow("g")
	opt.SetFlows("edge-1", "from-1", "to-1")
	opt.SetFlows("edge-2", "from-2", "to-2")

	flows := opt.GetFlows()
	if len(flows) != 2 {
		t.Fatalf("got %d flows, want 2", len(flows))
	}
	if flows[0] != (preFlow{name: "edge-1", from: "from-1", to: "to-1"}) {
		t.Errorf("flows[0] = %+v", flows[0])
	}
	if flows[1] != (preFlow{name: "edge-2", from: "from-2", to: "to-2"}) {
		t.Errorf("flows[1] = %+v", flows[1])
	}
}

func TestNewWorkflow(t *testing.T) {
	opt1 := RegisterFlow("g1")
	opt2 := RegisterFlow("g2")

	wf := NewWorkflow(opt1, opt2)
	if wf == nil {
		t.Fatal("NewWorkflow returned nil")
	}
	if len(wf.opts) != 2 {
		t.Errorf("opts len = %d, want 2", len(wf.opts))
	}
	if wf.opts[0].GetNameGroup() != "g1" || wf.opts[1].GetNameGroup() != "g2" {
		t.Errorf("opts not preserved in order: %q, %q",
			wf.opts[0].GetNameGroup(), wf.opts[1].GetNameGroup())
	}
}

func TestWorkflow_NewFlow(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("my-flow")

	if flow == nil {
		t.Fatal("NewFlow returned nil")
	}
	if flow.GetName() != "my-flow" {
		t.Errorf("GetName() = %q, want %q", flow.GetName(), "my-flow")
	}

	concrete, ok := flow.(*Flow)
	if !ok {
		t.Fatalf("expected *Flow, got %T", flow)
	}
	if concrete.dag == nil {
		t.Error("dag is nil")
	}
	if concrete.mapHandler == nil {
		t.Error("mapHandler is nil")
	}
}

func TestFlow_Node(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("f").(*Flow)

	id, err := flow.Node("a")
	if err != nil {
		t.Fatalf("Node(\"a\") error: %v", err)
	}
	if id == "" {
		t.Error("expected non-empty vertex id")
	}
	if len(flow.nodes) != 1 || flow.nodes[0] != id {
		t.Errorf("nodes = %v, want [%q]", flow.nodes, id)
	}

	// Adding a vertex with the same value should error (the underlying DAG
	// deduplicates by hashed value).
	if _, err := flow.Node("a"); err == nil {
		t.Error("expected error for duplicate Node, got nil")
	}
}

func TestFlow_Edge(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("f").(*Flow)

	idA, err := flow.Node("a")
	if err != nil {
		t.Fatalf("Node(a): %v", err)
	}
	idB, err := flow.Node("b")
	if err != nil {
		t.Fatalf("Node(b): %v", err)
	}

	if err := flow.Edge("edge1", idA, idB); err != nil {
		t.Fatalf("Edge: %v", err)
	}
	if flow.firstVertex != idA {
		t.Errorf("firstVertex = %q, want %q", flow.firstVertex, idA)
	}

	// firstVertex should not change after subsequent edges.
	idC, err := flow.Node("c")
	if err != nil {
		t.Fatalf("Node(c): %v", err)
	}
	if err := flow.Edge("edge2", idB, idC); err != nil {
		t.Fatalf("Edge: %v", err)
	}
	if flow.firstVertex != idA {
		t.Errorf("firstVertex changed to %q, want %q", flow.firstVertex, idA)
	}
}

func TestFlow_Job(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("f").(*Flow)

	fn := func(ctx context.Context, t *Task) error { return nil }
	id, err := flow.Job("job-1", fn)
	if err != nil {
		t.Fatalf("Job: %v", err)
	}
	if id == "" {
		t.Error("expected non-empty job id")
	}
	if _, ok := flow.mapHandler[id]; !ok {
		t.Error("handler not registered under returned id")
	}
	if len(flow.jobs) != 1 || flow.jobs[0] != id {
		t.Errorf("jobs = %v, want [%q]", flow.jobs, id)
	}

	// Duplicate job name should error.
	if _, err := flow.Job("job-1", fn); err == nil {
		t.Error("expected error for duplicate Job, got nil")
	}
}

func TestFlow_GetName(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("name-x")
	if got := flow.GetName(); got != "name-x" {
		t.Errorf("GetName() = %q, want %q", got, "name-x")
	}
}

func TestFlow_DescribeAndListFlow(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("f").(*Flow)

	if _, err := flow.Job("a", func(ctx context.Context, t *Task) error { return nil }); err != nil {
		t.Fatalf("Job: %v", err)
	}

	if flow.DescribeFlow() == "" {
		t.Error("DescribeFlow returned empty string")
	}
	if flow.ListFlow() == "" {
		t.Error("ListFlow returned empty string")
	}
}

func TestFlow_ProcessSequence_RunsHandlersInOrder(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("ordered").(*Flow)

	var mu sync.Mutex
	var order []string
	mkHandler := func(name string) func(ctx context.Context, t *Task) error {
		return func(ctx context.Context, t *Task) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		}
	}

	idA, err := flow.Job("step-a", mkHandler("a"))
	if err != nil {
		t.Fatalf("Job(a): %v", err)
	}
	idB, err := flow.Job("step-b", mkHandler("b"))
	if err != nil {
		t.Fatalf("Job(b): %v", err)
	}
	idC, err := flow.Job("step-c", mkHandler("c"))
	if err != nil {
		t.Fatalf("Job(c): %v", err)
	}
	if err := flow.Edge("e1", idA, idB); err != nil {
		t.Fatalf("Edge(a,b): %v", err)
	}
	if err := flow.Edge("e2", idB, idC); err != nil {
		t.Fatalf("Edge(b,c): %v", err)
	}

	task := NewTask("initial", nil)
	if err := flow.ProcessSequence(context.Background(), task); err != nil {
		t.Fatalf("ProcessSequence: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 {
		t.Fatalf("handler invocations = %d, want 3 (got %v)", len(order), order)
	}
	// Linear chain a→b→c must execute in that order.
	if order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Errorf("execution order = %v, want [a b c]", order)
	}
	// The task's typename should reflect the last executed vertex.
	if task.Type() != "step-c" {
		t.Errorf("task.Type() = %q, want %q", task.Type(), "step-c")
	}
}

func TestFlow_ProcessSequence_EmptyFlow(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("empty").(*Flow)

	err := flow.ProcessSequence(context.Background(), NewTask("x", nil))
	if err == nil || !strings.Contains(err.Error(), "flow is empty") {
		t.Errorf("got err %v, want error containing 'flow is empty'", err)
	}
}

func TestFlow_ProcessSequence_NilDAG(t *testing.T) {
	flow := &Flow{name: "no-dag"}
	err := flow.ProcessSequence(context.Background(), NewTask("x", nil))
	if err == nil || !strings.Contains(err.Error(), "dag is not initialized") {
		t.Errorf("got err %v, want error containing 'dag is not initialized'", err)
	}
}

func TestFlow_ProcessSequence_HandlerError(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("err").(*Flow)

	wantErr := errors.New("boom")
	idA, err := flow.Job("a", func(ctx context.Context, t *Task) error { return nil })
	if err != nil {
		t.Fatalf("Job(a): %v", err)
	}
	idB, err := flow.Job("b", func(ctx context.Context, t *Task) error { return wantErr })
	if err != nil {
		t.Fatalf("Job(b): %v", err)
	}
	if err := flow.Edge("e", idA, idB); err != nil {
		t.Fatalf("Edge: %v", err)
	}

	err = flow.ProcessSequence(context.Background(), NewTask("x", nil))
	if err == nil {
		t.Fatal("expected error from ProcessSequence, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %q does not contain underlying handler error %q", err.Error(), wantErr.Error())
	}
}

func TestFlow_GetJobFunction(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("f").(*Flow)

	called := false
	fn := func(ctx context.Context, t *Task) error {
		called = true
		return nil
	}
	id, err := flow.Job("j", fn)
	if err != nil {
		t.Fatalf("Job: %v", err)
	}

	got, err := flow.getJobFunction(id)
	if err != nil {
		t.Fatalf("getJobFunction: %v", err)
	}
	if err := got(context.Background(), NewTask("t", nil)); err != nil {
		t.Fatalf("invoking returned fn: %v", err)
	}
	if !called {
		t.Error("returned function was not the registered handler")
	}

	if _, err := flow.getJobFunction("missing-id"); err == nil {
		t.Error("expected error for missing id, got nil")
	}
}

func TestFlow_FlowCallback_MissingHandler(t *testing.T) {
	wf := NewWorkflow()
	flow := wf.NewFlow("f").(*Flow)

	// Insert a vertex directly into the DAG without registering a handler,
	// so flowCallback's getJobFunction lookup fails.
	id, err := flow.dag.AddVertex("orphan")
	if err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	flow.task = NewTask("init", nil)

	_, err = flow.flowCallback(flow.dag, id, nil)
	if err == nil || !strings.Contains(err.Error(), "no function registered") {
		t.Errorf("got err %v, want error containing 'no function registered'", err)
	}
}

func TestWorkflow_RegisterRoutes(t *testing.T) {
	mux := NewServeMux()
	h1 := HandlerFunc(func(ctx context.Context, t *Task) error { return nil })
	h2 := HandlerFunc(func(ctx context.Context, t *Task) error { return nil })
	mux.Handle("step1", h1)
	mux.Handle("step2", h2)

	wf := NewWorkflow()
	wf.RegisterRoutes(mux)

	if len(wf.routes) != 2 {
		t.Fatalf("routes len = %d, want 2", len(wf.routes))
	}
	if _, ok := wf.routes["step1"]; !ok {
		t.Error("route 'step1' missing")
	}
	if _, ok := wf.routes["step2"]; !ok {
		t.Error("route 'step2' missing")
	}
}

func TestWorkflow_InitiateAllFlows(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("step1", func(ctx context.Context, t *Task) error { return nil })
	mux.HandleFunc("step2", func(ctx context.Context, t *Task) error { return nil })

	opt := RegisterFlow("group-1")
	opt.SetFlows("e1", "step1", "step2")

	wf := NewWorkflow(opt)
	wf.RegisterRoutes(mux)
	wf.InitiateAllFlows()

	flows := wf.GetAllFlows()
	if len(flows) != 1 {
		t.Fatalf("flows len = %d, want 1", len(flows))
	}
	flow, ok := flows["group-1"]
	if !ok {
		t.Fatal("flow 'group-1' missing")
	}
	concrete := flow.(*Flow)
	if len(concrete.jobs) != 2 {
		t.Errorf("jobs len = %d, want 2", len(concrete.jobs))
	}
	if concrete.firstVertex == "" {
		t.Error("firstVertex not set after edges added")
	}
	// Verify the edge wired the two route vertices together.
	verts := concrete.dag.GetVertices()
	if len(verts) != 2 {
		t.Errorf("vertices = %d, want 2", len(verts))
	}
	var step1ID string
	for id, v := range verts {
		if s, ok := v.(string); ok && s == "step1" {
			step1ID = id
		}
	}
	if step1ID == "" {
		t.Fatal("could not find vertex for 'step1'")
	}
	children, err := concrete.dag.GetChildren(step1ID)
	if err != nil {
		t.Fatalf("GetChildren: %v", err)
	}
	if len(children) != 1 {
		t.Errorf("step1 children = %d, want 1", len(children))
	}
}

func TestWorkflow_InitiateAllFlows_MultipleGroups(t *testing.T) {
	mux := NewServeMux()
	mux.HandleFunc("a", func(ctx context.Context, t *Task) error { return nil })
	mux.HandleFunc("b", func(ctx context.Context, t *Task) error { return nil })

	opt1 := RegisterFlow("g1")
	opt1.SetFlows("e", "a", "b")
	opt2 := RegisterFlow("g2")
	opt2.SetFlows("e", "b", "a")

	wf := NewWorkflow(opt1, opt2)
	wf.RegisterRoutes(mux)
	wf.InitiateAllFlows()

	flows := wf.GetAllFlows()
	if len(flows) != 2 {
		t.Fatalf("flows len = %d, want 2", len(flows))
	}
	if _, ok := flows["g1"]; !ok {
		t.Error("missing flow 'g1'")
	}
	if _, ok := flows["g2"]; !ok {
		t.Error("missing flow 'g2'")
	}
}

func TestWorkflow_GetAllFlows_BeforeInit(t *testing.T) {
	wf := NewWorkflow()
	if got := wf.GetAllFlows(); got != nil {
		t.Errorf("GetAllFlows() = %v, want nil before InitiateAllFlows", got)
	}
}

// Compile-time assertion: *Flow satisfies FlowInterface. This guards against
// accidentally breaking the interface contract while editing the Flow type.
var _ FlowInterface = (*Flow)(nil)

// Compile-time assertion: *WorkflowOption satisfies WorkflowOptionInterface.
var _ WorkflowOptionInterface = (*WorkflowOption)(nil)

// dag.FlowResult is referenced indirectly through flowCallback; importing dag
// in tests keeps the import explicit so the file stays self-documenting.
var _ = dag.FlowResult{}
