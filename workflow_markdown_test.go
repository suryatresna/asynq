package asynq

import (
	"context"
	"maps"
	"strings"
	"sync"
	"testing"
)

// ---- parseMarkdown unit tests ----

func TestParseMarkdown_BasicChain(t *testing.T) {
	src := `
graph LR
    funA --> funB
    funB --> funC
`
	g, err := parseMarkdown(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(g.edges) != 2 {
		t.Fatalf("edges = %d, want 2", len(g.edges))
	}
	if g.edges[0].from != "funA" || g.edges[0].to != "funB" {
		t.Errorf("edge[0] = {%s %s}, want {funA funB}", g.edges[0].from, g.edges[0].to)
	}
	if g.edges[1].from != "funB" || g.edges[1].to != "funC" {
		t.Errorf("edge[1] = {%s %s}, want {funB funC}", g.edges[1].from, g.edges[1].to)
	}
	if g.edges[0].label != "funA->funB" {
		t.Errorf("auto-label = %q, want %q", g.edges[0].label, "funA->funB")
	}
}

func TestParseMarkdown_LabeledEdges(t *testing.T) {
	src := `
graph LR
    funD -->|Pass| funE
    funD -->|Fail| funF
`
	g, err := parseMarkdown(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(g.edges) != 2 {
		t.Fatalf("edges = %d, want 2", len(g.edges))
	}
	if g.edges[0].from != "funD" || g.edges[0].to != "funE" || g.edges[0].label != "Pass" {
		t.Errorf("edge[0] = %+v", g.edges[0])
	}
	if g.edges[1].from != "funD" || g.edges[1].to != "funF" || g.edges[1].label != "Fail" {
		t.Errorf("edge[1] = %+v", g.edges[1])
	}
}

func TestParseMarkdown_NodeShapes(t *testing.T) {
	src := `
graph LR
    funA[Start Data] --> funB(Clean Data)
    funB --> funC{Validate}
    funC -->|Pass| funD[Done]
`
	g, err := parseMarkdown(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(g.edges) != 3 {
		t.Fatalf("edges = %d, want 3", len(g.edges))
	}
	want := [][2]string{{"funA", "funB"}, {"funB", "funC"}, {"funC", "funD"}}
	for i, w := range want {
		if g.edges[i].from != w[0] || g.edges[i].to != w[1] {
			t.Errorf("edge[%d] = {%s %s}, want {%s %s}", i,
				g.edges[i].from, g.edges[i].to, w[0], w[1])
		}
	}
}

func TestParseMarkdown_FanOutFanIn(t *testing.T) {
	src := `
graph LR
    funA --> funB
    funA --> funC
    funB --> funD
    funC --> funD
`
	g, err := parseMarkdown(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(g.edges) != 4 {
		t.Fatalf("edges = %d, want 4", len(g.edges))
	}
	if len(g.nodes) != 4 {
		t.Errorf("nodes = %d, want 4", len(g.nodes))
	}
}

func TestParseMarkdown_StandaloneNodeDeclaration(t *testing.T) {
	src := `
graph LR
    orphan[Standalone Node]
    funA --> funB
`
	g, err := parseMarkdown(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := g.nodes["orphan"]; !ok {
		t.Error("standalone node 'orphan' not registered")
	}
	if len(g.edges) != 1 {
		t.Errorf("edges = %d, want 1", len(g.edges))
	}
}

func TestParseMarkdown_Comments(t *testing.T) {
	src := `
%% This is a comment
graph LR
%% Another comment
    funA --> funB
`
	g, err := parseMarkdown(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(g.edges) != 1 {
		t.Errorf("edges = %d, want 1", len(g.edges))
	}
}

func TestParseMarkdown_WindowsLineEndings(t *testing.T) {
	src := "graph LR\r\n    funA --> funB\r\n"
	g, err := parseMarkdown(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(g.edges) != 1 {
		t.Errorf("edges = %d, want 1", len(g.edges))
	}
}

func TestParseMarkdown_MissingHeader_EdgeFirst(t *testing.T) {
	_, err := parseMarkdown("funA --> funB")
	if err == nil || !strings.Contains(err.Error(), "expected 'graph'") {
		t.Errorf("want header-missing error, got %v", err)
	}
}

func TestParseMarkdown_MissingHeader_EmptyInput(t *testing.T) {
	_, err := parseMarkdown("")
	if err == nil || !strings.Contains(err.Error(), "missing 'graph'") {
		t.Errorf("want missing-header error, got %v", err)
	}
}

func TestParseMarkdown_DuplicateHeader(t *testing.T) {
	src := `
graph LR
graph TD
    funA --> funB
`
	_, err := parseMarkdown(src)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("want duplicate-header error, got %v", err)
	}
}

func TestParseMarkdown_BadSyntax(t *testing.T) {
	src := `
graph LR
    this line has no valid edge ~~~
`
	_, err := parseMarkdown(src)
	if err == nil || !strings.Contains(err.Error(), "unrecognized syntax") {
		t.Errorf("want syntax error, got %v", err)
	}
}

func TestParseMarkdown_ErrorIncludesLineNumber(t *testing.T) {
	src := "graph LR\n    funA --> funB\n    @@bad@@\n"
	_, err := parseMarkdown(src)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "line 3") {
		t.Errorf("error should mention line 3, got: %v", err)
	}
}

// ---- RegisterFlowMarkdown integration tests ----

func TestRegisterFlowMarkdown_Basic(t *testing.T) {
	src := `
graph LR
    funA --> funB
    funB --> funC
`
	opt, err := RegisterFlowMarkdown("FlowA", src)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}
	if opt.GetNameGroup() != "FlowA" {
		t.Errorf("group = %q, want %q", opt.GetNameGroup(), "FlowA")
	}
	flows := opt.GetFlows()
	if len(flows) != 2 {
		t.Fatalf("flows = %d, want 2", len(flows))
	}
	if flows[0].from != "funA" || flows[0].to != "funB" {
		t.Errorf("flow[0] = %+v", flows[0])
	}
	if flows[1].from != "funB" || flows[1].to != "funC" {
		t.Errorf("flow[1] = %+v", flows[1])
	}
}

func TestRegisterFlowMarkdown_LabeledEdgesBecomePrefFlowNames(t *testing.T) {
	src := `
graph LR
    validate -->|ok| store
    validate -->|err| notify
`
	opt, err := RegisterFlowMarkdown("Pipeline", src)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}
	flows := opt.GetFlows()
	if len(flows) != 2 {
		t.Fatalf("flows = %d, want 2", len(flows))
	}
	names := map[string]bool{}
	for _, f := range flows {
		names[f.name] = true
	}
	if !names["ok"] || !names["err"] {
		t.Errorf("edge labels not preserved as flow names: %v", names)
	}
}

func TestRegisterFlowMarkdown_NoEdgesError(t *testing.T) {
	src := `
graph LR
    funA[Standalone]
`
	_, err := RegisterFlowMarkdown("FlowA", src)
	if err == nil || !strings.Contains(err.Error(), "no edges") {
		t.Errorf("want 'no edges' error, got %v", err)
	}
}

func TestRegisterFlowMarkdown_ParseError_Propagated(t *testing.T) {
	_, err := RegisterFlowMarkdown("FlowA", "funA --> funB")
	if err == nil {
		t.Error("want error for missing header, got nil")
	}
}

func TestRegisterFlowMarkdown_FullDataPipelineExample(t *testing.T) {
	src := `
graph LR
    ingest[Start Data Ingestion] --> clean(Clean Datasets)
    ingest --> fetch(Fetch API Logs)
    clean --> validate{Run Validations}
    fetch --> validate
    validate -->|Pass| load[Load to Data Warehouse]
    validate -->|Fail| alert[Trigger Alert]
`
	opt, err := RegisterFlowMarkdown("DataPipeline", src)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}
	if opt.GetNameGroup() != "DataPipeline" {
		t.Errorf("group = %q, want %q", opt.GetNameGroup(), "DataPipeline")
	}
	flows := opt.GetFlows()
	if len(flows) != 6 {
		t.Fatalf("flows = %d, want 6", len(flows))
	}

	// Verify node IDs are extracted correctly (labels stripped).
	nodeIDs := map[string]bool{}
	for _, f := range flows {
		nodeIDs[f.from] = true
		nodeIDs[f.to] = true
	}
	for _, id := range []string{"ingest", "clean", "fetch", "validate", "load", "alert"} {
		if !nodeIDs[id] {
			t.Errorf("node ID %q missing from wired edges", id)
		}
	}
}

// Compile-time assertion: RegisterFlowMarkdown returns WorkflowOptionInterface.
var _ WorkflowOptionInterface = func() WorkflowOptionInterface {
	opt, _ := RegisterFlowMarkdown("T", "graph LR\nA --> B")
	return opt
}()

// ---- End-to-end pipeline test with real handlers ----

// TestWorkflow_DataPipeline_EndToEnd wires a full 6-step DAG through
// RegisterFlowMarkdown + HandleStep + UseWorkflow and verifies that every
// handler runs in topological order, that params produced by a step reach
// its children, and that fan-in merging works (validate receives output from
// both clean and fetch).
//
// Note: both load and alert always execute because edge labels (|Pass|/|Fail|)
// are cosmetic — they name edges in the DAG but do not create conditional
// runtime branching.
func TestWorkflow_DataPipeline_EndToEnd(t *testing.T) {
	var mu sync.Mutex
	var executionLog []string
	receivedParams := map[string]FlowParams{}

	// record captures the name and a snapshot of in-params under the mutex.
	// It is safe to call from concurrently-executing steps.
	record := func(name string, in FlowParams) {
		snap := FlowParams{}
		maps.Copy(snap, in)
		mu.Lock()
		defer mu.Unlock()
		executionLog = append(executionLog, name)
		receivedParams[name] = snap
	}

	handleIngest := func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
		record("ingest", in)
		return FlowParams{"record_id": "rec-42", "source": "s3"}, nil
	}

	handleClean := func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
		record("clean", in)
		return FlowParams{"record_id": in["record_id"], "cleaned_count": 100}, nil
	}

	handleFetch := func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
		record("fetch", in)
		return FlowParams{"record_id": in["record_id"], "log_count": 25}, nil
	}

	// validate receives merged output from both clean and fetch (fan-in).
	handleValidate := func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
		record("validate", in)
		return FlowParams{"status": "pass", "record_id": in["record_id"]}, nil
	}

	handleLoad := func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
		record("load", in)
		return nil, nil
	}

	handleAlert := func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
		record("alert", in)
		return nil, nil
	}

	markdown := `
graph LR
    ingest[Start Data Ingestion] --> clean(Clean Datasets)
    ingest --> fetch(Fetch API Logs)
    clean --> validate{Run Validations}
    fetch --> validate
    validate -->|Pass| load[Load to Data Warehouse]
    validate -->|Fail| alert[Trigger Alert]
`

	flow, err := RegisterFlowMarkdown("DataPipeline", markdown)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}

	mux := NewServeMux()
	mux.HandleStep("ingest", handleIngest)
	mux.HandleStep("clean", handleClean)
	mux.HandleStep("fetch", handleFetch)
	mux.HandleStep("validate", handleValidate)
	mux.HandleStep("load", handleLoad)
	mux.HandleStep("alert", handleAlert)
	if err := mux.UseWorkflow(NewWorkflow(flow)); err != nil {
		t.Fatalf("UseWorkflow: %v", err)
	}

	if err := mux.ProcessTask(context.Background(), NewTask("DataPipeline", nil)); err != nil {
		t.Fatalf("ProcessTask: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// All six handlers must have executed.
	for _, name := range []string{"ingest", "clean", "fetch", "validate", "load", "alert"} {
		if _, ok := receivedParams[name]; !ok {
			t.Errorf("handler %q did not run", name)
		}
	}

	// Build position map from execution log.
	pos := map[string]int{}
	for i, name := range executionLog {
		pos[name] = i
	}

	// mustPrecede asserts that step a ran strictly before step b.
	mustPrecede := func(a, b string) {
		t.Helper()
		if pos[a] >= pos[b] {
			t.Errorf("expected %s before %s, got order: %v", a, b, executionLog)
		}
	}
	mustPrecede("ingest", "clean")
	mustPrecede("ingest", "fetch")
	mustPrecede("clean", "validate")
	mustPrecede("fetch", "validate")
	mustPrecede("validate", "load")
	mustPrecede("validate", "alert")

	// ingest params must reach clean and fetch (fan-out).
	if got := receivedParams["clean"]["record_id"]; got != "rec-42" {
		t.Errorf("clean.record_id = %v, want rec-42", got)
	}
	if got := receivedParams["fetch"]["record_id"]; got != "rec-42" {
		t.Errorf("fetch.record_id = %v, want rec-42", got)
	}

	// validate must receive params from both clean and fetch (fan-in merge).
	if receivedParams["validate"]["cleaned_count"] == nil {
		t.Error("validate: missing cleaned_count (expected from clean step)")
	}
	if receivedParams["validate"]["log_count"] == nil {
		t.Error("validate: missing log_count (expected from fetch step)")
	}

	// Both load and alert receive validate's output params.
	if got := receivedParams["load"]["status"]; got != "pass" {
		t.Errorf("load.status = %v, want pass", got)
	}
	if got := receivedParams["alert"]["status"]; got != "pass" {
		t.Errorf("alert.status = %v, want pass", got)
	}
}

// ---- Missing-handler behaviour tests ----
//
// When a handler referenced in the workflow markdown is not registered with
// the mux, InitiateAllFlows silently drops every edge whose endpoint has no
// matching handler (it logs "[ERR] error adding edge").  The three tests
// below pin the resulting runtime behaviour so regressions are caught.

// TestWorkflow_MissingHandler_NoneRegistered: if no handlers in the flow are
// registered, UseWorkflow returns a validation error listing all missing names.
func TestWorkflow_MissingHandler_NoneRegistered(t *testing.T) {
	mux := NewServeMux()
	// intentionally register nothing

	flow, err := RegisterFlowMarkdown("Pipeline", `
graph LR
    ingest --> validate
    validate --> load
`)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}

	err = mux.UseWorkflow(NewWorkflow(flow))
	if err == nil {
		t.Fatal("expected UseWorkflow to return error for unregistered handlers, got nil")
	}
	for _, missing := range []string{"ingest", "validate", "load"} {
		if !strings.Contains(err.Error(), missing) {
			t.Errorf("error should mention missing handler %q: %v", missing, err)
		}
	}
}

// TestWorkflow_MissingHandler_TailStepsMissing: when downstream steps are not
// registered, UseWorkflow returns a validation error listing the missing names.
func TestWorkflow_MissingHandler_TailStepsMissing(t *testing.T) {
	mux := NewServeMux()
	mux.HandleStep("ingest", func(_ context.Context, _ *Task, _ FlowParams) (FlowParams, error) {
		return FlowParams{"id": 1}, nil
	})
	// "validate" and "load" intentionally not registered

	flow, err := RegisterFlowMarkdown("Pipeline", `
graph LR
    ingest --> validate
    validate --> load
`)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}

	err = mux.UseWorkflow(NewWorkflow(flow))
	if err == nil {
		t.Fatal("expected UseWorkflow to return error for unregistered handlers, got nil")
	}
	for _, missing := range []string{"validate", "load"} {
		if !strings.Contains(err.Error(), missing) {
			t.Errorf("error should mention missing handler %q: %v", missing, err)
		}
	}
	if strings.Contains(err.Error(), "ingest") {
		t.Errorf("error should not mention registered handler 'ingest': %v", err)
	}
}

// ---- subgraph tests ----

// TestParseMarkdown_Subgraph_BasicExpansion: a subgraph referenced by name in the
// parent graph is expanded to its entry node.
func TestParseMarkdown_Subgraph_BasicExpansion(t *testing.T) {
	g, err := parseMarkdown(`
graph LR
    clean --> DataValidation

    subgraph DataValidation [DataValidation]
        validate --> check
        check --> done
    end
`)
	if err != nil {
		t.Fatalf("parseMarkdown: %v", err)
	}

	// "clean --> DataValidation" should become "clean --> validate" (entry node).
	wantEdge := func(from, to string) {
		for _, e := range g.edges {
			if e.from == from && e.to == to {
				return
			}
		}
		t.Errorf("expected edge %s-->%s not found in %+v", from, to, g.edges)
	}
	wantEdge("clean", "validate")
	wantEdge("validate", "check")
	wantEdge("check", "done")

	// The raw "clean --> DataValidation" edge must be gone.
	for _, e := range g.edges {
		if e.to == "DataValidation" {
			t.Errorf("unexpected edge targeting subgraph name: %+v", e)
		}
	}

	// All subgraph nodes absorbed into parent.
	for _, id := range []string{"validate", "check", "done"} {
		if _, ok := g.nodes[id]; !ok {
			t.Errorf("expected node %q to be present in graph", id)
		}
	}
}

// TestParseMarkdown_Subgraph_FanIn: two parent edges targeting the same subgraph
// both expand to the subgraph's single entry node.
func TestParseMarkdown_Subgraph_FanIn(t *testing.T) {
	g, err := parseMarkdown(`
graph LR
    clean --> DataValidation
    fetch --> DataValidation

    subgraph DataValidation [DataValidation]
        validate --> check
    end
`)
	if err != nil {
		t.Fatalf("parseMarkdown: %v", err)
	}

	countEdgesTo := func(to string) int {
		n := 0
		for _, e := range g.edges {
			if e.to == to {
				n++
			}
		}
		return n
	}

	if countEdgesTo("validate") != 2 {
		t.Errorf("expected 2 edges to entry node 'validate', got %d (edges: %+v)", countEdgesTo("validate"), g.edges)
	}
	if countEdgesTo("DataValidation") != 0 {
		t.Errorf("expected 0 edges to subgraph name, got %d", countEdgesTo("DataValidation"))
	}
}

// TestParseMarkdown_Subgraph_ExitEdgesPassThrough: edges from subgraph nodes to
// external nodes are parsed as normal edges and kept as-is.
func TestParseMarkdown_Subgraph_ExitEdgesPassThrough(t *testing.T) {
	g, err := parseMarkdown(`
graph LR
    start --> DataValidation

    subgraph DataValidation [DataValidation]
        validate --> check
        check --> done
        check --> alert
    end

    done --> load
    alert --> load
`)
	if err != nil {
		t.Fatalf("parseMarkdown: %v", err)
	}

	wantEdge := func(from, to string) {
		for _, e := range g.edges {
			if e.from == from && e.to == to {
				return
			}
		}
		t.Errorf("expected edge %s-->%s not found", from, to)
	}
	wantEdge("done", "load")
	wantEdge("alert", "load")
}

// TestParseMarkdown_Subgraph_DirectionIgnored: "direction LR" inside a subgraph
// is silently ignored and does not cause a parse error.
func TestParseMarkdown_Subgraph_DirectionIgnored(t *testing.T) {
	_, err := parseMarkdown(`
graph LR
    start --> Validation

    subgraph Validation [Validation]
        direction LR
        a --> b
    end
`)
	if err != nil {
		t.Fatalf("parseMarkdown returned error for 'direction' line: %v", err)
	}
}

// TestParseMarkdown_Subgraph_UnclosedError: a subgraph block with no "end" line
// must return an error.
func TestParseMarkdown_Subgraph_UnclosedError(t *testing.T) {
	_, err := parseMarkdown(`
graph LR
    start --> Validation

    subgraph Validation [Validation]
        a --> b
`)
	if err == nil {
		t.Fatal("expected error for unclosed subgraph, got nil")
	}
	if !strings.Contains(err.Error(), "unclosed subgraph") {
		t.Errorf("error should mention 'unclosed subgraph', got: %v", err)
	}
}

// TestParseMarkdown_Subgraph_DuplicateError: two subgraph blocks with the same
// name must return an error.
func TestParseMarkdown_Subgraph_DuplicateError(t *testing.T) {
	_, err := parseMarkdown(`
graph LR
    start --> A

    subgraph A [A]
        a --> b
    end

    subgraph A [A]
        c --> d
    end
`)
	if err == nil {
		t.Fatal("expected error for duplicate subgraph name, got nil")
	}
}

// TestRegisterFlowMarkdown_SubgraphFullExample: the complete DataPipeline example
// from the design doc parses without error and produces the expected flat edges.
func TestRegisterFlowMarkdown_SubgraphFullExample(t *testing.T) {
	opt, err := RegisterFlowMarkdown("DataPipeline", `
graph LR
    Ingest[ingest] --> Clean[clean]
    Ingest --> Fetch[fetch]

    Clean --> DataValidation
    Fetch --> DataValidation

    subgraph DataValidation [DataValidation]
        direction LR
        Validate[validate] --> Check[check]
        Check --> Done[done]
        Check --> Alert[alert]
    end

    Done --> Load[load]
    Alert --> Load
`)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}

	if opt.GetNameGroup() != "DataPipeline" {
		t.Errorf("flow name = %q, want DataPipeline", opt.GetNameGroup())
	}

	// Build a quick from→to lookup.
	type pair struct{ from, to string }
	edges := make(map[pair]bool)
	for _, pf := range opt.GetFlows() {
		edges[pair{pf.from, pf.to}] = true
	}

	want := []pair{
		{"Ingest", "Clean"},
		{"Ingest", "Fetch"},
		{"Clean", "Validate"},  // expanded from Clean --> DataValidation
		{"Fetch", "Validate"},  // expanded from Fetch --> DataValidation
		{"Validate", "Check"},
		{"Check", "Done"},
		{"Check", "Alert"},
		{"Done", "Load"},
		{"Alert", "Load"},
	}
	for _, p := range want {
		if !edges[p] {
			t.Errorf("expected edge %s-->%s not found in flows: %+v", p.from, p.to, opt.GetFlows())
		}
	}
	if edges[pair{"Clean", "DataValidation"}] {
		t.Error("raw 'Clean-->DataValidation' edge should have been expanded away")
	}
}

// TestWorkflow_Subgraph_EndToEnd: registers the full DataPipeline markdown and
// runs ProcessSequence, verifying all nodes execute and params flow correctly.
func TestWorkflow_Subgraph_EndToEnd(t *testing.T) {
	var mu sync.Mutex
	executed := []string{}
	record := func(name string) func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
		return func(_ context.Context, _ *Task, in FlowParams) (FlowParams, error) {
			mu.Lock()
			executed = append(executed, name)
			mu.Unlock()
			out := maps.Clone(in)
			if out == nil {
				out = FlowParams{}
			}
			out[name] = true
			return out, nil
		}
	}

	mux := NewServeMux()
	for _, name := range []string{"Ingest", "Clean", "Fetch", "Validate", "Check", "Done", "Alert", "Load"} {
		mux.HandleStep(name, record(name))
	}

	opt, err := RegisterFlowMarkdown("DataPipeline", `
graph LR
    Ingest[ingest] --> Clean[clean]
    Ingest --> Fetch[fetch]

    Clean --> DataValidation
    Fetch --> DataValidation

    subgraph DataValidation [DataValidation]
        direction LR
        Validate[validate] --> Check[check]
        Check --> Done[done]
        Check --> Alert[alert]
    end

    Done --> Load[load]
    Alert --> Load
`)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}

	wf := NewWorkflow(opt)
	if err := mux.UseWorkflow(wf); err != nil {
		t.Fatalf("UseWorkflow: %v", err)
	}

	task := NewTask("DataPipeline", nil)
	flows := wf.GetAllFlows()
	flow, ok := flows["DataPipeline"]
	if !ok {
		t.Fatal("flow 'DataPipeline' not found")
	}
	if err := flow.ProcessSequence(context.Background(), task); err != nil {
		t.Fatalf("ProcessSequence: %v", err)
	}

	// All 8 handlers must have run.
	got := make(map[string]bool)
	for _, name := range executed {
		got[name] = true
	}
	for _, name := range []string{"Ingest", "Clean", "Fetch", "Validate", "Check", "Done", "Alert", "Load"} {
		if !got[name] {
			t.Errorf("handler %q did not execute", name)
		}
	}

	// Ingest must precede Clean, Fetch, Validate, Check, Done, Alert, Load.
	indexOf := func(name string) int {
		for i, n := range executed {
			if n == name {
				return i
			}
		}
		return -1
	}
	ingestIdx := indexOf("Ingest")
	for _, name := range []string{"Clean", "Fetch", "Validate", "Check", "Done", "Alert", "Load"} {
		if indexOf(name) <= ingestIdx {
			t.Errorf("Ingest (idx %d) should run before %s (idx %d)", ingestIdx, name, indexOf(name))
		}
	}
}

// TestWorkflow_MissingHandler_MidChainMissing: when a handler in the middle of
// the chain is absent from the mux, UseWorkflow returns a validation error
// naming specifically that missing handler.
func TestWorkflow_MissingHandler_MidChainMissing(t *testing.T) {
	mux := NewServeMux()
	mux.HandleStep("ingest", func(_ context.Context, _ *Task, _ FlowParams) (FlowParams, error) {
		return FlowParams{"id": 1}, nil
	})
	// "validate" is the missing middle step
	mux.HandleStep("load", func(_ context.Context, _ *Task, _ FlowParams) (FlowParams, error) {
		return nil, nil
	})

	flow, err := RegisterFlowMarkdown("Pipeline", `
graph LR
    ingest --> validate
    validate --> load
`)
	if err != nil {
		t.Fatalf("RegisterFlowMarkdown: %v", err)
	}

	err = mux.UseWorkflow(NewWorkflow(flow))
	if err == nil {
		t.Fatal("expected UseWorkflow to return error for missing 'validate' handler, got nil")
	}
	if !strings.Contains(err.Error(), "validate") {
		t.Errorf("error should mention missing handler 'validate': %v", err)
	}
	for _, registered := range []string{"ingest", "load"} {
		if strings.Contains(err.Error(), registered) {
			t.Errorf("error should not mention registered handler %q: %v", registered, err)
		}
	}
}

