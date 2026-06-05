package asynq

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type mdEdge struct {
	from, to, label string
}

type mdGraph struct {
	name  string
	nodes map[string]struct{}
	edges []mdEdge
}

func (g *mdGraph) addNode(id string) {
	g.nodes[id] = struct{}{}
}

// mdNodeSpec matches a node reference: id  or  id[label]  or  id(label)  or  id{label}.
// Only the id (first capture group) is used; the display label is consumed but discarded.
const mdNodeSpec = `(\w+)(?:\[[^\]]*\]|\([^)]*\)|\{[^}]*\})?`

var (
	mdHeaderRE = regexp.MustCompile(`^\s*graph\s+(\S+)\s*$`)

	// A -->|label| B
	mdLabeledEdgeRE = regexp.MustCompile(`^\s*` + mdNodeSpec + `\s*-->\|([^|]+)\|\s*` + mdNodeSpec + `\s*$`)

	// A --> B
	mdSimpleEdgeRE = regexp.MustCompile(`^\s*` + mdNodeSpec + `\s*-->\s*` + mdNodeSpec + `\s*$`)

	// Standalone node declaration: A  or  A[label]  or  A(label)  or  A{label}
	mdNodeOnlyRE = regexp.MustCompile(`^\s*` + mdNodeSpec + `\s*$`)
)

func parseMarkdown(src string) (*mdGraph, error) {
	g := &mdGraph{nodes: make(map[string]struct{})}

	lines := strings.Split(strings.ReplaceAll(src, "\r", ""), "\n")
	for i, raw := range lines {
		lineNum := i + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "%%") {
			continue
		}

		if m := mdHeaderRE.FindStringSubmatch(line); m != nil {
			if g.name != "" {
				return nil, fmt.Errorf("line %d: duplicate 'graph' header", lineNum)
			}
			g.name = m[1]
			continue
		}

		if g.name == "" {
			return nil, fmt.Errorf("line %d: expected 'graph <Name>' header before edge definitions", lineNum)
		}

		// A -->|label| B
		if m := mdLabeledEdgeRE.FindStringSubmatch(line); m != nil {
			fromID, edgeLabel, toID := m[1], strings.TrimSpace(m[2]), m[3]
			g.addNode(fromID)
			g.addNode(toID)
			g.edges = append(g.edges, mdEdge{from: fromID, to: toID, label: edgeLabel})
			continue
		}

		// A --> B
		if m := mdSimpleEdgeRE.FindStringSubmatch(line); m != nil {
			fromID, toID := m[1], m[2]
			g.addNode(fromID)
			g.addNode(toID)
			g.edges = append(g.edges, mdEdge{from: fromID, to: toID, label: fromID + "->" + toID})
			continue
		}

		// Standalone node declaration (no edge — cosmetic only)
		if m := mdNodeOnlyRE.FindStringSubmatch(line); m != nil {
			g.addNode(m[1])
			continue
		}

		return nil, fmt.Errorf("line %d: unrecognized syntax: %q", lineNum, line)
	}

	if g.name == "" {
		return nil, errors.New("missing 'graph <Name>' header")
	}

	return g, nil
}

// RegisterFlowMarkdown parses a Mermaid-like graph definition and returns a
// WorkflowOptionInterface ready to be passed to NewWorkflow.
//
// Supported syntax:
//
//	graph FlowName
//	    A --> B               simple edge (label auto-generated as "A->B")
//	    A -->|label| B        edge with an explicit label
//	    A[Display label]      node with a rectangle annotation (cosmetic only)
//	    A(Display label)      node with a rounded annotation
//	    A{Display label}      node with a diamond annotation
//	%% comment line
//
// Node IDs (A, B, …) must match the handler names registered with
// mux.HandleFunc or mux.HandleStep. Display annotations are cosmetic and
// ignored during workflow wiring.
func RegisterFlowMarkdown(markdown string) (WorkflowOptionInterface, error) {
	g, err := parseMarkdown(markdown)
	if err != nil {
		return nil, err
	}
	if len(g.edges) == 0 {
		return nil, fmt.Errorf("flow %q: no edges defined", g.name)
	}
	opt := &WorkflowOption{
		group: g.name,
		flows: make([]preFlow, 0, len(g.edges)),
	}
	for _, e := range g.edges {
		opt.SetFlows(e.label, e.from, e.to)
	}
	return opt, nil
}
