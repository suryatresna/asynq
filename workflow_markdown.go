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
	nodes map[string]struct{}
	edges []mdEdge
}

func (g *mdGraph) addNode(id string) {
	g.nodes[id] = struct{}{}
}

// mdSubgraph holds the nodes and internal edges of a subgraph block.
type mdSubgraph struct {
	nodes map[string]struct{}
	edges []mdEdge
}

// entryNodes returns node IDs with no incoming edges within the subgraph.
func (s *mdSubgraph) entryNodes() []string {
	toSet := make(map[string]struct{})
	for _, e := range s.edges {
		toSet[e.to] = struct{}{}
	}
	var entries []string
	for id := range s.nodes {
		if _, isTarget := toSet[id]; !isTarget {
			entries = append(entries, id)
		}
	}
	return entries
}

// mdNodeSpec matches a node reference: id  or  id[label]  or  id(label)  or  id{label}.
// Only the id (first capture group) is used; the display label is consumed but discarded.
const mdNodeSpec = `(\w+)(?:\[[^\]]*\]|\([^)]*\)|\{[^}]*\})?`

var (
	// graph  or  graph LR  or  graph TD  (direction is cosmetic; flow name is passed separately)
	mdHeaderRE = regexp.MustCompile(`^\s*graph\s*(?:LR|TD|TB|RL|BT)?\s*$`)

	// subgraph Name  or  subgraph Name [Display Label]
	mdSubgraphHeaderRE = regexp.MustCompile(`^\s*subgraph\s+(\w+)(?:\s+\[.*\])?\s*$`)

	// direction LR/TD/TB/RL/BT  (cosmetic inside subgraph — ignored)
	mdDirectionRE = regexp.MustCompile(`^\s*direction\s+(LR|TD|TB|RL|BT)\s*$`)

	// A -->|label| B
	mdLabeledEdgeRE = regexp.MustCompile(`^\s*` + mdNodeSpec + `\s*-->\|([^|]+)\|\s*` + mdNodeSpec + `\s*$`)

	// A --> B
	mdSimpleEdgeRE = regexp.MustCompile(`^\s*` + mdNodeSpec + `\s*-->\s*` + mdNodeSpec + `\s*$`)

	// Standalone node declaration: A  or  A[label]  or  A(label)  or  A{label}
	mdNodeOnlyRE = regexp.MustCompile(`^\s*` + mdNodeSpec + `\s*$`)
)

func parseMarkdown(src string) (*mdGraph, error) {
	g := &mdGraph{nodes: make(map[string]struct{})}
	subgraphs := make(map[string]*mdSubgraph)

	var headerSeen bool
	var currentSG *mdSubgraph // non-nil while inside a subgraph block

	lines := strings.Split(strings.ReplaceAll(src, "\r", ""), "\n")
	for i, raw := range lines {
		lineNum := i + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "%%") {
			continue
		}

		// Close a subgraph block.
		if currentSG != nil {
			if line == "end" {
				currentSG = nil
				continue
			}
			if mdDirectionRE.MatchString(line) {
				continue
			}
			if m := mdLabeledEdgeRE.FindStringSubmatch(line); m != nil {
				fromID, edgeLabel, toID := m[1], strings.TrimSpace(m[2]), m[3]
				currentSG.nodes[fromID] = struct{}{}
				currentSG.nodes[toID] = struct{}{}
				currentSG.edges = append(currentSG.edges, mdEdge{from: fromID, to: toID, label: edgeLabel})
				continue
			}
			if m := mdSimpleEdgeRE.FindStringSubmatch(line); m != nil {
				fromID, toID := m[1], m[2]
				currentSG.nodes[fromID] = struct{}{}
				currentSG.nodes[toID] = struct{}{}
				currentSG.edges = append(currentSG.edges, mdEdge{from: fromID, to: toID, label: fromID + "->" + toID})
				continue
			}
			if m := mdNodeOnlyRE.FindStringSubmatch(line); m != nil {
				currentSG.nodes[m[1]] = struct{}{}
				continue
			}
			return nil, fmt.Errorf("line %d: unrecognized syntax inside subgraph: %q", lineNum, line)
		}

		if mdHeaderRE.MatchString(line) {
			if headerSeen {
				return nil, fmt.Errorf("line %d: duplicate 'graph' header", lineNum)
			}
			headerSeen = true
			continue
		}

		if !headerSeen {
			return nil, fmt.Errorf("line %d: expected 'graph' header before edge definitions", lineNum)
		}

		// Open a subgraph block.
		if m := mdSubgraphHeaderRE.FindStringSubmatch(line); m != nil {
			sgName := m[1]
			if _, exists := subgraphs[sgName]; exists {
				return nil, fmt.Errorf("line %d: duplicate subgraph %q", lineNum, sgName)
			}
			sg := &mdSubgraph{nodes: make(map[string]struct{})}
			subgraphs[sgName] = sg
			currentSG = sg
			continue
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

	if currentSG != nil {
		return nil, errors.New("unclosed subgraph block: missing 'end'")
	}

	if !headerSeen {
		return nil, errors.New("missing 'graph' header")
	}

	// Post-processing: flatten subgraphs into the parent graph.
	if len(subgraphs) > 0 {
		// Add all subgraph-internal edges and nodes to the parent.
		for _, sg := range subgraphs {
			for id := range sg.nodes {
				g.addNode(id)
			}
			g.edges = append(g.edges, sg.edges...)
		}

		// Expand any parent-level edge whose target is a subgraph name into
		// edges targeting each entry node of that subgraph.
		var expanded []mdEdge
		for _, e := range g.edges {
			sg, isSGTarget := subgraphs[e.to]
			if !isSGTarget {
				expanded = append(expanded, e)
				continue
			}
			entries := sg.entryNodes()
			if len(entries) == 0 {
				return nil, fmt.Errorf("subgraph %q has no entry nodes", e.to)
			}
			for _, entry := range entries {
				expanded = append(expanded, mdEdge{from: e.from, to: entry, label: e.from + "->" + entry})
			}
		}
		g.edges = expanded
	}

	return g, nil
}

// RegisterFlowMarkdown parses a Mermaid graph definition and returns a
// WorkflowOptionInterface ready to be passed to NewWorkflow.
//
// name is the flow group name (matched against the task type when dispatching).
//
// Supported syntax:
//
//	graph LR
//	    A --> B               simple edge (label auto-generated as "A->B")
//	    A -->|label| B        edge with an explicit label
//	    A[Display label]      node with a rectangle annotation (cosmetic only)
//	    A(Display label)      node with a rounded annotation
//	    A{Display label}      node with a diamond annotation
//	    subgraph Name [Label]
//	        X --> Y           subgraph internal edge (expanded inline)
//	    end
//	%% comment line
//
// The graph direction (LR, TD, TB, RL, BT) is cosmetic and ignored during
// wiring. Node IDs must match the handler names registered with
// mux.HandleFunc or mux.HandleStep.
func RegisterFlowMarkdown(name, markdown string) (WorkflowOptionInterface, error) {
	g, err := parseMarkdown(markdown)
	if err != nil {
		return nil, err
	}
	if len(g.edges) == 0 {
		return nil, fmt.Errorf("flow %q: no edges defined", name)
	}
	opt := &WorkflowOption{
		group: name,
		flows: make([]preFlow, 0, len(g.edges)),
	}
	for _, e := range g.edges {
		opt.SetFlows(e.label, e.from, e.to)
	}
	return opt, nil
}
