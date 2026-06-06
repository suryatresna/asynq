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

// exitNodes returns node IDs with no outgoing edges within the subgraph.
func (s *mdSubgraph) exitNodes() []string {
	fromSet := make(map[string]struct{})
	for _, e := range s.edges {
		fromSet[e.from] = struct{}{}
	}
	var exits []string
	for id := range s.nodes {
		if _, isSource := fromSet[id]; !isSource {
			exits = append(exits, id)
		}
	}
	return exits
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

// expandEdges expands edges whose source or target is a subgraph name.
//   - X --> SubgraphName  →  X --> each entry node of that subgraph
//   - SubgraphName --> X  →  each exit node of that subgraph --> X
//   - SubgraphA --> SubgraphB  →  cross-product of exits(A) × entries(B)
func expandEdges(edges []mdEdge, subgraphs map[string]*mdSubgraph) ([]mdEdge, error) {
	var result []mdEdge
	for _, e := range edges {
		sgTo, isToSG := subgraphs[e.to]
		sgFrom, isFromSG := subgraphs[e.from]
		switch {
		case isToSG && isFromSG:
			exits := sgFrom.exitNodes()
			entries := sgTo.entryNodes()
			if len(exits) == 0 {
				return nil, fmt.Errorf("subgraph %q has no exit nodes", e.from)
			}
			if len(entries) == 0 {
				return nil, fmt.Errorf("subgraph %q has no entry nodes", e.to)
			}
			for _, ex := range exits {
				for _, en := range entries {
					result = append(result, mdEdge{from: ex, to: en, label: ex + "->" + en})
				}
			}
		case isToSG:
			entries := sgTo.entryNodes()
			if len(entries) == 0 {
				return nil, fmt.Errorf("subgraph %q has no entry nodes", e.to)
			}
			for _, entry := range entries {
				result = append(result, mdEdge{from: e.from, to: entry, label: e.from + "->" + entry})
			}
		case isFromSG:
			exits := sgFrom.exitNodes()
			if len(exits) == 0 {
				return nil, fmt.Errorf("subgraph %q has no exit nodes", e.from)
			}
			for _, ex := range exits {
				result = append(result, mdEdge{from: ex, to: e.to, label: ex + "->" + e.to})
			}
		default:
			result = append(result, e)
		}
	}
	return result, nil
}

// expandSubgraphInternals recursively absorbs nested subgraph references into sg
// and expands all reference edges, working bottom-up.
func expandSubgraphInternals(name string, sg *mdSubgraph, subgraphs map[string]*mdSubgraph, done map[string]bool) error {
	if done[name] {
		return nil
	}

	// Recursively expand any nested subgraphs referenced in our edges first.
	for _, e := range sg.edges {
		for _, refName := range []string{e.from, e.to} {
			if nested, ok := subgraphs[refName]; ok && !done[refName] {
				if err := expandSubgraphInternals(refName, nested, subgraphs, done); err != nil {
					return err
				}
			}
		}
	}

	// Absorb the nodes and (already-expanded) edges from each directly referenced subgraph.
	// Also remove the subgraph name from sg.nodes — it was added syntactically when the
	// edge "X --> SubgraphName" was parsed, but it is not a real handler node.
	referenced := map[string]bool{}
	for _, e := range sg.edges {
		if _, ok := subgraphs[e.to]; ok {
			referenced[e.to] = true
		}
		if _, ok := subgraphs[e.from]; ok {
			referenced[e.from] = true
		}
	}
	for refName := range referenced {
		nested := subgraphs[refName]
		for id := range nested.nodes {
			sg.nodes[id] = struct{}{}
		}
		sg.edges = append(sg.edges, nested.edges...)
		delete(sg.nodes, refName) // subgraph name is not a real handler node
	}

	// Expand reference edges in this subgraph.
	newEdges, err := expandEdges(sg.edges, subgraphs)
	if err != nil {
		return err
	}
	sg.edges = newEdges

	done[name] = true
	return nil
}

func parseMarkdown(src string) (*mdGraph, error) {
	g := &mdGraph{nodes: make(map[string]struct{})}
	subgraphs := make(map[string]*mdSubgraph)
	nestedSGs := make(map[string]bool) // subgraphs defined inside another subgraph

	var headerSeen bool
	var sgStack []*mdSubgraph // stack of open subgraph blocks; top = innermost

	lines := strings.Split(strings.ReplaceAll(src, "\r", ""), "\n")
	for i, raw := range lines {
		lineNum := i + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "%%") {
			continue
		}

		// Inside one or more nested subgraph blocks.
		if len(sgStack) > 0 {
			if line == "end" {
				sgStack = sgStack[:len(sgStack)-1] // pop
				continue
			}
			if mdDirectionRE.MatchString(line) {
				continue
			}
			// Nested subgraph open.
			if m := mdSubgraphHeaderRE.FindStringSubmatch(line); m != nil {
				sgName := m[1]
				if _, exists := subgraphs[sgName]; exists {
					return nil, fmt.Errorf("line %d: duplicate subgraph %q", lineNum, sgName)
				}
				sg := &mdSubgraph{nodes: make(map[string]struct{})}
				subgraphs[sgName] = sg
				nestedSGs[sgName] = true
				sgStack = append(sgStack, sg)
				continue
			}
			cur := sgStack[len(sgStack)-1]
			if m := mdLabeledEdgeRE.FindStringSubmatch(line); m != nil {
				fromID, edgeLabel, toID := m[1], strings.TrimSpace(m[2]), m[3]
				cur.nodes[fromID] = struct{}{}
				cur.nodes[toID] = struct{}{}
				cur.edges = append(cur.edges, mdEdge{from: fromID, to: toID, label: edgeLabel})
				continue
			}
			if m := mdSimpleEdgeRE.FindStringSubmatch(line); m != nil {
				fromID, toID := m[1], m[2]
				cur.nodes[fromID] = struct{}{}
				cur.nodes[toID] = struct{}{}
				cur.edges = append(cur.edges, mdEdge{from: fromID, to: toID, label: fromID + "->" + toID})
				continue
			}
			if m := mdNodeOnlyRE.FindStringSubmatch(line); m != nil {
				cur.nodes[m[1]] = struct{}{}
				continue
			}
			return nil, fmt.Errorf("line %d: unrecognized syntax inside subgraph: %q", lineNum, line)
		}

		// Top-level parsing.
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

		// Open a top-level subgraph block.
		if m := mdSubgraphHeaderRE.FindStringSubmatch(line); m != nil {
			sgName := m[1]
			if _, exists := subgraphs[sgName]; exists {
				return nil, fmt.Errorf("line %d: duplicate subgraph %q", lineNum, sgName)
			}
			sg := &mdSubgraph{nodes: make(map[string]struct{})}
			subgraphs[sgName] = sg
			sgStack = append(sgStack, sg)
			continue
		}

		if m := mdLabeledEdgeRE.FindStringSubmatch(line); m != nil {
			fromID, edgeLabel, toID := m[1], strings.TrimSpace(m[2]), m[3]
			g.addNode(fromID)
			g.addNode(toID)
			g.edges = append(g.edges, mdEdge{from: fromID, to: toID, label: edgeLabel})
			continue
		}

		if m := mdSimpleEdgeRE.FindStringSubmatch(line); m != nil {
			fromID, toID := m[1], m[2]
			g.addNode(fromID)
			g.addNode(toID)
			g.edges = append(g.edges, mdEdge{from: fromID, to: toID, label: fromID + "->" + toID})
			continue
		}

		if m := mdNodeOnlyRE.FindStringSubmatch(line); m != nil {
			g.addNode(m[1])
			continue
		}

		return nil, fmt.Errorf("line %d: unrecognized syntax: %q", lineNum, line)
	}

	if len(sgStack) > 0 {
		return nil, errors.New("unclosed subgraph block: missing 'end'")
	}

	if !headerSeen {
		return nil, errors.New("missing 'graph' header")
	}

	// Post-processing: expand nested subgraph internals, then flatten into parent.
	if len(subgraphs) > 0 {
		// Recursively expand all subgraphs bottom-up.
		done := make(map[string]bool)
		for name, sg := range subgraphs {
			if err := expandSubgraphInternals(name, sg, subgraphs, done); err != nil {
				return nil, err
			}
		}

		// Add only top-level subgraph nodes/edges to the parent graph.
		// Nested subgraphs were already absorbed into their parent subgraph.
		for sgName, sg := range subgraphs {
			if nestedSGs[sgName] {
				continue
			}
			for id := range sg.nodes {
				g.addNode(id)
			}
			g.edges = append(g.edges, sg.edges...)
		}

		// Expand parent-level edges that reference subgraph names.
		newEdges, err := expandEdges(g.edges, subgraphs)
		if err != nil {
			return nil, err
		}
		g.edges = newEdges
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
//	        subgraph Nested [Nested]
//	            P --> Q       nested subgraphs are supported at any depth
//	        end
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
