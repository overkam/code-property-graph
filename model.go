package main

import "encoding/json"

// Node represents a vertex in the Code Property Graph.
type Node struct {
	ID             string
	Kind           string
	Name           string
	File           string // relative to repo root
	Line, Col      int
	EndLine        int
	Package        string // relative import path
	ParentFunction string // node ID of enclosing function, or ""
	TypeInfo       string
	Properties     map[string]any
}

// Edge represents a directed edge in the Code Property Graph.
type Edge struct {
	Source     string
	Target     string
	Kind       string
	Properties map[string]any
}

// Metrics holds computed metrics for a single function.
type Metrics struct {
	FunctionID           string
	CyclomaticComplexity int
	FanIn                int
	FanOut               int
	LOC                  int
	NumParams            int
}

// edgeKey is the deduplication key for edges.
type edgeKey struct {
	Source, Target, Kind string
}

// CPG accumulates the entire Code Property Graph in memory before flushing to SQLite.
type CPG struct {
	Nodes    []Node
	Edges    []Edge
	nodeSeen map[string]struct{}
	edgeSeen map[edgeKey]struct{}
	Sources  map[string]string   // file → content
	Metrics  map[string]*Metrics // function_id → metrics
}

// NewCPG creates an empty CPG ready for population.
func NewCPG() *CPG {
	return &CPG{
		nodeSeen: make(map[string]struct{}),
		edgeSeen: make(map[edgeKey]struct{}),
		Sources:  make(map[string]string),
		Metrics:  make(map[string]*Metrics),
	}
}

// AddNode appends a node, deduplicating by ID (first wins).
func (g *CPG) AddNode(n Node) {
	if _, dup := g.nodeSeen[n.ID]; dup {
		return
	}
	g.nodeSeen[n.ID] = struct{}{}
	g.Nodes = append(g.Nodes, n)
}

// AddEdge appends an edge if no edge with the same (source, target, kind) already exists.
func (g *CPG) AddEdge(e Edge) {
	k := edgeKey{e.Source, e.Target, e.Kind}
	if _, dup := g.edgeSeen[k]; dup {
		return
	}
	g.edgeSeen[k] = struct{}{}
	g.Edges = append(g.Edges, e)
}

// PropsJSON marshals a properties map to JSON string, or "" if empty.
func PropsJSON(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(b)
}
