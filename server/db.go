package main

import (
	"database/sql"
	"encoding/json"
)

// nullStringJSON marshals as string or null (for API contract: "file": "x" or "file": null).
type nullStringJSON struct{ sql.NullString }

func (n nullStringJSON) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.String)
}

func (n *nullStringJSON) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Valid = false
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	n.String, n.Valid = s, true
	return nil
}

// nullInt64JSON marshals as number or null.
type nullInt64JSON struct{ sql.NullInt64 }

func (n nullInt64JSON) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Int64)
}

func (n *nullInt64JSON) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Valid = false
		return nil
	}
	var i int64
	if err := json.Unmarshal(data, &i); err != nil {
		return err
	}
	n.Int64, n.Valid = i, true
	return nil
}

// DB wraps *sql.DB and provides CPG query helpers.
type DB struct {
	*sql.DB
}

// NewDB returns a DB wrapper.
func NewDB(db *sql.DB) *DB {
	return &DB{DB: db}
}

// Node is a CPG node for API responses.
type Node struct {
	ID             string        `json:"id"`
	Kind           string        `json:"kind"`
	Name           string        `json:"name"`
	File           nullStringJSON `json:"file"`
	Line           nullInt64JSON  `json:"line"`
	EndLine        nullInt64JSON  `json:"end_line,omitempty"`
	Package        nullStringJSON `json:"package"`
	ParentFunction nullStringJSON `json:"parent_function,omitempty"`
	TypeInfo       nullStringJSON `json:"type_info,omitempty"`
	Direction      string        `json:"direction,omitempty"` // caller/callee from neighborhood
	Depth          int           `json:"depth,omitempty"`
}

// Edge is a CPG edge for API responses.
type Edge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Kind   string `json:"kind"`
}

// Subgraph is the unified response format: nodes + edges.
type Subgraph struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

// PackageGraphNode is a package node for package map (from treemap).
type PackageGraphNode struct {
	ID               string  `json:"id"`
	Package          string  `json:"package"`
	FileCount        int     `json:"file_count"`
	FunctionCount    int     `json:"function_count"`
	TotalLoc         int     `json:"total_loc"`
	TotalComplexity  int     `json:"total_complexity"`
	AvgComplexity    float64 `json:"avg_complexity"`
	MaxComplexity    int     `json:"max_complexity"`
	TypeCount        int     `json:"type_count"`
	InterfaceCount   int     `json:"interface_count"`
}

// PackageGraphEdge is a package dependency edge.
type PackageGraphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Weight int    `json:"weight"`
}

// PackageGraphResponse is the package map API response.
type PackageGraphResponse struct {
	Nodes []PackageGraphNode `json:"nodes"`
	Edges []PackageGraphEdge `json:"edges"`
}

// FunctionDetail is one row from dashboard_function_detail.
type FunctionDetail struct {
	FunctionID   string `json:"id"`
	Name         string `json:"name"`
	Package      string `json:"package"`
	File         string `json:"file"`
	Line         int    `json:"line"`
	EndLine      int    `json:"end_line"`
	Signature    string `json:"signature"`
	Complexity   int    `json:"complexity"`
	Loc          int    `json:"loc"`
	FanIn        int    `json:"fan_in"`
	FanOut       int    `json:"fan_out"`
	NumParams    int    `json:"num_params"`
	NumLocals    int    `json:"num_locals"`
	NumCalls     int    `json:"num_calls"`
	NumBranches  int    `json:"num_branches"`
	NumReturns   int    `json:"num_returns"`
	FindingCount int    `json:"finding_count"`
	Callers      string `json:"callers,omitempty"`
	Callees      string `json:"callees,omitempty"`
}

const maxSubgraphNodes = 200
