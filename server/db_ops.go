package main

import (
	"database/sql"
	"fmt"
	"strings"
)

// Search runs symbol_search and returns nodes (id, kind, name, file, line, package).
func (db *DB) Search(pattern string, limit int) ([]Node, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	// LIKE pattern: user may pass "Foo" -> we use %Foo%
	like := "%" + pattern + "%"
	rows, err := db.Query(querySymbolSearch, like, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		var pkg, file sql.NullString
		var line sql.NullInt64
		if err := rows.Scan(&n.ID, &n.Name, &n.Kind, &pkg, &file, &line); err != nil {
			return nil, err
		}
		n.Package = nullStringJSON{pkg}
		n.File = nullStringJSON{file}
		n.Line = nullInt64JSON{line}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []Node{}
	}
	return out, nil
}

// Subgraph returns nodes and edges for the neighborhood of function nodeID (callers + callees), capped at maxSubgraphNodes.
// If nodeID is not in the DB, the central node is omitted but neighbors from the neighborhood query may still be returned;
// callers may treat empty nodes or a missing center as "unknown node_id" and respond with 404 if desired.
func (db *DB) Subgraph(nodeID string, limit int) (*Subgraph, error) {
	if limit <= 0 || limit > maxSubgraphNodes {
		limit = maxSubgraphNodes
	}
	rows, err := db.Query(queryFunctionNeighborhood, nodeID, nodeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	nodeSet := map[string]struct{}{nodeID: {}}
	var nodes []Node
	for rows.Next() {
		var n Node
		var dir string
		var pkg, file sql.NullString
		var line sql.NullInt64
		if err := rows.Scan(&dir, &n.ID, &n.Name, &pkg, &file, &line); err != nil {
			return nil, err
		}
		n.Kind = "function"
		n.Direction = dir
		n.Package = nullStringJSON{pkg}
		n.File = nullStringJSON{file}
		n.Line = nullInt64JSON{line}
		nodeSet[n.ID] = struct{}{}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Central node: fetch from nodes table
	var center Node
	var cf, cpkg, cpf, ctype sql.NullString
	var cline, cend sql.NullInt64
	err = db.QueryRow("SELECT id, kind, name, file, line, end_line, package, parent_function, type_info FROM nodes WHERE id = ?", nodeID).Scan(
		&center.ID, &center.Kind, &center.Name, &cf, &cline, &cend, &cpkg, &cpf, &ctype)
	if err == nil {
		center.File = nullStringJSON{cf}
		center.Line = nullInt64JSON{cline}
		center.EndLine = nullInt64JSON{cend}
		center.Package = nullStringJSON{cpkg}
		center.ParentFunction = nullStringJSON{cpf}
		center.TypeInfo = nullStringJSON{ctype}
	}
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == nil {
		// prepend center
		nodes = append([]Node{center}, nodes...)
	}
	// Edges: call edges between our node set
	ids := make([]string, 0, len(nodeSet))
	for id := range nodeSet {
		ids = append(ids, id)
	}
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]
	q := fmt.Sprintf("SELECT source, target, kind FROM edges WHERE kind = 'call' AND source IN (%s) AND target IN (%s) LIMIT 500", placeholders, placeholders)
	args := make([]interface{}, 0, len(ids)*2)
	for _, id := range ids {
		args = append(args, id)
	}
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err = db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var edges []Edge
	for rows.Next() {
		var e Edge
		if err := rows.Scan(&e.Source, &e.Target, &e.Kind); err != nil {
			return nil, err
		}
		edges = append(edges, e)
	}
	return &Subgraph{Nodes: nodes, Edges: edges}, rows.Err()
}

// PackageGraph returns package graph (treemap nodes + graph edges), limited to maxPackageGraphNodes and maxPackageGraphEdges.
// Edges are filtered so that only edges whose source and target exist in the returned node set are included (avoids Cytoscape "nonexistant source/target" errors when limits differ).
func (db *DB) PackageGraph() (*PackageGraphResponse, error) {
	rows, err := db.Query(queryDashboardPackageGraph, maxPackageGraphEdges)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var allEdges []PackageGraphEdge
	for rows.Next() {
		var e PackageGraphEdge
		if err := rows.Scan(&e.Source, &e.Target, &e.Weight); err != nil {
			return nil, err
		}
		allEdges = append(allEdges, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows2, err := db.Query(queryDashboardPackageTreemap, maxPackageGraphNodes)
	if err != nil {
		return nil, err
	}
	defer rows2.Close()
	var nodes []PackageGraphNode
	nodeIDs := make(map[string]struct{})
	for rows2.Next() {
		var n PackageGraphNode
		if err := rows2.Scan(&n.Package, &n.FileCount, &n.FunctionCount, &n.TotalLoc, &n.TotalComplexity, &n.AvgComplexity, &n.MaxComplexity, &n.TypeCount, &n.InterfaceCount); err != nil {
			return nil, err
		}
		n.ID = n.Package
		nodes = append(nodes, n)
		nodeIDs[n.Package] = struct{}{}
	}
	if err := rows2.Err(); err != nil {
		return nil, err
	}
	// Only include edges whose both endpoints are in the node set (consistent subgraph for viz).
	var edges []PackageGraphEdge
	for _, e := range allEdges {
		if _, okSrc := nodeIDs[e.Source]; okSrc {
			if _, okTgt := nodeIDs[e.Target]; okTgt {
				edges = append(edges, e)
			}
		}
	}
	return &PackageGraphResponse{Nodes: nodes, Edges: edges}, nil
}

// PackageFunctions returns function list for a package (by package id/name).
func (db *DB) PackageFunctions(packageIDOrName string) ([]FunctionDetail, error) {
	like := "%" + packageIDOrName + "%"
	rows, err := db.Query(queryDashboardFunctionDetailByPackage, packageIDOrName, like)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FunctionDetail
	for rows.Next() {
		var f FunctionDetail
		var pkg, file, sig, callers, callees sql.NullString
		if err := rows.Scan(&f.FunctionID, &f.Name, &pkg, &file, &f.Line, &f.EndLine, &sig,
			&f.Complexity, &f.Loc, &f.FanIn, &f.FanOut, &f.NumParams, &f.NumLocals, &f.NumCalls, &f.NumBranches, &f.NumReturns, &f.FindingCount, &callers, &callees); err != nil {
			return nil, err
		}
		if pkg.Valid {
			f.Package = pkg.String
		}
		if file.Valid {
			f.File = file.String
		}
		if sig.Valid {
			f.Signature = sig.String
		}
		if callers.Valid {
			f.Callers = callers.String
		}
		if callees.Valid {
			f.Callees = callees.String
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// Source returns file content by path (key in sources table).
func (db *DB) Source(filePath string) (content string, packageName string, err error) {
	err = db.QueryRow(querySourceByFile, filePath).Scan(&filePath, &content, &packageName)
	return content, packageName, err
}

// Slice returns backward or forward slice as subgraph (nodes + edges).
func (db *DB) Slice(nodeID string, direction string, limit int) (*Subgraph, error) {
	if limit <= 0 || limit > maxSubgraphNodes {
		limit = maxSubgraphNodes
	}
	var query string
	switch direction {
	case "backward":
		query = queryBackwardSlice
	case "forward":
		query = queryForwardSlice
	default:
		query = queryBackwardSlice
	}
	rows, err := db.Query(query, nodeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	nodeSet := map[string]struct{}{}
	var nodes []Node
	for rows.Next() {
		var n Node
		var f, pkg, pf, ti sql.NullString
		var line, endLine sql.NullInt64
		if err := rows.Scan(&n.ID, &n.Kind, &n.Name, &f, &line, &endLine, &pkg, &pf, &ti); err != nil {
			return nil, err
		}
		n.File = nullStringJSON{f}
		n.Line = nullInt64JSON{line}
		n.EndLine = nullInt64JSON{endLine}
		n.Package = nullStringJSON{pkg}
		n.ParentFunction = nullStringJSON{pf}
		n.TypeInfo = nullStringJSON{ti}
		nodeSet[n.ID] = struct{}{}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(nodeSet))
	for id := range nodeSet {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return &Subgraph{Nodes: nodes, Edges: []Edge{}}, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	q := fmt.Sprintf("SELECT source, target, kind FROM edges WHERE source IN (%s) AND target IN (%s) AND kind IN ('dfg','param_in','param_out') LIMIT 1000", ph, ph)
	args := make([]interface{}, 0, len(ids)*2)
	for _, id := range ids {
		args = append(args, id)
	}
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err = db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var edges []Edge
	for rows.Next() {
		var e Edge
		if err := rows.Scan(&e.Source, &e.Target, &e.Kind); err != nil {
			return nil, err
		}
		edges = append(edges, e)
	}
	return &Subgraph{Nodes: nodes, Edges: edges}, rows.Err()
}
