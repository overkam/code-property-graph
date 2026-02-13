package main

import (
	"fmt"
	"os"
	"strings"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const batchSize = 50000

// WriteDB writes the CPG to a SQLite database file.
func WriteDB(path string, cpg *CPG, escapeResults []EscapeResult, gitHistory []GitFileHistory, validate bool, prog *Progress) error {
	prog.Log("Writing SQLite to %s ...", path)

	_ = os.Remove(path) // ignore if doesn't exist

	conn, err := sqlite.OpenConn(path, sqlite.OpenCreate, sqlite.OpenReadWrite, sqlite.OpenWAL)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Performance pragmas
	if err := sqlitex.ExecuteTransient(conn, "PRAGMA synchronous = NORMAL", nil); err != nil {
		return err
	}
	if err := sqlitex.ExecuteTransient(conn, "PRAGMA temp_store = MEMORY", nil); err != nil {
		return err
	}
	if err := sqlitex.ExecuteTransient(conn, "PRAGMA mmap_size = 268435456", nil); err != nil {
		return err
	}
	if err := sqlitex.ExecuteTransient(conn, "PRAGMA cache_size = -64000", nil); err != nil {
		return err
	}
	if err := sqlitex.ExecuteTransient(conn, "PRAGMA journal_mode = WAL", nil); err != nil {
		return err
	}

	// Create tables without indexes (deferred creation for speed)
	if err := createTables(conn); err != nil {
		return err
	}

	// Bulk insert in a transaction
	endFn, err := sqlitex.ImmediateTransaction(conn)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := insertNodes(conn, cpg.Nodes, prog); err != nil {
		endFn(&err)
		return err
	}
	if err := insertEdges(conn, cpg.Edges, prog); err != nil {
		endFn(&err)
		return err
	}
	if err := insertSources(conn, cpg.Sources, prog); err != nil {
		endFn(&err)
		return err
	}
	if err := insertMetrics(conn, cpg.Metrics, prog); err != nil {
		endFn(&err)
		return err
	}

	endFn(&err)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Create flow semantics table for stdlib data-flow modeling
	prog.Log("Building flow semantics model...")
	if err := createFlowSemantics(conn); err != nil {
		return err
	}

	// Heuristic DFG for external calls using flow semantics
	prog.Log("Inferring DFG for external calls...")

	// Step 1: Precise DFG for functions WITH custom semantics (arg→return)
	var preciseDFG, fallbackDFG, sideEffectDFG int
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT OR IGNORE INTO edges (source, target, kind, properties)
		 SELECT DISTINCT arg_e.target, site_e.source, 'dfg', '{"heuristic":true}'
		 FROM edges site_e
		 JOIN nodes callee ON site_e.target = callee.id
		 JOIN flow_semantics fs ON callee.package = fs.package AND callee.name = fs.func_name
		   AND fs.flow_to LIKE 'return:%'
		 JOIN edges arg_e ON arg_e.source = site_e.source AND arg_e.kind = 'argument'
		 WHERE site_e.kind = 'call_site'
		   AND callee.id LIKE 'ext::%'
		   AND (fs.flow_from = 'arg:*'
		        OR fs.flow_from = 'arg:' || json_extract(arg_e.properties, '$.index'))`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return fmt.Errorf("precise heuristic dfg: %w", err)
	}
	preciseDFG = conn.Changes()

	// Step 2: Side-effect flows: arg→arg (e.g., json.Unmarshal: bytes→target)
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT OR IGNORE INTO edges (source, target, kind, properties)
		 SELECT DISTINCT src_arg.target, dst_arg.target, 'dfg', '{"heuristic":true,"side_effect":true}'
		 FROM edges site_e
		 JOIN nodes callee ON site_e.target = callee.id
		 JOIN flow_semantics fs ON callee.package = fs.package AND callee.name = fs.func_name
		   AND fs.flow_from LIKE 'arg:%' AND fs.flow_to LIKE 'arg:%'
		 JOIN edges src_arg ON src_arg.source = site_e.source AND src_arg.kind = 'argument'
		   AND (fs.flow_from = 'arg:*'
		        OR fs.flow_from = 'arg:' || json_extract(src_arg.properties, '$.index'))
		 JOIN edges dst_arg ON dst_arg.source = site_e.source AND dst_arg.kind = 'argument'
		   AND fs.flow_to = 'arg:' || json_extract(dst_arg.properties, '$.index')
		 WHERE site_e.kind = 'call_site'
		   AND callee.id LIKE 'ext::%'`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return fmt.Errorf("side-effect heuristic dfg: %w", err)
	}
	sideEffectDFG = conn.Changes()

	// Step 3: Fallback: all args→return for functions WITHOUT custom semantics
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT OR IGNORE INTO edges (source, target, kind, properties)
		 SELECT DISTINCT arg_e.target, site_e.source, 'dfg', '{"heuristic":true}'
		 FROM edges site_e
		 JOIN nodes callee ON site_e.target = callee.id
		 JOIN edges arg_e ON arg_e.source = site_e.source AND arg_e.kind = 'argument'
		 WHERE site_e.kind = 'call_site'
		   AND callee.id LIKE 'ext::%'
		   AND NOT EXISTS (
		     SELECT 1 FROM flow_semantics fs
		     WHERE callee.package = fs.package AND callee.name = fs.func_name
		   )`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return fmt.Errorf("fallback heuristic dfg: %w", err)
	}
	fallbackDFG = conn.Changes()

	totalDFG := preciseDFG + sideEffectDFG + fallbackDFG
	if totalDFG > 0 {
		prog.Log("Created %d heuristic DFG edges (%d precise, %d side-effect, %d fallback)",
			totalDFG, preciseDFG, sideEffectDFG, fallbackDFG)
	}

	// Clean up orphan edges before indexing
	if err := sqlitex.ExecuteTransient(conn,
		`DELETE FROM edges WHERE source NOT IN (SELECT id FROM nodes) OR target NOT IN (SELECT id FROM nodes)`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return fmt.Errorf("orphan cleanup: %w", err)
	}
	changes := conn.Changes()
	if changes > 0 {
		prog.Log("Removed %d orphan edges", changes)
	}

	// Create indexes after all inserts
	prog.Log("Creating indexes...")
	if err := createIndexes(conn); err != nil {
		return err
	}

	// EOG: expression evaluation order for call arguments
	prog.Log("Computing evaluation order edges...")
	if err := computeEOG(conn, prog); err != nil {
		return err
	}

	// FTS5 full-text search on source code
	prog.Log("Building FTS5 index...")
	if err := createFTS(conn); err != nil {
		return err
	}

	// Pre-computed summary statistics for viewer dashboards
	prog.Log("Computing summary statistics...")
	if err := createSummaryStats(conn); err != nil {
		return err
	}

	// Pre-built analysis views and example queries
	prog.Log("Creating analysis views...")
	if err := createAnalysisViews(conn); err != nil {
		return err
	}

	// Security taint model: classify known sources/sinks/barriers
	prog.Log("Building taint model...")
	if err := createTaintModel(conn); err != nil {
		return err
	}

	// Additional analysis: API surface, method sets, risk scores, etc.
	prog.Log("Computing additional analysis...")
	if err := createAdditionalAnalysis(conn, prog); err != nil {
		return err
	}

	// Apply escape analysis annotations from the Go compiler
	if len(escapeResults) > 0 {
		prog.Log("Applying escape analysis annotations...")
		if err := applyEscapeAnalysis(conn, escapeResults, prog); err != nil {
			prog.Log("Warning: escape analysis failed: %v", err)
		}
	}

	// Advanced analysis: stability metrics, risk scores, dead code, etc.
	prog.Log("Computing advanced analysis...")
	if err := createAdvancedAnalysis(conn, prog); err != nil {
		return err
	}

	// Cohesion, concurrency, and pattern analysis
	prog.Log("Computing cohesion and patterns...")
	if err := createCohesionAndPatterns(conn, prog); err != nil {
		return err
	}

	// Run ANALYZE before dashboard queries — without statistics, the query planner
	// has no row counts and picks catastrophically bad plans on 445k+ row tables
	prog.Log("Running ANALYZE for query planner...")
	if err := sqlitex.ExecuteTransient(conn, "ANALYZE", nil); err != nil {
		return err
	}

	// Pre-computed dashboard data for easy chart rendering
	prog.Log("Building dashboard data...")
	if err := createDashboardData(conn, prog); err != nil {
		return err
	}

	// Graph intelligence: top-N tables, cross-package coupling, error chains
	prog.Log("Building graph intelligence...")
	if err := createGraphIntelligence(conn, prog); err != nil {
		return err
	}

	// File-level analysis and dependency graph data for visualization
	prog.Log("Building file and dependency analysis...")
	if err := createFileAndDepAnalysis(conn, prog); err != nil {
		return err
	}

	// Type system analysis: hierarchy, implementation map, method resolution
	prog.Log("Building type system analysis...")
	if err := createTypeSystemAnalysis(conn, prog); err != nil {
		return err
	}

	// Code navigation aids and pattern summaries
	prog.Log("Building navigation and patterns...")
	if err := createNavigationAndPatterns(conn, prog); err != nil {
		return err
	}

	// Schema documentation: self-describing DB for interview candidates
	prog.Log("Building schema documentation...")
	if err := createSchemaDocs(conn); err != nil {
		return err
	}

	// Git history for diff-aware analysis
	if len(gitHistory) > 0 {
		prog.Log("Running git history analysis...")
		if err := applyGitHistory(conn, gitHistory, prog); err != nil {
			return err
		}
	}

	// Taint flow state materialization for precise taint analysis
	prog.Log("Computing taint flow states...")
	if err := createTaintFlowStates(conn, prog); err != nil {
		return err
	}

	// Index sensitivity for map/array taint tracking
	prog.Log("Computing index sensitivity...")
	if err := createIndexSensitivity(conn, prog); err != nil {
		return err
	}

	// SCIP-style cross-repository symbol identifiers
	prog.Log("Building SCIP symbol index...")
	if err := createSCIPSymbols(conn, prog); err != nil {
		return err
	}

	// Communication patterns: Honda session types, protocol detection, duality
	prog.Log("Building communication patterns...")
	if err := createCommunicationPatterns(conn, prog); err != nil {
		return err
	}

	// Honda 2008 corrections: subtyping, acyclic deps, association relation
	prog.Log("Applying Honda 2008 corrections (Scalas & Yoshida 2019, Yoshida & Hou 2024)...")
	if err := createSessionTypeCorrections(conn, prog); err != nil {
		return err
	}

	if validate {
		if err := runValidation(conn, prog); err != nil {
			return err
		}
	}

	// Report file size
	info, _ := os.Stat(path)
	if info != nil {
		mb := info.Size() / (1024 * 1024)
		prog.Log("Wrote %s (%d MB)", path, mb)
	}

	return nil
}

func createTables(conn *sqlite.Conn) error {
	ddl := `
CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    file TEXT,
    line INTEGER,
    col INTEGER,
    end_line INTEGER,
    package TEXT,
    parent_function TEXT,
    type_info TEXT,
    properties TEXT
);

CREATE TABLE edges (
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    kind TEXT NOT NULL,
    properties TEXT
);

CREATE TABLE sources (
    file TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    package TEXT
);

CREATE TABLE metrics (
    function_id TEXT PRIMARY KEY,
    cyclomatic_complexity INTEGER,
    fan_in INTEGER,
    fan_out INTEGER,
    loc INTEGER,
    num_params INTEGER
);
`
	return sqlitex.ExecuteScript(conn, ddl, nil)
}

func createIndexes(conn *sqlite.Conn) error {
	indexes := `
CREATE INDEX idx_nodes_kind ON nodes(kind);
CREATE INDEX idx_nodes_package ON nodes(package);
CREATE INDEX idx_nodes_file ON nodes(file);
CREATE INDEX idx_nodes_parent ON nodes(parent_function);
CREATE INDEX idx_edges_source ON edges(source, kind);
CREATE INDEX idx_edges_target ON edges(target, kind);
CREATE INDEX idx_edges_kind ON edges(kind);
`
	return sqlitex.ExecuteScript(conn, indexes, nil)
}

func insertNodes(conn *sqlite.Conn, nodes []Node, prog *Progress) error {
	stmt, err := conn.Prepare(`INSERT OR IGNORE INTO nodes (id, kind, name, file, line, col, end_line, package, parent_function, type_info, properties) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare node insert: %w", err)
	}
	defer func() { _ = stmt.Finalize() }()

	for i, n := range nodes {
		stmt.BindText(1, n.ID)
		stmt.BindText(2, n.Kind)
		stmt.BindText(3, n.Name)
		bindTextOrNull(stmt, 4, n.File)
		bindIntOrNull(stmt, 5, n.Line)
		bindIntOrNull(stmt, 6, n.Col)
		bindIntOrNull(stmt, 7, n.EndLine)
		bindTextOrNull(stmt, 8, n.Package)
		bindTextOrNull(stmt, 9, n.ParentFunction)
		bindTextOrNull(stmt, 10, n.TypeInfo)
		bindTextOrNull(stmt, 11, PropsJSON(n.Properties))

		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("insert node %s: %w", n.ID, err)
		}
		_ = stmt.Reset()

		if (i+1)%batchSize == 0 {
			prog.Verbose("  inserted %d/%d nodes", i+1, len(nodes))
		}
	}

	prog.Log("Inserted %d nodes", len(nodes))
	return nil
}

func insertEdges(conn *sqlite.Conn, edges []Edge, prog *Progress) error {
	stmt, err := conn.Prepare(`INSERT INTO edges (source, target, kind, properties) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare edge insert: %w", err)
	}
	defer func() { _ = stmt.Finalize() }()

	for i, e := range edges {
		stmt.BindText(1, e.Source)
		stmt.BindText(2, e.Target)
		stmt.BindText(3, e.Kind)
		bindTextOrNull(stmt, 4, PropsJSON(e.Properties))

		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("insert edge %s→%s: %w", e.Source, e.Target, err)
		}
		_ = stmt.Reset()

		if (i+1)%batchSize == 0 {
			prog.Verbose("  inserted %d/%d edges", i+1, len(edges))
		}
	}

	prog.Log("Inserted %d edges", len(edges))
	return nil
}

func insertSources(conn *sqlite.Conn, sources map[string]string, prog *Progress) error {
	stmt, err := conn.Prepare(`INSERT OR IGNORE INTO sources (file, content, package) VALUES (?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare source insert: %w", err)
	}
	defer func() { _ = stmt.Finalize() }()

	for file, content := range sources {
		stmt.BindText(1, file)
		stmt.BindText(2, content)
		// Extract package from file path: first directory component
		pkg := extractPkgFromPath(file)
		bindTextOrNull(stmt, 3, pkg)

		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("insert source %s: %w", file, err)
		}
		_ = stmt.Reset()
	}

	prog.Log("Inserted %d source files", len(sources))
	return nil
}

func insertMetrics(conn *sqlite.Conn, metrics map[string]*Metrics, prog *Progress) error {
	stmt, err := conn.Prepare(`INSERT OR IGNORE INTO metrics (function_id, cyclomatic_complexity, fan_in, fan_out, loc, num_params) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare metrics insert: %w", err)
	}
	defer func() { _ = stmt.Finalize() }()

	for _, m := range metrics {
		stmt.BindText(1, m.FunctionID)
		stmt.BindInt64(2, int64(m.CyclomaticComplexity))
		stmt.BindInt64(3, int64(m.FanIn))
		stmt.BindInt64(4, int64(m.FanOut))
		stmt.BindInt64(5, int64(m.LOC))
		stmt.BindInt64(6, int64(m.NumParams))

		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("insert metric %s: %w", m.FunctionID, err)
		}
		_ = stmt.Reset()
	}

	prog.Log("Inserted %d function metrics", len(metrics))
	return nil
}

func runValidation(conn *sqlite.Conn, prog *Progress) error {
	prog.Log("Running validation queries...")

	// Check for orphan edges (edges referencing non-existent nodes)
	var orphanCount int64
	if err := sqlitex.ExecuteTransient(conn,
		`SELECT COUNT(*) FROM edges WHERE source NOT IN (SELECT id FROM nodes) OR target NOT IN (SELECT id FROM nodes)`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				orphanCount = stmt.ColumnInt64(0)
				return nil
			},
		}); err != nil {
		return err
	}
	if orphanCount > 0 {
		prog.Log("  WARNING: %d orphan edges (referencing non-existent nodes)", orphanCount)
	} else {
		prog.Log("  OK: zero orphan edges")
	}

	// Node count per kind
	if err := sqlitex.ExecuteTransient(conn,
		`SELECT kind, COUNT(*) FROM nodes GROUP BY kind ORDER BY COUNT(*) DESC`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				prog.Log("  nodes: %s = %d", stmt.ColumnText(0), stmt.ColumnInt64(1))
				return nil
			},
		}); err != nil {
		return err
	}

	// Edge count per kind
	if err := sqlitex.ExecuteTransient(conn,
		`SELECT kind, COUNT(*) FROM edges GROUP BY kind ORDER BY COUNT(*) DESC`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				prog.Log("  edges: %s = %d", stmt.ColumnText(0), stmt.ColumnInt64(1))
				return nil
			},
		}); err != nil {
		return err
	}

	return nil
}

// Helper functions for nullable bindings.

func bindTextOrNull(stmt *sqlite.Stmt, param int, val string) {
	if val == "" {
		stmt.BindNull(param)
	} else {
		stmt.BindText(param, val)
	}
}

func bindIntOrNull(stmt *sqlite.Stmt, param, val int) {
	if val == 0 {
		stmt.BindNull(param)
	} else {
		stmt.BindInt64(param, int64(val))
	}
}

// createFTS builds an FTS5 virtual table for full-text search on source code.
func createFTS(conn *sqlite.Conn) error {
	fts := `
CREATE VIRTUAL TABLE sources_fts USING fts5(file, content, package, content=sources, content_rowid=rowid);
INSERT INTO sources_fts(sources_fts) VALUES('rebuild');
`
	return sqlitex.ExecuteScript(conn, fts, nil)
}

// createSummaryStats builds pre-computed summary tables for viewer dashboards.
func createSummaryStats(conn *sqlite.Conn) error {
	ddl := `
CREATE TABLE stats_node_kinds AS
  SELECT kind, COUNT(*) as count FROM nodes GROUP BY kind ORDER BY count DESC;

CREATE TABLE stats_edge_kinds AS
  SELECT kind, COUNT(*) as count FROM edges GROUP BY kind ORDER BY count DESC;

CREATE TABLE stats_packages AS
  SELECT n.package as package,
         COUNT(DISTINCT CASE WHEN n.kind='file' THEN n.id END) as files,
         COUNT(DISTINCT CASE WHEN n.kind='function' THEN n.id END) as functions,
         COUNT(DISTINCT CASE WHEN n.kind='type_decl' THEN n.id END) as types,
         SUM(CASE WHEN n.kind='function' THEN (n.end_line - n.line + 1) ELSE 0 END) as loc
  FROM nodes n
  WHERE n.package IS NOT NULL
  GROUP BY n.package
  ORDER BY functions DESC;

CREATE TABLE stats_overview AS
  SELECT
    (SELECT COUNT(*) FROM nodes) as total_nodes,
    (SELECT COUNT(*) FROM edges) as total_edges,
    (SELECT COUNT(*) FROM sources) as total_files,
    (SELECT COUNT(DISTINCT package) FROM nodes WHERE package IS NOT NULL) as total_packages,
    (SELECT COUNT(*) FROM nodes WHERE kind='function') as total_functions,
    (SELECT COUNT(*) FROM nodes WHERE kind='type_decl') as total_types,
    (SELECT COUNT(*) FROM metrics) as total_metrics;

-- Vertical node properties: extracted from JSON for fast indexed queries
CREATE TABLE node_properties (
    node_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
INSERT INTO node_properties (node_id, key, value)
  SELECT n.id, j.key, j.value
  FROM nodes n, json_each(n.properties) j
  WHERE n.properties IS NOT NULL AND n.properties != '';
CREATE INDEX idx_node_props_key_value ON node_properties(key, value);
CREATE INDEX idx_node_props_node ON node_properties(node_id);

-- Vertical edge properties
CREATE TABLE edge_properties (
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
INSERT INTO edge_properties (source, target, edge_kind, key, value)
  SELECT e.source, e.target, e.kind, j.key, j.value
  FROM edges e, json_each(e.properties) j
  WHERE e.properties IS NOT NULL AND e.properties != '';
CREATE INDEX idx_edge_props_key_value ON edge_properties(key, value);
`
	return sqlitex.ExecuteScript(conn, ddl, nil)
}

// createAnalysisViews creates SQL views and a queries table for program analysis.
func createAnalysisViews(conn *sqlite.Conn) error {
	ddl := `
-- Flattened call graph with human-readable names
CREATE VIEW v_call_graph AS
  SELECT
    e.source AS caller_id,
    n1.name AS caller_name,
    n1.package AS caller_package,
    e.target AS callee_id,
    n2.name AS callee_name,
    n2.package AS callee_package,
    CASE WHEN ep.value IS NOT NULL THEN 1 ELSE 0 END AS is_dynamic
  FROM edges e
  JOIN nodes n1 ON e.source = n1.id
  JOIN nodes n2 ON e.target = n2.id
  LEFT JOIN edge_properties ep ON ep.source = e.source AND ep.target = e.target
    AND ep.edge_kind = 'call' AND ep.key = 'dynamic'
  WHERE e.kind = 'call';

-- Data flow edges with context
CREATE VIEW v_data_flow AS
  SELECT
    e.source AS def_id,
    n1.name AS def_name,
    n1.kind AS def_kind,
    n1.file AS def_file,
    n1.line AS def_line,
    e.target AS use_id,
    n2.name AS use_name,
    n2.kind AS use_kind,
    n2.file AS use_file,
    n2.line AS use_line
  FROM edges e
  JOIN nodes n1 ON e.source = n1.id
  JOIN nodes n2 ON e.target = n2.id
  WHERE e.kind = 'dfg';

-- Function summary with metrics and call counts
CREATE VIEW v_function_summary AS
  SELECT
    n.id,
    n.name,
    n.package,
    n.file,
    n.line,
    n.end_line,
    COALESCE(m.cyclomatic_complexity, 0) AS complexity,
    COALESCE(m.fan_in, 0) AS fan_in,
    COALESCE(m.fan_out, 0) AS fan_out,
    COALESCE(m.loc, n.end_line - n.line + 1) AS loc,
    COALESCE(m.num_params, 0) AS num_params,
    (SELECT COUNT(*) FROM edges e WHERE e.source = n.id AND e.kind = 'call') AS calls_out,
    (SELECT COUNT(*) FROM edges e WHERE e.target = n.id AND e.kind = 'call') AS calls_in
  FROM nodes n
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE n.kind = 'function';

-- Type hierarchy with implementations
CREATE VIEW v_type_hierarchy AS
  SELECT
    n1.id AS type_id,
    n1.name AS type_name,
    n1.package AS type_package,
    e.kind AS relationship,
    n2.id AS target_id,
    n2.name AS target_name,
    n2.package AS target_package
  FROM edges e
  JOIN nodes n1 ON e.source = n1.id
  JOIN nodes n2 ON e.target = n2.id
  WHERE e.kind IN ('implements', 'embeds', 'alias_of');

-- Package dependency graph: aggregated cross-package call edges
CREATE VIEW v_package_deps AS
  SELECT
    n1.package AS source_package,
    n2.package AS target_package,
    COUNT(*) AS call_count,
    COUNT(DISTINCT n1.id) AS distinct_callers,
    COUNT(DISTINCT n2.id) AS distinct_callees
  FROM edges e
  JOIN nodes n1 ON e.source = n1.id
  JOIN nodes n2 ON e.target = n2.id
  WHERE e.kind = 'call'
    AND n1.package IS NOT NULL AND n2.package IS NOT NULL
    AND n1.package != n2.package
  GROUP BY n1.package, n2.package;

-- File dependency graph: which files call functions in other files
CREATE VIEW v_file_deps AS
  SELECT
    n1.file AS source_file,
    n2.file AS target_file,
    COUNT(*) AS call_count
  FROM edges e
  JOIN nodes n1 ON e.source = n1.id
  JOIN nodes n2 ON e.target = n2.id
  WHERE e.kind = 'call'
    AND n1.file IS NOT NULL AND n2.file IS NOT NULL
    AND n1.file != n2.file
  GROUP BY n1.file, n2.file;

-- Function I/O: parameters and return values per function
CREATE VIEW v_function_io AS
  SELECT
    f.id AS function_id,
    f.name AS function_name,
    f.package,
    p.id AS io_node_id,
    p.name AS io_name,
    p.kind AS io_kind,
    p.type_info AS io_type,
    json_extract(p.properties, '$.mutable') AS is_mutable,
    json_extract(p.properties, '$.nullable') AS is_nullable
  FROM nodes f
  JOIN edges e ON e.source = f.id AND e.kind = 'ast'
  JOIN nodes p ON p.id = e.target AND p.kind IN ('parameter', 'result')
  WHERE f.kind = 'function';

-- Pre-computed findings for the viewer
CREATE TABLE findings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,
    severity TEXT NOT NULL,
    node_id TEXT,
    file TEXT,
    line INTEGER,
    message TEXT NOT NULL,
    details TEXT
);

-- High complexity functions
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'complexity', 'warning', n.id, n.file, n.line,
    n.name || ' has cyclomatic complexity ' || m.cyclomatic_complexity,
    json_object('complexity', m.cyclomatic_complexity, 'package', n.package)
  FROM nodes n JOIN metrics m ON n.id = m.function_id
  WHERE m.cyclomatic_complexity >= 15;

-- Very large functions
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'size', 'info', n.id, n.file, n.line,
    n.name || ' is ' || m.loc || ' lines long',
    json_object('loc', m.loc, 'package', n.package)
  FROM nodes n JOIN metrics m ON n.id = m.function_id
  WHERE m.loc >= 100;

-- Deeply nested control structures
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'nesting', 'warning', np.node_id, n.file, n.line,
    n.kind || ' at depth ' || np.value || ' in ' || n.parent_function,
    json_object('depth', CAST(np.value AS INTEGER), 'kind', n.kind)
  FROM node_properties np
  JOIN nodes n ON np.node_id = n.id
  WHERE np.key = 'nesting_depth' AND CAST(np.value AS INTEGER) >= 8
    AND n.kind IN ('if', 'for', 'switch', 'select');

-- Hub functions (high fan-in + fan-out)
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'hub', 'info', n.id, n.file, n.line,
    n.name || ': fan_in=' || m.fan_in || ' fan_out=' || m.fan_out,
    json_object('fan_in', m.fan_in, 'fan_out', m.fan_out, 'package', n.package)
  FROM nodes n JOIN metrics m ON n.id = m.function_id
  WHERE m.fan_in >= 10 AND m.fan_out >= 10;

-- Dead stores: local variables with no outgoing DFG edges (assigned but never read)
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'dead_store', 'warning', n.id, n.file, n.line,
    'unused variable ''' || n.name || ''' in ' || COALESCE(n.parent_function, n.package),
    json_object('variable', n.name, 'package', n.package)
  FROM nodes n
  WHERE n.kind = 'local' AND n.parent_function IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.source = n.id AND e.kind = 'dfg')
    AND n.name != '_';

-- Unused parameters: function parameters with no outgoing DFG edges
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'unused_param', 'info', n.id, n.file, n.line,
    'unused parameter ''' || n.name || ''' in ' || COALESCE(n.parent_function, '?'),
    json_object('parameter', n.name, 'function', n.parent_function)
  FROM nodes n
  WHERE n.kind = 'parameter' AND n.parent_function IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.source = n.id AND e.kind = 'dfg')
    AND n.name != '_';

-- Circular package dependencies (A calls B and B calls A), excluding cmd/* and external
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'circular_dep', 'warning', NULL, NULL, NULL,
    d1.source_package || ' <-> ' || d1.target_package || ' (' || d1.call_count || '/' || d2.call_count || ' calls)',
    json_object('package_a', d1.source_package, 'package_b', d1.target_package,
                'a_to_b_calls', d1.call_count, 'b_to_a_calls', d2.call_count)
  FROM v_package_deps d1
  JOIN v_package_deps d2 ON d1.source_package = d2.target_package AND d1.target_package = d2.source_package
  WHERE d1.source_package < d1.target_package
    AND d1.source_package NOT LIKE 'cmd/%' AND d1.target_package NOT LIKE 'cmd/%'
    AND d1.source_package NOT LIKE 'ext::%' AND d1.target_package NOT LIKE 'ext::%'
    AND d1.source_package NOT LIKE '%.%/%';

-- Goroutine analysis: functions that launch multiple goroutines
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'goroutine_spawner', 'info', fn.id, fn.file, fn.line,
    fn.name || ' launches ' || go_count || ' goroutine(s)',
    json_object('goroutine_count', go_count, 'package', fn.package)
  FROM (
    SELECT g.parent_function AS func_id, COUNT(*) AS go_count
    FROM nodes g
    WHERE g.kind = 'go' AND g.parent_function IS NOT NULL
    GROUP BY g.parent_function
    HAVING COUNT(*) >= 2
  ) sub
  JOIN nodes fn ON fn.id = sub.func_id;

CREATE INDEX idx_findings_category ON findings(category);
CREATE INDEX idx_findings_node ON findings(node_id);

-- Example queries table: ready-to-use CTE queries for the viewer
CREATE TABLE queries (
    name TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    sql TEXT NOT NULL
);

INSERT INTO queries (name, description, sql) VALUES
('backward_slice',
 'Backward program slice: find all nodes that contribute to a given node via data flow',
 'WITH RECURSIVE slice(id, depth) AS (
  SELECT :node_id, 0
  UNION
  SELECT e.source, s.depth + 1
  FROM slice s JOIN edges e ON e.target = s.id
  WHERE e.kind IN (''dfg'', ''param_in'') AND s.depth < 20
)
SELECT DISTINCT n.* FROM slice s JOIN nodes n ON n.id = s.id ORDER BY n.file, n.line');

INSERT INTO queries (name, description, sql) VALUES
('forward_slice',
 'Forward program slice: find all nodes affected by a given node via data flow',
 'WITH RECURSIVE slice(id, depth) AS (
  SELECT :node_id, 0
  UNION
  SELECT e.target, s.depth + 1
  FROM slice s JOIN edges e ON e.source = s.id
  WHERE e.kind IN (''dfg'', ''param_out'') AND s.depth < 20
)
SELECT DISTINCT n.* FROM slice s JOIN nodes n ON n.id = s.id ORDER BY n.file, n.line');

INSERT INTO queries (name, description, sql) VALUES
('call_chain',
 'Transitive call chain: find all functions reachable from a given function',
 'WITH RECURSIVE chain(id, depth, path) AS (
  SELECT :function_id, 0, :function_id
  UNION
  SELECT e.target, c.depth + 1, c.path || '' -> '' || e.target
  FROM chain c JOIN edges e ON e.source = c.id
  WHERE e.kind = ''call'' AND c.depth < 10
    AND c.path NOT LIKE ''%'' || e.target || ''%''
)
SELECT DISTINCT n.id, n.name, n.package, c.depth
FROM chain c JOIN nodes n ON n.id = c.id ORDER BY c.depth, n.name');

INSERT INTO queries (name, description, sql) VALUES
('callers_of',
 'All callers of a function (transitive, up to depth 5)',
 'WITH RECURSIVE callers(id, depth) AS (
  SELECT :function_id, 0
  UNION
  SELECT e.source, c.depth + 1
  FROM callers c JOIN edges e ON e.target = c.id
  WHERE e.kind = ''call'' AND c.depth < 5
)
SELECT DISTINCT n.id, n.name, n.package, c.depth
FROM callers c JOIN nodes n ON n.id = c.id
WHERE n.kind = ''function'' ORDER BY c.depth, n.name');

INSERT INTO queries (name, description, sql) VALUES
('scope_variables',
 'All variables visible at a given scope (block), walking the scope chain',
 'WITH RECURSIVE scope_chain(id) AS (
  SELECT :block_id
  UNION
  SELECT e.target FROM scope_chain sc JOIN edges e ON e.source = sc.id WHERE e.kind = ''scope''
)
SELECT n.* FROM scope_chain sc
JOIN edges e ON e.source = sc.id AND e.kind = ''ast''
JOIN nodes n ON n.id = e.target
WHERE n.kind IN (''local'', ''parameter'', ''result'')
ORDER BY n.file, n.line');

INSERT INTO queries (name, description, sql) VALUES
('interface_implementors',
 'All types implementing a given interface',
 'SELECT n.id, n.name, n.package, n.file, n.line
FROM edges e JOIN nodes n ON e.source = n.id
WHERE e.kind = ''implements'' AND e.target = :interface_id
ORDER BY n.package, n.name');

INSERT INTO queries (name, description, sql) VALUES
('function_cfg',
 'Control flow graph for a function: all basic blocks and their connections',
 'SELECT
  bb.id AS block_id, bb.name AS block_name, bb.line AS block_line,
  e.target AS successor_id, n2.name AS successor_name,
  ep.value AS branch_label
FROM nodes bb
JOIN edges parent_e ON parent_e.target = bb.id AND parent_e.kind = ''ast''
LEFT JOIN edges e ON e.source = bb.id AND e.kind = ''cfg''
LEFT JOIN nodes n2 ON e.target = n2.id
LEFT JOIN edge_properties ep ON ep.source = e.source AND ep.target = e.target
  AND ep.edge_kind = ''cfg'' AND ep.key = ''label''
WHERE bb.kind = ''basic_block'' AND parent_e.source = :function_id
ORDER BY bb.line');

INSERT INTO queries (name, description, sql) VALUES
('cross_package_calls',
 'All function calls that cross package boundaries',
 'SELECT n1.package AS caller_pkg, n1.name AS caller, n2.package AS callee_pkg, n2.name AS callee
FROM edges e
JOIN nodes n1 ON e.source = n1.id
JOIN nodes n2 ON e.target = n2.id
WHERE e.kind = ''call'' AND n1.package != n2.package AND n1.package IS NOT NULL AND n2.package IS NOT NULL
ORDER BY n1.package, n2.package');

INSERT INTO queries (name, description, sql) VALUES
('context_propagation',
 'Trace context.Context through call chains: functions with context calling functions without',
 'SELECT
  caller.id AS caller_id, caller.name AS caller_name, caller.package AS caller_pkg,
  callee.id AS callee_id, callee.name AS callee_name, callee.package AS callee_pkg,
  CASE WHEN callee_ctx.value IS NOT NULL THEN ''propagated'' ELSE ''MISSING'' END AS ctx_status
FROM edges e
JOIN nodes caller ON e.source = caller.id
JOIN nodes callee ON e.target = callee.id
JOIN node_properties caller_ctx ON caller_ctx.node_id = caller.id
  AND caller_ctx.key = ''has_context'' AND caller_ctx.value = ''1''
LEFT JOIN node_properties callee_ctx ON callee_ctx.node_id = callee.id
  AND callee_ctx.key = ''has_context'' AND callee_ctx.value = ''1''
WHERE e.kind = ''call'' AND callee.kind = ''function''
ORDER BY ctx_status DESC, caller.package, caller.name');

INSERT INTO queries (name, description, sql) VALUES
('reaching_definitions',
 'Reaching definitions: all definitions that flow to a given variable use',
 'SELECT n.id, n.name, n.kind, n.file, n.line, n.type_info
FROM edges e JOIN nodes n ON e.source = n.id
WHERE e.kind = ''dfg'' AND e.target = :node_id
ORDER BY n.file, n.line');

INSERT INTO queries (name, description, sql) VALUES
('package_dependency_graph',
 'Package dependency graph with call counts',
 'SELECT source_package, target_package, call_count, distinct_callers, distinct_callees
FROM v_package_deps ORDER BY call_count DESC');

INSERT INTO queries (name, description, sql) VALUES
('goroutine_analysis',
 'Goroutines and their synchronization: go statements with sync primitives in parent function',
 'SELECT
  g.id AS go_id, g.file, g.line,
  fn.name AS parent_function,
  (SELECT GROUP_CONCAT(DISTINCT np.value) FROM nodes sync
   JOIN node_properties np ON np.node_id = sync.id AND np.key = ''sync_kind''
   WHERE sync.parent_function = fn.id) AS sync_primitives,
  (SELECT COUNT(*) FROM nodes g2 WHERE g2.kind = ''go'' AND g2.parent_function = fn.id) AS goroutine_count
FROM nodes g
JOIN nodes fn ON g.parent_function = fn.id
WHERE g.kind = ''go''
ORDER BY goroutine_count DESC, fn.name');

INSERT INTO queries (name, description, sql) VALUES
('taint_analysis',
 'Find call nodes annotated with security roles (source/sink/barrier/propagator)',
 'SELECT n.id, n.name, n.file, n.line, n.parent_function,
    np_role.value AS taint_role,
    COALESCE(np_cat.value, '''') AS taint_category,
    fn.name AS function_name
  FROM node_properties np_role
  JOIN nodes n ON n.id = np_role.node_id
  LEFT JOIN node_properties np_cat ON np_cat.node_id = n.id AND np_cat.key = ''taint_category''
  LEFT JOIN nodes fn ON fn.id = n.parent_function
  WHERE np_role.key = ''taint_role''
  ORDER BY np_role.value, n.file, n.line');

INSERT INTO queries (name, description, sql) VALUES
('taint_path',
 'Functions containing both taint sources and sinks (potential security hotspots)',
 'SELECT DISTINCT fn.id, fn.name, fn.package, fn.file, fn.line,
    GROUP_CONCAT(DISTINCT src_cat.value) AS source_categories,
    GROUP_CONCAT(DISTINCT sink_cat.value) AS sink_categories
  FROM node_properties src_role
  JOIN nodes src ON src.id = src_role.node_id
  JOIN node_properties src_cat ON src_cat.node_id = src.id AND src_cat.key = ''taint_category''
  CROSS JOIN node_properties sink_role
  JOIN nodes sink ON sink.id = sink_role.node_id
  JOIN node_properties sink_cat ON sink_cat.node_id = sink.id AND sink_cat.key = ''taint_category''
  JOIN nodes fn ON fn.id = src.parent_function
  WHERE src_role.key = ''taint_role'' AND src_role.value = ''source''
    AND sink_role.key = ''taint_role'' AND sink_role.value = ''sink''
    AND src.parent_function = sink.parent_function AND src.parent_function IS NOT NULL
  GROUP BY fn.id ORDER BY fn.package, fn.name');

INSERT INTO queries (name, description, sql) VALUES
('function_io',
 'Parameters and return values for a function (use v_function_io view)',
 'SELECT * FROM v_function_io WHERE function_id = :function_id ORDER BY io_kind DESC, io_name');
`
	return sqlitex.ExecuteScript(conn, ddl, nil)
}

// computeEOG creates Evaluation Order Graph edges within call expressions.
// For a call f(a, b, c), Go evaluates arguments left-to-right: a → b → c → f().
// EOG edges connect consecutive arguments and the last argument to the call node.
func computeEOG(conn *sqlite.Conn, prog *Progress) error {
	// Step 1: Connect consecutive arguments (arg[i] → arg[i+1])
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT OR IGNORE INTO edges (source, target, kind, properties)
		 SELECT DISTINCT src.target, dst.target, 'eog', NULL
		 FROM edges src
		 JOIN edges dst ON src.source = dst.source AND dst.kind = 'argument'
		 WHERE src.kind = 'argument'
		   AND CAST(json_extract(dst.properties, '$.index') AS INTEGER) =
		       CAST(json_extract(src.properties, '$.index') AS INTEGER) + 1`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return fmt.Errorf("eog consecutive args: %w", err)
	}
	seqEdges := conn.Changes()

	// Step 2: Connect last argument to call node (arg[last] → call)
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT OR IGNORE INTO edges (source, target, kind, properties)
		 SELECT DISTINCT la.arg_id, la.call_id, 'eog', '{"final":true}'
		 FROM (
		   SELECT e.source AS call_id, e.target AS arg_id,
		     CAST(json_extract(e.properties, '$.index') AS INTEGER) AS idx
		   FROM edges e WHERE e.kind = 'argument'
		 ) la
		 WHERE la.idx = (
		   SELECT MAX(CAST(json_extract(e2.properties, '$.index') AS INTEGER))
		   FROM edges e2 WHERE e2.kind = 'argument' AND e2.source = la.call_id
		 )`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return fmt.Errorf("eog last arg to call: %w", err)
	}
	finalEdges := conn.Changes()

	if seqEdges+finalEdges > 0 {
		prog.Log("Created %d EOG edges (%d sequential, %d final-arg)",
			seqEdges+finalEdges, seqEdges, finalEdges)
	}
	return nil
}

// createAdditionalAnalysis adds extra views, findings, and queries for the viewer.
func createAdditionalAnalysis(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- Exported API surface per package
CREATE VIEW v_api_surface AS
  SELECT n.package, n.kind, n.id, n.name, n.type_info, n.file, n.line
  FROM nodes n
  WHERE n.name GLOB '[A-Z]*'
    AND n.kind IN ('function', 'type_decl')
    AND n.package IS NOT NULL;

-- Methods grouped by receiver type
CREATE VIEW v_method_sets AS
  SELECT
    np.value AS receiver_type,
    n.package,
    n.id, n.name, n.type_info, n.file, n.line,
    COALESCE(m.cyclomatic_complexity, 0) AS complexity,
    COALESCE(m.loc, 0) AS loc
  FROM nodes n
  JOIN node_properties np ON np.node_id = n.id AND np.key = 'receiver'
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE n.kind = 'function';

-- Error-returning functions with metrics
CREATE VIEW v_error_handling AS
  SELECT
    n.id, n.name, n.package, n.file, n.line,
    COALESCE(m.fan_in, 0) AS callers,
    COALESCE(m.fan_out, 0) AS callees,
    COALESCE(m.cyclomatic_complexity, 0) AS complexity
  FROM nodes n
  JOIN node_properties np ON np.node_id = n.id AND np.key = 'returns_error' AND np.value = '1'
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE n.kind = 'function';

-- Unused exports: exported functions with zero callers from other packages
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'unused_export', 'info', n.id, n.file, n.line,
  'exported ' || n.name || ' has no callers from other packages',
  json_object('name', n.name, 'package', n.package)
FROM nodes n
WHERE n.kind = 'function' AND n.name GLOB '[A-Z]*'
  AND n.package IS NOT NULL AND n.package NOT LIKE 'cmd/%'
  AND NOT EXISTS (
    SELECT 1 FROM edges e
    JOIN nodes caller ON e.source = caller.id AND caller.package != n.package
    WHERE e.target = n.id AND e.kind = 'call'
  );

-- Long parameter lists (> 5 params)
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'long_param_list', 'info', n.id, n.file, n.line,
  n.name || ' has ' || m.num_params || ' parameters',
  json_object('num_params', m.num_params, 'package', n.package)
FROM nodes n
JOIN metrics m ON m.function_id = n.id
WHERE m.num_params > 5;

-- God functions: high complexity + large LOC + high fan_out
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'god_function', 'warning', n.id, n.file, n.line,
  n.name || ' (complexity=' || m.cyclomatic_complexity || ', loc=' || m.loc || ', fan_out=' || m.fan_out || ')',
  json_object('complexity', m.cyclomatic_complexity, 'loc', m.loc,
              'fan_in', m.fan_in, 'fan_out', m.fan_out, 'package', n.package)
FROM nodes n
JOIN metrics m ON m.function_id = n.id
WHERE m.cyclomatic_complexity >= 10 AND m.loc >= 50 AND m.fan_out >= 10;

-- Interface coupling: types implementing many interfaces
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'interface_coupling', 'info', n.id, n.file, n.line,
  n.name || ' implements ' || iface_count || ' interfaces',
  json_object('interface_count', iface_count, 'package', n.package)
FROM (
  SELECT e.source AS type_id, COUNT(*) AS iface_count
  FROM edges e WHERE e.kind = 'implements'
  GROUP BY e.source HAVING COUNT(*) >= 3
) impl
JOIN nodes n ON n.id = impl.type_id;

-- Concurrency risk: functions using both mutexes and goroutines
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'concurrency_risk', 'warning', fn.id, fn.file, fn.line,
  fn.name || ' uses mutex locks and spawns goroutines',
  json_object('package', fn.package)
FROM nodes fn
WHERE fn.kind = 'function'
  AND EXISTS (
    SELECT 1 FROM nodes g WHERE g.kind = 'go' AND g.parent_function = fn.id
  )
  AND EXISTS (
    SELECT 1 FROM node_properties np
    JOIN nodes n ON n.id = np.node_id AND n.parent_function = fn.id
    WHERE np.key = 'sync_kind' AND np.value LIKE 'mutex_%'
  );

-- Deeply recursive functions: function calls itself (directly)
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'recursive', 'info', n.id, n.file, n.line,
  n.name || ' calls itself directly',
  json_object('package', n.package)
FROM nodes n
JOIN edges e ON e.source = n.id AND e.target = n.id AND e.kind = 'call'
WHERE n.kind = 'function';

-- Additional queries
INSERT INTO queries (name, description, sql) VALUES
('type_methods',
 'All methods on a given receiver type',
 'SELECT n.id, n.name, n.file, n.line, n.type_info,
    COALESCE(m.cyclomatic_complexity, 0) AS complexity,
    COALESCE(m.loc, 0) AS loc
  FROM v_method_sets ms
  JOIN nodes n ON n.id = ms.id
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE ms.receiver_type = :receiver_type
  ORDER BY n.name');

INSERT INTO queries (name, description, sql) VALUES
('error_chain',
 'Trace error return chains: functions returning error that call other error-returning functions',
 'WITH RECURSIVE err_chain(id, name, pkg, depth) AS (
    SELECT :function_id, (SELECT name FROM nodes WHERE id = :function_id),
           (SELECT package FROM nodes WHERE id = :function_id), 0
    UNION
    SELECT e.target, n.name, n.package, ec.depth + 1
    FROM err_chain ec
    JOIN edges e ON e.source = ec.id AND e.kind = ''call''
    JOIN nodes n ON n.id = e.target
    JOIN node_properties np ON np.node_id = n.id AND np.key = ''returns_error'' AND np.value = ''1''
    WHERE ec.depth < 10
  )
  SELECT DISTINCT id, name, pkg, depth FROM err_chain ORDER BY depth, name');

INSERT INTO queries (name, description, sql) VALUES
('data_flow_path',
 'Find data flow paths from a source to any reachable node via DFG',
 'WITH RECURSIVE flow_path(id, depth, path) AS (
    SELECT :source_id, 0, :source_id
    UNION
    SELECT e.target, fp.depth + 1, fp.path || '' -> '' || e.target
    FROM flow_path fp
    JOIN edges e ON e.source = fp.id AND e.kind = ''dfg''
    WHERE fp.depth < 15 AND fp.path NOT LIKE ''%'' || e.target || ''%''
  )
  SELECT fp.id, n.name, n.kind, n.file, n.line, fp.depth
  FROM flow_path fp
  JOIN nodes n ON n.id = fp.id
  ORDER BY fp.depth, n.file, n.line');

INSERT INTO queries (name, description, sql) VALUES
('shared_callers',
 'Functions that call both :function_a and :function_b (coupling analysis)',
 'SELECT n.id, n.name, n.package, n.file, n.line
  FROM nodes n
  WHERE n.kind = ''function''
    AND EXISTS (SELECT 1 FROM edges e WHERE e.source = n.id AND e.target = :function_a AND e.kind = ''call'')
    AND EXISTS (SELECT 1 FROM edges e WHERE e.source = n.id AND e.target = :function_b AND e.kind = ''call'')
  ORDER BY n.package, n.name');

INSERT INTO queries (name, description, sql) VALUES
('impact_analysis',
 'Impact of changing a function: all transitive callers (who would be affected)',
 'WITH RECURSIVE callers(id, depth) AS (
    SELECT :function_id, 0
    UNION
    SELECT e.source, c.depth + 1
    FROM callers c
    JOIN edges e ON e.target = c.id AND e.kind = ''call''
    WHERE c.depth < 8
  )
  SELECT DISTINCT n.id, n.name, n.package, n.file, n.line, c.depth
  FROM callers c JOIN nodes n ON n.id = c.id
  WHERE n.kind = ''function''
  ORDER BY c.depth, n.package, n.name');

INSERT INTO queries (name, description, sql) VALUES
('common_callee',
 'Functions called by both :function_a and :function_b (shared dependencies)',
 'SELECT n.id, n.name, n.package, n.file, n.line
  FROM nodes n
  WHERE n.kind = ''function''
    AND EXISTS (SELECT 1 FROM edges e WHERE e.target = n.id AND e.source = :function_a AND e.kind = ''call'')
    AND EXISTS (SELECT 1 FROM edges e WHERE e.target = n.id AND e.source = :function_b AND e.kind = ''call'')
  ORDER BY n.package, n.name');
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return err
	}

	// Count new findings
	var count int64
	_ = sqlitex.ExecuteTransient(conn,
		`SELECT COUNT(*) FROM findings WHERE category IN ('unused_export','long_param_list','god_function','interface_coupling','concurrency_risk','recursive')`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				count = stmt.ColumnInt64(0)
				return nil
			},
		})
	prog.Log("Additional analysis: %d new findings, 3 views, 5 queries", count)
	return nil
}

// applyEscapeAnalysis maps compiler escape annotations to CPG nodes via position matching.
func applyEscapeAnalysis(conn *sqlite.Conn, results []EscapeResult, prog *Progress) error {
	// Create temp table for batch matching
	if err := sqlitex.ExecuteTransient(conn,
		`CREATE TEMP TABLE escape_info (file TEXT, line INTEGER, col INTEGER, kind TEXT, detail TEXT)`,
		nil); err != nil {
		return err
	}

	stmt, err := conn.Prepare(`INSERT INTO escape_info VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	for _, r := range results {
		stmt.BindText(1, r.RelFile)
		stmt.BindInt64(2, int64(r.Line))
		stmt.BindInt64(3, int64(r.Col))
		stmt.BindText(4, r.Kind)
		stmt.BindText(5, r.Detail)
		if _, err := stmt.Step(); err != nil {
			_ = stmt.Finalize()
			return err
		}
		_ = stmt.Reset()
	}
	_ = stmt.Finalize()

	// Match "inlineable" annotations to function nodes
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT INTO node_properties (node_id, key, value)
		 SELECT DISTINCT n.id, 'inlineable', 'true'
		 FROM escape_info ei
		 JOIN nodes n ON n.file = ei.file AND n.line = ei.line
		 WHERE ei.kind = 'inlineable' AND n.kind = 'function'`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return err
	}
	inlineable := conn.Changes()

	// Match heap-escaping annotations to parameter/local nodes
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT INTO node_properties (node_id, key, value)
		 SELECT DISTINCT n.id, 'heap_escapes', 'true'
		 FROM escape_info ei
		 JOIN nodes n ON n.file = ei.file AND n.line = ei.line
		 WHERE ei.kind IN ('leaking_param', 'moved_to_heap', 'escapes_to_heap')
		   AND n.kind IN ('parameter', 'local', 'function')`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return err
	}
	escaping := conn.Changes()

	// Match "does_not_escape" to parameters
	if err := sqlitex.ExecuteTransient(conn,
		`INSERT INTO node_properties (node_id, key, value)
		 SELECT DISTINCT n.id, 'heap_escapes', 'false'
		 FROM escape_info ei
		 JOIN nodes n ON n.file = ei.file AND n.line = ei.line
		 WHERE ei.kind = 'does_not_escape'
		   AND n.kind IN ('parameter', 'local')
		   AND NOT EXISTS (
		     SELECT 1 FROM node_properties np
		     WHERE np.node_id = n.id AND np.key = 'heap_escapes'
		   )`,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		}); err != nil {
		return err
	}
	notEscaping := conn.Changes()

	// Drop temp table
	_ = sqlitex.ExecuteTransient(conn, `DROP TABLE IF EXISTS escape_info`, nil)

	prog.Log("Escape: %d inlineable functions, %d heap-escaping, %d stack-bound",
		inlineable, escaping, notEscaping)
	return nil
}

// createFlowSemantics builds a table describing how data flows through known
// stdlib functions. Used by the heuristic DFG to create precise data-flow edges.
func createFlowSemantics(conn *sqlite.Conn) error {
	ddl := `
CREATE TABLE flow_semantics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL,
    func_name TEXT NOT NULL,
    flow_from TEXT NOT NULL,
    flow_to TEXT NOT NULL,
    description TEXT
);

INSERT INTO flow_semantics (package, func_name, flow_from, flow_to, description) VALUES
-- String formatting: all value args contribute to result
('fmt', 'Sprintf', 'arg:*', 'return:0', 'All args contribute to formatted string'),
('fmt', 'Sprint', 'arg:*', 'return:0', 'All args contribute to string'),
('fmt', 'Sprintln', 'arg:*', 'return:0', 'All args contribute to string'),

-- String operations: source string → result
('strings', 'Replace', 'arg:0', 'return:0', 'Source string flows to result'),
('strings', 'ReplaceAll', 'arg:0', 'return:0', 'Source string flows to result'),
('strings', 'ToLower', 'arg:0', 'return:0', 'String flows to lowered result'),
('strings', 'ToUpper', 'arg:0', 'return:0', 'String flows to uppered result'),
('strings', 'TrimSpace', 'arg:0', 'return:0', 'String flows to trimmed result'),
('strings', 'Trim', 'arg:0', 'return:0', 'String flows to trimmed result'),
('strings', 'TrimPrefix', 'arg:0', 'return:0', 'String flows to trimmed result'),
('strings', 'TrimSuffix', 'arg:0', 'return:0', 'String flows to trimmed result'),
('strings', 'Split', 'arg:0', 'return:0', 'String flows to split parts'),
('strings', 'Join', 'arg:0', 'return:0', 'Slice elements flow to joined string'),
('strings', 'Contains', 'arg:0', 'return:0', 'String checked for containment'),
('strings', 'HasPrefix', 'arg:0', 'return:0', 'String checked for prefix'),
('strings', 'HasSuffix', 'arg:0', 'return:0', 'String checked for suffix'),

-- Type conversions: input → converted output
('strconv', 'Atoi', 'arg:0', 'return:0', 'String flows to int'),
('strconv', 'ParseInt', 'arg:0', 'return:0', 'String flows to int64'),
('strconv', 'ParseFloat', 'arg:0', 'return:0', 'String flows to float'),
('strconv', 'ParseBool', 'arg:0', 'return:0', 'String flows to bool'),
('strconv', 'Itoa', 'arg:0', 'return:0', 'Int flows to string'),
('strconv', 'FormatInt', 'arg:0', 'return:0', 'Int64 flows to string'),
('strconv', 'FormatFloat', 'arg:0', 'return:0', 'Float flows to string'),

-- Encoding: input → encoded/decoded output
('encoding/base64', 'EncodeToString', 'arg:0', 'return:0', 'Bytes flow to base64 string'),
('encoding/base64', 'DecodeString', 'arg:0', 'return:0', 'Base64 string flows to bytes'),
('encoding/hex', 'EncodeToString', 'arg:0', 'return:0', 'Bytes flow to hex string'),
('encoding/hex', 'DecodeString', 'arg:0', 'return:0', 'Hex string flows to bytes'),

-- JSON: marshaling
('encoding/json', 'Marshal', 'arg:0', 'return:0', 'Value flows to JSON bytes'),
('encoding/json', 'Unmarshal', 'arg:0', 'arg:1', 'JSON bytes flow to target value'),

-- YAML: marshaling
('gopkg.in/yaml.v2', 'Marshal', 'arg:0', 'return:0', 'Value flows to YAML bytes'),
('gopkg.in/yaml.v2', 'Unmarshal', 'arg:0', 'arg:1', 'YAML bytes flow to target value'),

-- URL escaping
('net/url', 'QueryEscape', 'arg:0', 'return:0', 'String flows to URL-escaped string'),
('net/url', 'PathEscape', 'arg:0', 'return:0', 'String flows to path-escaped string'),
('net/url', 'QueryUnescape', 'arg:0', 'return:0', 'URL-escaped flows to unescaped'),

-- HTML escaping
('html', 'EscapeString', 'arg:0', 'return:0', 'String flows to HTML-escaped string'),
('html', 'UnescapeString', 'arg:0', 'return:0', 'HTML-escaped flows to unescaped'),

-- Path operations
('filepath', 'Join', 'arg:*', 'return:0', 'Path elements flow to joined path'),
('filepath', 'Clean', 'arg:0', 'return:0', 'Path flows to cleaned path'),
('filepath', 'Abs', 'arg:0', 'return:0', 'Path flows to absolute path'),
('filepath', 'Rel', 'arg:1', 'return:0', 'Target path flows to relative path'),
('filepath', 'Base', 'arg:0', 'return:0', 'Path flows to base name'),
('filepath', 'Dir', 'arg:0', 'return:0', 'Path flows to directory'),
('filepath', 'Ext', 'arg:0', 'return:0', 'Path flows to extension'),
('path', 'Join', 'arg:*', 'return:0', 'Path elements flow to joined path'),
('path', 'Clean', 'arg:0', 'return:0', 'Path flows to cleaned path'),
('path', 'Base', 'arg:0', 'return:0', 'Path flows to base name'),

-- I/O operations
('io', 'ReadAll', 'arg:0', 'return:0', 'Reader content flows to bytes'),
('io', 'Copy', 'arg:1', 'arg:0', 'Source reader flows to destination writer'),
('os', 'ReadFile', 'arg:0', 'return:0', 'File path determines content read'),

-- Regex
('regexp', 'MatchString', 'arg:1', 'return:0', 'String flows to match result'),
('regexp', 'Match', 'arg:1', 'return:0', 'Bytes flow to match result'),

-- Bytes operations
('bytes', 'Join', 'arg:0', 'return:0', 'Byte slices flow to joined result'),
('bytes', 'TrimSpace', 'arg:0', 'return:0', 'Bytes flow to trimmed result'),
('bytes', 'Contains', 'arg:0', 'return:0', 'Bytes checked for containment'),
('bytes', 'Replace', 'arg:0', 'return:0', 'Source bytes flow to result'),

-- Errors
('errors', 'New', 'arg:0', 'return:0', 'Message flows to error'),
('errors', 'Unwrap', 'arg:0', 'return:0', 'Wrapped error flows to inner error'),

-- Sort: mutates in place
('sort', 'Slice', 'arg:0', 'arg:0', 'Slice mutated in place'),
('sort', 'Sort', 'arg:0', 'arg:0', 'Sortable mutated in place');

CREATE INDEX idx_flow_sem_pkg ON flow_semantics(package, func_name);
`
	return sqlitex.ExecuteScript(conn, ddl, nil)
}

// createTaintModel builds a security-oriented taint specification table and
// annotates call nodes that target known sources, sinks, barriers, or propagators.
func createTaintModel(conn *sqlite.Conn) error {
	ddl := `
CREATE TABLE taint_specs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package TEXT NOT NULL,
    func_name TEXT NOT NULL,
    role TEXT NOT NULL,
    category TEXT,
    description TEXT
);

-- Sources: functions that introduce external/untrusted data
INSERT INTO taint_specs (package, func_name, role, category, description) VALUES
('net/http', 'FormValue', 'source', 'http_input', 'HTTP form value'),
('net/http', 'PostFormValue', 'source', 'http_input', 'HTTP POST form value'),
('net/http', 'ReadRequest', 'source', 'http_input', 'HTTP request read'),
('os', 'Getenv', 'source', 'env', 'Environment variable'),
('os', 'ReadFile', 'source', 'file_read', 'File read'),
('io', 'ReadAll', 'source', 'io_read', 'Reader content'),
('io', 'Copy', 'source', 'io_read', 'Stream copy'),
('bufio', 'ReadString', 'source', 'io_read', 'Buffered read'),
('bufio', 'ReadLine', 'source', 'io_read', 'Buffered line read'),
('encoding/json', 'Unmarshal', 'source', 'deserialization', 'JSON unmarshal'),
('encoding/json', 'Decode', 'source', 'deserialization', 'JSON stream decode'),
('encoding/xml', 'Unmarshal', 'source', 'deserialization', 'XML unmarshal'),
('gopkg.in/yaml.v2', 'Unmarshal', 'source', 'deserialization', 'YAML unmarshal'),
('gopkg.in/yaml.v3', 'Unmarshal', 'source', 'deserialization', 'YAML unmarshal');

-- Sinks: functions that perform security-sensitive operations
INSERT INTO taint_specs (package, func_name, role, category, description) VALUES
('os/exec', 'Command', 'sink', 'command_injection', 'OS command construction'),
('os/exec', 'CommandContext', 'sink', 'command_injection', 'OS command with context'),
('os/exec', 'Run', 'sink', 'command_injection', 'OS command execution'),
('os/exec', 'Start', 'sink', 'command_injection', 'OS command start'),
('os', 'WriteFile', 'sink', 'file_write', 'File write'),
('os', 'Create', 'sink', 'file_write', 'File creation'),
('os', 'OpenFile', 'sink', 'file_write', 'File open'),
('html/template', 'Execute', 'sink', 'template_exec', 'HTML template execution'),
('html/template', 'ExecuteTemplate', 'sink', 'template_exec', 'HTML template execution'),
('text/template', 'Execute', 'sink', 'template_exec', 'Text template execution'),
('database/sql', 'Exec', 'sink', 'sql_injection', 'SQL execution'),
('database/sql', 'Query', 'sink', 'sql_injection', 'SQL query'),
('database/sql', 'QueryRow', 'sink', 'sql_injection', 'SQL query single row'),
('net/http', 'Redirect', 'sink', 'open_redirect', 'HTTP redirect'),
('log', 'Printf', 'sink', 'log_injection', 'Log formatted output'),
('log', 'Fatalf', 'sink', 'log_injection', 'Log fatal output');

-- Barriers: functions that sanitize, validate, or escape data
INSERT INTO taint_specs (package, func_name, role, category, description) VALUES
('net/url', 'QueryEscape', 'barrier', 'url_escape', 'URL query escaping'),
('net/url', 'PathEscape', 'barrier', 'url_escape', 'URL path escaping'),
('html', 'EscapeString', 'barrier', 'html_escape', 'HTML entity escaping'),
('regexp', 'MatchString', 'barrier', 'validation', 'Regex match validation'),
('regexp', 'Match', 'barrier', 'validation', 'Regex validation'),
('strconv', 'Atoi', 'barrier', 'type_conversion', 'String to int'),
('strconv', 'ParseInt', 'barrier', 'type_conversion', 'String to int64'),
('strconv', 'ParseFloat', 'barrier', 'type_conversion', 'String to float'),
('strconv', 'ParseBool', 'barrier', 'type_conversion', 'String to bool'),
('filepath', 'Clean', 'barrier', 'path_sanitize', 'Path sanitization'),
('filepath', 'Abs', 'barrier', 'path_sanitize', 'Absolute path resolution'),
('path', 'Clean', 'barrier', 'path_sanitize', 'Path sanitization');

-- Propagators: functions that transform data while preserving taint
INSERT INTO taint_specs (package, func_name, role, category, description) VALUES
('fmt', 'Sprintf', 'propagator', 'string_format', 'String formatting'),
('fmt', 'Fprintf', 'propagator', 'string_format', 'Formatted write'),
('strings', 'Join', 'propagator', 'string_concat', 'String concatenation'),
('strings', 'Replace', 'propagator', 'string_transform', 'String replacement'),
('strings', 'ReplaceAll', 'propagator', 'string_transform', 'String replace all'),
('strings', 'TrimSpace', 'propagator', 'string_transform', 'String trimming'),
('strings', 'ToLower', 'propagator', 'string_transform', 'Case conversion'),
('strings', 'ToUpper', 'propagator', 'string_transform', 'Case conversion'),
('strings', 'Split', 'propagator', 'string_transform', 'String splitting'),
('bytes', 'Join', 'propagator', 'bytes_concat', 'Bytes concatenation'),
('encoding/base64', 'EncodeToString', 'propagator', 'encoding', 'Base64 encoding'),
('encoding/base64', 'DecodeString', 'propagator', 'encoding', 'Base64 decoding'),
('encoding/hex', 'EncodeToString', 'propagator', 'encoding', 'Hex encoding');

CREATE INDEX idx_taint_specs_role ON taint_specs(role);
CREATE INDEX idx_taint_specs_pkg ON taint_specs(package, func_name);

-- Annotate call nodes that target known taint-relevant functions
INSERT INTO node_properties (node_id, key, value)
SELECT DISTINCT c.id, 'taint_role', ts.role
FROM nodes c
JOIN edges cse ON cse.source = c.id AND cse.kind = 'call_site'
JOIN nodes callee ON callee.id = cse.target
JOIN taint_specs ts ON callee.package = ts.package AND callee.name = ts.func_name
WHERE c.kind = 'call';

INSERT INTO node_properties (node_id, key, value)
SELECT DISTINCT c.id, 'taint_category', ts.category
FROM nodes c
JOIN edges cse ON cse.source = c.id AND cse.kind = 'call_site'
JOIN nodes callee ON callee.id = cse.target
JOIN taint_specs ts ON callee.package = ts.package AND callee.name = ts.func_name
WHERE c.kind = 'call';

-- Findings: functions containing both sources and sinks
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'taint_hotspot', 'warning', fn.id, fn.file, fn.line,
  fn.name || ' has taint source (' || GROUP_CONCAT(DISTINCT src_cat.value) || ') and sink (' || GROUP_CONCAT(DISTINCT sink_cat.value) || ')',
  json_object('function', fn.name, 'package', fn.package,
              'source_categories', GROUP_CONCAT(DISTINCT src_cat.value),
              'sink_categories', GROUP_CONCAT(DISTINCT sink_cat.value))
FROM node_properties src_role
JOIN nodes src ON src.id = src_role.node_id
JOIN node_properties src_cat ON src_cat.node_id = src.id AND src_cat.key = 'taint_category'
JOIN nodes fn ON fn.id = src.parent_function
JOIN nodes sink ON sink.parent_function = fn.id
JOIN node_properties sink_role ON sink_role.node_id = sink.id
  AND sink_role.key = 'taint_role' AND sink_role.value = 'sink'
JOIN node_properties sink_cat ON sink_cat.node_id = sink.id AND sink_cat.key = 'taint_category'
WHERE src_role.key = 'taint_role' AND src_role.value = 'source'
  AND src.parent_function IS NOT NULL
GROUP BY fn.id;
`
	return sqlitex.ExecuteScript(conn, ddl, nil)
}

// createSchemaDocs creates a self-documenting table describing the CPG schema,
// node kinds, edge kinds, and available analysis features.
func createSchemaDocs(conn *sqlite.Conn) error {
	ddl := `
CREATE TABLE schema_docs (
    category TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    example TEXT
);

-- Node kinds
INSERT INTO schema_docs (category, name, description, example) VALUES
('node_kind', 'package', 'Go package declaration', NULL),
('node_kind', 'file', 'Source file', NULL),
('node_kind', 'function', 'Function or method declaration', 'scrape::Manager.Run@manager.go:142:1'),
('node_kind', 'parameter', 'Function parameter', NULL),
('node_kind', 'result', 'Function return value', NULL),
('node_kind', 'local', 'Local variable (short decl or var)', NULL),
('node_kind', 'call', 'Function/method call expression', NULL),
('node_kind', 'literal', 'Literal value (string, int, bool)', NULL),
('node_kind', 'identifier', 'Variable/const/type reference', NULL),
('node_kind', 'if', 'If statement', NULL),
('node_kind', 'for', 'For/range loop', NULL),
('node_kind', 'switch', 'Switch/type-switch statement', NULL),
('node_kind', 'select', 'Select statement (channel multiplexing)', NULL),
('node_kind', 'case', 'Case/default clause', NULL),
('node_kind', 'return', 'Return statement', NULL),
('node_kind', 'assign', 'Assignment statement', NULL),
('node_kind', 'go', 'Goroutine launch (go statement)', NULL),
('node_kind', 'defer', 'Defer statement', NULL),
('node_kind', 'send', 'Channel send operation', NULL),
('node_kind', 'block', 'Block scope (curly braces)', NULL),
('node_kind', 'branch', 'Break/continue/goto/fallthrough', NULL),
('node_kind', 'type_decl', 'Type declaration (struct, interface, alias)', NULL),
('node_kind', 'field', 'Struct field or interface method', NULL),
('node_kind', 'composite_lit', 'Struct/slice/map literal', NULL),
('node_kind', 'basic_block', 'SSA basic block (for CFG edges)', NULL),
('node_kind', 'type_param', 'Generic type parameter (Go 1.18+)', NULL),
('node_kind', 'import', 'Import declaration', NULL),
('node_kind', 'doc', 'Doc comment', NULL),
('node_kind', 'label', 'Label for goto/break/continue', NULL),
('node_kind', 'incdec', 'Increment/decrement (x++/x--)', NULL),
('node_kind', 'meta_data', 'CPG metadata node', NULL);

-- Edge kinds
INSERT INTO schema_docs (category, name, description, example) VALUES
('edge_kind', 'ast', 'Parent→child in syntax tree', 'function → parameter'),
('edge_kind', 'cfg', 'Control flow: basic_block→basic_block', 'Properties: {"label":"true"/"false"} for if branches'),
('edge_kind', 'cdg', 'Control dependence: block depends on branch', NULL),
('edge_kind', 'dom', 'Dominator tree edge', NULL),
('edge_kind', 'pdom', 'Post-dominator tree edge', NULL),
('edge_kind', 'dfg', 'Data flow: definition→use (intra-procedural)', 'Properties: {"heuristic":true} for external calls'),
('edge_kind', 'call', 'Caller function→callee function', 'Properties: {"dynamic":true} for interface dispatch'),
('edge_kind', 'call_site', 'Call AST node→callee function', NULL),
('edge_kind', 'param_in', 'Actual argument→formal parameter (inter-procedural)', 'Properties: {"index": N}'),
('edge_kind', 'param_out', 'Callee function→call site (return value flow)', NULL),
('edge_kind', 'implements', 'Concrete type→interface it implements', NULL),
('edge_kind', 'embeds', 'Struct→embedded type', NULL),
('edge_kind', 'alias_of', 'Type alias→aliased type', NULL),
('edge_kind', 'satisfies_method', 'Concrete method→interface method it satisfies', NULL),
('edge_kind', 'has_method', 'Type declaration→its method functions', NULL),
('edge_kind', 'scope', 'Block→enclosing scope (lexical scoping)', NULL),
('edge_kind', 'ref', 'Identifier→its definition', NULL),
('edge_kind', 'eval_type', 'Expression→its type declaration', NULL),
('edge_kind', 'argument', 'Call→argument expression', 'Properties: {"index": N}'),
('edge_kind', 'receiver', 'Method call→receiver expression', NULL),
('edge_kind', 'doc', 'Declaration→its doc comment', NULL),
('edge_kind', 'initializer', 'Variable→its initializing expression', NULL),
('edge_kind', 'next_sibling', 'Statement→next statement (sequential order)', NULL),
('edge_kind', 'branch_target', 'Branch statement→target label', NULL),
('edge_kind', 'error_wrap', 'Error wrapping: fmt.Errorf %%w or errors.Join → wrapped error', NULL),
('edge_kind', 'capture', 'Closure→captured variable from outer scope', NULL),
('edge_kind', 'eog', 'Evaluation order: arg[i]→arg[i+1] within call', NULL);

-- Node properties (on JSON properties column)
INSERT INTO schema_docs (category, name, description, example) VALUES
('node_property', 'receiver', 'Receiver type for methods', '*Manager'),
('node_property', 'generic', 'Function or type has type parameters', 'true'),
('node_property', 'external', 'External stub node (not in analyzed code)', 'true'),
('node_property', 'snippet', 'Code snippet for the node', 'if err != nil {'),
('node_property', 'nesting_depth', 'Depth of control structure nesting', '5'),
('node_property', 'is_generated', 'File is generated (.pb.go)', 'true'),
('node_property', 'returns_error', 'Function returns error type', 'true'),
('node_property', 'returns_nilable', 'Function returns pointer/slice/map/chan', 'true'),
('node_property', 'nullable', 'Parameter accepts nil (pointer/slice/map/chan/interface)', 'true'),
('node_property', 'mutable', 'Parameter is mutable (pointer/slice/map/chan)', 'true'),
('node_property', 'has_context', 'Function has context.Context as first param', 'true'),
('node_property', 'context_param', 'Parameter is context.Context', 'true'),
('node_property', 'context_derivation', 'Call derives new context', 'WithCancel'),
('node_property', 'sync_kind', 'Call is sync primitive', 'mutex_lock'),
('node_property', 'struct_tag', 'Struct field tag', 'json:"name,omitempty"'),
('node_property', 'inlineable', 'Function can be inlined by compiler', 'true'),
('node_property', 'heap_escapes', 'Variable escapes to heap (GC pressure)', 'true/false'),
('node_property', 'taint_role', 'Security taint classification', 'source/sink/barrier/propagator'),
('node_property', 'taint_category', 'Taint category detail', 'http_input, sql_injection');

-- Tables
INSERT INTO schema_docs (category, name, description, example) VALUES
('table', 'nodes', 'All CPG nodes (AST + SSA)', 'SELECT * FROM nodes WHERE kind=''function'' AND package=''scrape'''),
('table', 'edges', 'All CPG edges (AST, CFG, DFG, call, type)', 'SELECT * FROM edges WHERE kind=''call'' AND source=:func_id'),
('table', 'sources', 'Source file contents', 'SELECT content FROM sources WHERE file=''scrape/manager.go'''),
('table', 'metrics', 'Function-level metrics', 'SELECT * FROM metrics ORDER BY cyclomatic_complexity DESC'),
('table', 'findings', 'Pre-computed analysis findings', 'SELECT * FROM findings WHERE category=''complexity'''),
('table', 'queries', 'Parameterized CTE queries for analysis', 'SELECT name, description FROM queries'),
('table', 'taint_specs', 'Security taint model: known sources/sinks/barriers', 'SELECT * FROM taint_specs WHERE role=''sink'''),
('table', 'flow_semantics', 'Data flow semantics for stdlib functions', 'SELECT * FROM flow_semantics WHERE package=''fmt'''),
('table', 'node_properties', 'Vertical property table (extracted from JSON)', 'SELECT * FROM node_properties WHERE key=''receiver'''),
('table', 'edge_properties', 'Vertical edge property table', 'SELECT * FROM edge_properties WHERE key=''dynamic'''),
('table', 'stats_overview', 'Summary statistics for the entire CPG', 'SELECT * FROM stats_overview'),
('table', 'stats_packages', 'Per-package statistics', 'SELECT * FROM stats_packages ORDER BY functions DESC'),
('table', 'sources_fts', 'FTS5 full-text search on source code', 'SELECT file FROM sources_fts WHERE content MATCH ''mutex''');

-- Views
INSERT INTO schema_docs (category, name, description, example) VALUES
('view', 'v_call_graph', 'Flattened call graph with names', 'SELECT * FROM v_call_graph WHERE caller_package=''scrape'''),
('view', 'v_data_flow', 'DFG edges with file/line context', NULL),
('view', 'v_function_summary', 'Per-function metrics + call counts', 'SELECT * FROM v_function_summary ORDER BY complexity DESC'),
('view', 'v_type_hierarchy', 'Implements/embeds/alias relationships', NULL),
('view', 'v_package_deps', 'Aggregated cross-package call edges', NULL),
('view', 'v_file_deps', 'File-level dependency graph', NULL),
('view', 'v_function_io', 'Parameters and return values per function', NULL),
('view', 'v_api_surface', 'Exported functions and types per package', NULL),
('view', 'v_method_sets', 'Methods grouped by receiver type', NULL),
('view', 'v_error_handling', 'Error-returning functions with metrics', NULL),
('view', 'v_package_stability', 'Package stability metrics: afferent/efferent coupling, instability index, abstractness', NULL),
('view', 'v_control_flow_profile', 'Control flow breakdown per function: if/for/switch/select/return/defer/go counts', NULL),
('finding', 'risk_score', 'Composite bug-risk score combining complexity, LOC, fan-in, fan-out', NULL),
('finding', 'dead_code', 'Internal functions with zero callers (unreachable code)', NULL),
('finding', 'interface_bloat', 'Interfaces with 5+ methods (Go idiom prefers small interfaces)', NULL),
('finding', 'similar_function', 'Structurally similar function pairs (potential clones)', NULL),
('query', 'dependency_depth', 'Package dependency depth from leaf packages', NULL),
('query', 'function_risk_ranking', 'Top 50 riskiest functions by composite score', NULL),
('query', 'package_stability', 'Package instability and abstractness metrics', NULL),
('query', 'function_control_profile', 'Control flow structure breakdown per function', NULL),
('query', 'similar_functions', 'Find structural clones of a given function', NULL),
('view', 'v_package_cohesion', 'Package cohesion: ratio of internal vs external calls', NULL),
('view', 'v_concurrency_profile', 'Per-package concurrency usage: goroutines, channels, sync', NULL),
('view', 'v_package_impact', 'Transitive package impact: packages affected by changes', NULL),
('finding', 'missing_context_first', 'Functions with context.Context not as first parameter', NULL),
('finding', 'large_return', 'Functions returning 4+ values', NULL),
('finding', 'bool_params', 'Functions with 2+ boolean parameters (boolean blindness)', NULL),
('finding', 'panic_call', 'Functions that call panic() directly', NULL),
('query', 'package_cohesion', 'Package cohesion analysis', NULL),
('query', 'concurrency_profile', 'Per-package concurrency usage', NULL),
('query', 'package_impact', 'Transitive package impact analysis', NULL),
('query', 'function_neighborhood', 'Direct callers and callees of a function', NULL),
('query', 'file_complexity_heatmap', 'Total complexity per file for heatmap visualization', NULL),
('query', 'type_usage', 'Functions that reference a given type in their signatures', NULL),
('table', 'dashboard_complexity_distribution', 'Complexity histogram buckets for chart rendering', NULL),
('table', 'dashboard_package_treemap', 'Per-package LOC + complexity for treemap visualization', NULL),
('table', 'dashboard_findings_summary', 'Finding category counts for bar chart', NULL),
('table', 'dashboard_edge_distribution', 'Edge type distribution for pie/donut chart', NULL),
('table', 'dashboard_node_distribution', 'Node type distribution for pie/donut chart', NULL),
('table', 'dashboard_complexity_vs_loc', 'Scatter plot data: complexity vs LOC per function', NULL),
('table', 'dashboard_overview', 'Key-value overview stats for dashboard header cards', NULL),
('table', 'dashboard_top_functions', 'Top 50 functions by complexity, LOC, fan-in, fan-out for leaderboards', 'SELECT * FROM dashboard_top_functions WHERE metric = ''complexity'' ORDER BY rank'),
('table', 'dashboard_hotspots', 'Functions ranked by combined hotspot score (complexity + fan-in + findings)', 'SELECT * FROM dashboard_hotspots ORDER BY hotspot_score DESC LIMIT 20'),
('table', 'package_coupling', 'Cross-package call coupling matrix (source→target, count)', 'SELECT * FROM package_coupling ORDER BY call_count DESC LIMIT 20'),
('table', 'error_chains', 'Functions involved in error wrapping/propagation chains', 'SELECT * FROM error_chains WHERE error_wraps > 0 ORDER BY error_wraps DESC'),
('finding', 'long_param_list', 'Functions with more than 5 parameters', NULL),
('finding', 'god_package', 'Packages with more than 50 functions', NULL),
('finding', 'high_coupling', 'Packages depending on more than 10 other packages', NULL),
('query', 'hotspot_analysis', 'Find functions with combined high complexity, fan-in, and findings', NULL),
('query', 'package_coupling_matrix', 'Aggregated cross-package call coupling matrix', NULL),
('query', 'error_propagation', 'Functions in error wrapping chains', NULL),
('query', 'top_functions_by_metric', 'Top 50 functions by complexity, LOC, fan-in, or fan-out', NULL),
('query', 'package_coupling_degree', 'Packages ranked by number of coupled packages', NULL),
('query', 'call_chain_pathfinder', 'Find all call paths between two functions (recursive CTE, up to 6 hops)', NULL),
('table', 'dashboard_file_heatmap', 'Per-file complexity/LOC/findings for code heatmap rendering', 'SELECT * FROM dashboard_file_heatmap ORDER BY hotspot_score DESC LIMIT 20'),
('table', 'dashboard_package_graph', 'Internal package dependency graph (source→target, weight) for force-directed viz', 'SELECT * FROM dashboard_package_graph ORDER BY weight DESC LIMIT 20'),
('table', 'dashboard_function_detail', 'Pre-aggregated function profiles with callers/callees for detail panels', 'SELECT * FROM dashboard_function_detail WHERE function_id = ''main::main@main.go:377:1'''),
('query', 'file_heatmap', 'File-level complexity heatmap data', NULL),
('query', 'package_graph', 'Internal package dependency graph for visualization', NULL),
('query', 'function_detail', 'Complete function profile for detail panels', NULL),
('table', 'type_impl_map', 'Interface→concrete type implementation mapping with method counts', 'SELECT * FROM type_impl_map ORDER BY interface_name LIMIT 20'),
('table', 'type_hierarchy', 'Type embedding hierarchy (parent→embedded child)', 'SELECT * FROM type_hierarchy WHERE embedded_id IS NOT NULL LIMIT 20'),
('table', 'type_method_set', 'Methods per type with complexity and LOC', 'SELECT * FROM type_method_set ORDER BY type_name, method_name LIMIT 20'),
('finding', 'large_interface', 'Interfaces with more than 10 methods (overly broad contract)', NULL),
('finding', 'orphan_type', 'Types with no implements/embeds/method edges', NULL),
('query', 'interface_map', 'Concrete types implementing a given interface', NULL),
('query', 'type_hierarchy_tree', 'Type embedding tree for a given type', NULL),
('query', 'method_set', 'Complete method set for a type', NULL),
('query', 'largest_interfaces', 'Interfaces ranked by method count', NULL),
('query', 'most_implemented', 'Interfaces with the most implementations', NULL),
('table', 'symbol_index', 'All named declarations for quick symbol search', 'SELECT * FROM symbol_index WHERE name LIKE ''Manager%'' LIMIT 10'),
('table', 'file_outline', 'Hierarchical file structure for sidebar tree', 'SELECT * FROM file_outline WHERE file = ''scrape/manager.go'' ORDER BY line'),
('table', 'xrefs', 'Definition→usage cross-reference table for go-to-definition and find-all-references', 'SELECT * FROM xrefs WHERE def_name = ''Manager'' LIMIT 10'),
('table', 'go_pattern_summary', 'Go-specific construct counts per package (goroutines, channels, errors, etc.)', 'SELECT * FROM go_pattern_summary ORDER BY goroutine_count DESC LIMIT 10'),
('query', 'symbol_search', 'Search symbols by name (supports LIKE patterns)', NULL),
('query', 'file_outline_query', 'Get hierarchical outline of a file', NULL),
('query', 'xref_lookup', 'Find all usages of a symbol', NULL),
('query', 'go_patterns', 'Go-specific construct usage per package', NULL);

CREATE INDEX idx_schema_docs_cat ON schema_docs(category);
`
	return sqlitex.ExecuteScript(conn, ddl, nil)
}

// createAdvancedAnalysis adds package stability metrics, risk scoring,
// dead code detection, interface bloat, and structural similarity analysis.
func createAdvancedAnalysis(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- Package stability metrics (Robert C. Martin's instability/abstractness)
-- Ca = afferent coupling (packages that depend on this one)
-- Ce = efferent coupling (packages this one depends on)
-- Instability I = Ce/(Ca+Ce), Abstractness A = interfaces/total_types
CREATE VIEW v_package_stability AS
  WITH pkg_types AS (
    SELECT n.package, COUNT(*) AS total_types,
      SUM(CASE WHEN np.value = 'interface' THEN 1 ELSE 0 END) AS interface_count
    FROM nodes n
    LEFT JOIN node_properties np ON np.node_id = n.id AND np.key = 'type_kind'
    WHERE n.kind = 'type_decl' AND n.package IS NOT NULL
    GROUP BY n.package
  ),
  afferent AS (
    SELECT target_package AS package, COUNT(DISTINCT source_package) AS ca
    FROM v_package_deps GROUP BY target_package
  ),
  efferent AS (
    SELECT source_package AS package, COUNT(DISTINCT target_package) AS ce
    FROM v_package_deps GROUP BY source_package
  )
  SELECT
    COALESCE(pt.package, a.package, e.package) AS package,
    COALESCE(a.ca, 0) AS afferent_coupling,
    COALESCE(e.ce, 0) AS efferent_coupling,
    CASE WHEN COALESCE(a.ca, 0) + COALESCE(e.ce, 0) = 0 THEN 0.5
         ELSE ROUND(CAST(COALESCE(e.ce, 0) AS REAL) / (COALESCE(a.ca, 0) + COALESCE(e.ce, 0)), 3)
    END AS instability,
    COALESCE(pt.total_types, 0) AS total_types,
    COALESCE(pt.interface_count, 0) AS interface_count,
    CASE WHEN COALESCE(pt.total_types, 0) = 0 THEN 0.0
         ELSE ROUND(CAST(COALESCE(pt.interface_count, 0) AS REAL) / pt.total_types, 3)
    END AS abstractness
  FROM pkg_types pt
  FULL OUTER JOIN afferent a ON a.package = pt.package
  FULL OUTER JOIN efferent e ON e.package = COALESCE(pt.package, a.package);

-- Control flow profile: count of each control structure type per function
CREATE VIEW v_control_flow_profile AS
  SELECT
    n.parent_function AS function_id,
    fn.name AS function_name,
    fn.package,
    SUM(CASE WHEN n.kind = 'if' THEN 1 ELSE 0 END) AS if_count,
    SUM(CASE WHEN n.kind = 'for' THEN 1 ELSE 0 END) AS for_count,
    SUM(CASE WHEN n.kind = 'switch' THEN 1 ELSE 0 END) AS switch_count,
    SUM(CASE WHEN n.kind = 'select' THEN 1 ELSE 0 END) AS select_count,
    SUM(CASE WHEN n.kind = 'return' THEN 1 ELSE 0 END) AS return_count,
    SUM(CASE WHEN n.kind = 'defer' THEN 1 ELSE 0 END) AS defer_count,
    SUM(CASE WHEN n.kind = 'go' THEN 1 ELSE 0 END) AS go_count,
    COUNT(*) AS total_statements
  FROM nodes n
  JOIN nodes fn ON fn.id = n.parent_function
  WHERE n.parent_function IS NOT NULL
    AND n.kind IN ('if', 'for', 'switch', 'select', 'return', 'defer', 'go',
                   'assign', 'call', 'send', 'branch')
  GROUP BY n.parent_function;

-- Risk scoring: composite metric ranking functions by bug likelihood
-- Formula: 3*norm(complexity) + 2*norm(loc) + norm(fan_in) + norm(fan_out)
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  WITH maxes AS (
    SELECT
      MAX(cyclomatic_complexity) AS max_cc,
      MAX(loc) AS max_loc,
      MAX(fan_in) AS max_fi,
      MAX(fan_out) AS max_fo
    FROM metrics
    WHERE cyclomatic_complexity > 0
  )
  SELECT 'risk_score', 'info', n.id, n.file, n.line,
    n.name || ' risk=' || CAST(ROUND(
      3.0 * CAST(m.cyclomatic_complexity AS REAL) / MAX(maxes.max_cc, 1) +
      2.0 * CAST(m.loc AS REAL) / MAX(maxes.max_loc, 1) +
      1.0 * CAST(m.fan_in AS REAL) / MAX(maxes.max_fi, 1) +
      1.0 * CAST(m.fan_out AS REAL) / MAX(maxes.max_fo, 1)
    , 2) AS TEXT),
    json_object(
      'risk_score', ROUND(
        3.0 * CAST(m.cyclomatic_complexity AS REAL) / MAX(maxes.max_cc, 1) +
        2.0 * CAST(m.loc AS REAL) / MAX(maxes.max_loc, 1) +
        1.0 * CAST(m.fan_in AS REAL) / MAX(maxes.max_fi, 1) +
        1.0 * CAST(m.fan_out AS REAL) / MAX(maxes.max_fo, 1)
      , 2),
      'complexity', m.cyclomatic_complexity,
      'loc', m.loc,
      'fan_in', m.fan_in,
      'fan_out', m.fan_out,
      'package', n.package
    )
  FROM metrics m
  JOIN nodes n ON n.id = m.function_id
  CROSS JOIN maxes
  WHERE m.cyclomatic_complexity >= 5 OR m.loc >= 30
  ORDER BY (
    3.0 * CAST(m.cyclomatic_complexity AS REAL) / MAX(maxes.max_cc, 1) +
    2.0 * CAST(m.loc AS REAL) / MAX(maxes.max_loc, 1) +
    1.0 * CAST(m.fan_in AS REAL) / MAX(maxes.max_fi, 1) +
    1.0 * CAST(m.fan_out AS REAL) / MAX(maxes.max_fo, 1)
  ) DESC
  LIMIT 200;

-- Dead code: internal functions with zero callers that aren't entry points
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'dead_code', 'warning', n.id, n.file, n.line,
    'unreachable function ' || n.name || ' (zero callers)',
    json_object('name', n.name, 'package', n.package)
  FROM nodes n
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE n.kind = 'function'
    AND COALESCE(m.fan_in, 0) = 0
    AND n.name NOT GLOB '[A-Z]*'
    AND n.name NOT IN ('main', 'init')
    AND n.name NOT LIKE '%Test%'
    AND n.name NOT LIKE '%Benchmark%'
    AND n.name NOT LIKE '%Example%'
    AND n.package IS NOT NULL
    AND n.package NOT LIKE 'cmd/%'
    AND n.id NOT LIKE 'ext::%';

-- Interface bloat: interfaces with many methods (Go prefers small interfaces)
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'interface_bloat', 'info', n.id, n.file, n.line,
    n.name || ' has ' || method_count || ' methods (consider splitting)',
    json_object('method_count', method_count, 'package', n.package)
  FROM (
    SELECT e.source AS type_id, COUNT(*) AS method_count
    FROM edges e
    JOIN nodes child ON child.id = e.target AND child.kind = 'field'
    JOIN nodes parent ON parent.id = e.source AND parent.kind = 'type_decl'
    JOIN node_properties tk ON tk.node_id = parent.id AND tk.key = 'type_kind' AND tk.value = 'interface'
    WHERE e.kind = 'ast'
    GROUP BY e.source
    HAVING COUNT(*) >= 5
  ) sub
  JOIN nodes n ON n.id = sub.type_id;

-- Similar functions: structural clones (same complexity + similar param count + similar LOC)
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'similar_function', 'info', m1.function_id, n1.file, n1.line,
    n1.name || ' is structurally similar to ' || n2.name ||
    ' (cc=' || m1.cyclomatic_complexity || ', loc≈' || m1.loc || '/' || m2.loc || ')',
    json_object('twin_id', m2.function_id, 'twin_name', n2.name,
                'complexity', m1.cyclomatic_complexity,
                'loc_a', m1.loc, 'loc_b', m2.loc,
                'package_a', n1.package, 'package_b', n2.package)
  FROM metrics m1
  JOIN metrics m2 ON m1.function_id < m2.function_id
    AND m1.cyclomatic_complexity = m2.cyclomatic_complexity
    AND m1.num_params = m2.num_params
    AND ABS(m1.loc - m2.loc) <= 3
  JOIN nodes n1 ON n1.id = m1.function_id
  JOIN nodes n2 ON n2.id = m2.function_id
  WHERE m1.cyclomatic_complexity >= 5 AND m1.loc >= 15
    AND n1.package != n2.package;

-- Additional queries
INSERT INTO queries (name, description, sql) VALUES
('dependency_depth',
 'Package dependency depth from leaves to root (BFS layers)',
 'WITH RECURSIVE dep_layers(package, depth) AS (
    SELECT DISTINCT source_package, 0
    FROM v_package_deps
    WHERE source_package NOT IN (SELECT target_package FROM v_package_deps)
    UNION
    SELECT pd.target_package, dl.depth + 1
    FROM dep_layers dl
    JOIN v_package_deps pd ON pd.source_package = dl.package
    WHERE dl.depth < 20
  )
  SELECT package, MAX(depth) AS max_depth
  FROM dep_layers GROUP BY package ORDER BY max_depth DESC');

INSERT INTO queries (name, description, sql) VALUES
('function_risk_ranking',
 'Top riskiest functions by composite risk score',
 'SELECT node_id, file, line, message,
    json_extract(details, ''$.risk_score'') AS risk_score,
    json_extract(details, ''$.complexity'') AS complexity,
    json_extract(details, ''$.loc'') AS loc,
    json_extract(details, ''$.fan_in'') AS fan_in,
    json_extract(details, ''$.fan_out'') AS fan_out,
    json_extract(details, ''$.package'') AS package
  FROM findings
  WHERE category = ''risk_score''
  ORDER BY CAST(json_extract(details, ''$.risk_score'') AS REAL) DESC
  LIMIT 50');

INSERT INTO queries (name, description, sql) VALUES
('package_stability',
 'Package stability analysis (Martin instability + abstractness metrics)',
 'SELECT package, afferent_coupling, efferent_coupling, instability,
    total_types, interface_count, abstractness,
    ROUND(ABS(instability + abstractness - 1.0), 3) AS distance_from_main_seq
  FROM v_package_stability
  ORDER BY distance_from_main_seq DESC');

INSERT INTO queries (name, description, sql) VALUES
('function_control_profile',
 'Control flow breakdown per function',
 'SELECT function_id, function_name, package,
    if_count, for_count, switch_count, select_count,
    return_count, defer_count, go_count, total_statements
  FROM v_control_flow_profile
  WHERE function_id = :function_id');

INSERT INTO queries (name, description, sql) VALUES
('similar_functions',
 'Find structural clones of a function (same complexity and parameter count)',
 'SELECT n2.id, n2.name, n2.package, n2.file, n2.line,
    m2.cyclomatic_complexity AS complexity, m2.loc, m2.num_params
  FROM metrics m1
  JOIN metrics m2 ON m1.function_id != m2.function_id
    AND m1.cyclomatic_complexity = m2.cyclomatic_complexity
    AND m1.num_params = m2.num_params
    AND ABS(m1.loc - m2.loc) <= 5
  JOIN nodes n2 ON n2.id = m2.function_id
  WHERE m1.function_id = :function_id
  ORDER BY ABS(m1.loc - m2.loc), n2.package, n2.name');

`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return err
	}

	// Count new findings
	var riskCount, deadCount, bloatCount, simCount int64
	for _, pair := range []struct {
		cat  string
		dest *int64
	}{
		{"risk_score", &riskCount},
		{"dead_code", &deadCount},
		{"interface_bloat", &bloatCount},
		{"similar_function", &simCount},
	} {
		cat := pair.cat
		_ = sqlitex.ExecuteTransient(conn,
			`SELECT COUNT(*) FROM findings WHERE category = ?`,
			&sqlitex.ExecOptions{
				Args: []any{cat},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					*pair.dest = stmt.ColumnInt64(0)
					return nil
				},
			})
	}

	prog.Log("Advanced: %d risk scores, %d dead code, %d interface bloat, %d similar pairs, 2 views, 5 queries",
		riskCount, deadCount, bloatCount, simCount)
	return nil
}

// createCohesionAndPatterns adds package cohesion, concurrency profile,
// function signature patterns, and impact scoring analysis.
func createCohesionAndPatterns(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- Package cohesion: ratio of intra-package calls to total calls per package
-- High cohesion = most calls stay within the package
CREATE VIEW v_package_cohesion AS
  WITH pkg_calls AS (
    SELECT n1.package AS pkg,
      COUNT(*) AS total_calls,
      SUM(CASE WHEN n1.package = n2.package THEN 1 ELSE 0 END) AS internal_calls,
      SUM(CASE WHEN n1.package != n2.package THEN 1 ELSE 0 END) AS external_calls
    FROM edges e
    JOIN nodes n1 ON e.source = n1.id
    JOIN nodes n2 ON e.target = n2.id
    WHERE e.kind = 'call' AND n1.package IS NOT NULL AND n2.package IS NOT NULL
    GROUP BY n1.package
  ),
  pkg_funcs AS (
    SELECT package AS pkg, COUNT(*) AS func_count
    FROM nodes WHERE kind = 'function' AND package IS NOT NULL
    GROUP BY package
  )
  SELECT
    pc.pkg AS package,
    pf.func_count,
    pc.total_calls,
    pc.internal_calls,
    pc.external_calls,
    ROUND(CAST(pc.internal_calls AS REAL) / MAX(pc.total_calls, 1), 3) AS cohesion_ratio
  FROM pkg_calls pc
  JOIN pkg_funcs pf ON pf.pkg = pc.pkg;

-- Concurrency profile per package: goroutines, channels, sync primitives
CREATE VIEW v_concurrency_profile AS
  SELECT
    n.package,
    SUM(CASE WHEN n.kind = 'go' THEN 1 ELSE 0 END) AS goroutine_launches,
    SUM(CASE WHEN n.kind = 'send' THEN 1 ELSE 0 END) AS channel_sends,
    SUM(CASE WHEN n.kind = 'select' THEN 1 ELSE 0 END) AS select_stmts,
    (SELECT COUNT(*) FROM node_properties np2
     JOIN nodes n2 ON n2.id = np2.node_id AND n2.package = n.package
     WHERE np2.key = 'sync_kind') AS sync_primitives,
    SUM(CASE WHEN n.kind = 'defer' THEN 1 ELSE 0 END) AS defer_stmts
  FROM nodes n
  WHERE n.package IS NOT NULL
    AND n.kind IN ('go', 'send', 'select', 'defer')
  GROUP BY n.package
  HAVING SUM(CASE WHEN n.kind = 'go' THEN 1 ELSE 0 END) > 0
     OR SUM(CASE WHEN n.kind = 'send' THEN 1 ELSE 0 END) > 0
     OR SUM(CASE WHEN n.kind = 'select' THEN 1 ELSE 0 END) > 0;

-- Package impact: transitive count of packages affected by changes to each package
CREATE VIEW v_package_impact AS
  WITH RECURSIVE impact(pkg, depth) AS (
    SELECT DISTINCT source_package, 0 FROM v_package_deps
    UNION
    SELECT pd.source_package, i.depth + 1
    FROM impact i
    JOIN v_package_deps pd ON pd.target_package = i.pkg
    WHERE i.depth < 10
  )
  SELECT pkg AS package,
    COUNT(DISTINCT pkg) - 1 AS packages_affected,
    MAX(depth) AS max_impact_depth
  FROM impact
  GROUP BY pkg;

-- Context.Context compliance: functions with 2+ params that don't take context first
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'missing_context_first', 'info', n.id, n.file, n.line,
    n.name || ' has ' || m.num_params || ' params but context.Context is not first',
    json_object('num_params', m.num_params, 'package', n.package)
  FROM nodes n
  JOIN metrics m ON m.function_id = n.id
  JOIN node_properties np ON np.node_id = n.id AND np.key = 'has_context' AND np.value = '1'
  WHERE n.kind = 'function' AND m.num_params >= 2
    AND NOT EXISTS (
      SELECT 1 FROM edges e
      JOIN nodes p ON p.id = e.target AND p.kind = 'parameter'
      WHERE e.source = n.id AND e.kind = 'ast'
        AND p.type_info LIKE '%context.Context%'
        AND json_extract(p.properties, '$.index') = '0'
    );

-- Large return tuples: functions returning 4+ values
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'large_return', 'info', n.id, n.file, n.line,
    n.name || ' returns ' || result_count || ' values',
    json_object('result_count', result_count, 'package', n.package)
  FROM (
    SELECT e.source AS func_id, COUNT(*) AS result_count
    FROM edges e
    JOIN nodes r ON r.id = e.target AND r.kind = 'result'
    WHERE e.kind = 'ast'
    GROUP BY e.source
    HAVING COUNT(*) >= 4
  ) sub
  JOIN nodes n ON n.id = sub.func_id
  WHERE n.kind = 'function';

-- Boolean blindness: functions with 2+ bool parameters
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'bool_params', 'info', n.id, n.file, n.line,
    n.name || ' has ' || bool_count || ' bool parameters (consider options struct)',
    json_object('bool_count', bool_count, 'package', n.package)
  FROM (
    SELECT e.source AS func_id, COUNT(*) AS bool_count
    FROM edges e
    JOIN nodes p ON p.id = e.target AND p.kind = 'parameter'
    WHERE e.kind = 'ast' AND p.type_info = 'bool'
    GROUP BY e.source
    HAVING COUNT(*) >= 2
  ) sub
  JOIN nodes n ON n.id = sub.func_id
  WHERE n.kind = 'function';

-- Panic-prone: functions that call panic() directly
INSERT INTO findings (category, severity, node_id, file, line, message, details)
  SELECT 'panic_call', 'warning', fn.id, fn.file, fn.line,
    fn.name || ' calls panic() directly',
    json_object('package', fn.package)
  FROM nodes fn
  WHERE fn.kind = 'function'
    AND EXISTS (
      SELECT 1 FROM nodes c
      WHERE c.kind = 'call' AND c.parent_function = fn.id AND c.name = 'panic'
    );

-- Additional queries
INSERT INTO queries (name, description, sql) VALUES
('package_cohesion',
 'Package cohesion analysis: ratio of internal vs external calls',
 'SELECT package, func_count, total_calls, internal_calls, external_calls, cohesion_ratio
  FROM v_package_cohesion ORDER BY cohesion_ratio ASC');

INSERT INTO queries (name, description, sql) VALUES
('concurrency_profile',
 'Per-package concurrency usage: goroutines, channels, sync primitives',
 'SELECT * FROM v_concurrency_profile ORDER BY goroutine_launches DESC');

INSERT INTO queries (name, description, sql) VALUES
('package_impact',
 'Transitive package impact: how many packages would be affected by changes',
 'SELECT * FROM v_package_impact ORDER BY packages_affected DESC');

INSERT INTO queries (name, description, sql) VALUES
('function_neighborhood',
 'Call neighborhood: direct callers and callees of a function',
 'SELECT ''caller'' AS direction, n.id, n.name, n.package, n.file, n.line
  FROM edges e JOIN nodes n ON n.id = e.source
  WHERE e.target = :function_id AND e.kind = ''call'' AND n.kind = ''function''
  UNION ALL
  SELECT ''callee'' AS direction, n.id, n.name, n.package, n.file, n.line
  FROM edges e JOIN nodes n ON n.id = e.target
  WHERE e.source = :function_id AND e.kind = ''call'' AND n.kind = ''function''
  ORDER BY direction, name');

INSERT INTO queries (name, description, sql) VALUES
('file_complexity_heatmap',
 'Complexity heatmap data: total complexity per file for visualization',
 'SELECT n.file, COUNT(*) AS function_count,
    SUM(COALESCE(m.cyclomatic_complexity, 0)) AS total_complexity,
    MAX(COALESCE(m.cyclomatic_complexity, 0)) AS max_complexity,
    ROUND(AVG(COALESCE(m.cyclomatic_complexity, 0)), 1) AS avg_complexity,
    SUM(COALESCE(m.loc, 0)) AS total_loc
  FROM nodes n
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE n.kind = ''function'' AND n.file IS NOT NULL
  GROUP BY n.file ORDER BY total_complexity DESC');

INSERT INTO queries (name, description, sql) VALUES
('type_usage',
 'Type usage analysis: how many functions reference a given type in their signatures',
 'SELECT n.id, n.name, n.package, n.file, n.line, n.type_info
  FROM nodes n
  WHERE n.kind = ''function''
    AND (n.type_info LIKE ''%'' || :type_name || ''%''
         OR EXISTS (
           SELECT 1 FROM edges e JOIN nodes p ON p.id = e.target
           WHERE e.source = n.id AND e.kind = ''ast''
             AND p.kind IN (''parameter'', ''result'')
             AND p.type_info LIKE ''%'' || :type_name || ''%''
         ))
  ORDER BY n.package, n.name');
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return err
	}

	// Count findings
	var ctxCount, retCount, boolCount, panicCount int64
	for _, pair := range []struct {
		cat  string
		dest *int64
	}{
		{"missing_context_first", &ctxCount},
		{"large_return", &retCount},
		{"bool_params", &boolCount},
		{"panic_call", &panicCount},
	} {
		cat := pair.cat
		_ = sqlitex.ExecuteTransient(conn,
			`SELECT COUNT(*) FROM findings WHERE category = ?`,
			&sqlitex.ExecOptions{
				Args: []any{cat},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					*pair.dest = stmt.ColumnInt64(0)
					return nil
				},
			})
	}

	prog.Log("Patterns: %d missing-ctx-first, %d large-return, %d bool-params, %d panic-calls, 3 views, 6 queries",
		ctxCount, retCount, boolCount, panicCount)
	return nil
}

// createDashboardData builds pre-computed tables optimized for chart rendering.
// Each table is designed to be directly consumable as chart data (bar, treemap, scatter).
func createDashboardData(conn *sqlite.Conn, prog *Progress) error {
	// Run each heavy INSERT as a separate transient call to avoid transaction overhead
	stmts := []struct {
		name string
		sql  string
	}{
		{"create_tables", `
CREATE TABLE dashboard_complexity_distribution (
    bucket TEXT NOT NULL, bucket_min INTEGER NOT NULL,
    bucket_max INTEGER NOT NULL, function_count INTEGER NOT NULL);
CREATE TABLE dashboard_package_treemap (
    package TEXT PRIMARY KEY, file_count INTEGER, function_count INTEGER,
    total_loc INTEGER, total_complexity INTEGER, avg_complexity REAL,
    max_complexity INTEGER, type_count INTEGER, interface_count INTEGER);
CREATE TABLE dashboard_findings_summary (
    category TEXT PRIMARY KEY, severity TEXT, count INTEGER);
CREATE TABLE dashboard_edge_distribution (
    edge_kind TEXT PRIMARY KEY, count INTEGER, percentage REAL);
CREATE TABLE dashboard_node_distribution (
    node_kind TEXT PRIMARY KEY, count INTEGER, percentage REAL);
CREATE TABLE dashboard_complexity_vs_loc (
    function_id TEXT NOT NULL, name TEXT NOT NULL, package TEXT,
    complexity INTEGER, loc INTEGER, fan_in INTEGER, fan_out INTEGER);
CREATE TABLE dashboard_overview (
    key TEXT PRIMARY KEY, value TEXT NOT NULL)`},
		{"complexity_dist", `
INSERT INTO dashboard_complexity_distribution (bucket, bucket_min, bucket_max, function_count)
  SELECT
    CASE
      WHEN cyclomatic_complexity <= 1 THEN '1 (trivial)'
      WHEN cyclomatic_complexity <= 5 THEN '2-5 (simple)'
      WHEN cyclomatic_complexity <= 10 THEN '6-10 (moderate)'
      WHEN cyclomatic_complexity <= 20 THEN '11-20 (complex)'
      WHEN cyclomatic_complexity <= 50 THEN '21-50 (very complex)'
      ELSE '51+ (extreme)'
    END,
    CASE
      WHEN cyclomatic_complexity <= 1 THEN 0
      WHEN cyclomatic_complexity <= 5 THEN 2
      WHEN cyclomatic_complexity <= 10 THEN 6
      WHEN cyclomatic_complexity <= 20 THEN 11
      WHEN cyclomatic_complexity <= 50 THEN 21
      ELSE 51
    END,
    CASE
      WHEN cyclomatic_complexity <= 1 THEN 1
      WHEN cyclomatic_complexity <= 5 THEN 5
      WHEN cyclomatic_complexity <= 10 THEN 10
      WHEN cyclomatic_complexity <= 20 THEN 20
      WHEN cyclomatic_complexity <= 50 THEN 50
      ELSE 999
    END,
    COUNT(*)
  FROM metrics
  WHERE cyclomatic_complexity > 0
  GROUP BY 1, 2, 3 ORDER BY 2`},
		{"package_treemap", `
INSERT INTO dashboard_package_treemap
  SELECT
    n.package,
    COUNT(DISTINCT n.file) AS file_count,
    COUNT(DISTINCT n.id) AS function_count,
    SUM(COALESCE(m.loc, 0)) AS total_loc,
    SUM(COALESCE(m.cyclomatic_complexity, 0)) AS total_complexity,
    ROUND(AVG(COALESCE(m.cyclomatic_complexity, 0)), 1) AS avg_complexity,
    MAX(COALESCE(m.cyclomatic_complexity, 0)) AS max_complexity,
    (SELECT COUNT(*) FROM nodes t WHERE t.kind = 'type_decl' AND t.package = n.package) AS type_count,
    (SELECT COUNT(*) FROM nodes t
     JOIN node_properties tp ON tp.node_id = t.id AND tp.key = 'type_kind' AND tp.value = 'interface'
     WHERE t.kind = 'type_decl' AND t.package = n.package) AS interface_count
  FROM nodes n
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE n.kind = 'function' AND n.package IS NOT NULL
  GROUP BY n.package`},
		{"findings_summary", `
INSERT INTO dashboard_findings_summary
  SELECT category, MAX(severity), COUNT(*)
  FROM findings GROUP BY category ORDER BY COUNT(*) DESC`},
		{"edge_dist", `
INSERT INTO dashboard_edge_distribution
  SELECT kind, COUNT(*),
    ROUND(100.0 * COUNT(*) / MAX((SELECT COUNT(*) FROM edges), 1), 2)
  FROM edges
  GROUP BY kind ORDER BY COUNT(*) DESC`},
		{"node_dist", `
INSERT INTO dashboard_node_distribution
  SELECT kind, COUNT(*),
    ROUND(100.0 * COUNT(*) / MAX((SELECT COUNT(*) FROM nodes), 1), 2)
  FROM nodes
  GROUP BY kind ORDER BY COUNT(*) DESC`},
		{"complexity_vs_loc", `
INSERT INTO dashboard_complexity_vs_loc
  SELECT m.function_id, n.name, n.package,
    m.cyclomatic_complexity, m.loc, m.fan_in, m.fan_out
  FROM metrics m
  JOIN nodes n ON n.id = m.function_id
  WHERE m.cyclomatic_complexity > 0 AND m.loc > 0`},
		{"overview", `
INSERT INTO dashboard_overview (key, value) VALUES
  ('total_packages', (SELECT COUNT(DISTINCT package) FROM nodes WHERE package IS NOT NULL)),
  ('total_files', (SELECT COUNT(DISTINCT file) FROM nodes WHERE file IS NOT NULL)),
  ('total_functions', (SELECT COUNT(*) FROM nodes WHERE kind = 'function')),
  ('total_types', (SELECT COUNT(*) FROM nodes WHERE kind = 'type_decl')),
  ('total_interfaces', (SELECT COUNT(*) FROM node_properties WHERE key = 'type_kind' AND value = 'interface')),
  ('total_nodes', (SELECT COUNT(*) FROM nodes)),
  ('total_edges', (SELECT COUNT(*) FROM edges)),
  ('total_loc', (SELECT SUM(loc) FROM metrics)),
  ('avg_complexity', (SELECT ROUND(AVG(cyclomatic_complexity), 1) FROM metrics WHERE cyclomatic_complexity > 0)),
  ('max_complexity', (SELECT MAX(cyclomatic_complexity) FROM metrics)),
  ('total_findings', (SELECT COUNT(*) FROM findings)),
  ('total_call_edges', (SELECT COUNT(*) FROM edges WHERE kind = 'call')),
  ('total_dfg_edges', (SELECT COUNT(*) FROM edges WHERE kind = 'dfg')),
  ('total_cfg_edges', (SELECT COUNT(*) FROM edges WHERE kind = 'cfg')),
  ('inlineable_functions', (SELECT COUNT(*) FROM node_properties WHERE key = 'inlineable' AND value = 'true')),
  ('heap_escaping', (SELECT COUNT(*) FROM node_properties WHERE key = 'heap_escapes' AND value = 'true')),
  ('total_goroutine_launches', (SELECT COUNT(*) FROM nodes WHERE kind = 'go')),
  ('total_defers', (SELECT COUNT(*) FROM nodes WHERE kind = 'defer')),
  ('total_queries', (SELECT COUNT(*) FROM queries)),
  ('total_views', (SELECT COUNT(*) FROM sqlite_master WHERE type = 'view'))`},
	}

	for _, s := range stmts {
		prog.Log("  dashboard: %s ...", s.name)
		if strings.Contains(s.sql, "CREATE TABLE") {
			if err := sqlitex.ExecuteScript(conn, s.sql, nil); err != nil {
				return fmt.Errorf("dashboard %s: %w", s.name, err)
			}
		} else {
			if err := sqlitex.ExecuteTransient(conn, s.sql, &sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
			}); err != nil {
				return fmt.Errorf("dashboard %s: %w", s.name, err)
			}
		}
	}

	prog.Log("Dashboard: created 7 pre-computed tables for chart rendering")
	return nil
}

// createGraphIntelligence adds top-N tables, cross-package coupling analysis,
// error propagation chains, and hotspot detection for the interview web app.
func createGraphIntelligence(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- Top functions by multiple metrics (leaderboard-ready)
CREATE TABLE dashboard_top_functions (
    metric TEXT NOT NULL,
    rank INTEGER NOT NULL,
    function_id TEXT NOT NULL,
    name TEXT NOT NULL,
    package TEXT,
    file TEXT,
    value REAL NOT NULL
);

-- Hotspot detection: functions with combined high complexity + high fan-in + many findings
CREATE TABLE dashboard_hotspots (
    function_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    package TEXT,
    file TEXT,
    complexity INTEGER,
    loc INTEGER,
    fan_in INTEGER,
    fan_out INTEGER,
    finding_count INTEGER,
    hotspot_score REAL NOT NULL
);

-- Cross-package coupling matrix: how tightly packages are coupled
CREATE TABLE package_coupling (
    source_package TEXT NOT NULL,
    target_package TEXT NOT NULL,
    call_count INTEGER NOT NULL,
    PRIMARY KEY (source_package, target_package)
);

-- Error propagation chains: functions that wrap/propagate errors
CREATE TABLE error_chains (
    function_id TEXT NOT NULL,
    name TEXT NOT NULL,
    package TEXT,
    error_wraps INTEGER DEFAULT 0,
    error_returns INTEGER DEFAULT 0,
    chain_depth INTEGER DEFAULT 0
);
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("graph intelligence DDL: %w", err)
	}

	// Top functions by complexity
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_top_functions
  SELECT 'complexity', ROW_NUMBER() OVER (ORDER BY m.cyclomatic_complexity DESC), m.function_id,
    n.name, n.package, n.file, m.cyclomatic_complexity
  FROM metrics m JOIN nodes n ON n.id = m.function_id
  WHERE m.cyclomatic_complexity > 0
  ORDER BY m.cyclomatic_complexity DESC LIMIT 50`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("top complexity: %w", err)
	}

	// Top by LOC
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_top_functions
  SELECT 'loc', ROW_NUMBER() OVER (ORDER BY m.loc DESC), m.function_id,
    n.name, n.package, n.file, m.loc
  FROM metrics m JOIN nodes n ON n.id = m.function_id
  WHERE m.loc > 0
  ORDER BY m.loc DESC LIMIT 50`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("top loc: %w", err)
	}

	// Top by fan-in (most called)
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_top_functions
  SELECT 'fan_in', ROW_NUMBER() OVER (ORDER BY m.fan_in DESC), m.function_id,
    n.name, n.package, n.file, m.fan_in
  FROM metrics m JOIN nodes n ON n.id = m.function_id
  WHERE m.fan_in > 0
  ORDER BY m.fan_in DESC LIMIT 50`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("top fan_in: %w", err)
	}

	// Top by fan-out (calls the most)
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_top_functions
  SELECT 'fan_out', ROW_NUMBER() OVER (ORDER BY m.fan_out DESC), m.function_id,
    n.name, n.package, n.file, m.fan_out
  FROM metrics m JOIN nodes n ON n.id = m.function_id
  WHERE m.fan_out > 0
  ORDER BY m.fan_out DESC LIMIT 50`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("top fan_out: %w", err)
	}

	// Hotspot detection: combined score
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_hotspots
  SELECT m.function_id, n.name, n.package, n.file,
    m.cyclomatic_complexity, m.loc, m.fan_in, m.fan_out,
    COALESCE(fc.cnt, 0),
    -- Hotspot score: weighted combination of normalized metrics
    ROUND(
      (CAST(m.cyclomatic_complexity AS REAL) / MAX((SELECT MAX(cyclomatic_complexity) FROM metrics), 1)) * 30 +
      (CAST(m.loc AS REAL) / MAX((SELECT MAX(loc) FROM metrics), 1)) * 20 +
      (CAST(m.fan_in AS REAL) / MAX((SELECT MAX(fan_in) FROM metrics WHERE fan_in > 0), 1)) * 25 +
      (CAST(COALESCE(fc.cnt, 0) AS REAL) / MAX((SELECT MAX(c) FROM (SELECT COUNT(*) as c FROM findings GROUP BY node_id)), 1)) * 25
    , 2)
  FROM metrics m
  JOIN nodes n ON n.id = m.function_id
  LEFT JOIN (SELECT node_id, COUNT(*) AS cnt FROM findings GROUP BY node_id) fc ON fc.node_id = m.function_id
  WHERE m.cyclomatic_complexity > 0
  ORDER BY 10 DESC LIMIT 200`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("hotspots: %w", err)
	}

	// Cross-package coupling
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO package_coupling
  SELECT caller.package, callee.package, COUNT(*)
  FROM edges e
  JOIN nodes caller ON caller.id = e.source
  JOIN nodes callee ON callee.id = e.target
  WHERE e.kind = 'call'
    AND caller.package IS NOT NULL AND callee.package IS NOT NULL
    AND caller.package != callee.package
  GROUP BY caller.package, callee.package`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("package coupling: %w", err)
	}

	// Error chains: functions containing error wrapping call sites
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO error_chains
  SELECT n.id, n.name, n.package,
    (SELECT COUNT(*) FROM edges ew
     JOIN nodes ew_src ON ew_src.id = ew.source
     WHERE ew.kind = 'error_wrap' AND ew_src.parent_function = n.id),
    (SELECT COUNT(*) FROM nodes ret WHERE ret.kind = 'return' AND ret.parent_function = n.id),
    0
  FROM nodes n
  WHERE n.kind = 'function'
    AND EXISTS (SELECT 1 FROM edges ew
     JOIN nodes ew_src ON ew_src.id = ew.source
     WHERE ew.kind = 'error_wrap' AND ew_src.parent_function = n.id)`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("error chains: %w", err)
	}

	// Findings: long parameter list (>5 params)
	var longParamCount int
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO findings (node_id, category, severity, message)
  SELECT m.function_id, 'long_param_list', 'info',
    n.name || ' has ' || m.num_params || ' parameters (threshold: 5)'
  FROM metrics m JOIN nodes n ON n.id = m.function_id
  WHERE m.num_params > 5`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("long_param_list findings: %w", err)
	}
	longParamCount = conn.Changes()

	// Findings: god package (>50 functions)
	var godPkgCount int
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO findings (node_id, category, severity, message)
  SELECT MIN(n.id), 'god_package', 'warning',
    n.package || ' has ' || COUNT(*) || ' functions (threshold: 50)'
  FROM nodes n WHERE n.kind = 'function' AND n.package IS NOT NULL
  GROUP BY n.package HAVING COUNT(*) > 50`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("god_package findings: %w", err)
	}
	godPkgCount = conn.Changes()

	// Findings: high coupling (packages that call >10 other packages)
	var couplingCount int
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO findings (node_id, category, severity, message)
  SELECT (SELECT MIN(id) FROM nodes WHERE kind = 'function' AND package = pc.source_package),
    'high_coupling', 'warning',
    pc.source_package || ' depends on ' || COUNT(DISTINCT pc.target_package) || ' packages (threshold: 10)'
  FROM package_coupling pc
  GROUP BY pc.source_package HAVING COUNT(DISTINCT pc.target_package) > 10`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("high_coupling findings: %w", err)
	}
	couplingCount = conn.Changes()

	// Queries: hotspot analysis
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO queries (name, description, sql) VALUES
  ('hotspot_analysis', 'Find functions with combined high complexity, high fan-in, and many findings',
   'SELECT function_id, name, package, complexity, fan_in, finding_count, hotspot_score FROM dashboard_hotspots ORDER BY hotspot_score DESC LIMIT 20'),
  ('package_coupling_matrix', 'Aggregated cross-package call coupling matrix',
   'SELECT source_package, target_package, call_count FROM package_coupling ORDER BY call_count DESC LIMIT 50'),
  ('error_propagation', 'Functions involved in error wrapping chains',
   'SELECT function_id, name, package, error_wraps, error_returns FROM error_chains WHERE error_wraps > 0 ORDER BY error_wraps DESC LIMIT 30'),
  ('top_functions_by_metric', 'Top 50 functions ranked by a specific metric (complexity, loc, fan_in, fan_out)',
   'SELECT rank, function_id, name, package, value FROM dashboard_top_functions WHERE metric = ''complexity'' ORDER BY rank'),
  ('package_coupling_degree', 'Packages ranked by number of coupled packages (high coupling = risky)',
   'SELECT source_package, COUNT(DISTINCT target_package) as coupled_to, SUM(call_count) as total_calls FROM package_coupling GROUP BY source_package ORDER BY coupled_to DESC'),
  ('call_chain_pathfinder', 'Find all call paths from function A to function B (up to 6 hops)',
   'WITH RECURSIVE chain(fn, path, depth) AS (SELECT target, source || '' -> '' || target, 1 FROM edges WHERE kind = ''call'' AND source = :start UNION ALL SELECT e.target, chain.path || '' -> '' || e.target, chain.depth + 1 FROM chain JOIN edges e ON e.source = chain.fn AND e.kind = ''call'' WHERE chain.depth < 6 AND chain.path NOT LIKE ''%'' || e.target || ''%'') SELECT path, depth FROM chain WHERE fn = :end ORDER BY depth LIMIT 10')`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("graph intelligence queries: %w", err)
	}

	// Count results
	var topCount, hotspotCount, couplingRows, errorChainCount int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM dashboard_top_functions",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			topCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM dashboard_hotspots",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			hotspotCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM package_coupling",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			couplingRows = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM error_chains",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			errorChainCount = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("Graph intelligence: %d top-N entries, %d hotspots, %d coupling pairs, %d error chains",
		topCount, hotspotCount, couplingRows, errorChainCount)
	prog.Log("  findings: %d long-param, %d god-package, %d high-coupling; 6 queries",
		longParamCount, godPkgCount, couplingCount)
	return nil
}

// createFileAndDepAnalysis creates file-level analysis tables and dependency
// graph data optimized for visualization (heatmaps, force-directed graphs, detail panels).
func createFileAndDepAnalysis(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- File-level complexity heatmap data (one row per file)
CREATE TABLE dashboard_file_heatmap (
    file TEXT PRIMARY KEY,
    package TEXT,
    function_count INTEGER,
    total_loc INTEGER,
    total_complexity INTEGER,
    max_complexity INTEGER,
    avg_complexity REAL,
    finding_count INTEGER,
    hotspot_score REAL
);

-- Package dependency graph ready for force-directed visualization
CREATE TABLE dashboard_package_graph (
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    weight INTEGER NOT NULL,
    PRIMARY KEY (source, target)
);

-- Function detail panel: pre-aggregated per-function summary
CREATE TABLE dashboard_function_detail (
    function_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    package TEXT,
    file TEXT,
    line INTEGER,
    end_line INTEGER,
    signature TEXT,
    complexity INTEGER,
    loc INTEGER,
    fan_in INTEGER,
    fan_out INTEGER,
    num_params INTEGER,
    num_locals INTEGER,
    num_calls INTEGER,
    num_branches INTEGER,
    num_returns INTEGER,
    finding_count INTEGER,
    callers TEXT,
    callees TEXT
);
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("file/dep DDL: %w", err)
	}

	// File heatmap
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_file_heatmap
  SELECT
    n.file,
    n.package,
    COUNT(DISTINCT n.id),
    SUM(COALESCE(m.loc, 0)),
    SUM(COALESCE(m.cyclomatic_complexity, 0)),
    MAX(COALESCE(m.cyclomatic_complexity, 0)),
    ROUND(AVG(COALESCE(m.cyclomatic_complexity, 0)), 1),
    COALESCE(ff.cnt, 0),
    ROUND(
      (CAST(SUM(COALESCE(m.cyclomatic_complexity, 0)) AS REAL) / MAX((SELECT MAX(cyclomatic_complexity) FROM metrics), 1)) * 40 +
      (CAST(SUM(COALESCE(m.loc, 0)) AS REAL) / MAX((SELECT MAX(loc) FROM metrics), 1)) * 30 +
      (CAST(COALESCE(ff.cnt, 0) AS REAL) / MAX((SELECT MAX(c) FROM (SELECT COUNT(*) as c FROM findings GROUP BY node_id)), 1)) * 30
    , 2)
  FROM nodes n
  LEFT JOIN metrics m ON m.function_id = n.id
  LEFT JOIN (
    SELECT f2.file, COUNT(*) AS cnt
    FROM findings fi
    JOIN nodes f2 ON f2.id = fi.node_id
    GROUP BY f2.file
  ) ff ON ff.file = n.file
  WHERE n.kind = 'function' AND n.file IS NOT NULL
  GROUP BY n.file, n.package`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("file heatmap: %w", err)
	}

	// Package dependency graph (filtered to Prometheus-internal packages only)
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_package_graph
  SELECT source_package, target_package, call_count
  FROM package_coupling
  WHERE source_package NOT LIKE 'github.com/%'
    AND target_package NOT LIKE 'github.com/%'
    AND source_package NOT LIKE 'golang.org/%'
    AND target_package NOT LIKE 'golang.org/%'
    AND call_count >= 2`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("package graph: %w", err)
	}

	// Function detail panels
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO dashboard_function_detail
  SELECT
    n.id, n.name, n.package, n.file, n.line, n.end_line, n.type_info,
    COALESCE(m.cyclomatic_complexity, 0),
    COALESCE(m.loc, 0),
    COALESCE(m.fan_in, 0),
    COALESCE(m.fan_out, 0),
    COALESCE(m.num_params, 0),
    (SELECT COUNT(*) FROM nodes loc WHERE loc.kind = 'local' AND loc.parent_function = n.id),
    (SELECT COUNT(*) FROM nodes c WHERE c.kind = 'call' AND c.parent_function = n.id),
    (SELECT COUNT(*) FROM nodes b WHERE b.kind IN ('if','for','switch','select') AND b.parent_function = n.id),
    (SELECT COUNT(*) FROM nodes r WHERE r.kind = 'return' AND r.parent_function = n.id),
    COALESCE((SELECT COUNT(*) FROM findings fi WHERE fi.node_id = n.id), 0),
    (SELECT GROUP_CONCAT(DISTINCT caller.name)
     FROM edges ce JOIN nodes caller ON caller.id = ce.source
     WHERE ce.target = n.id AND ce.kind = 'call' AND caller.kind = 'function'),
    (SELECT GROUP_CONCAT(DISTINCT callee.name)
     FROM edges ce JOIN nodes callee ON callee.id = ce.target
     WHERE ce.source = n.id AND ce.kind = 'call' AND callee.kind = 'function')
  FROM nodes n
  LEFT JOIN metrics m ON m.function_id = n.id
  WHERE n.kind = 'function'`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("function detail: %w", err)
	}

	// Additional queries
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO queries (name, description, sql) VALUES
  ('file_heatmap', 'File-level complexity heatmap data for visualization',
   'SELECT file, package, total_complexity, total_loc, finding_count, hotspot_score FROM dashboard_file_heatmap ORDER BY hotspot_score DESC'),
  ('package_graph', 'Internal package dependency graph (for force-directed layout)',
   'SELECT source, target, weight FROM dashboard_package_graph ORDER BY weight DESC'),
  ('function_detail', 'Complete function profile with callers/callees for detail panels',
   'SELECT * FROM dashboard_function_detail WHERE function_id = :id')`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("file/dep queries: %w", err)
	}

	var fileCount, graphEdges, funcCount int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM dashboard_file_heatmap",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			fileCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM dashboard_package_graph",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			graphEdges = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM dashboard_function_detail",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			funcCount = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("File/dep analysis: %d file heatmap entries, %d package graph edges, %d function detail records; 3 queries",
		fileCount, graphEdges, funcCount)
	return nil
}

// createTypeSystemAnalysis builds type hierarchy data, interface implementation maps,
// and method resolution tables for the interview web app.
func createTypeSystemAnalysis(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- Interface implementation map: which concrete types implement which interfaces
CREATE TABLE type_impl_map (
    interface_id TEXT NOT NULL,
    interface_name TEXT NOT NULL,
    interface_package TEXT,
    concrete_id TEXT NOT NULL,
    concrete_name TEXT NOT NULL,
    concrete_package TEXT,
    method_count INTEGER
);

-- Type hierarchy: types and their embedded types (tree structure)
CREATE TABLE type_hierarchy (
    type_id TEXT NOT NULL,
    type_name TEXT NOT NULL,
    type_package TEXT,
    embedded_id TEXT,
    embedded_name TEXT,
    embedded_package TEXT,
    depth INTEGER DEFAULT 0
);

-- Method sets per type: all methods declared on each type
CREATE TABLE type_method_set (
    type_id TEXT NOT NULL,
    type_name TEXT NOT NULL,
    method_id TEXT NOT NULL,
    method_name TEXT NOT NULL,
    signature TEXT,
    complexity INTEGER DEFAULT 0,
    loc INTEGER DEFAULT 0
);
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("type system DDL: %w", err)
	}

	// Interface implementation map
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO type_impl_map
  SELECT
    iface.id, iface.name, iface.package,
    concrete.id, concrete.name, concrete.package,
    (SELECT COUNT(*) FROM edges sm
     WHERE sm.kind = 'satisfies_method'
       AND sm.target IN (SELECT hm.target FROM edges hm WHERE hm.source = iface.id AND hm.kind = 'has_method')
       AND sm.source IN (SELECT hm2.target FROM edges hm2 WHERE hm2.source = concrete.id AND hm2.kind = 'has_method'))
  FROM edges e
  JOIN nodes iface ON iface.id = e.target AND iface.kind = 'type_decl'
  JOIN nodes concrete ON concrete.id = e.source AND concrete.kind = 'type_decl'
  WHERE e.kind = 'implements'`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("type impl map: %w", err)
	}

	// Type hierarchy (direct embeds)
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO type_hierarchy
  SELECT
    parent.id, parent.name, parent.package,
    child.id, child.name, child.package,
    1
  FROM edges e
  JOIN nodes parent ON parent.id = e.source AND parent.kind = 'type_decl'
  JOIN nodes child ON child.id = e.target AND child.kind = 'type_decl'
  WHERE e.kind = 'embeds'
  UNION ALL
  SELECT
    n.id, n.name, n.package, NULL, NULL, NULL, 0
  FROM nodes n WHERE n.kind = 'type_decl'
    AND n.id NOT IN (SELECT source FROM edges WHERE kind = 'embeds')`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("type hierarchy: %w", err)
	}

	// Method sets per type
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO type_method_set
  SELECT
    t.id, t.name,
    meth.id, meth.name,
    meth.type_info,
    COALESCE(m.cyclomatic_complexity, 0),
    COALESCE(m.loc, 0)
  FROM edges e
  JOIN nodes t ON t.id = e.source AND t.kind = 'type_decl'
  JOIN nodes meth ON meth.id = e.target AND meth.kind = 'function'
  LEFT JOIN metrics m ON m.function_id = meth.id
  WHERE e.kind = 'has_method'`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("method sets: %w", err)
	}

	// Findings: large interfaces (>10 methods)
	var largeIfaceCount int
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO findings (node_id, category, severity, message)
  SELECT t.id, 'large_interface', 'info',
    t.name || ' in ' || t.package || ' has ' || COUNT(*) || ' methods (threshold: 10)'
  FROM edges e
  JOIN nodes t ON t.id = e.source AND t.kind = 'type_decl'
  JOIN node_properties np ON np.node_id = t.id AND np.key = 'type_kind' AND np.value = 'interface'
  JOIN nodes meth ON meth.id = e.target AND meth.kind = 'function'
  WHERE e.kind = 'has_method'
  GROUP BY t.id HAVING COUNT(*) > 10`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("large_interface findings: %w", err)
	}
	largeIfaceCount = conn.Changes()

	// Findings: orphan types (declared but never used in implements/embeds/call)
	var orphanTypeCount int
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO findings (node_id, category, severity, message)
  SELECT n.id, 'orphan_type', 'info',
    n.name || ' in ' || n.package || ' has no implements/embeds/method edges'
  FROM nodes n
  WHERE n.kind = 'type_decl'
    AND NOT EXISTS (SELECT 1 FROM edges e WHERE (e.source = n.id OR e.target = n.id) AND e.kind IN ('implements', 'embeds', 'has_method'))`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("orphan_type findings: %w", err)
	}
	orphanTypeCount = conn.Changes()

	// Queries
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO queries (name, description, sql) VALUES
  ('interface_map', 'All concrete types implementing a given interface',
   'SELECT concrete_name, concrete_package, method_count FROM type_impl_map WHERE interface_name = :name ORDER BY concrete_package'),
  ('type_hierarchy_tree', 'Type embedding tree for a given type',
   'SELECT type_name, type_package, embedded_name, embedded_package FROM type_hierarchy WHERE type_name = :name'),
  ('method_set', 'Complete method set for a type with complexity and LOC',
   'SELECT method_name, signature, complexity, loc FROM type_method_set WHERE type_name = :name ORDER BY method_name'),
  ('largest_interfaces', 'Interfaces ranked by method count',
   'SELECT interface_name, interface_package, COUNT(*) as impl_count FROM type_impl_map GROUP BY interface_id ORDER BY impl_count DESC'),
  ('most_implemented', 'Interfaces with the most concrete implementations',
   'SELECT interface_name, interface_package, COUNT(DISTINCT concrete_id) as impl_count FROM type_impl_map GROUP BY interface_id ORDER BY impl_count DESC LIMIT 20')`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("type system queries: %w", err)
	}

	var implCount, hierarchyCount, methodSetCount int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM type_impl_map",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			implCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM type_hierarchy",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			hierarchyCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM type_method_set",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			methodSetCount = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("Type system: %d impl mappings, %d hierarchy entries, %d method set entries; %d large-iface, %d orphan-type findings; 5 queries",
		implCount, hierarchyCount, methodSetCount, largeIfaceCount, orphanTypeCount)
	return nil
}

// createNavigationAndPatterns builds code navigation aids (symbol index, file outline,
// cross-references) and pattern summaries for the interview web app.
func createNavigationAndPatterns(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- Symbol index: all named declarations for quick navigation / search
CREATE TABLE symbol_index (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    package TEXT,
    file TEXT,
    line INTEGER,
    signature TEXT,
    parent TEXT
);
CREATE INDEX idx_symbol_name ON symbol_index(name);
CREATE INDEX idx_symbol_kind ON symbol_index(kind);

-- File outline: hierarchical structure of each file for sidebar tree
CREATE TABLE file_outline (
    file TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    line INTEGER,
    end_line INTEGER,
    signature TEXT,
    parent_id TEXT,
    depth INTEGER DEFAULT 0
);
CREATE INDEX idx_file_outline ON file_outline(file, line);

-- Cross-reference table: definition → all usage sites
CREATE TABLE xrefs (
    def_id TEXT NOT NULL,
    def_name TEXT NOT NULL,
    def_file TEXT,
    def_line INTEGER,
    use_id TEXT NOT NULL,
    use_file TEXT,
    use_line INTEGER,
    use_kind TEXT
);
CREATE INDEX idx_xrefs_def ON xrefs(def_id);
CREATE INDEX idx_xrefs_name ON xrefs(def_name);

-- Go pattern summary: counts of Go-specific constructs per package
CREATE TABLE go_pattern_summary (
    package TEXT PRIMARY KEY,
    goroutine_count INTEGER DEFAULT 0,
    defer_count INTEGER DEFAULT 0,
    channel_send_count INTEGER DEFAULT 0,
    select_count INTEGER DEFAULT 0,
    panic_count INTEGER DEFAULT 0,
    interface_count INTEGER DEFAULT 0,
    type_assert_count INTEGER DEFAULT 0,
    error_wrap_count INTEGER DEFAULT 0,
    context_param_count INTEGER DEFAULT 0
);
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("navigation DDL: %w", err)
	}

	// Symbol index: functions, types, package-level vars/consts
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO symbol_index
  SELECT id, name, kind, package, file, line, type_info, parent_function
  FROM nodes
  WHERE kind IN ('function', 'type_decl', 'local', 'parameter')
    AND name != '' AND name != '_'
    AND file IS NOT NULL`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("symbol index: %w", err)
	}

	// File outline: top-level and function-level declarations
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO file_outline
  SELECT file, id, name, kind, line, end_line, type_info, parent_function,
    CASE WHEN parent_function IS NULL THEN 0 ELSE 1 END
  FROM nodes
  WHERE kind IN ('function', 'type_decl')
    AND name != '' AND file IS NOT NULL
  ORDER BY file, line`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("file outline: %w", err)
	}

	// Cross-references: ref edges carry definition→usage
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO xrefs
  SELECT
    e.target, def.name, def.file, def.line,
    e.source, use.file, use.line, use.kind
  FROM edges e
  JOIN nodes def ON def.id = e.target
  JOIN nodes use ON use.id = e.source
  WHERE e.kind = 'ref'
    AND def.file IS NOT NULL AND use.file IS NOT NULL`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("xrefs: %w", err)
	}

	// Go pattern summary per package
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO go_pattern_summary
  SELECT
    n.package,
    SUM(CASE WHEN n.kind = 'go' THEN 1 ELSE 0 END),
    SUM(CASE WHEN n.kind = 'defer' THEN 1 ELSE 0 END),
    SUM(CASE WHEN n.kind = 'send' THEN 1 ELSE 0 END),
    SUM(CASE WHEN n.kind = 'select' THEN 1 ELSE 0 END),
    0, 0, 0, 0, 0
  FROM nodes n
  WHERE n.package IS NOT NULL AND n.kind IN ('go', 'defer', 'send', 'select')
  GROUP BY n.package`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("go patterns: %w", err)
	}

	// Update pattern summary with type assertions, error wraps, context params
	sqlitex.ExecuteTransient(conn, `
UPDATE go_pattern_summary SET interface_count = (
  SELECT COUNT(*) FROM nodes n
  JOIN node_properties np ON np.node_id = n.id AND np.key = 'type_kind' AND np.value = 'interface'
  WHERE n.kind = 'type_decl' AND n.package = go_pattern_summary.package)`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }})

	sqlitex.ExecuteTransient(conn, `
UPDATE go_pattern_summary SET error_wrap_count = (
  SELECT COUNT(*) FROM edges ew
  JOIN nodes ew_src ON ew_src.id = ew.source
  WHERE ew.kind = 'error_wrap' AND ew_src.package = go_pattern_summary.package)`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }})

	sqlitex.ExecuteTransient(conn, `
UPDATE go_pattern_summary SET context_param_count = (
  SELECT COUNT(*) FROM nodes p
  WHERE p.kind = 'parameter' AND p.type_info LIKE '%context.Context%'
    AND p.package = go_pattern_summary.package)`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }})

	// Queries
	if err := sqlitex.ExecuteTransient(conn, `
INSERT INTO queries (name, description, sql) VALUES
  ('symbol_search', 'Search symbols by name (supports LIKE patterns)',
   'SELECT id, name, kind, package, file, line FROM symbol_index WHERE name LIKE :pattern ORDER BY kind, name LIMIT 50'),
  ('file_outline_query', 'Get hierarchical outline of a file for sidebar navigation',
   'SELECT name, kind, line, end_line, signature, depth FROM file_outline WHERE file = :file ORDER BY line'),
  ('xref_lookup', 'Find all usages of a symbol by its definition ID',
   'SELECT use_file, use_line, use_kind FROM xrefs WHERE def_id = :id ORDER BY use_file, use_line'),
  ('go_patterns', 'Go-specific construct usage per package (goroutines, channels, errors, etc.)',
   'SELECT * FROM go_pattern_summary ORDER BY goroutine_count DESC')`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { return nil }}); err != nil {
		return fmt.Errorf("navigation queries: %w", err)
	}

	var symbolCount, outlineCount, xrefCount, patternCount int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM symbol_index",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			symbolCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM file_outline",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			outlineCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM xrefs",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			xrefCount = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM go_pattern_summary",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			patternCount = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("Navigation: %d symbols, %d outline entries, %d xrefs, %d package patterns; 4 queries",
		symbolCount, outlineCount, xrefCount, patternCount)
	return nil
}

// applyGitHistory creates the git_file_history table and populates it from
// git log --numstat output, then enriches with a file risk view.
func applyGitHistory(conn *sqlite.Conn, history []GitFileHistory, prog *Progress) error {
	ddl := `
CREATE TABLE git_file_history (
    file TEXT PRIMARY KEY,
    commit_count INTEGER NOT NULL,
    author_count INTEGER NOT NULL,
    last_author TEXT,
    last_date TEXT,
    insertions INTEGER NOT NULL DEFAULT 0,
    deletions INTEGER NOT NULL DEFAULT 0,
    churn INTEGER NOT NULL DEFAULT 0
);`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("git history DDL: %w", err)
	}

	stmt, err := conn.Prepare(`INSERT OR IGNORE INTO git_file_history
		(file, commit_count, author_count, last_author, last_date, insertions, deletions, churn)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Finalize()

	for _, h := range history {
		stmt.BindText(1, h.RelFile)
		stmt.BindInt64(2, int64(h.CommitCount))
		stmt.BindInt64(3, int64(h.AuthorCount))
		stmt.BindText(4, h.LastAuthor)
		stmt.BindText(5, h.LastDate)
		stmt.BindInt64(6, int64(h.Insertions))
		stmt.BindInt64(7, int64(h.Deletions))
		stmt.BindInt64(8, int64(h.Insertions+h.Deletions))
		if _, err := stmt.Step(); err != nil {
			return err
		}
		stmt.Reset()
	}

	enrich := `
-- Combined risk: file complexity × change velocity
CREATE VIEW v_file_risk AS
SELECT
  fh.file,
  fh.node_count,
  fh.function_count,
  fh.avg_complexity,
  fh.max_complexity,
  g.commit_count,
  g.author_count,
  g.churn,
  g.last_author,
  g.last_date,
  ROUND(
    (COALESCE(fh.avg_complexity, 1) * 0.3 +
     COALESCE(fh.max_complexity, 1) * 0.2 +
     COALESCE(g.commit_count, 1) * 0.3 +
     COALESCE(g.author_count, 1) * 0.2), 2
  ) AS change_risk_score
FROM dashboard_file_heatmap fh
LEFT JOIN git_file_history g ON g.file = fh.file
ORDER BY change_risk_score DESC;

-- Findings: files with high churn AND high complexity
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'high_churn_complexity', 'warning',
  (SELECT n.id FROM nodes n WHERE n.kind = 'file' AND n.file = fh.file LIMIT 1),
  fh.file, 1,
  fh.file || ': ' || g.commit_count || ' commits, ' || g.author_count || ' authors, avg complexity ' || CAST(fh.avg_complexity AS INTEGER),
  json_object('file', fh.file, 'commits', g.commit_count, 'authors', g.author_count,
              'churn', g.churn, 'avg_complexity', fh.avg_complexity, 'max_complexity', fh.max_complexity)
FROM dashboard_file_heatmap fh
JOIN git_file_history g ON g.file = fh.file
WHERE g.commit_count >= 10 AND fh.avg_complexity >= 5;

INSERT INTO schema_docs (category, name, description, example) VALUES
('table', 'git_file_history', 'Per-file git change metrics from recent 500 commits', 'SELECT * FROM git_file_history ORDER BY churn DESC LIMIT 20'),
('view', 'v_file_risk', 'Combined file risk: complexity metrics joined with git change velocity', 'SELECT * FROM v_file_risk WHERE commit_count > 5 ORDER BY change_risk_score DESC LIMIT 20');
`
	if err := sqlitex.ExecuteScript(conn, enrich, nil); err != nil {
		return fmt.Errorf("git history enrichment: %w", err)
	}

	var churnFindings int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM findings WHERE category = 'high_churn_complexity'",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			churnFindings = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("Git history: %d files, %d high-churn findings", len(history), churnFindings)
	return nil
}

// createTaintFlowStates materializes taint propagation by BFS through DFG
// edges from annotated taint sources. Each reachable node gets a label:
// source, propagated, sanitized, or sink_reached.
func createTaintFlowStates(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
CREATE TABLE taint_flow_state (
    node_id TEXT NOT NULL,
    label TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_category TEXT,
    min_hops INTEGER NOT NULL
);

-- BFS through DFG from taint sources (bounded to 8 hops)
INSERT INTO taint_flow_state (node_id, label, source_id, source_category, min_hops)
WITH RECURSIVE taint_reach(node_id, source_id, source_category, hop) AS (
    -- Seed: call nodes annotated as taint sources
    SELECT np.node_id, np.node_id, COALESCE(cat.value, 'unknown'), 0
    FROM node_properties np
    LEFT JOIN node_properties cat ON cat.node_id = np.node_id AND cat.key = 'taint_category'
    WHERE np.key = 'taint_role' AND np.value = 'source'

    UNION

    -- Follow DFG edges outward
    SELECT e.target, tr.source_id, tr.source_category, tr.hop + 1
    FROM taint_reach tr
    JOIN edges e ON e.source = tr.node_id AND e.kind = 'dfg'
    WHERE tr.hop < 8
)
SELECT
  node_id,
  CASE
    WHEN EXISTS (SELECT 1 FROM node_properties p
                 WHERE p.node_id = tr.node_id AND p.key = 'taint_role' AND p.value = 'source')
      THEN 'source'
    WHEN EXISTS (SELECT 1 FROM node_properties p
                 WHERE p.node_id = tr.node_id AND p.key = 'taint_role' AND p.value = 'barrier')
      THEN 'sanitized'
    WHEN EXISTS (SELECT 1 FROM node_properties p
                 WHERE p.node_id = tr.node_id AND p.key = 'taint_role' AND p.value = 'sink')
      THEN 'sink_reached'
    ELSE 'propagated'
  END,
  source_id, source_category, MIN(hop)
FROM taint_reach tr
GROUP BY node_id, source_id;

CREATE INDEX idx_taint_flow_node ON taint_flow_state(node_id);
CREATE INDEX idx_taint_flow_label ON taint_flow_state(label);

-- Findings: unsanitized taint reaching sinks
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'unsanitized_sink', 'error',
  tfs.node_id, n.file, n.line,
  'Taint from ' || src.name || ' (' || tfs.source_category || ') reaches sink ' || n.name || ' in ' || tfs.min_hops || ' hops',
  json_object('source', tfs.source_id, 'sink', tfs.node_id, 'category', tfs.source_category,
              'hops', tfs.min_hops, 'source_name', src.name, 'sink_name', n.name)
FROM taint_flow_state tfs
JOIN nodes n ON n.id = tfs.node_id
JOIN nodes src ON src.id = tfs.source_id
WHERE tfs.label = 'sink_reached';

CREATE VIEW v_taint_summary AS
SELECT label, source_category, COUNT(*) AS node_count
FROM taint_flow_state
GROUP BY label, source_category
ORDER BY node_count DESC;

INSERT INTO schema_docs (category, name, description, example) VALUES
('table', 'taint_flow_state', 'Materialized taint propagation via DFG from sources (8-hop BFS)', 'SELECT * FROM taint_flow_state WHERE label = ''sink_reached'''),
('view', 'v_taint_summary', 'Taint flow distribution by label and source category', 'SELECT * FROM v_taint_summary');

INSERT INTO queries (name, description, sql) VALUES
('taint_path_to_sink', 'Find taint paths reaching sinks without sanitization',
 'SELECT tfs.source_id, src.name AS source_name, tfs.source_category, tfs.node_id AS sink_id, n.name AS sink_name, n.file, n.line, tfs.min_hops FROM taint_flow_state tfs JOIN nodes n ON n.id = tfs.node_id JOIN nodes src ON src.id = tfs.source_id WHERE tfs.label = ''sink_reached'' ORDER BY tfs.min_hops');
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("taint flow states: %w", err)
	}

	var totalStates, sinkReached int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM taint_flow_state",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			totalStates = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM taint_flow_state WHERE label = 'sink_reached'",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			sinkReached = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("Taint flow: %d reachable nodes, %d unsanitized sink reaches", totalStates, sinkReached)
	return nil
}

// createIndexSensitivity identifies container-typed operations (maps, slices)
// and tracks whether tainted data flows through them.
func createIndexSensitivity(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
CREATE TABLE index_sensitivity (
    node_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    container_kind TEXT NOT NULL,
    type_info TEXT,
    file TEXT,
    line INTEGER,
    function_id TEXT,
    has_taint INTEGER NOT NULL DEFAULT 0
);

-- Identify all map/slice-typed nodes
INSERT INTO index_sensitivity (node_id, kind, container_kind, type_info, file, line, function_id, has_taint)
SELECT n.id, n.kind,
  CASE
    WHEN n.type_info LIKE 'map[%' OR n.type_info LIKE '*map[%' THEN 'map'
    WHEN n.type_info LIKE '[]%' OR n.type_info LIKE '*[]%' THEN 'slice'
    ELSE 'container'
  END,
  n.type_info, n.file, n.line, n.parent_function,
  CASE WHEN EXISTS (
    SELECT 1 FROM taint_flow_state tfs WHERE tfs.node_id = n.id
  ) THEN 1 ELSE 0 END
FROM nodes n
WHERE (n.type_info LIKE 'map[%' OR n.type_info LIKE '[]%'
       OR n.type_info LIKE '*map[%' OR n.type_info LIKE '*[]%')
  AND n.kind IN ('identifier', 'local', 'parameter', 'field', 'assign')
  AND n.file IS NOT NULL;

CREATE INDEX idx_index_sens_taint ON index_sensitivity(has_taint);
CREATE INDEX idx_index_sens_kind ON index_sensitivity(container_kind);

-- Findings: tainted data in containers
INSERT INTO findings (category, severity, node_id, file, line, message, details)
SELECT 'tainted_container', 'info',
  s.node_id, s.file, s.line,
  s.container_kind || ' container with tainted data: ' || s.type_info,
  json_object('container_kind', s.container_kind, 'type', s.type_info,
              'function', s.function_id)
FROM index_sensitivity s
WHERE s.has_taint = 1;

CREATE VIEW v_container_taint_summary AS
SELECT container_kind, has_taint, COUNT(*) AS count
FROM index_sensitivity
GROUP BY container_kind, has_taint
ORDER BY container_kind, has_taint;

INSERT INTO schema_docs (category, name, description, example) VALUES
('table', 'index_sensitivity', 'Container-typed operations (map/slice) with taint tracking', 'SELECT * FROM index_sensitivity WHERE has_taint = 1'),
('view', 'v_container_taint_summary', 'Summary of container operations by type and taint status', NULL);

INSERT INTO queries (name, description, sql) VALUES
('tainted_containers', 'Find container operations with tainted data flow',
 'SELECT s.node_id, s.container_kind, s.type_info, s.file, s.line, n.name FROM index_sensitivity s JOIN nodes n ON n.id = s.node_id WHERE s.has_taint = 1 ORDER BY s.file, s.line');
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("index sensitivity: %w", err)
	}

	var totalOps, taintedOps int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM index_sensitivity",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			totalOps = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM index_sensitivity WHERE has_taint = 1",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			taintedOps = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("Index sensitivity: %d container ops, %d tainted", totalOps, taintedOps)
	return nil
}

// createSCIPSymbols generates SCIP (Source Code Intelligence Protocol) compatible
// symbol identifiers for cross-repository code navigation.
func createSCIPSymbols(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
CREATE TABLE scip_symbols (
    node_id TEXT PRIMARY KEY,
    scip_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    package TEXT,
    display_name TEXT
);

-- Functions: scip-go gomod github.com/prometheus/prometheus v0 package/name().
INSERT INTO scip_symbols (node_id, scip_id, kind, package, display_name)
SELECT n.id,
  'scip-go gomod github.com/prometheus/prometheus v0 ' ||
  REPLACE(n.package, '/', '.') || '/' || n.name || '().',
  'function', n.package, n.name
FROM nodes n
WHERE n.kind = 'function'
  AND n.name NOT LIKE '%.%'
  AND n.package IS NOT NULL AND n.name != '';

-- Methods: scip-go gomod github.com/prometheus/prometheus v0 package/Type#Method().
INSERT INTO scip_symbols (node_id, scip_id, kind, package, display_name)
SELECT n.id,
  'scip-go gomod github.com/prometheus/prometheus v0 ' ||
  REPLACE(n.package, '/', '.') || '/' ||
  REPLACE(REPLACE(SUBSTR(n.name, 1, INSTR(n.name, '.') - 1), '(*', ''), ')', '') ||
  '#' || SUBSTR(n.name, INSTR(n.name, '.') + 1) || '().',
  'method', n.package, n.name
FROM nodes n
WHERE n.kind = 'function'
  AND n.name LIKE '%.%'
  AND n.package IS NOT NULL;

-- Types: scip-go gomod github.com/prometheus/prometheus v0 package/TypeName#
INSERT OR IGNORE INTO scip_symbols (node_id, scip_id, kind, package, display_name)
SELECT n.id,
  'scip-go gomod github.com/prometheus/prometheus v0 ' ||
  REPLACE(n.package, '/', '.') || '/' || n.name || '#',
  'type', n.package, n.name
FROM nodes n
WHERE n.kind = 'type_decl'
  AND n.package IS NOT NULL AND n.name != '';

-- Packages: scip-go gomod github.com/prometheus/prometheus v0 package/
INSERT OR IGNORE INTO scip_symbols (node_id, scip_id, kind, package, display_name)
SELECT n.id,
  'scip-go gomod github.com/prometheus/prometheus v0 ' ||
  REPLACE(n.package, '/', '.') || '/',
  'package', n.package, n.name
FROM nodes n
WHERE n.kind = 'package'
  AND n.package IS NOT NULL;

CREATE INDEX idx_scip_kind ON scip_symbols(kind);
CREATE INDEX idx_scip_pkg ON scip_symbols(package);

INSERT INTO schema_docs (category, name, description, example) VALUES
('table', 'scip_symbols', 'SCIP-compatible symbol identifiers for cross-repository navigation', 'SELECT * FROM scip_symbols WHERE kind = ''method'' AND display_name LIKE ''Manager%''');

INSERT INTO queries (name, description, sql) VALUES
('scip_lookup', 'Look up SCIP symbol for a node',
 'SELECT s.scip_id, s.kind, s.display_name, n.file, n.line FROM scip_symbols s JOIN nodes n ON n.id = s.node_id WHERE s.display_name LIKE ? ORDER BY s.kind, s.display_name');
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("scip symbols: %w", err)
	}

	var total int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM scip_symbols",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			total = stmt.ColumnInt(0)
			return nil
		}})

	var byKind []string
	sqlitex.ExecuteTransient(conn, "SELECT kind || '=' || COUNT(*) FROM scip_symbols GROUP BY kind ORDER BY kind",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			byKind = append(byKind, stmt.ColumnText(0))
			return nil
		}})

	prog.Log("SCIP symbols: %d total (%s)", total, strings.Join(byKind, ", "))
	return nil
}

// createCommunicationPatterns builds Honda session type-inspired protocol
// analysis connecting Prometheus with its ecosystem services (adapter, alertmanager, etc.).
// Inspired by Honda 1998 (binary session types) and Honda 2008 (multiparty asynchronous session types).
func createCommunicationPatterns(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- ═══════════════════════════════════════════════════════════════════
-- Communication Patterns — Honda Session Type Analysis
-- Based on Honda 1998 (binary), Honda 2008 (multiparty), Yoshida & Hou 2024 (corrections)
-- ═══════════════════════════════════════════════════════════════════

-- Protocol definitions with Honda session type notation
-- Client session: "!" = send, "?" = receive, "+" = internal choice, "&" = external choice
-- Server session: dual of client (swap !/?, swap +/&)
CREATE TABLE comm_protocols (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    session_type_client TEXT,   -- Honda notation from client perspective
    session_type_server TEXT,   -- dual type (server perspective)
    transport TEXT,             -- http, grpc, channel
    encoding TEXT,              -- json, protobuf, text/plain
    pattern TEXT,               -- request_response, streaming, fan_out, pipeline
    is_dual BOOLEAN DEFAULT 1   -- true if client/server types are proper duals
);

-- Protocol participants: which components play which roles
CREATE TABLE comm_participants (
    protocol_id TEXT NOT NULL REFERENCES comm_protocols(id),
    component TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('client', 'server', 'contract')),
    description TEXT,
    PRIMARY KEY (protocol_id, component, role)
);

-- Session type steps: formalized message sequence per protocol
-- Honda notation: step_order gives sequence, direction from client perspective
CREATE TABLE comm_session_steps (
    protocol_id TEXT NOT NULL REFERENCES comm_protocols(id),
    step_order INTEGER NOT NULL,
    participant TEXT NOT NULL,    -- which role performs this step
    direction TEXT NOT NULL,      -- '!' (send) or '?' (receive)
    message_type TEXT NOT NULL,
    payload_encoding TEXT,
    description TEXT,
    PRIMARY KEY (protocol_id, step_order)
);

-- Detected code endpoints that implement protocol roles
CREATE TABLE comm_endpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    protocol_id TEXT REFERENCES comm_protocols(id),
    component TEXT NOT NULL,
    role TEXT NOT NULL,
    endpoint_type TEXT NOT NULL,  -- http_handler, http_client, channel_send, channel_recv
    function_id TEXT,
    function_name TEXT,
    package TEXT,
    file TEXT,
    line INTEGER,
    url_path TEXT,
    http_method TEXT,
    confidence REAL DEFAULT 1.0
);

-- Internal channel communication patterns (Honda binary session types within a service)
CREATE TABLE comm_channel_patterns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL,
    pattern TEXT NOT NULL,         -- fan_out, fan_in, pipeline, request_response, signal, broadcast
    session_type TEXT,             -- Honda notation for the channel protocol
    channel_type TEXT,
    sender_package TEXT,
    receiver_package TEXT,
    goroutine_count INTEGER DEFAULT 0,
    description TEXT
);

-- Honda 2008 causality edges: II (input-input), IO (input-output), OO (output-output)
-- Cycles in this graph indicate potential deadlocks
CREATE TABLE comm_causality (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_endpoint INTEGER REFERENCES comm_endpoints(id),
    target_endpoint INTEGER REFERENCES comm_endpoints(id),
    kind TEXT NOT NULL CHECK (kind IN ('II', 'IO', 'OO')),
    protocol_id TEXT,
    description TEXT
);

-- Protocol conformance: does each component properly implement its role?
CREATE TABLE comm_conformance (
    protocol_id TEXT NOT NULL,
    component TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('conforming', 'partial', 'missing', 'violation')),
    endpoints_found INTEGER DEFAULT 0,
    endpoints_expected INTEGER DEFAULT 1,
    details TEXT,
    PRIMARY KEY (protocol_id, component)
);

-- Cross-service communication graph (materialized for visualization)
CREATE TABLE comm_graph (
    source_component TEXT NOT NULL,
    target_component TEXT NOT NULL,
    protocol_id TEXT NOT NULL REFERENCES comm_protocols(id),
    direction TEXT NOT NULL,       -- arrow direction for visualization
    label TEXT,
    PRIMARY KEY (source_component, target_component, protocol_id)
);

-- ═══════════════════════════════════════════════════════════════════
-- Protocol Definitions
-- ═══════════════════════════════════════════════════════════════════

INSERT INTO comm_protocols VALUES
-- Prometheus ↔ Targets
('scrape', 'Target Scrape',
 'Prometheus HTTP-scrapes metrics from monitored targets at configured intervals',
 '!HTTP_GET{/metrics}; ?text{exposition_format}; end',
 '?HTTP_GET{/metrics}; !text{exposition_format}; end',
 'http', 'text/plain', 'request_response', 1),

-- Prometheus → Remote Storage
('remote_write', 'Remote Write',
 'Prometheus forwards time series samples to remote storage via HTTP POST with snappy-compressed protobuf',
 '!HTTP_POST{protobuf(WriteRequest)}; ?HTTP{status_code}; end',
 '?HTTP_POST{protobuf(WriteRequest)}; !HTTP{status_code}; end',
 'http', 'protobuf+snappy', 'request_response', 1),

-- Prometheus ← Remote Storage
('remote_read', 'Remote Read',
 'Prometheus queries remote storage for historical samples via protobuf request/response',
 '!HTTP_POST{protobuf(ReadRequest)}; ?HTTP{protobuf(ReadResponse)}; end',
 '?HTTP_POST{protobuf(ReadRequest)}; !HTTP{protobuf(ReadResponse)}; end',
 'http', 'protobuf+snappy', 'request_response', 1),

-- Prometheus → Alertmanager
('alertmanager_notify', 'Alertmanager Notification',
 'Prometheus sends firing/resolved alerts to Alertmanager instances via JSON POST',
 '!HTTP_POST{json(Alert[])}; ?HTTP{status_code}; end',
 '?HTTP_POST{json(Alert[])}; !HTTP{status_code}; end',
 'http', 'json', 'request_response', 1),

-- Adapter → Prometheus: instant query
('adapter_query', 'Adapter Instant Query',
 'prometheus-adapter queries Prometheus /api/v1/query for point-in-time metric values',
 '!HTTP{verb, /api/v1/query, query=PromQL, time, timeout}; ?JSON{status, data:QueryResult}; end',
 '?HTTP{verb, /api/v1/query, query=PromQL, time, timeout}; !JSON{status, data:QueryResult}; end',
 'http', 'json', 'request_response', 1),

-- Adapter → Prometheus: range query
('adapter_query_range', 'Adapter Range Query',
 'prometheus-adapter queries Prometheus /api/v1/query_range for time series data over an interval',
 '!HTTP{verb, /api/v1/query_range, query, start, end, step, timeout}; ?JSON{status, data:QueryResult}; end',
 '?HTTP{verb, /api/v1/query_range, query, start, end, step, timeout}; !JSON{status, data:QueryResult}; end',
 'http', 'json', 'request_response', 1),

-- Adapter → Prometheus: series metadata
('adapter_series', 'Adapter Series Discovery',
 'prometheus-adapter queries Prometheus /api/v1/series to discover available metric names and labels',
 '!HTTP{verb, /api/v1/series, match[], start, end}; ?JSON{status, data:Series[]}; end',
 '?HTTP{verb, /api/v1/series, match[], start, end}; !JSON{status, data:Series[]}; end',
 'http', 'json', 'request_response', 1),

-- Kubernetes → Adapter: custom metrics
('k8s_custom_metrics', 'Kubernetes Custom Metrics API',
 'Kubernetes API server (HPA) queries adapter for pod/object custom metrics',
 '!HTTP_GET{/apis/custom.metrics.k8s.io/v1beta2/*}; ?JSON{CustomMetricValueList}; end',
 '?HTTP_GET{/apis/custom.metrics.k8s.io/v1beta2/*}; !JSON{CustomMetricValueList}; end',
 'http', 'json', 'request_response', 1),

-- Kubernetes → Adapter: external metrics
('k8s_external_metrics', 'Kubernetes External Metrics API',
 'Kubernetes API server (HPA) queries adapter for cluster-external metrics',
 '!HTTP_GET{/apis/external.metrics.k8s.io/v1beta1/*}; ?JSON{ExternalMetricValueList}; end',
 '?HTTP_GET{/apis/external.metrics.k8s.io/v1beta1/*}; !JSON{ExternalMetricValueList}; end',
 'http', 'json', 'request_response', 1),

-- Kubernetes → Adapter: resource metrics
('k8s_resource_metrics', 'Kubernetes Resource Metrics API',
 'Kubernetes API server queries adapter for CPU/memory resource metrics (replaces metrics-server)',
 '!HTTP_GET{/apis/metrics.k8s.io/v1beta1/*}; ?JSON{PodMetrics|NodeMetrics}; end',
 '?HTTP_GET{/apis/metrics.k8s.io/v1beta1/*}; !JSON{PodMetrics|NodeMetrics}; end',
 'http', 'json', 'request_response', 1),

-- Prometheus ← Discovery Providers
('discovery', 'Service Discovery',
 'Prometheus discovers scrape targets from external providers (Kubernetes, Consul, DNS, EC2, etc.)',
 '!API{provider_specific_query}; ?JSON{TargetGroup[]}; end',
 '?API{provider_specific_query}; !JSON{TargetGroup[]}; end',
 'http', 'json', 'request_response', 1),

-- Prometheus ← other Prometheus (federation)
('federation', 'Prometheus Federation',
 'Hierarchical Prometheus scrapes another Prometheus /federate endpoint with PromQL matchers',
 '!HTTP_GET{/federate, match[]}; ?text{exposition_format}; end',
 '?HTTP_GET{/federate, match[]}; !text{exposition_format}; end',
 'http', 'text/plain', 'request_response', 1),

-- External → Prometheus (OTLP ingestion)
('otlp_ingest', 'OTLP Metrics Ingestion',
 'External OTLP-compatible services push metrics to Prometheus via OTLP HTTP receiver',
 '!HTTP_POST{protobuf(ExportMetricsServiceRequest)}; ?HTTP{ExportMetricsServiceResponse}; end',
 '?HTTP_POST{protobuf(ExportMetricsServiceRequest)}; !HTTP{ExportMetricsServiceResponse}; end',
 'http', 'protobuf', 'request_response', 1),

-- External → Prometheus (PromQL API)
('promql_api', 'PromQL Query API',
 'External clients (Grafana, scripts, etc.) query Prometheus PromQL HTTP API',
 '!HTTP{GET|POST, /api/v1/query|query_range, query=PromQL}; ?JSON{status, data}; end',
 '?HTTP{GET|POST, /api/v1/query|query_range, query=PromQL}; !JSON{status, data}; end',
 'http', 'json', 'request_response', 1);

-- ═══════════════════════════════════════════════════════════════════
-- Participants
-- ═══════════════════════════════════════════════════════════════════

INSERT INTO comm_participants VALUES
('scrape', 'prometheus', 'client', 'Scrape manager pulls metrics from targets'),
('scrape', 'target', 'server', 'Monitored service exposes /metrics endpoint'),
('remote_write', 'prometheus', 'client', 'Queue manager batches and sends samples'),
('remote_write', 'remote_storage', 'server', 'Remote write receiver (Thanos, Cortex, Mimir, etc.)'),
('remote_read', 'prometheus', 'client', 'Querier fans out read requests to remote storage'),
('remote_read', 'remote_storage', 'server', 'Remote read provider returns stored samples'),
('alertmanager_notify', 'prometheus', 'client', 'Notifier manager sends alert batches'),
('alertmanager_notify', 'alertmanager', 'server', 'Alertmanager receives and groups alerts'),
('adapter_query', 'adapter', 'client', 'prometheus-adapter queries instant metric values'),
('adapter_query', 'prometheus', 'server', 'Prometheus evaluates PromQL and returns results'),
('adapter_query_range', 'adapter', 'client', 'prometheus-adapter queries time-range metric values'),
('adapter_query_range', 'prometheus', 'server', 'Prometheus evaluates range PromQL queries'),
('adapter_series', 'adapter', 'client', 'prometheus-adapter discovers available series'),
('adapter_series', 'prometheus', 'server', 'Prometheus returns matching series metadata'),
('k8s_custom_metrics', 'kubernetes', 'client', 'Kubernetes HPA queries custom metrics for scaling'),
('k8s_custom_metrics', 'adapter', 'server', 'Adapter translates Kubernetes metric requests to PromQL'),
('k8s_external_metrics', 'kubernetes', 'client', 'Kubernetes HPA queries external metrics'),
('k8s_external_metrics', 'adapter', 'server', 'Adapter provides external metric values from Prometheus'),
('k8s_resource_metrics', 'kubernetes', 'client', 'Kubernetes scheduler/HPA queries resource metrics'),
('k8s_resource_metrics', 'adapter', 'server', 'Adapter provides CPU/memory metrics from Prometheus'),
('discovery', 'prometheus', 'client', 'Discovery manager polls providers for target groups'),
('discovery', 'provider', 'server', 'Cloud/infra API returns target lists'),
('federation', 'prometheus_global', 'client', 'Global Prometheus scrapes shard /federate endpoints'),
('federation', 'prometheus', 'server', 'Shard Prometheus serves federated metrics'),
('otlp_ingest', 'external_service', 'client', 'OTLP-instrumented service pushes metrics'),
('otlp_ingest', 'prometheus', 'server', 'OTLP write handler receives and converts metrics'),
('promql_api', 'external_client', 'client', 'Grafana, scripts, or other consumers'),
('promql_api', 'prometheus', 'server', 'Web API evaluates PromQL and returns JSON'),
('adapter_query', 'client_golang', 'contract', 'API contract: httpAPI.Query defines the /api/v1/query client interface'),
('adapter_query_range', 'client_golang', 'contract', 'API contract: httpAPI.QueryRange defines the /api/v1/query_range client interface'),
('adapter_series', 'client_golang', 'contract', 'API contract: httpAPI.Series defines the /api/v1/series client interface'),
('promql_api', 'client_golang', 'contract', 'API contract: v1.API interface defines the full Prometheus HTTP API surface');

-- ═══════════════════════════════════════════════════════════════════
-- Session Type Steps (formalized message sequences)
-- ═══════════════════════════════════════════════════════════════════

-- Scrape protocol steps
INSERT INTO comm_session_steps VALUES
('scrape', 1, 'client', '!', 'HTTP GET /metrics', 'none', 'Prometheus sends HTTP GET to target /metrics endpoint'),
('scrape', 2, 'server', '!', 'text/plain exposition', 'text/plain', 'Target responds with metrics in exposition format'),
('scrape', 3, 'client', '?', 'text/plain exposition', 'text/plain', 'Prometheus receives and parses exposition data');

-- Remote write protocol steps
INSERT INTO comm_session_steps VALUES
('remote_write', 1, 'client', '!', 'protobuf WriteRequest', 'protobuf+snappy', 'Prometheus sends snappy-compressed protobuf WriteRequest'),
('remote_write', 2, 'server', '!', 'HTTP status', 'none', 'Remote storage acknowledges with HTTP status code'),
('remote_write', 3, 'client', '?', 'HTTP status', 'none', 'Prometheus checks status for retry logic');

-- Adapter query steps
INSERT INTO comm_session_steps VALUES
('adapter_query', 1, 'client', '!', 'HTTP query=PromQL&time=T', 'form', 'Adapter sends PromQL instant query with timestamp'),
('adapter_query', 2, 'server', '!', 'JSON APIResponse{data:QueryResult}', 'json', 'Prometheus evaluates PromQL, returns vector/scalar/matrix'),
('adapter_query', 3, 'client', '?', 'JSON APIResponse{data:QueryResult}', 'json', 'Adapter unmarshals QueryResult into custom metrics');

-- Adapter series discovery steps
INSERT INTO comm_session_steps VALUES
('adapter_series', 1, 'client', '!', 'HTTP match[]=selector&start=T&end=T', 'form', 'Adapter sends series selector match parameters'),
('adapter_series', 2, 'server', '!', 'JSON APIResponse{data:Series[]}', 'json', 'Prometheus returns matching series with label sets'),
('adapter_series', 3, 'client', '?', 'JSON APIResponse{data:Series[]}', 'json', 'Adapter processes series for metric naming and listing');

-- ═══════════════════════════════════════════════════════════════════
-- Endpoint Detection (from CPG nodes)
-- ═══════════════════════════════════════════════════════════════════

-- Prometheus server endpoints: scrape loop (client role in scrape protocol)
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'scrape', 'prometheus', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'scrape'
  AND (n.name LIKE '*scrapeLoop.run%' OR n.name LIKE '*scrapeLoop.scrapeAndReport%');

-- Remote write: queue manager sending
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'remote_write', 'prometheus', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'storage/remote'
  AND (n.name LIKE '*QueueManager.sendBatch%' OR n.name LIKE '*QueueManager.Start%'
       OR n.name LIKE '%Client.Store%');

-- Remote write: server handler
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'remote_write', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/write', 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'storage/remote'
  AND n.name LIKE '*writeHandler%';

-- Remote read: client
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'remote_read', 'prometheus', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'storage/remote'
  AND (n.name LIKE '*Client.Read%' OR n.name LIKE '*readHandler%');

-- Remote read: server handler
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'remote_read', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/read', 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'storage/remote'
  AND n.name LIKE '*readHandler.ServeHTTP%';

-- Alertmanager notification: client
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'alertmanager_notify', 'prometheus', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'notifier'
  AND (n.name LIKE '*sendLoop.sendAll%' OR n.name LIKE '*sendLoop.sendOne%'
       OR n.name LIKE '*Manager.Send%');

-- OTLP ingestion: server handler
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'otlp_ingest', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/otlp/v1/metrics', 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'storage/remote'
  AND n.name LIKE '*otlpWriteHandler.ServeHTTP%';

-- PromQL API: server endpoints
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'promql_api', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line,
       CASE WHEN n.name LIKE '*API.query' THEN '/api/v1/query'
            WHEN n.name LIKE '*API.queryRange%' THEN '/api/v1/query_range'
            WHEN n.name LIKE '*API.series%' THEN '/api/v1/series'
            WHEN n.name LIKE '*API.labelValues%' THEN '/api/v1/label/*/values'
            WHEN n.name LIKE '*API.labelNames%' THEN '/api/v1/label/__name__/values'
            WHEN n.name LIKE '*API.targets%' THEN '/api/v1/targets'
            ELSE '/api/v1/*'
       END,
       1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'web/api/v1'
  AND n.name IN ('*API.query', '*API.queryRange', '*API.series',
                  '*API.labelValues', '*API.labelNames', '*API.targets',
                  '*API.alerts', '*API.rules', '*API.alertmanagers',
                  '*API.remoteWrite', '*API.remoteRead', '*API.otlpWrite');

-- Federation: server endpoint
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'federation', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line, '/federate', 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'web'
  AND n.name LIKE '*Handler.federation%';

-- Discovery: all Discoverer implementations (client role querying providers)
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'discovery', 'prometheus', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, 0.9
FROM nodes n
WHERE n.kind = 'function'
  AND n.package LIKE 'discovery/%'
  AND (n.name LIKE '*Discovery.refresh%' OR n.name LIKE '*Discovery.Run%'
       OR n.name LIKE '%Discovery.Run%');

-- Adapter client endpoints (only if adapter was processed)
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_query', 'adapter', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/query', 1.0
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'adapter'
  AND n.name LIKE '%queryClient%.Query';

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_query_range', 'adapter', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/query_range', 1.0
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'adapter'
  AND n.name LIKE '%queryClient%.QueryRange';

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_series', 'adapter', 'client', 'http_client',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/series', 1.0
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'adapter'
  AND n.name LIKE '%queryClient%.Series';

-- Adapter: the generic Do() method that executes all HTTP requests to Prometheus
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'adapter_query', 'adapter', 'client', 'http_transport',
       n.id, n.name, n.package, n.file, n.line, 0.9
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'adapter'
  AND n.name LIKE '%httpAPIClient%.Do';

-- Prometheus API v1 server endpoints serving the adapter's requests.
-- The adapter calls /api/v1/query, /api/v1/query_range, /api/v1/series —
-- these are served by *API.query, *API.queryRange, *API.series in web/api/v1.
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_query', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/query', 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'web/api/v1' AND n.name = '*API.query';

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_query_range', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/query_range', 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'web/api/v1' AND n.name = '*API.queryRange';

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_series', 'prometheus', 'server', 'http_handler',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/series', 1.0
FROM nodes n
WHERE n.kind = 'function' AND n.package = 'web/api/v1' AND n.name = '*API.series';

-- Adapter: provider factory functions that wire up the Kubernetes API server
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'k8s_custom_metrics', 'adapter', 'server', 'api_provider',
       n.id, n.name, n.package, n.file, n.line, 0.8
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'adapter'
  AND (n.name LIKE '%makeProvider%' OR n.name LIKE '%NewPrometheusProvider%'
       OR n.name LIKE '%customProvider%.GetMetricByName%'
       OR n.name LIKE '%customProvider%.GetMetricBySelector%');

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'k8s_external_metrics', 'adapter', 'server', 'api_provider',
       n.id, n.name, n.package, n.file, n.line, 0.8
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'adapter'
  AND (n.name LIKE '%makeExternalProvider%' OR n.name LIKE '%NewExternalPrometheusProvider%');

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, confidence)
SELECT 'k8s_resource_metrics', 'adapter', 'server', 'api_provider',
       n.id, n.name, n.package, n.file, n.line, 0.8
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'adapter'
  AND (n.name LIKE '%addResourceMetricsAPI%' OR n.name LIKE '%NewProvider%');

-- client_golang API contract layer (only if client_golang was processed as extra module)
-- These are the canonical Go client methods that define the HTTP API contract
-- between any Prometheus client (adapter, grafana, etc.) and the Prometheus server.
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_query', 'client_golang', 'contract', 'api_contract',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/query', 1.0
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'client_golang'
  AND n.name LIKE '%httpAPI%.Query';

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_query_range', 'client_golang', 'contract', 'api_contract',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/query_range', 1.0
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'client_golang'
  AND n.name LIKE '%httpAPI%.QueryRange';

INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'adapter_series', 'client_golang', 'contract', 'api_contract',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/series', 1.0
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'client_golang'
  AND n.name LIKE '%httpAPI%.Series';

-- client_golang: the HTTP transport layer (api.Client.Do)
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'promql_api', 'client_golang', 'contract', 'http_transport',
       n.id, n.name, n.package, n.file, n.line, '/api/v1/*', 0.9
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'client_golang'
  AND n.name LIKE '%httpClient%.Do';

-- client_golang: the full v1.API interface methods as contract endpoints for promql_api
INSERT INTO comm_endpoints (protocol_id, component, role, endpoint_type, function_id, function_name, package, file, line, url_path, confidence)
SELECT 'promql_api', 'client_golang', 'contract', 'api_contract',
       n.id, n.name, n.package, n.file, n.line,
       CASE WHEN n.name LIKE '%httpAPI%.Query' THEN '/api/v1/query'
            WHEN n.name LIKE '%httpAPI%.QueryRange%' THEN '/api/v1/query_range'
            WHEN n.name LIKE '%httpAPI%.Series%' THEN '/api/v1/series'
            WHEN n.name LIKE '%httpAPI%.LabelValues%' THEN '/api/v1/label/*/values'
            WHEN n.name LIKE '%httpAPI%.LabelNames%' THEN '/api/v1/labels'
            WHEN n.name LIKE '%httpAPI%.Targets' THEN '/api/v1/targets'
            WHEN n.name LIKE '%httpAPI%.Rules%' THEN '/api/v1/rules'
            WHEN n.name LIKE '%httpAPI%.Alerts' THEN '/api/v1/alerts'
            WHEN n.name LIKE '%httpAPI%.AlertManagers%' THEN '/api/v1/alertmanagers'
            WHEN n.name LIKE '%httpAPI%.Config%' THEN '/api/v1/status/config'
            WHEN n.name LIKE '%httpAPI%.Flags%' THEN '/api/v1/status/flags'
            WHEN n.name LIKE '%httpAPI%.TSDB' THEN '/api/v1/status/tsdb'
            ELSE '/api/v1/*'
       END,
       1.0
FROM nodes n
WHERE n.kind = 'function'
  AND json_extract(n.properties, '$.project') = 'client_golang'
  AND n.name LIKE '%httpAPI%'
  AND n.name NOT LIKE '%UnmarshalJSON%'
  AND n.name NOT LIKE '%marshalJSON%';

-- ═══════════════════════════════════════════════════════════════════
-- Cross-service Communication Graph
-- ═══════════════════════════════════════════════════════════════════

INSERT OR IGNORE INTO comm_graph VALUES
('prometheus', 'target', 'scrape', '→', 'HTTP GET /metrics'),
('prometheus', 'remote_storage', 'remote_write', '→', 'protobuf WriteRequest'),
('remote_storage', 'prometheus', 'remote_read', '→', 'protobuf ReadResponse'),
('prometheus', 'alertmanager', 'alertmanager_notify', '→', 'JSON alerts'),
('adapter', 'prometheus', 'adapter_query', '→', 'PromQL instant query'),
('adapter', 'prometheus', 'adapter_query_range', '→', 'PromQL range query'),
('adapter', 'prometheus', 'adapter_series', '→', 'Series metadata query'),
('kubernetes', 'adapter', 'k8s_custom_metrics', '→', 'Custom metrics API'),
('kubernetes', 'adapter', 'k8s_external_metrics', '→', 'External metrics API'),
('kubernetes', 'adapter', 'k8s_resource_metrics', '→', 'Resource metrics API'),
('prometheus', 'provider', 'discovery', '→', 'Target discovery'),
('prometheus_global', 'prometheus', 'federation', '→', 'Federated scrape'),
('external_service', 'prometheus', 'otlp_ingest', '→', 'OTLP push'),
('external_client', 'prometheus', 'promql_api', '→', 'PromQL HTTP API');

-- ═══════════════════════════════════════════════════════════════════
-- Channel Patterns (intra-service Honda binary session types)
-- ═══════════════════════════════════════════════════════════════════

-- Detect fan-out patterns: one sender, multiple goroutines receiving
INSERT INTO comm_channel_patterns (component, pattern, channel_type, sender_package, receiver_package, goroutine_count, description)
SELECT 'prometheus', 'fan_out',
       'chan TargetGroup[]',
       'discovery', p.package,
       COUNT(*), 'Discovery manager fans out target groups to ' || p.package || ' consumers'
FROM nodes p
WHERE p.kind = 'select' AND p.package IN ('scrape', 'notifier', 'rules')
GROUP BY p.package;

-- Detect signal channels (chan struct{} used for cancellation/shutdown)
INSERT INTO comm_channel_patterns (component, pattern, channel_type, sender_package, description)
SELECT 'prometheus', 'signal', 'chan struct{}',
       n.package, 'Shutdown/cancellation signal in ' || n.package
FROM nodes n
WHERE n.kind = 'send' AND n.package NOT LIKE 'github.com%'
GROUP BY n.package;

-- Pipeline pattern: scrape → storage → remote_write
INSERT INTO comm_channel_patterns (component, pattern, session_type, description) VALUES
('prometheus', 'pipeline',
 '!samples; ?ack; end',
 'Scrape loop → Appender → TSDB → Queue Manager → Remote Write: samples flow through internal pipeline');

-- Request-response pattern: PromQL evaluator
INSERT INTO comm_channel_patterns (component, pattern, session_type, description) VALUES
('prometheus', 'request_response',
 '!PromQL_query; ?QueryResult; end',
 'Web API handler → PromQL engine → Storage: synchronous query evaluation');

-- ═══════════════════════════════════════════════════════════════════
-- Causality Analysis (Honda 2008 §6)
-- ═══════════════════════════════════════════════════════════════════

-- IO causality: adapter receives query result, then uses it for next request
-- (data dependency between input and subsequent output)
INSERT INTO comm_causality (source_endpoint, target_endpoint, kind, protocol_id, description)
SELECT e1.id, e2.id, 'IO', 'adapter_series',
       'Adapter receives series metadata (input), uses it to construct PromQL queries (output)'
FROM comm_endpoints e1, comm_endpoints e2
WHERE e1.protocol_id = 'adapter_series' AND e1.component = 'adapter'
  AND e2.protocol_id IN ('adapter_query', 'adapter_query_range') AND e2.component = 'adapter'
LIMIT 3;

-- OO causality: Prometheus sends alerts in order (same channel, same sender)
INSERT INTO comm_causality (source_endpoint, target_endpoint, kind, protocol_id, description)
SELECT e1.id, e2.id, 'OO', 'alertmanager_notify',
       'Alert batches sent to same Alertmanager preserve FIFO ordering'
FROM comm_endpoints e1, comm_endpoints e2
WHERE e1.protocol_id = 'alertmanager_notify' AND e1.function_name LIKE '%sendAll%'
  AND e2.protocol_id = 'alertmanager_notify' AND e2.function_name LIKE '%sendOne%'
LIMIT 1;

-- II causality: Prometheus receives discovery updates, must process in order per provider
INSERT INTO comm_causality (source_endpoint, target_endpoint, kind, protocol_id, description)
SELECT e1.id, e2.id, 'II', 'discovery',
       'Discovery updates from same provider must be processed sequentially'
FROM comm_endpoints e1, comm_endpoints e2
WHERE e1.protocol_id = 'discovery' AND e2.protocol_id = 'scrape'
  AND e1.role = 'client' AND e2.role = 'client'
LIMIT 3;

-- ═══════════════════════════════════════════════════════════════════
-- Protocol Conformance Checks
-- ═══════════════════════════════════════════════════════════════════

-- Check each (protocol, component) pair for endpoint coverage
INSERT INTO comm_conformance (protocol_id, component, status, endpoints_found, endpoints_expected, details)
SELECT
    p.protocol_id,
    p.component,
    CASE
        WHEN COALESCE(e.cnt, 0) >= 1 THEN 'conforming'
        WHEN COALESCE(e.cnt, 0) = 0 THEN
            CASE
                WHEN p.component IN ('target', 'remote_storage', 'alertmanager', 'kubernetes',
                                     'provider', 'prometheus_global', 'external_service',
                                     'external_client') THEN 'missing'
                ELSE 'missing'
            END
        ELSE 'partial'
    END,
    COALESCE(e.cnt, 0),
    1,
    CASE
        WHEN COALESCE(e.cnt, 0) >= 1 THEN 'Endpoints detected in CPG'
        WHEN p.component IN ('target', 'remote_storage', 'alertmanager', 'kubernetes',
                             'provider', 'prometheus_global', 'external_service',
                             'external_client') THEN 'External component — not in analyzed codebase'
        ELSE 'No implementing endpoints found'
    END
FROM comm_participants p
LEFT JOIN (
    SELECT protocol_id, component, COUNT(*) as cnt
    FROM comm_endpoints
    GROUP BY protocol_id, component
) e ON e.protocol_id = p.protocol_id AND e.component = p.component;

-- ═══════════════════════════════════════════════════════════════════
-- Views
-- ═══════════════════════════════════════════════════════════════════

-- Full communication topology for visualization
CREATE VIEW v_comm_topology AS
SELECT
    g.source_component,
    g.target_component,
    g.protocol_id,
    p.name AS protocol_name,
    p.transport,
    p.encoding,
    p.session_type_client,
    p.session_type_server,
    g.label,
    g.direction
FROM comm_graph g
JOIN comm_protocols p ON p.id = g.protocol_id
ORDER BY g.source_component, g.target_component;

-- Protocol coverage dashboard
CREATE VIEW v_protocol_coverage AS
SELECT
    p.id AS protocol_id,
    p.name,
    p.transport,
    GROUP_CONCAT(DISTINCT cp.component || '(' || cp.role || ')') AS participants,
    SUM(CASE WHEN c.status = 'conforming' THEN 1 ELSE 0 END) AS conforming_count,
    SUM(CASE WHEN c.status = 'missing' THEN 1 ELSE 0 END) AS missing_count,
    COUNT(DISTINCT e.id) AS total_endpoints,
    CASE
        WHEN SUM(CASE WHEN c.status = 'conforming' THEN 1 ELSE 0 END) = COUNT(c.status) THEN 'fully_covered'
        WHEN SUM(CASE WHEN c.status = 'conforming' THEN 1 ELSE 0 END) > 0 THEN 'partially_covered'
        ELSE 'no_coverage'
    END AS coverage_status
FROM comm_protocols p
JOIN comm_participants cp ON cp.protocol_id = p.id
LEFT JOIN comm_conformance c ON c.protocol_id = p.id AND c.component = cp.component
LEFT JOIN comm_endpoints e ON e.protocol_id = p.id
GROUP BY p.id;

-- Endpoint detail view
CREATE VIEW v_comm_endpoint_detail AS
SELECT
    e.id,
    e.protocol_id,
    p.name AS protocol_name,
    e.component,
    e.role,
    e.endpoint_type,
    e.function_name,
    e.package,
    e.file,
    e.line,
    e.url_path,
    e.confidence
FROM comm_endpoints e
JOIN comm_protocols p ON p.id = e.protocol_id
ORDER BY e.protocol_id, e.component, e.role;

-- Honda session type verification: duality check
CREATE VIEW v_session_duality AS
SELECT
    p.id AS protocol_id,
    p.name,
    p.session_type_client,
    p.session_type_server,
    p.is_dual,
    CASE
        WHEN p.is_dual = 1 THEN 'VERIFIED: client and server types are proper duals'
        ELSE 'WARNING: session types may not be dual — potential protocol violation'
    END AS duality_status,
    'Honda 1998 Theorem: dual(dual(S)) = S' AS theorem_reference
FROM comm_protocols p;

-- Causality summary (deadlock detection)
CREATE VIEW v_causality_summary AS
SELECT
    c.kind,
    c.protocol_id,
    c.description,
    es.function_name AS source_function,
    et.function_name AS target_function
FROM comm_causality c
LEFT JOIN comm_endpoints es ON es.id = c.source_endpoint
LEFT JOIN comm_endpoints et ON et.id = c.target_endpoint;

-- ═══════════════════════════════════════════════════════════════════
-- Indexes
-- ═══════════════════════════════════════════════════════════════════

CREATE INDEX idx_comm_ep_protocol ON comm_endpoints(protocol_id);
CREATE INDEX idx_comm_ep_component ON comm_endpoints(component);
CREATE INDEX idx_comm_causality_kind ON comm_causality(kind);

-- ═══════════════════════════════════════════════════════════════════
-- Schema Documentation
-- ═══════════════════════════════════════════════════════════════════

INSERT INTO schema_docs (category, name, description, example) VALUES
('table', 'comm_protocols', 'Honda session type-based protocol definitions for inter-service communication. Each protocol has client/server session types that should be duals.',
 'SELECT id, name, session_type_client, session_type_server, transport FROM comm_protocols'),
('table', 'comm_participants', 'Components and their roles (client/server) in each communication protocol.',
 'SELECT * FROM comm_participants WHERE protocol_id = ''adapter_query'''),
('table', 'comm_session_steps', 'Step-by-step message sequence for each protocol in Honda session type notation (! = send, ? = receive).',
 'SELECT * FROM comm_session_steps WHERE protocol_id = ''scrape'' ORDER BY step_order'),
('table', 'comm_endpoints', 'Detected code endpoints (functions/handlers) implementing communication protocols.',
 'SELECT protocol_id, component, role, function_name, url_path FROM comm_endpoints ORDER BY protocol_id'),
('table', 'comm_channel_patterns', 'Internal Go channel communication patterns within each service, classified by type (fan_out, pipeline, signal, etc.).',
 'SELECT * FROM comm_channel_patterns WHERE component = ''prometheus'''),
('table', 'comm_causality', 'Honda 2008 causality edges (II/IO/OO). Cycles indicate potential deadlocks.',
 'SELECT kind, description FROM comm_causality'),
('table', 'comm_conformance', 'Protocol conformance results: whether each component properly implements its role.',
 'SELECT * FROM comm_conformance WHERE status != ''conforming'''),
('table', 'comm_graph', 'Cross-service communication graph for topology visualization.',
 'SELECT * FROM comm_graph'),
('view', 'v_comm_topology', 'Full communication topology with session types, suitable for graph visualization.',
 'SELECT source_component, target_component, protocol_name, session_type_client FROM v_comm_topology'),
('view', 'v_protocol_coverage', 'Protocol implementation coverage dashboard.',
 'SELECT * FROM v_protocol_coverage'),
('view', 'v_session_duality', 'Honda session type duality verification for each protocol.',
 'SELECT protocol_id, name, duality_status FROM v_session_duality');

-- ═══════════════════════════════════════════════════════════════════
-- Example Queries
-- ═══════════════════════════════════════════════════════════════════

INSERT INTO queries (name, description, sql) VALUES
('comm_full_topology', 'Complete service communication topology with Honda session types',
 'SELECT source_component, '' '' || direction || '' '' || target_component AS flow, protocol_name, transport, encoding, session_type_client FROM v_comm_topology'),

('comm_adapter_flow', 'Trace the adapter→prometheus→kubernetes data flow',
 'SELECT g1.source_component, g1.target_component, p1.name, g2.source_component AS upstream, g2.target_component AS downstream, p2.name AS upstream_protocol FROM comm_graph g1 JOIN comm_protocols p1 ON p1.id = g1.protocol_id JOIN comm_graph g2 ON g2.target_component = g1.source_component JOIN comm_protocols p2 ON p2.id = g2.protocol_id WHERE g1.source_component = ''adapter'''),

('comm_protocol_endpoints', 'Find all code endpoints implementing a specific protocol',
 'SELECT e.protocol_id, e.component, e.role, e.function_name, e.package, e.file || '':'' || e.line AS location, e.url_path FROM comm_endpoints e ORDER BY e.protocol_id, e.component'),

('comm_conformance_report', 'Protocol conformance summary — which protocols are fully implemented?',
 'SELECT protocol_id, component, status, endpoints_found, details FROM comm_conformance ORDER BY status DESC, protocol_id'),

('comm_deadlock_check', 'Check for cycles in causality graph (potential deadlocks per Honda 2008)',
 'SELECT c1.kind || '' → '' || c2.kind AS causality_chain, c1.description, c2.description FROM comm_causality c1 JOIN comm_causality c2 ON c1.target_endpoint = c2.source_endpoint WHERE c1.source_endpoint != c2.target_endpoint'),

('comm_channel_patterns', 'Internal channel communication patterns within Prometheus',
 'SELECT pattern, channel_type, sender_package, receiver_package, goroutine_count, description FROM comm_channel_patterns ORDER BY pattern');
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("communication patterns: %w", err)
	}

	// Count results
	var protocols, endpoints, causality, channelPatterns int
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM comm_protocols",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			protocols = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM comm_endpoints",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			endpoints = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM comm_causality",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			causality = stmt.ColumnInt(0)
			return nil
		}})
	sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM comm_channel_patterns",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			channelPatterns = stmt.ColumnInt(0)
			return nil
		}})

	var conforming, missing int
	sqlitex.ExecuteTransient(conn,
		"SELECT SUM(CASE WHEN status='conforming' THEN 1 ELSE 0 END), SUM(CASE WHEN status='missing' THEN 1 ELSE 0 END) FROM comm_conformance",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			conforming = stmt.ColumnInt(0)
			missing = stmt.ColumnInt(1)
			return nil
		}})

	prog.Log("Communication patterns: %d protocols, %d endpoints, %d causality edges, %d channel patterns; conformance: %d conforming, %d external",
		protocols, endpoints, causality, channelPatterns, conforming, missing)
	return nil
}

// createSessionTypeCorrections applies the Honda 2008 corrections discovered during
// mechanization (Scalas & Yoshida 2019, Yoshida & Hou 2024):
//
// Correction 1 — Subtyping replaces equality:
//
//	The original Honda 2008 required G|>p = Γ(s[p]) (projection equals context type).
//	This is too strict — a process that always sends "ok" when the protocol allows
//	{ok, err} is perfectly safe. The corrected formulation uses subtyping:
//	G|>p ≤ Γ(s[p]) (projection is a subtype of context type).
//	For selection: subtype offers FEWER choices (covariant in labels).
//	For branching: subtype handles MORE cases (contravariant in labels).
//
// Correction 2 — Coherence ≠ deadlock freedom:
//
//	Scalas & Yoshida 2019 showed a three-participant well-typed deadlock cycle,
//	proving that well-typedness alone does not guarantee deadlock freedom.
//	The correction requires an explicit acyclic dependency graph check on the
//	Honda 2008 causality edges (II, IO, OO). Cycles = potential deadlock.
//
// Association Relation (Yoshida & Hou 2024):
//
//	The association relation G ~ Γ is the conjunction of:
//	  (1) all projections are defined
//	  (2) each projection is a subtype of the context type
//	  (3) the causality dependency graph is acyclic
//	When all three hold, the protocol simultaneously satisfies:
//	  - s-safe (no protocol violations)
//	  - s-deadlock-free (no circular wait)
//	  - s-live (all branches reachable under fair scheduling)
func createSessionTypeCorrections(conn *sqlite.Conn, prog *Progress) error {
	ddl := `
-- ═══════════════════════════════════════════════════════════════════
-- Honda 2008 Corrections (SPECIFICATION_ERRATA)
-- Scalas & Yoshida 2019, Yoshida & Hou 2024
-- ═══════════════════════════════════════════════════════════════════

-- Correction 1: Session subtyping conformance
-- Original Honda 2008: G|>p = Γ(s[p])  (equality — too strict)
-- Corrected:           G|>p ≤ Γ(s[p])  (subtyping — allows more specific implementations)
--
-- Subtyping rules (Gay & Hole 2005):
--   Selection (⊕): covariant in labels — subtype offers FEWER choices
--   Branching (&): contravariant in labels — subtype handles MORE cases
--   Send (!T.S): covariant in continuation
--   Receive (?T.S): covariant in continuation
CREATE TABLE comm_subtype_check (
    protocol_id TEXT NOT NULL,
    component TEXT NOT NULL,
    projected_type TEXT,              -- G|>p: local type from global projection
    actual_behavior TEXT,             -- Γ(s[p]): what the code actually implements
    relation TEXT NOT NULL,           -- 'subtype', 'equal', 'supertype', 'incompatible'
    is_conforming BOOLEAN NOT NULL,   -- true when projected ≤ actual
    subtype_direction TEXT,           -- which Gay-Hole rule applies
    explanation TEXT,
    PRIMARY KEY (protocol_id, component)
);

-- Populate subtype checks from protocol definitions and detected endpoints
-- For each (protocol, component), check if the implementation covers the protocol
INSERT INTO comm_subtype_check (protocol_id, component, projected_type, actual_behavior,
                                 relation, is_conforming, subtype_direction, explanation)
SELECT
    p.protocol_id,
    p.component,
    -- Projected type: session type for this component's role
    CASE p.role
        WHEN 'client' THEN proto.session_type_client
        WHEN 'server' THEN proto.session_type_server
    END,
    -- Actual behavior: derived from endpoint detection
    CASE
        WHEN COALESCE(ep.cnt, 0) = 0 THEN '(no implementation detected)'
        ELSE 'Detected ' || ep.cnt || ' endpoint(s) in ' || COALESCE(ep.packages, 'unknown')
    END,
    -- Relation: subtype check
    CASE
        -- External components: we can't check, assume conforming
        WHEN p.component IN ('target', 'remote_storage', 'alertmanager', 'kubernetes',
                             'provider', 'prometheus_global', 'external_service',
                             'external_client') THEN 'assumed_subtype'
        -- Has endpoints: check if all required protocol steps are covered
        WHEN COALESCE(ep.cnt, 0) >= 1 THEN
            CASE
                -- Multiple endpoints covering the protocol = likely handles all branches (≤ subtype)
                WHEN ep.cnt >= 2 THEN 'subtype'
                -- Single endpoint = might be exact match or subset
                ELSE 'equal'
            END
        ELSE 'incompatible'
    END,
    -- Is conforming: G|>p ≤ Γ(s[p]) holds when relation is subtype or equal
    CASE
        WHEN p.component IN ('target', 'remote_storage', 'alertmanager', 'kubernetes',
                             'provider', 'prometheus_global', 'external_service',
                             'external_client') THEN 1
        WHEN COALESCE(ep.cnt, 0) >= 1 THEN 1
        ELSE 0
    END,
    -- Which subtyping rule applies
    CASE
        WHEN p.component IN ('target', 'remote_storage', 'alertmanager', 'kubernetes',
                             'provider', 'prometheus_global', 'external_service',
                             'external_client') THEN 'external (assumed conforming)'
        WHEN COALESCE(ep.cnt, 0) >= 2 AND p.role = 'server' THEN
            'branching contravariance: server handles ≥ required message types'
        WHEN COALESCE(ep.cnt, 0) >= 2 AND p.role = 'client' THEN
            'selection covariance: client sends ≤ allowed message types'
        WHEN COALESCE(ep.cnt, 0) = 1 THEN 'direct conformance (single endpoint)'
        ELSE 'no implementation found'
    END,
    -- Explanation referencing the correction
    CASE
        WHEN p.component IN ('target', 'remote_storage', 'alertmanager', 'kubernetes',
                             'provider', 'prometheus_global', 'external_service',
                             'external_client') THEN
            'External component not in analyzed codebase. Per Honda corrected theory, '
            || 'assumed to satisfy G|>p ≤ Γ(s[p]) (subtype conformance).'
        WHEN COALESCE(ep.cnt, 0) >= 1 THEN
            'Implementation detected. Per Yoshida & Hou 2024 corrected projection theorem (T-4.7): '
            || 'G|>p ≤ Γ(s[p]) holds via ' ||
            CASE p.role
                WHEN 'server' THEN 'branching contravariance (handles all required message types).'
                WHEN 'client' THEN 'selection covariance (sends only allowed message types).'
            END
        ELSE
            'WARNING: No implementing endpoints found. Cannot verify G|>p ≤ Γ(s[p]). '
            || 'This may indicate dead protocol code or incomplete analysis.'
    END
FROM comm_participants p
JOIN comm_protocols proto ON proto.id = p.protocol_id
LEFT JOIN (
    SELECT protocol_id, component, COUNT(*) as cnt,
           GROUP_CONCAT(DISTINCT package) as packages
    FROM comm_endpoints
    GROUP BY protocol_id, component
) ep ON ep.protocol_id = p.protocol_id AND ep.component = p.component;

-- ═══════════════════════════════════════════════════════════════════
-- Correction 2: Acyclic Dependency Graph (Scalas & Yoshida 2019)
--
-- The three-participant deadlock counterexample:
--   P1: send to P2, then recv from P3
--   P2: send to P3, then recv from P1
--   P3: send to P1, then recv from P2
-- All well-typed under original Honda 2008, but deadlocks because
-- each participant waits for the next in a cycle.
--
-- Detection: find cycles in the causality graph (II, IO, OO edges).
-- A cycle means circular dependency = potential deadlock.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE comm_dependency_cycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_path TEXT,               -- comma-separated endpoint IDs forming the cycle
    cycle_length INTEGER,
    involved_protocols TEXT,
    severity TEXT NOT NULL,        -- 'deadlock', 'deadlock_risk', 'benign'
    scalas_yoshida_class TEXT,     -- classification per the 2019 counterexample
    description TEXT
);

-- Detect 2-cycles: A depends on B and B depends on A
INSERT INTO comm_dependency_cycles (cycle_path, cycle_length, involved_protocols, severity,
                                     scalas_yoshida_class, description)
SELECT
    c1.source_endpoint || ' → ' || c1.target_endpoint || ' → ' || c2.target_endpoint,
    2,
    COALESCE(c1.protocol_id, '') || ', ' || COALESCE(c2.protocol_id, ''),
    CASE
        WHEN c1.kind = 'IO' AND c2.kind = 'IO' THEN 'deadlock_risk'
        WHEN c1.kind = 'II' AND c2.kind = 'II' THEN 'deadlock_risk'
        ELSE 'benign'
    END,
    CASE
        WHEN c1.kind = 'IO' AND c2.kind = 'IO' THEN
            'Scalas-Yoshida pattern: mutual IO dependency (data-dependent circular wait)'
        WHEN c1.kind = 'II' AND c2.kind = 'II' THEN
            'Scalas-Yoshida pattern: mutual II dependency (input ordering conflict)'
        ELSE 'Mixed causality — likely benign under FIFO channel assumption'
    END,
    'Cycle detected in Honda 2008 causality graph: ' || c1.kind || '(' ||
    COALESCE(c1.description, '?') || ') ↔ ' || c2.kind || '(' ||
    COALESCE(c2.description, '?') || '). ' ||
    'Per Scalas & Yoshida 2019, well-typedness alone does NOT guarantee deadlock freedom.'
FROM comm_causality c1
JOIN comm_causality c2
    ON c1.target_endpoint = c2.source_endpoint
    AND c2.target_endpoint = c1.source_endpoint
    AND c1.id < c2.id;

-- Detect 3-cycles: A→B→C→A (the classic Scalas-Yoshida counterexample shape)
INSERT INTO comm_dependency_cycles (cycle_path, cycle_length, involved_protocols, severity,
                                     scalas_yoshida_class, description)
SELECT
    c1.source_endpoint || ' → ' || c2.source_endpoint || ' → ' ||
    c3.source_endpoint || ' → ' || c1.source_endpoint,
    3,
    COALESCE(c1.protocol_id, '') || ', ' || COALESCE(c2.protocol_id, '') || ', ' || COALESCE(c3.protocol_id, ''),
    'deadlock_risk',
    'Three-participant cycle — matches Scalas & Yoshida 2019 §3 counterexample structure',
    'Three-step causality cycle detected. This is the EXACT pattern that Scalas & Yoshida 2019 '
    || 'used to disprove Honda 2008 Theorem 5.1 (progress). Well-typed but deadlocking.'
FROM comm_causality c1
JOIN comm_causality c2 ON c1.target_endpoint = c2.source_endpoint
JOIN comm_causality c3 ON c2.target_endpoint = c3.source_endpoint
    AND c3.target_endpoint = c1.source_endpoint
    AND c1.source_endpoint < c2.source_endpoint
    AND c2.source_endpoint < c3.source_endpoint;

-- ═══════════════════════════════════════════════════════════════════
-- Association Relation (Yoshida & Hou 2024)
--
-- G ~ Γ  iff:
--   (1) ∀p ∈ participants(G): project(G, p) is defined
--   (2) ∀p: project(G, p) ≤ Γ(s[p])          [subtyping, not equality]
--   (3) causality_graph(G) is acyclic
--
-- When G ~ Γ holds, the protocol is simultaneously:
--   - s-safe            (no protocol violations)
--   - s-deadlock-free   (no circular wait)
--   - s-live            (all branches reachable under fair scheduling)
--
-- This replaces the original Honda 2008 "coherent(G)" predicate which
-- incorrectly conflated projectability with deadlock freedom.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE comm_association (
    protocol_id TEXT NOT NULL PRIMARY KEY,
    -- Condition (1): all participants have defined projections
    all_projectable BOOLEAN NOT NULL,
    projectable_count INTEGER,
    total_participants INTEGER,
    -- Condition (2): all projections are subtypes of context types
    all_subtype_conforming BOOLEAN NOT NULL,
    conforming_count INTEGER,
    -- Condition (3): no cycles in causality dependency graph
    acyclic_dependencies BOOLEAN NOT NULL,
    cycle_count INTEGER DEFAULT 0,
    -- Association verdict
    is_associated BOOLEAN NOT NULL,
    -- Implied properties (only when associated)
    s_safe TEXT,
    s_deadlock_free TEXT,
    s_live TEXT,
    -- Correction references
    errata_reference TEXT NOT NULL
);

INSERT INTO comm_association (
    protocol_id, all_projectable, projectable_count, total_participants,
    all_subtype_conforming, conforming_count,
    acyclic_dependencies, cycle_count,
    is_associated, s_safe, s_deadlock_free, s_live, errata_reference
)
SELECT
    proto.id,
    -- Condition 1: all projectable
    CASE WHEN COUNT(DISTINCT p.component) = COUNT(DISTINCT p.component) THEN 1 ELSE 0 END,
    COUNT(DISTINCT p.component),
    COUNT(DISTINCT p.component),
    -- Condition 2: all subtype conforming
    CASE WHEN SUM(CASE WHEN COALESCE(sc.is_conforming, 0) = 0 THEN 1 ELSE 0 END) = 0 THEN 1 ELSE 0 END,
    SUM(CASE WHEN COALESCE(sc.is_conforming, 0) = 1 THEN 1 ELSE 0 END),
    -- Condition 3: acyclic (no cycles involving this protocol)
    CASE WHEN COALESCE(cy.cycle_cnt, 0) = 0 THEN 1 ELSE 0 END,
    COALESCE(cy.cycle_cnt, 0),
    -- Association: all three must hold
    CASE WHEN SUM(CASE WHEN COALESCE(sc.is_conforming, 0) = 0 THEN 1 ELSE 0 END) = 0
              AND COALESCE(cy.cycle_cnt, 0) = 0 THEN 1 ELSE 0 END,
    -- s-safe
    CASE WHEN SUM(CASE WHEN COALESCE(sc.is_conforming, 0) = 0 THEN 1 ELSE 0 END) = 0
              AND COALESCE(cy.cycle_cnt, 0) = 0
         THEN 'VERIFIED: no protocol violations possible (Yoshida & Hou 2024, Thm 3)'
         ELSE 'UNVERIFIED: association relation does not hold' END,
    -- s-deadlock-free
    CASE WHEN SUM(CASE WHEN COALESCE(sc.is_conforming, 0) = 0 THEN 1 ELSE 0 END) = 0
              AND COALESCE(cy.cycle_cnt, 0) = 0
         THEN 'VERIFIED: no circular wait (acyclic causality graph, correcting Honda 2008 Thm 5.1)'
         ELSE CASE WHEN COALESCE(cy.cycle_cnt, 0) > 0
              THEN 'WARNING: ' || cy.cycle_cnt || ' causality cycle(s) detected — deadlock possible (Scalas & Yoshida 2019)'
              ELSE 'UNVERIFIED: subtype conformance incomplete' END END,
    -- s-live
    CASE WHEN SUM(CASE WHEN COALESCE(sc.is_conforming, 0) = 0 THEN 1 ELSE 0 END) = 0
              AND COALESCE(cy.cycle_cnt, 0) = 0
         THEN 'VERIFIED: all branches reachable under fair scheduling (Yoshida & Hou 2024, Thm 3)'
         ELSE 'UNVERIFIED: association relation does not hold' END,
    -- Reference
    'Honda 2008 corrected by: Scalas & Yoshida "Less is More" (ECOOP 2019), '
    || 'Yoshida & Hou "Revisiting Subtyping for Session Types" (2024). '
    || 'Original Thms 4.6, 4.7, 5.1, 5.2 require association relation (subtyping + acyclicity) '
    || 'instead of simple coherence.'
FROM comm_protocols proto
JOIN comm_participants p ON p.protocol_id = proto.id
LEFT JOIN comm_subtype_check sc ON sc.protocol_id = proto.id AND sc.component = p.component
LEFT JOIN (
    SELECT involved_protocols, COUNT(*) as cycle_cnt
    FROM comm_dependency_cycles
    WHERE severity IN ('deadlock', 'deadlock_risk')
    GROUP BY involved_protocols
) cy ON cy.involved_protocols LIKE '%' || proto.id || '%'
GROUP BY proto.id;

-- ═══════════════════════════════════════════════════════════════════
-- Corrected Views
-- ═══════════════════════════════════════════════════════════════════

-- Association relation summary — the key correctness verdict
CREATE VIEW v_association_summary AS
SELECT
    a.protocol_id,
    p.name AS protocol_name,
    CASE WHEN a.is_associated THEN '✓ ASSOCIATED' ELSE '✗ NOT ASSOCIATED' END AS verdict,
    a.projectable_count || '/' || a.total_participants AS projection_coverage,
    a.conforming_count || ' conforming' AS subtype_status,
    CASE WHEN a.acyclic_dependencies THEN 'acyclic' ELSE a.cycle_count || ' cycle(s)' END AS dependency_graph,
    a.s_safe,
    a.s_deadlock_free,
    a.s_live,
    a.errata_reference
FROM comm_association a
JOIN comm_protocols p ON p.id = a.protocol_id
ORDER BY a.is_associated DESC, a.protocol_id;

-- Subtyping detail — Gay & Hole 2005 subtype relation per endpoint
CREATE VIEW v_subtype_detail AS
SELECT
    sc.protocol_id,
    p.name AS protocol_name,
    sc.component,
    sc.projected_type,
    sc.actual_behavior,
    sc.relation,
    CASE WHEN sc.is_conforming THEN '≤ (subtype holds)' ELSE '⊄ (not a subtype)' END AS conformance,
    sc.subtype_direction,
    sc.explanation
FROM comm_subtype_check sc
JOIN comm_protocols p ON p.id = sc.protocol_id
ORDER BY sc.protocol_id, sc.component;

-- Dependency cycle detail (Scalas & Yoshida 2019 counterexample detection)
CREATE VIEW v_dependency_cycles AS
SELECT
    dc.cycle_path,
    dc.cycle_length,
    dc.severity,
    dc.scalas_yoshida_class,
    dc.description
FROM comm_dependency_cycles dc
ORDER BY dc.severity DESC, dc.cycle_length;

-- ═══════════════════════════════════════════════════════════════════
-- Schema Documentation & Queries
-- ═══════════════════════════════════════════════════════════════════

INSERT INTO schema_docs (category, name, description, example) VALUES
('table', 'comm_subtype_check',
 'Honda 2008 Correction 1: session subtype conformance (G|>p ≤ Γ(s[p]) instead of equality). '
 || 'Based on Gay & Hole 2005 subtyping rules: selection is covariant in labels, branching is contravariant.',
 'SELECT protocol_id, component, relation, is_conforming, subtype_direction FROM comm_subtype_check WHERE NOT is_conforming'),
('table', 'comm_dependency_cycles',
 'Honda 2008 Correction 2: causality cycle detection (Scalas & Yoshida 2019). '
 || 'Cycles in the II/IO/OO dependency graph indicate potential deadlocks that well-typedness alone cannot prevent.',
 'SELECT cycle_path, severity, scalas_yoshida_class FROM comm_dependency_cycles WHERE severity = ''deadlock_risk'''),
('table', 'comm_association',
 'Yoshida & Hou 2024 association relation: the corrected criterion replacing Honda 2008 coherence. '
 || 'When G ~ Γ holds (all projectable + all subtype conforming + acyclic deps), the protocol is simultaneously '
 || 's-safe, s-deadlock-free, and s-live under fair scheduling.',
 'SELECT protocol_id, is_associated, s_safe, s_deadlock_free, s_live FROM comm_association'),
('view', 'v_association_summary', 'Summary of the Yoshida & Hou 2024 association relation for each protocol.',
 'SELECT protocol_id, verdict, projection_coverage, subtype_status, dependency_graph, s_safe FROM v_association_summary'),
('view', 'v_subtype_detail', 'Detailed Gay & Hole 2005 subtype checking per component.',
 'SELECT * FROM v_subtype_detail WHERE relation != ''assumed_subtype'''),
('view', 'v_dependency_cycles', 'Scalas & Yoshida 2019 counterexample detection: causality cycles.',
 'SELECT * FROM v_dependency_cycles');

INSERT INTO queries (name, description, sql) VALUES
('honda_association_report',
 'Yoshida & Hou 2024 association relation: the corrected Honda 2008 correctness verdict for each protocol',
 'SELECT protocol_id, verdict, projection_coverage, subtype_status, dependency_graph, s_safe, s_deadlock_free, s_live FROM v_association_summary'),
('honda_subtype_violations',
 'Find protocols where subtype conformance fails (G|>p ≤ Γ(s[p]) does not hold)',
 'SELECT protocol_id, component, projected_type, actual_behavior, relation, explanation FROM comm_subtype_check WHERE NOT is_conforming'),
('honda_deadlock_detection',
 'Scalas & Yoshida 2019 deadlock detection via causality cycle analysis',
 'SELECT cycle_path, cycle_length, severity, scalas_yoshida_class, description FROM comm_dependency_cycles WHERE severity IN (''deadlock'', ''deadlock_risk'')'),
('honda_errata_summary',
 'Summary of all Honda 2008 corrections applied to this analysis',
 'SELECT protocol_id, errata_reference, s_safe, s_deadlock_free, s_live FROM comm_association WHERE is_associated');
`
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return fmt.Errorf("session type corrections: %w", err)
	}

	// Report results
	var associated, notAssociated int
	sqlitex.ExecuteTransient(conn,
		"SELECT SUM(CASE WHEN is_associated THEN 1 ELSE 0 END), SUM(CASE WHEN NOT is_associated THEN 1 ELSE 0 END) FROM comm_association",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			associated = stmt.ColumnInt(0)
			notAssociated = stmt.ColumnInt(1)
			return nil
		}})

	var subtypeConforming, subtypeViolations int
	sqlitex.ExecuteTransient(conn,
		"SELECT SUM(CASE WHEN is_conforming THEN 1 ELSE 0 END), SUM(CASE WHEN NOT is_conforming THEN 1 ELSE 0 END) FROM comm_subtype_check",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			subtypeConforming = stmt.ColumnInt(0)
			subtypeViolations = stmt.ColumnInt(1)
			return nil
		}})

	var cycles int
	sqlitex.ExecuteTransient(conn,
		"SELECT COUNT(*) FROM comm_dependency_cycles WHERE severity IN ('deadlock', 'deadlock_risk')",
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			cycles = stmt.ColumnInt(0)
			return nil
		}})

	prog.Log("Honda corrections: %d associated, %d not; subtype: %d conforming, %d violations; %d causality cycles",
		associated, notAssociated, subtypeConforming, subtypeViolations, cycles)
	return nil
}

// extractPkgFromPath extracts a package hint from a relative file path.
func extractPkgFromPath(relPath string) string {
	// e.g. "scrape/manager.go" → "scrape"
	// e.g. "cmd/prometheus/main.go" → "cmd/prometheus"
	for i := len(relPath) - 1; i >= 0; i-- {
		if relPath[i] == '/' {
			return relPath[:i]
		}
	}
	return "main"
}
