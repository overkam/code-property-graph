package main

// SQL constants aligned with docs/SCHEMA_AND_QUERIES.md and db.go (CPG generator).
// Parameters use ? for sqlite named params; we bind by position or use DB exec with named args.

const querySymbolSearch = `
SELECT id, name, kind, package, file, line FROM symbol_index WHERE name LIKE ? ORDER BY kind, name LIMIT ?
`

const queryFunctionNeighborhood = `
SELECT 'caller' AS direction, n.id, n.name, n.package, n.file, n.line
FROM edges e JOIN nodes n ON n.id = e.source
WHERE e.target = ? AND e.kind = 'call' AND n.kind = 'function'
UNION ALL
SELECT 'callee' AS direction, n.id, n.name, n.package, n.file, n.line
FROM edges e JOIN nodes n ON n.id = e.target
WHERE e.source = ? AND e.kind = 'call' AND n.kind = 'function'
ORDER BY direction, name
LIMIT ?
`

const queryCallEdgesForNeighborhood = `
SELECT source, target, 'call' AS kind FROM edges
WHERE kind = 'call' AND (source = ? OR target = ?)
LIMIT ?
`

const queryCallChain = `
WITH RECURSIVE chain(id, depth, path) AS (
  SELECT ?, 0, ?
  UNION
  SELECT e.target, c.depth + 1, c.path || ' -> ' || e.target
  FROM chain c JOIN edges e ON e.source = c.id
  WHERE e.kind = 'call' AND c.depth < 10
    AND c.path NOT LIKE '%' || e.target || '%'
)
SELECT DISTINCT n.id, n.name, n.package, n.file, n.line, c.depth
FROM chain c JOIN nodes n ON n.id = c.id
ORDER BY c.depth, n.name
LIMIT ?
`

const queryCallChainEdges = `
SELECT e.source, e.target, e.kind FROM edges e
WHERE e.kind = 'call'
  AND e.source IN (SELECT id FROM (SELECT id FROM nodes WHERE id = ? UNION SELECT unnest FROM (SELECT ? AS id))) 
  OR e.target IN (SELECT id FROM (SELECT id FROM nodes WHERE id = ? UNION SELECT unnest FROM (SELECT ? AS id)))
`
// Simpler: we'll collect node IDs from call_chain result and then query edges where source,target in (ids).

const queryNodesByIDs = `SELECT id, kind, name, file, line, end_line, package, parent_function, type_info FROM nodes WHERE id = ?`

// Limits for performance (block 7): cap package graph size per request.
const maxPackageGraphNodes = 200
const maxPackageGraphEdges = 500

const queryDashboardPackageGraph = `SELECT source, target, weight FROM dashboard_package_graph ORDER BY weight DESC LIMIT ?`
const queryDashboardPackageTreemap = `SELECT package, file_count, function_count, total_loc, total_complexity, avg_complexity, max_complexity, type_count, interface_count FROM dashboard_package_treemap LIMIT ?`

const queryDashboardFunctionDetailByPackage = `
SELECT function_id, name, package, file, COALESCE(line, 0), COALESCE(end_line, 0), signature,
  COALESCE(complexity,0), COALESCE(loc,0), COALESCE(fan_in,0), COALESCE(fan_out,0),
  COALESCE(num_params,0), COALESCE(num_locals,0), COALESCE(num_calls,0), COALESCE(num_branches,0), COALESCE(num_returns,0), COALESCE(finding_count,0), callers, callees
FROM dashboard_function_detail
WHERE package = ? OR package LIKE ?
ORDER BY name LIMIT 200
`

const querySourceByFile = `SELECT file, content, package FROM sources WHERE file = ?`

const queryBackwardSlice = `
WITH RECURSIVE slice(id, depth) AS (
  SELECT ?, 0
  UNION
  SELECT e.source, s.depth + 1
  FROM slice s JOIN edges e ON e.target = s.id
  WHERE e.kind IN ('dfg', 'param_in') AND s.depth < 20
)
SELECT DISTINCT n.id, n.kind, n.name, n.file, n.line, n.end_line, n.package, n.parent_function, n.type_info
FROM slice s JOIN nodes n ON n.id = s.id
ORDER BY n.file, n.line
LIMIT ?
`

const queryForwardSlice = `
WITH RECURSIVE slice(id, depth) AS (
  SELECT ?, 0
  UNION
  SELECT e.target, s.depth + 1
  FROM slice s JOIN edges e ON e.source = s.id
  WHERE e.kind IN ('dfg', 'param_out') AND s.depth < 20
)
SELECT DISTINCT n.id, n.kind, n.name, n.file, n.line, n.end_line, n.package, n.parent_function, n.type_info
FROM slice s JOIN nodes n ON n.id = s.id
ORDER BY n.file, n.line
LIMIT ?
`

// querySliceEdges is built dynamically with placeholders for node IDs (see db_slice.go).
