package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "modernc.org/sqlite"
)

// setupTestDB creates an in-memory SQLite DB with minimal CPG schema and test data.
func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`
	CREATE TABLE symbol_index (id TEXT, name TEXT, kind TEXT, package TEXT, file TEXT, line INTEGER);
	CREATE TABLE nodes (id TEXT PRIMARY KEY, kind TEXT, name TEXT, file TEXT, line INTEGER, end_line INTEGER, package TEXT, parent_function TEXT, type_info TEXT);
	CREATE TABLE edges (source TEXT, target TEXT, kind TEXT);
	CREATE TABLE sources (file TEXT PRIMARY KEY, content TEXT, package TEXT);
	CREATE TABLE dashboard_package_graph (source TEXT, target TEXT, weight INTEGER);
	CREATE TABLE dashboard_package_treemap (package TEXT PRIMARY KEY, file_count INTEGER, function_count INTEGER, total_loc INTEGER, total_complexity INTEGER, avg_complexity REAL, max_complexity INTEGER, type_count INTEGER, interface_count INTEGER);
	CREATE TABLE dashboard_function_detail (function_id TEXT PRIMARY KEY, name TEXT, package TEXT, file TEXT, line INTEGER, end_line INTEGER, signature TEXT, complexity INTEGER, loc INTEGER, fan_in INTEGER, fan_out INTEGER, num_params INTEGER, num_locals INTEGER, num_calls INTEGER, num_branches INTEGER, num_returns INTEGER, finding_count INTEGER, callers TEXT, callees TEXT);
	`)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}

	_, _ = db.Exec(`INSERT INTO symbol_index VALUES ('main::Handler@main.go:10:1', 'Handler', 'function', 'main', 'main.go', 10);`)
	_, _ = db.Exec(`INSERT INTO nodes VALUES ('main::Handler@main.go:10:1', 'function', 'Handler', 'main.go', 10, 20, 'main', NULL, NULL);`)
	_, _ = db.Exec(`INSERT INTO nodes VALUES ('main::Run@main.go:5:1', 'function', 'Run', 'main.go', 5, 8, 'main', NULL, NULL);`)
	_, _ = db.Exec(`INSERT INTO edges VALUES ('main::Run@main.go:5:1', 'main::Handler@main.go:10:1', 'call');`)
	_, _ = db.Exec(`INSERT INTO edges VALUES ('main::Run@main.go:5:1', 'main::Handler@main.go:10:1', 'dfg');`)
	_, _ = db.Exec(`INSERT INTO edges VALUES ('main::Handler@main.go:10:1', 'main::Run@main.go:5:1', 'param_out');`)
	_, _ = db.Exec(`INSERT INTO sources VALUES ('main.go', 'package main\n\nfunc Handler() {}', 'main');`)
	_, _ = db.Exec(`INSERT INTO dashboard_package_graph VALUES ('pkg_a', 'pkg_b', 5);`)
	_, _ = db.Exec(`INSERT INTO dashboard_package_treemap VALUES ('main', 1, 2, 100, 10, 1.5, 5, 0, 0);`)
	_, _ = db.Exec(`INSERT INTO dashboard_package_treemap VALUES ('pkg_a', 1, 1, 50, 5, 1.0, 3, 0, 0);`)
	_, _ = db.Exec(`INSERT INTO dashboard_package_treemap VALUES ('pkg_b', 1, 1, 50, 5, 1.0, 3, 0, 0);`)
	_, _ = db.Exec(`INSERT INTO dashboard_function_detail VALUES ('main::Handler@main.go:10:1', 'Handler', 'main', 'main.go', 10, 20, 'func Handler()', 1, 5, 0, 1, 0, 0, 0, 0, 0, 0, '', 'Run');`)

	return db
}

func TestAPI_Search_MissingParam(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/search", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("GET /api/search without q: want 400, got %d", rec.Code)
	}
}

func TestAPI_Search_Success(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/search?q=Handler", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /api/search?q=Handler: want 200, got %d", rec.Code)
	}
	var nodes []Node
	if err := json.NewDecoder(rec.Body).Decode(&nodes); err != nil {
		t.Fatalf("decode search response: %v", err)
	}
	if len(nodes) < 1 {
		t.Error("expected at least one node from search")
	}
	if nodes[0].ID != "main::Handler@main.go:10:1" || nodes[0].Name != "Handler" {
		t.Errorf("unexpected node: %+v", nodes[0])
	}
}

func TestAPI_Subgraph_MissingParam(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/subgraph", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("GET /api/subgraph without node_id: want 400, got %d", rec.Code)
	}
}

func TestAPI_Subgraph_Success(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/subgraph?node_id=main::Handler@main.go:10:1", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /api/subgraph: want 200, got %d", rec.Code)
	}
	var sg Subgraph
	if err := json.NewDecoder(rec.Body).Decode(&sg); err != nil {
		t.Fatalf("decode subgraph: %v", err)
	}
	if len(sg.Nodes) == 0 {
		t.Error("subgraph should have at least central node")
	}
	// Block 3 expects nodes[].id, nodes[].name, nodes[].kind, edges[].source, edges[].target, edges[].kind
	for i, n := range sg.Nodes {
		if n.ID == "" {
			t.Errorf("nodes[%d].id empty", i)
		}
	}
	for i, e := range sg.Edges {
		if e.Source == "" || e.Target == "" || e.Kind == "" {
			t.Errorf("edges[%d] missing source/target/kind", i)
		}
	}
}

func TestAPI_PackageGraph_Success(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/package-graph", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /api/package-graph: want 200, got %d", rec.Code)
	}
	var resp PackageGraphResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode package-graph: %v", err)
	}
	if len(resp.Nodes) == 0 {
		t.Error("package-graph nodes expected")
	}
	if len(resp.Edges) == 0 {
		t.Error("package-graph edges expected")
	}
}

func TestAPI_PackageFunctions_MissingParam(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/package/functions", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("GET /api/package/functions without package: want 400, got %d", rec.Code)
	}
}

func TestAPI_Source_MissingParam(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/source", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("GET /api/source without file: want 400, got %d", rec.Code)
	}
}

func TestAPI_Source_Success(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/source?file=main.go", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /api/source?file=main.go: want 200, got %d", rec.Code)
	}
	var out struct {
		File    string `json:"file"`
		Package string `json:"package"`
		Content string `json:"content"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode source: %v", err)
	}
	if out.File != "main.go" || out.Content == "" {
		t.Errorf("unexpected source response: %+v", out)
	}
}

func TestAPI_Source_NotFound(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/source?file=nonexistent.go", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /api/source?file=nonexistent: want 404, got %d", rec.Code)
	}
}

func TestAPI_Slice_MissingParam(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/slice", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("GET /api/slice without node_id: want 400, got %d", rec.Code)
	}
}

func TestAPI_Slice_Success(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/slice?node_id=main::Handler@main.go:10:1&direction=backward", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /api/slice (backward): want 200, got %d", rec.Code)
	}
	var sg Subgraph
	if err := json.NewDecoder(rec.Body).Decode(&sg); err != nil {
		t.Fatalf("decode slice response: %v", err)
	}
	if len(sg.Nodes) == 0 {
		t.Error("slice (backward) should return at least the seed node")
	}
	// Should contain Handler and optionally Run (via dfg edge)
	ids := make(map[string]bool)
	for _, n := range sg.Nodes {
		ids[n.ID] = true
	}
	if !ids["main::Handler@main.go:10:1"] {
		t.Error("slice should contain seed node main::Handler@main.go:10:1")
	}
}

func TestAPI_Slice_Forward(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/slice?node_id=main::Handler@main.go:10:1&direction=forward", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /api/slice (forward): want 200, got %d", rec.Code)
	}
	var sg Subgraph
	if err := json.NewDecoder(rec.Body).Decode(&sg); err != nil {
		t.Fatalf("decode slice response: %v", err)
	}
	if len(sg.Nodes) == 0 {
		t.Error("slice (forward) should return at least the seed node")
	}
}

func TestAPI_PackageFunctions_Success(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/package/functions?package=main", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /api/package/functions?package=main: want 200, got %d", rec.Code)
	}
	var list []FunctionDetail
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		t.Fatalf("decode package/functions response: %v", err)
	}
	if len(list) < 1 {
		t.Error("expected at least one function for package main")
	}
	if list[0].FunctionID != "main::Handler@main.go:10:1" || list[0].Name != "Handler" {
		t.Errorf("unexpected first function: %+v", list[0])
	}
}

func TestAPI_CORS(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/search?q=x", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	if origin := rec.Header().Get("Access-Control-Allow-Origin"); origin != "*" {
		t.Errorf("CORS Access-Control-Allow-Origin: want *, got %q", origin)
	}
}

func TestAPI_ContentType(t *testing.T) {
	db := setupTestDB(t)
	app := NewApp(db, "")
	req := httptest.NewRequest(http.MethodGet, "/api/package-graph", nil)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, req)
	ct := rec.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: want application/json; charset=utf-8, got %q", ct)
	}
}
