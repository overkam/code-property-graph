# Decisions and Trade-offs

## Technology stack

- **Backend:** Go (Chi router), pure-Go SQLite driver (`modernc.org/sqlite`), no CGO. Same language as the CPG generator; simple to run and deploy.
- **Frontend:** React (Vite). Single-page app with one main screen; graph in the centre, side panels for metadata and source code.
- **Graph:** Cytoscape.js — ready-made layouts and interactivity, good fit for 10–100 node subgraphs.
- **Database:** SQLite (`output.db` from cpg-gen). Single connection, read-only for the app; limits on query size keep latency acceptable on a ~900 MB DB.

---

## What we chose to build

We implemented all three suggested directions:

1. **Call Graph Explorer** — search by name → load function neighborhood (BFS over `call` edges, limit 60 nodes) → interactive graph; click node → load its neighborhood and show source from `sources`.
2. **Package Architecture Map** — package dependency graph from `dashboard_package_graph` / `dashboard_package_treemap`; node size by complexity, color by module; click package → list of functions → load call graph for a function.
3. **Data Flow Slicer** — backward/forward slice from selected node; DFG subgraph visualization; highlight participating lines in the code panel.

Choices about **which data matters**:

- **Search:** `symbol_index` for function/symbol lookup.
- **Subgraphs:** Built from `function_neighborhood`, `backward_slice` / `forward_slice`, and dashboard views; unified format `{ nodes, edges }` for all views.
- **Code:** Table `sources` by file path; one highlighted line per selected node plus a set of highlighted lines for the current slice.
- **Scale:** All responses are limited (e.g. 60 nodes for call graph, 200 for package graph nodes, 200 for slice) so the UI stays responsive and the graph remains readable (10–100 nodes per view as suggested).

---

## Architecture and UX

- **Graph in the centre:** The main area is the graph; metadata (search, package list, slice buttons) and code are in side panels. This matches the requirement that “graph visualization must be a central part of the experience.”
- **Single graph component:** One `GraphView` is used for call graph, package graph, and DFG slice; mode is inferred from data (e.g. presence of `total_complexity` for package map) or from state (call vs DFG). Reduces duplication and keeps behaviour consistent.
- **URL state:** Search query and selected `node_id` are reflected in the URL so that links and refresh restore the same view.

---

## Trade-offs

| Area | Decision | Trade-off |
|------|----------|-----------|
| **Database in Docker** | DB is not baked into the image (~900 MB). It lives in a host volume `./data`; container uses `DB_PATH=/data/output.db`. | Image stays small and reusable; first run either generates the DB in the container (four modules, takes a few minutes) or the user places a pre-built `output.db` in `./data`. |
| **Subgraph size** | Strict limits: call graph 60 nodes, package graph 200 nodes / 500 edges, slice 200 nodes. | Prevents overload and keeps rendering fast; very large neighborhoods are truncated. |
| **Backend queries** | SQL is in code (`server/queries.go`), not loaded from the DB `queries` table at runtime. | Simpler implementation and explicit limits; changing queries requires a rebuild. |
| **CORS** | `Access-Control-Allow-Origin: *` for API. | Fine for a take-home and local use; for production, origins would be restricted. |

---