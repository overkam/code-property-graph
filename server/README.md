# CPG API Server

HTTP server that exposes the Code Property Graph over SQLite: REST API for subgraphs, search, source code, and optional SPA static files.

## Requirements

- Go 1.21+
- SQLite database file (e.g. `output.db` produced by the CPG pipeline)

## Run

From the `server/` directory:

```bash
go mod tidy
go run . -db /path/to/output.db -port 8080
```

With SPA static files (e.g. after building the client into `client/dist`):

```bash
# From server/
go run . -db /path/to/output.db -port 8080 -static ../client/dist
```

Then open `http://localhost:8080` (or `http://localhost:8080/explore` for the app).

## Options

| Flag       | Env var     | Description |
|------------|-------------|--------------|
| `-db`      | `DB_PATH`   | Path to the SQLite `*.db` file (required) |
| `-port`    | `PORT`      | HTTP port (default: 8080) |
| `-static`  | `STATIC_DIR`| Directory for SPA static files (optional) |

## API

All endpoints live under `/api`; responses are JSON.

| Endpoint | Description |
|----------|-------------|
| `GET /api/search?q=...` | Search functions/packages by name |
| `GET /api/subgraph?node_id=...` | Call-graph neighborhood of a node |
| `GET /api/package-graph` | Package dependency graph |
| `GET /api/package/functions?package=...` | Functions in a package |
| `GET /api/source?file=...` | Source file content |
| `GET /api/slice?node_id=...&direction=backward\|forward` | Data-flow slice |

Details, parameters, and examples: [docs/API.md](../docs/API.md).

## Production build

```bash
go build -o cpg-server .
./cpg-server -db /path/to/output.db -port 8080 -static ../client/dist
```
