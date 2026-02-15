# Take-Home Assignment — Full-Stack Developer

## Quick start (Docker)

To run the app with no extra steps:

```bash
git clone <repository-url>
cd cpg-test-release
docker-compose up
```

Submodules are fetched automatically during the image build. Open http://localhost:8080 when the container is up. The first run may take several minutes while the CPG database is generated.

---

## Context

This archive ships `cpg-gen` — a Code Property Graph (CPG) generator for Go projects. A CPG fuses the abstract syntax tree, control flow graph, data flow graph, call graph, type system, and static analysis results into a single queryable graph stored as an SQLite database.

Three Go modules are included: **Prometheus**, **client_golang**, and **prometheus-adapter**. You will add a fourth module yourself (see below).

## Prerequisites

- **Go 1.25+** — the generator requires Go 1.25.0 or later

## Building and Generation

Build the generator and produce the CPG database. The primary module is `./prometheus`; additional modules are specified with the `-modules` flag. Run `./cpg-gen -help` for all available options.

Use this value for `-modules`:

```
./client_golang:github.com/prometheus/client_golang:client_golang,./prometheus-adapter:sigs.k8s.io/prometheus-adapter:adapter
```

Pick a **fourth Go module** from the Prometheus ecosystem — alertmanager, node_exporter, pushgateway, blackbox_exporter, or any other — add it via the same `-modules` flag, and regenerate the database.

The database is self-documenting: the `schema_docs` table describes every table and column; the `queries` table contains ready-made SQL for common operations. Start there.

### What to expect

The generated database is roughly **900 MB** and contains approximately **555,000 nodes** and **1,500,000 edges**. Design your application with this scale in mind.

## Task

Build a web application (an in-browser IDE) that lets a developer explore and understand a codebase through the lens of its CPG.

Technology stack is entirely your choice — use whatever languages, frameworks, and libraries you believe produce the best result. What matters is a well-engineered product.

One hard constraint: **graph visualization must be a central part of the experience**, not a sidebar widget.

## Example Features

Three directions to consider. You may pursue any one, combine several, or take an entirely different approach.

### 1. Call Graph Explorer

Click a function → BFS over `call` edges → render an interactive call graph (10–60 nodes). Click any node to navigate into its neighborhood. Display source code from the `sources` table on selection.

Relevant built-in queries: `function_neighborhood`, `call_chain`, `callers_of`.

### 2. Data Flow Slicer

Select a variable → trace backward or forward along `dfg` edges → visualize the data path from definition to use. Overlay the slice onto source code by highlighting the participating lines.

Relevant built-in queries: `backward_slice`, `forward_slice`, `data_flow_path`.

### 3. Package Architecture Map

Render the package dependency graph from `dashboard_package_graph` (~170 packages, ~400 edges). Size nodes by complexity (`dashboard_package_treemap`), color them by module. Click a package to drill down into its functions via `dashboard_function_detail`.

## What We're Looking For

- **Deliberate choices** — which data from the database matters most to a developer, and why you chose it
- **A working prototype** that handles the full dataset, not a static mockup
- **Focused subgraphs** (10–100 nodes per view) rather than an attempt to render the entire graph at once

## Evaluation Criteria

| Criterion | Weight |
|---|---|
| Graph work — visualization, interactivity, meaningful subgraph selection | 25% |
| Developer utility — how effectively the tool aids code comprehension | 20% |
| Engineering quality — architecture, clean code, separation of concerns | 20% |
| Performance — smooth operation on the full dataset | 15% |
| Schema exploration — depth of investigation, creative use of the data | 10% |
| UI/UX — intuitive interface, loading states, error handling | 10% |

## Submission Format

- A git repository with clear setup instructions
- **A `docker-compose.yml` is required.** We must be able to run `docker compose up` and have the application fully operational — no manual setup steps beyond cloning the repo
- A brief write-up of the decisions you made and any trade-offs (in the README or a separate file)

## Deadline

24 hours from the moment you begin.

## Questions?

If anything is unclear or you run into issues, reach out to [matvei@theartisan.ai](mailto:matvei@theartisan.ai).
