# CPG Explorer — Frontend

SPA for exploring the Code Property Graph: graph view, metadata panel, and source code panel.

## Development

```bash
npm install
npm run dev
```

Runs at `http://localhost:5173`. API requests to `/api` are proxied to `http://localhost:8080` — start the backend (Block 2) with the DB and without `-static` for dev.

## Production build

```bash
npm run build
```

Output: `client/dist`. Serve via the backend:

```bash
cd ../server
go run . -db /path/to/output.db -port 8080 -static ./client/dist
```

Then open `http://localhost:8080`.
