package main

import (
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// App holds server dependencies.
type App struct {
	db        *DB
	staticDir string
}

// NewApp creates an App with the given database and optional static directory.
func NewApp(db *sql.DB, staticDir string) *App {
	return &App{
		db:        NewDB(db),
		staticDir: strings.TrimSuffix(staticDir, "/"),
	}
}

// Handler returns the HTTP handler (router with CORS, recovery, routes).
func (a *App) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(corsMiddleware)

	r.Route("/api", func(r chi.Router) {
		r.Get("/search", a.handleSearch)
		r.Get("/subgraph", a.handleSubgraph)
		r.Get("/package-graph", a.handlePackageGraph)
		r.Get("/package/functions", a.handlePackageFunctions)
		r.Get("/source", a.handleSource)
		r.Get("/slice", a.handleSlice)
	})

	// SPA: serve static files if dir set, else 404 for /
	if a.staticDir != "" {
		r.Get("/*", a.serveSPA)
	} else {
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "No static dir configured (set -static or STATIC_DIR)", http.StatusNotFound)
		})
	}

	return r
}

// corsMiddleware sets CORS headers for API so frontend on another port can call.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// serveSPA serves index.html for SPA routes and static files from staticDir.
func (a *App) serveSPA(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	fpath := filepath.Join(a.staticDir, filepath.Clean(path))
	if info, err := os.Stat(fpath); err == nil && !info.IsDir() {
		http.ServeFile(w, r, fpath)
		return
	}
	// Client-side routing: any other path → index.html
	indexPath := filepath.Join(a.staticDir, "index.html")
	if _, err := os.Stat(indexPath); err == nil {
		http.ServeFile(w, r, indexPath)
		return
	}
	http.NotFound(w, r)
}
