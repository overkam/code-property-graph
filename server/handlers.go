package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
)

func (a *App) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "missing query parameter q", http.StatusBadRequest)
		return
	}
	limitStr := r.URL.Query().Get("limit")
	limit, atoiErr := strconv.Atoi(limitStr)
	if limitStr != "" && atoiErr != nil {
		log.Printf("search: invalid limit %q, using default", limitStr)
	}
	nodes, err := a.db.Search(q, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, nodes)
}

func (a *App) handleSubgraph(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("node_id")
	if nodeID == "" {
		http.Error(w, "missing query parameter node_id", http.StatusBadRequest)
		return
	}
	limitStr := r.URL.Query().Get("limit")
	limit, atoiErr := strconv.Atoi(limitStr)
	if limitStr != "" && atoiErr != nil {
		log.Printf("subgraph: invalid limit %q, using default", limitStr)
	}
	sg, err := a.db.Subgraph(nodeID, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, sg)
}

func (a *App) handlePackageGraph(w http.ResponseWriter, r *http.Request) {
	resp, err := a.db.PackageGraph()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

func (a *App) handlePackageFunctions(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("package")
	if id == "" {
		http.Error(w, "missing query parameter package", http.StatusBadRequest)
		return
	}
	list, err := a.db.PackageFunctions(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, list)
}

func (a *App) handleSource(w http.ResponseWriter, r *http.Request) {
	file := r.URL.Query().Get("file")
	if file == "" {
		http.Error(w, "missing query parameter file", http.StatusBadRequest)
		return
	}
	content, pkg, err := a.db.Source(file)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "file not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"file": file, "package": pkg, "content": content})
}

func (a *App) handleSlice(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("node_id")
	if nodeID == "" {
		http.Error(w, "missing query parameter node_id", http.StatusBadRequest)
		return
	}
	direction := r.URL.Query().Get("direction")
	if direction == "" {
		direction = "backward"
	}
	limitStr := r.URL.Query().Get("limit")
	limit, atoiErr := strconv.Atoi(limitStr)
	if limitStr != "" && atoiErr != nil {
		log.Printf("slice: invalid limit %q, using default", limitStr)
	}
	sg, err := a.db.Slice(nodeID, direction, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, sg)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
