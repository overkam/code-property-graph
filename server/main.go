package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := flag.String("db", "", "Path to SQLite database (e.g. output.db). Can be set via DB_PATH env.")
	port := flag.String("port", "8080", "HTTP port. Can be set via PORT env.")
	staticDir := flag.String("static", "", "Directory for SPA static files (e.g. client/dist). Can be set via STATIC_DIR env.")
	flag.Parse()

	if *dbPath == "" {
		*dbPath = os.Getenv("DB_PATH")
	}
	if *dbPath == "" {
		log.Fatal("DB path required: set -db or DB_PATH")
	}
	if *port == "" {
		*port = os.Getenv("PORT")
	}
	if *port == "" {
		*port = "8080"
	}
	if *staticDir == "" {
		*staticDir = os.Getenv("STATIC_DIR")
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		log.Fatalf("ping db: %v", err)
	}

	// Ensure dashboard tables exist (they are created by the CPG pipeline; if DB was created
	// without full pipeline or is old schema, they may be missing — create empty tables so API returns 200 with empty data).
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS dashboard_package_graph (
			source TEXT NOT NULL,
			target TEXT NOT NULL,
			weight INTEGER NOT NULL,
			PRIMARY KEY (source, target)
		);
		CREATE TABLE IF NOT EXISTS dashboard_package_treemap (
			package TEXT PRIMARY KEY,
			file_count INTEGER,
			function_count INTEGER,
			total_loc INTEGER,
			total_complexity INTEGER,
			avg_complexity REAL,
			max_complexity INTEGER,
			type_count INTEGER,
			interface_count INTEGER
		);
	`); err != nil {
		log.Fatalf("ensure dashboard tables: %v", err)
	}

	app := NewApp(db, *staticDir)
	srv := &http.Server{
		Addr:         ":" + *port,
		Handler:      app.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	go func() {
		log.Printf("Listening on http://localhost:%s (db=%s)", *port, *dbPath)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
		os.Exit(1)
	}
	log.Println("Bye")
}
