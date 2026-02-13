package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// run is the real entry point. Using a separate function ensures all defers
// (including temp file cleanup) execute even on error paths, unlike os.Exit
// which skips deferred calls.
func run() error {
	skipGenerated := flag.Bool("skip-generated", true, "Skip .pb.go files")
	skipTests := flag.Bool("skip-tests", true, "Skip _test.go files")
	verbose := flag.Bool("verbose", false, "Print detailed progress")
	validate := flag.Bool("validate", false, "Run validation queries after write")
	modules := flag.String("modules", "", "Comma-separated dir:modpath:name triples for additional modules (e.g. ./adapter:sigs.k8s.io/prometheus-adapter:adapter)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cpg-gen [flags] <primary-dir> <output.db>\n\n")
		fmt.Fprintf(os.Stderr, "Generates a Code Property Graph (CPG) SQLite database from Go modules.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		return fmt.Errorf("expected 2 arguments, got %d", flag.NArg())
	}

	promDir, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		return fmt.Errorf("invalid primary dir: %w", err)
	}
	outputPath := flag.Arg(1)

	// Set memory limit for GC pressure
	debug.SetMemoryLimit(8 * 1024 * 1024 * 1024) // 8 GiB

	// Wire skip flags into the package-level config used by shouldSkipFile
	flagSkipGenerated = *skipGenerated
	flagSkipTests = *skipTests

	prog := NewProgress(*verbose)

	// Build ModuleSet from primary dir + extra modules
	primary := ModuleInfo{
		ModPath: "github.com/prometheus/prometheus",
		Dir:     promDir,
		Prefix:  "", // primary module keeps paths unprefixed for backward compat
	}

	var extras []ModuleInfo
	if *modules != "" {
		for _, spec := range strings.Split(*modules, ",") {
			parts := strings.SplitN(strings.TrimSpace(spec), ":", 3)
			if len(parts) != 3 {
				prog.Log("Warning: invalid --modules spec %q (want dir:modpath:name)", spec)
				continue
			}
			dir, err := filepath.Abs(parts[0])
			if err != nil {
				prog.Log("Warning: invalid module dir %q: %v", parts[0], err)
				continue
			}
			extras = append(extras, ModuleInfo{
				Dir:     dir,
				ModPath: parts[1],
				Prefix:  parts[2],
			})
		}
	}

	modSet = NewModuleSet(primary, extras)
	prog.Log("Analyzing %d modules: %s", len(modSet.Dirs()), moduleNames(modSet))

	// Create temporary go.work for unified type universe
	goworkPath, err := CreateTempGoWork(modSet)
	if err != nil {
		return err
	}
	defer os.Remove(goworkPath)
	prog.Verbose("Created workspace: %s", goworkPath)

	cpg := NewCPG()

	// Phase 1: Load packages (all modules, single type universe)
	loadResult, err := LoadPackages(goworkPath, prog)
	if err != nil {
		return err
	}

	// Phase 2: Walk AST → nodes + AST edges + position lookup
	posLookup, funcLookup := WalkAST(loadResult.Packages, loadResult.Fset, cpg, prog)

	// Phase 3: Build SSA
	ssaResult := BuildSSA(loadResult.Packages, prog)

	// Phase 4: Extract CFG + DFG from SSA
	ExtractCFGAndDFG(ssaResult, loadResult.Fset, posLookup, funcLookup, cpg, prog)

	// Phase 4b: Extract CDG from post-dominator tree
	ExtractCDG(ssaResult, loadResult.Fset, funcLookup, cpg, prog)

	// Phase 4c: Extract channel send→receive flow edges
	ExtractChannelFlow(ssaResult, loadResult.Fset, posLookup, cpg, prog)

	// Phase 4d: Extract panic/recover flow edges
	ExtractPanicRecover(ssaResult, loadResult.Fset, posLookup, funcLookup, cpg, prog)

	// Phase 5: Build VTA call graph → call edges
	BuildCallGraph(ssaResult, loadResult.Fset, posLookup, funcLookup, cpg, prog)

	// Phase 6: Extract type relationships (implements, embeds)
	ExtractTypeRelationships(loadResult.Packages, loadResult.Fset, posLookup, cpg, prog)

	// Phase 7: Compute function metrics
	ComputeMetrics(loadResult.Packages, loadResult.Fset, funcLookup, cpg, prog)

	// Phase 7b: Fill fan-in/fan-out from call graph
	ComputeFanInOut(cpg)

	// Add META_DATA node with generator info
	cpg.AddNode(Node{
		ID:   "META_DATA",
		Kind: "meta_data",
		Name: "CPG Metadata",
		Properties: map[string]any{
			"language":  "go",
			"version":   "1.0",
			"generator": "cpg-gen",
			"root":      promDir,
			"modules":   len(modSet.Dirs()),
		},
	})

	// Phase 7c: Escape analysis from Go compiler (all modules)
	escapeResults := RunEscapeAnalysis(prog)

	// Phase 7d: Git history for diff-aware analysis (all modules)
	gitHistory := RunGitHistory(prog)

	// Phase 8: Write SQLite
	if err := WriteDB(outputPath, cpg, escapeResults, gitHistory, *validate, prog); err != nil {
		return err
	}

	prog.Log("Done. %d nodes, %d edges.", len(cpg.Nodes), len(cpg.Edges))
	return nil
}

// moduleNames returns a human-readable list of module prefixes.
func moduleNames(ms *ModuleSet) string {
	names := make([]string, len(ms.Dirs()))
	for i, m := range ms.Dirs() {
		if m.Prefix == "" {
			names[i] = "prometheus (primary)"
		} else {
			names[i] = m.Prefix
		}
	}
	return strings.Join(names, ", ")
}
