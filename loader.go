package main

import (
	"bufio"
	"fmt"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

// LoadResult holds the output of package loading.
type LoadResult struct {
	Packages []*packages.Package
	Fset     *token.FileSet
}

// readModulePath returns the module path from dir/go.mod, or "" if unreadable.
func readModulePath(dir string) string {
	f, err := os.Open(filepath.Join(dir, "go.mod"))
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module "))
		}
	}
	return ""
}

// CreateTempGoWork writes a temporary go.work file that includes all modules
// in the ModuleSet. Returns the path to the temp file (caller must os.Remove).
// If a nested submodule declares the same module path as an already-listed
// directory (e.g. alertmanager/internal/tools vs prometheus/internal/tools),
// only the first occurrence is used to avoid "module appears multiple times".
func CreateTempGoWork(ms *ModuleSet) (string, error) {
	var buf strings.Builder
	buf.WriteString("go 1.25.7\n\nuse (\n")

	// Track dirs and module paths we've already added.
	seenDirs := make(map[string]bool, len(ms.Dirs()))
	seenModPaths := make(map[string]bool, len(ms.Dirs())*2)

	for _, m := range ms.Dirs() {
		buf.WriteString("\t" + m.Dir + "\n")
		seenDirs[m.Dir] = true
		if m.ModPath != "" {
			seenModPaths[m.ModPath] = true
		} else {
			if p := readModulePath(m.Dir); p != "" {
				seenModPaths[p] = true
			}
		}
	}

	// Walk ALL module directories to find nested sub-modules. Skip any submodule
	// whose module path is already in the workspace (avoids duplicate e.g.
	// github.com/prometheus/prometheus/internal/tools from alertmanager).
	for _, m := range ms.Dirs() {
		for _, d := range findSubModules(m.Dir) {
			if seenDirs[d] {
				continue
			}
			modPath := readModulePath(d)
			if modPath != "" && seenModPaths[modPath] {
				continue
			}
			buf.WriteString("\t" + d + "\n")
			seenDirs[d] = true
			if modPath != "" {
				seenModPaths[modPath] = true
			}
		}
	}

	buf.WriteString(")\n")

	f, err := os.CreateTemp("", "cpg-workspace-*.work")
	if err != nil {
		return "", fmt.Errorf("create temp go.work: %w", err)
	}
	if _, err := f.WriteString(buf.String()); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", fmt.Errorf("write go.work: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// findSubModules walks dir looking for directories with go.mod (excluding dir itself).
func findSubModules(dir string) []string {
	var dirs []string
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if info.IsDir() {
			// Skip vendor and hidden dirs
			base := filepath.Base(path)
			if base == "vendor" || base == ".git" || strings.HasPrefix(base, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if info.Name() == "go.mod" && path != filepath.Join(dir, "go.mod") {
			dirs = append(dirs, filepath.Dir(path))
		}
		return nil
	})
	return dirs
}

// LoadPackages loads all Go packages from all modules via a workspace,
// filtering to only packages belonging to known modules.
func LoadPackages(goworkPath string, prog *Progress) (*LoadResult, error) {
	prog.Log("Loading packages via workspace (%d modules)...", len(modSet.Dirs()))

	fset := token.NewFileSet()
	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedFiles |
			packages.NeedCompiledGoFiles |
			packages.NeedImports |
			packages.NeedDeps |
			packages.NeedTypes |
			packages.NeedSyntax |
			packages.NeedTypesInfo |
			packages.NeedTypesSizes,
		Dir:   modSet.PrimaryDir(),
		Fset:  fset,
		Tests: false,
		Env:   replaceEnv(os.Environ(), "GOWORK", goworkPath),
	}

	initial, err := packages.Load(cfg, modSet.LoadPatterns()...)
	if err != nil {
		return nil, fmt.Errorf("packages.Load: %w", err)
	}

	// Filter to known module packages only
	filtered := make([]*packages.Package, 0, len(initial))
	var errCount int
	for _, pkg := range initial {
		if !modSet.IsKnownPkg(pkg.PkgPath) {
			continue
		}
		if len(pkg.Errors) > 0 {
			errCount++
			prog.Verbose("  warning: %s has %d errors: %v", pkg.PkgPath, len(pkg.Errors), pkg.Errors[0])
		}
		filtered = append(filtered, pkg)
	}

	// Count files and LOC (respecting skip filters)
	var fileCount, loc int
	for _, pkg := range filtered {
		for i, f := range pkg.CompiledGoFiles {
			if shouldSkipFile(f) {
				continue
			}
			fileCount++
			if i < len(pkg.Syntax) {
				end := fset.Position(pkg.Syntax[i].End())
				loc += end.Line
			}
		}
	}

	prog.Log("Loaded %d packages (%d files, ~%dk LOC)", len(filtered), fileCount, loc/1000)
	if errCount > 0 {
		prog.Log("  %d packages had type-check errors (continuing)", errCount)
	}

	return &LoadResult{
		Packages: filtered,
		Fset:     fset,
	}, nil
}

// Skip flags, set by main before any pipeline phase runs.
var (
	flagSkipTests     = true
	flagSkipGenerated = true
)

// replaceEnv returns a copy of environ with key set to val, replacing any
// existing entry for key. This avoids duplicate env vars which have
// platform-dependent behavior (last-wins on Linux, first-wins on some BSDs).
func replaceEnv(environ []string, key, val string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environ)+1)
	for _, e := range environ {
		if !strings.HasPrefix(e, prefix) {
			result = append(result, e)
		}
	}
	return append(result, prefix+val)
}

// shouldSkipFile returns true for generated/test files that should be excluded.
func shouldSkipFile(path string) bool {
	base := BaseName(path)
	if flagSkipTests && strings.HasSuffix(base, "_test.go") {
		return true
	}
	if flagSkipGenerated && strings.HasSuffix(base, ".pb.go") {
		return true
	}
	return false
}
