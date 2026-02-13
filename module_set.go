package main

import (
	"path/filepath"
	"strings"
)

// ModuleInfo describes one Go module in the analysis set.
type ModuleInfo struct {
	ModPath string // e.g. "github.com/prometheus/prometheus"
	Dir     string // absolute path to module root
	Prefix  string // node ID prefix: "" for primary, "adapter", "client_golang", etc.
}

// ModuleSet holds all modules under analysis. It provides path resolution
// that replaces the old single-module promDir + isPrometheusPkg approach.
//
// Global instance (modSet) is set once in main() before any pipeline phase,
// consistent with flagSkipTests/flagSkipGenerated globals.
type ModuleSet struct {
	modules []ModuleInfo
}

// Global instance — set in main() before pipeline runs.
// Initialized to empty (not nil) so methods are safe before main() assigns the real value.
var modSet = &ModuleSet{}

// NewModuleSet builds a ModuleSet from a primary module and optional extras.
func NewModuleSet(primary ModuleInfo, extras []ModuleInfo) *ModuleSet {
	ms := &ModuleSet{
		modules: make([]ModuleInfo, 0, 1+len(extras)),
	}
	ms.modules = append(ms.modules, primary)
	ms.modules = append(ms.modules, extras...)
	return ms
}

// IsKnownPkg returns true if pkgPath belongs to any module in the set.
func (ms *ModuleSet) IsKnownPkg(pkgPath string) bool {
	for _, m := range ms.modules {
		if pkgPath == m.ModPath || strings.HasPrefix(pkgPath, m.ModPath+"/") {
			return true
		}
	}
	return false
}

// RelPkg strips the module prefix from a full import path and prepends the
// module's Prefix. Prometheus (Prefix:"") yields "scrape"; adapter (Prefix:"adapter")
// yields "adapter/pkg/client".
//
// When module paths are nested (e.g., "github.com/foo" and "github.com/foo/bar"),
// we prefer the longest matching ModPath to avoid the parent claiming child packages.
func (ms *ModuleSet) RelPkg(fullPath string) string {
	bestResult := ""
	bestModLen := -1
	matched := false

	for _, m := range ms.modules {
		if fullPath == m.ModPath && len(m.ModPath) > bestModLen {
			bestModLen = len(m.ModPath)
			matched = true
			if m.Prefix == "" {
				bestResult = "main"
			} else {
				bestResult = m.Prefix
			}
		} else if rel, ok := strings.CutPrefix(fullPath, m.ModPath+"/"); ok && len(m.ModPath) > bestModLen {
			bestModLen = len(m.ModPath)
			matched = true
			if m.Prefix == "" {
				bestResult = rel
			} else {
				bestResult = m.Prefix + "/" + rel
			}
		}
	}

	if matched {
		return bestResult
	}
	return fullPath
}

// RelFile converts an absolute file path to a module-relative path with prefix.
// Returns "" for files outside all known modules (replaces the ".." prefix check).
//
// When module directories are nested (e.g., /project and /project/vendor/lib),
// we prefer the most specific match (longest Dir) to avoid the parent module
// claiming files that belong to a child module.
func (ms *ModuleSet) RelFile(absPath string) string {
	bestRel := ""
	bestPrefix := ""
	bestDirLen := -1

	for _, m := range ms.modules {
		rel, err := filepath.Rel(m.Dir, absPath)
		if err != nil {
			continue
		}
		if strings.HasPrefix(rel, "..") {
			continue
		}
		// Prefer the module with the longest Dir path (most specific match).
		if len(m.Dir) > bestDirLen {
			bestDirLen = len(m.Dir)
			bestRel = rel
			bestPrefix = m.Prefix
		}
	}

	if bestDirLen < 0 {
		return ""
	}
	if bestPrefix == "" {
		return bestRel
	}
	return bestPrefix + "/" + bestRel
}

// PrimaryDir returns the first (primary) module's directory.
func (ms *ModuleSet) PrimaryDir() string {
	return ms.modules[0].Dir
}

// Dirs returns all module infos for operations that need to iterate modules
// (escape analysis, git history).
func (ms *ModuleSet) Dirs() []ModuleInfo {
	return ms.modules
}

// LoadPatterns returns the "dir/..." patterns for packages.Load.
func (ms *ModuleSet) LoadPatterns() []string {
	patterns := make([]string, len(ms.modules))
	for i, m := range ms.modules {
		patterns[i] = m.ModPath + "/..."
	}
	return patterns
}
