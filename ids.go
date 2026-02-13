package main

import (
	"fmt"
	"strings"
)

// FuncID generates a deterministic ID for a function or method.
// recv is empty for plain functions.
func FuncID(pkg, recv, name, file string, line, col int) string {
	if recv != "" {
		return fmt.Sprintf("%s::%s.%s@%s:%d:%d", pkg, recv, name, file, line, col)
	}
	return fmt.Sprintf("%s::%s@%s:%d:%d", pkg, name, file, line, col)
}

// StmtID generates a deterministic ID for a statement-level AST node.
func StmtID(pkg, file string, line, col int, kind string) string {
	return fmt.Sprintf("%s::@%s:%d:%d:%s", pkg, file, line, col, kind)
}

// PkgID generates a node ID for a package.
func PkgID(pkgPath string) string {
	return fmt.Sprintf("pkg::%s", modSet.RelPkg(pkgPath))
}

// FileID generates a node ID for a source file.
func FileID(relFile string) string {
	return fmt.Sprintf("file::%s", relFile)
}

// BlockID generates a node ID for an SSA basic block.
func BlockID(funcID string, blockIndex int) string {
	return fmt.Sprintf("%s::bb%d", funcID, blockIndex)
}

// BaseName extracts the filename without directory from a path.
func BaseName(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return path
	}
	return path[idx+1:]
}
