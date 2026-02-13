package main

import (
	"go/ast"
	"go/token"

	"golang.org/x/tools/go/packages"
)

// ComputeMetrics calculates cyclomatic complexity, LOC, and num_params for all functions.
// Handles both FuncDecl (named functions/methods) and FuncLit (anonymous function literals).
// Fan-in/fan-out are computed later by ComputeFanInOut after call graph construction.
func ComputeMetrics(pkgs []*packages.Package, fset *token.FileSet, funcLookup *FuncLookup, cpg *CPG, prog *Progress) {
	prog.Log("Computing metrics...")

	var count int

	for _, pkg := range pkgs {
		for i, file := range pkg.Syntax {
			if i >= len(pkg.CompiledGoFiles) {
				continue
			}
			relFile := modSet.RelFile(pkg.CompiledGoFiles[i])
			if relFile == "" || shouldSkipFile(relFile) {
				continue
			}

			ast.Inspect(file, func(n ast.Node) bool {
				var funcType *ast.FuncType
				var body *ast.BlockStmt
				var nodePos, endPos token.Pos

				switch fn := n.(type) {
				case *ast.FuncDecl:
					funcType, body = fn.Type, fn.Body
					nodePos, endPos = fn.Pos(), fn.End()
				case *ast.FuncLit:
					funcType, body = fn.Type, fn.Body
					nodePos, endPos = fn.Pos(), fn.End()
				default:
					return true
				}

				line, col := fset.Position(nodePos).Line, fset.Position(nodePos).Column
				funcID := funcLookup.Get(relFile, line, col)
				if funcID == "" {
					return true
				}

				// Cyclomatic complexity: count decision points + 1
				complexity := 1
				if body != nil {
					ast.Inspect(body, func(inner ast.Node) bool {
						switch bn := inner.(type) {
						case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.CaseClause, *ast.CommClause:
							_ = bn
							complexity++
						case *ast.BinaryExpr:
							if bn.Op == token.LAND || bn.Op == token.LOR {
								complexity++
							}
						}
						return true
					})
				}

				// LOC
				endLine := fset.Position(endPos).Line
				loc := endLine - line + 1

				cpg.Metrics[funcID] = &Metrics{
					FunctionID:           funcID,
					CyclomaticComplexity: complexity,
					LOC:                  loc,
					NumParams:            countParams(funcType),
				}
				count++

				return true
			})
		}
	}

	prog.Log("Computed metrics for %d functions", count)
}

// countParams returns the total number of parameters in a function signature.
func countParams(ft *ast.FuncType) int {
	if ft == nil || ft.Params == nil {
		return 0
	}
	n := 0
	for _, field := range ft.Params.List {
		if len(field.Names) == 0 {
			n++ // unnamed parameter
		} else {
			n += len(field.Names)
		}
	}
	return n
}
