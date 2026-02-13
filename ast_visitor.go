package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"strings"

	"golang.org/x/tools/go/packages"
)

// PosLookup maps file:line:col to node IDs, enabling SSA→AST position mapping.
type PosLookup struct {
	m map[string]string // "file:line:col" → nodeID
}

func NewPosLookup() *PosLookup {
	return &PosLookup{m: make(map[string]string)}
}

// Set records a node ID for a position. First-wins: if a position is already mapped,
// later calls are ignored. This preserves statement-level nodes that SSA references.
func (pl *PosLookup) Set(file string, line, col int, id string) {
	key := fmt.Sprintf("%s:%d:%d", file, line, col)
	if _, exists := pl.m[key]; !exists {
		pl.m[key] = id
	}
}

func (pl *PosLookup) Get(file string, line, col int) string {
	return pl.m[fmt.Sprintf("%s:%d:%d", file, line, col)]
}

// DefLookup maps types.Object (declaration) to node IDs for REF edges.
type DefLookup struct {
	m map[types.Object]string
}

func NewDefLookup() *DefLookup {
	return &DefLookup{m: make(map[types.Object]string)}
}

func (dl *DefLookup) Set(obj types.Object, id string) {
	if obj != nil {
		dl.m[obj] = id
	}
}

func (dl *DefLookup) Get(obj types.Object) string {
	if obj == nil {
		return ""
	}
	return dl.m[obj]
}

// FuncLookup maps function positions to node IDs for parent tracking.
type FuncLookup struct {
	m map[string]string // "file:line:col" → funcNodeID
}

func NewFuncLookup() *FuncLookup {
	return &FuncLookup{m: make(map[string]string)}
}

func (fl *FuncLookup) Set(file string, line, col int, id string) {
	fl.m[fmt.Sprintf("%s:%d:%d", file, line, col)] = id
}

func (fl *FuncLookup) Get(file string, line, col int) string {
	return fl.m[fmt.Sprintf("%s:%d:%d", file, line, col)]
}

// WalkAST walks the AST of all packages, producing CPG nodes and AST edges.
// Returns a PosLookup for SSA→AST mapping and a FuncLookup for parent tracking.
func WalkAST(pkgs []*packages.Package, fset *token.FileSet, cpg *CPG, prog *Progress) (*PosLookup, *FuncLookup) {
	prog.Log("Walking AST...")

	posLookup := NewPosLookup()
	funcLookup := NewFuncLookup()
	defLookup := NewDefLookup()

	var nodeCount, edgeCount int
	var skippedFiles int

	for _, pkg := range pkgs {
		relPkg := modSet.RelPkg(pkg.PkgPath)

		// Create package node
		pkgID := PkgID(pkg.PkgPath)
		cpg.AddNode(Node{
			ID:      pkgID,
			Kind:    "package",
			Name:    pkg.Name,
			Package: relPkg,
		})
		nodeCount++

		// Import edges: package → imported package (internal modules only)
		for impPath := range pkg.Imports {
			if modSet.IsKnownPkg(impPath) {
				cpg.AddEdge(Edge{Source: pkgID, Target: PkgID(impPath), Kind: "imports"})
				edgeCount++
			}
		}

		var initFuncIDs []string // collect init() funcs for ordering

		for i, file := range pkg.Syntax {
			// Get the actual file path
			if i >= len(pkg.CompiledGoFiles) {
				continue
			}
			absFile := pkg.CompiledGoFiles[i]

			// Compute relative path via ModuleSet
			relFile := modSet.RelFile(absFile)
			if relFile == "" {
				continue
			}

			if shouldSkipFile(relFile) {
				skippedFiles++
				continue
			}

			// Create file node
			fileID := FileID(relFile)
			fileProps := map[string]any{
				"loc": 0, // overwritten below from file.End() position
			}
			if strings.HasSuffix(relFile, ".pb.go") || strings.HasSuffix(relFile, "_generated.go") {
				fileProps["is_generated"] = true
			}
			// Extract build tags from file comments
			for _, cg := range file.Comments {
				for _, c := range cg.List {
					if strings.HasPrefix(c.Text, "//go:build ") {
						fileProps["build_tags"] = strings.TrimPrefix(c.Text, "//go:build ")
					} else if strings.HasPrefix(c.Text, "// +build ") {
						if _, exists := fileProps["build_tags"]; !exists {
							fileProps["build_tags"] = strings.TrimPrefix(c.Text, "// +build ")
						}
					}
				}
			}
			// Compute actual LOC from file end position
			if file.End().IsValid() {
				fileProps["loc"] = fset.Position(file.End()).Line
			}
			cpg.AddNode(Node{
				ID:         fileID,
				Kind:       "file",
				Name:       BaseName(relFile),
				File:       relFile,
				Package:    relPkg,
				EndLine:    fset.Position(file.End()).Line,
				Properties: fileProps,
			})
			nodeCount++
			cpg.AddEdge(Edge{Source: pkgID, Target: fileID, Kind: "ast"})
			edgeCount++

			// Read source content for the sources table
			if _, ok := cpg.Sources[relFile]; !ok {
				content, err := os.ReadFile(absFile)
				if err == nil {
					cpg.Sources[relFile] = string(content)
				}
			}

			// Walk AST of this file
			v := &astVisitor{
				pkg:         pkg,
				relPkg:      relPkg,
				relFile:     relFile,
				fileID:      fileID,
				fset:        fset,
				cpg:         cpg,
				posLookup:   posLookup,
				funcLookup:  funcLookup,
				defLookup:   defLookup,
				source:      cpg.Sources[relFile],
				parentStack: []string{fileID},
				initIDs:     &initFuncIDs,
				scopeNodes:  make(map[string]bool),
			}
			ast.Walk(v, file)

			// Extract comments (not visited by ast.Walk — they're separate)
			for _, cg := range file.Comments {
				cLine, cCol := v.pos(cg.Pos())
				if cLine == 0 {
					continue
				}
				cID := StmtID(relPkg, BaseName(relFile), cLine, cCol, "comment")
				text := cg.Text()
				if len(text) > 200 {
					text = text[:200] + "..."
				}
				cpg.AddNode(Node{
					ID:      cID,
					Kind:    "comment",
					Name:    text,
					File:    relFile,
					Line:    cLine,
					Col:     cCol,
					EndLine: v.endLine(cg.End()),
					Package: relPkg,
				})
				cpg.AddEdge(Edge{Source: fileID, Target: cID, Kind: "ast"})
				nodeCount += 1
				edgeCount += 1
			}

			nodeCount += v.nodeCount
			edgeCount += v.edgeCount
		}

		// Chain init() functions within this package in source order
		for i := 1; i < len(initFuncIDs); i++ {
			cpg.AddEdge(Edge{
				Source: initFuncIDs[i-1], Target: initFuncIDs[i], Kind: "init_order",
				Properties: map[string]any{"order": i},
			})
			edgeCount++
		}
	}

	// Emit has_method edges: type_decl → function for each method.
	// Done after all packages are walked so defLookup is fully populated.
	hmCount := emitHasMethodEdges(pkgs, fset, defLookup, cpg)

	prog.Log("Created %d nodes, %d AST edges, %d has_method edges (skipped %d generated/test files)",
		nodeCount, edgeCount, hmCount, skippedFiles)

	return posLookup, funcLookup
}

type astVisitor struct {
	pkg        *packages.Package
	relPkg     string
	relFile    string
	fileID     string
	fset       *token.FileSet
	cpg        *CPG
	posLookup  *PosLookup
	funcLookup *FuncLookup
	defLookup  *DefLookup
	source     string // raw source text for current file
	// parentStack tracks the current parent node ID for AST edges.
	// Top of stack = current parent.
	parentStack []string
	// curFunc tracks the enclosing function node ID for parent_function field.
	curFunc string
	// deferIDs collects defer node IDs in source order for LIFO ordering edges.
	deferIDs []string
	// initIDs collects init() function node IDs for ordering.
	initIDs *[]string
	// scopeNodes tracks node IDs that introduce a new lexical scope (functions and blocks).
	scopeNodes map[string]bool
	nodeCount  int
	edgeCount  int
}

func (v *astVisitor) currentParent() string {
	return v.parentStack[len(v.parentStack)-1]
}

func (v *astVisitor) addNodeAndEdge(n Node) {
	n.Package = v.relPkg
	n.File = v.relFile
	n.ParentFunction = v.curFunc

	// Add nesting depth for statement/expression nodes inside functions.
	// Depth 0 = direct function body, 1 = inside one control structure, etc.
	if v.curFunc != "" && n.Kind != "function" && n.Kind != "parameter" && n.Kind != "result" {
		depth := len(v.parentStack) - 2 // subtract file + function
		if depth < 0 {
			depth = 0
		}
		if n.Properties == nil {
			n.Properties = map[string]any{"nesting_depth": depth}
		} else {
			n.Properties["nesting_depth"] = depth
		}
	}

	v.cpg.AddNode(n)
	v.nodeCount++

	// AST edge from parent to this node
	v.cpg.AddEdge(Edge{Source: v.currentParent(), Target: n.ID, Kind: "ast"})
	v.edgeCount++

	// Register in position lookup
	if n.Line > 0 {
		v.posLookup.Set(n.File, n.Line, n.Col, n.ID)
	}
}

// emitDocEdge emits a doc edge from a declaration node to its doc comment node.
func (v *astVisitor) emitDocEdge(declID string, doc *ast.CommentGroup) {
	if doc == nil {
		return
	}
	cLine, cCol := v.pos(doc.Pos())
	if cLine == 0 {
		return
	}
	commentID := StmtID(v.relPkg, BaseName(v.relFile), cLine, cCol, "comment")
	v.cpg.AddEdge(Edge{Source: declID, Target: commentID, Kind: "doc"})
	v.edgeCount++
}

func (v *astVisitor) pos(p token.Pos) (line, col int) {
	if !p.IsValid() {
		return 0, 0
	}
	pos := v.fset.Position(p)
	return pos.Line, pos.Column
}

func (v *astVisitor) endLine(end token.Pos) int {
	if !end.IsValid() {
		return 0
	}
	return v.fset.Position(end).Line
}

func (v *astVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		// Popping back up — restore parent
		if len(v.parentStack) > 1 {
			v.parentStack = v.parentStack[:len(v.parentStack)-1]
		}
		return nil
	}

	switch n := node.(type) {
	case *ast.FuncDecl:
		return v.visitFuncDecl(n)
	case *ast.FuncLit:
		return v.visitFuncLit(n)
	case *ast.CallExpr:
		id := v.visitCallExpr(n)
		v.parentStack = append(v.parentStack, id)
	case *ast.IfStmt:
		v.visitStmtWithCode(n.If, v.endLine(n.End()), "if", "if", n.Pos(), n.Body.Lbrace)
		v.emitConditionEdge("if", n.If, n.Cond)
	case *ast.ForStmt:
		v.visitStmtWithCode(n.For, v.endLine(n.End()), "for", "for", n.Pos(), n.Body.Lbrace)
		v.emitConditionEdge("for", n.For, n.Cond)
	case *ast.RangeStmt:
		v.visitStmtWithCode(n.Range, v.endLine(n.End()), "for", "range", n.Pos(), n.Body.Lbrace)
	case *ast.SwitchStmt:
		v.visitStmtWithCode(n.Switch, v.endLine(n.End()), "switch", "switch", n.Pos(), n.Body.Lbrace)
		v.emitConditionEdge("switch", n.Switch, n.Tag)
	case *ast.TypeSwitchStmt:
		v.visitStmtWithCode(n.Switch, v.endLine(n.End()), "switch", "type switch", n.Pos(), n.Body.Lbrace)
	case *ast.SelectStmt:
		v.visitStmt(n.Select, v.endLine(n.End()), "select", "select")
	case *ast.CaseClause:
		v.visitStmt(n.Case, v.endLine(n.End()), "case", "case")
	case *ast.CommClause:
		v.visitStmt(n.Case, v.endLine(n.End()), "case", "comm case")
	case *ast.ReturnStmt:
		v.visitStmtWithCode(n.Return, v.endLine(n.End()), "return", "return", n.Pos(), n.End())
	case *ast.AssignStmt:
		v.visitAssign(n)
	case *ast.GoStmt:
		v.visitGoStmt(n)
	case *ast.DeferStmt:
		v.visitStmt(n.Defer, v.endLine(n.End()), "defer", "defer")
		// Track defers for LIFO ordering
		line, col := v.pos(n.Defer)
		if line > 0 {
			v.deferIDs = append(v.deferIDs, StmtID(v.relPkg, BaseName(v.relFile), line, col, "defer"))
		}
	case *ast.SendStmt:
		line, col := v.pos(n.Arrow)
		v.visitStmtAt(line, col, v.endLine(n.End()), "send", "send")
	case *ast.BranchStmt:
		v.visitStmt(n.TokPos, v.endLine(n.End()), "branch", n.Tok.String())
		// branch_target edge: break/continue/goto with label → labeled statement
		if n.Label != nil {
			if obj := v.pkg.TypesInfo.Uses[n.Label]; obj != nil {
				if targetID := v.defLookup.Get(obj); targetID != "" {
					bLine, bCol := v.pos(n.TokPos)
					branchID := StmtID(v.relPkg, BaseName(v.relFile), bLine, bCol, "branch")
					v.cpg.AddEdge(Edge{Source: branchID, Target: targetID, Kind: "branch_target"})
					v.edgeCount++
				}
			}
		}
	case *ast.LabeledStmt:
		line, col := v.pos(n.Colon)
		id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "label")
		v.addNodeAndEdge(Node{
			ID:   id,
			Kind: "label",
			Name: n.Label.Name,
			Line: line,
			Col:  col,
		})
		// Register label for branch_target resolution
		if obj := v.pkg.TypesInfo.Defs[n.Label]; obj != nil {
			v.defLookup.Set(obj, id)
		}
		v.parentStack = append(v.parentStack, id)
	case *ast.BlockStmt:
		v.visitBlock(n)
	case *ast.GenDecl:
		v.visitGenDecl(n)
		v.parentStack = append(v.parentStack, v.currentParent()) // balance push for pop in Visit(nil)
	case *ast.TypeSpec:
		v.visitTypeSpec(n)
		return nil // we handle children manually
	case *ast.CompositeLit:
		id := v.visitCompositeLit(n)
		v.parentStack = append(v.parentStack, id)
	case *ast.BasicLit:
		v.visitBasicLit(n)
		return nil // leaf node, no AST children to walk
	case *ast.Ident:
		v.visitIdent(n)
		return nil // leaf node
	case *ast.SelectorExpr:
		id := v.visitSelectorExpr(n)
		v.parentStack = append(v.parentStack, id)
	case *ast.UnaryExpr:
		v.visitExpr(n.OpPos, n.Op.String(), "unary_expr")
	case *ast.BinaryExpr:
		v.visitExpr(n.OpPos, n.Op.String(), "binary_expr")
	case *ast.IndexExpr:
		v.visitExpr(n.Lbrack, "index", "index_expr")
	case *ast.SliceExpr:
		v.visitExpr(n.Lbrack, "slice", "slice_expr")
	case *ast.TypeAssertExpr:
		v.visitExpr(n.Lparen, "type_assert", "type_assert_expr")
	case *ast.KeyValueExpr:
		v.visitExpr(n.Colon, "key_value", "key_value_expr")
	case *ast.ImportSpec:
		v.visitImportSpec(n)
		return nil // leaf node
	case *ast.IncDecStmt:
		v.visitStmt(n.TokPos, v.endLine(n.End()), "inc_dec", n.Tok.String())
	default:
		v.parentStack = append(v.parentStack, v.currentParent()) // balance push
	}

	return v
}

func (v *astVisitor) visitFuncDecl(n *ast.FuncDecl) ast.Visitor {
	line, col := v.pos(n.Pos())
	el := v.endLine(n.End())

	var recv, name string
	name = n.Name.Name
	if n.Recv != nil && len(n.Recv.List) > 0 {
		recv = exprTypeName(n.Recv.List[0].Type)
	}

	funcID := FuncID(v.relPkg, recv, name, BaseName(v.relFile), line, col)

	var typeInfo string
	if obj := v.pkg.TypesInfo.Defs[n.Name]; obj != nil {
		typeInfo = obj.Type().String()
		v.defLookup.Set(obj, funcID)
	}

	displayName := name
	if recv != "" {
		displayName = recv + "." + name
	}

	// Full qualified name for cross-references
	fullName := v.relPkg + "." + displayName

	node := Node{
		ID:       funcID,
		Kind:     "function",
		Name:     displayName,
		Line:     line,
		Col:      col,
		EndLine:  el,
		TypeInfo: typeInfo,
		Properties: map[string]any{
			"full_name": fullName,
			"exported":  token.IsExported(name),
		},
	}
	if recv != "" {
		node.Properties["receiver"] = recv
	}
	if n.Type.TypeParams != nil && n.Type.TypeParams.NumFields() > 0 {
		node.Properties["generic"] = true
	}
	// Signature analysis: return types and context parameter
	if obj := v.pkg.TypesInfo.Defs[n.Name]; obj != nil {
		if sig, ok := obj.Type().(*types.Signature); ok {
			// Check if first parameter is context.Context
			if sig.Params() != nil && sig.Params().Len() > 0 {
				if isContextType(sig.Params().At(0).Type()) {
					node.Properties["has_context"] = true
				}
			}
		}
		if sig, ok := obj.Type().(*types.Signature); ok && sig.Results() != nil {
			for i := range sig.Results().Len() {
				rt := sig.Results().At(i).Type()
				if rt.String() == "error" {
					node.Properties["returns_error"] = true
				}
				if isNilableType(rt) && rt.String() != "error" {
					node.Properties["returns_nilable"] = true
				}
			}
		}
	}
	// Source snippet — just the signature line, not the full body
	if sig := v.codeSnippet(n.Pos(), n.Type.End(), 200); sig != "" {
		node.Properties["code"] = sig
	}
	v.addNodeAndEdge(node)
	v.emitDocEdge(funcID, n.Doc)

	// Register in func lookup for SSA mapping.
	// Store both func-keyword position AND name position because
	// SSA uses the name identifier position (types.Func.Pos()), not the func keyword.
	v.funcLookup.Set(v.relFile, line, col, funcID)
	if n.Name != nil {
		nameLine, nameCol := v.pos(n.Name.Pos())
		v.funcLookup.Set(v.relFile, nameLine, nameCol, funcID)
		// Also register name position in posLookup for type relationship resolution
		v.posLookup.Set(v.relFile, nameLine, nameCol, funcID)
	}

	// Track init() functions for ordering
	if name == "init" && recv == "" && v.initIDs != nil {
		*v.initIDs = append(*v.initIDs, funcID)
	}

	// Push as parent for children, set as current function
	v.scopeNodes[funcID] = true
	v.parentStack = append(v.parentStack, funcID)
	prevFunc := v.curFunc
	v.curFunc = funcID

	// Visit type parameters (generics)
	if n.Type.TypeParams != nil {
		v.visitFieldList(n.Type.TypeParams, "type_param")
	}
	// Visit parameters
	if n.Type.Params != nil {
		v.visitFieldList(n.Type.Params, "parameter")
	}
	if n.Type.Results != nil {
		v.visitFieldList(n.Type.Results, "result")
	}

	// Visit body
	prevDefers := v.deferIDs
	v.deferIDs = nil
	if n.Body != nil {
		ast.Walk(v, n.Body)
	}
	v.emitDeferOrdering()
	v.deferIDs = prevDefers

	v.curFunc = prevFunc
	v.parentStack = v.parentStack[:len(v.parentStack)-1]
	return nil // we handled children manually
}

func (v *astVisitor) visitFuncLit(n *ast.FuncLit) ast.Visitor {
	line, col := v.pos(n.Pos())
	el := v.endLine(n.End())

	funcID := StmtID(v.relPkg, BaseName(v.relFile), line, col, "func_lit")

	node := Node{
		ID:      funcID,
		Kind:    "function",
		Name:    "func literal",
		Line:    line,
		Col:     col,
		EndLine: el,
	}
	v.addNodeAndEdge(node)

	v.funcLookup.Set(v.relFile, line, col, funcID)

	v.scopeNodes[funcID] = true
	v.parentStack = append(v.parentStack, funcID)
	prevFunc := v.curFunc
	v.curFunc = funcID

	if n.Type.Params != nil {
		v.visitFieldList(n.Type.Params, "parameter")
	}
	if n.Type.Results != nil {
		v.visitFieldList(n.Type.Results, "result")
	}
	prevDefers := v.deferIDs
	v.deferIDs = nil
	if n.Body != nil {
		ast.Walk(v, n.Body)
	}
	v.emitDeferOrdering()
	v.deferIDs = prevDefers

	v.curFunc = prevFunc
	v.parentStack = v.parentStack[:len(v.parentStack)-1]
	return nil
}

func (v *astVisitor) visitCallExpr(n *ast.CallExpr) string {
	line, col := v.pos(n.Lparen)
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "call")

	callee := resolveCalleeName(n)
	var typeInfo string
	if tv, ok := v.pkg.TypesInfo.Types[n.Fun]; ok {
		typeInfo = tv.Type.String()
	}

	// Determine dispatch type from the function expression.
	// Static: direct function call, method call on concrete receiver, func literal.
	// Dynamic: interface dispatch, call through function value variable.
	dispatchType := "static"
	switch fun := n.Fun.(type) {
	case *ast.SelectorExpr:
		if tv, ok := v.pkg.TypesInfo.Types[fun.X]; ok {
			recvType := tv.Type
			if ptr, ok := recvType.(*types.Pointer); ok {
				recvType = ptr.Elem()
			}
			if types.IsInterface(recvType) {
				dispatchType = "dynamic"
			}
		}
	case *ast.Ident:
		// Direct call by name — static dispatch.
		// However, if the ident refers to a variable (not a func/builtin),
		// it's a call through a function value and is dynamic.
		if obj := v.pkg.TypesInfo.Uses[fun]; obj != nil {
			if _, isVar := obj.(*types.Var); isVar {
				dispatchType = "dynamic"
			}
		}
	case *ast.FuncLit:
		// Immediately invoked function literal: func(){}() — always static
		dispatchType = "static"
	default:
		// Other expression types (index, type assert, paren, etc.):
		// calls through computed function values are dynamic
		dispatchType = "dynamic"
	}

	props := map[string]any{
		"dispatch_type": dispatchType,
	}
	if code := v.codeSnippet(n.Fun.Pos(), n.Rparen+1, 120); code != "" {
		props["code"] = code
	}
	// Detect sync primitive calls via receiver type
	if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
		if syncKind := v.detectSyncPrimitive(sel); syncKind != "" {
			props["sync_kind"] = syncKind
		}
	}
	// Detect context derivation calls (context.WithCancel, etc.)
	if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok {
			if obj := v.pkg.TypesInfo.Uses[ident]; obj != nil {
				if pkg, ok := obj.(*types.PkgName); ok && pkg.Imported().Path() == "context" {
					switch sel.Sel.Name {
					case "WithCancel", "WithTimeout", "WithDeadline", "WithValue",
						"WithCancelCause", "WithTimeoutCause", "WithDeadlineCause":
						props["context_derivation"] = sel.Sel.Name
					}
				}
			}
		}
	}

	v.addNodeAndEdge(Node{
		ID:         id,
		Kind:       "call",
		Name:       callee,
		Line:       line,
		Col:        col,
		EndLine:    v.endLine(n.End()),
		TypeInfo:   typeInfo,
		Properties: props,
	})

	// eval_type: call expression → return type declaration
	v.emitEvalType(id, n)

	// Receiver edge: method call → receiver expression
	if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
		if recvID := v.exprNodeID(sel.X); recvID != "" {
			v.cpg.AddEdge(Edge{Source: id, Target: recvID, Kind: "receiver"})
			v.edgeCount++
		}
	}

	// Emit argument edges from call → each argument expression
	for i, arg := range n.Args {
		if argID := v.exprNodeID(arg); argID != "" {
			v.cpg.AddEdge(Edge{
				Source: id, Target: argID, Kind: "argument",
				Properties: map[string]any{"index": i},
			})
			v.edgeCount++
		}
	}

	// Error wrapping: fmt.Errorf with %w wraps an error argument
	v.emitErrorWrapEdge(id, callee, n.Args)

	return id
}

// visitStmtWithCode creates a statement node with an optional code snippet.
// codeStart/codeEnd define the range for the snippet (pass invalid Pos to skip).
func (v *astVisitor) visitStmtWithCode(p token.Pos, el int, kind, name string, codeStart, codeEnd token.Pos) {
	line, col := v.pos(p)
	if line == 0 {
		v.parentStack = append(v.parentStack, v.currentParent()) // balance push
		return
	}
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, kind)

	var props map[string]any
	if code := v.codeSnippet(codeStart, codeEnd, 120); code != "" {
		props = map[string]any{"code": code}
	}

	v.addNodeAndEdge(Node{
		ID:         id,
		Kind:       kind,
		Name:       name,
		Line:       line,
		Col:        col,
		EndLine:    el,
		Properties: props,
	})

	v.parentStack = append(v.parentStack, id)
}

func (v *astVisitor) visitStmt(p token.Pos, el int, kind, name string) {
	v.visitStmtWithCode(p, el, kind, name, 0, 0)
}

func (v *astVisitor) visitStmtAt(line, col, el int, kind, name string) {
	if line == 0 {
		v.parentStack = append(v.parentStack, v.currentParent()) // balance push
		return
	}
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, kind)

	v.addNodeAndEdge(Node{
		ID:      id,
		Kind:    kind,
		Name:    name,
		Line:    line,
		Col:     col,
		EndLine: el,
	})

	v.parentStack = append(v.parentStack, id)
}

// visitExpr creates a node for expression types and pushes onto parent stack.
func (v *astVisitor) visitExpr(p token.Pos, name, kind string) {
	line, col := v.pos(p)
	if line == 0 {
		v.parentStack = append(v.parentStack, v.currentParent())
		return
	}
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, kind)
	v.addNodeAndEdge(Node{
		ID:   id,
		Kind: kind,
		Name: name,
		Line: line,
		Col:  col,
	})
	v.parentStack = append(v.parentStack, id)
}

// visitBlock creates a block node and emits next_sibling edges between
// consecutive statements in the block's statement list.
func (v *astVisitor) visitBlock(n *ast.BlockStmt) {
	line, col := v.pos(n.Lbrace)
	el := v.endLine(n.Rbrace)
	if line == 0 {
		v.parentStack = append(v.parentStack, v.currentParent())
		return
	}
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "block")
	v.addNodeAndEdge(Node{
		ID:      id,
		Kind:    "block",
		Name:    "block",
		Line:    line,
		Col:     col,
		EndLine: el,
	})
	// Register as scope boundary and emit scope edge to nearest enclosing scope
	v.scopeNodes[id] = true
	for i := len(v.parentStack) - 1; i >= 0; i-- {
		if v.scopeNodes[v.parentStack[i]] {
			v.cpg.AddEdge(Edge{Source: id, Target: v.parentStack[i], Kind: "scope"})
			v.edgeCount++
			break
		}
	}
	v.parentStack = append(v.parentStack, id)

	// Emit next_sibling edges between consecutive statements
	if len(n.List) > 1 {
		var prevID string
		for _, stmt := range n.List {
			curID := v.stmtNodeID(stmt)
			if curID != "" && prevID != "" {
				v.cpg.AddEdge(Edge{Source: prevID, Target: curID, Kind: "next_sibling"})
				v.edgeCount++
			}
			if curID != "" {
				prevID = curID
			}
		}
	}
}

// stmtNodeID predicts the node ID for a statement (for next_sibling edges).
func (v *astVisitor) stmtNodeID(stmt ast.Stmt) string {
	base := BaseName(v.relFile)
	switch s := stmt.(type) {
	case *ast.IfStmt:
		line, col := v.pos(s.If)
		return StmtID(v.relPkg, base, line, col, "if")
	case *ast.ForStmt:
		line, col := v.pos(s.For)
		return StmtID(v.relPkg, base, line, col, "for")
	case *ast.RangeStmt:
		line, col := v.pos(s.Range)
		return StmtID(v.relPkg, base, line, col, "for")
	case *ast.SwitchStmt:
		line, col := v.pos(s.Switch)
		return StmtID(v.relPkg, base, line, col, "switch")
	case *ast.TypeSwitchStmt:
		line, col := v.pos(s.Switch)
		return StmtID(v.relPkg, base, line, col, "switch")
	case *ast.SelectStmt:
		line, col := v.pos(s.Select)
		return StmtID(v.relPkg, base, line, col, "select")
	case *ast.ReturnStmt:
		line, col := v.pos(s.Return)
		return StmtID(v.relPkg, base, line, col, "return")
	case *ast.AssignStmt:
		line, col := v.pos(s.TokPos)
		return StmtID(v.relPkg, base, line, col, "assign")
	case *ast.ExprStmt:
		return v.exprNodeID(s.X)
	case *ast.GoStmt:
		line, col := v.pos(s.Go)
		return StmtID(v.relPkg, base, line, col, "go")
	case *ast.DeferStmt:
		line, col := v.pos(s.Defer)
		return StmtID(v.relPkg, base, line, col, "defer")
	case *ast.SendStmt:
		line, col := v.pos(s.Arrow)
		return StmtID(v.relPkg, base, line, col, "send")
	case *ast.BranchStmt:
		line, col := v.pos(s.TokPos)
		return StmtID(v.relPkg, base, line, col, "branch")
	case *ast.BlockStmt:
		line, col := v.pos(s.Lbrace)
		return StmtID(v.relPkg, base, line, col, "block")
	case *ast.DeclStmt:
		// GenDecl inside a block
		if gd, ok := s.Decl.(*ast.GenDecl); ok && len(gd.Specs) > 0 {
			if vs, ok := gd.Specs[0].(*ast.ValueSpec); ok && len(vs.Names) > 0 {
				line, col := v.pos(vs.Names[0].Pos())
				return StmtID(v.relPkg, base, line, col, "local")
			}
		}
	}
	return ""
}

// emitDeferOrdering emits defer_order edges in LIFO order (last defer executes first).
func (v *astVisitor) emitDeferOrdering() {
	if len(v.deferIDs) < 2 {
		return
	}
	// LIFO: last defer runs first → chain in reverse
	for i := len(v.deferIDs) - 1; i > 0; i-- {
		v.cpg.AddEdge(Edge{
			Source: v.deferIDs[i], Target: v.deferIDs[i-1],
			Kind:       "defer_order",
			Properties: map[string]any{"exec_order": len(v.deferIDs) - i},
		})
		v.edgeCount++
	}
}

func (v *astVisitor) visitGoStmt(n *ast.GoStmt) {
	line, col := v.pos(n.Go)
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "go")
	v.addNodeAndEdge(Node{
		ID:      id,
		Kind:    "go",
		Name:    "go",
		Line:    line,
		Col:     col,
		EndLine: v.endLine(n.End()),
	})
	v.parentStack = append(v.parentStack, id)

	// Spawn edge: go stmt → launched function (if identifiable).
	// Two spawn edges: one to the function reference, one to the call expression.
	// The function reference spawn enables tracing which function is launched.
	// The call expression spawn enables tracing the actual invocation (with args).
	if calleeID := v.exprNodeID(n.Call.Fun); calleeID != "" {
		v.cpg.AddEdge(Edge{Source: id, Target: calleeID, Kind: "spawn"})
		v.edgeCount++
	}
	// spawn_call edge: go stmt → call expression that is launched as a goroutine
	if callID := v.exprNodeID(n.Call); callID != "" {
		v.cpg.AddEdge(Edge{Source: id, Target: callID, Kind: "spawn_call"})
		v.edgeCount++
	}
}

func (v *astVisitor) visitAssign(n *ast.AssignStmt) {
	line, col := v.pos(n.TokPos)
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "assign")

	var props map[string]any
	if code := v.codeSnippet(n.Pos(), n.End(), 120); code != "" {
		props = map[string]any{"code": code}
	}

	v.addNodeAndEdge(Node{
		ID:         id,
		Kind:       "assign",
		Name:       n.Tok.String(),
		Line:       line,
		Col:        col,
		EndLine:    v.endLine(n.End()),
		Properties: props,
	})

	// For short variable declarations, create local variable nodes
	if n.Tok == token.DEFINE {
		for i, lhs := range n.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok || ident.Name == "_" {
				continue
			}
			vLine, vCol := v.pos(ident.Pos())
			vid := StmtID(v.relPkg, BaseName(v.relFile), vLine, vCol, "local")

			var typeInfo string
			if obj := v.pkg.TypesInfo.Defs[ident]; obj != nil {
				typeInfo = obj.Type().String()
				v.defLookup.Set(obj, vid)
			}

			v.addNodeAndEdge(Node{
				ID:       vid,
				Kind:     "local",
				Name:     ident.Name,
				Line:     vLine,
				Col:      vCol,
				TypeInfo: typeInfo,
			})

			// Initializer edge: local variable → RHS expression
			if i < len(n.Rhs) {
				if rhsID := v.exprNodeID(n.Rhs[i]); rhsID != "" {
					v.cpg.AddEdge(Edge{Source: vid, Target: rhsID, Kind: "initializer"})
					v.edgeCount++
				}
			}
		}
	}

	v.parentStack = append(v.parentStack, id)
}

func (v *astVisitor) visitGenDecl(n *ast.GenDecl) {
	switch n.Tok { //nolint:exhaustive // only VAR/CONST/TYPE are relevant
	case token.VAR, token.CONST:
		for _, spec := range n.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if name.Name == "_" {
					continue
				}
				line, col := v.pos(name.Pos())
				id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "local")

				var typeInfo string
				if obj := v.pkg.TypesInfo.Defs[name]; obj != nil {
					typeInfo = obj.Type().String()
					v.defLookup.Set(obj, id)
				}

				v.addNodeAndEdge(Node{
					ID:       id,
					Kind:     "local",
					Name:     name.Name,
					Line:     line,
					Col:      col,
					TypeInfo: typeInfo,
					Properties: map[string]any{
						"decl":     n.Tok.String(),
						"exported": token.IsExported(name.Name),
					},
				})
				// Initializer edge: var/const → RHS expression
				if i < len(vs.Values) {
					if rhsID := v.exprNodeID(vs.Values[i]); rhsID != "" {
						v.cpg.AddEdge(Edge{Source: id, Target: rhsID, Kind: "initializer"})
						v.edgeCount++
					}
				}
				// Doc from ValueSpec first, fall back to GenDecl doc
				doc := vs.Doc
				if doc == nil {
					doc = n.Doc
				}
				v.emitDocEdge(id, doc)
			}
		}
	case token.TYPE:
		// TypeSpec is handled by visitTypeSpec when ast.Walk visits it
	}
}

func (v *astVisitor) visitTypeSpec(n *ast.TypeSpec) {
	line, col := v.pos(n.Pos())
	el := v.endLine(n.End())
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "type_decl")

	var typeKind string
	switch n.Type.(type) {
	case *ast.StructType:
		typeKind = "struct"
	case *ast.InterfaceType:
		typeKind = "interface"
	default:
		typeKind = "alias"
	}

	var typeInfo string
	if obj := v.pkg.TypesInfo.Defs[n.Name]; obj != nil {
		typeInfo = obj.Type().String()
		v.defLookup.Set(obj, id)
	}

	props := map[string]any{
		"type_kind": typeKind,
		"full_name": v.relPkg + "." + n.Name.Name,
		"exported":  token.IsExported(n.Name.Name),
	}
	if code := v.codeSnippet(n.Pos(), n.End(), 200); code != "" {
		props["code"] = code
	}
	if n.TypeParams != nil && n.TypeParams.NumFields() > 0 {
		props["generic"] = true
	}

	v.addNodeAndEdge(Node{
		ID:         id,
		Kind:       "type_decl",
		Name:       n.Name.Name,
		Line:       line,
		Col:        col,
		EndLine:    el,
		TypeInfo:   typeInfo,
		Properties: props,
	})

	v.emitDocEdge(id, n.Doc)

	// Register type_decl in pos lookup for type relationship edges
	v.posLookup.Set(v.relFile, line, col, id)

	// Push type_decl onto parent stack so children (type params, fields) are parented correctly.
	v.parentStack = append(v.parentStack, id)

	// Visit type parameters (generics)
	if n.TypeParams != nil && n.TypeParams.NumFields() > 0 {
		v.visitFieldList(n.TypeParams, "type_param")
	}

	// Visit struct fields or interface methods.
	switch t := n.Type.(type) {
	case *ast.StructType:
		if t.Fields != nil {
			for _, field := range t.Fields.List {
				v.visitField(field)
			}
		}
	case *ast.InterfaceType:
		if t.Methods != nil {
			for _, method := range t.Methods.List {
				v.visitField(method)
			}
		}
	}
	v.parentStack = v.parentStack[:len(v.parentStack)-1]
}

func (v *astVisitor) visitImportSpec(n *ast.ImportSpec) {
	line, col := v.pos(n.Pos())
	path := strings.Trim(n.Path.Value, `"`)

	var name string
	if n.Name != nil {
		name = n.Name.Name // alias (dot import, named import, blank import)
	} else {
		// Use last path component as the display name
		if idx := strings.LastIndex(path, "/"); idx >= 0 {
			name = path[idx+1:]
		} else {
			name = path
		}
	}

	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "import")
	props := map[string]any{
		"path": path,
	}
	if n.Name != nil {
		props["alias"] = n.Name.Name
	}

	v.addNodeAndEdge(Node{
		ID:         id,
		Kind:       "import",
		Name:       name,
		Line:       line,
		Col:        col,
		Properties: props,
	})
	v.emitDocEdge(id, n.Doc)
}

func (v *astVisitor) visitField(field *ast.Field) {
	line, col := v.pos(field.Pos())

	var name string
	if len(field.Names) > 0 {
		name = field.Names[0].Name
	} else {
		// Embedded field — use type name
		name = exprTypeName(field.Type)
	}

	var typeInfo string
	if tv, ok := v.pkg.TypesInfo.Types[field.Type]; ok {
		typeInfo = tv.Type.String()
	}

	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "field")

	// Register field definition for REF edges
	if len(field.Names) > 0 {
		v.defLookup.Set(v.pkg.TypesInfo.Defs[field.Names[0]], id)
	}

	props := map[string]any{
		"exported": token.IsExported(name),
	}
	if field.Tag != nil && field.Tag.Value != "" {
		// Raw tag includes backticks; strip them for the property
		tag := field.Tag.Value
		if len(tag) >= 2 && tag[0] == '`' && tag[len(tag)-1] == '`' {
			tag = tag[1 : len(tag)-1]
		}
		props["tag"] = tag
	}
	if len(field.Names) == 0 {
		props["embedded"] = true
	}

	v.addNodeAndEdge(Node{
		ID:         id,
		Kind:       "field",
		Name:       name,
		Line:       line,
		Col:        col,
		TypeInfo:   typeInfo,
		Properties: props,
	})
	v.emitDocEdge(id, field.Doc)
}

func (v *astVisitor) visitFieldList(fl *ast.FieldList, kind string) {
	for _, field := range fl.List {
		var typeInfo string
		var props map[string]any
		if tv, ok := v.pkg.TypesInfo.Types[field.Type]; ok {
			typeInfo = tv.Type.String()
			if kind == "parameter" {
				if isMutableType(tv.Type) {
					if props == nil {
						props = map[string]any{}
					}
					props["mutable"] = true
				}
				if isNilableType(tv.Type) {
					if props == nil {
						props = map[string]any{}
					}
					props["nullable"] = true
				}
				if isContextType(tv.Type) {
					if props == nil {
						props = map[string]any{}
					}
					props["context_param"] = true
				}
			}
		}

		if len(field.Names) == 0 {
			// Unnamed parameter
			line, col := v.pos(field.Pos())
			id := StmtID(v.relPkg, BaseName(v.relFile), line, col, kind)
			v.addNodeAndEdge(Node{
				ID:         id,
				Kind:       kind,
				Name:       exprTypeName(field.Type),
				Line:       line,
				Col:        col,
				TypeInfo:   typeInfo,
				Properties: props,
			})
			continue
		}

		for _, name := range field.Names {
			line, col := v.pos(name.Pos())
			id := StmtID(v.relPkg, BaseName(v.relFile), line, col, kind)
			v.addNodeAndEdge(Node{
				ID:         id,
				Kind:       kind,
				Name:       name.Name,
				Line:       line,
				Col:        col,
				TypeInfo:   typeInfo,
				Properties: props,
			})
			v.defLookup.Set(v.pkg.TypesInfo.Defs[name], id)
		}
	}
}

func (v *astVisitor) visitCompositeLit(n *ast.CompositeLit) string {
	line, col := v.pos(n.Lbrace)
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "composite_lit")

	var typeName string
	if n.Type != nil {
		typeName = exprTypeName(n.Type)
	}

	v.addNodeAndEdge(Node{
		ID:   id,
		Kind: "composite_lit",
		Name: typeName,
		Line: line,
		Col:  col,
	})

	// eval_type: composite literal → type declaration
	v.emitEvalType(id, n)

	return id
}

func (v *astVisitor) visitBasicLit(n *ast.BasicLit) {
	line, col := v.pos(n.Pos())
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "literal")

	val := n.Value
	if len(val) > 50 {
		val = val[:50] + "..."
	}

	v.addNodeAndEdge(Node{
		ID:   id,
		Kind: "literal",
		Name: val,
		Line: line,
		Col:  col,
		Properties: map[string]any{
			"literal_kind": n.Kind.String(),
		},
	})
}

// visitIdent creates an identifier node for variable/function/type/const references.
// Only handles Uses (references), not Defs (declarations handled elsewhere).
func (v *astVisitor) visitIdent(n *ast.Ident) {
	obj := v.pkg.TypesInfo.Uses[n]
	if obj == nil {
		return // definition-side ident or unresolved
	}
	// Only create nodes for meaningful references
	switch obj.(type) {
	case *types.Var, *types.Func, *types.Const, *types.TypeName:
		// proceed
	default:
		return // skip PkgName, Builtin, Label, Nil
	}

	line, col := v.pos(n.Pos())
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "identifier")

	v.addNodeAndEdge(Node{
		ID:       id,
		Kind:     "identifier",
		Name:     n.Name,
		Line:     line,
		Col:      col,
		TypeInfo: obj.Type().String(),
	})

	// eval_type: identifier → type declaration
	v.emitEvalType(id, n)

	// REF edge: identifier → declaration
	if declID := v.defLookup.Get(obj); declID != "" {
		v.cpg.AddEdge(Edge{Source: id, Target: declID, Kind: "ref"})
		v.edgeCount++
	}
}

// visitSelectorExpr creates a node for field/method access (x.Field).
// Distinguishes three selection kinds via the type checker:
//   - FieldVal:  field access (x.Field)
//   - MethodVal: bound method reference (x.Method) — receiver is captured
//   - MethodExpr: unbound method expression (T.Method) — receiver is first argument
//
// This maps to brrr-lang's EField vs EMethodRef vs ETypeMethod distinction.
func (v *astVisitor) visitSelectorExpr(n *ast.SelectorExpr) string {
	line, col := v.pos(n.Sel.Pos())
	id := StmtID(v.relPkg, BaseName(v.relFile), line, col, "selector")

	name := n.Sel.Name
	if ident, ok := n.X.(*ast.Ident); ok {
		name = ident.Name + "." + n.Sel.Name
	}

	var typeInfo string
	if obj := v.pkg.TypesInfo.Uses[n.Sel]; obj != nil {
		typeInfo = obj.Type().String()
	} else if tv, ok := v.pkg.TypesInfo.Selections[n]; ok {
		typeInfo = tv.Type().String()
	}

	props := map[string]any{}
	// Annotate the selection kind: field_val, method_val (bound), or method_expr (unbound).
	// This is critical for call graph precision: method values capture the receiver
	// and are equivalent to closures, while method expressions do not.
	if sel, ok := v.pkg.TypesInfo.Selections[n]; ok {
		switch sel.Kind() {
		case types.FieldVal:
			props["selection_kind"] = "field_val"
		case types.MethodVal:
			// Bound method value: receiver is captured (like brrr-lang EMethodRef).
			// When used as a callback, calls through this are dynamic dispatch.
			props["selection_kind"] = "method_val"
		case types.MethodExpr:
			// Unbound method expression: receiver becomes first argument
			// (like brrr-lang ETypeMethod). T.Method has signature func(T, ...).
			props["selection_kind"] = "method_expr"
		}
	}

	node := Node{
		ID:         id,
		Kind:       "selector",
		Name:       name,
		Line:       line,
		Col:        col,
		TypeInfo:   typeInfo,
		Properties: props,
	}
	v.addNodeAndEdge(node)

	// eval_type: selector → type declaration
	v.emitEvalType(id, n)

	// REF edge: selector → field/method declaration
	if obj := v.pkg.TypesInfo.Uses[n.Sel]; obj != nil {
		if declID := v.defLookup.Get(obj); declID != "" {
			v.cpg.AddEdge(Edge{Source: id, Target: declID, Kind: "ref"})
			v.edgeCount++
		}
	} else if sel, ok := v.pkg.TypesInfo.Selections[n]; ok {
		if declID := v.defLookup.Get(sel.Obj()); declID != "" {
			v.cpg.AddEdge(Edge{Source: id, Target: declID, Kind: "ref"})
			v.edgeCount++
		}
	}

	return id
}

// codeSnippet extracts source text for an AST node, truncated to maxLen chars.
func (v *astVisitor) codeSnippet(start, end token.Pos, maxLen int) string {
	if v.source == "" || !start.IsValid() || !end.IsValid() {
		return ""
	}
	f := v.fset.File(start)
	if f == nil {
		return ""
	}
	startOff := f.Offset(start)
	endOff := f.Offset(end)
	if startOff < 0 || endOff <= startOff || endOff > len(v.source) {
		return ""
	}
	snippet := v.source[startOff:endOff]
	if len(snippet) > maxLen {
		snippet = snippet[:maxLen] + "..."
	}
	return snippet
}

// emitErrorWrapEdge detects error wrapping calls and emits error_wrap edges
// from the call to the wrapped error argument(s).
// Handles: fmt.Errorf with %w, errors.Join (Go 1.20+).
func (v *astVisitor) emitErrorWrapEdge(callID, callee string, args []ast.Expr) {
	switch callee {
	case "errors.Join":
		// errors.Join(errs ...error) wraps all arguments into a single error.
		for _, arg := range args {
			if errID := v.exprNodeID(arg); errID != "" {
				v.cpg.AddEdge(Edge{Source: callID, Target: errID, Kind: "error_wrap"})
				v.edgeCount++
			}
		}
	case "fmt.Errorf":
		v.emitFmtErrorfWrapEdges(callID, args)
	}
}

// emitFmtErrorfWrapEdges parses the format string in fmt.Errorf to find %w verbs
// and emits error_wrap edges to the corresponding error arguments.
func (v *astVisitor) emitFmtErrorfWrapEdges(callID string, args []ast.Expr) {
	if len(args) < 2 {
		return
	}
	// First arg should be a format string containing %w
	fmtLit, ok := args[0].(*ast.BasicLit)
	if !ok || fmtLit.Kind != token.STRING {
		return
	}
	fmtStr := fmtLit.Value

	// Count %w occurrences and find the argument positions they correspond to.
	// Format verbs consume arguments in order, starting from args[1].
	argIdx := 1 // start after format string
	for i := 0; i < len(fmtStr)-1; i++ {
		if fmtStr[i] != '%' {
			continue
		}
		i++ // skip past %
		if fmtStr[i] == '%' {
			continue // %% is literal
		}
		// Skip flags, width, precision
		for i < len(fmtStr) && strings.ContainsRune("+-# 0", rune(fmtStr[i])) {
			i++
		}
		for i < len(fmtStr) && fmtStr[i] >= '0' && fmtStr[i] <= '9' {
			i++
		}
		if i < len(fmtStr) && fmtStr[i] == '.' {
			i++
			for i < len(fmtStr) && fmtStr[i] >= '0' && fmtStr[i] <= '9' {
				i++
			}
		}
		if i >= len(fmtStr) {
			break
		}
		verb := fmtStr[i]
		if verb == 'w' && argIdx < len(args) {
			if errID := v.exprNodeID(args[argIdx]); errID != "" {
				v.cpg.AddEdge(Edge{
					Source: callID, Target: errID,
					Kind: "error_wrap",
				})
				v.edgeCount++
			}
		}
		argIdx++
	}
}

// emitEvalType emits an eval_type edge from a node to its resolved type declaration,
// if the expression's type is a named type from the analyzed modules.
func (v *astVisitor) emitEvalType(nodeID string, expr ast.Expr) {
	tv, ok := v.pkg.TypesInfo.Types[expr]
	if !ok {
		return
	}
	typ := tv.Type
	// Unwrap pointer
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	named, ok := typ.(*types.Named)
	if !ok {
		return
	}
	tObj := named.Obj()
	if typeID := v.defLookup.Get(tObj); typeID != "" && typeID != nodeID {
		v.cpg.AddEdge(Edge{Source: nodeID, Target: typeID, Kind: "eval_type"})
		v.edgeCount++
	}
}

// exprNodeID predicts the CPG node ID that will be created for an expression.
// Returns "" for expression types we don't create dedicated nodes for.
func (v *astVisitor) exprNodeID(expr ast.Expr) string {
	base := BaseName(v.relFile)
	switch e := expr.(type) {
	case *ast.Ident:
		line, col := v.pos(e.Pos())
		return StmtID(v.relPkg, base, line, col, "identifier")
	case *ast.CallExpr:
		line, col := v.pos(e.Lparen)
		return StmtID(v.relPkg, base, line, col, "call")
	case *ast.BasicLit:
		line, col := v.pos(e.Pos())
		return StmtID(v.relPkg, base, line, col, "literal")
	case *ast.CompositeLit:
		line, col := v.pos(e.Lbrace)
		return StmtID(v.relPkg, base, line, col, "composite_lit")
	case *ast.SelectorExpr:
		line, col := v.pos(e.Sel.Pos())
		return StmtID(v.relPkg, base, line, col, "selector")
	case *ast.FuncLit:
		line, col := v.pos(e.Pos())
		return StmtID(v.relPkg, base, line, col, "func_lit")
	case *ast.UnaryExpr:
		line, col := v.pos(e.OpPos)
		return StmtID(v.relPkg, base, line, col, "unary_expr")
	case *ast.BinaryExpr:
		line, col := v.pos(e.OpPos)
		return StmtID(v.relPkg, base, line, col, "binary_expr")
	case *ast.IndexExpr:
		line, col := v.pos(e.Lbrack)
		return StmtID(v.relPkg, base, line, col, "index_expr")
	case *ast.SliceExpr:
		line, col := v.pos(e.Lbrack)
		return StmtID(v.relPkg, base, line, col, "slice_expr")
	case *ast.TypeAssertExpr:
		line, col := v.pos(e.Lparen)
		return StmtID(v.relPkg, base, line, col, "type_assert_expr")
	case *ast.ParenExpr:
		return v.exprNodeID(e.X) // unwrap parentheses
	case *ast.StarExpr:
		return v.exprNodeID(e.X)
	}
	return ""
}

// emitConditionEdge emits a condition edge from a control structure to its condition expr.
func (v *astVisitor) emitConditionEdge(kind string, stmtPos token.Pos, cond ast.Expr) {
	if cond == nil {
		return
	}
	condID := v.exprNodeID(cond)
	if condID == "" {
		return
	}
	line, col := v.pos(stmtPos)
	stmtID := StmtID(v.relPkg, BaseName(v.relFile), line, col, kind)
	v.cpg.AddEdge(Edge{Source: stmtID, Target: condID, Kind: "condition"})
	v.edgeCount++
}

// resolveCalleeName resolves the human-readable name of a call expression.
func resolveCalleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		if x, ok := fn.X.(*ast.Ident); ok {
			return x.Name + "." + fn.Sel.Name
		}
		return fn.Sel.Name
	case *ast.IndexExpr:
		// Generic function instantiation: f[T](...)
		return resolveCalleeName(&ast.CallExpr{Fun: fn.X})
	case *ast.IndexListExpr:
		return resolveCalleeName(&ast.CallExpr{Fun: fn.X})
	}
	return "?"
}

// emitHasMethodEdges iterates all named types in analyzed packages and emits
// has_method edges from each type_decl to its method function nodes.
// Uses the type checker's method sets so we catch both value and pointer receivers.
func emitHasMethodEdges(pkgs []*packages.Package, _ *token.FileSet, defLookup *DefLookup, cpg *CPG) int {
	count := 0
	// Track emitted (typeDeclID, methodID) pairs to avoid double-counting
	// value-receiver methods that appear in both T and *T method sets.
	type methodEdge struct{ typeID, methodID string }
	seen := make(map[methodEdge]bool)

	for _, pkg := range pkgs {
		scope := pkg.Types.Scope()
		for _, name := range scope.Names() {
			obj := scope.Lookup(name)
			tn, ok := obj.(*types.TypeName)
			if !ok {
				continue
			}
			typeDeclID := defLookup.Get(tn)
			if typeDeclID == "" {
				continue
			}
			// Check methods on both T and *T
			for _, base := range []types.Type{tn.Type(), types.NewPointer(tn.Type())} {
				mset := types.NewMethodSet(base)
				for i := 0; i < mset.Len(); i++ {
					sel := mset.At(i)
					// Only direct methods (not promoted from embedded types)
					if len(sel.Index()) != 1 {
						continue
					}
					fnObj := sel.Obj()
					methodID := defLookup.Get(fnObj)
					if methodID == "" {
						continue
					}
					key := methodEdge{typeDeclID, methodID}
					if seen[key] {
						continue
					}
					seen[key] = true
					cpg.AddEdge(Edge{
						Source: typeDeclID,
						Target: methodID,
						Kind:   "has_method",
					})
					count++
				}
			}
		}
	}
	return count
}

// exprTypeName extracts a human-readable name from a type expression.
func exprTypeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		if x, ok := t.X.(*ast.Ident); ok {
			return x.Name + "." + t.Sel.Name
		}
		return t.Sel.Name
	case *ast.StarExpr:
		return "*" + exprTypeName(t.X)
	case *ast.ArrayType:
		return "[]" + exprTypeName(t.Elt)
	case *ast.MapType:
		return "map[" + exprTypeName(t.Key) + "]" + exprTypeName(t.Value)
	case *ast.ChanType:
		return "chan " + exprTypeName(t.Value)
	case *ast.FuncType:
		return "func"
	case *ast.InterfaceType:
		return "interface{}"
	case *ast.IndexExpr:
		return exprTypeName(t.X) + "[" + exprTypeName(t.Index) + "]"
	case *ast.IndexListExpr:
		parts := make([]string, len(t.Indices))
		for i, idx := range t.Indices {
			parts[i] = exprTypeName(idx)
		}
		return exprTypeName(t.X) + "[" + strings.Join(parts, ", ") + "]"
	}
	return "?"
}

// isMutableType returns true if a type allows callee-visible mutations
// (pointer, slice, map, channel, or interface containing such).
func isMutableType(t types.Type) bool {
	t = t.Underlying()
	switch t.(type) {
	case *types.Pointer, *types.Slice, *types.Map, *types.Chan:
		return true
	case *types.Interface:
		return true // interfaces may hold mutable types
	}
	return false
}

// isContextType returns true if the type is context.Context.
func isContextType(t types.Type) bool {
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == "context" && obj.Name() == "Context"
}

// isNilableType returns true if a type can be nil
// (pointer, slice, map, channel, interface, or function).
func isNilableType(t types.Type) bool {
	t = t.Underlying()
	switch t.(type) {
	case *types.Pointer, *types.Slice, *types.Map, *types.Chan, *types.Interface, *types.Signature:
		return true
	}
	return false
}

// detectSyncPrimitive checks if a method call targets a known sync primitive
// and returns the sync_kind (e.g., "mutex_lock", "wg_wait") or "".
func (v *astVisitor) detectSyncPrimitive(sel *ast.SelectorExpr) string {
	tv, ok := v.pkg.TypesInfo.Types[sel.X]
	if !ok {
		return ""
	}
	recvType := tv.Type
	if ptr, ok := recvType.(*types.Pointer); ok {
		recvType = ptr.Elem()
	}
	named, ok := recvType.(*types.Named)
	if !ok {
		return ""
	}
	pkg := named.Obj().Pkg()
	if pkg == nil {
		return ""
	}
	pkgPath := pkg.Path()
	typeName := named.Obj().Name()
	methodName := sel.Sel.Name

	switch {
	case pkgPath == "sync" && typeName == "Mutex":
		switch methodName {
		case "Lock":
			return "mutex_lock"
		case "Unlock":
			return "mutex_unlock"
		}
	case pkgPath == "sync" && typeName == "RWMutex":
		switch methodName {
		case "Lock":
			return "rwmutex_lock"
		case "Unlock":
			return "rwmutex_unlock"
		case "RLock":
			return "rwmutex_rlock"
		case "RUnlock":
			return "rwmutex_runlock"
		}
	case pkgPath == "sync" && typeName == "WaitGroup":
		switch methodName {
		case "Add":
			return "wg_add"
		case "Done":
			return "wg_done"
		case "Wait":
			return "wg_wait"
		}
	case pkgPath == "sync" && typeName == "Once":
		if methodName == "Do" {
			return "once_do"
		}
	case pkgPath == "sync" && typeName == "Cond":
		switch methodName {
		case "Wait":
			return "cond_wait"
		case "Signal":
			return "cond_signal"
		case "Broadcast":
			return "cond_broadcast"
		}
	case pkgPath == "context" && methodName == "Cancel":
		return "context_cancel"
	}
	return ""
}
