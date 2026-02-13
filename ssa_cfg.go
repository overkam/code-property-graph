package main

import (
	"go/token"
	"go/types"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// SSAResult holds the SSA program and all functions for downstream consumers.
type SSAResult struct {
	Prog     *ssa.Program
	AllFuncs map[*ssa.Function]bool
}

// BuildSSA constructs the SSA representation from loaded packages.
func BuildSSA(pkgs []*packages.Package, prog *Progress) *SSAResult {
	prog.Log("Building SSA...")

	ssaProg, ssaPkgs := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	var ssaFailed int
	for i, sp := range ssaPkgs {
		if sp == nil && i < len(pkgs) {
			prog.Verbose("SSA build skipped package: %s", pkgs[i].PkgPath)
			ssaFailed++
		}
	}
	if ssaFailed > 0 {
		prog.Log("Warning: %d packages failed SSA construction", ssaFailed)
	}
	ssaProg.Build()

	allFuncs := ssautil.AllFunctions(ssaProg)

	var count int
	for fn := range allFuncs {
		if fn.Synthetic != "" {
			continue
		}
		if fn.Pkg == nil {
			continue
		}
		if modSet.IsKnownPkg(fn.Pkg.Pkg.Path()) {
			count++
		}
	}

	prog.Log("Built SSA for %d functions across %d modules", count, len(modSet.Dirs()))

	return &SSAResult{
		Prog:     ssaProg,
		AllFuncs: allFuncs,
	}
}

// ExtractCFGAndDFG extracts control-flow and data-flow edges from SSA.
func ExtractCFGAndDFG(
	ssaResult *SSAResult,
	fset *token.FileSet,
	posLookup *PosLookup,
	funcLookup *FuncLookup,
	cpg *CPG,
	prog *Progress,
) {
	prog.Log("Extracting CFG + DFG...")

	var cfgEdges, dfgEdges, bbNodes, captureEdges int
	var ssaPromFuncs, ssaWithBlocks, ssaMatched int

	for fn := range ssaResult.AllFuncs {
		if fn.Pkg == nil || fn.Synthetic != "" {
			continue
		}
		if !modSet.IsKnownPkg(fn.Pkg.Pkg.Path()) {
			continue
		}
		ssaPromFuncs++
		if len(fn.Blocks) == 0 {
			continue
		}
		ssaWithBlocks++

		// Find the function's node ID via position
		funcNodeID := ssaFuncNodeID(fn, fset, funcLookup)
		if funcNodeID == "" {
			if ssaWithBlocks-ssaMatched <= 5 {
				pos := fn.Pos()
				if pos.IsValid() {
					p := fset.Position(pos)
					rel := modSet.RelFile(p.Filename)
					prog.Verbose("  SSA miss: %s at %s:%d:%d", fn.String(), rel, p.Line, p.Column)
				} else {
					prog.Verbose("  SSA miss (no pos): %s", fn.String())
				}
			}
			continue
		}
		ssaMatched++

		// Closure capture edges: FuncLit → captured variables from enclosing scope.
		// Go closures always capture by reference (the closure and the enclosing
		// scope share the same variable). This is annotated as capture_kind so
		// downstream analysis can correctly model mutation semantics.
		if fn.Parent() != nil && len(fn.FreeVars) > 0 {
			for _, fv := range fn.FreeVars {
				fvPos := fv.Pos()
				if !fvPos.IsValid() {
					continue
				}
				p := fset.Position(fvPos)
				relFile := modSet.RelFile(p.Filename)
				if relFile == "" {
					continue
				}
				varID := posLookup.Get(relFile, p.Line, p.Column)
				if varID != "" {
					cpg.AddEdge(Edge{
						Source: funcNodeID, Target: varID, Kind: "capture",
						Properties: map[string]any{
							"var_name":     fv.Name(),
							"capture_kind": "by_reference",
						},
					})
					captureEdges++
				}
			}
		}

		// Create basic block nodes and CFG edges
		blockIDs := make([]string, len(fn.Blocks))
		for i, block := range fn.Blocks {
			bbID := BlockID(funcNodeID, i)
			blockIDs[i] = bbID

			// Determine position from first instruction with valid pos
			line, col, file := blockPos(block, fset)

			cpg.AddNode(Node{
				ID:             bbID,
				Kind:           "basic_block",
				Name:           block.Comment,
				File:           file,
				Line:           line,
				Col:            col,
				Package:        modSet.RelPkg(fn.Pkg.Pkg.Path()),
				ParentFunction: funcNodeID,
				Properties: map[string]any{
					"index": i,
				},
			})
			bbNodes++
		}

		// CFG entry edge: function → first block
		cpg.AddEdge(Edge{
			Source: funcNodeID, Target: blockIDs[0],
			Kind:       "cfg",
			Properties: map[string]any{"label": "entry"},
		})
		cfgEdges++

		// CFG exit edges: terminal blocks (no successors) → function
		for i, block := range fn.Blocks {
			if len(block.Succs) == 0 {
				cpg.AddEdge(Edge{
					Source: blockIDs[i], Target: funcNodeID,
					Kind:       "cfg",
					Properties: map[string]any{"label": "exit"},
				})
				cfgEdges++
			}
		}

		// CFG edges between basic blocks
		for i, block := range fn.Blocks {
			for j, succ := range block.Succs {
				props := map[string]any{}
				// Label branch edges for If terminators
				if len(block.Instrs) > 0 {
					if _, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If); ok {
						if j == 0 {
							props["label"] = "true"
						} else {
							props["label"] = "false"
						}
					}
				}
				cpg.AddEdge(Edge{
					Source:     blockIDs[i],
					Target:     blockIDs[succ.Index],
					Kind:       "cfg",
					Properties: props,
				})
				cfgEdges++
			}
		}

		// DFG edges: definition → use (intra-procedural)
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				val, ok := instr.(ssa.Value)
				if !ok {
					continue
				}
				refs := val.Referrers()
				if refs == nil {
					continue
				}

				defFile, defLine, defCol := instrPos(instr, fset)
				if defFile == "" {
					continue
				}
				defNodeID := posLookup.Get(defFile, defLine, defCol)
				if defNodeID == "" {
					continue
				}

				for _, ref := range *refs {
					useFile, useLine, useCol := instrPos(ref, fset)
					if useFile == "" {
						continue
					}
					useNodeID := posLookup.Get(useFile, useLine, useCol)
					if useNodeID == "" || useNodeID == defNodeID {
						continue
					}

					props := map[string]any{}
					if name := ssaValueName(val); name != "" {
						props["var_name"] = name
					}
					cpg.AddEdge(Edge{
						Source:     defNodeID,
						Target:     useNodeID,
						Kind:       "dfg",
						Properties: props,
					})
					dfgEdges++
				}
			}
		}
	}

	prog.Log("SSA: %d Prometheus funcs, %d with blocks, %d matched to AST", ssaPromFuncs, ssaWithBlocks, ssaMatched)
	prog.Log("Created %d basic_block nodes, %d CFG edges, %d DFG edges, %d capture edges", bbNodes, cfgEdges, dfgEdges, captureEdges)
}

// ExtractChannelFlow finds channel send→receive pairs by tracking MakeChan
// values through SSA referrers (including closures) and emits chan_flow edges.
func ExtractChannelFlow(
	ssaResult *SSAResult,
	fset *token.FileSet,
	posLookup *PosLookup,
	cpg *CPG,
	prog *Progress,
) {
	prog.Log("Extracting channel flow edges...")

	var chanFlowEdges int

	// For each MakeChan, follow referrers to find all sends and receives
	for fn := range ssaResult.AllFuncs {
		if fn.Pkg == nil || fn.Synthetic != "" {
			continue
		}
		if !modSet.IsKnownPkg(fn.Pkg.Pkg.Path()) {
			continue
		}

		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				mc, ok := instr.(*ssa.MakeChan)
				if !ok {
					continue
				}

				// Follow all referrers to find sends/receives on this channel
				var sends, receives []string
				visited := map[ssa.Value]bool{}
				chanFollowRefs(mc, fset, posLookup, &sends, &receives, visited)

				for _, sendID := range sends {
					for _, recvID := range receives {
						cpg.AddEdge(Edge{
							Source: sendID, Target: recvID,
							Kind: "chan_flow",
						})
						chanFlowEdges++
					}
				}
			}
		}
	}

	prog.Log("Created %d channel flow edges", chanFlowEdges)
}

// chanFollowRefs recursively follows SSA referrers of a channel value to find
// all send and receive operations, including through closures and phi nodes.
func chanFollowRefs(
	val ssa.Value,
	fset *token.FileSet, posLookup *PosLookup,
	sends, receives *[]string,
	visited map[ssa.Value]bool,
) {
	if visited[val] {
		return
	}
	visited[val] = true

	refs := val.Referrers()
	if refs == nil {
		return
	}

	for _, ref := range *refs {
		switch inst := ref.(type) {
		case *ssa.Send:
			if inst.Chan == val {
				file, line, col := instrPos(inst, fset)
				if file != "" {
					if id := posLookup.Get(file, line, col); id != "" {
						*sends = append(*sends, id)
					}
				}
			}
		case *ssa.UnOp:
			if inst.Op == token.ARROW && inst.X == val {
				// Channel receive: <-ch
				file, line, col := instrPos(inst, fset)
				if file != "" {
					if id := posLookup.Get(file, line, col); id != "" {
						*receives = append(*receives, id)
					}
				}
			} else if inst.Op == token.MUL {
				// Pointer dereference (load): channel was stored to an address,
				// now being loaded back. Follow the loaded value's referrers.
				chanFollowRefs(inst, fset, posLookup, sends, receives, visited)
			}
		case *ssa.Select:
			// select{} statement: each state is a send or receive on a channel.
			// Match states where the channel operand is the value we're tracking.
			for _, st := range inst.States {
				if st.Chan != val {
					continue
				}
				if !st.Pos.IsValid() {
					continue
				}
				pos := fset.Position(st.Pos)
				rel := modSet.RelFile(pos.Filename)
				if rel == "" {
					continue
				}
				id := posLookup.Get(rel, pos.Line, pos.Column)
				if id == "" {
					continue
				}
				if st.Dir == types.SendOnly {
					*sends = append(*sends, id)
				} else {
					*receives = append(*receives, id)
				}
			}
		case *ssa.Call:
			// Channel passed as argument — follow into statically-resolvable callee.
			chanFollowCallArgs(&inst.Call, val, fset, posLookup, sends, receives, visited)
			// Also follow the return value: callee may return the channel.
			chanFollowRefs(inst, fset, posLookup, sends, receives, visited)
		case *ssa.Go:
			// Channel passed to a goroutine — follow into the launched function.
			// *ssa.Go does NOT implement ssa.Value so the fallback won't catch it.
			chanFollowCallArgs(&inst.Call, val, fset, posLookup, sends, receives, visited)
		case *ssa.Defer:
			// Channel passed to a deferred call — follow into the deferred function.
			// *ssa.Defer does NOT implement ssa.Value so the fallback won't catch it.
			chanFollowCallArgs(&inst.Call, val, fset, posLookup, sends, receives, visited)
		case *ssa.Phi:
			// Channel flows through a phi node — follow it
			chanFollowRefs(inst, fset, posLookup, sends, receives, visited)
		case *ssa.MakeClosure:
			// Channel captured by a closure — follow into FreeVars
			closureFn, ok := inst.Fn.(*ssa.Function)
			if !ok {
				continue
			}
			for i, binding := range inst.Bindings {
				if binding == val && i < len(closureFn.FreeVars) {
					chanFollowRefs(closureFn.FreeVars[i], fset, posLookup, sends, receives, visited)
				}
			}
		case *ssa.Store:
			// Channel stored to an address — follow loads from same address
			if inst.Val == val {
				chanFollowRefs(inst.Addr, fset, posLookup, sends, receives, visited)
			}
		case ssa.Value:
			// Other values that use this channel — follow referrers
			chanFollowRefs(inst, fset, posLookup, sends, receives, visited)
		}
	}
}

// chanFollowCallArgs handles cross-function channel tracking: when a channel
// value is passed as an argument to a call/go/defer, follow it into the callee's
// corresponding parameter to discover sends/receives inside the called function.
// Only works for statically-resolvable callees (*ssa.Function); interface dispatch
// and calls through function-value variables are skipped.
func chanFollowCallArgs(
	common *ssa.CallCommon,
	val ssa.Value,
	fset *token.FileSet, posLookup *PosLookup,
	sends, receives *[]string,
	visited map[ssa.Value]bool,
) {
	if common.IsInvoke() {
		return // interface dispatch — callee not statically resolvable
	}
	callee, ok := common.Value.(*ssa.Function)
	if !ok {
		return // indirect call (function variable) — not resolvable
	}
	for i, arg := range common.Args {
		if arg == val && i < len(callee.Params) {
			chanFollowRefs(callee.Params[i], fset, posLookup, sends, receives, visited)
		}
	}
}

// ExtractPanicRecover connects panic() calls to recover() calls within the same
// function scope (including deferred closures) via panic_recover edges.
func ExtractPanicRecover(
	ssaResult *SSAResult,
	fset *token.FileSet,
	posLookup *PosLookup,
	funcLookup *FuncLookup,
	cpg *CPG,
	prog *Progress,
) {
	prog.Log("Extracting panic/recover flow edges...")

	var panicRecoverEdges int

	for fn := range ssaResult.AllFuncs {
		if fn.Pkg == nil || fn.Synthetic != "" {
			continue
		}
		if !modSet.IsKnownPkg(fn.Pkg.Pkg.Path()) {
			continue
		}

		// Find all panic sites in this function
		var panicIDs []string
		// Find all recover sites in deferred closures of this function
		var recoverIDs []string

		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				switch inst := instr.(type) {
				case *ssa.Panic:
					file, line, col := instrPos(inst, fset)
					if file != "" {
						if id := posLookup.Get(file, line, col); id != "" {
							panicIDs = append(panicIDs, id)
						}
					}
				case *ssa.Call:
					// Check for recover() builtin (direct call, not deferred)
					if b, ok := inst.Call.Value.(*ssa.Builtin); ok && b.Name() == "recover" {
						file, line, col := instrPos(inst, fset)
						if file != "" {
							if id := posLookup.Get(file, line, col); id != "" {
								recoverIDs = append(recoverIDs, id)
							}
						}
					}
				case *ssa.Defer:
					// Look for recover() inside deferred functions.
					// Three patterns in SSA:
					//   1. defer func() { recover() }()  — MakeClosure
					//   2. defer recoverFunc()            — *ssa.Function reference
					//   3. defer recover()                — direct builtin call
					deferredFn := deferTarget(inst)
					if deferredFn != nil {
						collectRecoverIDs(deferredFn, fset, posLookup, &recoverIDs)
					} else if b, ok := inst.Call.Value.(*ssa.Builtin); ok && b.Name() == "recover" {
						// Pattern 3: defer recover() — the defer itself is the recover site
						file, line, col := instrPos(inst, fset)
						if file != "" {
							if id := posLookup.Get(file, line, col); id != "" {
								recoverIDs = append(recoverIDs, id)
							}
						}
					}
				}
			}
		}

		// Connect panics → recovers within same function scope
		for _, panicID := range panicIDs {
			for _, recoverID := range recoverIDs {
				cpg.AddEdge(Edge{
					Source: panicID, Target: recoverID,
					Kind: "panic_recover",
				})
				panicRecoverEdges++
			}
		}
	}

	prog.Log("Created %d panic/recover flow edges", panicRecoverEdges)
}

// deferTarget extracts the SSA function from a Defer instruction.
// Handles both MakeClosure (deferred func literals) and direct function references.
// Returns nil if the deferred value is not a resolvable function (e.g., function pointer).
func deferTarget(inst *ssa.Defer) *ssa.Function {
	switch v := inst.Call.Value.(type) {
	case *ssa.MakeClosure:
		if fn, ok := v.Fn.(*ssa.Function); ok {
			return fn
		}
	case *ssa.Function:
		return v
	}
	return nil
}

// collectRecoverIDs scans all blocks of an SSA function for recover() builtin calls
// and appends their position-based node IDs to the provided slice.
func collectRecoverIDs(fn *ssa.Function, fset *token.FileSet, posLookup *PosLookup, ids *[]string) {
	for _, block := range fn.Blocks {
		for _, instr := range block.Instrs {
			call, ok := instr.(*ssa.Call)
			if !ok {
				continue
			}
			b, ok := call.Call.Value.(*ssa.Builtin)
			if !ok || b.Name() != "recover" {
				continue
			}
			file, line, col := instrPos(call, fset)
			if file == "" {
				continue
			}
			if id := posLookup.Get(file, line, col); id != "" {
				*ids = append(*ids, id)
			}
		}
	}
}

// ssaFuncNodeID finds the CPG node ID for an SSA function using the func lookup.
func ssaFuncNodeID(fn *ssa.Function, fset *token.FileSet, funcLookup *FuncLookup) string {
	pos := fn.Pos()
	if !pos.IsValid() {
		return ""
	}
	p := fset.Position(pos)
	relFile := modSet.RelFile(p.Filename)
	if relFile == "" {
		return ""
	}
	return funcLookup.Get(relFile, p.Line, p.Column)
}

// blockPos returns the position of the first instruction with a valid Pos in a block.
func blockPos(block *ssa.BasicBlock, fset *token.FileSet) (line, col int, relFile string) {
	for _, instr := range block.Instrs {
		p := instr.Pos()
		if !p.IsValid() {
			continue
		}
		pos := fset.Position(p)
		rel := modSet.RelFile(pos.Filename)
		if rel == "" {
			continue
		}
		return pos.Line, pos.Column, rel
	}
	return 0, 0, ""
}

// deref strips a pointer type to its element, or returns t unchanged.
func deref(t types.Type) types.Type {
	if p, ok := t.(*types.Pointer); ok {
		return p.Elem()
	}
	return t
}

// ssaValueName extracts the source-level variable name from an SSA value.
// Returns "" if no meaningful name is available.
func ssaValueName(v ssa.Value) string {
	switch val := v.(type) {
	case *ssa.Alloc:
		if val.Comment != "" {
			return val.Comment
		}
	case *ssa.Parameter:
		return val.Name()
	case *ssa.FreeVar:
		return val.Name()
	case *ssa.Global:
		return val.Name()
	case *ssa.FieldAddr:
		// Field name from the struct type
		if st, ok := deref(val.X.Type()).Underlying().(*types.Struct); ok && val.Field < st.NumFields() {
			return st.Field(val.Field).Name()
		}
	}
	return ""
}

// instrPos returns the relative file, line, col for an SSA instruction.
// Returns "" for files outside all known modules.
func instrPos(instr ssa.Instruction, fset *token.FileSet) (file string, line, col int) {
	p := instr.Pos()
	if !p.IsValid() {
		return "", 0, 0
	}
	pos := fset.Position(p)
	rel := modSet.RelFile(pos.Filename)
	if rel == "" {
		return "", 0, 0
	}
	return rel, pos.Line, pos.Column
}
