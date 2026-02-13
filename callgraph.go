package main

import (
	"go/token"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/vta"
)

// BuildCallGraph constructs a VTA call graph and emits call/call_site edges.
func BuildCallGraph(
	ssaResult *SSAResult,
	fset *token.FileSet,
	posLookup *PosLookup,
	funcLookup *FuncLookup,
	cpg *CPG,
	prog *Progress,
) {
	prog.Log("Building VTA call graph...")

	cg := vta.CallGraph(ssaResult.AllFuncs, nil)
	cg.DeleteSyntheticNodes()

	var callEdges, callSiteEdges, paramInEdges, paramOutEdges, callToReturnEdges int
	var vtaTotal, vtaProm, vtaMatched, stubCount int
	stubs := make(map[string]bool) // track created stub nodes

	_ = callgraph.GraphVisitEdges(cg, func(edge *callgraph.Edge) error {
		caller := edge.Caller.Func
		callee := edge.Callee.Func

		vtaTotal++

		// At least one must be in a known module
		callerKnown := caller.Pkg != nil && modSet.IsKnownPkg(caller.Pkg.Pkg.Path())
		calleeKnown := callee.Pkg != nil && modSet.IsKnownPkg(callee.Pkg.Pkg.Path())
		if !callerKnown && !calleeKnown {
			return nil
		}
		vtaProm++

		callerID := ssaFuncNodeID(caller, fset, funcLookup)
		calleeID := ssaFuncNodeID(callee, fset, funcLookup)

		if callerID == "" {
			return nil
		}

		// Create stub node for external callee if it doesn't have a known module node.
		// If the callee belongs to a known module but wasn't found in funcLookup
		// (e.g., in a skipped generated/test file), don't create a misleading
		// "ext::" stub — just skip the edge entirely.
		if calleeID == "" && callee.Pkg != nil {
			if calleeKnown {
				// Known-module function without an AST node (skipped file).
				// Skip rather than create a phantom external stub.
				return nil
			}
			pkgPath := callee.Pkg.Pkg.Path()
			stubID := "ext::" + callee.String()
			if !stubs[stubID] {
				cpg.AddNode(Node{
					ID:       stubID,
					Kind:     "function",
					Name:     callee.Name(),
					Package:  modSet.RelPkg(pkgPath),
					TypeInfo: callee.Signature.String(),
					Properties: map[string]any{
						"external":  true,
						"full_name": callee.String(),
					},
				})
				stubs[stubID] = true
				stubCount++
			}
			calleeID = stubID
		}
		if calleeID == "" {
			return nil
		}
		vtaMatched++

		// Determine if this is a dynamic (interface) dispatch
		props := map[string]any{}
		if edge.Site != nil && edge.Site.Common().IsInvoke() {
			props["dynamic"] = true
		}

		// Emit function→function call edge
		cpg.AddEdge(Edge{
			Source:     callerID,
			Target:     calleeID,
			Kind:       "call",
			Properties: props,
		})
		callEdges++

		// Emit call_site→function edge (AST call node → callee)
		if edge.Site == nil {
			return nil
		}
		sitePos := edge.Site.Pos()
		if !sitePos.IsValid() {
			return nil
		}
		p := fset.Position(sitePos)
		relFile := modSet.RelFile(p.Filename)
		var siteID string
		if relFile != "" {
			siteID = posLookup.Get(relFile, p.Line, p.Column)
		}
		if siteID != "" {
			cpg.AddEdge(Edge{
				Source:     siteID,
				Target:     calleeID,
				Kind:       "call_site",
				Properties: props,
			})
			callSiteEdges++
		}

		// ParamIn edges: actual argument position → formal parameter
		callInstr := edge.Site.Common()
		args := callInstr.Args
		params := callee.Params
		// For interface dispatch, Args[0] is the receiver, which
		// doesn't correspond to a Params slot
		offset := 0
		if callInstr.IsInvoke() {
			offset = 1
		}
		for i := offset; i < len(args) && (i-offset) < len(params); i++ {
			argPos := args[i].Pos()
			if !argPos.IsValid() {
				continue
			}
			aPos := fset.Position(argPos)
			aFile := modSet.RelFile(aPos.Filename)
			if aFile == "" {
				continue // argument from file outside known modules
			}
			argID := posLookup.Get(aFile, aPos.Line, aPos.Column)
			if argID == "" {
				continue
			}
			paramPos := params[i-offset].Pos()
			if !paramPos.IsValid() {
				continue
			}
			pPos := fset.Position(paramPos)
			pFile := modSet.RelFile(pPos.Filename)
			if pFile == "" {
				continue // parameter from file outside known modules
			}
			paramID := posLookup.Get(pFile, pPos.Line, pPos.Column)
			if paramID == "" {
				continue
			}
			cpg.AddEdge(Edge{
				Source: argID, Target: paramID, Kind: "param_in",
				Properties: map[string]any{"index": i - offset},
			})
			paramInEdges++
		}

		// ParamOut edge: callee function → call site (return value flow)
		if siteID != "" && callee.Signature.Results().Len() > 0 {
			cpg.AddEdge(Edge{
				Source: calleeID, Target: siteID, Kind: "param_out",
				Properties: map[string]any{"num_results": callee.Signature.Results().Len()},
			})
			paramOutEdges++
		}

		// CallToReturn bypass edge: call site → return site (same node for Go).
		// This edge is essential for IFDS/IDE-style inter-procedural analysis:
		// it represents the flow of local variables that are NOT passed to
		// the callee but survive the call. Without this edge, dataflow facts
		// about locals are killed at every call site.
		if siteID != "" {
			cpg.AddEdge(Edge{
				Source: callerID, Target: siteID, Kind: "call_to_return",
			})
			callToReturnEdges++
		}

		return nil
	})

	prog.Log("VTA: %d total edges, %d known-module pairs, %d matched to AST, %d external stubs", vtaTotal, vtaProm, vtaMatched, stubCount)
	prog.Log("Created %d call, %d call_site, %d param_in, %d param_out, %d call_to_return edges", callEdges, callSiteEdges, paramInEdges, paramOutEdges, callToReturnEdges)
}

// ComputeFanInOut calculates fan-in, fan-out, and recursion from the call graph edges.
// Must be called after BuildCallGraph has populated call edges.
// For call targets that have no AST-derived Metrics entry (e.g., external stubs),
// a minimal Metrics record is created so fan-in data is not silently lost.
// Also detects direct and mutual recursion by finding self-referencing call edges
// and marking involved function nodes with a "recursive" property.
func ComputeFanInOut(cpg *CPG) {
	fanIn := make(map[string]int)      // target → count
	fanOut := make(map[string]int)     // source → count
	recursive := make(map[string]bool) // functions with self-referencing call edges

	for _, e := range cpg.Edges {
		if e.Kind != "call" {
			continue
		}
		fanOut[e.Source]++
		fanIn[e.Target]++
		// Direct recursion: function calls itself
		if e.Source == e.Target {
			recursive[e.Source] = true
		}
	}

	// Mark recursive functions in node properties
	for i := range cpg.Nodes {
		if cpg.Nodes[i].Kind == "function" && recursive[cpg.Nodes[i].ID] {
			if cpg.Nodes[i].Properties == nil {
				cpg.Nodes[i].Properties = map[string]any{}
			}
			cpg.Nodes[i].Properties["recursive"] = true
		}
	}

	// Update existing metrics entries
	for funcID, m := range cpg.Metrics {
		m.FanIn = fanIn[funcID]
		m.FanOut = fanOut[funcID]
	}

	// Create minimal metrics entries for call targets without AST-derived metrics
	// (external stubs, known-module functions in skipped files) so fan-in is preserved
	for targetID, fi := range fanIn {
		if _, exists := cpg.Metrics[targetID]; !exists {
			cpg.Metrics[targetID] = &Metrics{
				FunctionID: targetID,
				FanIn:      fi,
				FanOut:     fanOut[targetID],
			}
		}
	}
	// Same for sources that only appear as callers but have no metrics
	for sourceID, fo := range fanOut {
		if _, exists := cpg.Metrics[sourceID]; !exists {
			cpg.Metrics[sourceID] = &Metrics{
				FunctionID: sourceID,
				FanOut:     fo,
				FanIn:      fanIn[sourceID],
			}
		}
	}
}
