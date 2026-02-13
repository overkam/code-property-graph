package main

import (
	"go/token"

	"golang.org/x/tools/go/ssa"
)

// ExtractCDG computes control dependence edges from the SSA CFG using
// post-dominator trees and post-dominance frontiers.
//
// A block Y is control-dependent on edge (X→Y_succ) when:
//   - Y post-dominates Y_succ (or Y == Y_succ)
//   - Y does NOT strictly post-dominate X
//
// We emit CDG edges: branching_block → dependent_block.
func ExtractCDG(
	ssaResult *SSAResult,
	fset *token.FileSet,
	funcLookup *FuncLookup,
	cpg *CPG,
	prog *Progress,
) {
	prog.Log("Extracting CDG (control dependence)...")

	var cdgEdges, domEdges, pdomEdges, cdgFuncs int

	for fn := range ssaResult.AllFuncs {
		if fn.Pkg == nil || fn.Synthetic != "" {
			continue
		}
		if !modSet.IsKnownPkg(fn.Pkg.Pkg.Path()) {
			continue
		}
		if len(fn.Blocks) < 2 {
			continue
		}

		funcNodeID := ssaFuncNodeID(fn, fset, funcLookup)
		if funcNodeID == "" {
			continue
		}

		n := len(fn.Blocks)
		blockIDs := make([]string, n)
		for i := range fn.Blocks {
			blockIDs[i] = BlockID(funcNodeID, i)
		}

		// Compute post-dominator tree
		ipdom := postDominators(fn.Blocks)

		// Emit CDG edges from post-dominance frontiers.
		// For each CFG edge (u → v) where v ≠ ipdom(u):
		//   Walk from v up the pdom tree to ipdom(u), stopping there.
		//   Each visited node w gets CDG edge: u → w.
		for u, block := range fn.Blocks {
			if len(block.Succs) < 2 {
				continue // only branching blocks create control dependence
			}
			for _, succBlock := range block.Succs {
				v := succBlock.Index
				stop := ipdom[u] // stop at immediate post-dominator of u

				w := v
				for w != -1 && w != stop {
					cpg.AddEdge(Edge{
						Source: blockIDs[u],
						Target: blockIDs[w],
						Kind:   "cdg",
					})
					cdgEdges++
					w = ipdom[w]
				}
			}
		}
		// Dominator edges (from SSA's built-in dominator tree)
		for _, block := range fn.Blocks {
			for _, child := range block.Dominees() {
				cpg.AddEdge(Edge{
					Source: blockIDs[block.Index],
					Target: blockIDs[child.Index],
					Kind:   "dom",
				})
				domEdges++
			}
		}

		// Post-dominator edges (from our computed pdom tree)
		for i := 0; i < n; i++ {
			if ipdom[i] >= 0 && ipdom[i] < n {
				cpg.AddEdge(Edge{
					Source: blockIDs[ipdom[i]],
					Target: blockIDs[i],
					Kind:   "pdom",
				})
				pdomEdges++
			}
		}

		cdgFuncs++
	}

	prog.Log("Created %d CDG, %d dom, %d pdom edges across %d functions", cdgEdges, domEdges, pdomEdges, cdgFuncs)
}

// postDominators computes the immediate post-dominator tree using the
// Cooper-Harvey-Kennedy (CHK) algorithm on the reversed CFG.
//
// Returns ipdom[i] = immediate post-dominator of block i.
// ipdom[i] == -1 means the block is post-dominated by the virtual exit
// (i.e., it's an exit block or unreachable in the pdom tree).
func postDominators(blocks []*ssa.BasicBlock) []int {
	n := len(blocks)
	vExit := n // virtual exit node index

	// Find exit blocks (no successors)
	var exits []int
	for i, b := range blocks {
		if len(b.Succs) == 0 {
			exits = append(exits, i)
		}
	}

	if len(exits) == 0 {
		// Infinite loop with no exit — no post-dominators
		ipdom := make([]int, n)
		for i := range ipdom {
			ipdom[i] = -1
		}
		return ipdom
	}

	total := n + 1 // real blocks + virtual exit

	// Build reversed CFG adjacency list.
	// Original edge (i → j) becomes reversed edge (j → i).
	// Virtual exit → each exit block.
	revAdj := make([][]int, total)
	for i, b := range blocks {
		for _, succ := range b.Succs {
			revAdj[succ.Index] = append(revAdj[succ.Index], i)
		}
	}
	revAdj[vExit] = append(revAdj[vExit], exits...)

	// Reverse postorder on the reversed CFG (root = vExit)
	rpo := reversePostorder(revAdj, vExit, total)

	// RPO position lookup
	rpoPos := make([]int, total)
	for i := range rpoPos {
		rpoPos[i] = -1
	}
	for i, node := range rpo {
		rpoPos[node] = i
	}

	// Build predecessor list for reversed CFG
	revPreds := make([][]int, total)
	for from, neighbors := range revAdj {
		for _, to := range neighbors {
			revPreds[to] = append(revPreds[to], from)
		}
	}

	// CHK iterative dominator algorithm
	idom := make([]int, total)
	for i := range idom {
		idom[i] = -1
	}
	idom[vExit] = vExit

	changed := true
	for changed {
		changed = false
		for _, b := range rpo {
			if b == vExit {
				continue
			}

			newIdom := -1
			for _, p := range revPreds[b] {
				if idom[p] != -1 {
					newIdom = p
					break
				}
			}
			if newIdom == -1 {
				continue // unreachable
			}

			for _, p := range revPreds[b] {
				if p == newIdom || idom[p] == -1 {
					continue
				}
				newIdom = chkIntersect(idom, rpoPos, p, newIdom)
			}

			if idom[b] != newIdom {
				idom[b] = newIdom
				changed = true
			}
		}
	}

	// Extract real block post-dominators (map vExit → -1)
	result := make([]int, n)
	for i := 0; i < n; i++ {
		d := idom[i]
		if d >= n || d < 0 {
			result[i] = -1
		} else {
			result[i] = d
		}
	}
	return result
}

// chkIntersect finds the nearest common ancestor of a and b in the dominator tree,
// using RPO positions for efficient traversal.
func chkIntersect(idom, rpoPos []int, a, b int) int {
	for a != b {
		for rpoPos[a] > rpoPos[b] {
			a = idom[a]
		}
		for rpoPos[b] > rpoPos[a] {
			b = idom[b]
		}
	}
	return a
}

// reversePostorder computes a reverse-postorder traversal of a directed graph.
func reversePostorder(adj [][]int, root, n int) []int {
	visited := make([]bool, n)
	order := make([]int, 0, n)

	var dfs func(int)
	dfs = func(node int) {
		visited[node] = true
		for _, next := range adj[node] {
			if !visited[next] {
				dfs(next)
			}
		}
		order = append(order, node)
	}
	dfs(root)

	// Reverse to get RPO
	for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
		order[i], order[j] = order[j], order[i]
	}
	return order
}
