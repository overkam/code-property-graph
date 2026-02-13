package main

import (
	"go/token"
	"go/types"

	"golang.org/x/tools/go/packages"
)

// ExtractTypeRelationships emits implements and embeds edges between type declarations.
func ExtractTypeRelationships(
	pkgs []*packages.Package,
	fset *token.FileSet,
	posLookup *PosLookup,
	cpg *CPG,
	prog *Progress,
) {
	prog.Log("Extracting type relationships...")

	// Collect all named types and interfaces from known module packages
	type namedInfo struct {
		obj *types.TypeName
		id  string // CPG node ID
	}

	var concretes []namedInfo
	var ifaces []namedInfo

	for _, pkg := range pkgs {
		scope := pkg.Types.Scope()
		for _, name := range scope.Names() {
			obj, ok := scope.Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}

			// Find the CPG node ID via position
			pos := fset.Position(obj.Pos())
			relFile := modSet.RelFile(pos.Filename)
			if relFile == "" {
				continue
			}
			nodeID := posLookup.Get(relFile, pos.Line, pos.Column)
			if nodeID == "" {
				continue
			}

			info := namedInfo{obj: obj, id: nodeID}
			if types.IsInterface(obj.Type()) {
				ifaces = append(ifaces, info)
			} else {
				concretes = append(concretes, info)
			}
		}
	}

	// Alias edges: type alias → aliased type
	var aliasCount int
	for _, pkg := range pkgs {
		scope := pkg.Types.Scope()
		for _, name := range scope.Names() {
			obj, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || !obj.IsAlias() {
				continue
			}
			// Find alias node
			pos := fset.Position(obj.Pos())
			relFile := modSet.RelFile(pos.Filename)
			if relFile == "" {
				continue
			}
			aliasID := posLookup.Get(relFile, pos.Line, pos.Column)
			if aliasID == "" {
				continue
			}
			// Find target type's node.
			// Unalias handles Go 1.23+ where obj.Type() returns *types.Alias
			// instead of directly resolving to *types.Named.
			target := types.Unalias(obj.Type())
			if named, ok := target.(*types.Named); ok {
				tObj := named.Obj()
				if tObj != nil && tObj.Pos().IsValid() {
					tPos := fset.Position(tObj.Pos())
					tFile := modSet.RelFile(tPos.Filename)
					if tFile != "" {
						if tID := posLookup.Get(tFile, tPos.Line, tPos.Column); tID != "" {
							cpg.AddEdge(Edge{Source: aliasID, Target: tID, Kind: "alias_of"})
							aliasCount++
						}
					}
				}
			}
		}
	}

	// Check implements relationships
	var implementsCount, embedsCount, satisfiesCount int

	for _, concrete := range concretes {
		concreteType := concrete.obj.Type()
		ptrType := types.NewPointer(concreteType)

		for _, iface := range ifaces {
			ifaceType, ok := iface.obj.Type().Underlying().(*types.Interface)
			if !ok {
				continue
			}
			if ifaceType.NumMethods() == 0 {
				continue // skip empty interfaces
			}

			if types.Implements(concreteType, ifaceType) || types.Implements(ptrType, ifaceType) {
				cpg.AddEdge(Edge{
					Source: concrete.id,
					Target: iface.id,
					Kind:   "implements",
				})
				implementsCount++

				// satisfies_method: concrete method → interface method it satisfies
				emitSatisfiesMethod(concreteType, ifaceType, fset, posLookup, cpg, &satisfiesCount)
			}
		}

		// Check embedded fields
		st, ok := concreteType.Underlying().(*types.Struct)
		if !ok {
			continue
		}
		for field := range st.Fields() {
			if !field.Embedded() {
				continue
			}

			// Find the embedded type's node ID
			embeddedType := field.Type()
			if ptr, ok := embeddedType.(*types.Pointer); ok {
				embeddedType = ptr.Elem()
			}
			named, ok := embeddedType.(*types.Named)
			if !ok {
				continue
			}
			embObj := named.Obj()
			if embObj == nil || !embObj.Pos().IsValid() {
				continue
			}

			embPos := fset.Position(embObj.Pos())
			embFile := modSet.RelFile(embPos.Filename)
			if embFile == "" {
				continue
			}
			embID := posLookup.Get(embFile, embPos.Line, embPos.Column)
			if embID == "" {
				continue
			}

			cpg.AddEdge(Edge{
				Source: concrete.id,
				Target: embID,
				Kind:   "embeds",
			})
			embedsCount++
		}
	}

	prog.Log("Created %d implements, %d embeds, %d alias_of, %d satisfies_method edges", implementsCount, embedsCount, aliasCount, satisfiesCount)
}

// emitSatisfiesMethod connects each method on concreteType to the interface method
// it satisfies. This enables tracing which concrete method fulfills which interface contract.
func emitSatisfiesMethod(
	concreteType types.Type,
	ifaceType *types.Interface,
	fset *token.FileSet,
	posLookup *PosLookup,
	cpg *CPG,
	count *int,
) {
	// Build method set for both T and *T.
	// Use a local counter to decide whether T already covered this pair,
	// avoiding false early-return when *count has edges from previous calls.
	for _, base := range []types.Type{concreteType, types.NewPointer(concreteType)} {
		mset := types.NewMethodSet(base)
		localFound := 0
		for i := 0; i < ifaceType.NumMethods(); i++ {
			ifaceMethod := ifaceType.Method(i)
			sel := mset.Lookup(ifaceMethod.Pkg(), ifaceMethod.Name())
			if sel == nil || len(sel.Index()) != 1 {
				continue // promoted method or not found
			}
			concreteMethod := sel.Obj()

			// Resolve both to CPG node IDs via position
			cmPos := fset.Position(concreteMethod.Pos())
			cmFile := modSet.RelFile(cmPos.Filename)
			if cmFile == "" {
				continue
			}
			cmID := posLookup.Get(cmFile, cmPos.Line, cmPos.Column)

			imPos := fset.Position(ifaceMethod.Pos())
			imFile := modSet.RelFile(imPos.Filename)
			if imFile == "" {
				continue
			}
			imID := posLookup.Get(imFile, imPos.Line, imPos.Column)

			if cmID != "" && imID != "" {
				cpg.AddEdge(Edge{Source: cmID, Target: imID, Kind: "satisfies_method"})
				*count++
				localFound++
			}
		}
		if localFound > 0 {
			return // found methods on this receiver type, don't duplicate for pointer
		}
	}
}
