import { GraphView, CodePanel, MetadataPanel } from '@/features/explorer/components';
import { useCpgExplorer } from '@/features/explorer/hooks/useCpgExplorer';
import { isPackageGraphNodes } from '@/shared/utils/packageMap';
import '@/app/App.css';

export function ExplorerPage() {
  const {
    searchQuery,
    setSearchQuery,
    nodes,
    edges,
    selectedNode,
    sourceFile,
    sourceContent,
    sourceLoadFailed,
    highlightLine,
    searchResults,
    loading,
    loadingMessage,
    loadSourcePending,
    error,
    graphMode,
    modulePalette,
    moduleNames,
    packageFunctions,
    loadingPackageFunctions,
    sliceLinesByFile,
    codePanelExpanded,
    setCodePanelExpanded,
    graphRendering,
    graphDataLoading,
    nodeActionsEnabled,
    hasAttemptedLoadRef,
    clearError,
    handleGraphLayoutComplete,
    loadSubgraph,
    loadSlice,
    loadPackageGraph,
    doSearch,
    handleSelectSearchResult,
    handleNodeClick,
  } = useCpgExplorer();

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-title">CPG Explorer</div>
        <div className="app-toolbar">
          <input
            type="text"
            className="app-search-input"
            placeholder="Search function or package…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && doSearch()}
          />
          <button type="button" className="app-btn" onClick={doSearch} disabled={loading}>
            Search
          </button>
          <button
            type="button"
            className="app-btn"
            onClick={loadPackageGraph}
            disabled={loading}
            title="Load package dependency graph (Package Map)"
          >
            Package Map
          </button>
        </div>
      </header>

      {error && (
        <div className="app-error" role="alert">
          <span>{error}</span>
          <button
            type="button"
            className="app-error-dismiss"
            onClick={clearError}
            aria-label="Dismiss"
          >
            ×
          </button>
        </div>
      )}

      {loading && !graphDataLoading && (
        <div className="app-loading-overlay" aria-hidden aria-busy="true">
          <div className="app-loading-spinner" />
          <span>{loadingMessage}</span>
        </div>
      )}

      <div className="app-main">
        <aside className="app-sidebar app-sidebar-left">
          <MetadataPanel
            node={selectedNode}
            searchResults={searchResults}
            onSelectSearchResult={handleSelectSearchResult}
            onLoadSubgraph={loadSubgraph}
            onBackwardSlice={selectedNode ? () => loadSlice(selectedNode.id, 'backward') : undefined}
            onForwardSlice={selectedNode ? () => loadSlice(selectedNode.id, 'forward') : undefined}
            sliceLoading={loading}
            isPackageGraph={isPackageGraphNodes(nodes)}
            modulePalette={modulePalette ?? undefined}
            moduleNames={moduleNames}
            packageFunctions={packageFunctions}
            loadingPackageFunctions={loadingPackageFunctions}
            onBackToPackageMap={loadPackageGraph}
            nodeActionsEnabled={nodeActionsEnabled}
          />
        </aside>
        <div className="app-main-content">
          <main className="app-graph-area">
            {(graphDataLoading || graphRendering) && (
              <div
                className="app-loading-overlay app-loading-overlay-graph"
                aria-hidden
                aria-busy="true"
              >
                <div className="app-loading-spinner" />
                <span>{graphDataLoading ? loadingMessage : 'Rendering graph…'}</span>
              </div>
            )}
            <GraphView
              nodes={nodes}
              edges={edges}
              graphTitle={graphMode === 'dfg' ? 'Data flow' : null}
              onNodeClick={handleNodeClick}
              modulePalette={modulePalette ?? undefined}
              onLayoutComplete={handleGraphLayoutComplete}
              emptyMessage={
                searchResults !== null && searchResults.length === 0
                  ? 'Nothing found'
                  : hasAttemptedLoadRef.current && nodes.length === 0 && edges.length === 0
                    ? 'No data'
                    : 'Enter a search term or click «Package Map» to start.'
              }
            />
          </main>
          <div className="app-sidebar-right-spacer" aria-hidden />
          <aside
            className={`app-sidebar app-sidebar-right ${codePanelExpanded ? 'app-sidebar-right-expanded' : ''}`}
          >
            <CodePanel
              file={sourceFile}
              content={sourceContent}
              highlightLine={highlightLine}
              highlightLines={sourceFile ? sliceLinesByFile[sourceFile] ?? null : null}
              loading={loadSourcePending}
              sourceLoadFailed={sourceLoadFailed}
              expanded={codePanelExpanded}
              onExpandToggle={() => setCodePanelExpanded((v) => !v)}
              emptyStateMessage={
                selectedNode && !sourceFile
                  ? isPackageGraphNodes(nodes)
                    ? `Package «${selectedNode.name ?? selectedNode.package ?? selectedNode.id}» selected. Choose a function from the list to view source.`
                    : 'Selected node has no source location. Try another node.'
                  : null
              }
            />
          </aside>
        </div>
      </div>
    </div>
  );
}
