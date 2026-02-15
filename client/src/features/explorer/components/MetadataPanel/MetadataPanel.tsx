import { useState } from 'react';
import type { ApiNode, FunctionDetail } from '@/shared/types';
import './MetadataPanel.css';

interface MetadataPanelProps {
  node: ApiNode | null;
  searchResults: ApiNode[] | null;
  onSelectSearchResult: (node: ApiNode) => void;
  onLoadSubgraph: (nodeId: string) => void;
  onBackwardSlice?: (nodeId: string) => void;
  onForwardSlice?: (nodeId: string) => void;
  sliceLoading?: boolean;
  isPackageGraph?: boolean;
  modulePalette?: Record<string, string>;
  moduleNames?: string[];
  packageFunctions?: FunctionDetail[] | null;
  loadingPackageFunctions?: boolean;
  onBackToPackageMap?: () => void;
  /** Show action buttons (Call graph, slices) only after user has selected a node in the current graph. */
  nodeActionsEnabled?: boolean;
}

export function MetadataPanel({
  node,
  searchResults,
  onSelectSearchResult,
  onLoadSubgraph,
  onBackwardSlice,
  onForwardSlice,
  sliceLoading = false,
  isPackageGraph = false,
  modulePalette,
  moduleNames = [],
  packageFunctions,
  loadingPackageFunctions = false,
  onBackToPackageMap,
  nodeActionsEnabled = false,
}: MetadataPanelProps) {
  const [modulesCollapsed, setModulesCollapsed] = useState(false);

  return (
    <div className="metadata-panel">
      <div className="metadata-panel-header">Metadata</div>
      <div className="metadata-panel-body">
        {isPackageGraph && moduleNames.length > 0 && modulePalette && (
          <section className="metadata-section">
            <button
              type="button"
              className="metadata-section-toggle"
              onClick={() => setModulesCollapsed((c) => !c)}
              aria-expanded={!modulesCollapsed}
            >
              <span className="metadata-section-toggle-icon">{modulesCollapsed ? '▶' : '▼'}</span>
              <h4 className="metadata-section-title">Modules</h4>
            </button>
            {!modulesCollapsed && (
              <ul className="metadata-legend-list" aria-label="Module colors">
                {moduleNames.map((mod) => (
                  <li key={mod} className="metadata-legend-item">
                    <span
                      className="metadata-legend-swatch"
                      style={{ backgroundColor: modulePalette[mod] ?? '#718096' }}
                      aria-hidden
                    />
                    <span className="metadata-legend-name">{mod}</span>
                  </li>
                ))}
              </ul>
            )}
          </section>
        )}
        {isPackageGraph && node && (loadingPackageFunctions || packageFunctions != null) && (
          <section className="metadata-section metadata-section-package-list">
            <h4 className="metadata-section-title metadata-section-title-package">
              Package: <span className="metadata-package-name">{node.name ?? node.id}</span>
            </h4>
            {loadingPackageFunctions ? (
              <p className="metadata-loading">Loading…</p>
            ) : packageFunctions && packageFunctions.length > 0 ? (
              <ul className="metadata-list metadata-package-functions">
                {packageFunctions.map((f) => (
                  <li key={f.id} className="metadata-package-fn-item">
                    <span className="metadata-package-fn-name">{f.name}</span>
                    <span className="metadata-package-fn-meta">
                      {f.file}
                      {f.line != null ? `:${f.line}` : ''}
                    </span>
                    <button
                      type="button"
                      className="metadata-load-subgraph-btn"
                      onClick={() => onLoadSubgraph(f.id)}
                      title="Open call graph for this function"
                    >
                      Load graph
                    </button>
                  </li>
                ))}
              </ul>
            ) : packageFunctions && packageFunctions.length === 0 ? (
              <p className="metadata-empty">No data.</p>
            ) : null}
          </section>
        )}
        {searchResults !== null && searchResults.length === 0 && (
          <section className="metadata-section">
            <h4 className="metadata-section-title">Search results</h4>
            <p className="metadata-empty">Nothing found.</p>
          </section>
        )}
        {searchResults !== null && searchResults.length > 0 && (
          <section className="metadata-section">
            <h4 className="metadata-section-title">Search results</h4>
            <ul className="metadata-list metadata-search-results">
              {searchResults.map((n) => (
                <li key={n.id} className="metadata-search-item">
                  <button
                    type="button"
                    className="metadata-search-result-card"
                    onClick={() => onSelectSearchResult(n)}
                    title="Open call graph and show code"
                  >
                    <span className="metadata-search-result-name">{n.name ?? n.id}</span>
                    {n.package != null && (
                      <span className="metadata-search-result-meta">pkg: {String(n.package)}</span>
                    )}
                    {n.file != null && (
                      <span className="metadata-search-result-meta">
                        {String(n.file)}
                        {n.line != null ? `:${n.line}` : ''}
                      </span>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          </section>
        )}
        {node && !isPackageGraph ? (
          <section className="metadata-section">
            {onBackToPackageMap && (
              <button
                type="button"
                className="metadata-back-to-packages"
                onClick={onBackToPackageMap}
              >
                ← Package Map
              </button>
            )}
            <h4 className="metadata-section-title">Selected node</h4>
            <dl className="metadata-dl">
              <dt>id</dt>
              <dd>{node.id}</dd>
              {node.kind != null && (
                <>
                  <dt>kind</dt>
                  <dd>{String(node.kind)}</dd>
                </>
              )}
              {node.name != null && (
                <>
                  <dt>name</dt>
                  <dd>{String(node.name)}</dd>
                </>
              )}
              {node.package != null && (
                <>
                  <dt>package</dt>
                  <dd>{String(node.package)}</dd>
                </>
              )}
              {node.file != null && (
                <>
                  <dt>file</dt>
                  <dd>{String(node.file)}</dd>
                </>
              )}
              {node.line != null && (
                <>
                  <dt>line</dt>
                  <dd>{String(node.line)}</dd>
                </>
              )}
            </dl>
            {!isPackageGraph && nodeActionsEnabled && (
              <button
                type="button"
                className="metadata-load-subgraph-single"
                onClick={() => onLoadSubgraph(node.id)}
              >
                Show neighborhood / Call graph
              </button>
            )}
            {nodeActionsEnabled && (onBackwardSlice || onForwardSlice) && (
              <div className="metadata-slice-buttons">
                <button
                  type="button"
                  className="metadata-slice-btn"
                  onClick={() => onBackwardSlice?.(node.id)}
                  disabled={sliceLoading}
                  title="Backward slice: nodes that influence this node"
                >
                  Backward slice
                </button>
                <button
                  type="button"
                  className="metadata-slice-btn"
                  onClick={() => onForwardSlice?.(node.id)}
                  disabled={sliceLoading}
                  title="Forward slice: nodes that depend on this node"
                >
                  Forward slice
                </button>
              </div>
            )}
          </section>
        ) : null}
      </div>
    </div>
  );
}
