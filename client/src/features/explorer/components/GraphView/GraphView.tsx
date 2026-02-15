import { useEffect, useRef, useState } from 'react';
import cytoscape, { type Core, type NodeSingular } from 'cytoscape';
import type { ApiNode, ApiEdge } from '@/shared/types';
import { getModuleFromPackage, complexityToSize } from '@/shared/utils/packageMap';
import './GraphView.css';

export interface GraphViewProps {
  nodes: ApiNode[];
  edges: ApiEdge[];
  /** Shown above graph when set (e.g. "Data flow"). Edge styling uses data(kind) on edges. */
  graphTitle?: string | null;
  onNodeClick?: (node: ApiNode) => void;
  emptyMessage?: string;
  /** When set, node size and color are driven by package map (total_complexity, module). */
  modulePalette?: Record<string, string>;
  /** Called when layout has finished (so parent can hide loading overlay after paint). */
  onLayoutComplete?: () => void;
}

const MAX_NODE_LABEL_LENGTH = 11;

function nodeLabel(node: ApiNode): string {
  if (node.name) return node.name;
  const pkg = node.package;
  if (pkg) {
    const parts = pkg.split('/');
    return parts[parts.length - 1] || pkg;
  }
  return node.id.slice(0, 20) + (node.id.length > 20 ? '…' : '');
}

function truncateLabel(full: string, maxLen: number): string {
  if (full.length <= maxLen) return full;
  return full.slice(0, maxLen) + '…';
}

export function GraphView({
  nodes,
  edges,
  graphTitle,
  onNodeClick,
  emptyMessage,
  modulePalette,
  onLayoutComplete,
}: GraphViewProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const layoutRef = useRef<{ stop(): void } | null>(null);
  const [tooltip, setTooltip] = useState<{ content: string; x: number; y: number } | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const cy = cytoscape({
      container: containerRef.current,
      elements: [],
      style: [
        {
          selector: 'node',
          style: {
            label: 'data(label)',
            color: '#e2e8f0',
            'text-valign': 'bottom',
            'text-halign': 'center',
            'font-size': '10px',
            'text-wrap': 'ellipsis',
            'text-max-width': '12em',
            width: 'data(size)',
            height: 'data(size)',
            'background-color': 'data(backgroundColor)',
            'border-width': 1,
            'border-color': 'data(borderColor)',
          },
        },
        {
          selector: 'edge',
          style: {
            width: 1,
            'line-color': '#718096',
            'target-arrow-color': '#718096',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
          },
        },
        {
          selector: 'edge[kind="dfg"], edge[kind="param_in"], edge[kind="param_out"]',
          style: {
            'line-color': '#38a169',
            'target-arrow-color': '#38a169',
            width: 1.5,
          },
        },
        {
          selector: ':selected',
          style: { 'background-color': '#e53e3e', 'border-color': '#c53030' },
        },
        {
          selector: ':hover',
          style: { 'border-width': 2 },
        },
      ],
      layout: { name: 'cose', animate: false },
    });
    cyRef.current = cy;
    return () => {
      layoutRef.current?.stop();
      layoutRef.current = null;
      cy.destroy();
      cyRef.current = null;
    };
  }, []);

  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    layoutRef.current?.stop();
    layoutRef.current = null;

    const safeNodes = nodes ?? [];
    let safeEdges = edges ?? [];
    const nodeIdSet = new Set(safeNodes.map((n) => n.id));
    safeEdges = safeEdges.filter((e) => nodeIdSet.has(e.source) && nodeIdSet.has(e.target));
    if (safeNodes.length === 0 && safeEdges.length === 0) {
      cy.elements().remove();
      onLayoutComplete?.();
      return;
    }

    const cyNodes = safeNodes.map((n) => {
      const { id, ...rest } = n;
      const fullLabel = nodeLabel(n);
      const data: Record<string, unknown> = {
        id,
        label: truncateLabel(fullLabel, MAX_NODE_LABEL_LENGTH),
        fullLabel,
        ...rest,
      };
      if (modulePalette) {
        const pkg = n.package ?? (n as { package?: string }).package;
        const mod = getModuleFromPackage(pkg != null ? String(pkg) : undefined);
        const color = modulePalette[mod] ?? '#718096';
        data.size = complexityToSize((n as { total_complexity?: number }).total_complexity);
        data.backgroundColor = color;
        data.borderColor = color;
      } else {
        data.size = 20;
        data.backgroundColor = '#4a90d9';
        data.borderColor = '#2c5282';
      }
      return {
        group: 'nodes' as const,
        data,
      };
    });
    const cyEdges = safeEdges.map((e, i) => ({
      group: 'edges' as const,
      data: {
        id: `e${i}-${e.source}-${e.target}`,
        source: e.source,
        target: e.target,
        kind: e.kind ?? undefined,
      },
    }));
    cy.elements().remove();
    cy.add([...cyNodes, ...cyEdges]);
    const layout = cy.layout({ name: 'cose', animate: true, animationDuration: 300 });
    layoutRef.current = layout;
    const handleLayoutStop = () => {
      onLayoutComplete?.();
    };
    layout.on('layoutstop', handleLayoutStop);
    layout.run();
    return () => {
      layout.removeListener('layoutstop', handleLayoutStop);
      layout.stop();
      layoutRef.current = null;
    };
  }, [nodes, edges, modulePalette, onLayoutComplete]);

  useEffect(() => {
    const cy = cyRef.current;
    const container = containerRef.current;
    if (!cy) return;
    const nodeList = nodes ?? [];
    const tapHandler = (ev: { target: NodeSingular }) => {
      if (!onNodeClick) return;
      const cyNode = ev.target;
      if (!cyNode.isNode()) return;
      const id = cyNode.data('id') as string | undefined;
      const apiNode = id ? nodeList.find((n) => n.id === id) : null;
      const payload = apiNode ?? (cyNode.data() as ApiNode & { label?: string });
      onNodeClick(payload);
    };
    const mouseoverHandler = (ev: { target: NodeSingular }) => {
      const full = ev.target.data('fullLabel') as string | undefined;
      const label = ev.target.data('label') as string | undefined;
      if (!full || full === label) {
        setTooltip(null);
        return;
      }
      const cy = cyRef.current;
      if (!cy || !container) return;
      const rpos = ev.target.renderedPosition();
      const rect = container.getBoundingClientRect();
      setTooltip({
        content: full,
        x: rect.left + rpos.x,
        y: rect.top + rpos.y,
      });
    };
    const mouseoutHandler = () => setTooltip(null);
    cy.on('tap', 'node', tapHandler);
    cy.on('mouseover', 'node', mouseoverHandler);
    cy.on('mouseout', 'node', mouseoutHandler);
    return () => {
      cy.off('tap', 'node', tapHandler);
      cy.off('mouseover', 'node', mouseoverHandler);
      cy.off('mouseout', 'node', mouseoutHandler);
      setTooltip(null);
    };
  }, [onNodeClick, nodes]);

  const isEmpty = (nodes ?? []).length === 0 && (edges ?? []).length === 0;

  return (
    <div className="graph-view-container">
      {graphTitle && (
        <div className="graph-view-title" aria-live="polite">
          {graphTitle}
        </div>
      )}
      <div
        ref={containerRef}
        className="graph-view-cy"
        role="img"
        aria-label="Graph of nodes and edges"
      />
      {tooltip && (
        <div
          className="graph-view-tooltip"
          style={{
            left: tooltip.x,
            top: tooltip.y,
          }}
          role="tooltip"
        >
          {tooltip.content}
        </div>
      )}
      {isEmpty && emptyMessage && (
        <div className="graph-view-empty">{emptyMessage}</div>
      )}
    </div>
  );
}
