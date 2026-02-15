import { useReducer, useState, useCallback, useRef, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import * as api from '@/api';
import type { ApiNode, SubgraphResponse } from '@/shared/types';
import { buildModulePalette, buildSliceLinesByFile, isPackageGraphNodes } from '@/shared/utils/packageMap';
import { ExplorerActionType, initialExplorerState, explorerReducer } from '../state/explorerReducer';
import { useSourceLoader } from './useSourceLoader';

export function useCpgExplorer() {
  const [state, dispatch] = useReducer(explorerReducer, initialExplorerState);
  const [searchQuery, setSearchQuery] = useState('');
  const source = useSourceLoader();
  const [searchParams, setSearchParams] = useSearchParams();

  const initialUrlProcessedRef = useRef(false);
  const hasAttemptedLoadRef = useRef(false);
  const codePanelExpandedRef = useRef(state.codePanelExpanded);
  codePanelExpandedRef.current = state.codePanelExpanded;

  const graphDataLoading =
    state.loading &&
    (state.loadingMessage === 'Loading graph…' || state.loadingMessage === 'Loading slice…');

  const clearError = useCallback(() => dispatch({ type: ExplorerActionType.ERROR, value: null }), []);
  const handleGraphLayoutComplete = useCallback(
    () => dispatch({ type: ExplorerActionType.GRAPH_RENDERING, value: false }),
    [],
  );
  const setCodePanelExpanded = useCallback(
    (value: boolean | ((prev: boolean) => boolean)) => {
      const next =
        typeof value === 'function' ? value(codePanelExpandedRef.current) : value;
      dispatch({ type: ExplorerActionType.CODE_PANEL_EXPANDED, value: next });
    },
    [],
  );

  const loadSubgraph = useCallback(
    async (nodeId: string): Promise<SubgraphResponse> => {
      hasAttemptedLoadRef.current = true;
      dispatch({ type: ExplorerActionType.RESET_FOR_SUBGRAPH });
      dispatch({ type: ExplorerActionType.LOADING, value: true, message: 'Loading graph…' });
      dispatch({ type: ExplorerActionType.ERROR, value: null });
      dispatch({ type: ExplorerActionType.GRAPH_DATA, nodes: [], edges: [], graphMode: 'call' });
      try {
        const data = await api.apiSubgraph(nodeId, api.SUBGRAPH_CALL_GRAPH_LIMIT);
        dispatch({
          type: ExplorerActionType.GRAPH_DATA,
          nodes: data.nodes ?? [],
          edges: data.edges ?? [],
          graphMode: 'call',
        });
        dispatch({ type: ExplorerActionType.GRAPH_RENDERING, value: true });
        dispatch({ type: ExplorerActionType.SEARCH_RESULTS, value: null });
        setSearchParams((prev) => {
          const next = new URLSearchParams(prev);
          next.set('node_id', nodeId);
          next.delete('q');
          return next;
        });
        return data;
      } catch (e) {
        dispatch({ type: ExplorerActionType.ERROR, value: e instanceof Error ? e.message : String(e) });
        return { nodes: [], edges: [] };
      } finally {
        dispatch({ type: ExplorerActionType.LOADING, value: false });
      }
    },
    [setSearchParams],
  );

  const loadPackageGraph = useCallback(async () => {
    hasAttemptedLoadRef.current = true;
    source.clearSource();
    dispatch({ type: ExplorerActionType.RESET_FOR_PACKAGE });
    dispatch({ type: ExplorerActionType.LOADING, value: true, message: 'Loading graph…' });
    dispatch({ type: ExplorerActionType.ERROR, value: null });
    dispatch({ type: ExplorerActionType.GRAPH_DATA, nodes: [], edges: [], graphMode: 'call' });
    dispatch({ type: ExplorerActionType.SLICE_LINES, value: {} });
    setSearchQuery('');
    setSearchParams({});
    try {
      const data = await api.apiPackageGraph();
      const nodeList = data.nodes ?? [];
      const edgeList = data.edges ?? [];
      dispatch({ type: ExplorerActionType.GRAPH_DATA, nodes: nodeList, edges: edgeList, graphMode: 'call' });
      dispatch({ type: ExplorerActionType.SEARCH_RESULTS, value: null });
      const { palette, moduleNames: names } = buildModulePalette(nodeList);
      dispatch({ type: ExplorerActionType.GRAPH_MODULE_PALETTE, palette, moduleNames: names });
      dispatch({ type: ExplorerActionType.GRAPH_RENDERING, value: true });
    } catch (e) {
      dispatch({ type: ExplorerActionType.ERROR, value: e instanceof Error ? e.message : String(e) });
    } finally {
      dispatch({ type: ExplorerActionType.LOADING, value: false });
    }
  }, [setSearchParams, source]);

  const loadSlice = useCallback(
    async (nodeId: string, direction: api.SliceDirection) => {
      hasAttemptedLoadRef.current = true;
      dispatch({ type: ExplorerActionType.LOADING, value: true, message: 'Loading slice…' });
      dispatch({ type: ExplorerActionType.ERROR, value: null });
      try {
        const data = await api.apiSlice(nodeId, direction);
        const nodeList = data.nodes ?? [];
        const edgeList = data.edges ?? [];
        dispatch({
          type: ExplorerActionType.GRAPH_DATA,
          nodes: nodeList,
          edges: edgeList,
          graphMode: 'dfg',
        });
        dispatch({ type: ExplorerActionType.SLICE_LINES, value: buildSliceLinesByFile(nodeList) });
        dispatch({ type: ExplorerActionType.GRAPH_RENDERING, value: true });
        dispatch({ type: ExplorerActionType.RESET_FOR_SLICE });
        dispatch({ type: ExplorerActionType.SEARCH_RESULTS, value: null });
        const sel =
          state.selectedNode?.id === nodeId
            ? state.selectedNode
            : nodeList[0] ?? null;
        if (sel?.file) {
          dispatch({ type: ExplorerActionType.SELECT_NODE, node: sel });
          const ln = sel.line != null ? Number(sel.line) : null;
          source.loadSourceForFile(
            sel.file,
            typeof ln === 'number' && !Number.isNaN(ln) ? ln : null,
          );
        }
      } catch (e) {
        dispatch({ type: ExplorerActionType.ERROR, value: e instanceof Error ? e.message : String(e) });
      } finally {
        dispatch({ type: ExplorerActionType.LOADING, value: false });
      }
    },
    [state.selectedNode, source],
  );

  const doSearch = useCallback(async () => {
    const q = searchQuery.trim();
    if (!q) {
      dispatch({ type: ExplorerActionType.SEARCH_RESULTS, value: [] });
      return;
    }
    hasAttemptedLoadRef.current = true;
    dispatch({ type: ExplorerActionType.LOADING, value: true, message: 'Searching…' });
    dispatch({ type: ExplorerActionType.ERROR, value: null });
    dispatch({ type: ExplorerActionType.SEARCH_RESULTS, value: null });
    try {
      const list = await api.apiSearch(q, 20);
      const results = list ?? [];
      dispatch({ type: ExplorerActionType.SEARCH_RESULTS, value: results });
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.set('q', q);
        next.delete('node_id');
        return next;
      });
      if (results.length === 0) {
        dispatch({ type: ExplorerActionType.GRAPH_DATA, nodes: [], edges: [] });
      }
    } catch (e) {
      dispatch({ type: ExplorerActionType.ERROR, value: e instanceof Error ? e.message : String(e) });
    } finally {
      dispatch({ type: ExplorerActionType.LOADING, value: false });
    }
  }, [searchQuery, setSearchParams]);

  const handleSelectSearchResult = useCallback(
    (node: ApiNode) => {
      setSearchQuery('');
      dispatch({ type: ExplorerActionType.SEARCH_RESULTS, value: null });
      dispatch({ type: ExplorerActionType.SELECT_NODE, node });
      const file = node.file ?? null;
      const line = node.line != null ? node.line : null;
      if (file) {
        source.loadSourceForFile(file, typeof line === 'number' ? line : null);
      } else {
        source.clearSource();
      }
      loadSubgraph(node.id).then((data) => {
        const center = data.nodes.find((n) => n.id === node.id);
        if (center) dispatch({ type: ExplorerActionType.SELECT_NODE, node: center });
      });
    },
    [source, loadSubgraph],
  );

  const handleNodeClick = useCallback(
    (node: ApiNode) => {
      dispatch({ type: ExplorerActionType.SELECT_NODE, node });
      const isPkgGraph = isPackageGraphNodes(state.nodes);
      if (!isPkgGraph) dispatch({ type: ExplorerActionType.NODE_ACTIONS_ENABLED, value: true });
      if (isPkgGraph) {
        source.clearSource();
        dispatch({ type: ExplorerActionType.PACKAGE_FUNCTIONS, value: null });
        dispatch({ type: ExplorerActionType.PACKAGE_FUNCTIONS_LOADING, value: true });
        api
          .apiPackageFunctions(node.id)
          .then((list) => dispatch({ type: ExplorerActionType.PACKAGE_FUNCTIONS, value: list }))
          .catch(() => dispatch({ type: ExplorerActionType.PACKAGE_FUNCTIONS, value: [] }))
          .finally(() => dispatch({ type: ExplorerActionType.PACKAGE_FUNCTIONS_LOADING, value: false }));
        return;
      }
      const file = node.file ?? null;
      const line = node.line ?? null;
      if (!file) {
        source.clearSource();
        return;
      }
      source.loadSourceForFile(file, typeof line === 'number' ? line : null);
    },
    [state.nodes, source],
  );

  useEffect(() => {
    if (initialUrlProcessedRef.current) return;
    initialUrlProcessedRef.current = true;
    const nodeId = searchParams.get('node_id');
    const q = searchParams.get('q');
    if (q != null) setSearchQuery(q);
    if (nodeId) {
      loadSubgraph(nodeId).then((data) => {
        const n = data.nodes.find((x) => x.id === nodeId);
        if (n) {
          dispatch({ type: ExplorerActionType.SELECT_NODE, node: n });
          const file = n.file ?? null;
          const line = n.line != null ? n.line : null;
          if (file) source.loadSourceForFile(file, typeof line === 'number' ? line : null);
        }
      });
    }
  }, [searchParams, loadSubgraph, source]);

  return {
    searchQuery,
    setSearchQuery,
    nodes: state.nodes,
    edges: state.edges,
    selectedNode: state.selectedNode,
    sourceFile: source.sourceFile,
    sourceContent: source.sourceContent,
    sourceLoadFailed: source.sourceLoadFailed,
    highlightLine: source.highlightLine,
    searchResults: state.searchResults,
    loading: state.loading,
    loadingMessage: state.loadingMessage,
    loadSourcePending: source.loadSourcePending,
    error: state.error,
    graphMode: state.graphMode,
    modulePalette: state.modulePalette,
    moduleNames: state.moduleNames,
    packageFunctions: state.packageFunctions,
    loadingPackageFunctions: state.loadingPackageFunctions,
    sliceLinesByFile: state.sliceLinesByFile,
    codePanelExpanded: state.codePanelExpanded,
    setCodePanelExpanded,
    graphRendering: state.graphRendering,
    graphDataLoading,
    nodeActionsEnabled: state.nodeActionsEnabled,
    hasAttemptedLoadRef,
    clearError,
    handleGraphLayoutComplete,
    loadSubgraph,
    loadPackageGraph,
    loadSlice,
    doSearch,
    handleSelectSearchResult,
    handleNodeClick,
  };
}
