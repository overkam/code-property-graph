import type { ApiNode, FunctionDetail, GraphMode } from '@/shared/types';

export enum ExplorerActionType {
  GRAPH_DATA = 'GRAPH_DATA',
  GRAPH_MODULE_PALETTE = 'GRAPH_MODULE_PALETTE',
  GRAPH_RENDERING = 'GRAPH_RENDERING',
  GRAPH_RESET_MODULE = 'GRAPH_RESET_MODULE',
  SELECT_NODE = 'SELECT_NODE',
  PACKAGE_FUNCTIONS = 'PACKAGE_FUNCTIONS',
  PACKAGE_FUNCTIONS_LOADING = 'PACKAGE_FUNCTIONS_LOADING',
  SLICE_LINES = 'SLICE_LINES',
  SEARCH_RESULTS = 'SEARCH_RESULTS',
  LOADING = 'LOADING',
  ERROR = 'ERROR',
  CODE_PANEL_EXPANDED = 'CODE_PANEL_EXPANDED',
  NODE_ACTIONS_ENABLED = 'NODE_ACTIONS_ENABLED',
  RESET_FOR_SUBGRAPH = 'RESET_FOR_SUBGRAPH',
  RESET_FOR_PACKAGE = 'RESET_FOR_PACKAGE',
  RESET_FOR_SLICE = 'RESET_FOR_SLICE',
}

export interface ExplorerState {
  nodes: ApiNode[];
  edges: { source: string; target: string; kind?: string | null }[];
  graphMode: GraphMode;
  modulePalette: Record<string, string> | null;
  moduleNames: string[];
  selectedNode: ApiNode | null;
  packageFunctions: FunctionDetail[] | null;
  loadingPackageFunctions: boolean;
  sliceLinesByFile: Record<string, Set<number>>;
  codePanelExpanded: boolean;
  graphRendering: boolean;
  nodeActionsEnabled: boolean;
  searchResults: ApiNode[] | null;
  loading: boolean;
  loadingMessage: string;
  error: string | null;
}

export const initialExplorerState: ExplorerState = {
  nodes: [],
  edges: [],
  graphMode: 'call',
  modulePalette: null,
  moduleNames: [],
  selectedNode: null,
  packageFunctions: null,
  loadingPackageFunctions: false,
  sliceLinesByFile: {},
  codePanelExpanded: false,
  graphRendering: false,
  nodeActionsEnabled: false,
  searchResults: null,
  loading: false,
  loadingMessage: 'Loading…',
  error: null,
};

export type ExplorerAction =
  | { type: ExplorerActionType.GRAPH_DATA; nodes: ApiNode[]; edges: ExplorerState['edges']; graphMode?: GraphMode }
  | { type: ExplorerActionType.GRAPH_MODULE_PALETTE; palette: Record<string, string>; moduleNames: string[] }
  | { type: ExplorerActionType.GRAPH_RENDERING; value: boolean }
  | { type: ExplorerActionType.GRAPH_RESET_MODULE }
  | { type: ExplorerActionType.SELECT_NODE; node: ApiNode | null }
  | { type: ExplorerActionType.PACKAGE_FUNCTIONS; value: FunctionDetail[] | null }
  | { type: ExplorerActionType.PACKAGE_FUNCTIONS_LOADING; value: boolean }
  | { type: ExplorerActionType.SLICE_LINES; value: Record<string, Set<number>> }
  | { type: ExplorerActionType.SEARCH_RESULTS; value: ApiNode[] | null }
  | { type: ExplorerActionType.LOADING; value: boolean; message?: string }
  | { type: ExplorerActionType.ERROR; value: string | null }
  | { type: ExplorerActionType.CODE_PANEL_EXPANDED; value: boolean }
  | { type: ExplorerActionType.NODE_ACTIONS_ENABLED; value: boolean }
  | { type: ExplorerActionType.RESET_FOR_SUBGRAPH }
  | { type: ExplorerActionType.RESET_FOR_PACKAGE }
  | { type: ExplorerActionType.RESET_FOR_SLICE };

export function explorerReducer(state: ExplorerState, action: ExplorerAction): ExplorerState {
  switch (action.type) {
    case ExplorerActionType.GRAPH_DATA:
      return {
        ...state,
        nodes: action.nodes,
        edges: action.edges,
        ...(action.graphMode != null && { graphMode: action.graphMode }),
      };
    case ExplorerActionType.GRAPH_MODULE_PALETTE:
      return {
        ...state,
        modulePalette: action.palette,
        moduleNames: action.moduleNames,
      };
    case ExplorerActionType.GRAPH_RENDERING:
      return { ...state, graphRendering: action.value };
    case ExplorerActionType.GRAPH_RESET_MODULE:
      return {
        ...state,
        modulePalette: null,
        moduleNames: [],
      };
    case ExplorerActionType.SELECT_NODE:
      return { ...state, selectedNode: action.node };
    case ExplorerActionType.PACKAGE_FUNCTIONS:
      return { ...state, packageFunctions: action.value };
    case ExplorerActionType.PACKAGE_FUNCTIONS_LOADING:
      return { ...state, loadingPackageFunctions: action.value };
    case ExplorerActionType.SLICE_LINES:
      return { ...state, sliceLinesByFile: action.value };
    case ExplorerActionType.SEARCH_RESULTS:
      return { ...state, searchResults: action.value };
    case ExplorerActionType.LOADING:
      return {
        ...state,
        loading: action.value,
        ...(action.message != null && { loadingMessage: action.message }),
      };
    case ExplorerActionType.ERROR:
      return { ...state, error: action.value };
    case ExplorerActionType.CODE_PANEL_EXPANDED:
      return { ...state, codePanelExpanded: action.value };
    case ExplorerActionType.NODE_ACTIONS_ENABLED:
      return { ...state, nodeActionsEnabled: action.value };
    case ExplorerActionType.RESET_FOR_SUBGRAPH:
      return {
        ...state,
        sliceLinesByFile: {},
        modulePalette: null,
        moduleNames: [],
        packageFunctions: null,
        nodeActionsEnabled: false,
        searchResults: null,
      };
    case ExplorerActionType.RESET_FOR_PACKAGE:
      return {
        ...state,
        packageFunctions: null,
        searchResults: null,
        selectedNode: null,
      };
    case ExplorerActionType.RESET_FOR_SLICE:
      return {
        ...state,
        nodeActionsEnabled: false,
      };
    default:
      return state;
  }
}
