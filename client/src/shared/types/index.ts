/** Node from API (search, subgraph, package-graph) */
export interface ApiNode {
  id: string;
  kind?: string | null;
  name?: string | null;
  package?: string | null;
  file?: string | null;
  line?: number | null;
  [key: string]: unknown;
}

/** Edge from API */
export interface ApiEdge {
  source: string;
  target: string;
  kind?: string | null;
  [key: string]: unknown;
}

/** Subgraph response: /api/subgraph, /api/package-graph, /api/slice */
export interface SubgraphResponse {
  nodes: ApiNode[];
  edges: ApiEdge[];
}

/** Search response: /api/search — array of nodes */
export type SearchResponse = ApiNode[];

/** Source response: /api/source */
export interface SourceResponse {
  file: string;
  package?: string | null;
  content: string;
}

/** Function row from /api/package/functions (dashboard_function_detail) */
export interface FunctionDetail {
  id: string;
  name: string;
  package: string;
  file: string;
  line: number;
  end_line?: number;
  signature?: string;
  complexity?: number;
  loc?: number;
  fan_in?: number;
  fan_out?: number;
  num_params?: number;
  callers?: string;
  callees?: string;
}

/** Response: /api/package/functions — array of function details */
export type PackageFunctionsResponse = FunctionDetail[];

/** Graph display mode (call graph vs data-flow) */
export type GraphMode = 'call' | 'dfg';
