import type {
  PackageFunctionsResponse,
  SearchResponse,
  SourceResponse,
  SubgraphResponse,
} from '../shared/types';
import { checkResponse, fetchWithTimeout, DEFAULT_TIMEOUT_MS } from './client';
import { SUBGRAPH_CALL_GRAPH_LIMIT } from '../shared/constants';

const API_BASE = '/api';

export async function apiSearch(q: string, limit = 50): Promise<SearchResponse> {
  const params = new URLSearchParams({ q });
  if (limit) params.set('limit', String(limit));
  const res = await fetchWithTimeout(`${API_BASE}/search?${params}`);
  await checkResponse(res);
  return res.json();
}

export { SUBGRAPH_CALL_GRAPH_LIMIT };

export async function apiSubgraph(
  nodeId: string,
  limit = SUBGRAPH_CALL_GRAPH_LIMIT,
): Promise<SubgraphResponse> {
  const params = new URLSearchParams({ node_id: nodeId });
  if (limit) params.set('limit', String(limit));
  const res = await fetchWithTimeout(`${API_BASE}/subgraph?${params}`);
  await checkResponse(res);
  return res.json();
}

export async function apiPackageGraph(): Promise<SubgraphResponse> {
  const res = await fetchWithTimeout(`${API_BASE}/package-graph`);
  await checkResponse(res);
  return res.json();
}

export async function apiPackageFunctions(
  packageId: string,
): Promise<PackageFunctionsResponse> {
  const params = new URLSearchParams({ package: packageId });
  const res = await fetchWithTimeout(`${API_BASE}/package/functions?${params}`);
  await checkResponse(res);
  return res.json();
}

export type SliceDirection = 'backward' | 'forward';

export async function apiSlice(
  nodeId: string,
  direction: SliceDirection,
  limit = 200,
): Promise<SubgraphResponse> {
  const params = new URLSearchParams({ node_id: nodeId, direction });
  if (limit) params.set('limit', String(limit));
  const res = await fetchWithTimeout(`${API_BASE}/slice?${params}`);
  await checkResponse(res);
  return res.json();
}

export interface ApiSourceOptions {
  signal?: AbortSignal;
}

export async function apiSource(
  file: string,
  options?: ApiSourceOptions,
): Promise<SourceResponse> {
  const params = new URLSearchParams({ file });
  const res = await fetchWithTimeout(`${API_BASE}/source?${params}`, {
    signal: options?.signal,
    timeoutMs: DEFAULT_TIMEOUT_MS,
  });
  await checkResponse(res);
  return res.json();
}
