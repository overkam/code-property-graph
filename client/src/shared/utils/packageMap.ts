import type { ApiNode } from '../types';
import { PACKAGE_MODULE_COLORS } from '../constants';

/** Extract module from package path (e.g. github.com/prometheus/prometheus/config -> prometheus) */
export function getModuleFromPackage(pkg: string | null | undefined): string {
  if (!pkg || typeof pkg !== 'string') return 'other';
  const parts = pkg.trim().split('/');
  if (parts.length >= 2) return parts[1];
  return parts[0] || 'other';
}

/** Build module -> color map from package graph nodes; returns record and sorted module names for legend */
export function buildModulePalette(
  nodes: ApiNode[],
): { palette: Record<string, string>; moduleNames: string[] } {
  const modules = new Set<string>();
  for (const n of nodes) {
    const pkg = n.package ?? (n as { package?: string }).package;
    if (pkg) modules.add(getModuleFromPackage(String(pkg)));
  }
  const moduleNames = Array.from(modules).sort();
  const palette: Record<string, string> = {};
  moduleNames.forEach((m, i) => {
    palette[m] = PACKAGE_MODULE_COLORS[i % PACKAGE_MODULE_COLORS.length];
  });
  return { palette, moduleNames };
}

const SIZE_MIN = 14;
const SIZE_MAX = 48;

/** Scale total_complexity to node size in px */
export function complexityToSize(totalComplexity: number | null | undefined): number {
  if (totalComplexity == null || typeof totalComplexity !== 'number' || totalComplexity <= 0)
    return SIZE_MIN;
  const log = Math.log10(totalComplexity + 1);
  const t = Math.min(log / 3, 1);
  return Math.round(SIZE_MIN + t * (SIZE_MAX - SIZE_MIN));
}

/** Check if nodes look like package graph nodes (from /api/package-graph) */
export function isPackageGraphNodes(nodes: ApiNode[]): boolean {
  return nodes.length > 0 && 'total_complexity' in nodes[0];
}

/** Build file -> set of line numbers from graph nodes (for slice highlighting). */
export function buildSliceLinesByFile(
  nodes: ApiNode[],
): Record<string, Set<number>> {
  const byFile: Record<string, Set<number>> = {};
  for (const n of nodes) {
    const file = n.file ?? null;
    const line = n.line != null ? Number(n.line) : null;
    if (file && line != null && !Number.isNaN(line)) {
      if (!byFile[file]) byFile[file] = new Set();
      byFile[file].add(line);
    }
  }
  return byFile;
}
