import { useEffect, useRef } from 'react';
import './CodePanel.css';

interface CodePanelProps {
  file: string | null;
  content: string | null;
  highlightLine: number | null;
  /** Line numbers to highlight as part of data-flow slice (weaker than highlightLine) */
  highlightLines?: Set<number> | number[] | null;
  loading?: boolean;
  sourceLoadFailed?: boolean;
  /** When no file/content: show this instead of default "Click a node…" (e.g. for selected package) */
  emptyStateMessage?: string | null;
  /** Panel expanded to the left (full width for reading). */
  expanded?: boolean;
  /** Toggle expand/collapse. */
  onExpandToggle?: () => void;
}

export function CodePanel({
  file,
  content,
  highlightLine,
  highlightLines,
  loading,
  sourceLoadFailed = false,
  emptyStateMessage,
  expanded = false,
  onExpandToggle,
}: CodePanelProps) {
  const lineRefs = useRef<Map<number, HTMLDivElement>>(new Map());

  useEffect(() => {
    lineRefs.current.clear();
  }, [file]);

  useEffect(() => {
    if (highlightLine === null) return;
    const el = lineRefs.current.get(highlightLine);
    el?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }, [highlightLine, file]);

  const sliceSet =
    highlightLines instanceof Set
      ? highlightLines
      : Array.isArray(highlightLines)
        ? new Set(highlightLines)
        : null;
  const headerTitle = file ?? 'Source';
  const header = (
    <div className="code-panel-header">
      <span className="code-panel-header-title">{headerTitle}</span>
      {onExpandToggle && (
        <button
          type="button"
          className="code-panel-expand-btn"
          onClick={onExpandToggle}
          title={expanded ? 'Collapse code panel' : 'Expand code panel to the left'}
          aria-label={expanded ? 'Collapse code panel' : 'Expand code panel to the left'}
        >
          {expanded ? '◧' : '◐'}
        </button>
      )}
    </div>
  );

  if (loading) {
    return (
      <div className="code-panel">
        {header}
        <div className="code-panel-loading">Loading…</div>
      </div>
    );
  }
  if (file && content === null && sourceLoadFailed) {
    return (
      <div className="code-panel">
        {header}
        <div className="code-panel-error">Failed to load source.</div>
      </div>
    );
  }
  if (!file && !content) {
    return (
      <div className="code-panel">
        {header}
        <div className="code-panel-empty">
          {emptyStateMessage ?? 'Click a node to view source code.'}
        </div>
      </div>
    );
  }
  const lines = (content ?? '').split('\n');

  return (
    <div className="code-panel">
      {header}
      <pre className="code-panel-content">
        <code>
          {lines.map((line, i) => {
            const lineNum = i + 1;
            const isFocused = highlightLine !== null && lineNum === highlightLine;
            const isInSlice = sliceSet !== null && sliceSet.has(lineNum);
            const className = [
              'code-line',
              isFocused ? 'code-line-highlight' : '',
              isInSlice && !isFocused ? 'code-line-slice' : '',
            ]
              .filter(Boolean)
              .join(' ');
            return (
              <div
                key={lineNum}
                className={className}
                ref={(el) => {
                  if (el) lineRefs.current.set(lineNum, el);
                }}
              >
                <span className="code-line-num">{lineNum}</span>
                <span className="code-line-text">{line || '\n'}</span>
              </div>
            );
          })}
        </code>
      </pre>
    </div>
  );
}
