import { useEffect, useRef, useCallback, useMemo } from 'react';
import type { LogEntry } from '../types';

interface LogViewerProps {
  logs: LogEntry[];
  maxHeight?: number;
}

export function LogViewer({ logs, maxHeight = 400 }: LogViewerProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const autoScrollRef = useRef(true);

  useEffect(() => {
    if (autoScrollRef.current && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [logs]);

  const handleScroll = useCallback(() => {
    if (!containerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
    // Enable auto-scroll if user is near the bottom
    autoScrollRef.current = scrollHeight - scrollTop - clientHeight < 50;
  }, []);

  const formatTime = useMemo(() => (date: Date) => {
    return date.toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  }, []);

  if (logs.length === 0) {
    return (
      <div 
        style={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'center', 
          color: '#bfbfbf',
          height: maxHeight,
          background: '#fafafa',
          borderRadius: 4,
        }}
      >
        No logs yet. Start a sync to see progress.
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      style={{ 
        maxHeight, 
        overflow: 'auto', 
        background: '#fafafa', 
        borderRadius: 4, 
        border: '1px solid #e8e8e8' 
      }}
    >
      {logs.map((log) => (
        <div key={log.id} className={`log-entry ${log.level}`}>
          <span className="log-timestamp">{formatTime(log.timestamp)}</span>
          {log.table && (
            <span style={{ color: '#722ed1', marginRight: 8 }}>[{log.table}]</span>
          )}
          <span>{log.message}</span>
        </div>
      ))}
    </div>
  );
}
