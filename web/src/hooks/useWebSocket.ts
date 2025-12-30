import { useEffect, useRef, useState, useCallback } from 'react';
import type { ProgressMessage, SyncState, LogEntry } from '../types';

// In production, use the same host. In dev, connect to Go backend on port 8080
const getWsUrl = () => {
  if (import.meta.env.DEV) {
    return `ws://${window.location.hostname}:8080/ws`;
  }
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}/ws`;
};

// Counter for unique log IDs within session
let logIdCounter = 0;

export function useWebSocket() {
  const [connected, setConnected] = useState(false);
  const [syncState, setSyncState] = useState<SyncState>({
    running: false,
    progress: 0,
    tableIndex: 0,
    totalTables: 0,
    logs: [],
  });
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const connectRef = useRef<() => void>(() => {});

  const addLog = useCallback((level: LogEntry['level'], message: string, table?: string) => {
    const entry: LogEntry = {
      id: `log-${Date.now()}-${++logIdCounter}`,
      timestamp: new Date(),
      level,
      message,
      table,
    };
    setSyncState(prev => ({
      ...prev,
      logs: [...prev.logs, entry].slice(-500), // Keep last 500 logs
    }));
  }, []);

  const handleMessage = useCallback((event: MessageEvent) => {
    try {
      const msg: ProgressMessage = JSON.parse(event.data);
      
      // Ignore empty or malformed messages
      if (!msg || !msg.type) {
        return;
      }
      
      switch (msg.type) {
        case 'progress':
          setSyncState(prev => ({
            ...prev,
            running: true,
            progress: msg.progress ?? prev.progress,
            currentTable: msg.table ?? prev.currentTable,
            tableIndex: msg.tableIndex ?? prev.tableIndex,
            totalTables: msg.totalTables ?? prev.totalTables,
          }));
          if (msg.message) {
            addLog(msg.level || 'info', msg.message, msg.table);
          }
          break;
        
        case 'log':
          if (msg.message) {
            addLog(msg.level || 'info', msg.message, msg.table);
          }
          break;
        
        case 'complete':
          setSyncState(prev => ({
            ...prev,
            running: false,
            progress: 100,
            stats: msg.stats,
          }));
          if (msg.message) {
            addLog('info', msg.message);
          }
          break;
        
        case 'error':
          setSyncState(prev => ({
            ...prev,
            running: false,
            error: msg.message,
          }));
          if (msg.message) {
            addLog('error', msg.message);
          }
          break;
        
        case 'status':
          setSyncState(prev => ({
            ...prev,
            running: prev.running,
            progress: msg.progress ?? prev.progress,
          }));
          break;
      }
    } catch (err) {
      console.error('Failed to parse WebSocket message:', err);
    }
  }, [addLog]);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(getWsUrl());
    
    ws.onopen = () => {
      console.log('WebSocket connected');
      setConnected(true);
    };
    
    ws.onclose = () => {
      console.log('WebSocket disconnected');
      setConnected(false);
      // Reconnect after 3 seconds
      reconnectTimeoutRef.current = window.setTimeout(() => {
        reconnectTimeoutRef.current = null; // Clear ref after timeout fires
        connectRef.current();
      }, 3000);
    };
    
    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
    
    ws.onmessage = handleMessage;
    
    wsRef.current = ws;
  }, [handleMessage]);

  useEffect(() => {
    connectRef.current = connect;
  }, [connect]);

  useEffect(() => {
    connect();
    
    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connect]);

  const clearLogs = useCallback(() => {
    setSyncState(prev => ({ ...prev, logs: [] }));
  }, []);

  const resetState = useCallback(() => {
    setSyncState({
      running: false,
      progress: 0,
      tableIndex: 0,
      totalTables: 0,
      logs: [],
    });
  }, []);

  return {
    connected,
    syncState,
    clearLogs,
    resetState,
  };
}
