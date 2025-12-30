import { useState, useEffect, useCallback, useRef } from 'react';
import type { SchemaResponse, TableInfo, SyncRequest, ConfigResponse } from '../types';

const API_BASE = import.meta.env.DEV ? 'http://localhost:8080' : '';

export function useApi() {
  const [tables, setTables] = useState<string[]>([]);
  const [tableInfoMap, setTableInfoMap] = useState<Map<string, TableInfo>>(new Map());
  const [schema, setSchema] = useState<string>('');
  const [config, setConfig] = useState<ConfigResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingStats, setLoadingStats] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const fetchConfig = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE}/api/config`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data: ConfigResponse = await response.json();
      setConfig(data);
      setSchema(data.schema);
    } catch (err) {
      console.error('Failed to fetch config:', err);
    }
  }, []);

  const fetchTables = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/api/schema/tables`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data: SchemaResponse = await response.json();
      setTables(data.tables || []);
      setSchema(data.schema);
      return data.tables || [];
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to fetch tables';
      setError(message);
      console.error('Failed to fetch tables:', err);
      return [];
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchTableInfo = useCallback(async (tableName: string, signal?: AbortSignal): Promise<TableInfo | null> => {
    try {
      const response = await fetch(`${API_BASE}/api/schema/table/${tableName}`, { signal });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      return await response.json();
    } catch (err) {
      if ((err as Error).name === 'AbortError') {
        return null;
      }
      console.error(`Failed to fetch table info for ${tableName}:`, err);
      return null;
    }
  }, []);

  const fetchAllTableInfo = useCallback(async (tableList?: string[]) => {
    const tablesToFetch = tableList || tables;
    if (tablesToFetch.length === 0) return;

    // Cancel any ongoing fetch
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    abortControllerRef.current = new AbortController();
    const signal = abortControllerRef.current.signal;

    setLoadingStats(true);

    // Fetch in parallel batches of 5
    const batchSize = 5;
    for (let i = 0; i < tablesToFetch.length; i += batchSize) {
      if (signal.aborted) break;
      
      const batch = tablesToFetch.slice(i, i + batchSize);
      const results = await Promise.all(
        batch.map(tableName => fetchTableInfo(tableName, signal))
      );
      
      // Update incrementally so UI shows progress
      setTableInfoMap(prev => {
        const updated = new Map(prev);
        results.forEach((info, idx) => {
          if (info) {
            updated.set(batch[idx], info);
          }
        });
        return updated;
      });
    }

    setLoadingStats(false);
    // Note: `tables` is intentionally excluded from deps to avoid re-render loops.
    // Callers should pass tableList explicitly when tables state might be stale.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fetchTableInfo]);

  const refreshStats = useCallback(async () => {
    const tableList = await fetchTables();
    if (tableList.length > 0) {
      await fetchAllTableInfo(tableList);
    }
  }, [fetchTables, fetchAllTableInfo]);

  const startSync = useCallback(async (request: SyncRequest = {}): Promise<boolean> => {
    try {
      const response = await fetch(`${API_BASE}/api/sync/start`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(request),
      });
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `HTTP ${response.status}: ${response.statusText}`);
      }
      return true;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to start sync';
      setError(message);
      console.error('Failed to start sync:', err);
      return false;
    }
  }, []);

  useEffect(() => {
    const init = async () => {
      await fetchConfig();
      const tableList = await fetchTables();
      if (tableList.length > 0) {
        await fetchAllTableInfo(tableList);
      }
    };
    init();

    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, [fetchConfig, fetchTables, fetchAllTableInfo]);

  return {
    tables,
    tableInfoMap,
    schema,
    config,
    loading,
    loadingStats,
    error,
    fetchTables,
    fetchTableInfo,
    fetchAllTableInfo,
    refreshStats,
    startSync,
    clearError: () => setError(null),
  };
}
