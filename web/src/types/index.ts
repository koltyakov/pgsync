// API Types

export interface ConfigResponse {
  sourceDb: string;
  targetDb: string;
  schema: string;
}

export interface TableInfo {
  name: string;
  columns: ColumnInfo[];
  primaryKey: string[];
  rowCount: number;
  sourceRowCount?: number;
  targetRowCount?: number;
  existsInTarget: boolean;
}

export interface ColumnInfo {
  name: string;
  dataType: string;
  isNullable: boolean;
  isPrimary: boolean;
  isForeignKey: boolean;
  fkReferencesTable?: string;
  existsInTarget: boolean;
}

export interface SchemaResponse {
  tables: string[];
  schema: string;
}

export interface SyncRequest {
  tables?: string[];
  columns?: { [table: string]: string[] };
  reconcile?: boolean;
  dryRun?: boolean;
  parallel?: number;
  batchSize?: number;
  timestampCol?: string;
}

export interface SyncStats {
  totalUpserts: number;
  totalDeletes: number;
  tableStats?: Record<string, number>;
}

// Selection state for tables and columns
export interface TableSelection {
  [tableName: string]: {
    selected: boolean;
    columns: Set<string>;
    allColumns: string[];
    primaryKeys: string[];
  };
}

// WebSocket Message Types

export type MessageType = 'progress' | 'log' | 'complete' | 'error' | 'status';

export interface ProgressMessage {
  type: MessageType;
  message?: string;
  level?: 'info' | 'debug' | 'warn' | 'error';
  progress?: number;
  table?: string;
  tableIndex?: number;
  totalTables?: number;
  stats?: SyncStats;
  timestamp: string;
}

// UI State Types

export interface SyncState {
  running: boolean;
  progress: number;
  currentTable?: string;
  tableIndex: number;
  totalTables: number;
  logs: LogEntry[];
  stats?: SyncStats;
  error?: string;
}

export interface LogEntry {
  id: string;
  timestamp: Date;
  level: 'info' | 'debug' | 'warn' | 'error';
  message: string;
  table?: string;
}
