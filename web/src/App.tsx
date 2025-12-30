import { useState, useEffect, useRef } from 'react';
import { Layout, Card, Button, Switch, Space, Badge, Tooltip, message, Divider, Alert, Typography, ConfigProvider, theme } from 'antd';
import {
  SyncOutlined,
  DatabaseOutlined,
  ClearOutlined,
  WifiOutlined,
  DisconnectOutlined,
  SettingOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import { SchemaTree } from './components/SchemaTree';
import { LogViewer } from './components/LogViewer';
import { SyncProgress } from './components/SyncProgress';
import { useWebSocket } from './hooks/useWebSocket';
import { useApi } from './hooks/useApi';
import type { TableSelection } from './types';

const { Header, Content } = Layout;
const { Text } = Typography;

const MIN_SIDER_WIDTH = 280;
const MAX_SIDER_WIDTH = 600;
const DEFAULT_SIDER_WIDTH = 350;

function App() {
  const { connected, syncState, clearLogs, resetState } = useWebSocket();
  const { tables, tableInfoMap, config, loading, loadingStats, error, refreshStats, startSync, clearError } = useApi();
  
  const [selection, setSelection] = useState<TableSelection>({});
  const [reconcileMode, setReconcileMode] = useState(false);
  const [dryRun, setDryRun] = useState(false);
  const [siderWidth, setSiderWidth] = useState(DEFAULT_SIDER_WIDTH);
  const [isResizing, setIsResizing] = useState(false);
  const siderRef = useRef<HTMLDivElement>(null);

  // Get selected tables count
  const selectedTablesCount = Object.values(selection).filter(s => s.selected).length;

  const handleStartSync = async () => {
    clearError();
    resetState();
    
    // Build tables and columns from selection
    const selectedTables = Object.entries(selection)
      .filter(([, s]) => s.selected)
      .map(([name]) => name);
    
    const columns: { [table: string]: string[] } = {};
    for (const [tableName, sel] of Object.entries(selection)) {
      if (sel.selected && sel.columns.size > 0) {
        columns[tableName] = Array.from(sel.columns);
      }
    }
    
    const success = await startSync({
      tables: selectedTables.length > 0 ? selectedTables : undefined,
      columns: Object.keys(columns).length > 0 ? columns : undefined,
      reconcile: reconcileMode,
      dryRun: dryRun,
    });
    
    if (success) {
      message.success('Sync started');
    } else {
      message.error('Failed to start sync');
    }
  };

  const handleSelectAll = () => {
    const newSelection: TableSelection = {};
    for (const tableName of tables) {
      const tableInfo = tableInfoMap.get(tableName);
      if (tableInfo && tableInfo.existsInTarget) {
        const allColumns = tableInfo.columns.map(c => c.name);
        // Only select columns that exist in target
        const syncableColumns = tableInfo.columns.filter(c => c.existsInTarget).map(c => c.name);
        newSelection[tableName] = {
          selected: true,
          columns: new Set(syncableColumns),
          allColumns,
          primaryKeys: tableInfo.primaryKey,
        };
      }
    }
    setSelection(newSelection);
  };

  const handleSelectNone = () => {
    const newSelection: TableSelection = {};
    for (const tableName of tables) {
      const tableInfo = tableInfoMap.get(tableName);
      if (tableInfo) {
        const allColumns = tableInfo.columns.map(c => c.name);
        newSelection[tableName] = {
          selected: false,
          columns: new Set(allColumns),
          allColumns,
          primaryKeys: tableInfo.primaryKey,
        };
      }
    }
    setSelection(newSelection);
  };

  const handleRefresh = async () => {
    await refreshStats();
    message.success('Stats refreshed');
  };

  // Refresh stats when sync completes
  useEffect(() => {
    if (syncState.progress === 100 && !syncState.running && !syncState.error) {
      refreshStats();
    }
  }, [syncState.progress, syncState.running, syncState.error]);

  // Resizable sider logic
  const handleMouseDown = (e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  };

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      const newWidth = e.clientX;
      if (newWidth >= MIN_SIDER_WIDTH && newWidth <= MAX_SIDER_WIDTH) {
        setSiderWidth(newWidth);
      }
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing]);

  return (
    <ConfigProvider
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          colorBgContainer: '#ffffff',
          colorBgLayout: '#f5f5f5',
        },
      }}
    >
      <Layout style={{ minHeight: '100vh', background: '#f5f5f5' }}>
        <Header style={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between', 
          padding: '0 24px',
          background: '#ffffff',
          borderBottom: '1px solid #e8e8e8',
          height: 56,
          flexShrink: 0,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <DatabaseOutlined style={{ fontSize: 24, color: '#1890ff' }} />
            <span style={{ fontSize: 20, fontWeight: 600, color: '#000' }}>PGSync</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Tooltip title={connected ? 'Connected to server' : 'Disconnected from server'}>
              <Space size={4}>
                <Badge status={connected ? 'success' : 'error'} />
                {connected ? (
                  <WifiOutlined style={{ color: '#52c41a' }} />
                ) : (
                  <DisconnectOutlined style={{ color: '#ff4d4f' }} />
                )}
              </Space>
            </Tooltip>
          </div>
        </Header>

        <div style={{ display: 'flex', flexDirection: 'row', flex: 1, overflow: 'hidden', background: '#f5f5f5' }}>
          {/* Resizable Sider */}
          <div
            ref={siderRef}
            style={{ 
              width: siderWidth,
              minWidth: MIN_SIDER_WIDTH,
              maxWidth: MAX_SIDER_WIDTH,
              background: '#ffffff', 
              borderRight: '1px solid #e8e8e8',
              display: 'flex',
              flexDirection: 'column',
              position: 'relative',
              flexShrink: 0,
            }}
          >
            {/* Sider Header */}
            <div style={{ padding: 16, borderBottom: '1px solid #f0f0f0', flexShrink: 0 }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
                <span style={{ fontWeight: 500, color: '#000' }}>Tables</span>
                <Space size={8}>
                  <Tooltip title="Refresh stats">
                    <Button 
                      size="small" 
                      icon={<ReloadOutlined />} 
                      onClick={handleRefresh}
                      loading={loadingStats}
                    />
                  </Tooltip>
                  <Button size="small" onClick={handleSelectAll} style={{ minWidth: 40 }}>All</Button>
                  <Button size="small" onClick={handleSelectNone} style={{ minWidth: 50 }}>None</Button>
                </Space>
              </div>
              <div style={{ fontSize: 12, color: '#999' }}>
                {selectedTablesCount === 0 
                  ? `All ${tables.length} tables will be synced`
                  : `${selectedTablesCount} of ${tables.length} tables selected`}
              </div>
            </div>
            
            {/* Scrollable Tree */}
            <div style={{ flex: 1, overflow: 'auto', padding: '8px 16px 16px' }}>
              <SchemaTree
                tables={tables}
                tableInfoMap={tableInfoMap}
                loading={loading}
                loadingStats={loadingStats}
                selection={selection}
                onSelectionChange={setSelection}
              />
            </div>

            {/* Resize Handle */}
            <div
              onMouseDown={handleMouseDown}
              style={{
                position: 'absolute',
                right: -3,
                top: 0,
                bottom: 0,
                width: 6,
                cursor: 'col-resize',
                zIndex: 10,
              }}
            />
          </div>

          {/* Main Content */}
          <Content style={{ 
            padding: 24, 
            background: '#f5f5f5', 
            overflow: 'auto',
            flex: 1,
          }}>
            <div style={{ maxWidth: 900, margin: '0 auto' }}>
              {error && (
                <Alert 
                  type="error" 
                  message="Error" 
                  description={error} 
                  closable 
                  onClose={clearError}
                  style={{ marginBottom: 16 }}
                />
              )}

              {/* Connection Info */}
              {config && (
                <Card size="small" style={{ marginBottom: 16, overflow: 'hidden' }} bodyStyle={{ padding: 0 }}>
                  <div style={{ display: 'flex' }}>
                    <div style={{ flex: 1, padding: '12px 16px', borderRight: '1px solid #f0f0f0', minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                        <div style={{ width: 8, height: 8, borderRadius: '50%', background: '#1890ff', flexShrink: 0 }} />
                        <Text type="secondary" style={{ fontSize: 12, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Source</Text>
                      </div>
                      <Tooltip title={config.sourceDb} placement="bottom">
                        <Text code style={{ fontSize: 11, background: 'transparent', border: 'none', padding: 0, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{config.sourceDb}</Text>
                      </Tooltip>
                    </div>
                    <div style={{ flex: 1, padding: '12px 16px', background: '#fafafa', minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                        <div style={{ width: 8, height: 8, borderRadius: '50%', background: '#52c41a', flexShrink: 0 }} />
                        <Text type="secondary" style={{ fontSize: 12, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Target</Text>
                      </div>
                      <Tooltip title={config.targetDb} placement="bottom">
                        <Text code style={{ fontSize: 11, background: 'transparent', border: 'none', padding: 0, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{config.targetDb}</Text>
                      </Tooltip>
                    </div>
                  </div>
                </Card>
              )}

              {/* Sync Options */}
              <Card 
                title={
                  <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <SettingOutlined />
                    Sync Options
                  </span>
                }
                size="small"
                style={{ marginBottom: 16 }}
              >
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 24 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <Switch 
                      checked={reconcileMode} 
                      onChange={setReconcileMode}
                      disabled={syncState.running}
                    />
                    <Tooltip title="Compare all rows by primary key instead of using timestamps. Slower but ensures full consistency.">
                      <span style={{ color: '#000' }}>Reconcile Mode</span>
                    </Tooltip>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <Switch 
                      checked={dryRun} 
                      onChange={setDryRun}
                      disabled={syncState.running}
                    />
                    <Tooltip title="Preview changes without actually modifying the target database.">
                      <span style={{ color: '#000' }}>Dry Run</span>
                    </Tooltip>
                  </div>
                </div>
              </Card>

              {/* Sync Control */}
              <Card
                title={
                  <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <SyncOutlined spin={syncState.running} />
                    Sync Progress
                  </span>
                }
                style={{ marginBottom: 16 }}
              >
                <div>
                  <Space size={12} style={{ marginBottom: 16 }}>
                    <Button
                      type="primary"
                      icon={<SyncOutlined />}
                      onClick={handleStartSync}
                      loading={syncState.running}
                      disabled={syncState.running || !connected}
                      style={{ minWidth: 120 }}
                    >
                      {syncState.running ? 'Syncing...' : 'Start Sync'}
                    </Button>
                    <Button
                      icon={<ClearOutlined />}
                      onClick={() => { clearLogs(); resetState(); }}
                      disabled={syncState.running}
                      style={{ minWidth: 80 }}
                    >
                      Clear
                    </Button>
                  </Space>

                  <Divider style={{ margin: '16px 0' }} />

                  <SyncProgress syncState={syncState} />
                </div>
              </Card>

              {/* Log Viewer */}
              <Card
                title="Logs"
                extra={
                  <span style={{ fontSize: 12, color: '#999' }}>
                    {syncState.logs.length} entries
                  </span>
                }
              >
                <LogViewer logs={syncState.logs} maxHeight={400} />
              </Card>
            </div>
          </Content>
        </div>
      </Layout>
    </ConfigProvider>
  );
}

export default App;
