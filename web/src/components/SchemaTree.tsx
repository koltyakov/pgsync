import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { Tree, Spin, Tag, Tooltip } from 'antd';
import { TableOutlined, KeyOutlined, LinkOutlined } from '@ant-design/icons';
import type { DataNode } from 'antd/es/tree';
import type { TableInfo, ColumnInfo, TableSelection } from '../types';

interface SchemaTreeProps {
  tables: string[];
  tableInfoMap: Map<string, TableInfo>;
  loading: boolean;
  loadingStats: boolean;
  selection: TableSelection;
  onSelectionChange: (selection: TableSelection) => void;
}

export function SchemaTree({
  tables,
  tableInfoMap,
  loading,
  loadingStats,
  selection,
  onSelectionChange,
}: SchemaTreeProps) {
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const treeRef = useRef<HTMLDivElement>(null);

  // Jump to a table in the tree
  const jumpToTable = useCallback((tableName: string) => {
    // Expand the table if not already expanded
    if (!expandedKeys.includes(tableName)) {
      setExpandedKeys(prev => [...prev, tableName]);
    }
    // Scroll to the table after a brief delay to allow expansion
    setTimeout(() => {
      const treeNode = treeRef.current?.querySelector(`[data-tree-node="${tableName}"]`);
      if (treeNode) {
        treeNode.scrollIntoView({ behavior: 'smooth', block: 'center' });
        // Flash highlight
        treeNode.classList.add('tree-node-highlight');
        setTimeout(() => treeNode.classList.remove('tree-node-highlight'), 1500);
      }
    }, 100);
  }, [expandedKeys]);

  // Build tree data from tables
  const treeData: DataNode[] = useMemo(() => {
    return tables.map((tableName) => {
      const tableInfo = tableInfoMap.get(tableName);
      const tableSelection = selection[tableName];
      const sourceCount = tableInfo?.sourceRowCount ?? tableInfo?.rowCount;
      const targetCount = tableInfo?.targetRowCount;
      const tableExistsInTarget = tableInfo?.existsInTarget ?? true;

      const children: DataNode[] = tableInfo?.columns.map((col: ColumnInfo) => {
        const colKey = `${tableName}.${col.name}`;
        // Column is selected if: table is selected AND column is in the columns set
        // When table is not selected, columns should appear grayed out
        const tableIsSelected = tableSelection?.selected ?? false;
        const isSelected = tableIsSelected && (tableSelection?.columns.has(col.name) ?? false);
        const colExistsInTarget = col.existsInTarget;
        const cannotSync = !tableExistsInTarget || !colExistsInTarget;
        
        return {
          key: colKey,
          title: (
            <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              {col.isPrimary && (
                <Tooltip title="Primary Key">
                  <KeyOutlined style={{ color: '#faad14' }} />
                </Tooltip>
              )}
              {col.isForeignKey && (
                <Tooltip title={`FK → ${col.fkReferencesTable}`}>
                  <LinkOutlined 
                    style={{ color: '#722ed1', cursor: 'pointer' }} 
                    onClick={(e) => {
                      e.stopPropagation();
                      if (col.fkReferencesTable) {
                        jumpToTable(col.fkReferencesTable);
                      }
                    }}
                  />
                </Tooltip>
              )}
              <span style={{ 
                color: cannotSync ? '#ff4d4f' : (isSelected ? '#000' : '#bfbfbf'),
                textDecoration: cannotSync ? 'line-through' : 'none',
              }}>{col.name}</span>
              <Tag color={cannotSync ? 'error' : (isSelected ? 'blue' : 'default')} style={{ fontSize: 11 }}>{col.dataType}</Tag>
              {col.isNullable && <Tag color="default" style={{ fontSize: 11 }}>nullable</Tag>}
              {!colExistsInTarget && tableExistsInTarget && (
                <Tooltip title="Column missing in target database">
                  <Tag color="error" style={{ fontSize: 10 }}>missing</Tag>
                </Tooltip>
              )}
            </span>
          ),
          isLeaf: true,
          disabled: col.isPrimary || cannotSync, // Primary keys and missing columns cannot be toggled
          checkable: true,
        };
      }) || [];

      // Format row count display
      const formatRowCount = () => {
        if (!tableInfo) return null;
        if (!tableExistsInTarget) {
          return <Tag color="error" style={{ fontSize: 11 }}>missing in target</Tag>;
        }
        const src = sourceCount?.toLocaleString() ?? '?';
        const tgt = targetCount?.toLocaleString() ?? '0';
        const diff = (sourceCount ?? 0) - (targetCount ?? 0);
        
        if (diff === 0) {
          return <span style={{ color: '#52c41a', fontSize: 12 }}>{src} ✓</span>;
        } else if (diff > 0) {
          return (
            <span style={{ fontSize: 12 }}>
              <span style={{ color: '#1890ff' }}>{src}</span>
              <span style={{ color: '#bfbfbf' }}> → </span>
              <span style={{ color: '#8c8c8c' }}>{tgt}</span>
              <span style={{ color: '#faad14', marginLeft: 4 }}>+{diff.toLocaleString()}</span>
            </span>
          );
        } else {
          return (
            <span style={{ fontSize: 12 }}>
              <span style={{ color: '#1890ff' }}>{src}</span>
              <span style={{ color: '#bfbfbf' }}> → </span>
              <span style={{ color: '#8c8c8c' }}>{tgt}</span>
              <span style={{ color: '#ff4d4f', marginLeft: 4 }}>{diff.toLocaleString()}</span>
            </span>
          );
        }
      };

      // Check if this is a partial sync (some columns deselected)
      const syncableColumns = tableInfo?.columns.filter(c => c.existsInTarget) || [];
      const selectedColumns = tableSelection?.columns || new Set<string>();
      const isPartialSync = tableSelection?.selected && 
        selectedColumns.size > 0 && 
        selectedColumns.size < syncableColumns.length;

      return {
        key: tableName,
        title: (
          <span style={{ display: 'flex', alignItems: 'center', gap: 8, whiteSpace: 'nowrap' }}>
            <TableOutlined style={{ flexShrink: 0, color: tableExistsInTarget ? undefined : '#ff4d4f' }} />
            <span style={{ 
              fontWeight: 500, 
              overflow: 'hidden', 
              textOverflow: 'ellipsis',
              color: tableExistsInTarget ? undefined : '#ff4d4f',
              textDecoration: tableExistsInTarget ? 'none' : 'line-through',
            }}>{tableName}</span>
            {isPartialSync && (
              <Tooltip title={`Partial sync: ${selectedColumns.size} of ${syncableColumns.length} columns selected`}>
                <Tag color="warning" style={{ fontSize: 10 }}>
                  {selectedColumns.size}/{syncableColumns.length} cols
                </Tag>
              </Tooltip>
            )}
            <span style={{ flexShrink: 0 }}>
              {tableInfo ? (
                formatRowCount()
              ) : loadingStats ? (
                <Spin size="small" />
              ) : null}
            </span>
          </span>
        ),
        children: children.length > 0 ? children : undefined,
        disabled: !tableExistsInTarget,
      };
    });
  }, [tables, tableInfoMap, selection, loadingStats, jumpToTable]);

  // Calculate checked keys based on selection
  const checkedKeys = useMemo(() => {
    const keys: string[] = [];
    
    for (const tableName of tables) {
      const tableSelection = selection[tableName];
      
      if (!tableSelection?.selected) continue;
      
      // Always add table key when selected (Tree will show partial check if not all columns selected)
      keys.push(tableName);
      
      // Add selected column keys
      for (const col of tableSelection.columns) {
        keys.push(`${tableName}.${col}`);
      }
    }
    
    return keys;
  }, [tables, selection]);

  // Handle check changes
  const handleCheck = useCallback((
    checked: React.Key[] | { checked: React.Key[]; halfChecked: React.Key[] }
  ) => {
    const checkedKeysArray = Array.isArray(checked) ? checked : checked.checked;
    const checkedSet = new Set(checkedKeysArray.map(k => String(k)));
    
    const newSelection: TableSelection = {};
    
    for (const tableName of tables) {
      const tableInfo = tableInfoMap.get(tableName);
      const columns = tableInfo?.columns || [];
      const primaryKeys = tableInfo?.primaryKey || [];
      const allColumnNames = columns.map(c => c.name);
      // Only include columns that exist in target
      const syncableColumns = columns.filter(c => c.existsInTarget).map(c => c.name);
      const prevSelection = selection[tableName];
      
      // Get previous state
      const wasTableSelected = prevSelection?.selected ?? false;
      const prevColumns = prevSelection?.columns ?? new Set<string>();
      const prevColumnCount = prevColumns.size;
      
      // Get current checked columns from tree
      const checkedColumns = new Set<string>();
      for (const col of columns) {
        const colKey = `${tableName}.${col.name}`;
        if (checkedSet.has(colKey)) {
          checkedColumns.add(col.name);
        }
      }
      
      // Check if table key is in checked set
      const tableKeyChecked = checkedSet.has(tableName);
      
      // Detect what changed - column count same means table checkbox was toggled
      const columnCountSame = checkedColumns.size === prevColumnCount;
      const tableToggled = wasTableSelected !== tableKeyChecked && columnCountSame;
      
      // Determine new state
      let isSelected = false;
      let finalColumns = new Set<string>();
      
      if (tableToggled && tableKeyChecked) {
        // Table checkbox was checked - select only syncable columns (exist in target)
        isSelected = true;
        finalColumns = new Set(syncableColumns);
      } else if (tableToggled && !tableKeyChecked) {
        // Table checkbox was unchecked - deselect all
        isSelected = false;
        finalColumns = new Set<string>();
      } else {
        // Column-level operation - use exactly what's checked
        if (checkedColumns.size > 0) {
          isSelected = true;
          finalColumns = new Set(checkedColumns);
          // Always include primary keys when table is selected
          for (const pk of primaryKeys) {
            finalColumns.add(pk);
          }
        } else {
          isSelected = false;
          finalColumns = new Set<string>();
        }
      }
      
      newSelection[tableName] = {
        selected: isSelected,
        columns: finalColumns,
        allColumns: allColumnNames,
        primaryKeys,
      };
    }
    
    onSelectionChange(newSelection);
  }, [tables, tableInfoMap, selection, onSelectionChange]);

  // Initialize selection when table info is loaded
  useEffect(() => {
    if (tables.length === 0 || tableInfoMap.size === 0) return;
    
    // Only initialize if selection is empty
    const hasSelection = Object.keys(selection).length > 0;
    if (hasSelection) return;
    
    const initialSelection: TableSelection = {};
    for (const tableName of tables) {
      const tableInfo = tableInfoMap.get(tableName);
      if (tableInfo) {
        const allColumns = tableInfo.columns.map(c => c.name);
        // Not selected by default - columns set is empty (will be populated when table is checked)
        initialSelection[tableName] = {
          selected: false,
          columns: new Set<string>(),
          allColumns,
          primaryKeys: tableInfo.primaryKey,
        };
      }
    }
    onSelectionChange(initialSelection);
  }, [tables, tableInfoMap]);

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 32 }}>
        <Spin tip="Loading tables..." />
      </div>
    );
  }

  if (tables.length === 0) {
    return (
      <div style={{ textAlign: 'center', color: '#999', padding: 16 }}>
        No tables found in schema
      </div>
    );
  }

  return (
    <div ref={treeRef}>
      <style>{`
        .tree-node-highlight {
          background-color: #e6f7ff !important;
          transition: background-color 0.3s;
        }
        .ant-tree-treenode {
          transition: background-color 0.3s;
        }
      `}</style>
      <Tree
        checkable
        showLine
        checkedKeys={checkedKeys}
        expandedKeys={expandedKeys}
        onExpand={(keys) => setExpandedKeys(keys as string[])}
        onCheck={handleCheck}
        treeData={treeData}
        style={{ background: '#ffffff' }}
        checkStrictly
        titleRender={(nodeData) => {
          const key = String(nodeData.key);
          // Only add data attribute to table nodes (not column nodes which have dots)
          const isTable = !key.includes('.');
          return (
            <span data-tree-node={isTable ? key : undefined}>
              {nodeData.title as React.ReactNode}
            </span>
          );
        }}
      />
    </div>
  );
}
