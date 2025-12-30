import { Progress as AntProgress } from 'antd';
import { CheckCircleOutlined, SyncOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import type { SyncState } from '../types';

interface SyncProgressProps {
  syncState: SyncState;
}

export function SyncProgress({ syncState }: SyncProgressProps) {
  const { running, progress, currentTable, tableIndex, totalTables, stats, error } = syncState;

  const getStatus = () => {
    if (error) return 'exception';
    if (progress === 100) return 'success';
    if (running) return 'active';
    return 'normal';
  };

  const getIcon = () => {
    if (error) return <ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />;
    if (progress === 100) return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
    if (running) return <SyncOutlined spin style={{ color: '#1890ff' }} />;
    return null;
  };

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
        {getIcon()}
        <div style={{ flex: 1 }}>
          <AntProgress 
            percent={Math.round(progress)} 
            status={getStatus()}
            strokeColor={{
              '0%': '#108ee9',
              '100%': '#87d068',
            }}
          />
        </div>
      </div>

      {running && currentTable && (
        <div style={{ fontSize: 14, color: '#595959', marginBottom: 16 }}>
          Syncing: <span style={{ fontWeight: 500, color: '#1890ff' }}>{currentTable}</span>
          {totalTables > 0 && (
            <span style={{ marginLeft: 8, color: '#bfbfbf' }}>
              ({tableIndex + 1} of {totalTables})
            </span>
          )}
        </div>
      )}

      {stats && (
        <div style={{ display: 'flex', gap: 24, fontSize: 14 }}>
          <div>
            <span style={{ color: '#8c8c8c' }}>Upserts:</span>{' '}
            <span style={{ fontWeight: 500, color: '#52c41a' }}>{stats.totalUpserts.toLocaleString()}</span>
          </div>
          <div>
            <span style={{ color: '#8c8c8c' }}>Deletes:</span>{' '}
            <span style={{ fontWeight: 500, color: '#ff4d4f' }}>{stats.totalDeletes.toLocaleString()}</span>
          </div>
        </div>
      )}

      {error && (
        <div style={{ fontSize: 14, color: '#ff4d4f', background: '#fff2f0', padding: 12, borderRadius: 4, marginTop: 16 }}>
          {error}
        </div>
      )}
    </div>
  );
}
