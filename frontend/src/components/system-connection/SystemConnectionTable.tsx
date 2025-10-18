import { useMemo } from 'react';
import {
  DataGrid,
  GridActionsCellItem,
  GridColDef,
  GridRowParams,
  GridRowSelectionModel
} from '@mui/x-data-grid';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import FlashOnIcon from '@mui/icons-material/FlashOn';
import { Chip, Tooltip } from '@mui/material';

import { SystemConnection, System } from '../../types/data';
import { formatConnectionSummary, parseJdbcConnectionString } from '../../utils/connectionString';

interface SystemConnectionTableProps {
  data: SystemConnection[];
  systems: System[];
  loading: boolean;
  selectedId?: string | null;
  canManage?: boolean;
  onSelect?: (connection: SystemConnection | null) => void;
  onEdit?: (connection: SystemConnection) => void;
  onDelete?: (connection: SystemConnection) => void;
  onTest?: (connection: SystemConnection) => void;
}

const DATABASE_LABELS: Record<string, string> = {
  postgresql: 'PostgreSQL'
};

const buildColumns = (
  systemLookup: Map<string, string>,
  canManage: boolean,
  onEdit?: (connection: SystemConnection) => void,
  onDelete?: (connection: SystemConnection) => void,
  onTest?: (connection: SystemConnection) => void
): GridColDef<SystemConnection>[] => {
  const baseColumns: GridColDef<SystemConnection>[] = [
    {
      field: 'systemId',
      headerName: 'System',
      flex: 1,
      valueGetter: ({ row }) => systemLookup.get(row.systemId) ?? '—'
    },
    {
      field: 'databaseType',
      headerName: 'Database',
      flex: 0.8,
      valueGetter: ({ row }) => {
        const parsed = parseJdbcConnectionString(row.connectionString);
        if (!parsed) return '—';
        return DATABASE_LABELS[parsed.databaseType] ?? parsed.databaseType;
      }
    },
    {
      field: 'endpoint',
      headerName: 'Endpoint',
      flex: 1.4,
      renderCell: ({ row }) => {
        const summary = formatConnectionSummary(row.connectionString);
        return (
          <Tooltip title={row.notes ?? summary} placement="top" enterDelay={600}>
            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{summary}</span>
          </Tooltip>
        );
      }
    },
    {
      field: 'active',
      headerName: 'Status',
      flex: 0.6,
      renderCell: ({ value }) => (
        <Chip
          label={value ? 'Active' : 'Disabled'}
          color={value ? 'success' : 'default'}
          size="small"
        />
      )
    }
  ];

  if (!canManage) {
    return baseColumns;
  }

  return [
    ...baseColumns,
    {
      field: 'actions',
      type: 'actions',
      headerName: 'Actions',
      getActions: (params: GridRowParams<SystemConnection>) => [
        <GridActionsCellItem
          key="test"
          icon={<FlashOnIcon fontSize="small" />}
          label="Test"
          onClick={() => onTest?.(params.row)}
          showInMenu
        />,
        <GridActionsCellItem
          key="edit"
          icon={<EditIcon fontSize="small" />}
          label="Edit"
          onClick={() => onEdit?.(params.row)}
          showInMenu
        />,
        <GridActionsCellItem
          key="delete"
          icon={<DeleteIcon fontSize="small" />}
          label="Delete"
          onClick={() => onDelete?.(params.row)}
          showInMenu
        />
      ]
    }
  ];
};

const SystemConnectionTable = ({
  data,
  systems,
  loading,
  selectedId,
  canManage = false,
  onSelect,
  onEdit,
  onDelete,
  onTest
}: SystemConnectionTableProps) => {
  const systemLookup = useMemo(
    () => new Map(systems.map((system) => [system.id, system.name])),
    [systems]
  );

  const columns = useMemo(
    () => buildColumns(systemLookup, canManage, onEdit, onDelete, onTest),
    [systemLookup, canManage, onEdit, onDelete, onTest]
  );

  const handleSelectionChange = (selection: GridRowSelectionModel) => {
    const id = (selection[0] as string) ?? null;
    const selected = data.find((item) => item.id === id) ?? null;
    onSelect?.(selected);
  };

  return (
    <div style={{ height: 520, width: '100%' }}>
      <DataGrid
        rows={data}
        columns={columns}
        loading={loading}
        rowSelectionModel={selectedId ? [selectedId] : []}
        onRowSelectionModelChange={handleSelectionChange}
        onRowClick={(params) => onSelect?.(params.row)}
        getRowId={(row) => row.id}
        disableRowSelectionOnClick={false}
      />
    </div>
  );
};

export default SystemConnectionTable;
