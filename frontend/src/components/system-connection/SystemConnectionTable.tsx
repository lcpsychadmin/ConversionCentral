import { ReactElement, useMemo } from 'react';
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
import { Chip, Link, Tooltip } from '@mui/material';
import { useTheme } from '@mui/material/styles';

import { SystemConnection } from '../../types/data';
import { formatConnectionSummary, parseJdbcConnectionString } from '../../utils/connectionString';
import { getDataGridStyles } from '../../utils/tableStyles';

interface SystemConnectionTableProps {
  data: SystemConnection[];
  loading: boolean;
  selectedId?: string | null;
  canManage?: boolean;
  onSelect?: (connection: SystemConnection | null) => void;
  onViewDetail?: (connection: SystemConnection) => void;
  onEdit?: (connection: SystemConnection) => void;
  onDelete?: (connection: SystemConnection) => void;
  onTest?: (connection: SystemConnection) => void;
}

const DATABASE_LABELS: Record<string, string> = {
  postgresql: 'PostgreSQL',
  databricks: 'Databricks SQL Warehouse',
  sap: 'SAP HANA'
};

const buildColumns = (
  canManage: boolean,
  onViewDetail?: (connection: SystemConnection) => void,
  onEdit?: (connection: SystemConnection) => void,
  onDelete?: (connection: SystemConnection) => void,
  onTest?: (connection: SystemConnection) => void
): GridColDef<SystemConnection>[] => {
  const baseColumns: GridColDef<SystemConnection>[] = [
    {
      field: 'name',
      headerName: 'Connection',
      flex: 0.9,
      minWidth: 180,
      renderCell: ({ row }) => (
        <Link
          component="button"
          variant="body2"
          onClick={(event) => {
            event.preventDefault();
            onViewDetail?.(row);
          }}
          sx={{ cursor: 'pointer', fontWeight: 600 }}
        >
          {row.name}
        </Link>
      )
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
        const tooltipMessage = row.notes ?? summary;
        return (
          <Tooltip title={tooltipMessage} placement="top" enterDelay={600}>
            <Link
              component="button"
              variant="body2"
              onClick={(e) => {
                e.preventDefault();
                onViewDetail?.(row);
              }}
              sx={{ cursor: 'pointer', textAlign: 'left', overflow: 'hidden', textOverflow: 'ellipsis' }}
            >
              {summary}
            </Link>
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
      getActions: (params: GridRowParams<SystemConnection>) => {
  const items: ReactElement[] = [];

        if (onTest) {
          items.push(
            <GridActionsCellItem
              key="test"
              icon={<FlashOnIcon fontSize="small" />}
              label="Test"
              onClick={() => onTest?.(params.row)}
              showInMenu
            />
          );
        }

        items.push(
          <GridActionsCellItem
            key="edit"
            icon={<EditIcon fontSize="small" />}
            label="Edit"
            onClick={() => onEdit?.(params.row)}
            showInMenu
          />
        );

        items.push(
          <GridActionsCellItem
            key="delete"
            icon={<DeleteIcon fontSize="small" />}
            label="Delete"
            onClick={() => onDelete?.(params.row)}
            showInMenu
          />
        );

        return items;
      }
    }
  ];
};

const SystemConnectionTable = ({
  data,
  loading,
  selectedId,
  canManage = false,
  onSelect,
  onViewDetail,
  onEdit,
  onDelete,
  onTest
}: SystemConnectionTableProps) => {
  const theme = useTheme();

  const columns = useMemo(
    () => buildColumns(canManage, onViewDetail, onEdit, onDelete, onTest),
    [canManage, onViewDetail, onEdit, onDelete, onTest]
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
        sx={getDataGridStyles(theme)}
      />
    </div>
  );
};

export default SystemConnectionTable;
