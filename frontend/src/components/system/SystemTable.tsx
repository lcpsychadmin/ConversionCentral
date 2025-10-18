import {
  DataGrid,
  GridActionsCellItem,
  GridColDef,
  GridRowParams,
  GridRowSelectionModel
} from '@mui/x-data-grid';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import { useMemo } from 'react';

import { System } from '../../types/data';

interface SystemTableProps {
  data: System[];
  loading: boolean;
  selectedId?: string | null;
  canManage: boolean;
  onSelect?: (system: System | null) => void;
  onEdit?: (system: System) => void;
  onDelete?: (system: System) => void;
}

const buildColumns = (
  canManage: boolean,
  onEdit?: (system: System) => void,
  onDelete?: (system: System) => void
): GridColDef<System>[] => {
  const baseColumns: GridColDef<System>[] = [
    { field: 'name', headerName: 'Name', flex: 1 },
    { field: 'physicalName', headerName: 'Physical Name', flex: 1 },
    {
      field: 'systemType',
      headerName: 'Type',
      flex: 0.7,
      valueGetter: ({ row }) => row.systemType ?? '—'
    },
    { field: 'status', headerName: 'Status', flex: 0.6 }
  ];

  if (!canManage) return baseColumns;

  return [
    ...baseColumns,
    {
      field: 'actions',
      type: 'actions',
      headerName: 'Actions',
      getActions: (params: GridRowParams<System>) => [
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

const SystemTable = ({ data, loading, selectedId, canManage, onSelect, onEdit, onDelete }: SystemTableProps) => {
  const columns = useMemo(() => buildColumns(canManage, onEdit, onDelete), [canManage, onEdit, onDelete]);

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

export default SystemTable;
