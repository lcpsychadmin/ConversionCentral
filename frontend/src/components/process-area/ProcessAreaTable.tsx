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
import { useTheme } from '@mui/material/styles';

import { ProcessArea } from '../../types/data';
import { getDataGridStyles } from '../../utils/tableStyles';

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

interface ProcessAreaTableProps {
  data: ProcessAreaRow[];
  loading: boolean;
  selectedId?: string | null;
  canManage: boolean;
  onSelect?: (processArea: ProcessAreaRow | null) => void;
  onEdit?: (processArea: ProcessAreaRow) => void;
  onDelete?: (processArea: ProcessAreaRow) => void;
}

export type ProcessAreaRow = ProcessArea;

const columns = (
  canManage: boolean,
  onEdit?: (processArea: ProcessAreaRow) => void,
  onDelete?: (processArea: ProcessAreaRow) => void
): GridColDef<ProcessAreaRow>[] => {
  const baseColumns: GridColDef<ProcessAreaRow>[] = [
    { field: 'name', headerName: 'Name', flex: 1 },
    {
      field: 'description',
      headerName: 'Description',
      flex: 1.2,
      valueGetter: ({ row }) => row.description ?? ''
    },
    {
      field: 'status',
      headerName: 'Status',
      flex: 0.6,
      valueGetter: ({ row }) => formatStatusLabel(row.status)
    }
  ];

  if (!canManage) return baseColumns;

  return [
    ...baseColumns,
    {
      field: 'actions',
      type: 'actions',
      headerName: 'Actions',
      getActions: (params: GridRowParams<ProcessAreaRow>) => [
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

const ProcessAreaTable = ({
  data,
  loading,
  selectedId,
  canManage,
  onSelect,
  onEdit,
  onDelete
}: ProcessAreaTableProps) => {
  const theme = useTheme();
  const columnConfig = useMemo(() => columns(canManage, onEdit, onDelete), [canManage, onEdit, onDelete]);

  const handleSelectionChange = (selection: GridRowSelectionModel) => {
    const id = (selection[0] as string) ?? null;
    const selectedProcessArea = data.find((item) => item.id === id) ?? null;
    onSelect?.(selectedProcessArea);
  };

  return (
    <div style={{ height: 520, width: '100%' }}>
      <DataGrid
        rows={data}
        columns={columnConfig}
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

export default ProcessAreaTable;
