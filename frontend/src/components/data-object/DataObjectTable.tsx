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

import { DataObject } from '../../types/data';
import { getDataGridStyles } from '../../utils/tableStyles';

interface DataObjectTableProps {
  data: DataObjectRow[];
  loading: boolean;
  selectedId?: string | null;
  canManage: boolean;
  onSelect?: (dataObject: DataObjectRow | null) => void;
  onEdit?: (dataObject: DataObjectRow) => void;
  onDelete?: (dataObject: DataObjectRow) => void;
}

export type DataObjectRow = DataObject & {
  processAreaName?: string | null;
  systemNames: string[];
};

const buildColumns = (
  canManage: boolean,
  onEdit?: (dataObject: DataObjectRow) => void,
  onDelete?: (dataObject: DataObjectRow) => void
): GridColDef<DataObjectRow>[] => {
  const baseColumns: GridColDef<DataObjectRow>[] = [
    { field: 'name', headerName: 'Name', flex: 1 },
    {
      field: 'description',
      headerName: 'Description',
      flex: 1.2,
      valueGetter: ({ row }) => row.description ?? ''
    },
    {
      field: 'processAreaName',
      headerName: 'Process Area',
      flex: 1,
      valueGetter: ({ row }) => row.processAreaName ?? 'Unassigned'
    },
    {
      field: 'systems',
      headerName: 'Systems',
      flex: 1,
      valueGetter: ({ row }) => (row.systemNames.length > 0 ? row.systemNames.join(', ') : '—')
    },
    { field: 'status', headerName: 'Status', flex: 0.7 }
  ];

  if (!canManage) return baseColumns;

  return [
    ...baseColumns,
    {
      field: 'actions',
      type: 'actions',
      headerName: 'Actions',
      getActions: (params: GridRowParams<DataObjectRow>) => [
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

const DataObjectTable = ({
  data,
  loading,
  selectedId,
  canManage,
  onSelect,
  onEdit,
  onDelete
}: DataObjectTableProps) => {
  const columns = useMemo(() => buildColumns(canManage, onEdit, onDelete), [canManage, onEdit, onDelete]);
  const theme = useTheme();

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

export default DataObjectTable;
