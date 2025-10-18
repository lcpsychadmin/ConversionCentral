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

import { Release } from '../../types/data';

export type ReleaseRow = Release & { projectName?: string | null };

interface ReleaseTableProps {
  data: ReleaseRow[];
  loading: boolean;
  selectedId?: string | null;
  canManage: boolean;
  onSelect?: (release: ReleaseRow | null) => void;
  onEdit?: (release: ReleaseRow) => void;
  onDelete?: (release: ReleaseRow) => void;
}

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const buildColumns = (
  canManage: boolean,
  onEdit?: (release: ReleaseRow) => void,
  onDelete?: (release: ReleaseRow) => void
): GridColDef<ReleaseRow>[] => {
  const baseColumns: GridColDef<ReleaseRow>[] = [
    { field: 'name', headerName: 'Name', flex: 1 },
    {
      field: 'projectName',
      headerName: 'Project',
      flex: 1,
      valueGetter: ({ row }) => row.projectName ?? ''
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
      getActions: (params: GridRowParams<ReleaseRow>) => [
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

const ReleaseTable = ({
  data,
  loading,
  selectedId,
  canManage,
  onSelect,
  onEdit,
  onDelete
}: ReleaseTableProps) => {
  const columns = useMemo(() => buildColumns(canManage, onEdit, onDelete), [canManage, onEdit, onDelete]);

  const handleSelectionChange = (selection: GridRowSelectionModel) => {
    const id = (selection[0] as string) ?? null;
    const selectedRelease = data.find((item) => item.id === id) ?? null;
    onSelect?.(selectedRelease);
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

export default ReleaseTable;
