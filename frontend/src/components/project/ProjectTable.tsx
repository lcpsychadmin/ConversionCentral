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

import { Project } from '../../types/data';

export type ProjectRow = Project;

interface ProjectTableProps {
  data: ProjectRow[];
  loading: boolean;
  selectedId?: string | null;
  canManage: boolean;
  onSelect?: (project: ProjectRow | null) => void;
  onEdit?: (project: ProjectRow) => void;
  onDelete?: (project: ProjectRow) => void;
}

const formatStatusLabel = (status: string) =>
  status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const buildColumns = (
  canManage: boolean,
  onEdit?: (project: ProjectRow) => void,
  onDelete?: (project: ProjectRow) => void
): GridColDef<ProjectRow>[] => {
  const baseColumns: GridColDef<ProjectRow>[] = [
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
      getActions: (params: GridRowParams<ProjectRow>) => [
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

const ProjectTable = ({
  data,
  loading,
  selectedId,
  canManage,
  onSelect,
  onEdit,
  onDelete
}: ProjectTableProps) => {
  const columns = useMemo(() => buildColumns(canManage, onEdit, onDelete), [canManage, onEdit, onDelete]);

  const handleSelectionChange = (selection: GridRowSelectionModel) => {
    const id = (selection[0] as string) ?? null;
    const selectedProject = data.find((item) => item.id === id) ?? null;
    onSelect?.(selectedProject);
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

export default ProjectTable;
