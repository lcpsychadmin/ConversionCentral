import { DataGrid, GridColDef } from '@mui/x-data-grid';
import { DataObject } from '../../types/data';

interface DataObjectTableProps {
  data: DataObject[];
  loading: boolean;
}

const columns: GridColDef<DataObject>[] = [
  { field: 'name', headerName: 'Name', flex: 1 },
  {
    field: 'processAreaName',
    headerName: 'Product Team',
    flex: 1,
    valueGetter: ({ row }) => row.processAreaName ?? 'Unassigned'
  },
  { field: 'status', headerName: 'Status', flex: 0.6 },
  {
    field: 'description',
    headerName: 'Description',
    flex: 1.2,
    valueGetter: ({ row }) => row.description ?? ''
  }
];

const DataObjectTable = ({ data, loading }: DataObjectTableProps) => (
  <div style={{ height: 520, width: '100%' }}>
    <DataGrid rows={data} columns={columns} loading={loading} getRowId={(row: DataObject) => row.id} />
  </div>
);

export default DataObjectTable;
