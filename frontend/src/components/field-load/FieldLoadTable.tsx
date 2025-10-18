import { DataGrid, GridColDef, GridValueFormatterParams } from '@mui/x-data-grid';

import { FieldLoad } from '../../types/data';

interface FieldLoadTableProps {
  data: FieldLoad[];
  loading: boolean;
}

const columns: GridColDef<FieldLoad>[] = [
  { field: 'fieldName', headerName: 'Field', flex: 1 },
  { field: 'tableName', headerName: 'Table', flex: 1 },
  { field: 'releaseName', headerName: 'Release', flex: 1 },
  {
    field: 'loadFlag',
    headerName: 'Load?',
    flex: 0.5,
    valueFormatter: ({ value }: GridValueFormatterParams<boolean>) => (value ? 'Yes' : 'No')
  }
];

const FieldLoadTable = ({ data, loading }: FieldLoadTableProps) => (
  <div style={{ height: 520, width: '100%' }}>
    <DataGrid
      rows={data}
      columns={columns}
      loading={loading}
      getRowId={(row: FieldLoad) => row.id}
      disableRowSelectionOnClick
    />
  </div>
);

export default FieldLoadTable;
