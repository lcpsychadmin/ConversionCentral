import { jsx as _jsx } from "react/jsx-runtime";
import { DataGrid } from '@mui/x-data-grid';
const columns = [
    { field: 'fieldName', headerName: 'Field', flex: 1 },
    { field: 'tableName', headerName: 'Table', flex: 1 },
    { field: 'releaseName', headerName: 'Release', flex: 1 },
    {
        field: 'loadFlag',
        headerName: 'Load?',
        flex: 0.5,
        valueFormatter: ({ value }) => (value ? 'Yes' : 'No')
    }
];
const FieldLoadTable = ({ data, loading }) => (_jsx("div", { style: { height: 520, width: '100%' }, children: _jsx(DataGrid, { rows: data, columns: columns, loading: loading, getRowId: (row) => row.id, disableRowSelectionOnClick: true }) }));
export default FieldLoadTable;
