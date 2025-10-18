import { jsx as _jsx } from "react/jsx-runtime";
import { DataGrid } from '@mui/x-data-grid';
const columns = [
    { field: 'name', headerName: 'Name', flex: 1 },
    {
        field: 'processAreaName',
        headerName: 'Process Area',
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
const DataObjectTable = ({ data, loading }) => (_jsx("div", { style: { height: 520, width: '100%' }, children: _jsx(DataGrid, { rows: data, columns: columns, loading: loading, getRowId: (row) => row.id }) }));
export default DataObjectTable;
