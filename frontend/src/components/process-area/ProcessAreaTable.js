import { jsx as _jsx } from "react/jsx-runtime";
import { DataGrid, GridActionsCellItem } from '@mui/x-data-grid';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import { useMemo } from 'react';
import { useTheme } from '@mui/material/styles';
import { getDataGridStyles } from '../../utils/tableStyles';
const formatStatusLabel = (status) => status
    .split('_')
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
const columns = (canManage, onEdit, onDelete) => {
    const baseColumns = [
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
    if (!canManage)
        return baseColumns;
    return [
        ...baseColumns,
        {
            field: 'actions',
            type: 'actions',
            headerName: 'Actions',
            getActions: (params) => [
                _jsx(GridActionsCellItem, { icon: _jsx(EditIcon, { fontSize: "small" }), label: "Edit", onClick: () => onEdit?.(params.row), showInMenu: true }, "edit"),
                _jsx(GridActionsCellItem, { icon: _jsx(DeleteIcon, { fontSize: "small" }), label: "Delete", onClick: () => onDelete?.(params.row), showInMenu: true }, "delete")
            ]
        }
    ];
};
const ProcessAreaTable = ({ data, loading, selectedId, canManage, onSelect, onEdit, onDelete }) => {
    const theme = useTheme();
    const columnConfig = useMemo(() => columns(canManage, onEdit, onDelete), [canManage, onEdit, onDelete]);
    const handleSelectionChange = (selection) => {
        const id = selection[0] ?? null;
        const selectedProcessArea = data.find((item) => item.id === id) ?? null;
        onSelect?.(selectedProcessArea);
    };
    return (_jsx("div", { style: { height: 520, width: '100%' }, children: _jsx(DataGrid, { rows: data, columns: columnConfig, loading: loading, rowSelectionModel: selectedId ? [selectedId] : [], onRowSelectionModelChange: handleSelectionChange, onRowClick: (params) => onSelect?.(params.row), getRowId: (row) => row.id, disableRowSelectionOnClick: false, sx: getDataGridStyles(theme) }) }));
};
export default ProcessAreaTable;
