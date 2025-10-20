import { useEffect, useMemo, useState } from 'react';
import {
  Box,
  Button,
  Chip,
  CircularProgress,
  Stack,
  Typography,
  Alert
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import {
  DataGrid,
  GridColDef,
  GridRenderCellParams,
  GridRowClassNameParams,
  GridRowSelectionModel
} from '@mui/x-data-grid';

import { ConnectionCatalogTable as CatalogTable } from '../../types/data';
import { getDataGridStyles } from '../../utils/tableStyles';

export interface ConnectionCatalogTableProps {
  rows: CatalogTable[];
  loading?: boolean;
  saving?: boolean;
  error?: string | null;
  onRefresh?: () => void;
  onSelectionChange?: (selectedIds: string[]) => void;
  onPreview?: (row: CatalogTable) => void;
}

const buildRowId = (row: CatalogTable) => `${row.schemaName}.${row.tableName}`;

const formatNumber = (value: number | null | undefined): string => {
  if (value === null || value === undefined) {
    return '—';
  }
  return new Intl.NumberFormat().format(value);
};

interface GridRow {
  id: string;
  schema: string;
  table: string;
  type?: string | null;
  columnCount?: number | null;
  estimatedRows?: number | null;
  available: boolean;
  selected: boolean;
  original: CatalogTable;
}

const ConnectionCatalogTable = ({
  rows,
  loading = false,
  saving = false,
  error,
  onRefresh,
  onSelectionChange,
  onPreview
}: ConnectionCatalogTableProps) => {
  const theme = useTheme();
  const [selectionModel, setSelectionModel] = useState<GridRowSelectionModel>([]);

  const gridRows = useMemo<GridRow[]>(
    () =>
      rows.map((row) => ({
        id: buildRowId(row),
        schema: row.schemaName,
        table: row.tableName,
        type: row.tableType,
        columnCount: row.columnCount,
        estimatedRows: row.estimatedRows,
        available: row.available,
        selected: row.selected,
        original: row
      })),
    [rows]
  );

  useEffect(() => {
    setSelectionModel(gridRows.filter((row) => row.selected).map((row) => row.id));
  }, [gridRows]);

  const columns = useMemo<GridColDef<GridRow>[]>(() => {
    const base: GridColDef<GridRow>[] = [
      { field: 'schema', headerName: 'Schema', flex: 0.6, minWidth: 140 },
      { field: 'table', headerName: 'Table', flex: 1, minWidth: 200 },
      {
        field: 'type',
        headerName: 'Type',
        width: 140,
        valueFormatter: (params) => params.value?.toString().replace('_', ' ') ?? '—'
      },
      {
        field: 'columnCount',
        headerName: 'Columns',
        width: 120,
        align: 'center',
        headerAlign: 'center',
        valueFormatter: (params) => (params.value ?? '—') as string
      },
      {
        field: 'estimatedRows',
        headerName: 'Rows (est.)',
        width: 150,
        valueFormatter: (params) => formatNumber(params.value as number | undefined)
      },
      {
        field: 'status',
        headerName: 'Status',
        width: 150,
        sortable: false,
        filterable: false,
        disableColumnMenu: true,
        renderCell: (params: GridRenderCellParams<GridRow>) => (
          <Chip
            label={params.row.available ? 'Available' : 'Missing'}
            color={params.row.available ? 'success' : 'warning'}
            size="small"
            variant={params.row.available ? 'outlined' : 'filled'}
          />
        )
      }
    ];

    if (onPreview) {
      base.push({
        field: 'preview',
        headerName: 'Preview',
        width: 140,
        sortable: false,
        filterable: false,
        disableColumnMenu: true,
        renderCell: (params: GridRenderCellParams<GridRow>) => (
          <Button
            variant="text"
            size="small"
            onClick={() => onPreview?.(params.row.original)}
            disabled={!params.row.available || saving}
          >
            Preview
          </Button>
        )
      });
    }

    return base;
  }, [onPreview, saving]);

  const handleSelectionChange = (model: GridRowSelectionModel) => {
    if (saving) {
      setSelectionModel(gridRows.filter((row) => row.selected).map((row) => row.id));
      return;
    }
    setSelectionModel(model);
    onSelectionChange?.(model.map((id) => id.toString()));
  };

  const getRowClassName = (params: GridRowClassNameParams) =>
    params.row.available ? '' : 'catalog-row-missing';

  return (
    <Stack spacing={2} sx={{ position: 'relative' }}>
      <Stack direction="row" justifyContent="space-between" alignItems="center">
        <Typography variant="h6" sx={{ fontWeight: 700, letterSpacing: 0.3 }}>
          Source Catalog
        </Typography>
        <Box display="flex" alignItems="center" gap={1.5}>
          {saving && <CircularProgress size={22} thickness={5} />}
          <Button
            variant="outlined"
            onClick={onRefresh}
            disabled={loading || saving}
          >
            Refresh Catalog
          </Button>
        </Box>
      </Stack>

      {error && (
        <Alert severity="error" sx={{ mb: 1 }}>
          {error}
        </Alert>
      )}

      <Box sx={{ height: 520, width: '100%' }}>
        <DataGrid<GridRow>
          rows={gridRows}
          columns={columns}
          loading={loading}
          checkboxSelection
          disableColumnSelector
          disableDensitySelector
          disableRowSelectionOnClick
          hideFooterSelectedRowCount
          rowSelectionModel={selectionModel}
          onRowSelectionModelChange={handleSelectionChange}
          getRowClassName={getRowClassName}
          sx={{
            ...getDataGridStyles(theme),
            '& .catalog-row-missing .MuiDataGrid-cell': {
              color: theme.palette.text.disabled,
              fontStyle: 'italic',
              backgroundColor: alpha(theme.palette.warning.light, 0.08)
            }
          }}
        />
      </Box>
    </Stack>
  );
};

export default ConnectionCatalogTable;
