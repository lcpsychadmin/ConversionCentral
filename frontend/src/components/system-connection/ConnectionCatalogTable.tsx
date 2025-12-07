import { useEffect, useMemo, useState } from 'react';
import { LoadingButton } from '@mui/lab';
import {
  Box,
  Button,
  Chip,
  CircularProgress,
  Stack,
  Typography,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Checkbox,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

import { ConnectionCatalogTable as CatalogTable } from '../../types/data';

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
  const [selectionModel, setSelectionModel] = useState<string[]>([]);

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

  // Group rows by schema
  const groupedBySchema = useMemo(() => {
    const groups: { [schema: string]: GridRow[] } = {};
    gridRows.forEach((row) => {
      if (!groups[row.schema]) {
        groups[row.schema] = [];
      }
      groups[row.schema].push(row);
    });
    // Sort schemas and tables alphabetically
    return Object.keys(groups)
      .sort()
      .reduce((acc, schema) => {
        acc[schema] = groups[schema].sort((a, b) => (a.table ?? '').localeCompare(b.table ?? ''));
        return acc;
      }, {} as { [schema: string]: GridRow[] });
  }, [gridRows]);

  const initialSelection = useMemo(() => {
    const unique = new Set<string>();
    gridRows.forEach((row) => {
      if (row.selected) {
        unique.add(row.id);
      }
    });
    return Array.from(unique);
  }, [gridRows]);

  useEffect(() => {
    setSelectionModel(initialSelection);
  }, [initialSelection]);

  const hasPendingChanges = useMemo(() => {
    if (initialSelection.length !== selectionModel.length) {
      return true;
    }
    const currentSet = new Set(selectionModel);
    return initialSelection.some((id) => !currentSet.has(id));
  }, [initialSelection, selectionModel]);

  const handleRowCheckChange = (rowId: string, checked: boolean) => {
    setSelectionModel((prev) => {
      if (checked) {
        if (prev.includes(rowId)) {
          return prev;
        }
        return [...prev, rowId];
      }
      return prev.filter((id) => id !== rowId);
    });
  };

  const handleSchemaCheckChange = (schema: string, checked: boolean) => {
    const schemaRows = groupedBySchema[schema] ?? [];
    const schemaRowIds = schemaRows.map((row: GridRow) => row.id);
    setSelectionModel((prev) => {
      if (checked) {
        const next = new Set(prev);
        schemaRowIds.forEach((id) => next.add(id));
        return Array.from(next);
      }
      return prev.filter((id) => !schemaRowIds.includes(id));
    });
  };

  // Persist selection only after the user explicitly clicks Save.
  const handleSaveSelection = () => {
    if (!onSelectionChange || !hasPendingChanges || loading || saving) {
      return;
    }
    const normalized = [...selectionModel].sort();
    onSelectionChange(normalized);
  };

  const isSchemaSelected = (schema: string): boolean => {
    const schemaRows = groupedBySchema[schema] ?? [];
    return schemaRows.every((row) => selectionModel.includes(row.id));
  };

  const isSchemaIndeterminate = (schema: string): boolean => {
    const schemaRows = groupedBySchema[schema] ?? [];
    const selectedCount = schemaRows.filter((row) => selectionModel.includes(row.id)).length;
    return selectedCount > 0 && selectedCount < schemaRows.length;
  };

  const getSchemaSelectedCount = (schema: string): number => {
    const schemaRows = groupedBySchema[schema] ?? [];
    return schemaRows.filter((row) => selectionModel.includes(row.id)).length;
  };

  return (
    <Stack spacing={2} sx={{ position: 'relative' }}>
      <Stack direction="row" justifyContent="space-between" alignItems="center">
        <Typography variant="h6" sx={{ fontWeight: 700, letterSpacing: 0.3 }}>
          Tables
        </Typography>
        <Box display="flex" alignItems="center" gap={1.5}>
          <LoadingButton
            variant="contained"
            onClick={handleSaveSelection}
            disabled={!hasPendingChanges || loading || saving}
            loading={saving}
          >
            Save Selection
          </LoadingButton>
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

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
          <CircularProgress />
        </Box>
      ) : (
        <Box sx={{ maxHeight: 600, overflow: 'auto', border: `1px solid ${theme.palette.divider}`, borderRadius: 1 }}>
          {Object.entries(groupedBySchema).map(([schema, tables]) => (
            <Accordion key={schema} defaultExpanded={false}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                    <Checkbox
                      checked={isSchemaSelected(schema)}
                      indeterminate={isSchemaIndeterminate(schema)}
                      onChange={(e) => handleSchemaCheckChange(schema, e.target.checked)}
                      onClick={(e) => e.stopPropagation()}
                      sx={{ mr: 1 }}
                      disabled={saving || loading}
                    />
                <Typography sx={{ fontWeight: 600, flex: 1 }}>
                  {schema} ({getSchemaSelectedCount(schema)}/{tables.length})
                </Typography>
              </AccordionSummary>
              <AccordionDetails sx={{ p: 0 }}>
                <TableContainer component={Paper} elevation={0}>
                  <Table size="small">
                    <TableHead>
                      <TableRow sx={{ backgroundColor: alpha(theme.palette.primary.main, 0.05) }}>
                        <TableCell padding="checkbox" sx={{ width: 44 }} />
                        <TableCell sx={{ fontWeight: 600 }}>Table</TableCell>
                        <TableCell sx={{ fontWeight: 600, width: 120 }}>Type</TableCell>
                        <TableCell align="center" sx={{ fontWeight: 600, width: 100 }}>
                          Columns
                        </TableCell>
                        <TableCell align="right" sx={{ fontWeight: 600, width: 130 }}>
                          Rows (est.)
                        </TableCell>
                        <TableCell align="center" sx={{ fontWeight: 600, width: 110 }}>
                          Status
                        </TableCell>
                        {onPreview && <TableCell align="center" sx={{ fontWeight: 600, width: 100 }}>
                          Preview
                        </TableCell>}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {tables.map((row) => (
                        <TableRow
                          key={row.id}
                          sx={{
                            backgroundColor: !row.available ? alpha(theme.palette.warning.light, 0.08) : undefined,
                            '&:hover': {
                              backgroundColor: alpha(theme.palette.action.hover, 0.5)
                            }
                          }}
                        >
                          <TableCell padding="checkbox">
                            <Checkbox
                              checked={selectionModel.includes(row.id)}
                              onChange={(e) => handleRowCheckChange(row.id, e.target.checked)}
                              disabled={saving || loading}
                            />
                          </TableCell>
                          <TableCell sx={{ color: !row.available ? theme.palette.text.disabled : undefined }}>
                            {row.table}
                          </TableCell>
                          <TableCell sx={{ color: !row.available ? theme.palette.text.disabled : undefined }}>
                            {row.type?.replace('_', ' ') ?? '—'}
                          </TableCell>
                          <TableCell align="center" sx={{ color: !row.available ? theme.palette.text.disabled : undefined }}>
                            {row.columnCount ?? '—'}
                          </TableCell>
                          <TableCell align="right" sx={{ color: !row.available ? theme.palette.text.disabled : undefined }}>
                            {formatNumber(row.estimatedRows)}
                          </TableCell>
                          <TableCell align="center">
                            <Chip
                              label={row.available ? 'Available' : 'Missing'}
                              color={row.available ? 'success' : 'warning'}
                              size="small"
                              variant={row.available ? 'outlined' : 'filled'}
                            />
                          </TableCell>
                          {onPreview && <TableCell align="center">
                            <Button
                              variant="text"
                              size="small"
                              onClick={() => onPreview?.(row.original)}
                              disabled={!row.available || saving}
                            >
                              Preview
                            </Button>
                          </TableCell>}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </AccordionDetails>
            </Accordion>
          ))}
        </Box>
      )}
    </Stack>
  );
};

export default ConnectionCatalogTable;
