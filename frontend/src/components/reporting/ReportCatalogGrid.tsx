import { Box, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { useCallback, useMemo, useEffect } from 'react';
import { DataGrid, type GridColDef, type GridRowModel } from '@mui/x-data-grid';

interface ReportCatalogGridProps {
  columns: string[];
  rows: Record<string, unknown>[];
  loading?: boolean;
  height?: number | string;
  emptyMessage?: string;
}

const ReportCatalogGrid = ({
  columns,
  rows,
  loading = false,
  height = 520,
  emptyMessage = 'No published rows available yet.'
}: ReportCatalogGridProps) => {
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';

  const fieldKeys = useMemo(() => {
    const seen = new Map<string, number>();

    const normalize = (label: string | undefined | null, index: number) => {
      const baseRaw = (label ?? '').trim();
      if (!baseRaw) {
        return `column_${index + 1}`;
      }

      const snake = baseRaw
        .toLowerCase()
        .replace(/[^0-9a-z]+/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_|_$/g, '');

      const candidate = snake || `column_${index + 1}`;
      const usage = seen.get(candidate) ?? 0;
      seen.set(candidate, usage + 1);
      return usage === 0 ? candidate : `${candidate}_${usage}`;
    };

    return columns.map((label, index) => normalize(label, index));
  }, [columns]);

  const extractCellValue = useCallback(
    (
      model: Record<string, unknown>,
      columnName: string,
      columnIndex: number,
      preferredKey: string
    ) => {
      if (!model) {
        return undefined;
      }

      if (preferredKey && Object.prototype.hasOwnProperty.call(model, preferredKey)) {
        return model[preferredKey];
      }

      // Also check camel-cased variant of the preferred key since the API response
      // may be processed by the client transformer which converts snake_case to camelCase.
      const toCamel = (s: string) => s.replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
      if (preferredKey) {
        const prefCamel = toCamel(preferredKey);
        if (Object.prototype.hasOwnProperty.call(model, prefCamel)) {
          return model[prefCamel];
        }
      }

      if (Object.prototype.hasOwnProperty.call(model, columnName)) {
        return model[columnName];
      }

      const trimmed = columnName.trim();
      if (trimmed && Object.prototype.hasOwnProperty.call(model, trimmed)) {
        return model[trimmed];
      }

      if (trimmed) {
        const normalized = trimmed.toLowerCase();
        if (Object.prototype.hasOwnProperty.call(model, normalized)) {
          return model[normalized];
        }

        const snakeCase = trimmed.replace(/\s+/g, '_');
        if (Object.prototype.hasOwnProperty.call(model, snakeCase)) {
          return model[snakeCase];
        }

        const snakeCamel = snakeCase.toLowerCase().replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
        if (Object.prototype.hasOwnProperty.call(model, snakeCamel)) {
          return model[snakeCamel];
        }

        const lowerSnake = snakeCase.toLowerCase();
        if (Object.prototype.hasOwnProperty.call(model, lowerSnake)) {
          return model[lowerSnake];
        }

        const sanitized = trimmed.replace(/[^0-9a-z]+/gi, '_').replace(/_+/g, '_');
        if (Object.prototype.hasOwnProperty.call(model, sanitized)) {
          return model[sanitized];
        }

        const sanitizedCamel = sanitized.replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
        if (Object.prototype.hasOwnProperty.call(model, sanitizedCamel)) {
          return model[sanitizedCamel];
        }

        const lowerSanitized = sanitized.toLowerCase();
        if (Object.prototype.hasOwnProperty.call(model, lowerSanitized)) {
          return model[lowerSanitized];
        }

        const compact = trimmed.replace(/\s+/g, '').toLowerCase();
        if (Object.prototype.hasOwnProperty.call(model, compact)) {
          return model[compact];
        }
      }

      const numericKey = String(columnIndex);
      if (Object.prototype.hasOwnProperty.call(model, numericKey)) {
        return model[numericKey];
      }

      return undefined;
    },
    []
  );

  const dataGridRows = useMemo(() => {
    if (rows.length === 0 || columns.length === 0) {
      return [] as GridRowModel[];
    }

    return rows.map((rowModel, rowIndex) => {
      const result: GridRowModel = { id: rowIndex };
      columns.forEach((columnName, columnIndex) => {
        const fieldKey = fieldKeys[columnIndex];
  const value = extractCellValue(rowModel, columnName, columnIndex, fieldKey);
        result[fieldKey] = value === undefined || value === null ? '' : value;
      });
      return result;
    });
  }, [columns, extractCellValue, fieldKeys, rows]);

  // Temporary debug: log columns + first few rows so we can see how keys are mapped
  useEffect(() => {
    try {
      // Limit printing to a few items to reduce noise
      const sampleRows = rows.slice(0, 5);
      console.debug('[ReportCatalogGrid] columns:', columns);
      console.debug('[ReportCatalogGrid] first dataset rows:', sampleRows);
      console.debug('[ReportCatalogGrid] dataGridRows:', dataGridRows.slice(0, 5));
    } catch (err) {
      // best-effort; avoid breaking the page
      console.debug('[ReportCatalogGrid] debug log error', err);
    }
  }, [columns, rows, dataGridRows]);

  const modalHeaderColor = useMemo(
    () => theme.palette.primary.dark ?? theme.palette.primary.main,
    [theme]
  );
  const modalHeaderTextColor = useMemo(
    () => theme.palette.getContrastText(modalHeaderColor),
    [modalHeaderColor, theme]
  );
  const headerGradient = useMemo(
    () =>
      `linear-gradient(180deg, ${alpha(modalHeaderColor, isDarkMode ? 0.96 : 0.98)} 0%, ${modalHeaderColor} 100%)`,
    [isDarkMode, modalHeaderColor]
  );
  const headerBorderColor = useMemo(
    () => alpha(modalHeaderColor, isDarkMode ? 0.7 : 0.6),
    [isDarkMode, modalHeaderColor]
  );
  const cellBackground = useMemo(() => {
    const paper = theme.palette.background.paper;
    return isDarkMode ? alpha(paper, 0.34) : alpha(theme.palette.common.white, 0.96);
  }, [isDarkMode, theme]);
  const zebraBackground = useMemo(() => {
    const tone = theme.palette.primary.light ?? theme.palette.primary.main;
    // Lower the alpha to soften the accent on alternating rows.
    // Keep dark mode slightly stronger for contrast, but much less than before.
    return isDarkMode ? alpha(tone, 0.24) : alpha(tone, 0.06);
  }, [isDarkMode, theme]);
  const cellBorderColor = useMemo(
    () => alpha(theme.palette.divider, isDarkMode ? 0.5 : 0.22),
    [isDarkMode, theme]
  );
  const secondaryCellBorderColor = useMemo(
    () => alpha(theme.palette.divider, isDarkMode ? 0.32 : 0.15),
    [isDarkMode, theme]
  );
  const rowHoverBackground = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.28 : 0.12),
    [isDarkMode, theme]
  );
  const rowSelectionBackground = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.42 : 0.18),
    [isDarkMode, theme]
  );
  const rowHoverOutline = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.5 : 0.22),
    [isDarkMode, theme]
  );
  const rowSelectionOutline = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.62 : 0.32),
    [isDarkMode, theme]
  );
  const focusRing = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.72 : 0.4),
    [isDarkMode, theme]
  );
  const gridBackground = useMemo(() => {
    if (isDarkMode) {
      const topShade = alpha(theme.palette.primary.main, 0.2);
      const bottomShade = alpha(theme.palette.background.paper, 0.12);
      return `linear-gradient(180deg, ${topShade} 0%, ${bottomShade} 100%)`;
    }
    return alpha(theme.palette.common.white, 0.98);
  }, [isDarkMode, theme]);
  const gridBorderColor = useMemo(
    () =>
      alpha(
        isDarkMode
          ? theme.palette.primary.dark ?? theme.palette.divider
          : theme.palette.primary.light ?? theme.palette.divider,
        isDarkMode ? 0.32 : 0.2
      ),
    [isDarkMode, theme]
  );
  const gridShadow = useMemo(
    () =>
      isDarkMode
        ? theme.shadows[8]
        : `0 10px 28px ${alpha(theme.palette.common.black, 0.1)}`,
    [isDarkMode, theme]
  );

  const debugMode = typeof window !== 'undefined' && new URLSearchParams(window.location.search).has('debug_report');

  const dataGridColumns = useMemo<GridColDef[]>(
    () =>
      columns.map((columnLabel, index) => {
        const field = fieldKeys[index];
        const estimatedWidth = columnLabel ? columnLabel.length * 14 : 160;
        const minWidth = Math.min(Math.max(estimatedWidth, 140), 360);

        return {
          field,
          headerName: columnLabel || `Column ${index + 1}`,
          headerAlign: 'left',
          align: 'left',
          sortable: true,
          disableColumnMenu: true,
          flex: 1,
          minWidth,
          renderCell: (params) => {
            const rawValue = params.value;
              const displayValue = rawValue === null || rawValue === undefined ? '' : String(rawValue);
              const rawType = rawValue === null ? 'null' : typeof rawValue;
            const textColor = isDarkMode
              ? alpha(theme.palette.common.white, 0.94)
              : theme.palette.text.primary;

            return (
              <Typography
                component="span"
                data-value={displayValue}
                title={displayValue}
                sx={{
                  display: 'block',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                  width: '100%',
                  minWidth: 0,
                  color: textColor,
                  fontSize: theme.typography.pxToRem(16),
                  lineHeight: 1.6,
                  fontWeight: 400
                }}
              >
                {displayValue || '\u00A0'}
                {debugMode && (
                  <Typography component="span" sx={{ fontSize: theme.typography.pxToRem(11), color: alpha(theme.palette.text.secondary, 0.9), marginLeft: 0.5 }}>
                    {`[${rawType}]`}
                  </Typography>
                )}
              </Typography>
            );
          }
        } satisfies GridColDef;
      }),
    [columns, debugMode, fieldKeys, isDarkMode, theme]
  );

  const debugRows = debugMode ? dataGridRows.slice(0, 5) : [];

  const NoRowsOverlay = useMemo(
    () =>
      function NoRowsOverlayComponent() {
        return (
          <Box
            sx={{
              height: '100%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              px: 3,
              textAlign: 'center'
            }}
          >
            <Typography variant="body2" color="text.secondary">
              {emptyMessage}
            </Typography>
          </Box>
        );
      },
    [emptyMessage]
  );

  return (
    <Box
      sx={{
        position: 'relative',
        height,
        borderRadius: 3,
        border: `1px solid ${gridBorderColor}`,
        boxShadow: gridShadow,
        overflow: 'hidden',
        background: gridBackground
      }}
    >
      <DataGrid
        rows={dataGridRows}
        columns={dataGridColumns}
        loading={loading}
        disableRowSelectionOnClick
        disableColumnMenu
        disableDensitySelector
        disableColumnFilter
        hideFooter
        sortingOrder={['asc', 'desc']}
        rowHeight={48}
        columnHeaderHeight={52}
        sx={{
          border: 'none',
          fontFamily: theme.typography.fontFamily,
          '& .MuiDataGrid-columnHeaders': {
            backgroundColor: modalHeaderColor,
            backgroundImage: headerGradient,
            color: modalHeaderTextColor,
            borderBottom: `1px solid ${headerBorderColor}`,
            boxShadow: `inset 0 -1px 0 ${alpha(headerBorderColor, 0.75)}`,
            letterSpacing: 0.3,
            textTransform: 'uppercase',
            fontWeight: 600
          },
          '& .MuiDataGrid-columnHeaderTitle': {
            fontWeight: 700,
            fontSize: theme.typography.pxToRem(13)
          },
          '& .MuiDataGrid-columnHeaders .MuiSvgIcon-root': {
            color: alpha(modalHeaderTextColor, 0.88)
          },
          '& .MuiDataGrid-columnSeparator': {
            color: headerBorderColor
          },
          '& .MuiDataGrid-cell': {
            display: 'flex',
            alignItems: 'center',
            borderBottom: `1px solid ${cellBorderColor}`,
            borderRight: `1px solid ${secondaryCellBorderColor}`,
            backgroundColor: cellBackground,
            color: isDarkMode
              ? alpha(theme.palette.common.white, 0.94)
              : theme.palette.text.primary,
            padding: theme.spacing(0.75, 1.25),
            fontSize: theme.typography.pxToRem(16),
            lineHeight: 1.6,
            transition: theme.transitions.create(['background-color', 'box-shadow'], {
              duration: theme.transitions.duration.shortest
            })
          },
          '& .MuiDataGrid-row': {
            borderLeft: 'none',
            borderRight: 'none'
          },
          '& .MuiDataGrid-row:nth-of-type(even) .MuiDataGrid-cell': {
            backgroundColor: zebraBackground
          },
          '& .MuiDataGrid-row:hover .MuiDataGrid-cell': {
            backgroundColor: rowHoverBackground,
            boxShadow: `inset 0 0 0 1px ${rowHoverOutline}`
          },
          '& .MuiDataGrid-row.Mui-selected .MuiDataGrid-cell': {
            backgroundColor: rowSelectionBackground,
            boxShadow: `inset 0 0 0 2px ${rowSelectionOutline}`
          },
          '& .MuiDataGrid-cell:focus, & .MuiDataGrid-cell:focus-within': {
            outline: `2px solid ${focusRing}`,
            outlineOffset: -2
          },
          '& .MuiDataGrid-virtualScroller': {
            background: 'transparent'
          },
          '& .MuiDataGrid-overlay': {
            background: 'transparent'
          }
        }}
        slots={{ noRowsOverlay: NoRowsOverlay }}
      />

      {debugMode && (
        <Box sx={{ p: 1, borderTop: `1px dashed ${alpha(theme.palette.divider, 0.6)}`, background: alpha(theme.palette.background.paper, 0.02) }}>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontWeight: 600, mb: 1 }}>
            Debug: first dataset rows (keys and values)
          </Typography>
          {debugRows.length === 0 ? (
            <Typography variant="caption" color="text.secondary">(no rows)</Typography>
          ) : (
            debugRows.map((r) => (
              <pre key={`debug-${r.id}`} style={{ margin: 0, fontSize: '12px', whiteSpace: 'pre-wrap' }}>
                {JSON.stringify(r, null, 2)}
              </pre>
            ))
          )}
        </Box>
      )}
    </Box>
  );
};

export default ReportCatalogGrid;
