import { ChangeEvent, DragEvent, KeyboardEvent, MouseEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControl,
  FormControlLabel,
  FormHelperText,
  IconButton,
  InputLabel,
  ListItemIcon,
  MenuItem,
  Menu,
  Paper,
  Select,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import FunctionsIcon from '@mui/icons-material/Functions';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import NumbersIcon from '@mui/icons-material/Numbers';
import TextFieldsIcon from '@mui/icons-material/TextFields';
import { useMutation, useQuery } from 'react-query';
import { isAxiosError } from 'axios';
import type { SelectChangeEvent } from '@mui/material/Select';
import PageHeader from '../components/common/PageHeader';

import { useAuth } from '../context/AuthContext';
import { useToast } from '../hooks/useToast';
import {
  DataWarehouseTarget,
  UploadDataColumnOverrideInput,
  UploadDataPreview,
  UploadTableMode
} from '../types/data';
import {
  createTableFromUpload,
  previewUploadData
} from '../services/uploadDataService';
import {
  fetchProcessAreas,
  fetchDataObjects,
  fetchSystems,
  ProcessArea,
  DataObject,
  System
} from '../services/constructedDataService';

const getErrorMessage = (error: unknown, fallback: string): string => {
  if (isAxiosError(error)) {
    const detail = error.response?.data as { detail?: unknown } | undefined;
    if (detail && typeof detail.detail === 'string') {
      return detail.detail;
    }
  }
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return fallback;
};

const sanitizeTableName = (value: string): string => {
  const replaced = value.replace(/[^0-9a-zA-Z_]/g, '_');
  const compacted = replaced.replace(/_+/g, '_');
  const trimmed = compacted.replace(/^_+|_+$/g, '');
  let candidate = trimmed || 'uploaded_table';
  if (/^\d/.test(candidate)) {
    candidate = `tbl_${candidate}`;
  }
  candidate = candidate.toLowerCase();
  if (candidate.length > 128) {
    candidate = candidate.slice(0, 128);
  }
  return candidate;
};

const TARGET_OPTIONS: Array<{ value: DataWarehouseTarget; label: string; disabled?: boolean }> = [
  { value: 'databricks_sql', label: 'Databricks SQL Warehouse' }
];

type UploadColumnType = 'string' | 'integer' | 'float' | 'timestamp' | 'boolean';

const COLUMN_TYPE_OPTIONS: Array<{ value: UploadColumnType; label: string }> = [
  { value: 'string', label: 'String' },
  { value: 'integer', label: 'Integer' },
  { value: 'float', label: 'Decimal' },
  { value: 'timestamp', label: 'Timestamp' },
  { value: 'boolean', label: 'Boolean' }
];

const normalizeColumnType = (value: string): UploadColumnType => {
  const normalized = value.toLowerCase() as UploadColumnType;
  return ['string', 'integer', 'float', 'timestamp', 'boolean'].includes(normalized)
    ? normalized
    : 'string';
};

const sanitizeColumnName = (value: string, fallback: string): string => {
  let candidate = value.replace(/[^0-9a-zA-Z_]/g, '_');
  candidate = candidate.replace(/_+/g, '_');
  candidate = candidate.replace(/^_+|_+$/g, '');
  if (!candidate) {
    candidate = fallback;
  }
  if (/^\d/.test(candidate)) {
    candidate = `${fallback}_${candidate}`;
  }
  candidate = candidate.toLowerCase();
  if (candidate.length > 128) {
    candidate = candidate.slice(0, 128);
  }
  return candidate;
};

interface UploadColumnConfig {
  sourceFieldName: string;
  originalName: string;
  inferredType: UploadColumnType;
  displayName: string;
  selectedType: UploadColumnType;
  included: boolean;
}

const UploadDataPage = () => {
  const theme = useTheme();
  const toast = useToast();
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<UploadDataPreview | null>(null);
  const [columnConfigs, setColumnConfigs] = useState<UploadColumnConfig[]>([]);
  const [tableName, setTableName] = useState('');
  const [productTeamId, setProductTeamId] = useState<string>('');
  const [dataObjectId, setDataObjectId] = useState<string>('');
  const [systemId, setSystemId] = useState<string>('');
  const [hasHeader, setHasHeader] = useState(true);
  const [delimiter, setDelimiter] = useState('');
  const [targetWarehouse, setTargetWarehouse] = useState<DataWarehouseTarget>('databricks_sql');
  const [mode, setMode] = useState<UploadTableMode>('create');
  const [typeMenuAnchorEl, setTypeMenuAnchorEl] = useState<HTMLElement | null>(null);
  const [typeMenuColumnIndex, setTypeMenuColumnIndex] = useState<number | null>(null);
  const [columnMenuAnchorEl, setColumnMenuAnchorEl] = useState<HTMLElement | null>(null);
  const [columnMenuIndex, setColumnMenuIndex] = useState<number | null>(null);
  const [editingColumnIndex, setEditingColumnIndex] = useState<number | null>(null);
  const [editingColumnValue, setEditingColumnValue] = useState('');

  const { data: processAreas = [], isLoading: isLoadingProcessAreas } = useQuery(
    ['upload-data-process-areas'],
    fetchProcessAreas,
    {
      onError: (error: unknown) => {
        toast.showError(getErrorMessage(error, 'Unable to load product teams.'));
      }
    }
  );

  const { data: dataObjects = [], isLoading: isLoadingDataObjects } = useQuery(
    ['upload-data-objects', productTeamId],
    () =>
      productTeamId
        ? fetchDataObjects(productTeamId)
        : Promise.resolve<DataObject[]>([]),
    {
      enabled: Boolean(productTeamId),
      onError: (error: unknown) => {
        toast.showError(getErrorMessage(error, 'Unable to load data objects.'));
      }
    }
  );

  const { data: systems = [], isLoading: isLoadingSystems } = useQuery(
    ['upload-data-systems'],
    fetchSystems,
    {
      onError: (error: unknown) => {
        toast.showError(getErrorMessage(error, 'Unable to load systems.'));
      }
    }
  );

  const selectedDataObject = useMemo<DataObject | null>(
    () => dataObjects.find((item) => item.id === dataObjectId) ?? null,
    [dataObjectId, dataObjects]
  );

  const availableSystems = useMemo<System[]>(() => {
    if (selectedDataObject?.systems && selectedDataObject.systems.length > 0) {
      return selectedDataObject.systems;
    }
    return systems;
  }, [selectedDataObject, systems]);

  const handleProductTeamSelect = useCallback(
    (event: SelectChangeEvent<string>) => {
      const value = event.target.value;
      setProductTeamId(value);
      setDataObjectId('');
      setSystemId('');
    },
    []
  );

  const handleDataObjectSelect = useCallback(
    (event: SelectChangeEvent<string>) => {
      const value = event.target.value;
      setDataObjectId(value);
      const next = dataObjects.find((item) => item.id === value);
      if (!next?.systems?.some((entry) => entry.id === systemId)) {
        setSystemId('');
      }
    },
    [dataObjects, systemId]
  );

  const handleSystemSelect = useCallback((event: SelectChangeEvent<string>) => {
    setSystemId(event.target.value);
  }, []);

  const previewMutation = useMutation(
    ({ file, header, separator }: { file: File; header: boolean; separator?: string | null }) =>
      previewUploadData(file, { hasHeader: header, delimiter: separator ?? null }),
    {
      onSuccess: (data) => {
        setPreview(data);
      },
      onError: (error) => {
        setPreview(null);
        toast.showError(getErrorMessage(error, 'Unable to preview the uploaded file.'));
      }
    }
  );

  const createMutation = useMutation(
    () => {
      if (!selectedFile || !preview) {
        throw new Error('No file selected.');
      }
      if (!productTeamId || !dataObjectId || !systemId) {
        throw new Error('Select a product team, data object, and system before uploading.');
      }
      return createTableFromUpload({
        file: selectedFile,
        tableName,
        targetWarehouse,
        mode,
        hasHeader,
        delimiter: delimiter.trim() ? delimiter : null,
        productTeamId,
        dataObjectId,
        systemId,
        columnOverrides
      });
    },
    {
      onSuccess: (result) => {
        toast.showSuccess(
          `Created table ${result.tableName} with ${result.rowsInserted} row${result.rowsInserted === 1 ? '' : 's'}. Manage it in Data Management -> Manage Data.`
        );
        setSelectedFile(null);
        setPreview(null);
        setColumnConfigs([]);
        setTableName('');
        setDelimiter('');
        setMode('create');
        if (fileInputRef.current) {
          fileInputRef.current.value = '';
        }
      },
      onError: (error) => {
        toast.showError(getErrorMessage(error, 'Unable to create the table.'));
      }
    }
  );

  const handleFileSelected = useCallback(
    (file: File) => {
      setSelectedFile(file);
      setPreview(null);
      const baseName = file.name.replace(/\.[^.]+$/, '');
      setTableName(sanitizeTableName(baseName));
      previewMutation.mutate({
        file,
        header: hasHeader,
        separator: delimiter.trim() ? delimiter : null
      });
    },
    [delimiter, hasHeader, previewMutation]
  );

  const handleInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      handleFileSelected(file);
    }
  };

  const handleDrop = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    if (!canManage) {
      return;
    }
    const file = event.dataTransfer.files?.[0];
    if (file) {
      handleFileSelected(file);
    }
  };

  const handleDragOver = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
  };

  const handlePreviewRefresh = () => {
    if (!selectedFile) {
      return;
    }
    previewMutation.mutate({
      file: selectedFile,
      header: hasHeader,
      separator: delimiter.trim() ? delimiter : null
    });
  };

  const handleColumnNameChange = useCallback((index: number, value: string) => {
    setColumnConfigs((previous) =>
      previous.map((column, columnIndex) => {
        if (columnIndex !== index) {
          return column;
        }
        const fallback = `column_${index + 1}`;
        return {
          ...column,
          displayName: sanitizeColumnName(value, fallback)
        };
      })
    );
  }, []);

  const handleColumnTypeChange = useCallback((index: number, value: UploadColumnType) => {
    setColumnConfigs((previous) =>
      previous.map((column, columnIndex) =>
        columnIndex === index
          ? {
              ...column,
              selectedType: value
            }
          : column
      )
    );
  }, []);

  const handleColumnIncludeToggle = useCallback((index: number, included: boolean) => {
    setColumnConfigs((previous) =>
      previous.map((column, columnIndex) =>
        columnIndex === index
          ? {
              ...column,
              included
            }
          : column
      )
    );
  }, []);

  const handleTypeMenuOpen = useCallback((event: MouseEvent<HTMLElement>, index: number) => {
    setTypeMenuAnchorEl(event.currentTarget);
    setTypeMenuColumnIndex(index);
  }, []);

  const handleTypeMenuClose = useCallback(() => {
    setTypeMenuAnchorEl(null);
    setTypeMenuColumnIndex(null);
  }, []);

  const handleTypeSelect = useCallback(
    (value: UploadColumnType) => {
      if (typeMenuColumnIndex === null) {
        return;
      }
      handleColumnTypeChange(typeMenuColumnIndex, value);
      handleTypeMenuClose();
    },
    [handleColumnTypeChange, handleTypeMenuClose, typeMenuColumnIndex]
  );

  const handleColumnMenuOpen = useCallback((event: MouseEvent<HTMLElement>, index: number) => {
    setColumnMenuAnchorEl(event.currentTarget);
    setColumnMenuIndex(index);
  }, []);

  const handleColumnMenuClose = useCallback(() => {
    setColumnMenuAnchorEl(null);
    setColumnMenuIndex(null);
  }, []);

  const handleStartEditingColumn = useCallback(
    (index: number) => {
      const target = columnConfigs[index];
      if (!target) {
        return;
      }
      setEditingColumnIndex(index);
      setEditingColumnValue(target.displayName);
    },
    [columnConfigs]
  );

  const handleEditingNameChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      if (editingColumnIndex === null) {
        return;
      }
      const fallback = `column_${editingColumnIndex + 1}`;
      const sanitized = sanitizeColumnName(event.target.value, fallback);
      setEditingColumnValue(sanitized);
    },
    [editingColumnIndex]
  );

  const handleCommitEditingColumn = useCallback(() => {
    if (editingColumnIndex === null) {
      return;
    }
    handleColumnNameChange(editingColumnIndex, editingColumnValue);
    setEditingColumnIndex(null);
    setEditingColumnValue('');
  }, [editingColumnIndex, editingColumnValue, handleColumnNameChange]);

  const handleCancelEditingColumn = useCallback(() => {
    setEditingColumnIndex(null);
    setEditingColumnValue('');
  }, []);

  const handleNameEditKeyDown = useCallback(
    (event: KeyboardEvent<HTMLInputElement>) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        handleCommitEditingColumn();
      } else if (event.key === 'Escape') {
        event.preventDefault();
        handleCancelEditingColumn();
      }
    },
    [handleCancelEditingColumn, handleCommitEditingColumn]
  );

  const handleReset = () => {
    setSelectedFile(null);
    setPreview(null);
    setColumnConfigs([]);
    setTableName('');
    setDelimiter('');
    setMode('create');
    setHasHeader(true);
    setTypeMenuAnchorEl(null);
    setTypeMenuColumnIndex(null);
    setColumnMenuAnchorEl(null);
    setColumnMenuIndex(null);
    setEditingColumnIndex(null);
    setEditingColumnValue('');
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const isBusy = previewMutation.isLoading || createMutation.isLoading;

  const headerDescription = useMemo(() => {
    if (!preview) {
      return 'Upload a CSV or TSV file to create a new table in the configured warehouse.';
    }
    return `Detected ${preview.columns.length} column${preview.columns.length === 1 ? '' : 's'} and ${preview.totalRows} row${preview.totalRows === 1 ? '' : 's'}.`;
  }, [preview]);

  useEffect(() => {
    if (!preview) {
      setColumnConfigs([]);
      setTypeMenuAnchorEl(null);
      setTypeMenuColumnIndex(null);
      setColumnMenuAnchorEl(null);
      setColumnMenuIndex(null);
      setEditingColumnIndex(null);
      setEditingColumnValue('');
      return;
    }

    setColumnConfigs((previous) => {
      const previousByField = new Map(previous.map((column) => [column.sourceFieldName, column]));
      return preview.columns.map((column, index) => {
        const fallback = `column_${index + 1}`;
        const existing = previousByField.get(column.fieldName);
        const currentName = existing?.displayName ?? column.fieldName;
        const displayName = sanitizeColumnName(currentName, fallback);
        const currentType = existing?.selectedType ?? column.inferredType;
        return {
          sourceFieldName: column.fieldName,
          originalName: column.originalName,
          inferredType: normalizeColumnType(column.inferredType),
          displayName,
          selectedType: normalizeColumnType(currentType),
          included: existing?.included ?? true
        } satisfies UploadColumnConfig;
      });
    });
  }, [preview]);

  const duplicateNameSet = useMemo(() => {
    const counts = new Map<string, number>();
    columnConfigs.forEach((column) => {
      if (!column.included) {
        return;
      }
      const key = column.displayName;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    });
    return new Set<string>([...counts.entries()].filter(([, count]) => count > 1).map(([name]) => name));
  }, [columnConfigs]);

  const columnOverrides = useMemo<UploadDataColumnOverrideInput[]>(() => {
    return columnConfigs
      .map((column) => {
        const override: UploadDataColumnOverrideInput = {
          fieldName: column.sourceFieldName
        };
        if (column.displayName !== column.sourceFieldName) {
          override.targetName = column.displayName;
        }
        if (column.selectedType !== column.inferredType) {
          override.targetType = column.selectedType;
        }
        if (!column.included) {
          override.exclude = true;
        }
        return override;
      })
      .filter((override) => override.targetName || override.targetType || override.exclude === true);
  }, [columnConfigs]);

  const hasIncludedColumns = columnConfigs.some((column) => column.included);
  const hasColumnNameConflicts = duplicateNameSet.size > 0;
  const createDisabled =
    !canManage ||
    !selectedFile ||
    !preview ||
    !tableName.trim() ||
    !productTeamId ||
    !dataObjectId ||
    !systemId ||
    createMutation.isLoading ||
    hasColumnNameConflicts ||
    !hasIncludedColumns;

  const getColumnTypeLabel = (type: UploadColumnType): string =>
    COLUMN_TYPE_OPTIONS.find((option) => option.value === type)?.label ?? type;

  const renderColumnTypeIcon = (type: UploadColumnType) => {
    switch (type) {
      case 'boolean':
        return <CheckCircleOutlineIcon fontSize="small" />;
      case 'integer':
        return <NumbersIcon fontSize="small" />;
      case 'float':
        return <FunctionsIcon fontSize="small" />;
      case 'timestamp':
        return <AccessTimeIcon fontSize="small" />;
      default:
        return <TextFieldsIcon fontSize="small" />;
    }
  };

  const activeTypeColumn = typeMenuColumnIndex !== null ? columnConfigs[typeMenuColumnIndex] ?? null : null;
  const activeColumnMenuTarget = columnMenuIndex !== null ? columnConfigs[columnMenuIndex] ?? null : null;

  return (
    <Box>
      <PageHeader title="Upload Data" subtitle={headerDescription} />

      {!canManage && (
        <Alert severity="info" sx={{ mb: 3 }}>
          You have read-only access. Contact an administrator to upload data.
        </Alert>
      )}

      <Paper
        elevation={3}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        sx={{
          p: 3,
          mb: 3,
          border: `2px dashed ${alpha(theme.palette.primary.main, 0.4)}`,
          borderRadius: 3,
          backgroundColor: alpha(theme.palette.primary.light, 0.05)
        }}
      >
        <Stack spacing={2} alignItems="center" textAlign="center">
          {selectedFile ? (
            <InsertDriveFileIcon sx={{ fontSize: 48, color: theme.palette.primary.main }} />
          ) : (
            <CloudUploadIcon sx={{ fontSize: 48, color: theme.palette.primary.main }} />
          )}
          <Typography variant="h6">
            {selectedFile ? selectedFile.name : 'Drag & drop a file here, or click to browse'}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Supported formats: CSV and TSV. Maximum size 5 MB.
          </Typography>
          <Stack direction="row" spacing={2} alignItems="center">
            <Button
              variant="contained"
              component="label"
              disabled={!canManage || isBusy}
            >
              {selectedFile ? 'Choose a different file' : 'Browse'}
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.tsv,.txt,.tab"
                hidden
                onChange={handleInputChange}
              />
            </Button>
            {selectedFile && (
              <Button variant="outlined" onClick={handleReset} disabled={isBusy}>
                Reset
              </Button>
            )}
          </Stack>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} alignItems="center">
            <FormControlLabel
              control={
                <Switch
                  checked={hasHeader}
                  onChange={(event) => setHasHeader(event.target.checked)}
                  disabled={!selectedFile || isBusy}
                />
              }
              label="File includes header row"
            />
            <TextField
              label="Delimiter"
              value={delimiter}
              onChange={(event) => setDelimiter(event.target.value.slice(0, 1))}
              helperText="Leave blank for automatic detection"
              sx={{ width: 200 }}
              disabled={!selectedFile || isBusy}
            />
            <Button
              variant="text"
              onClick={handlePreviewRefresh}
              disabled={!selectedFile || previewMutation.isLoading}
            >
              {previewMutation.isLoading ? 'Refreshing…' : 'Refresh preview'}
            </Button>
          </Stack>
        </Stack>
      </Paper>

      {previewMutation.isLoading && (
        <Box display="flex" alignItems="center" gap={1} sx={{ mb: 3 }}>
          <CircularProgress size={20} />
          <Typography variant="body2">Analyzing file…</Typography>
        </Box>
      )}

      {preview && (
        <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
          <Stack spacing={2}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Preview & Column Settings
            </Typography>
            {hasColumnNameConflicts && (
              <Alert severity="warning" sx={{ mb: 1 }}>
                Column names must be unique. Resolve the highlighted fields before continuing.
              </Alert>
            )}
            {!hasIncludedColumns && (
              <Alert severity="warning" sx={{ mb: 1 }}>
                Select at least one column to include before continuing.
              </Alert>
            )}
            {columnConfigs.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                Preparing preview…
              </Typography>
            ) : (
              <Box sx={{ overflowX: 'auto' }}>
                <Table size="small" sx={{ minWidth: Math.max(columnConfigs.length * 220, 600) }}>
                  <TableHead>
                    <TableRow>
                      {columnConfigs.map((column, index) => {
                        const nameError = column.included && duplicateNameSet.has(column.displayName);
                        const typeLabel = getColumnTypeLabel(column.selectedType);
                        return (
                          <TableCell
                            key={column.sourceFieldName}
                            sx={{
                              minWidth: 220,
                              verticalAlign: 'top',
                              backgroundColor: column.included
                                ? alpha(theme.palette.primary.main, 0.04)
                                : alpha(theme.palette.warning.main, 0.1),
                              borderTop: nameError ? `2px solid ${theme.palette.error.main}` : undefined,
                              opacity: column.included ? 1 : 0.7
                            }}
                          >
                            <Stack spacing={1}>
                              <Box display="flex" alignItems="center" gap={1}>
                                <Tooltip title={`Data type: ${typeLabel}`}>
                                  <span>
                                    <IconButton
                                      size="small"
                                      color="primary"
                                      onClick={(event) => handleTypeMenuOpen(event, index)}
                                      disabled={!canManage || isBusy}
                                      sx={{ backgroundColor: alpha(theme.palette.primary.main, 0.08) }}
                                    >
                                      {renderColumnTypeIcon(column.selectedType)}
                                    </IconButton>
                                  </span>
                                </Tooltip>
                                <Box flexGrow={1}>
                                  {editingColumnIndex === index ? (
                                    <TextField
                                      size="small"
                                      value={editingColumnValue}
                                      onChange={handleEditingNameChange}
                                      onBlur={handleCommitEditingColumn}
                                      onKeyDown={handleNameEditKeyDown}
                                      fullWidth
                                      disabled={isBusy}
                                      error={nameError}
                                    />
                                  ) : (
                                    <Typography
                                      variant="subtitle2"
                                      align="center"
                                      sx={{
                                        fontWeight: 600,
                                        cursor: canManage ? 'text' : 'default',
                                        color: nameError ? theme.palette.error.main : undefined,
                                        textDecoration: column.included ? 'none' : 'line-through'
                                      }}
                                      tabIndex={canManage ? 0 : -1}
                                      onDoubleClick={() => {
                                        if (canManage && !isBusy) {
                                          handleStartEditingColumn(index);
                                        }
                                      }}
                                      onKeyDown={(event) => {
                                        if (!canManage || isBusy) {
                                          return;
                                        }
                                        if (event.key === 'Enter') {
                                          event.preventDefault();
                                          handleStartEditingColumn(index);
                                        }
                                      }}
                                    >
                                      {column.displayName}
                                    </Typography>
                                  )}
                                </Box>
                                <Tooltip title={column.included ? 'Column included in table' : 'Column excluded from table'}>
                                  <span>
                                    <IconButton
                                      size="small"
                                      onClick={(event) => handleColumnMenuOpen(event, index)}
                                      disabled={!canManage || isBusy}
                                    >
                                      <KeyboardArrowDownIcon fontSize="small" />
                                    </IconButton>
                                  </span>
                                </Tooltip>
                              </Box>
                              {!column.included && (
                                <Typography variant="caption" sx={{ color: theme.palette.warning.dark }}>
                                  Excluded from table
                                </Typography>
                              )}
                              {nameError && (
                                <Typography variant="caption" sx={{ color: theme.palette.error.main }}>
                                  Column name must be unique
                                </Typography>
                              )}
                            </Stack>
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {preview.sampleRows.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={columnConfigs.length} align="center">
                          No data rows detected in the file.
                        </TableCell>
                      </TableRow>
                    ) : (
                      preview.sampleRows.map((row, rowIndex) => (
                        <TableRow key={`sample-${rowIndex}`}>
                          {columnConfigs.map((column, columnIndex) => {
                            const value = row[columnIndex];
                            return (
                              <TableCell
                                key={`${column.sourceFieldName}-${rowIndex}`}
                                sx={{
                                  whiteSpace: 'nowrap',
                                  backgroundColor: column.included
                                    ? undefined
                                    : alpha(theme.palette.warning.main, 0.08),
                                  opacity: column.included ? 1 : 0.5
                                }}
                              >
                                {value !== null && value !== undefined && value !== '' ? (
                                  value
                                ) : (
                                  <Typography
                                    variant="body2"
                                    sx={{ color: theme.palette.error.main, fontStyle: 'italic' }}
                                  >
                                    null
                                  </Typography>
                                )}
                              </TableCell>
                            );
                          })}
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
                <Menu
                  anchorEl={typeMenuAnchorEl}
                  open={Boolean(typeMenuAnchorEl)}
                  onClose={handleTypeMenuClose}
                  anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                >
                  {COLUMN_TYPE_OPTIONS.map((option) => (
                    <MenuItem
                      key={option.value}
                      onClick={() => handleTypeSelect(option.value)}
                      selected={activeTypeColumn?.selectedType === option.value}
                      disabled={!canManage || isBusy}
                    >
                      <ListItemIcon sx={{ minWidth: 32 }}>
                        {renderColumnTypeIcon(option.value)}
                      </ListItemIcon>
                      {option.label}
                    </MenuItem>
                  ))}
                </Menu>
                <Menu
                  anchorEl={columnMenuAnchorEl}
                  open={Boolean(columnMenuAnchorEl)}
                  onClose={handleColumnMenuClose}
                  anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                  transformOrigin={{ vertical: 'top', horizontal: 'right' }}
                >
                  <MenuItem
                    onClick={() => {
                      if (columnMenuIndex === null || !activeColumnMenuTarget) {
                        return;
                      }
                      handleColumnIncludeToggle(columnMenuIndex, !activeColumnMenuTarget.included);
                      handleColumnMenuClose();
                    }}
                    disabled={!canManage || isBusy || columnMenuIndex === null}
                  >
                    {activeColumnMenuTarget?.included ? 'Exclude from table' : 'Include in table'}
                  </MenuItem>
                </Menu>
              </Box>
            )}
          </Stack>
        </Paper>
      )}

      <Paper
        elevation={3}
        sx={{
          p: 3,
          backgroundColor: alpha(theme.palette.primary.light, 0.1),
          '& .MuiInputBase-root': {
            backgroundColor: theme.palette.background.paper
          }
        }}
      >
        <Stack spacing={3}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Table configuration
          </Typography>

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <FormControl
              fullWidth
              required
              disabled={!canManage || createMutation.isLoading || isLoadingProcessAreas}
            >
              <InputLabel id="upload-product-team-label">Product Team</InputLabel>
              <Select
                labelId="upload-product-team-label"
                value={productTeamId}
                label="Product Team"
                onChange={handleProductTeamSelect}
              >
                <MenuItem value="">
                  <em>Select a product team</em>
                </MenuItem>
                {processAreas.map((area: ProcessArea) => (
                  <MenuItem key={area.id} value={area.id}>
                    {area.name}
                  </MenuItem>
                ))}
              </Select>
              {!isLoadingProcessAreas && processAreas.length === 0 && (
                <FormHelperText>No product teams available.</FormHelperText>
              )}
            </FormControl>
            <FormControl
              fullWidth
              required
              disabled={!canManage || createMutation.isLoading || !productTeamId || isLoadingDataObjects}
            >
              <InputLabel id="upload-data-object-label">Data Object</InputLabel>
              <Select
                labelId="upload-data-object-label"
                value={dataObjectId}
                label="Data Object"
                onChange={handleDataObjectSelect}
              >
                <MenuItem value="">
                  <em>Select a data object</em>
                </MenuItem>
                {dataObjects.map((dataObject: DataObject) => (
                  <MenuItem key={dataObject.id} value={dataObject.id}>
                    {dataObject.name}
                  </MenuItem>
                ))}
              </Select>
              {!productTeamId && (
                <FormHelperText>Select a product team first.</FormHelperText>
              )}
            </FormControl>
          </Stack>

          <FormControl
            fullWidth
              required
            disabled={!canManage || createMutation.isLoading || !dataObjectId || isLoadingSystems}
          >
            <InputLabel id="upload-system-label">System</InputLabel>
            <Select
              labelId="upload-system-label"
              value={systemId}
              label="System"
              onChange={handleSystemSelect}
            >
              <MenuItem value="">
                <em>Select a system</em>
              </MenuItem>
              {availableSystems.map((system: System) => (
                <MenuItem key={system.id} value={system.id}>
                  {system.name}
                </MenuItem>
              ))}
            </Select>
            {!dataObjectId ? (
              <FormHelperText>Select a data object to continue.</FormHelperText>
            ) : availableSystems.length === 0 ? (
              <FormHelperText>No systems are linked to this data object.</FormHelperText>
            ) : null}
          </FormControl>

          <TextField
            label="Table name"
            value={tableName}
            onChange={(event) => setTableName(sanitizeTableName(event.target.value))}
            helperText="Automatically sanitized to match warehouse identifier rules."
            disabled={!canManage || createMutation.isLoading}
            fullWidth
          />

          <FormControl fullWidth disabled={!canManage || createMutation.isLoading}>
            <InputLabel id="target-warehouse-label">Target warehouse</InputLabel>
            <Select
              labelId="target-warehouse-label"
              value={targetWarehouse}
              label="Target warehouse"
              onChange={(event) => setTargetWarehouse(event.target.value as DataWarehouseTarget)}
            >
              {TARGET_OPTIONS.map((option) => (
                <MenuItem key={option.value} value={option.value} disabled={option.disabled}>
                  {option.label}
                </MenuItem>
              ))}
            </Select>
            {targetWarehouse !== 'databricks_sql' && (
              <FormHelperText>Only Databricks uploads are supported today.</FormHelperText>
            )}
          </FormControl>

          <FormControl fullWidth disabled={!canManage || createMutation.isLoading}>
            <InputLabel id="table-mode-label">Write mode</InputLabel>
            <Select
              labelId="table-mode-label"
              value={mode}
              label="Write mode"
              onChange={(event) => setMode(event.target.value as UploadTableMode)}
            >
              <MenuItem value="create">Create new table</MenuItem>
              <MenuItem value="replace">Replace existing table</MenuItem>
            </Select>
            <FormHelperText>
              {mode === 'create'
                ? 'Fails if the table already exists.'
                : 'Drops the existing table before recreating it.'}
            </FormHelperText>
          </FormControl>

          <Stack direction="row" spacing={2} justifyContent="flex-end">
            <Button
              variant="contained"
              onClick={() => createMutation.mutate()}
              disabled={createDisabled}
            >
              {createMutation.isLoading ? 'Creating…' : 'Create table'}
            </Button>
          </Stack>
        </Stack>
      </Paper>
    </Box>
  );
};

export default UploadDataPage;
