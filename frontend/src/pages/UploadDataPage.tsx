import { ChangeEvent, DragEvent, useCallback, useMemo, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControl,
  FormControlLabel,
  FormHelperText,
  InputLabel,
  MenuItem,
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
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';
import { useMutation, useQuery } from 'react-query';
import { isAxiosError } from 'axios';
import type { SelectChangeEvent } from '@mui/material/Select';

import { useAuth } from '../context/AuthContext';
import { useToast } from '../hooks/useToast';
import {
  DataWarehouseTarget,
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
  { value: 'databricks_sql', label: 'Databricks SQL Warehouse' },
  { value: 'sap_hana', label: 'SAP HANA (coming soon)', disabled: true }
];

const UploadDataPage = () => {
  const theme = useTheme();
  const toast = useToast();
  const { hasRole } = useAuth();
  const canManage = hasRole('admin');

  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<UploadDataPreview | null>(null);
  const [tableName, setTableName] = useState('');
  const [schemaName, setSchemaName] = useState('');
  const [catalog, setCatalog] = useState('');
  const [productTeamId, setProductTeamId] = useState<string>('');
  const [dataObjectId, setDataObjectId] = useState<string>('');
  const [systemId, setSystemId] = useState<string>('');
  const [hasHeader, setHasHeader] = useState(true);
  const [delimiter, setDelimiter] = useState('');
  const [targetWarehouse, setTargetWarehouse] = useState<DataWarehouseTarget>('databricks_sql');
  const [mode, setMode] = useState<UploadTableMode>('create');

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
        schemaName: schemaName.trim() || undefined,
        catalog: catalog.trim() || undefined,
        mode,
        hasHeader,
        delimiter: delimiter.trim() ? delimiter : null,
        productTeamId,
        dataObjectId,
        systemId
      });
    },
    {
      onSuccess: (result) => {
        toast.showSuccess(
          `Created table ${result.tableName} with ${result.rowsInserted} row${result.rowsInserted === 1 ? '' : 's'}. Manage it in Data Management -> Manage Data.`
        );
        setSelectedFile(null);
        setPreview(null);
        setTableName('');
        setSchemaName('');
        setCatalog('');
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

  const handleReset = () => {
    setSelectedFile(null);
    setPreview(null);
    setTableName('');
    setSchemaName('');
    setCatalog('');
    setDelimiter('');
    setMode('create');
    setHasHeader(true);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const isBusy = previewMutation.isLoading || createMutation.isLoading;
  const createDisabled =
    !canManage ||
    !selectedFile ||
    !preview ||
    !tableName.trim() ||
    !productTeamId ||
    !dataObjectId ||
    !systemId ||
    createMutation.isLoading;

  const headerDescription = useMemo(() => {
    if (!preview) {
      return 'Upload a CSV or TSV file to create a new table in the configured warehouse.';
    }
    return `Detected ${preview.columns.length} column${preview.columns.length === 1 ? '' : 's'} and ${preview.totalRows} row${preview.totalRows === 1 ? '' : 's'}.`;
  }, [preview]);

  return (
    <Box>
      <Box
        sx={{
          background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.08)} 100%)`,
          borderBottom: `3px solid ${theme.palette.primary.main}`,
          borderRadius: '12px',
          p: 3,
          mb: 3,
          boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.12)}`
        }}
      >
        <Typography variant="h4" gutterBottom sx={{ color: theme.palette.primary.dark, fontWeight: 800, fontSize: '1.75rem' }}>
          Upload Data
        </Typography>
        <Typography variant="body2" sx={{ color: theme.palette.primary.dark, opacity: 0.85, fontSize: '0.95rem' }}>
          {headerDescription}
        </Typography>
      </Box>

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
        <Stack spacing={3} sx={{ mb: 3 }}>
          <Paper elevation={2} sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Column summary
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>#</TableCell>
                  <TableCell>Field name</TableCell>
                  <TableCell>Original header</TableCell>
                  <TableCell>Inferred type</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {preview.columns.map((column, index) => (
                  <TableRow key={column.fieldName}>
                    <TableCell>{index + 1}</TableCell>
                    <TableCell>
                      <Typography variant="body2" fontWeight={600}>
                        {column.fieldName}
                      </Typography>
                    </TableCell>
                    <TableCell>{column.originalName}</TableCell>
                    <TableCell sx={{ textTransform: 'capitalize' }}>{column.inferredType}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Paper>

          <Paper elevation={2} sx={{ p: 3 }}>
            <Typography variant="h6" gutterBottom>
              Sample preview
            </Typography>
            <Table size="small" sx={{ tableLayout: 'fixed' }}>
              <TableHead>
                <TableRow>
                  {preview.columns.map((column) => (
                    <TableCell key={column.fieldName} sx={{ fontWeight: 600 }}>
                      {column.fieldName}
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {preview.sampleRows.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={preview.columns.length} align="center">
                      No data rows detected in the file.
                    </TableCell>
                  </TableRow>
                ) : (
                  preview.sampleRows.map((row, rowIndex) => (
                    <TableRow key={`sample-${rowIndex}`}>
                      {preview.columns.map((column, columnIndex) => (
                        <TableCell key={column.fieldName}>
                          {row[columnIndex] ?? <Typography color="text.secondary">null</Typography>}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </Paper>
        </Stack>
      )}

      <Paper elevation={3} sx={{ p: 3 }}>
        <Stack spacing={3}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Table configuration
          </Typography>

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <FormControl
              fullWidth
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

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Catalog"
              value={catalog}
              onChange={(event) => setCatalog(event.target.value)}
              helperText="Optional. Applies to Databricks uploads."
              disabled={!canManage || createMutation.isLoading || targetWarehouse !== 'databricks_sql'}
              fullWidth
            />
            <TextField
              label="Schema"
              value={schemaName}
              onChange={(event) => setSchemaName(event.target.value)}
              helperText="Optional target schema."
              disabled={!canManage || createMutation.isLoading}
              fullWidth
            />
          </Stack>

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
