import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Stack,
  CircularProgress,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Snackbar,
  Checkbox,
  FormControl,
  FormControlLabel,
  MenuItem,
  Select,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import { alpha, useTheme } from '@mui/material/styles';
import { RevoGrid, Template } from '@revolist/react-datagrid';
import type { ColumnDataSchemaModel, ColumnRegular, ColumnTemplateProp } from '@revolist/revogrid';
import type { SelectChangeEvent } from '@mui/material/Select';
import type { AxiosError } from 'axios';

import {
  ConstructedField,
  ConstructedData,
  ConstructedRowPayload,
  batchSaveConstructedData,
  createConstructedData,
  updateConstructedData,
  deleteConstructedData,
} from '../../services/constructedDataService';
import { useToast } from '../../hooks/useToast';
import { useAuth } from '../../context/AuthContext';
import { fetchProjects } from '../../services/projectService';
import { fetchReleases } from '../../services/releaseService';
import type { Project, Release } from '../../types/data';

type EditableConstructedRow = ConstructedData & { isNew?: boolean };
type PayloadFieldKey = `payload.${string}`;
type GridRowData = EditableConstructedRow &
  Record<PayloadFieldKey, unknown> & {
    __isPlaceholder?: boolean;
  };

type RevoGridSelectionRange = {
  y: number;
  y1: number;
};

type RevoGridElement = HTMLRevoGridElement & {
  getSelectedRange?: () => Promise<RevoGridSelectionRange | null>;
  cancelEdit?: (rowType?: string, colType?: string) => Promise<void> | void;
  isCellEditing?: () => boolean;
};

const isColumnDataSchemaModel = (
  props: ColumnDataSchemaModel | ColumnTemplateProp
): props is ColumnDataSchemaModel => 'model' in props;

type GridModel = {
  id?: string | number;
  rowId?: string | number;
  rowIndex?: number;
  constructedTableId?: string;
  payload?: Record<string, unknown>;
  rowIdentifier?: string | null;
  createdAt?: string;
  updatedAt?: string;
} & Record<string, unknown>;

interface AfterEditDetail {
  model?: GridModel;
  models?: GridModel[];
}

interface SaveRowOptions {
  skipRefresh?: boolean;
  suppressSuccessToast?: boolean;
  suppressErrorToast?: boolean;
}

interface SaveRowResult {
  success: boolean;
  error?: string;
}

interface Props {
  constructedTableId: string;
  fields: ConstructedField[];
  rows: ConstructedData[];
  onDataChange: () => void;
}

const emptyRowIdPrefix = 'new-';

const AUDIT_PROJECT_FIELD = 'Project';
const AUDIT_RELEASE_FIELD = 'Release';
const AUDIT_CREATED_BY_FIELD = 'Created By';
const AUDIT_CREATED_DATE_FIELD = 'Created Date';
const AUDIT_MODIFIED_BY_FIELD = 'Modified By';
const AUDIT_MODIFIED_DATE_FIELD = 'Modified Date';

const READ_ONLY_AUDIT_FIELDS = new Set<string>([
  AUDIT_CREATED_BY_FIELD,
  AUDIT_CREATED_DATE_FIELD,
  AUDIT_MODIFIED_BY_FIELD,
  AUDIT_MODIFIED_DATE_FIELD,
]);

const cloneConstructedRow = (row: ConstructedData): EditableConstructedRow => ({
  ...row,
  payload: { ...row.payload },
  isNew: false,
});

const ConstructedDataGridAgGrid: React.FC<Props> = ({
  constructedTableId,
  fields,
  rows,
  onDataChange,
}) => {
  const theme = useTheme();
  const toast = useToast();
  const { user } = useAuth();

  const [projects, setProjects] = useState<Project[]>([]);
  const [releases, setReleases] = useState<Release[]>([]);

  const projectOptions = useMemo(
    () =>
      projects.map((project) => ({
        label: project.name,
        value: project.name,
      })),
    [projects]
  );

  const releaseOptions = useMemo(
    () =>
      releases.map((release) => ({
        label: release.projectName ? `${release.projectName} - ${release.name}` : release.name,
        value: release.name,
      })),
    [releases]
  );

  const userDisplayName = useMemo(() => {
    if (!user || !user.name) {
      return 'Unknown User';
    }
    return user.name;
  }, [user]);

  const auditFieldPresence = useMemo(
    () => ({
      project: fields.some((field) => field.name === AUDIT_PROJECT_FIELD),
      release: fields.some((field) => field.name === AUDIT_RELEASE_FIELD),
      createdBy: fields.some((field) => field.name === AUDIT_CREATED_BY_FIELD),
      createdDate: fields.some((field) => field.name === AUDIT_CREATED_DATE_FIELD),
      modifiedBy: fields.some((field) => field.name === AUDIT_MODIFIED_BY_FIELD),
      modifiedDate: fields.some((field) => field.name === AUDIT_MODIFIED_DATE_FIELD),
    }),
    [fields]
  );

  const ensureAuditFields = useCallback(
    (payload: ConstructedRowPayload, action: 'create' | 'update') => {
      const timestamp = new Date().toISOString();

      if (auditFieldPresence.createdBy && action === 'create') {
        payload[AUDIT_CREATED_BY_FIELD] = userDisplayName;
      }

      if (auditFieldPresence.createdDate && action === 'create') {
        payload[AUDIT_CREATED_DATE_FIELD] = timestamp;
      }

      if (auditFieldPresence.modifiedBy) {
        payload[AUDIT_MODIFIED_BY_FIELD] = userDisplayName;
      }

      if (auditFieldPresence.modifiedDate) {
        payload[AUDIT_MODIFIED_DATE_FIELD] = timestamp;
      }

      return payload;
    },
    [auditFieldPresence.createdBy, auditFieldPresence.createdDate, auditFieldPresence.modifiedBy, auditFieldPresence.modifiedDate, userDisplayName]
  );

  const [localRows, setLocalRows] = useState<EditableConstructedRow[]>(() =>
    rows.map(cloneConstructedRow)
  );
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<EditableConstructedRow | null>(null);
  const [bulkDeleteTargets, setBulkDeleteTargets] = useState<EditableConstructedRow[]>([]);
  const [isBulkDeleting, setIsBulkDeleting] = useState(false);
  const [undoState, setUndoState] = useState<{ rows: EditableConstructedRow[] } | null>(null);
  const [isRestoring, setIsRestoring] = useState(false);
  const [selectedRowIds, setSelectedRowIds] = useState<Set<string>>(new Set());
  const [activeAuditCell, setActiveAuditCell] = useState<{
    rowId: string;
    fieldName: string;
  } | null>(null);
  const [editingAuditCell, setEditingAuditCell] = useState<{
    rowId: string;
    fieldName: string;
  } | null>(null);
  const gridRef = useRef<HTMLRevoGridElement | null>(null);

  useEffect(() => {
    setLocalRows(rows.map(cloneConstructedRow));
  }, [rows]);

  useEffect(() => {
    let isCancelled = false;

    const loadReferenceData = async () => {
      try {
        const [projectPayload, releasePayload] = await Promise.all([
          fetchProjects(),
          fetchReleases(),
        ]);
        if (!isCancelled) {
          setProjects(projectPayload);
          setReleases(releasePayload);
        }
      } catch (error) {
        if (!isCancelled) {
          const message = error instanceof Error ? error.message : 'Failed to load reference data';
          toast.showToast(message, 'error');
        }
      }
    };

    void loadReferenceData();

    return () => {
      isCancelled = true;
    };
  }, [toast]);

  useEffect(() => {
    const validIds = new Set(localRows.map((row) => String(row.id)));
    setSelectedRowIds((prev) => {
      let changed = false;
      const next = new Set<string>();
      prev.forEach((id) => {
        if (validIds.has(id)) {
          next.add(id);
        } else {
          changed = true;
        }
      });
      if (!changed && next.size === prev.size) {
        return prev;
      }
      return next;
    });
  }, [localRows]);

  useEffect(() => {
    if (!editingAuditCell) {
      return;
    }
    const exists = localRows.some((row) => String(row.id) === editingAuditCell.rowId);
    if (!exists) {
      setEditingAuditCell(null);
    }
  }, [editingAuditCell, localRows]);


  const createEmptyRow = useCallback((options?: { applyAuditDefaults?: boolean }): EditableConstructedRow => {
    const payload = fields.reduce<ConstructedRowPayload>((acc, field) => {
      acc[field.name] = '';
      return acc;
    }, {} as ConstructedRowPayload);
    if (options?.applyAuditDefaults !== false) {
      ensureAuditFields(payload, 'create');
    }
    const uniqueSuffix = Math.random().toString(16).slice(2, 10);
    return {
      id: `${emptyRowIdPrefix}${Date.now()}-${uniqueSuffix}`,
      constructedTableId,
      payload,
      rowIdentifier: null,
      isNew: true,
    };
  }, [constructedTableId, ensureAuditFields, fields]);

  const toEditableRow = useCallback(
    (row: GridRowData | EditableConstructedRow): EditableConstructedRow => ({
      id: row.id,
      constructedTableId: row.constructedTableId ?? constructedTableId,
      payload: { ...row.payload },
      rowIdentifier: row.rowIdentifier ?? null,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      isNew: row.isNew ?? String(row.id).startsWith(emptyRowIdPrefix),
    }),
    [constructedTableId]
  );

  const toGridRow = useCallback(
    (row: EditableConstructedRow, options?: { isPlaceholder?: boolean }): GridRowData => {
      const flattened: GridRowData = {
        ...row,
        payload: { ...row.payload },
        isNew: row.isNew ?? String(row.id).startsWith(emptyRowIdPrefix),
      } as GridRowData;

      if (options?.isPlaceholder) {
        flattened.__isPlaceholder = true;
      } else if (options?.isPlaceholder === false) {
        flattened.__isPlaceholder = false;
      }

      fields.forEach((field) => {
        const key = `payload.${field.name}` as PayloadFieldKey;
        flattened[key] = row.payload?.[field.name] ?? '';
      });

      return flattened;
    },
    [fields]
  );

  const placeholderRow = useMemo<EditableConstructedRow | null>(() => {
    if (!localRows.length) {
      return createEmptyRow({ applyAuditDefaults: false });
    }

    const last = localRows[localRows.length - 1];
    const hasContent = Object.values(last.payload ?? {}).some(
      (value) => value !== '' && value !== null && value !== undefined
    );

    return hasContent ? createEmptyRow({ applyAuditDefaults: false }) : null;
  }, [createEmptyRow, localRows]);

  const gridRows = useMemo<GridRowData[]>(() => {
    const base = localRows.map((row) => toGridRow(row));
    if (placeholderRow) {
      base.push(toGridRow(placeholderRow, { isPlaceholder: true }));
    }
    return base;
  }, [localRows, placeholderRow, toGridRow]);

  const toUndoRow = useCallback(
    (row: GridRowData | EditableConstructedRow): EditableConstructedRow => toEditableRow(row),
    [toEditableRow]
  );

  const saveRow = useCallback(
    async (rowData: GridRowData, options: SaveRowOptions = {}): Promise<SaveRowResult> => {
      const {
        skipRefresh = false,
        suppressSuccessToast = false,
        suppressErrorToast = false,
      } = options;

      if (!rowData || rowData.__isPlaceholder) {
        return { success: false, error: 'Nothing to save' };
      }

      try {
        setIsSaving(true);

        const isNewRow = rowData.isNew ?? String(rowData.id).startsWith(emptyRowIdPrefix);
        const targetTableId = rowData.constructedTableId ?? constructedTableId;

        const basePayload = fields.reduce<ConstructedRowPayload>((acc, field) => {
          const fieldName = field.name;
          const flattenedKey = `payload.${fieldName}` as PayloadFieldKey;
          let value = rowData.payload?.[fieldName];
          if (value === undefined) {
            value = rowData[flattenedKey] as string | undefined;
          }
          if (READ_ONLY_AUDIT_FIELDS.has(fieldName) && !isNewRow) {
            value = rowData.payload?.[fieldName] ?? value ?? '';
          }
          acc[fieldName] = (value ?? '') as string;
          return acc;
        }, {} as ConstructedRowPayload);

        const payload = ensureAuditFields({ ...basePayload }, isNewRow ? 'create' : 'update');

        if (isNewRow) {
          const validation = await batchSaveConstructedData(constructedTableId, {
            rows: [payload],
            validateOnly: true,
          });

          if (!validation.success) {
            if (!suppressErrorToast) {
              toast.showToast('Validation failed for row. Fix errors before continuing.', 'warning');
            }
            return {
              success: false,
              error: 'Validation failed for row. Fix errors before continuing.',
            };
          }
        }

        if (isNewRow) {
          const created = await createConstructedData({
            constructedTableId: targetTableId,
            payload,
            rowIdentifier: rowData.rowIdentifier ?? undefined,
          });
          setLocalRows((prev) =>
            prev.map((row) =>
              String(row.id) === String(rowData.id) ? { ...created, isNew: false } : row
            )
          );
          setSelectedRowIds((prev) => {
            if (!prev.has(String(rowData.id))) {
              return prev;
            }
            const next = new Set(prev);
            next.delete(String(rowData.id));
            next.add(String(created.id));
            return next;
          });
          if (!suppressSuccessToast) {
            toast.showToast('Row created', 'success');
          }
        } else {
          await updateConstructedData(rowData.id, { payload });
          setLocalRows((prev) =>
            prev.map((row) =>
              String(row.id) === String(rowData.id)
                ? { ...row, payload: { ...payload }, isNew: false }
                : row
            )
          );
          if (!suppressSuccessToast) {
            toast.showToast('Row saved', 'success');
          }
        }

        if (!skipRefresh) {
          onDataChange();
        }

        return { success: true };
      } catch (rawError) {
        const axiosError = rawError as AxiosError<
          | string
          | {
              detail?: string;
              errors?: Array<{ message?: string }>;
            }
        >;
        const responseData = axiosError.response?.data;

        let errorMessage: string | undefined;
        if (responseData) {
          if (typeof responseData === 'string') {
            errorMessage = responseData;
          } else if (responseData.detail) {
            errorMessage = responseData.detail;
          } else if (Array.isArray(responseData.errors) && responseData.errors.length) {
            errorMessage = responseData.errors.map((error) => error.message).filter(Boolean).join('\n');
          }
        }

        if (!errorMessage) {
          errorMessage = rawError instanceof Error ? rawError.message : 'Failed to save row';
        }

        const statusCode = axiosError.response?.status;
        if (statusCode) {
          errorMessage = `[${statusCode}] ${errorMessage}`;
        }

        console.error('Failed to save constructed data row', rawError);
        if (!suppressErrorToast) {
          toast.showToast(errorMessage, 'error');
        }
        return { success: false, error: errorMessage };
      } finally {
        setIsSaving(false);
      }
    },
    [constructedTableId, ensureAuditFields, fields, onDataChange, toast]
  );

  const handleModelChange = useCallback(
    async (model: GridModel, saveOptions: SaveRowOptions = {}): Promise<SaveRowResult | null> => {
      if (!model) {
        return null;
      }

      const baseRowId =
        model.id !== undefined && model.id !== null
          ? model.id
          : model.rowId !== undefined && model.rowId !== null
          ? model.rowId
          : null;

      let stringRowId: string | null =
        baseRowId !== null ? String(baseRowId) : null;

      const rowIndex =
        typeof (model as { rowIndex?: number }).rowIndex === 'number'
          ? (model as { rowIndex: number }).rowIndex
          : undefined;

      let pendingRowForSave: EditableConstructedRow | null = null;

      setLocalRows((prev) => {
        const next = [...prev];

        if (!stringRowId && rowIndex !== undefined && rowIndex < next.length) {
          const existing = next[rowIndex];
          if (existing) {
            stringRowId = String(existing.id);
          }
        }

        if (!stringRowId) {
          stringRowId = `${emptyRowIdPrefix}${Date.now()}-${Math.random()
            .toString(16)
            .slice(2, 10)}`;
        }

        const derivePayload = (
          existing: EditableConstructedRow | undefined,
          action: 'create' | 'update'
        ): ConstructedRowPayload => {
          const base = fields.reduce<ConstructedRowPayload>((acc, field) => {
            const fieldName = field.name;

            if (READ_ONLY_AUDIT_FIELDS.has(fieldName) && existing) {
              acc[fieldName] = existing.payload?.[fieldName] ?? '';
              return acc;
            }

            const flattenedKey = `payload.${fieldName}` as PayloadFieldKey;
            let cellValue = (model as Record<string, unknown>)[flattenedKey] as string | undefined;
            if (cellValue === undefined && model.payload) {
              cellValue = model.payload[fieldName] as string | undefined;
            }
            if (cellValue === undefined && existing) {
              cellValue = existing.payload?.[fieldName] as string | undefined;
            }

            acc[fieldName] = (cellValue ?? '') as string;
            return acc;
          }, {} as ConstructedRowPayload);

          return ensureAuditFields(base, action);
        };

        const targetId = stringRowId as string;
        const index = next.findIndex((row) => String(row.id) === targetId);

        if (index === -1) {
          const payload = derivePayload(undefined, 'create');
          const newRow: EditableConstructedRow = {
            id: targetId,
            constructedTableId: model.constructedTableId ?? constructedTableId,
            payload,
            rowIdentifier: model.rowIdentifier ?? null,
            createdAt: model.createdAt,
            updatedAt: model.updatedAt,
            isNew: true,
          };
          pendingRowForSave = newRow;
          if (rowIndex !== undefined && rowIndex <= next.length) {
            next.splice(Math.max(rowIndex, 0), 0, newRow);
          } else {
            next.push(newRow);
          }
          return next;
        }

        const current = next[index];
        const derived = derivePayload(current, 'update');
        const updatedPayload = { ...current.payload };
        let changed = false;

        fields.forEach((field) => {
          const fieldName = field.name;
          if (updatedPayload[fieldName] !== derived[fieldName]) {
            updatedPayload[fieldName] = derived[fieldName];
            changed = true;
          }
        });

        if (!changed) {
          return prev;
        }

        const updatedRow: EditableConstructedRow = {
          ...current,
          payload: updatedPayload,
          isNew: current.isNew ?? String(current.id).startsWith(emptyRowIdPrefix),
        };
        next[index] = updatedRow;
        pendingRowForSave = updatedRow;
        return next;
      });

      if (pendingRowForSave) {
        return await saveRow(toGridRow(pendingRowForSave), {
          suppressSuccessToast: true,
          ...saveOptions,
        });
      }

      return null;
    },
    [constructedTableId, ensureAuditFields, fields, saveRow, toGridRow]
  );

  const handleAfterEdit = useCallback(
    (event: CustomEvent<AfterEditDetail>) => {
      const detail = event.detail;
      if (!detail) {
        return;
      }

      if (Array.isArray(detail.models) && detail.models.length) {
        const uniqueModels = new Map<string, GridModel>();
        detail.models.forEach((model) => {
          const baseId = model.id ?? model.rowId;
          const rowIndex =
            typeof (model as { rowIndex?: number }).rowIndex === 'number'
              ? (model as { rowIndex: number }).rowIndex
              : undefined;
          const key =
            baseId !== undefined && baseId !== null
              ? `id-${String(baseId)}`
              : rowIndex !== undefined
              ? `index-${rowIndex}`
              : `generated-${uniqueModels.size}`;
          uniqueModels.set(key, model);
        });
        const modelsToProcess = Array.from(uniqueModels.values());
        const promises = modelsToProcess.map((model) =>
          handleModelChange(model, { skipRefresh: true, suppressSuccessToast: true })
        );
        void Promise.all(promises).then((results) => {
          const successCount = results.filter((result) => result?.success).length;
          if (successCount > 0) {
            onDataChange();
            if (successCount > 1) {
              toast.showToast(`${successCount} rows saved`, 'success');
            }
          }
        });
        return;
      }

      if (detail.model) {
        void handleModelChange(detail.model);
      }
    },
    [handleModelChange, onDataChange, toast]
  );

  const handleAuditFieldChange = useCallback(
    async (row: GridRowData, fieldName: string, value: string) => {
      if (row.__isPlaceholder) {
        return false;
      }

      const previousValue = (row.payload?.[fieldName] ?? '') as string;
      if (previousValue === value) {
        return true;
      }

      const nextPayload = { ...row.payload, [fieldName]: value };
      const model = {
        id: row.id,
        rowId: row.id,
        constructedTableId: row.constructedTableId,
        payload: nextPayload,
        rowIdentifier: row.rowIdentifier,
      } as GridModel;

      (model as Record<string, unknown>)[`payload.${fieldName}`] = value;

      try {
        const result = await handleModelChange(model, { suppressSuccessToast: false });
        if (!result?.success) {
          const message = result?.error ?? 'Failed to save row';
          toast.showToast(message, 'error');
          return false;
        }
        return true;
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Failed to save row';
        toast.showToast(errorMessage, 'error');
        return false;
      }
    },
    [handleModelChange, toast]
  );

  const createSelectCellTemplate = useCallback(
    (fieldName: string, options: { label: string; value: string }[]) =>
      Template((props: ColumnDataSchemaModel | ColumnTemplateProp) => {
        if (!isColumnDataSchemaModel(props)) {
          return null;
        }
        const row = props.model as GridRowData | undefined;
        if (!row || row.__isPlaceholder) {
          return null;
        }

        const currentValue = (row.payload?.[fieldName] ?? '') as string;
        const hasOptions = options.length > 0;
        const effectiveOptions = hasOptions
          ? options
          : currentValue
          ? [{ label: currentValue, value: currentValue }]
          : [];

        const rowId = String(row.id);

        const closeEditor = () => {
          setEditingAuditCell((prev) => {
            if (!prev) {
              return prev;
            }
            if (prev.rowId === rowId && prev.fieldName === fieldName) {
              return null;
            }
            return prev;
          });
        };

        const commitValue = async (nextValue: string) => {
          if (nextValue === currentValue) {
            closeEditor();
            return;
          }

          const saved = await handleAuditFieldChange(row, fieldName, nextValue);
          if (!saved) {
            console.warn('Dropdown selection could not be persisted for row', row.id, fieldName);
            return;
          }

          const gridElement = gridRef.current as RevoGridElement | null;

          try {
            await gridElement?.cancelEdit?.(props.type, props.colType);
          } catch (error) {
            console.error('Failed to cancel grid edit after dropdown commit', error);
          }

          closeEditor();
        };

        const handleChange = (event: SelectChangeEvent<string>) => {
          const { value } = event.target;
          void commitValue(typeof value === 'string' ? value : '');
        };

        const handleOpen = () => {
          setActiveAuditCell({ rowId, fieldName });
          setEditingAuditCell({ rowId, fieldName });
        };

        const handleClose = () => {
          closeEditor();
        };

        return (
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              height: '100%',
              px: 0.25,
            }}
            onClick={() => {
              setActiveAuditCell({ rowId, fieldName });
            }}
          >
            <FormControl
              variant="outlined"
              size="small"
              sx={{
                width: '100%',
                '& .MuiOutlinedInput-root': {
                  height: '100%',
                  fontSize: 13,
                  paddingRight: 3,
                },
                '& .MuiOutlinedInput-notchedOutline': {
                  border: 'none',
                },
              }}
            >
              <Select
                value={currentValue}
                displayEmpty
                onOpen={handleOpen}
                onClose={handleClose}
                onChange={handleChange}
                IconComponent={ArrowDropDownIcon}
                MenuProps={{
                  MenuListProps: { dense: true },
                  anchorOrigin: { vertical: 'bottom', horizontal: 'left' },
                  transformOrigin: { vertical: 'top', horizontal: 'left' },
                }}
                renderValue={(value) => {
                  if (!value) {
                    return (
                      <Typography component="span" variant="body2" sx={{ color: 'text.disabled' }}>
                        Select...
                      </Typography>
                    );
                  }
                  const label = effectiveOptions.find((option) => option.value === value)?.label;
                  return label ?? String(value);
                }}
                sx={{
                  '& .MuiSelect-select': {
                    display: 'flex',
                    alignItems: 'center',
                    px: 0.75,
                    py: 0,
                  },
                  '& .MuiSelect-icon': {
                    right: 4,
                    opacity: 0.5,
                  },
                  '&:hover .MuiSelect-icon, &.Mui-focused .MuiSelect-icon': {
                    opacity: 1,
                  },
                  '& .MuiSelect-select.MuiSelect-outlined.MuiInputBase-inputAdornedEnd': {
                    paddingRight: 24,
                  },
                }}
              >
                <MenuItem value="">
                  <em>None</em>
                </MenuItem>
                {effectiveOptions.map((option) => (
                  <MenuItem key={option.value} value={option.value}>
                    {option.label}
                  </MenuItem>
                ))}
                {!hasOptions && !effectiveOptions.length && (
                  <MenuItem disabled>
                    No options available
                  </MenuItem>
                )}
              </Select>
            </FormControl>
          </Box>
        );
      }),
    [handleAuditFieldChange, setActiveAuditCell, setEditingAuditCell]
  );
  const handleSelectionToggle = useCallback((row: GridRowData, checked: boolean) => {
    if (row.__isPlaceholder) {
      return;
    }
    setSelectedRowIds((prev) => {
      const next = new Set(prev);
      const rowId = String(row.id);
      if (checked) {
        next.add(rowId);
      } else {
        next.delete(rowId);
      }
      return next;
    });
  }, []);

  const handleSelectAllChange = useCallback(
    (checked: boolean) => {
      if (!checked) {
        setSelectedRowIds(new Set<string>());
        return;
      }
      const allIds = localRows.map((row) => String(row.id));
      setSelectedRowIds(new Set(allIds));
    },
    [localRows]
  );

  // Intercepts Excel-style clipboard pastes so we can create and persist multiple rows at once.
  const handlePaste = useCallback(
    async (event: React.ClipboardEvent<HTMLDivElement>) => {
      const clipboardData = event.clipboardData?.getData('text/plain');
      if (!clipboardData) {
        return;
      }

      if (!fields.length) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();

      const sanitized = clipboardData.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const rawLines = sanitized.split('\n').map((line) => line.trimEnd());
      const parsedRowsRaw = rawLines.map((line) => line.split('\t'));
      const hasNonEmptyCells = parsedRowsRaw.some((cells) =>
        cells.some((cell) => cell.trim().length > 0)
      );
      if (!hasNonEmptyCells) {
        return;
      }

      const maxColumns = parsedRowsRaw.reduce((max, cells) => Math.max(max, cells.length), 0);

      if (activeAuditCell && maxColumns <= 1) {
        const targetCell = activeAuditCell;
        const startIndex = localRows.findIndex((row) => String(row.id) === targetCell.rowId);
        if (startIndex !== -1) {
          const columnRows = [...parsedRowsRaw];
          while (
            columnRows.length &&
            columnRows[columnRows.length - 1].every((cell) => cell.trim().length === 0)
          ) {
            columnRows.pop();
          }

          if (!columnRows.length) {
            return;
          }

          const columnValues = columnRows.map((cells) => (cells[0] ?? ''));
          const models: GridModel[] = [];

          const selectedRowIndices = localRows.reduce<number[]>((acc, row, index) => {
            if (selectedRowIds.has(String(row.id))) {
              acc.push(index);
            }
            return acc;
          }, []);

          const rangePromise = (gridRef.current as RevoGridElement | null)?.getSelectedRange?.();
          const selectedRange = rangePromise ? await rangePromise : null;

          const highlightedRowIndices: number[] = selectedRange &&
            typeof selectedRange.y === 'number' &&
            typeof selectedRange.y1 === 'number'
            ? (() => {
                const lower = Math.min(selectedRange.y, selectedRange.y1);
                const upper = Math.max(selectedRange.y, selectedRange.y1);
                const indices: number[] = [];
                for (let index = lower; index <= upper; index += 1) {
                  indices.push(index);
                }
                return indices;
              })()
            : [];

          const candidateSet = new Set<number>();
          candidateSet.add(startIndex);
          selectedRowIndices.forEach((index) => {
            candidateSet.add(index);
          });
          highlightedRowIndices.forEach((index) => {
            candidateSet.add(index);
          });

          const sortedCandidates = Array.from(candidateSet)
            .filter((index) => index >= 0 && index < localRows.length)
            .sort((a, b) => a - b);

          const validCandidates = sortedCandidates.filter((index) => {
            const candidateRow = localRows[index];
            return Boolean(candidateRow);
          });

          const applyIndices: number[] = [];

          if (columnValues.length === 1) {
            applyIndices.push(...(validCandidates.length ? validCandidates : [startIndex]));
          } else if (validCandidates.length >= columnValues.length) {
            applyIndices.push(...validCandidates.slice(0, columnValues.length));
          } else {
            for (let offset = 0; offset < columnValues.length; offset += 1) {
              const candidateIndex = startIndex + offset;
              const candidateRow = localRows[candidateIndex];
              if (!candidateRow) {
                continue;
              }
              applyIndices.push(candidateIndex);
            }
          }

          applyIndices.forEach((index, position) => {
            if (index < 0 || index >= localRows.length) {
              return;
            }
            const targetRow = localRows[index];
            if (!targetRow) {
              return;
            }
            const value = columnValues[Math.min(position, columnValues.length - 1)] ?? '';
            const model: GridModel = {
              id: targetRow.id,
              rowId: targetRow.id,
              rowIndex: index,
              constructedTableId: targetRow.constructedTableId,
              payload: {
                ...targetRow.payload,
                [targetCell.fieldName]: value,
              },
              rowIdentifier: targetRow.rowIdentifier,
            } as GridModel;
            (model as Record<string, unknown>)[`payload.${targetCell.fieldName}`] = value;
            models.push(model);
          });

          if (models.length) {
            setEditingAuditCell(null);
            const results = await Promise.all(
              models.map((model) =>
                handleModelChange(model, { skipRefresh: true, suppressSuccessToast: true })
              )
            );
            const successCount = results.filter((result) => result?.success).length;
            if (successCount > 0) {
              onDataChange();
              if (successCount > 1) {
                toast.showToast(`${successCount} rows updated`, 'success');
              }
            }
            setActiveAuditCell(targetCell);
            return;
          }
        }
      }

      const parsedRows = parsedRowsRaw.filter((cells) =>
        cells.some((cell) => cell.trim().length > 0)
      );

      if (!parsedRows.length) {
        return;
      }

      const normalizedFieldNames = fields.map((field) => field.name.trim().toLowerCase());
      const firstRow = parsedRows[0];
      const headerMatches =
        firstRow.length > 0 &&
        firstRow.every((value, index) => {
          const targetField = normalizedFieldNames[index];
          if (!targetField) {
            return false;
          }
          return targetField === value.trim().toLowerCase();
        });

      const candidateRows = headerMatches ? parsedRows.slice(1) : parsedRows;
      const meaningfulRows = candidateRows.filter((cells) =>
        cells.some((cell) => cell.trim().length > 0)
      );

      if (!meaningfulRows.length) {
        return;
      }

      const selectedIndices = localRows.reduce<number[]>((acc, row, index) => {
        if (selectedRowIds.has(String(row.id))) {
          acc.push(index);
        }
        return acc;
      }, []);

      const insertionIndex = selectedIndices.length
        ? Math.max(Math.min(Math.min(...selectedIndices), localRows.length), 0)
        : localRows.length;

      const rowsToInsert: EditableConstructedRow[] = meaningfulRows.map((cells, offset) => {
        const payload = fields.reduce<ConstructedRowPayload>((acc, field, columnIndex) => {
          if (READ_ONLY_AUDIT_FIELDS.has(field.name)) {
            acc[field.name] = '';
            return acc;
          }
          const value = columnIndex < cells.length ? cells[columnIndex] : '';
          acc[field.name] = value;
          return acc;
        }, {} as ConstructedRowPayload);

        ensureAuditFields(payload, 'create');

        return {
          id: `${emptyRowIdPrefix}${Date.now()}-${Math.random().toString(16).slice(2, 10)}-${offset}`,
          constructedTableId,
          payload,
          rowIdentifier: null,
          isNew: true,
        };
      });

      if (!rowsToInsert.length) {
        return;
      }

      setLocalRows((prev) => {
        const next = [...prev];
        const safeIndex = Math.min(Math.max(insertionIndex, 0), next.length);
        next.splice(safeIndex, 0, ...rowsToInsert);
        return next;
      });

      const saveResults: SaveRowResult[] = [];
      for (const row of rowsToInsert) {
        const result = await saveRow(toGridRow(row), {
          skipRefresh: true,
          suppressSuccessToast: true,
        });
        if (result) {
          saveResults.push(result);
        }
      }

      const successCount = saveResults.filter((result) => result.success).length;
      if (successCount > 0) {
        onDataChange();
        toast.showToast(
          successCount === 1 ? 'Row pasted successfully' : `${successCount} rows pasted successfully`,
          'success'
        );
      } else {
        toast.showToast('Failed to paste rows', 'error');
      }
    },
    [
      activeAuditCell,
      constructedTableId,
      ensureAuditFields,
      fields,
      handleModelChange,
      localRows,
      onDataChange,
      saveRow,
      selectedRowIds,
      setActiveAuditCell,
      setEditingAuditCell,
      toGridRow,
      toast,
    ]
  );

  const handleDeleteRequest = useCallback(
    (row: GridRowData) => {
      if (row.__isPlaceholder) {
        return;
      }
      const editableRow = toEditableRow(row);
      if (editableRow.isNew ?? String(editableRow.id).startsWith(emptyRowIdPrefix)) {
        setLocalRows((prev) => prev.filter((existing) => String(existing.id) !== String(editableRow.id)));
        setSelectedRowIds((prev) => {
          if (!prev.has(String(editableRow.id))) {
            return prev;
          }
          const next = new Set(prev);
          next.delete(String(editableRow.id));
          return next;
        });
        setUndoState({ rows: [editableRow] });
        return;
      }
      setDeleteTarget(editableRow);
    },
    [toEditableRow]
  );

  const handleConfirmDelete = useCallback(async () => {
    if (!deleteTarget) {
      return;
    }

    const target = deleteTarget;
    const undoRow = toUndoRow(target);

    try {
      setIsDeleting(true);
      if (!String(target.id).startsWith(emptyRowIdPrefix)) {
        await deleteConstructedData(target.id);
      }
      setLocalRows((prev) => prev.filter((row) => String(row.id) !== String(target.id)));
      setSelectedRowIds((prev) => {
        if (!prev.has(String(target.id))) {
          return prev;
        }
        const next = new Set(prev);
        next.delete(String(target.id));
        return next;
      });
      setUndoState({ rows: [undoRow] });
      toast.showToast('Row deleted', 'success');
      onDataChange();
    } catch (rawError) {
      const message = rawError instanceof Error ? rawError.message : 'Failed to delete row';
      toast.showToast(message, 'error');
    } finally {
      setIsDeleting(false);
      setDeleteTarget(null);
    }
  }, [deleteTarget, onDataChange, toast, toUndoRow]);

  const handleCancelDelete = useCallback(() => {
    if (isDeleting) {
      return;
    }
    setDeleteTarget(null);
  }, [isDeleting]);

  const handleBulkDeleteRequest = useCallback(() => {
    if (selectedRowIds.size === 0) {
      return;
    }
    const idSet = new Set(selectedRowIds);
    const targets = localRows.filter((row) => idSet.has(String(row.id)));
    if (!targets.length) {
      return;
    }
    setBulkDeleteTargets(targets.map((row) => ({ ...row, payload: { ...row.payload } })));
  }, [localRows, selectedRowIds]);

  const handleConfirmBulkDelete = useCallback(async () => {
    if (!bulkDeleteTargets.length) {
      return;
    }

    const targets = [...bulkDeleteTargets];
    const idsToRemove = new Set(targets.map((row) => String(row.id)));
    const persistedTargets = targets.filter((row) => !String(row.id).startsWith(emptyRowIdPrefix));
    const unsavedTargets = targets.filter((row) => String(row.id).startsWith(emptyRowIdPrefix));
    const undoRows = targets.map((row) => toUndoRow(row));

    try {
      setIsBulkDeleting(true);
      setLocalRows((prev) => prev.filter((row) => !idsToRemove.has(String(row.id))));
      setSelectedRowIds((prev) => {
        if (!prev.size) {
          return prev;
        }
        const next = new Set(prev);
        idsToRemove.forEach((id) => next.delete(id));
        return next;
      });

      if (persistedTargets.length) {
        for (const row of persistedTargets) {
          await deleteConstructedData(row.id);
        }
        onDataChange();
      }

      const message =
        persistedTargets.length > 1
          ? `${persistedTargets.length} rows deleted`
          : persistedTargets.length === 1
          ? 'Row deleted'
          : 'Rows removed';
      toast.showToast(message, 'success');
      setUndoState({ rows: undoRows });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to delete selected rows';
      toast.showToast(message, 'error');
      if (unsavedTargets.length) {
        setLocalRows((prev) => [
          ...prev,
          ...unsavedTargets.map((row) => ({ ...row, payload: { ...row.payload } })),
        ]);
      }
      onDataChange();
    } finally {
      setIsBulkDeleting(false);
      setBulkDeleteTargets([]);
    }
  }, [bulkDeleteTargets, onDataChange, toast, toUndoRow]);

  const handleCancelBulkDelete = useCallback(() => {
    if (isBulkDeleting) {
      return;
    }
    setBulkDeleteTargets([]);
  }, [isBulkDeleting]);

  const handleAddRow = useCallback(() => {
    setLocalRows((prev) => [...prev, createEmptyRow()]);
  }, [createEmptyRow]);

  const handleUndoRestore = useCallback(async () => {
    if (!undoState || isRestoring) {
      return;
    }

    try {
      setIsRestoring(true);
      const rowsToRestore = undoState.rows;
      if (!rowsToRestore.length) {
        return;
      }

      const unsavedRows = rowsToRestore.filter((row) => String(row.id).startsWith(emptyRowIdPrefix));
      const persistedRows = rowsToRestore.filter(
        (row) => !String(row.id).startsWith(emptyRowIdPrefix)
      );

      if (unsavedRows.length) {
        setLocalRows((prev) => {
          const existingIds = new Set(prev.map((row) => String(row.id)));
          const restored = unsavedRows.map((row) => {
            let id = String(row.id);
            if (existingIds.has(id)) {
              id = `${emptyRowIdPrefix}${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
            }
            return { ...row, id, isNew: true, payload: { ...row.payload } };
          });
          return [...prev, ...restored];
        });
      }

      let restoredPersisted = 0;
      let failedPersisted = 0;

      for (const row of persistedRows) {
        try {
          await createConstructedData({
            constructedTableId: row.constructedTableId,
            payload: row.payload,
            rowIdentifier: row.rowIdentifier ?? undefined,
          });
          restoredPersisted += 1;
        } catch (error) {
          failedPersisted += 1;
        }
      }

      if (restoredPersisted > 0) {
        onDataChange();
      }

      const restoredCount = unsavedRows.length + restoredPersisted;
      if (restoredCount > 0) {
        toast.showToast(
          restoredCount === 1 ? 'Row restored' : `${restoredCount} rows restored`,
          'success'
        );
      }

      if (failedPersisted > 0) {
        toast.showToast(
          failedPersisted === 1 ? 'Failed to restore 1 row' : `Failed to restore ${failedPersisted} rows`,
          'error'
        );
      }
    } finally {
      setUndoState(null);
      setIsRestoring(false);
    }
  }, [undoState, isRestoring, onDataChange, toast]);

  const handleSnackbarClose = useCallback(
    (_: Event | React.SyntheticEvent | undefined, reason?: string) => {
      if (reason === 'clickaway' || isRestoring) {
        return;
      }
      setUndoState(null);
    },
    [isRestoring]
  );

  const selectCellTemplate = useMemo(
    () =>
      Template((props: ColumnDataSchemaModel | ColumnTemplateProp) => {
        if (!isColumnDataSchemaModel(props)) {
          return null;
        }
        const row = props.model as GridRowData | undefined;
        if (!row || row.__isPlaceholder) {
          return null;
        }
        const rowId = String(row.id);
        const checked = selectedRowIds.has(rowId);
        return (
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
            <Checkbox
              size="small"
              checked={checked}
              onClick={(event) => event.stopPropagation()}
              onChange={(event) => handleSelectionToggle(row, event.target.checked)}
            />
          </Box>
        );
      }),
    [handleSelectionToggle, selectedRowIds]
  );

  const deleteCellTemplate = useMemo(
    () =>
      Template((props: ColumnDataSchemaModel | ColumnTemplateProp) => {
        if (!isColumnDataSchemaModel(props)) {
          return null;
        }
        const row = props.model as GridRowData | undefined;
        if (!row || row.__isPlaceholder) {
          return null;
        }
        return (
          <Tooltip title="Delete row" arrow>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                cursor: 'pointer',
              }}
              onClick={(event) => {
                event.stopPropagation();
                handleDeleteRequest(row);
              }}
            >
              <DeleteIcon fontSize="small" />
            </Box>
          </Tooltip>
        );
      }),
    [handleDeleteRequest]
  );

  const projectSelectCellTemplate = useMemo(() => {
    if (!auditFieldPresence.project) {
      return undefined;
    }
    return createSelectCellTemplate(AUDIT_PROJECT_FIELD, projectOptions);
  }, [auditFieldPresence.project, createSelectCellTemplate, projectOptions]);

  const releaseSelectCellTemplate = useMemo(() => {
    if (!auditFieldPresence.release) {
      return undefined;
    }
    return createSelectCellTemplate(AUDIT_RELEASE_FIELD, releaseOptions);
  }, [auditFieldPresence.release, createSelectCellTemplate, releaseOptions]);

  const columns = useMemo<ColumnRegular[]>(() => {
    const base: ColumnRegular[] = [
      {
        prop: '__select__',
        name: '',
        size: 48,
        minSize: 40,
        maxSize: 56,
        readonly: true,
        sortable: false,
        resizable: false,
        pin: 'colPinStart',
        cellTemplate: selectCellTemplate,
      },
      {
        prop: '__actions__',
        name: 'Actions',
        size: 72,
        minSize: 60,
        readonly: true,
        sortable: false,
        resizable: false,
        pin: 'colPinStart',
        cellTemplate: deleteCellTemplate,
      },
    ];

    fields.forEach((field) => {
      const columnProp = `payload.${field.name}` as const;
      const column: ColumnRegular = {
        prop: columnProp,
        name: field.isNullable ? field.name : `${field.name} *`,
        size: 180,
        minSize: 140,
        sortable: true,
        columnType: 'text',
      };

      if (READ_ONLY_AUDIT_FIELDS.has(field.name)) {
        column.readonly = true;
      }

      if (field.name === AUDIT_PROJECT_FIELD && projectSelectCellTemplate) {
        column.cellTemplate = projectSelectCellTemplate;
        column.readonly = false;
      } else if (field.name === AUDIT_RELEASE_FIELD && releaseSelectCellTemplate) {
        column.cellTemplate = releaseSelectCellTemplate;
        column.readonly = false;
      }

      base.push(column);
    });

    return base;
  }, [deleteCellTemplate, fields, projectSelectCellTemplate, releaseSelectCellTemplate, selectCellTemplate]);

  const dataRowCount = localRows.length;
  const selectedRowCount = selectedRowIds.size;
  const isAllSelected = dataRowCount > 0 && selectedRowCount === dataRowCount;
  const isIndeterminateSelection = selectedRowCount > 0 && selectedRowCount < dataRowCount;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 2 }}>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Data Rows
          </Typography>
          <Typography variant="body2" color="text.secondary">
            ({dataRowCount} rows)
          </Typography>
          <FormControlLabel
            sx={{ ml: 1, userSelect: 'none' }}
            control={
              <Checkbox
                size="small"
                indeterminate={isIndeterminateSelection}
                checked={isAllSelected}
                onChange={(event) => handleSelectAllChange(event.target.checked)}
                disabled={dataRowCount === 0}
              />
            }
            label="Select all"
          />
          {dataRowCount === 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ ml: 1 }}>
              No rows yet. Use Add Row to get started.
            </Typography>
          )}
        </Box>
        <Stack direction="row" spacing={1}>
          <Button
            size="small"
            variant="outlined"
            color="error"
            startIcon={<DeleteSweepIcon />}
            onClick={handleBulkDeleteRequest}
            disabled={selectedRowCount === 0 || isSaving || isDeleting || isBulkDeleting}
          >
            Delete Selected
          </Button>
          <Button
            size="small"
            variant="outlined"
            startIcon={<AddIcon />}
            onClick={handleAddRow}
            disabled={isSaving}
          >
            Add Row
          </Button>
        </Stack>
      </Box>

      <Box
        sx={{
          flex: 1,
          width: '100%',
          height: '60vh',
          minHeight: 320,
          maxHeight: '70vh',
          mb: 2,
          borderRadius: 3,
          boxShadow: theme.shadows[1],
          border: `1px solid ${alpha(theme.palette.divider, 0.25)}`,
          backgroundColor: alpha(theme.palette.background.default, 0.4),
          overflow: 'hidden',
          '& revo-grid': {
            width: '100%',
            height: '100%',
            fontFamily: theme.typography.fontFamily,
            '--revo-grid-header-bg': alpha(theme.palette.primary.main, 0.06),
            '--revo-grid-header-color': theme.palette.text.primary,
            '--revo-grid-row-hover': alpha(theme.palette.action.hover, 0.12),
            '--revo-grid-row-selection': alpha(theme.palette.primary.main, 0.08),
            '--revo-grid-border-color': alpha(theme.palette.divider, 0.4),
          },
          }}
          onPasteCapture={handlePaste}
      >
        <RevoGrid
            ref={gridRef}
          theme="default"
          source={gridRows}
          columns={columns}
          rowHeaders={true}
          range={true}
          resize={true}
          autoSizeColumn={true}
          useClipboard={true}
          onAfteredit={(event) => handleAfterEdit(event as unknown as CustomEvent<AfterEditDetail>)}
        />
      </Box>

      {isSaving && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'info.main' }}>
          <CircularProgress size={20} />
          <Typography variant="body2">Saving...</Typography>
        </Box>
      )}

      <Dialog open={Boolean(deleteTarget)} onClose={handleCancelDelete} maxWidth="xs" fullWidth>
        <DialogTitle sx={{ fontWeight: 600 }}>Delete Row</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary">
            This row will be permanently removed. This action cannot be undone.
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleCancelDelete} disabled={isDeleting} variant="outlined" size="small">
            Cancel
          </Button>
          <Button
            onClick={handleConfirmDelete}
            color="error"
            variant="contained"
            size="small"
            disabled={isDeleting}
          >
            {isDeleting ? 'Deleting...' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={bulkDeleteTargets.length > 0} onClose={handleCancelBulkDelete} maxWidth="xs" fullWidth>
        <DialogTitle sx={{ fontWeight: 600 }}>Delete Selected Rows</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary">
            {`You are about to delete ${bulkDeleteTargets.length} selected row${
              bulkDeleteTargets.length === 1 ? '' : 's'
            }. This action cannot be undone.`}
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleCancelBulkDelete} disabled={isBulkDeleting} variant="outlined" size="small">
            Cancel
          </Button>
          <Button
            onClick={handleConfirmBulkDelete}
            color="error"
            variant="contained"
            size="small"
            disabled={isBulkDeleting}
          >
            {isBulkDeleting ? 'Deleting...' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={Boolean(undoState)}
        autoHideDuration={6000}
        onClose={handleSnackbarClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        message={
          undoState
            ? undoState.rows.length === 1
              ? 'Row deleted'
              : `${undoState.rows.length} rows deleted`
            : undefined
        }
        action={
          <Button color="secondary" size="small" onClick={handleUndoRestore} disabled={isRestoring}>
            {isRestoring ? 'Restoring...' : 'Undo'}
          </Button>
        }
      />
    </Box>
  );
};

export default ConstructedDataGridAgGrid;
