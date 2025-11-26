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
  Menu,
  MenuItem,
  Divider,
  ListItemIcon,
  ListItemText,
  Select,
  TextField,
  InputAdornment,
  IconButton,
  Popover,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import SaveIcon from '@mui/icons-material/Save';
import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import SearchIcon from '@mui/icons-material/Search';
import ClearIcon from '@mui/icons-material/Clear';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import ClearAllIcon from '@mui/icons-material/ClearAll';
import ViewColumnIcon from '@mui/icons-material/ViewColumn';
import TableChartIcon from '@mui/icons-material/TableChart';
import { alpha, lighten, useTheme } from '@mui/material/styles';
import { RevoGrid, Template } from '@revolist/react-datagrid';
import type { ColumnDataSchemaModel, ColumnRegular } from '@revolist/revogrid';
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
    __isSelectedRow?: boolean;
  };

type ColumnTemplateContext = {
  prop?: string | number | symbol;
  rowIndex?: number;
  column?: ColumnRegular;
  [key: string]: unknown;
};

type TemplateCallbackProps = ColumnDataSchemaModel | ColumnTemplateContext;

type BeforeUnloadSentinelWindow = Window & {
  __CC_SUPPRESS_BEFOREUNLOAD?: boolean;
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
  props: TemplateCallbackProps
): props is ColumnDataSchemaModel =>
  typeof props === 'object' && props !== null && 'model' in props;

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
  manageSavingState?: boolean;
}

interface SaveRowResult {
  success: boolean;
  error?: string;
  persistedId?: string | number;
  tempId?: string | number;
}

type ValidationHighlight = {
  fieldName?: string | null;
  message: string;
};

type RowValidationState = {
  fields: Record<string, string>;
  rowMessages: string[];
};

const humanizeRuleType = (ruleType?: string): string | undefined => {
  if (!ruleType) {
    return undefined;
  }
  return ruleType
    .split('_')
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ');
};

const buildValidationRuleLabel = (ruleName?: string, ruleType?: string): string | undefined => {
  const friendlyType = humanizeRuleType(ruleType);
    if (ruleName && friendlyType) {
      return `Validation rule "${ruleName}" (${friendlyType})`;
    }
    if (ruleName) {
      return `Validation rule "${ruleName}"`;
  }
  if (friendlyType) {
    return `Validation rule (${friendlyType})`;
  }
  return undefined;
};

const formatValidationMessage = (
  message?: string,
  ruleName?: string,
  ruleType?: string
): string => {
  const trimmedMessage = message?.trim();
  const label = buildValidationRuleLabel(ruleName, ruleType);
    if (label && trimmedMessage) {
      return `${label}: ${trimmedMessage}`;
    }
    if (label) {
      return `${label}: Validation failed.`;
  }
  return trimmedMessage ?? 'Validation failed.';
};

const resolveRowKey = (row: { id?: unknown; rowId?: unknown }): string | undefined => {
  if (row.id !== undefined && row.id !== null) {
    return String(row.id);
  }
  if (row.rowId !== undefined && row.rowId !== null) {
    return String(row.rowId);
  }
  return undefined;
};

const auditDateFormatter = new Intl.DateTimeFormat([], {
  year: 'numeric',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

const formatAuditDateValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '—';
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return '—';
    }
    return auditDateFormatter.format(value);
  }

  if (typeof value === 'number' || typeof value === 'string') {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      const stringValue = String(value).trim();
      return stringValue.length > 0 ? stringValue : '—';
    }
    return auditDateFormatter.format(date);
  }

  return '—';
};

interface Props {
  constructedTableId: string;
  fields: ConstructedField[];
  rows: ConstructedData[];
  onDataChange: () => void;
  onDirtyStateChange?: (hasUnsavedChanges: boolean) => void;
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

const HEADER_HEIGHT_PX = 72;
const DATA_ROW_HEIGHT_PX = 68;

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
  onDirtyStateChange,
}) => {
  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';
  const headerSurfaceColor = useMemo(
    () => theme.palette.primary.dark ?? theme.palette.primary.main,
    [theme]
  );
  const headerTextColor = useMemo(
    () => theme.palette.getContrastText(headerSurfaceColor),
    [headerSurfaceColor, theme]
  );
  const headerGradient = useMemo(
    () =>
      `linear-gradient(180deg, ${alpha(headerSurfaceColor, isDarkMode ? 0.96 : 0.98)} 0%, ${headerSurfaceColor} 100%)`,
    [headerSurfaceColor, isDarkMode]
  );
  const headerBaseColor = useMemo(() => headerSurfaceColor, [headerSurfaceColor]);
  const headerColor = useMemo(() => headerTextColor, [headerTextColor]);
  const headerBorderColor = useMemo(
    () => alpha(headerSurfaceColor, isDarkMode ? 0.7 : 0.6),
    [headerSurfaceColor, isDarkMode]
  );
  const headerHoverColor = useMemo(
    () => lighten(headerSurfaceColor, isDarkMode ? 0.08 : 0.16),
    [headerSurfaceColor, isDarkMode]
  );
  const headerActiveColor = useMemo(
    () => lighten(headerSurfaceColor, isDarkMode ? 0.04 : 0.12),
    [headerSurfaceColor, isDarkMode]
  );
  const cellBackground = useMemo(
    () => alpha(theme.palette.grey[50], isDarkMode ? 0.18 : 1),
    [isDarkMode, theme]
  );
  const zebraBackground = useMemo(
    () => alpha(theme.palette.grey[300], isDarkMode ? 0.6 : 0.92),
    [isDarkMode, theme]
  );
  const cellBorderColor = useMemo(
    () => alpha(theme.palette.grey[300], isDarkMode ? 0.7 : 0.6),
    [isDarkMode, theme]
  );
  const secondaryCellBorderColor = useMemo(
    () => alpha(theme.palette.grey[200], isDarkMode ? 0.5 : 0.4),
    [isDarkMode, theme]
  );
  const rowHoverBackground = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.22 : 0.08),
    [isDarkMode, theme]
  );
  const rowSelectionBackground = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.35 : 0.16),
    [isDarkMode, theme]
  );
  const rowHoverOutline = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.4 : 0.15),
    [isDarkMode, theme]
  );
  const rowSelectionOutline = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.5 : 0.22),
    [isDarkMode, theme]
  );
  const focusRing = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.6 : 0.35),
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
    () => alpha(theme.palette.grey[400], isDarkMode ? 0.6 : 0.45),
    [isDarkMode, theme]
  );
  const gridShadow = useMemo(() => 'none', []);
  const rowHeaderTextColor = useMemo(() => headerTextColor, [headerTextColor]);
  const rowHeaderDividerColor = useMemo(
    () => alpha(headerTextColor, isDarkMode ? 0.25 : 0.18),
    [headerTextColor, isDarkMode]
  );
  const toolbarGradient = useMemo(() => {
    if (isDarkMode) {
      return alpha(theme.palette.background.paper, 0.86);
    }
    return alpha(theme.palette.common.white, 0.96);
  }, [isDarkMode, theme]);
  const toolbarBorderColor = useMemo(
    () => alpha(theme.palette.grey[400], isDarkMode ? 0.5 : 0.35),
    [isDarkMode, theme]
  );
  const toolbarTextColor = useMemo(
    () => (isDarkMode ? alpha(theme.palette.common.white, 0.92) : theme.palette.text.primary),
    [isDarkMode, theme]
  );
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
    [
      auditFieldPresence.createdBy,
      auditFieldPresence.createdDate,
      auditFieldPresence.modifiedBy,
      auditFieldPresence.modifiedDate,
      userDisplayName,
    ]
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
  const [dirtyRowIds, setDirtyRowIds] = useState<Set<string>>(new Set());
  const [validationHighlights, setValidationHighlights] = useState<Record<string, RowValidationState>>({});
  const [columnFilters, setColumnFilters] = useState<Record<string, string>>({});
  const [globalFilter, setGlobalFilter] = useState('');
  const [columnSort, setColumnSort] = useState<{ prop: string; direction: 'asc' | 'desc' } | null>(
    null
  );
  const [columnMenuState, setColumnMenuState] = useState<{
    anchorEl: HTMLElement;
    columnProp: string;
    columnLabel: string;
  } | null>(null);
  const [filterPopoverState, setFilterPopoverState] = useState<{
    anchorEl: HTMLElement;
    columnProp: string;
    columnLabel: string;
  } | null>(null);

  const getValidationMeta = useCallback(
    (row: { id?: unknown; rowId?: unknown }, fieldName?: string | null) => {
      const rowKey = resolveRowKey(row);
      if (!rowKey) {
        return { rowClass: undefined, cellClass: undefined, message: undefined };
      }

      const rowState = validationHighlights[rowKey];
      if (!rowState) {
        return { rowClass: undefined, cellClass: undefined, message: undefined };
      }

      const rowMessages = Array.isArray(rowState.rowMessages) ? rowState.rowMessages : [];
      const cellMessage = fieldName ? rowState.fields?.[fieldName] : undefined;
      const message = cellMessage ?? (rowMessages.length ? rowMessages.join('\n') : undefined);

      return {
        rowClass: rowMessages.length ? 'cc-validation-row' : undefined,
        cellClass: cellMessage ? 'cc-validation-error-cell' : undefined,
        message,
      };
    },
    [validationHighlights]
  );
  const gridRef = useRef<HTMLRevoGridElement | null>(null);

  const handleFilterChange = useCallback((columnProp: string, value: string) => {
    setColumnFilters((prev) => {
      const trimmed = value.trim();
      const exists = Object.prototype.hasOwnProperty.call(prev, columnProp);
      if (!trimmed) {
        if (!exists) {
          return prev;
        }
        const next = { ...prev };
        delete next[columnProp];
        return next;
      }
      const nextValue = value;
      if (exists && prev[columnProp] === nextValue) {
        return prev;
      }
      return { ...prev, [columnProp]: nextValue };
    });
  }, []);

  const handleClearFilters = useCallback(() => {
    setColumnFilters((prev) => {
      if (!Object.keys(prev).length) {
        return prev;
      }
      return {};
    });
  }, []);

  const handleColumnMenuOpen = useCallback(
    (anchorEl: HTMLElement, columnProp: string, columnLabel: string) => {
      setColumnMenuState({ anchorEl, columnProp, columnLabel });
    },
    []
  );

  const handleColumnMenuClose = useCallback(() => {
    setColumnMenuState(null);
  }, []);

  const handleFilterPopoverClose = useCallback(() => {
    setFilterPopoverState(null);
  }, []);

  const handleColumnSortChange = useCallback((prop: string, direction: 'asc' | 'desc') => {
    setColumnSort({ prop, direction });
  }, []);

  const handleClearColumnSort = useCallback(() => {
    setColumnSort(null);
  }, []);

  const handleClearFilterValue = useCallback(
    (columnProp: string) => {
      handleFilterChange(columnProp, '');
    },
    [handleFilterChange]
  );

  const handleGlobalFilterChange = useCallback((value: string) => {
    setGlobalFilter(value);
  }, []);

  const handleClearGlobalFilter = useCallback(() => {
    setGlobalFilter('');
  }, []);

  const applyValidationHighlights = useCallback(
    (rowId: string | number | undefined | null, errors: ValidationHighlight[]) => {
      if (rowId === undefined || rowId === null) {
        return;
      }
      const normalizedId = String(rowId);
      setValidationHighlights((prev) => {
        const fieldMap: Record<string, string> = {};
        const rowMessages: string[] = [];

        errors.forEach((error) => {
          if (!error || !error.message) {
            return;
          }
          if (error.fieldName) {
            const key = String(error.fieldName);
            fieldMap[key] = fieldMap[key]
              ? `${fieldMap[key]}
${error.message}`
              : error.message;
          } else {
            rowMessages.push(error.message);
          }
        });

        if (!Object.keys(fieldMap).length && !rowMessages.length) {
          if (!prev[normalizedId]) {
            return prev;
          }
          const next = { ...prev };
          delete next[normalizedId];
          return next;
        }

        const existing = prev[normalizedId];
        const existingFieldEntries = existing ? Object.entries(existing.fields) : [];
        const nextFieldEntries = Object.entries(fieldMap);
        const hasSameFields =
          existingFieldEntries.length === nextFieldEntries.length &&
          existingFieldEntries.every(([key, value]) => fieldMap[key] === value);
        const hasSameRowMessages = existing
          ? existing.rowMessages.length === rowMessages.length &&
            existing.rowMessages.every((message, index) => message === rowMessages[index])
          : rowMessages.length === 0;

        if (hasSameFields && hasSameRowMessages) {
          return prev;
        }

        return {
          ...prev,
          [normalizedId]: {
            fields: fieldMap,
            rowMessages,
          },
        };
      });
    },
    []
  );

  const clearValidationForRows = useCallback((rowIds: Array<string | number | undefined | null>) => {
    const keys = rowIds
      .filter((rowId): rowId is string | number => rowId !== undefined && rowId !== null)
      .map((rowId) => String(rowId));
    if (!keys.length) {
      return;
    }
    setValidationHighlights((prev) => {
      let changed = false;
      const next = { ...prev };
      keys.forEach((key) => {
        if (next[key]) {
          delete next[key];
          changed = true;
        }
      });
      return changed ? next : prev;
    });
  }, []);

  const clearValidationFields = useCallback((rowId: string | number | undefined | null, fieldNames: string[]) => {
    if (!fieldNames.length || rowId === undefined || rowId === null) {
      return;
    }
    const key = String(rowId);
    setValidationHighlights((prev) => {
      const existing = prev[key];
      if (!existing) {
        return prev;
      }

      let changed = false;
      const nextFields = { ...existing.fields };
      fieldNames.forEach((field) => {
        if (nextFields[field]) {
          delete nextFields[field];
          changed = true;
        }
      });

      let nextRowMessages = existing.rowMessages;
      if (existing.rowMessages.length) {
        nextRowMessages = [];
        changed = true;
      }

      if (!changed) {
        return prev;
      }

      const hasFields = Object.keys(nextFields).length > 0;
      const hasRowMessages = nextRowMessages.length > 0;
      const next = { ...prev };

      if (!hasFields && !hasRowMessages) {
        delete next[key];
      } else {
        next[key] = {
          fields: nextFields,
          rowMessages: nextRowMessages,
        };
      }

      return next;
    });
  }, []);

  useEffect(() => {
    setLocalRows(rows.map(cloneConstructedRow));
    setValidationHighlights((prev) => {
      if (!Object.keys(prev).length) {
        return prev;
      }
      const validIds = new Set(rows.map((row) => String(row.id)));
      const next = { ...prev };
      let changed = false;
      Object.keys(next).forEach((key) => {
        if (!validIds.has(key)) {
          delete next[key];
          changed = true;
        }
      });
      return changed ? next : prev;
    });
  }, [rows]);

  useEffect(() => {
    setDirtyRowIds(new Set());
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
    const validIds = new Set(localRows.map((row) => String(row.id)));
    setDirtyRowIds((prev) => {
      if (!prev.size) {
        return prev;
      }
      const next = new Set<string>();
      let changed = false;
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

  const normalizedFilters = useMemo(() => {
    return Object.entries(columnFilters)
      .map(([key, value]) => [key, (value ?? '').trim().toLowerCase()] as const)
      .filter(([, value]) => value.length > 0);
  }, [columnFilters]);

  const normalizedGlobalFilter = useMemo(() => globalFilter.trim().toLowerCase(), [globalFilter]);

  const hasActiveColumnFilters = normalizedFilters.length > 0;
  const hasGlobalFilter = normalizedGlobalFilter.length > 0;

  const columnFilteredRows = useMemo(() => {
    if (!normalizedFilters.length) {
      return localRows;
    }
    return localRows.filter((row) => {
      return normalizedFilters.every(([prop, needle]) => {
        const fieldName = prop.startsWith('payload.') ? prop.slice('payload.'.length) : prop;
        const value = row.payload?.[fieldName];
        const haystack = value === null || value === undefined ? '' : String(value);
        return haystack.toLowerCase().includes(needle);
      });
    });
  }, [localRows, normalizedFilters]);

  const filteredRows = useMemo(() => {
    if (!hasGlobalFilter) {
      return columnFilteredRows;
    }
    return columnFilteredRows.filter((row) => {
      const candidates: Array<unknown> = [
        row.rowIdentifier,
        row.createdAt,
        row.updatedAt,
        ...fields.map((field) => row.payload?.[field.name]),
      ];
      return candidates.some((value) => {
        if (value === null || value === undefined) {
          return false;
        }
        const haystack = String(value).toLowerCase();
        return haystack.includes(normalizedGlobalFilter);
      });
    });
  }, [columnFilteredRows, fields, hasGlobalFilter, normalizedGlobalFilter]);

  const sortedRows = useMemo(() => {
    if (!columnSort) {
      return filteredRows;
    }
    const { prop, direction } = columnSort;
    const fieldName = prop.startsWith('payload.') ? prop.slice('payload.'.length) : prop;
    const order = direction === 'asc' ? 1 : -1;
    const getValue = (row: EditableConstructedRow) => {
      if (prop.startsWith('payload.')) {
        return row.payload?.[fieldName] ?? '';
      }
      return (row as unknown as Record<string, unknown>)[fieldName];
    };
    return [...filteredRows].sort((a, b) => {
      const aValue = getValue(a);
      const bValue = getValue(b);
      if (aValue === bValue) {
        return 0;
      }
      if (aValue === undefined || aValue === null) {
        return -1 * order;
      }
      if (bValue === undefined || bValue === null) {
        return 1 * order;
      }
      const aNum = Number(aValue);
      const bNum = Number(bValue);
      const bothNumeric = !Number.isNaN(aNum) && !Number.isNaN(bNum);
      if (bothNumeric) {
        return (aNum - bNum) * order;
      }
      return String(aValue).localeCompare(String(bValue)) * order;
    });
  }, [columnSort, filteredRows]);

  const hasActiveFilters = hasActiveColumnFilters || hasGlobalFilter;

  const columnSizeHints = useMemo(() => {
    const hints: Record<string, number> = {};
    if (!fields.length) {
      return hints;
    }

    const approximateCharWidth = 8.2;
    const paddingPx = 40;

    fields.forEach((field) => {
      const columnProp = `payload.${field.name}` as const;
      let maxCharacters = Math.max(6, field.name.length + (field.isNullable ? 2 : 0));

      localRows.forEach((row) => {
        const rawValue = row.payload?.[field.name];
        if (rawValue === null || rawValue === undefined) {
          return;
        }
        const normalized = typeof rawValue === 'string' ? rawValue : String(rawValue);
        const length = normalized.trim().length || normalized.length;
        if (length > maxCharacters) {
          maxCharacters = length;
        }
      });

      const computedWidth = Math.round(maxCharacters * approximateCharWidth + paddingPx);
      const baseMin = field.name === AUDIT_CREATED_DATE_FIELD || field.name === AUDIT_MODIFIED_DATE_FIELD ? 220 : 140;
      const baseMax = field.name === AUDIT_CREATED_DATE_FIELD || field.name === AUDIT_MODIFIED_DATE_FIELD ? 560 : 420;
      hints[columnProp] = Math.min(Math.max(computedWidth, baseMin), baseMax);
    });

    return hints;
  }, [fields, localRows]);

  const gridRows = useMemo<GridRowData[]>(() => {
    const base = sortedRows.map((row) => {
      const gridRow = toGridRow(row);
      gridRow.__isSelectedRow = selectedRowIds.has(String(row.id));
      return gridRow;
    });
    if (!hasActiveFilters && placeholderRow) {
      const placeholderGridRow = toGridRow(placeholderRow, { isPlaceholder: true });
      placeholderGridRow.__isSelectedRow = false;
      base.push(placeholderGridRow);
    }
    return base;
  }, [hasActiveFilters, placeholderRow, selectedRowIds, sortedRows, toGridRow]);

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
        manageSavingState = true,
      } = options;

      if (!rowData || rowData.__isPlaceholder) {
        return { success: false, error: 'Nothing to save', tempId: rowData?.id };
      }

      const targetRowKey = resolveRowKey(rowData);

      try {
        if (manageSavingState) {
          setIsSaving(true);
        }

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
            applyValidationHighlights(
              targetRowKey,
              validation.errors.map<ValidationHighlight>((error) => ({
                fieldName: error.fieldName,
                message: formatValidationMessage(error.message, error.ruleName, error.ruleType),
              }))
            );
            return {
              success: false,
              error: 'Validation failed for row. Fix errors before continuing.',
            };
          }

          clearValidationForRows([targetRowKey]);
        }

        let result: SaveRowResult;

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
          result = { success: true, persistedId: created.id, tempId: rowData.id };
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
          result = { success: true, persistedId: rowData.id };
        }

        clearValidationForRows([
          targetRowKey,
          rowData.id,
          result.tempId,
          result.persistedId,
        ]);

        if (!skipRefresh) {
          onDataChange();
        }

        return result;
      } catch (rawError) {
        const axiosError = rawError as AxiosError<
          | string
          | {
              detail?: string;
              errors?: Array<{
                message?: string;
                fieldName?: string | null;
                ruleName?: string;
                ruleType?: string;
              }>;
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
            const formattedMessages = responseData.errors
              .map((error) =>
                formatValidationMessage(error.message, error.ruleName, error.ruleType)
              )
              .filter(Boolean);
            if (formattedMessages.length) {
              errorMessage = formattedMessages.join('\n');
            }
            applyValidationHighlights(
              targetRowKey,
              responseData.errors
                .map<ValidationHighlight | null>((error) => {
                  if (!error) {
                    return null;
                  }
                  return {
                    fieldName: error.fieldName ?? undefined,
                    message: formatValidationMessage(error.message, error.ruleName, error.ruleType),
                  };
                })
                .filter((error): error is ValidationHighlight => Boolean(error))
            );
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
        return { success: false, error: errorMessage, tempId: rowData.id };
      } finally {
        if (manageSavingState) {
          setIsSaving(false);
        }
      }
    },
    [applyValidationHighlights, clearValidationForRows, constructedTableId, ensureAuditFields, fields, onDataChange, toast]
  );

  const handleModelChange = useCallback(
    async (model: GridModel): Promise<EditableConstructedRow | null> => {
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

      let changedRow: EditableConstructedRow | null = null;
      const changedFieldNames: string[] = [];

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
          changedRow = newRow;
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
            changedFieldNames.push(fieldName);
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
        changedRow = updatedRow;
        return next;
      });

      const rowForDirty = changedRow as EditableConstructedRow | null;

      if (rowForDirty) {
        const targetId = String(rowForDirty.id);
        setDirtyRowIds((prev) => {
          const next = new Set(prev);
          next.add(targetId);
          return next;
        });
      }

      if (rowForDirty && changedFieldNames.length) {
        clearValidationFields(rowForDirty.id, changedFieldNames);
      }

      return rowForDirty;
    },
    [clearValidationFields, constructedTableId, ensureAuditFields, fields]
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
        void Promise.all(modelsToProcess.map((model) => handleModelChange(model)));
        return;
      }

      if (detail.model) {
        void handleModelChange(detail.model);
      }
    },
    [handleModelChange]
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

      await handleModelChange(model);
      return true;
    },
    [handleModelChange]
  );

  const createSelectCellTemplate = useCallback(
    (fieldName: string, options: { label: string; value: string }[]) =>
      Template((props: TemplateCallbackProps) => {
        if (!isColumnDataSchemaModel(props)) {
          return null;
        }
        const row = props.model as GridRowData | undefined;
        if (!row || row.__isPlaceholder) {
          return null;
        }

        const currentValue = (row.payload?.[fieldName] ?? '') as string;
        const validation = getValidationMeta(row, fieldName);
        const className = [
          validation.rowClass,
          validation.cellClass,
          row.__isSelectedRow ? 'cc-row-selected' : undefined,
        ]
          .filter(Boolean)
          .join(' ')
          .trim();
        const validationProps = validation.message
          ? { title: validation.message, 'data-validation-message': validation.message }
          : {};
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
            className={className ? className : undefined}
            sx={{
              display: 'flex',
              alignItems: 'center',
              height: '100%',
              px: 0.25,
            }}
            onClick={() => {
              setActiveAuditCell({ rowId, fieldName });
            }}
            {...validationProps}
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
    [getValidationMeta, handleAuditFieldChange, setActiveAuditCell, setEditingAuditCell]
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
      setSelectedRowIds((prev) => {
        const next = new Set(prev);
        if (!checked) {
          filteredRows.forEach((row) => next.delete(String(row.id)));
          return next;
        }
        filteredRows.forEach((row) => next.add(String(row.id)));
        return next;
      });
    },
    [filteredRows]
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
            const results = await Promise.all(models.map((model) => handleModelChange(model)));
            const changeCount = results.filter(Boolean).length;
            if (changeCount > 0) {
              toast.showToast(
                changeCount === 1
                  ? 'Row updated. Save to apply changes.'
                  : `${changeCount} rows updated. Save to apply changes.`,
                'info'
              );
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

      setDirtyRowIds((prev) => {
        const next = new Set(prev);
        rowsToInsert.forEach((row) => {
          next.add(String(row.id));
        });
        return next;
      });

      toast.showToast(
        rowsToInsert.length === 1
          ? 'Row pasted. Save to apply changes.'
          : `${rowsToInsert.length} rows pasted. Save to apply changes.`,
        'info'
      );
    },
    [
      activeAuditCell,
      constructedTableId,
      ensureAuditFields,
      fields,
      handleModelChange,
      localRows,
      selectedRowIds,
      setActiveAuditCell,
      setEditingAuditCell,
      setDirtyRowIds,
      toast,
    ]
  );

  const handleSaveDirtyRows = useCallback(async () => {
    if (!dirtyRowIds.size) {
      return;
    }

    const dirtyIds = Array.from(dirtyRowIds);
    const rowLookup = new Map<string, EditableConstructedRow>(
      localRows.map((row) => [String(row.id), row])
    );
    const rowsToSave = dirtyIds
      .map((id) => rowLookup.get(id))
      .filter((row): row is EditableConstructedRow => Boolean(row));

    if (!rowsToSave.length) {
      setDirtyRowIds(new Set());
      return;
    }

    setIsSaving(true);
    const clearedIds = new Set<string>();
    const errors: string[] = [];
    let successCount = 0;

    try {
      for (const row of rowsToSave) {
        const originalId = String(row.id);
        const result = await saveRow(toGridRow(row), {
          skipRefresh: true,
          suppressSuccessToast: true,
          manageSavingState: false,
        });

        if (result.success) {
          successCount += 1;
          [originalId, result.tempId, result.persistedId].forEach((id) => {
            if (id !== undefined && id !== null) {
              clearedIds.add(String(id));
            }
          });
        } else if (result.error) {
          errors.push(result.error);
        }
      }

      if (successCount > 0) {
        onDataChange();
        toast.showToast(
          successCount === 1 ? 'Row saved' : `${successCount} rows saved`,
          'success'
        );
      }

      if (errors.length) {
        const summary =
          errors.length === 1
            ? errors[0]
            : `${errors.length} rows failed to save`;
        toast.showToast(summary, 'error');
      }
    } finally {
      setIsSaving(false);
      if (clearedIds.size) {
        setDirtyRowIds((prev) => {
          if (!prev.size) {
            return prev;
          }
          const next = new Set(prev);
          clearedIds.forEach((id) => next.delete(id));
          return next;
        });
      }
    }
  }, [dirtyRowIds, localRows, onDataChange, saveRow, setDirtyRowIds, toGridRow, toast]);

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
        setDirtyRowIds((prev) => {
          if (!prev.has(String(editableRow.id))) {
            return prev;
          }
          const next = new Set(prev);
          next.delete(String(editableRow.id));
          return next;
        });
        clearValidationForRows([editableRow.id]);
        setUndoState({ rows: [editableRow] });
        return;
      }
      setDeleteTarget(editableRow);
    },
    [clearValidationForRows, toEditableRow]
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
      setDirtyRowIds((prev) => {
        if (!prev.has(String(target.id))) {
          return prev;
        }
        const next = new Set(prev);
        next.delete(String(target.id));
        return next;
      });
      clearValidationForRows([target.id]);
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
  }, [clearValidationForRows, deleteTarget, onDataChange, setDirtyRowIds, toast, toUndoRow]);

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
      setDirtyRowIds((prev) => {
        if (!prev.size) {
          return prev;
        }
        const next = new Set(prev);
        idsToRemove.forEach((id) => next.delete(id));
        return next;
      });
      clearValidationForRows(Array.from(idsToRemove));

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
  }, [bulkDeleteTargets, clearValidationForRows, onDataChange, setDirtyRowIds, toast, toUndoRow]);

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
        const restoredRows: EditableConstructedRow[] = [];
        setLocalRows((prev) => {
          const existingIds = new Set(prev.map((row) => String(row.id)));
          const restored = unsavedRows.map((row) => {
            let id = String(row.id);
            const ensureUniqueId = () =>
              `${emptyRowIdPrefix}${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
            while (existingIds.has(id) || restoredRows.some((candidate) => String(candidate.id) === id)) {
              id = ensureUniqueId();
            }
            const restoredRow: EditableConstructedRow = {
              ...row,
              id,
              isNew: true,
              payload: { ...row.payload },
            };
            restoredRows.push(restoredRow);
            return restoredRow;
          });
          return [...prev, ...restored];
        });
        setDirtyRowIds((prev) => {
          const next = new Set(prev);
          restoredRows.forEach((row) => {
            next.add(String(row.id));
          });
          return next;
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
  }, [undoState, isRestoring, onDataChange, setDirtyRowIds, toast]);

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
      Template((props: TemplateCallbackProps) => {
        if (!isColumnDataSchemaModel(props)) {
          return null;
        }
        const row = props.model as GridRowData | undefined;
        if (!row || row.__isPlaceholder) {
          return null;
        }
        const rowId = String(row.id);
        const validation = getValidationMeta(row, null);
        const className = [
          validation.rowClass,
          row.__isSelectedRow ? 'cc-row-selected' : undefined,
        ]
          .filter(Boolean)
          .join(' ')
          .trim();
        const validationProps = validation.message
          ? { title: validation.message, 'data-validation-message': validation.message }
          : {};
        const checked = selectedRowIds.has(rowId);
        return (
          <Box
            className={className || undefined}
            sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}
            {...validationProps}
          >
            <Checkbox
              size="small"
              checked={checked}
              onClick={(event) => event.stopPropagation()}
              onChange={(event) => handleSelectionToggle(row, event.target.checked)}
              sx={{
                p: 0,
                color: isDarkMode
                  ? alpha(theme.palette.common.white, 0.9)
                  : alpha(theme.palette.text.secondary, 0.7),
                '&.Mui-checked': {
                  color: isDarkMode ? theme.palette.common.white : theme.palette.primary.main,
                },
                '&.MuiCheckbox-indeterminate': {
                  color: isDarkMode ? theme.palette.common.white : theme.palette.primary.main,
                },
              }}
            />
          </Box>
        );
      }),
    [getValidationMeta, handleSelectionToggle, isDarkMode, selectedRowIds, theme]
  );

  const deleteCellTemplate = useMemo(
    () =>
      Template((props: TemplateCallbackProps) => {
        if (!isColumnDataSchemaModel(props)) {
          return null;
        }
        const row = props.model as GridRowData | undefined;
        if (!row || row.__isPlaceholder) {
          return null;
        }
        const validation = getValidationMeta(row, null);
        const className = [
          validation.rowClass,
          row.__isSelectedRow ? 'cc-row-selected' : undefined,
        ]
          .filter(Boolean)
          .join(' ')
          .trim();
        const validationProps = validation.message
          ? { title: validation.message, 'data-validation-message': validation.message }
          : {};
        return (
          <Tooltip title="Delete row" arrow>
            <Box
              className={className || undefined}
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
              {...validationProps}
            >
              <DeleteIcon fontSize="small" />
            </Box>
          </Tooltip>
        );
      }),
    [getValidationMeta, handleDeleteRequest]
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

  const unsavedCount = dirtyRowIds.size;
  const hasUnsavedChanges = unsavedCount > 0;
  const totalRowCount = localRows.length;
  const dataRowCount = filteredRows.length;
  const selectedRowCount = selectedRowIds.size;
  const visibleSelectedRowCount = useMemo(() => {
    return filteredRows.reduce((count, row) => {
      if (selectedRowIds.has(String(row.id))) {
        return count + 1;
      }
      return count;
    }, 0);
  }, [filteredRows, selectedRowIds]);
  const isAllSelected = dataRowCount > 0 && visibleSelectedRowCount === dataRowCount;
  const isIndeterminateSelection =
    dataRowCount > 0 && visibleSelectedRowCount > 0 && visibleSelectedRowCount < dataRowCount;
  const rowCountLabel = hasActiveFilters ? `${dataRowCount} of ${totalRowCount}` : `${dataRowCount}`;

  const dirtyStateChangeRef = useRef(onDirtyStateChange);

  useEffect(() => {
    dirtyStateChangeRef.current = onDirtyStateChange;
  });

  useEffect(() => {
    dirtyStateChangeRef.current?.(hasUnsavedChanges);
  }, [hasUnsavedChanges]);

  useEffect(() => {
    return () => {
      dirtyStateChangeRef.current?.(false);
    };
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined' || !hasUnsavedChanges) {
      return undefined;
    }

    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      try {
        // If a global sentinel is set (e.g., user confirmed navigation via in-app modal),
        // allow the unload to proceed without setting returnValue or preventing default.
        // This is checked to avoid triggering the browser native unload confirmation
        // when an in-app flow has already confirmed navigation.
        const suppressBeforeUnload = Boolean(
          (window as BeforeUnloadSentinelWindow).__CC_SUPPRESS_BEFOREUNLOAD
        );
        if (suppressBeforeUnload) {
          return;
        }
      } catch {
        // ignore
      }
      event.preventDefault();
      event.returnValue = '';
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [hasUnsavedChanges]);

  const selectAllHeaderTemplate = useMemo(
    () =>
      Template((templateProps: TemplateCallbackProps) => {
        if (isColumnDataSchemaModel(templateProps)) {
          return null;
        }

        const columnProp = templateProps.prop;

        return (
          <Box
            data-column={columnProp !== undefined ? String(columnProp) : undefined}
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              height: '100%',
              width: '100%',
              px: 0.5,
            }}
            onMouseDown={(event) => event.stopPropagation()}
            onClick={(event) => event.stopPropagation()}
          >
            <Tooltip title="Select all rows" arrow>
              <Checkbox
                size="small"
                indeterminate={isIndeterminateSelection}
                checked={isAllSelected}
                disabled={dataRowCount === 0}
                onClick={(event) => event.stopPropagation()}
                onChange={(event) => handleSelectAllChange(event.target.checked)}
                inputProps={{ 'aria-label': 'Select all rows' }}
                sx={{
                  p: 0,
                  color: headerColor,
                  '& .MuiSvgIcon-root': {
                    color: headerColor,
                  },
                  '&.Mui-checked': {
                    color: headerColor,
                    '& .MuiSvgIcon-root': {
                      color: headerColor,
                    },
                  },
                  '&.MuiCheckbox-indeterminate': {
                    color: headerColor,
                    '& .MuiSvgIcon-root': {
                      color: headerColor,
                    },
                  },
                }}
              />
            </Tooltip>
          </Box>
        );
      }),
    [dataRowCount, handleSelectAllChange, isAllSelected, isDarkMode, isIndeterminateSelection, theme]
  );

  const filterableHeaderTemplate = useCallback(
    (label: string, columnProp: string) =>
      Template((templateProps: TemplateCallbackProps) => {
        if (isColumnDataSchemaModel(templateProps)) {
          return null;
        }

        const filterValue = columnFilters[columnProp] ?? '';
        const isFilterActive = filterValue.length > 0;
        const isSorted = columnSort?.prop === columnProp ? columnSort.direction : undefined;

        const stopPropagation = (event: React.SyntheticEvent) => {
          event.stopPropagation();
        };

        return (
          <Box
            sx={{
              position: 'relative',
              height: '100%',
              width: '100%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              textAlign: 'center',
              px: 1.5,
            }}
          >
            <Typography
              variant="subtitle2"
              sx={{
                fontWeight: 700,
                fontSize: theme.typography.pxToRem(14),
                color: headerColor,
                lineHeight: 1.3,
                textTransform: 'none',
                width: '100%',
                pr: theme.spacing(3),
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {label}
            </Typography>
            {(isFilterActive || isSorted) && (
              <Box
                sx={{
                  position: 'absolute',
                  bottom: theme.spacing(0.6),
                  left: '50%',
                  transform: 'translateX(-50%)',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 0.5,
                  fontSize: theme.typography.pxToRem(11),
                  fontWeight: 600,
                  letterSpacing: 0.2,
                  color: alpha(headerColor, 0.85),
                }}
              >
                {isSorted && (isSorted === 'desc' ? <ArrowDownwardIcon fontSize="inherit" /> : <ArrowUpwardIcon fontSize="inherit" />)}
                {isFilterActive && <FilterAltIcon fontSize="inherit" />}
              </Box>
            )}
            <IconButton
              size="small"
              aria-label={`Open menu for ${label}`}
              onClick={(event) => {
                event.stopPropagation();
                handleColumnMenuOpen(event.currentTarget, columnProp, label);
              }}
              onMouseDown={stopPropagation}
              sx={{
                position: 'absolute',
                top: '50%',
                right: theme.spacing(0.5),
                transform: 'translateY(-50%)',
                color: headerColor,
                opacity: isFilterActive || isSorted ? 1 : 0.6,
                height: 28,
                width: 28,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                p: 0.25,
                borderRadius: 1,
                '&:hover': { opacity: 1, backgroundColor: alpha(headerColor, 0.18) },
              }}
            >
              <MoreVertIcon fontSize="inherit" />
            </IconButton>
          </Box>
        );
      }),
    [
      columnFilters,
      columnSort,
      handleColumnMenuOpen,
      headerColor,
      theme,
    ]
  );

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
        columnTemplate: selectAllHeaderTemplate,
        cellTemplate: selectCellTemplate,
      },
      {
        prop: '__actions__',
        name: '',
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
      const fieldName = field.name;
      const column: ColumnRegular = {
        prop: columnProp,
        name: field.isNullable ? field.name : `${field.name} *`,
        size: 180,
        minSize: 140,
        sortable: true,
        columnType: 'text',
      };

      if (fieldName === AUDIT_CREATED_DATE_FIELD || fieldName === AUDIT_MODIFIED_DATE_FIELD) {
        column.size = 240;
        column.minSize = 200;
        column.maxSize = 340;
        (column as ColumnRegular & { autoSize?: boolean }).autoSize = true;
      }

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

      if (!column.cellTemplate) {
  column.cellTemplate = Template((templateProps: TemplateCallbackProps) => {
          if (!isColumnDataSchemaModel(templateProps)) {
            return null;
          }

          const model = templateProps.model as GridRowData | undefined;
          if (!model || model.__isPlaceholder) {
            return null;
          }

          const validation = getValidationMeta(model, fieldName);
          const className = [
            validation.rowClass,
            validation.cellClass,
            model.__isSelectedRow ? 'cc-row-selected' : undefined,
          ]
            .filter(Boolean)
            .join(' ')
            .trim();
          const message = validation.message;
          const rawValue =
            model.payload?.[fieldName] ?? (model[columnProp as keyof GridRowData] ?? '');
          const displayValue = (() => {
            if (fieldName === AUDIT_CREATED_DATE_FIELD || fieldName === AUDIT_MODIFIED_DATE_FIELD) {
              return formatAuditDateValue(rawValue);
            }
            if (rawValue === null || rawValue === undefined) {
              return '—';
            }
            const stringValue = String(rawValue).trim();
            return stringValue.length > 0 ? stringValue : '—';
          })();

          return (
            <Box
              className={className ? className : undefined}
              sx={{
                width: '100%',
                height: '100%',
                display: 'flex',
                alignItems: 'center',
                px: 1,
                boxSizing: 'border-box',
                overflow: 'hidden',
              }}
              {...(message ? { title: message, 'data-validation-message': message } : {})}
            >
              <Typography
                component="span"
                variant="body2"
                sx={{
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                  width: '100%',
                }}
              >
                {displayValue}
              </Typography>
            </Box>
          );
        });
      }

      const sizeHint = columnSizeHints[columnProp];
      if (sizeHint) {
        const minSize = column.minSize ?? 140;
        column.size = Math.max(sizeHint, minSize);
        column.minSize = Math.min(minSize, column.size);
      }

      column.columnTemplate = filterableHeaderTemplate(column.name ?? field.name, columnProp);
      base.push(column);
    });

    return base;
  }, [
    deleteCellTemplate,
    fields,
    columnSizeHints,
    filterableHeaderTemplate,
    getValidationMeta,
    projectSelectCellTemplate,
    releaseSelectCellTemplate,
    selectAllHeaderTemplate,
    selectCellTemplate,
  ]);

  useEffect(() => {
    const gridEl = gridRef.current;
    if (!gridEl) {
      return;
    }
    const autoSizer = (gridEl as unknown as { autoSizeColumnAll?: () => void }).autoSizeColumnAll;
    if (typeof autoSizer !== 'function') {
      return;
    }
    const frame = requestAnimationFrame(() => {
      autoSizer.call(gridEl);
    });
    return () => cancelAnimationFrame(frame);
  }, [columns, gridRows]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 2 }}>
      <Box
        sx={{
          display: 'flex',
          gap: 1,
          alignItems: 'center',
          justifyContent: 'space-between',
          px: 2,
          py: 1.5,
          borderRadius: 2,
          backgroundImage: toolbarGradient,
          border: `1px solid ${toolbarBorderColor}`,
          boxShadow: 'none',
          color: toolbarTextColor,
        }}
      >
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexWrap: 'wrap' }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'inherit' }}>
            Data Rows
          </Typography>
          <Typography variant="body2" sx={{ color: alpha(toolbarTextColor, 0.82), fontWeight: 500 }}>
            ({rowCountLabel} rows)
          </Typography>
          {dataRowCount === 0 && (
            <Typography variant="body2" sx={{ ml: 1, color: alpha(toolbarTextColor, 0.82) }}>
              {totalRowCount === 0
                ? 'No rows yet. Use Add Row to get started.'
                : hasActiveFilters
                  ? 'No rows match the current filters.'
                  : 'No rows yet. Use Add Row to get started.'}
            </Typography>
          )}
        </Box>
        <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" justifyContent="flex-end">
          <TextField
            value={globalFilter}
            onChange={(event) => handleGlobalFilterChange(event.target.value)}
            placeholder="Search rows"
            size="small"
            variant="outlined"
            aria-label="Search constructed data rows"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start" sx={{ color: alpha(toolbarTextColor, 0.8) }}>
                  <SearchIcon fontSize="small" />
                </InputAdornment>
              ),
              endAdornment: hasGlobalFilter ? (
                <InputAdornment position="end">
                  <IconButton size="small" onClick={handleClearGlobalFilter} edge="end">
                    <ClearIcon fontSize="small" />
                  </IconButton>
                </InputAdornment>
              ) : undefined,
            }}
            sx={{
              minWidth: 260,
              flexShrink: 0,
              '& .MuiOutlinedInput-root': {
                borderRadius: 2,
                backgroundColor: alpha(toolbarTextColor, 0.08),
                color: toolbarTextColor,
                height: 38,
                '&:hover .MuiOutlinedInput-notchedOutline': {
                  borderColor: alpha(toolbarTextColor, 0.6),
                },
                '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
                  borderColor: alpha(toolbarTextColor, 0.85),
                  borderWidth: 1,
                },
              },
              '& .MuiOutlinedInput-notchedOutline': {
                borderColor: alpha(toolbarTextColor, 0.3),
              },
              '& .MuiInputBase-input': {
                fontSize: theme.typography.pxToRem(13.5),
                py: 0.5,
              },
            }}
          />
          {hasActiveFilters && (
            <Button
              size="small"
              variant="text"
              color="inherit"
              onClick={() => {
                handleClearFilters();
                handleClearGlobalFilter();
              }}
              sx={{ textTransform: 'none', fontWeight: 500 }}
            >
              Clear Filters
            </Button>
          )}
          <Button
            size="small"
            variant="contained"
            color="primary"
            startIcon={<SaveIcon />}
            onClick={handleSaveDirtyRows}
            disabled={!hasUnsavedChanges || isSaving || isDeleting || isBulkDeleting}
          >
            {hasUnsavedChanges ? `Save Changes (${unsavedCount})` : 'Save Changes'}
          </Button>
          {hasUnsavedChanges && (
            <Typography variant="body2" color="warning.main" sx={{ fontWeight: 500 }}>
              {unsavedCount === 1 ? '1 unsaved change' : `${unsavedCount} unsaved changes`}
            </Typography>
          )}
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

      <Menu
        anchorEl={columnMenuState?.anchorEl ?? null}
        open={Boolean(columnMenuState)}
        onClose={handleColumnMenuClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        transformOrigin={{ vertical: 'top', horizontal: 'center' }}
        keepMounted
      >
        <MenuItem
          onClick={() => {
            if (columnMenuState) {
              handleColumnSortChange(columnMenuState.columnProp, 'asc');
            }
            handleColumnMenuClose();
          }}
        >
          <ListItemIcon>
            <ArrowUpwardIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText primary="Sort by ASC" />
        </MenuItem>
        <MenuItem
          onClick={() => {
            if (columnMenuState) {
              handleColumnSortChange(columnMenuState.columnProp, 'desc');
            }
            handleColumnMenuClose();
          }}
        >
          <ListItemIcon>
            <ArrowDownwardIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText primary="Sort by DESC" />
        </MenuItem>
        {columnMenuState && columnSort?.prop === columnMenuState.columnProp && (
          <MenuItem
            onClick={() => {
              handleClearColumnSort();
              handleColumnMenuClose();
            }}
          >
            <ListItemIcon>
              <ClearAllIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText primary="Clear sort" />
          </MenuItem>
        )}
        <Divider sx={{ my: 0.5 }} />
        <MenuItem
          onClick={() => {
            if (columnMenuState?.anchorEl) {
              setFilterPopoverState({
                anchorEl: columnMenuState.anchorEl,
                columnProp: columnMenuState.columnProp,
                columnLabel: columnMenuState.columnLabel,
              });
            }
            handleColumnMenuClose();
          }}
        >
          <ListItemIcon>
            <FilterAltIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText primary="Filter" />
        </MenuItem>
        <Divider sx={{ my: 0.5 }} />
        <MenuItem disabled>
          <ListItemIcon>
            <ViewColumnIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText primary="Hide column" />
        </MenuItem>
        <MenuItem disabled>
          <ListItemIcon>
            <TableChartIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText primary="Manage columns" />
        </MenuItem>
      </Menu>

      <Popover
        open={Boolean(filterPopoverState)}
        anchorEl={filterPopoverState?.anchorEl ?? null}
        onClose={handleFilterPopoverClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
        PaperProps={{
          sx: {
            borderRadius: 2,
            border: `1px solid ${alpha(toolbarBorderColor, 0.7)}`,
            boxShadow: theme.shadows[4],
          },
        }}
      >
        {filterPopoverState && (
          <Box sx={{ p: 2, width: 280, display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
              {`Filter ${filterPopoverState.columnLabel}`}
            </Typography>
            <TextField
              autoFocus
              size="small"
              value={columnFilters[filterPopoverState.columnProp] ?? ''}
              onChange={(event) => handleFilterChange(filterPopoverState.columnProp, event.target.value)}
              placeholder="Enter value"
              variant="outlined"
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <FilterAltIcon fontSize="small" sx={{ color: alpha(toolbarTextColor, 0.6) }} />
                  </InputAdornment>
                ),
              }}
            />
            <Stack direction="row" justifyContent="flex-end" spacing={1.5}>
              <Button
                size="small"
                color="inherit"
                onClick={() => handleClearFilterValue(filterPopoverState.columnProp)}
              >
                Clear
              </Button>
              <Button size="small" variant="contained" onClick={handleFilterPopoverClose}>
                Apply
              </Button>
            </Stack>
          </Box>
        )}
      </Popover>

      <Box
        sx={{
          flex: 1,
          width: '100%',
          height: '60vh',
          minHeight: 320,
          maxHeight: '70vh',
          mb: 2,
          borderRadius: 2,
          boxShadow: gridShadow,
          border: `1px solid ${gridBorderColor}`,
          background: gridBackground,
          overflow: 'hidden',
          '& revo-grid': {
            width: '100%',
            height: '100%',
            fontFamily: theme.typography.fontFamily,
            color: isDarkMode ? theme.palette.common.white : theme.palette.text.primary,
            fontSize: theme.typography.pxToRem(16),
            '--revo-grid-header-bg': headerBaseColor,
            '--revo-grid-header-color': headerColor,
            '--revo-grid-row-header-bg': headerBaseColor,
            '--revo-grid-row-header-color': headerColor,
            '--revo-grid-row-hover': rowHoverBackground,
            '--revo-grid-row-selection': rowSelectionBackground,
            '--revo-grid-border-color': alpha(theme.palette.divider, isDarkMode ? 0.45 : 0.24),
            '--revo-grid-primary': theme.palette.primary.main,
            '--revo-grid-text': isDarkMode
              ? alpha(theme.palette.common.white, 0.95)
              : theme.palette.text.primary,
            '--revo-grid-cell-border': cellBorderColor,
            '--revo-grid-header-height': `${HEADER_HEIGHT_PX}px`,
            '--revo-grid-row-size': `${DATA_ROW_HEIGHT_PX}px`,
          },
          '& revo-grid revogr-header, & revo-grid .rowHeaders': {
            backgroundColor: headerBaseColor,
            backgroundImage: headerGradient,
            color: headerColor,
            minHeight: `${HEADER_HEIGHT_PX}px`,
            height: `${HEADER_HEIGHT_PX}px`,
            borderBottom: 'none',
          },
          '& revo-grid revogr-header revogr-data': {
            borderBottom: 'none',
          },
          '& revo-grid revogr-header revogr-data .rgRow': {
            borderBottom: 'none',
          },
          '& revo-grid revogr-header revogr-data .rgRow .rgCell': {
            borderBottom: 'none',
            borderTop: 'none',
            borderRight: 'none',
            borderLeft: 'none',
            boxShadow: 'none',
          },
          '& revo-grid[theme="default"] revogr-header .header-rgRow, & revo-grid:not([theme]) revogr-header .header-rgRow': {
            height: 0,
            minHeight: 0,
            boxShadow: 'none',
            border: 'none',
          },
          '& revo-grid revogr-header .rgHeaderCell': {
            borderBottom: 'none',
            borderRight: 'none',
            boxShadow: 'none',
            minHeight: `${HEADER_HEIGHT_PX}px`,
            height: '100%',
            padding: theme.spacing(1, 1.25),
            alignItems: 'stretch',
            boxSizing: 'border-box',
          },
          '& revo-grid .rgHeader .rgCell': {
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'center',
            color: headerColor,
            textTransform: 'none',
            letterSpacing: 0.2,
            fontWeight: 700,
            fontSize: theme.typography.pxToRem(15),
            gap: theme.spacing(0.5),
            minHeight: `${HEADER_HEIGHT_PX - 8}px`,
            textAlign: 'center',
          },
          '& revo-grid .rowHeaders revogr-data .rgRow .rgCell': {
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: rowHeaderTextColor,
            backgroundColor: headerBaseColor,
            backgroundImage: headerGradient,
            borderBottom: `1px solid ${rowHeaderDividerColor}`,
            borderRight: `1px solid ${rowHeaderDividerColor}`,
            fontWeight: 600,
            transition: theme.transitions.create(['background-color', 'box-shadow'], {
              duration: theme.transitions.duration.shortest,
            }),
          },
          '& revo-grid .rowHeaders revogr-data .rgRow:nth-of-type(even) .rgCell': {
            backgroundColor: headerBaseColor,
            backgroundImage: headerGradient,
          },
          '& revo-grid .rowHeaders revogr-data .rgRow:hover .rgCell': {
            backgroundColor: headerHoverColor,
            backgroundImage: 'none',
            color: rowHeaderTextColor,
            boxShadow: `inset 0 0 0 1px ${rowHeaderDividerColor}`,
          },
          '& revo-grid .rowHeaders revogr-data .rgRow[aria-selected="true"] .rgCell': {
            backgroundColor: headerActiveColor,
            backgroundImage: 'none',
            color: rowHeaderTextColor,
            boxShadow: `inset 0 0 0 1px ${rowHeaderDividerColor}`,
          },
          '& revo-grid revogr-data .rgRow': {
            minHeight: DATA_ROW_HEIGHT_PX,
            maxHeight: DATA_ROW_HEIGHT_PX,
          },
          '& revo-grid revogr-data .rgRow:nth-of-type(even) .rgCell': {
            backgroundColor: zebraBackground,
          },
          '& revo-grid revogr-data .rgRow:nth-of-type(odd) .rgCell': {
            backgroundColor: cellBackground,
          },
          '& revo-grid revogr-data .rgRow:hover': {
            backgroundColor: rowHoverBackground,
            boxShadow: `inset 0 0 0 1px ${rowHoverOutline}`,
          },
          '& revo-grid revogr-data .rgRow:hover .rgCell': {
            backgroundColor: 'transparent',
          },
          '& revo-grid revogr-data .rgRow[aria-selected="true"]': {
            backgroundColor: rowSelectionBackground,
            boxShadow: `inset 0 0 0 1px ${rowSelectionOutline}`,
          },
          '& revo-grid revogr-data .rgRow[aria-selected="true"] .rgCell': {
            backgroundColor: 'transparent',
          },
          '& revo-grid revogr-data .rgRow[aria-selected="true"]:hover': {
            backgroundColor: alpha(rowSelectionBackground, isDarkMode ? 0.95 : 0.85),
            boxShadow: `inset 0 0 0 1px ${rowSelectionOutline}`,
          },
          '& revo-grid revogr-data .rgRow[aria-selected="true"]:hover .rgCell': {
            backgroundColor: 'transparent',
          },
          '& revo-grid revogr-data .rgCell': {
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-start',
            color: isDarkMode
              ? alpha(theme.palette.common.white, 0.95)
              : theme.palette.text.primary,
            borderBottom: `1px solid ${cellBorderColor}`,
            borderRight: `1px solid ${secondaryCellBorderColor}`,
            padding: theme.spacing(1.1, 1.5),
            fontSize: theme.typography.pxToRem(15),
            lineHeight: 1.5,
            backgroundColor: 'transparent',
            transition: theme.transitions.create(['background-color', 'box-shadow'], {
              duration: theme.transitions.duration.shortest,
            }),
          },
          '& revo-grid revogr-data .rgRow.cc-placeholder-row .rgCell': {
            backgroundColor: alpha(theme.palette.action.disabledBackground, 0.12),
            fontStyle: 'italic',
          },
          '& revo-grid revogr-data .rgCell input, & revo-grid revogr-data .rgCell .MuiSelect-select, & revo-grid revogr-data .rgCell .MuiInputBase-input': {
            color: isDarkMode ? theme.palette.common.white : undefined,
          },
          '& revo-grid .focused-cell, & revo-grid .selection-range': {
            boxShadow: `0 0 0 2px ${focusRing}`,
            borderColor: focusRing,
          },
          '& .cc-validation-row': {
            backgroundColor: alpha(theme.palette.error.light, isDarkMode ? 0.2 : 0.12),
          },
          '& .cc-row-selected': {
            backgroundColor: rowSelectionBackground,
            boxShadow: `inset 0 0 0 1px ${rowSelectionOutline}`,
          },
          '& .cc-validation-error-cell': {
            position: 'relative',
            backgroundColor: alpha(theme.palette.error.light, isDarkMode ? 0.3 : 0.2),
            boxShadow: `inset 0 0 0 1px ${alpha(theme.palette.error.main, 0.55)}`,
            color: isDarkMode ? theme.palette.common.white : theme.palette.error.dark,
          },
          '& .cc-validation-error-cell:hover': {
            backgroundColor: alpha(theme.palette.error.light, isDarkMode ? 0.38 : 0.26),
          },
          '& .cc-validation-error-cell input': {
            color: isDarkMode ? theme.palette.common.white : theme.palette.error.dark,
          },
          '& .cc-validation-error-cell .MuiSelect-select': {
            color: isDarkMode ? theme.palette.common.white : theme.palette.error.dark,
          },
          '& .cc-validation-error-cell::after': {
            content: '""',
            position: 'absolute',
            inset: 0,
            borderLeft: `3px solid ${theme.palette.error.main}`,
            pointerEvents: 'none',
          },
          '& .cc-validation-row.cc-validation-error-cell': {
            backgroundColor: alpha(theme.palette.error.main, isDarkMode ? 0.32 : 0.26),
          },
        }}
        onPasteCapture={handlePaste}
      >
        <RevoGrid
          ref={gridRef}
          theme="default"
          source={gridRows}
          columns={columns}
          rowHeaders={false}
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
