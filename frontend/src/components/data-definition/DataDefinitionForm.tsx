import { FormEvent, useEffect, useMemo, useState } from 'react';
import { AxiosError } from 'axios';
import { LoadingButton } from '@mui/lab';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  IconButton,
  Stack,
  Switch,
  TextField,
  Typography
} from '@mui/material';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import PostAddIcon from '@mui/icons-material/PostAdd';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import PreviewIcon from '@mui/icons-material/Preview';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
import {
  DndContext,
  DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors
} from '@dnd-kit/core';
import {
  SortableContext,
  arrayMove,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

import { DataDefinition, DataDefinitionTableInput, Table, TableInput, Field, ConnectionTablePreview } from '../../types/data';
import { createTable, updateTable, createField, fetchTablePreview } from '../../services/tableService';
import { fetchAvailableSourceTables, AvailableSourceTable, SourceTableColumn } from '../../services/dataDefinitionService';
import { useToast } from '../../hooks/useToast';
import ConfirmDialog from '../common/ConfirmDialog';
import CreateTableDialog from './CreateTableDialog';
import AddExistingSourceTableDialog from './AddExistingSourceTableDialog';
import ConnectionDataPreviewDialog from '../system-connection/ConnectionDataPreviewDialog';

interface DataDefinitionFormProps {
  open: boolean;
  mode: 'create' | 'edit';
  loading?: boolean;
  onClose: () => void;
  onSubmit: (payload: { description: string | null; tables: DataDefinitionTableInput[] }) => void;
  initialDefinition?: DataDefinition | null;
  dataObjectId?: string;
  tables: Table[];
  fields: Field[];
  systemId: string;
  onMetadataRefresh?: () => Promise<void>;
}

type FieldRow = {
  id: string;
  fieldId: string;
  fieldName: string;
  notes: string;
  isUnique: boolean;
};

type TableRow = {
  id: string;
  tableId: string;
  alias: string;
  description: string;
  loadOrder: string;
  isConstruction: boolean;
  fields: FieldRow[];
};

type SanitizedTablePayload = {
  name: string;
  physicalName: string;
  schemaName: string | null;
  description: string | null;
  tableType: string | null;
  status: string;
};

type Snapshot = {
  description: string;
  tables: Array<{
    tableId: string;
    alias: string;
    description: string;
    loadOrder: string;
    isConstruction: boolean;
    fields: Array<{ fieldId: string; fieldName: string; notes: string; isUnique: boolean }>;
  }>;
};

const generateId = () => Math.random().toString(36).slice(2, 11);

const getNextLoadOrderValue = (rows: Array<{ loadOrder: string }>): string => {
  const numericValues = rows
    .map((row) => Number(row.loadOrder))
    .filter((value) => Number.isInteger(value) && value > 0);
  const next = numericValues.length ? Math.max(...numericValues) + 1 : 1;
  return next.toString();
};

const toSourceTableKey = (schemaName?: string | null, tableName?: string | null) => {
  if (!tableName) {
    return null;
  }
  return `${(schemaName ?? '').toLowerCase()}::${tableName.toLowerCase()}`;
};

const buildSourceTableKeyList = (tableList: AvailableSourceTable[]) => {
  const keys = new Set<string>();
  tableList.forEach((table) => {
    const key = toSourceTableKey(table.schemaName, table.tableName);
    if (key) {
      keys.add(key);
    }
  });
  return Array.from(keys);
};

const CONSTRUCTION_SCHEMA = 'construction_data';

const getErrorMessage = (error: unknown, fallback: string) => {
  if (error instanceof AxiosError) {
    return error.response?.data?.detail ?? error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return fallback;
};

const sanitizeOptionalString = (value: string) => {
  const trimmed = value.trim();
  return trimmed === '' ? null : trimmed;
};

const snapshotFromDefinition = (definition?: DataDefinition | null): Snapshot => ({
  description: definition?.description ?? '',
  tables:
    definition?.tables.map((table, index) => ({
      tableId: table.tableId,
      alias: table.alias ?? '',
      description: table.description ?? '',
      loadOrder: (table.loadOrder ?? index + 1).toString(),
      isConstruction: table.isConstruction ?? false,
      fields: table.fields.map((field) => ({
        fieldId: field.fieldId,
        fieldName: field.field?.name ?? '',
        notes: field.notes ?? '',
        isUnique: field.isUnique ?? false
      }))
    })) ?? []
});

const normalizeSnapshot = (snapshot: Snapshot) => ({
  description: snapshot.description.trim(),
  tables: snapshot.tables.map((table) => ({
    tableId: table.tableId,
    alias: table.alias.trim(),
    description: table.description.trim(),
    loadOrder: table.loadOrder.trim(),
    isConstruction: table.isConstruction,
    fields: table.fields.map((field) => ({
      fieldId: field.fieldId,
      fieldName: field.fieldName.trim(),
      notes: field.notes.trim(),
      isUnique: field.isUnique
    }))
  }))
});

const buildRows = (tables: Snapshot['tables']): TableRow[] =>
  tables.map((table) => ({
    id: generateId(),
    tableId: table.tableId,
    alias: table.alias,
    description: table.description,
    loadOrder: table.loadOrder,
    isConstruction: table.isConstruction,
    fields: table.fields.map((field) => ({
      id: generateId(),
      fieldId: field.fieldId,
      fieldName: field.fieldName,
      notes: field.notes,
      isUnique: field.isUnique
    }))
  }));

type FieldDisplayItemProps = {
  fieldRow: FieldRow;
  field: Field | undefined;
};

const fieldItemStyles = {
  border: '1px solid',
  borderColor: 'divider',
  borderRadius: 1,
  px: 1.5,
  py: 1,
  display: 'flex',
  alignItems: 'center',
  gap: 1.5,
  backgroundColor: 'background.paper'
} as const;

const FieldDisplayItem = ({ fieldRow, field }: FieldDisplayItemProps) => {
  const displayName = field?.name ?? (fieldRow.fieldName ? fieldRow.fieldName : fieldRow.fieldId);
  const trimmedDescription = field?.description?.trim() ?? '';
  const trimmedNotes = fieldRow.notes.trim();

  return (
    <Box sx={fieldItemStyles}>
      <Box sx={{ display: 'flex', alignItems: 'center', color: 'text.disabled' }}>
        <DragIndicatorIcon fontSize="small" />
      </Box>
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Stack direction="row" spacing={1} alignItems="center" sx={{ flexWrap: 'wrap' }}>
          <Typography variant="body2" sx={{ fontWeight: 500, wordBreak: 'break-word' }}>
            {displayName}
          </Typography>
          {fieldRow.isUnique && (
            <Chip size="small" color="primary" label="Unique" sx={{ height: 20, fontSize: 11 }} />
          )}
        </Stack>
        {trimmedDescription && (
          <Typography variant="caption" color="text.secondary" sx={{ wordBreak: 'break-word' }}>
            {trimmedDescription}
          </Typography>
        )}
        {trimmedNotes && (
          <Typography variant="caption" color="text.secondary" sx={{ wordBreak: 'break-word' }}>
            Notes: {trimmedNotes}
          </Typography>
        )}
      </Box>
    </Box>
  );
};

type SortableFieldItemProps = {
  fieldRow: FieldRow;
  field: Field | undefined;
  onDelete: () => void;
};

const SortableFieldItem = ({ fieldRow, field, onDelete }: SortableFieldItemProps) => {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: fieldRow.id
  });
  const style = {
    transform: CSS.Transform.toString(transform),
    transition
  };

  const displayName = field?.name ?? (fieldRow.fieldName ? fieldRow.fieldName : fieldRow.fieldId);
  const trimmedDescription = field?.description?.trim() ?? '';
  const trimmedNotes = fieldRow.notes.trim();

  return (
    <Box
      ref={setNodeRef}
      style={style}
      sx={{
        ...fieldItemStyles,
        gap: 1.5,
        backgroundColor: isDragging ? 'action.hover' : fieldItemStyles.backgroundColor,
        transition: 'background-color 120ms ease, box-shadow 120ms ease',
        boxShadow: isDragging ? 2 : 0
      }}
    >
      <IconButton
        size="small"
        aria-label="Reorder field"
        {...attributes}
        {...listeners}
        sx={{ cursor: 'grab' }}
      >
        <DragIndicatorIcon fontSize="small" />
      </IconButton>
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Stack direction="row" spacing={1} alignItems="center" sx={{ flexWrap: 'wrap' }}>
          <Typography variant="body2" sx={{ fontWeight: 500, wordBreak: 'break-word' }}>
            {displayName}
          </Typography>
          {fieldRow.isUnique && (
            <Chip size="small" color="primary" label="Unique" sx={{ height: 20, fontSize: 11 }} />
          )}
        </Stack>
        {trimmedDescription && (
          <Typography variant="caption" color="text.secondary" sx={{ wordBreak: 'break-word' }}>
            {trimmedDescription}
          </Typography>
        )}
        {trimmedNotes && (
          <Typography variant="caption" color="text.secondary" sx={{ wordBreak: 'break-word' }}>
            Notes: {trimmedNotes}
          </Typography>
        )}
      </Box>
      <IconButton size="small" aria-label="Remove field" onClick={onDelete}>
        <DeleteOutlineIcon fontSize="small" />
      </IconButton>
    </Box>
  );
};

type TableFieldsEditorProps = {
  tableRowId: string;
  fields: FieldRow[];
  fieldMap: Map<string, Field>;
  canEdit: boolean;
  onDelete: (fieldRowId: string) => void;
  onReorder: (tableRowId: string, activeId: string, overId: string) => void;
};

const TableFieldsEditor = ({
  tableRowId,
  fields,
  fieldMap,
  canEdit,
  onDelete,
  onReorder
}: TableFieldsEditorProps) => {
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 6 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  );

  if (!fields.length) {
    return null;
  }

  if (!canEdit) {
    return (
      <Stack spacing={1}>
        {fields.map((fieldRow) => (
          <FieldDisplayItem
            key={fieldRow.id}
            fieldRow={fieldRow}
            field={fieldMap.get(fieldRow.fieldId)}
          />
        ))}
      </Stack>
    );
  }

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) {
      return;
    }
    onReorder(tableRowId, String(active.id), String(over.id));
  };

  return (
    <DndContext
      sensors={sensors}
      collisionDetection={closestCenter}
      onDragEnd={handleDragEnd}
    >
      <SortableContext
        items={fields.map((field) => field.id)}
        strategy={verticalListSortingStrategy}
      >
        <Stack spacing={1}>
          {fields.map((fieldRow) => (
            <SortableFieldItem
              key={fieldRow.id}
              fieldRow={fieldRow}
              field={fieldMap.get(fieldRow.fieldId)}
              onDelete={() => onDelete(fieldRow.id)}
            />
          ))}
        </Stack>
      </SortableContext>
    </DndContext>
  );
};

const DataDefinitionForm = ({
  open,
  mode,
  loading = false,
  onClose,
  onSubmit,
  initialDefinition,
  dataObjectId,
  tables,
  fields,
  systemId,
  onMetadataRefresh
}: DataDefinitionFormProps) => {
  const toast = useToast();
  const initialSnapshot = useMemo(() => snapshotFromDefinition(initialDefinition), [initialDefinition]);
  const normalizedInitial = useMemo(() => normalizeSnapshot(initialSnapshot), [initialSnapshot]);

  const [description, setDescription] = useState<string>(initialSnapshot.description);
  const [tableRows, setTableRows] = useState<TableRow[]>(buildRows(initialSnapshot.tables));
  const [formError, setFormError] = useState<string | null>(null);
  const [localTables, setLocalTables] = useState<Table[]>([]);
  const [tableDialogOpen, setTableDialogOpen] = useState(false);
  const [tableDialogMode, setTableDialogMode] = useState<'create' | 'edit'>('create');
  const [editingTable, setEditingTable] = useState<Table | null>(null);
  const [tableDialogLoading, setTableDialogLoading] = useState(false);
  const [existingTablePrompt, setExistingTablePrompt] = useState<{
    table: Table;
    sanitized: SanitizedTablePayload;
  } | null>(null);
  const [sourceTableDialogOpen, setSourceTableDialogOpen] = useState(false);
  const [availableSourceTables, setAvailableSourceTables] = useState<AvailableSourceTable[]>([]);
  const [sourceTableKeys, setSourceTableKeys] = useState<string[]>([]);
  const [sourceTableDialogLoading, setSourceTableDialogLoading] = useState(false);
  const [sourceTableDialogError, setSourceTableDialogError] = useState<string | null>(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewTableId, setPreviewTableId] = useState<string | null>(null);
  const [previewTableName, setPreviewTableName] = useState<string>('');
  const [previewSchemaName, setPreviewSchemaName] = useState<string | null>(null);
  const [previewData, setPreviewData] = useState<ConnectionTablePreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const fieldsById = useMemo(() => {
    const map = new Map<string, Field>();
    fields.forEach((field) => {
      map.set(field.id, field);
    });
    return map;
  }, [fields]);

  useEffect(() => {
    if (!open) return;
    setDescription(initialSnapshot.description);
    setTableRows(buildRows(initialSnapshot.tables));
    setFormError(null);
  }, [initialSnapshot, open]);

  useEffect(() => {
    setLocalTables((prev) => prev.filter((table) => !tables.some((item) => item.id === table.id)));
  }, [tables]);

  const initialDefinitionObjectId = initialDefinition?.dataObjectId ?? null;

  useEffect(() => {
    if (!open) {
      return;
    }

    const objectId = initialDefinitionObjectId ?? dataObjectId;
    if (!objectId) {
      setSourceTableKeys([]);
      return;
    }

    let cancelled = false;

    const loadSourceTables = async () => {
      try {
        const tablesResponse = await fetchAvailableSourceTables(objectId);
        if (cancelled) {
          return;
        }
        setAvailableSourceTables(tablesResponse);
        setSourceTableKeys(buildSourceTableKeyList(tablesResponse));
      } catch (error) {
        if (!cancelled) {
          setSourceTableKeys([]);
        }
      }
    };

    loadSourceTables();

    return () => {
      cancelled = true;
    };
  }, [open, dataObjectId, initialDefinitionObjectId]);

  const combinedTables = useMemo(() => {
    const map = new Map<string, Table>();
    [...tables, ...localTables].forEach((table) => {
      map.set(table.id, table);
    });
    return Array.from(map.values());
  }, [tables, localTables]);

  const sourceTableKeySet = useMemo(() => new Set(sourceTableKeys), [sourceTableKeys]);

  const normalizedCurrent: Snapshot = useMemo(
    () => ({
      description,
      tables: tableRows.map((table) => ({
        tableId: table.tableId,
        alias: table.alias,
        description: table.description,
        loadOrder: table.loadOrder,
        isConstruction: table.isConstruction,
        fields: table.fields.map((field) => ({
          fieldId: field.fieldId,
          fieldName: field.fieldName,
          notes: field.notes,
          isUnique: field.isUnique
        }))
      }))
    }),
    [description, tableRows]
  );

  const normalizedCurrentTrimmed = useMemo(() => normalizeSnapshot(normalizedCurrent), [normalizedCurrent]);

  const isDirty = useMemo(() => {
    return JSON.stringify(normalizedCurrentTrimmed) !== JSON.stringify(normalizedInitial);
  }, [normalizedCurrentTrimmed, normalizedInitial]);

  const handleClose = () => {
    setDescription(initialSnapshot.description);
    setTableRows(buildRows(initialSnapshot.tables));
    setFormError(null);
    setTableDialogOpen(false);
    setTableDialogMode('create');
    setEditingTable(null);
    onClose();
  };

  const addExistingTableToDefinition = (existingTable: Table, sanitizedDescription: string | null) => {
    setTableRows((prev) => {
      const emptyIndex = prev.findIndex((row) => !row.tableId);
      const fallbackDescription = existingTable.description ?? sanitizedDescription ?? '';

      if (emptyIndex !== -1) {
        const updated = [...prev];
        const target = updated[emptyIndex];
        updated[emptyIndex] = {
          ...target,
          tableId: existingTable.id,
          alias: target.alias || existingTable.name,
          description: target.description || existingTable.description || fallbackDescription,
          loadOrder: target.loadOrder,
          fields: []
        };
        return updated;
      }

      return [
        ...prev,
        {
          id: generateId(),
          tableId: existingTable.id,
          alias: existingTable.name,
          description: existingTable.description ?? fallbackDescription,
          loadOrder: getNextLoadOrderValue(prev),
          isConstruction: false,
          fields: []
        }
      ];
    });
  };

  const handleExistingTableConfirm = () => {
    if (!existingTablePrompt) return;
    addExistingTableToDefinition(existingTablePrompt.table, existingTablePrompt.sanitized.description);
    toast.showSuccess('Existing table added to the data definition.');
    setExistingTablePrompt(null);
    setTableDialogOpen(false);
    setTableDialogMode('create');
    setEditingTable(null);
  };

  const handleExistingTableCancel = () => {
    setExistingTablePrompt(null);
  };

  const handleAddTable = () => {
    setTableRows((prev) => {
      const nextLoadOrder = getNextLoadOrderValue(prev);
      return [
        ...prev,
        {
          id: generateId(),
          tableId: '',
          alias: '',
          description: '',
          loadOrder: nextLoadOrder,
          isConstruction: false,
          fields: []
        }
      ];
    });
  };

  const handleRemoveTable = (id: string) => {
    setTableRows((prev) => prev.filter((table) => table.id !== id));
  };

  const handleRemoveField = (tableRowId: string, fieldRowId: string) => {
    setTableRows((prev) =>
      prev.map((table) => {
        if (table.id !== tableRowId) {
          return table;
        }
        const nextFields = table.fields.filter((field) => field.id !== fieldRowId);
        if (nextFields.length === table.fields.length) {
          return table;
        }
        return {
          ...table,
          fields: nextFields
        };
      })
    );
  };

  const handleReorderFields = (tableRowId: string, activeId: string, overId: string) => {
    if (activeId === overId) {
      return;
    }
    setTableRows((prev) =>
      prev.map((table) => {
        if (table.id !== tableRowId) {
          return table;
        }
        const fromIndex = table.fields.findIndex((field) => field.id === activeId);
        const toIndex = table.fields.findIndex((field) => field.id === overId);
        if (fromIndex === -1 || toIndex === -1) {
          return table;
        }
        return {
          ...table,
          fields: arrayMove(table.fields, fromIndex, toIndex)
        };
      })
    );
  };

  const handleTableChange = (rowId: string, table: Table | null) => {
    setTableRows((prev) =>
      prev.map((row) => {
        if (row.id !== rowId) return row;
        const nextAlias = row.alias || table?.name || '';
        const selectedKey = table
          ? toSourceTableKey(table.schemaName, table.physicalName ?? table.name)
          : null;
        const shouldDisableConstruction = !table || (selectedKey && sourceTableKeySet.has(selectedKey));
        return {
          ...row,
          tableId: table?.id ?? '',
          alias: table ? nextAlias : '',
          description: table ? row.description || table.description || '' : row.description,
          fields: [],
          loadOrder: row.loadOrder,
          isConstruction: shouldDisableConstruction ? false : row.isConstruction
        };
      })
    );
  };

  const handleOpenCreateTable = () => {
    setFormError(null);
    setTableDialogMode('create');
    setEditingTable(null);
    setTableDialogOpen(true);
  };

  const handleOpenEditTable = (row: TableRow) => {
    if (!row.tableId) {
      toast.showError('Select a table before editing.');
      return;
    }

    const tableMeta = combinedTables.find((table) => table.id === row.tableId);
    if (!tableMeta) {
      toast.showError('Unable to load table metadata for editing.');
      return;
    }

    setFormError(null);
    setEditingTable(tableMeta);
    setTableDialogMode('edit');
    setTableDialogOpen(true);
  };

  const handleOpenAddSourceTable = async () => {
    const objectId = initialDefinition?.dataObjectId ?? dataObjectId;
    if (!objectId) {
      toast.showError('Data object not available.');
      return;
    }
    
    setSourceTableDialogLoading(true);
    setSourceTableDialogError(null);
    try {
      const tables = await fetchAvailableSourceTables(objectId);
      setAvailableSourceTables(tables);
      setSourceTableKeys(buildSourceTableKeyList(tables));
      setSourceTableDialogOpen(true);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unable to load source tables.';
      setSourceTableDialogError(message);
    } finally {
      setSourceTableDialogLoading(false);
    }
  };

  const handleAddSourceTable = async (sourceTable: {
    schemaName: string;
    tableName: string;
    tableType?: string | null;
    columnCount?: number | null;
    estimatedRows?: number | null;
    selectedColumns: SourceTableColumn[];
  }) => {
    // Check if table already exists in definition
    const existingTableInDefinition = tableRows.find(
      (row) => row.tableId && combinedTables.find(
        (t) => t.id === row.tableId && 
        t.physicalName === sourceTable.tableName && 
        (t.schemaName ?? '') === sourceTable.schemaName
      )
    );
    
    if (existingTableInDefinition) {
      toast.showError('This table is already in the data definition.');
      return;
    }

    // Check if table exists in the Table repository
    let table = combinedTables.find(
      (t) => t.physicalName === sourceTable.tableName && (t.schemaName ?? '') === sourceTable.schemaName && t.systemId === systemId
    );

    if (!table) {
      // Create a new table from the source table
      try {
        table = await createTable({
          systemId,
          name: sourceTable.tableName,
          physicalName: sourceTable.tableName,
          schemaName: sourceTable.schemaName,
          description: null,
          tableType: sourceTable.tableType,
          status: 'active'
        });
        setLocalTables((prev) => [...prev, table!]);
      } catch (error) {
        toast.showError(getErrorMessage(error, 'Unable to create table from source.'));
        return;
      }
    }

    // Create fields for the selected columns
    const createdFields: FieldRow[] = [];
    for (const column of sourceTable.selectedColumns) {
      try {
        // Map source column type to field type
        const fieldType = column.typeName || 'VARCHAR';
        
        const newField: Field = await createField({
          tableId: table.id,
          name: column.name,
          description: null,
          fieldType: fieldType,
          fieldLength: column.length ?? null,
          decimalPlaces: column.numericScale ?? null,
          systemRequired: false,
          businessProcessRequired: false,
          suppressedField: false,
          active: true,
          applicationUsage: null,
          businessDefinition: null,
          enterpriseAttribute: null,
          legalRegulatoryImplications: null,
          legalRequirementId: null,
          securityClassificationId: null,
          dataValidation: null,
          referenceTable: null,
          groupingTab: null
        });

        createdFields.push({
          id: generateId(),
          fieldId: newField.id,
          fieldName: newField.name,
          notes: '',
          isUnique: false
        });
      } catch (error) {
        toast.showError(
          getErrorMessage(error, `Unable to create field "${column.name}" from source.`)
        );
        // Continue with remaining columns
      }
    }

    // Add the table to the data definition
    setTableRows((prev) => {
      const nextLoadOrder = getNextLoadOrderValue(prev);
      const alias = `${sourceTable.schemaName}.${sourceTable.tableName}`;
      return [
        ...prev,
        {
          id: generateId(),
          tableId: table!.id,
          alias,
          description: `Source: ${alias}`,
          loadOrder: nextLoadOrder,
          isConstruction: false,
          fields: createdFields
        }
      ];
    });

    setSourceTableDialogOpen(false);
    setSourceTableKeys((prev) => {
      const key = toSourceTableKey(sourceTable.schemaName, sourceTable.tableName);
      if (!key || prev.includes(key)) {
        return prev;
      }
      return [...prev, key];
    });
    toast.showSuccess(
      `Table "${sourceTable.tableName}" added with ${createdFields.length} field${createdFields.length === 1 ? '' : 's'}.`
    );
  };

  const handleOpenPreview = async (table: Table) => {
    setPreviewTableId(table.id);
    setPreviewTableName(table.physicalName);
    setPreviewSchemaName(table.schemaName ?? null);
    setPreviewOpen(true);
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);

    try {
      const data = await fetchTablePreview(table.id);
      setPreviewData(data);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load preview data';
      setPreviewError(message);
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleClosePreview = () => {
    setPreviewOpen(false);
  };

  const handleRefreshPreview = async () => {
    if (!previewTableId) return;
    
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);

    try {
      const data = await fetchTablePreview(previewTableId);
      setPreviewData(data);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load preview data';
      setPreviewError(message);
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleTableDialogSubmit = async (values: {
    name: string;
    physicalName: string;
    schemaName: string;
    description: string;
    tableType: string;
    status: string;
  }) => {
    const sanitized: SanitizedTablePayload = {
      name: values.name.trim(),
      physicalName: values.physicalName.trim(),
      schemaName: CONSTRUCTION_SCHEMA,
      description: sanitizeOptionalString(values.description),
      tableType: sanitizeOptionalString(values.tableType),
      status: values.status.trim() || 'active'
    };

    if (tableDialogMode !== 'edit') {
      const existingTable = combinedTables.find(
        (table) =>
          table.systemId === systemId && (table.physicalName ?? '').toLowerCase() === sanitized.physicalName.toLowerCase()
      );

      if (existingTable) {
        const alreadyLinked = tableRows.some((row) => row.tableId === existingTable.id);
        if (alreadyLinked) {
          toast.showInfo('This table is already part of the data definition.');
          setTableDialogOpen(false);
          setTableDialogMode('create');
          setEditingTable(null);
        } else {
          setExistingTablePrompt({ table: existingTable, sanitized });
        }

        return;
      }
    }

    setTableDialogLoading(true);

    try {
      if (tableDialogMode === 'edit' && editingTable) {
        const updatedTable = await updateTable(editingTable.id, {
          name: sanitized.name,
          physicalName: sanitized.physicalName,
          schemaName: sanitized.schemaName,
          description: sanitized.description,
          tableType: sanitized.tableType,
          status: sanitized.status
        });

        toast.showSuccess('Table updated.');

        setLocalTables((prev) => [...prev.filter((table) => table.id !== updatedTable.id), updatedTable]);

        if (onMetadataRefresh) {
          await onMetadataRefresh();
        }

        setTableDialogOpen(false);
        setTableDialogMode('create');
        setEditingTable(null);
        return;
      }

      const payload: TableInput = {
        systemId,
        name: sanitized.name,
        physicalName: sanitized.physicalName,
        schemaName: sanitized.schemaName,
        description: sanitized.description,
        tableType: sanitized.tableType,
        status: sanitized.status
      };

      const newTable = await createTable(payload);
      toast.showSuccess('Table created.');

      setLocalTables((prev) => [...prev.filter((table) => table.id !== newTable.id), newTable]);

      if (onMetadataRefresh) {
        await onMetadataRefresh();
      }

      setTableRows((prev) => {
        const emptyIndex = prev.findIndex((row) => !row.tableId);
        if (emptyIndex !== -1) {
          const updated = [...prev];
          const target = updated[emptyIndex];
          const fallbackDescription = sanitized.description ?? '';
          updated[emptyIndex] = {
            ...target,
            tableId: newTable.id,
            alias: target.alias || newTable.name,
            description: target.description || newTable.description || fallbackDescription,
            loadOrder: target.loadOrder,
            fields: []
          };
          return updated;
        }
        return [
          ...prev,
          {
            id: generateId(),
            tableId: newTable.id,
            alias: newTable.name,
            description: newTable.description ?? sanitized.description ?? '',
            loadOrder: getNextLoadOrderValue(prev),
            isConstruction: false,
            fields: []
          }
        ];
      });

      setTableDialogOpen(false);
      setTableDialogMode('create');
      setEditingTable(null);
    } catch (error) {
      toast.showError(
        getErrorMessage(
          error,
          tableDialogMode === 'edit' ? 'Unable to update table.' : 'Unable to create table.'
        )
      );
    } finally {
      setTableDialogLoading(false);
    }
  };

  const handleTableDialogClose = () => {
    if (tableDialogLoading) return;
    setTableDialogOpen(false);
    setTableDialogMode('create');
    setEditingTable(null);
  };


  const validate = (tablesPayload: TableRow[]) => {
    if (!tablesPayload.length) {
      return 'Add at least one table to the data definition.';
    }

    const seenTables = new Set<string>();
    const seenLoadOrders = new Set<number>();

    for (const table of tablesPayload) {
      if (!table.tableId) {
        return 'Every entry must select a table.';
      }

      if (seenTables.has(table.tableId)) {
        return 'Tables must be unique within the data definition.';
      }
      seenTables.add(table.tableId);

      const loadOrderValue = table.loadOrder.trim();
      if (!loadOrderValue) {
        return 'Each table must include a load order.';
      }
      const loadOrderNumber = Number(loadOrderValue);
      if (!Number.isInteger(loadOrderNumber) || loadOrderNumber <= 0) {
        return 'Table load order must be a positive whole number.';
      }
      if (seenLoadOrders.has(loadOrderNumber)) {
        return 'Table load order values must be unique.';
      }
      seenLoadOrders.add(loadOrderNumber);
    }

    return null;
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const validationError = validate(tableRows);
    if (validationError) {
      setFormError(validationError);
      return;
    }

    const payloadTables: DataDefinitionTableInput[] = tableRows.map((table) => ({
      tableId: table.tableId,
      alias: table.alias.trim() || null,
      description: table.description.trim() || null,
      loadOrder: Number(table.loadOrder.trim()),
      isConstruction: table.isConstruction,
      fields: table.fields.map((field) => ({
        fieldId: field.fieldId,
        notes: field.notes.trim() || null,
        isUnique: field.isUnique
      }))
    }));

    onSubmit({
      description: description.trim() || null,
      tables: payloadTables
    });
  };

  const tablesAvailable = combinedTables.length > 0;

  useEffect(() => {
    if (!combinedTables.length) {
      return;
    }

    setTableRows((prev) => {
      let changed = false;
      const next = prev.map((row) => {
        if (!row.tableId || !row.isConstruction) {
          return row;
        }
        const tableMeta = combinedTables.find((table) => table.id === row.tableId);
        if (!tableMeta) {
          return row;
        }
        const key = toSourceTableKey(
          tableMeta.schemaName,
          tableMeta.physicalName ?? tableMeta.name
        );
        if (key && sourceTableKeySet.has(key)) {
          changed = true;
          return { ...row, isConstruction: false };
        }
        return row;
      });
      return changed ? next : prev;
    });
  }, [combinedTables, sourceTableKeySet]);

  return (
    <>
      <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
        <Box component="form" noValidate onSubmit={handleSubmit}>
          <DialogTitle>{mode === 'create' ? 'Create Data Definition' : 'Edit Data Definition'}</DialogTitle>
          <DialogContent dividers>
          <Stack spacing={3} mt={1}>
            {!tablesAvailable && (
              <Alert severity="warning">
                No tables are available for the selected system yet. Use the Create Table action below to add one before building the data definition.
              </Alert>
            )}

            <TextField
              label="Description"
              value={description}
              onChange={(event) => setDescription(event.target.value)}
              multiline
              minRows={2}
              fullWidth
            />

            <Stack spacing={2}>
              {tableRows.map((row) => {
                const selectedTable = combinedTables.find((table) => table.id === row.tableId) ?? null;
                const selectedTableKey = selectedTable
                  ? toSourceTableKey(
                      selectedTable.schemaName,
                      selectedTable.physicalName ?? selectedTable.name
                    )
                  : null;
                const isSourceTable = Boolean(
                  selectedTableKey && sourceTableKeySet.has(selectedTableKey)
                );
                const canEditFields = row.isConstruction && Boolean(row.tableId) && !loading;
                let fieldHelperText = 'Fields sourced from existing tables are read-only within this form.';
                if (row.isConstruction) {
                  if (!row.tableId) {
                    fieldHelperText = 'Select a construction table to manage its fields.';
                  } else if (loading) {
                    fieldHelperText = 'Field changes are disabled while saving.';
                  } else {
                    fieldHelperText = 'Drag to reorder fields or remove entries to update this constructed table.';
                  }
                }

                return (
                  <Box key={row.id} sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 1, p: 2 }}>
                    <Stack spacing={2}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Autocomplete
                          fullWidth
                          options={combinedTables}
                          value={selectedTable}
                          disabled={!tablesAvailable}
                          onChange={(_, value) => handleTableChange(row.id, value)}
                          getOptionLabel={(option) => option.name}
                          isOptionEqualToValue={(option, value) => option.id === value?.id}
                          renderInput={(params) => <TextField {...params} label="Table" placeholder="Select table" required />}
                        />
                        <Stack direction="row" spacing={1} alignItems="center">
                          <IconButton
                            aria-label="Preview table data"
                            onClick={() => selectedTable && handleOpenPreview(selectedTable)}
                            disabled={loading || !selectedTable}
                            title="Preview data"
                          >
                            <PreviewIcon />
                          </IconButton>
                          <IconButton
                            aria-label="Edit table"
                            onClick={() => handleOpenEditTable(row)}
                            disabled={loading || !row.tableId}
                          >
                            <EditOutlinedIcon />
                          </IconButton>
                          <IconButton
                            aria-label="Remove table"
                            onClick={() => handleRemoveTable(row.id)}
                            disabled={loading}
                          >
                            <DeleteOutlineIcon />
                          </IconButton>
                        </Stack>
                      </Stack>

                      <Stack spacing={2} direction={{ xs: 'column', md: 'row' }} alignItems={{ md: 'flex-start' }}>
                        <TextField
                          label="Alias"
                          value={row.alias}
                          onChange={(event) =>
                            setTableRows((prev) =>
                              prev.map((table) =>
                                table.id === row.id ? { ...table, alias: event.target.value } : table
                              )
                            )
                          }
                          fullWidth
                        />
                        <TextField
                          label="Table Description"
                          value={row.description}
                          onChange={(event) =>
                            setTableRows((prev) =>
                              prev.map((table) =>
                                table.id === row.id ? { ...table, description: event.target.value } : table
                              )
                            )
                          }
                          fullWidth
                        />
                        <TextField
                          label="Load Order"
                          value={row.loadOrder}
                          onChange={(event) =>
                            setTableRows((prev) =>
                              prev.map((table) =>
                                table.id === row.id
                                  ? {
                                      ...table,
                                      loadOrder: event.target.value.replace(/[^0-9]/g, '')
                                    }
                                  : table
                              )
                            )
                          }
                          type="number"
                          inputProps={{ min: 1 }}
                          required
                          sx={{ width: { xs: '100%', md: 160 } }}
                        />
                      </Stack>
                      {!isSourceTable && (
                        <FormControlLabel
                          control={
                            <Switch
                              checked={row.isConstruction}
                              onChange={(event) =>
                                setTableRows((prev) =>
                                  prev.map((table) =>
                                    table.id === row.id
                                      ? { ...table, isConstruction: event.target.checked }
                                      : table
                                  )
                                )
                              }
                              disabled={!row.tableId}
                            />
                          }
                          label="Construction table"
                          sx={{ pl: 1 }}
                        />
                      )}
                      {isSourceTable && (
                        <Typography variant="caption" color="text.secondary" sx={{ pl: 1 }}>
                          Construction tables must originate from non-source tables.
                        </Typography>
                      )}

                      <Box>
                        <Typography variant="subtitle1" gutterBottom>
                          Fields
                        </Typography>
                        {row.fields.length ? (
                          <>
                            <TableFieldsEditor
                              tableRowId={row.id}
                              fields={row.fields}
                              fieldMap={fieldsById}
                              canEdit={canEditFields}
                              onDelete={(fieldRowId) => handleRemoveField(row.id, fieldRowId)}
                              onReorder={handleReorderFields}
                            />
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              sx={{ display: 'block', mt: 0.5 }}
                            >
                              {fieldHelperText}
                            </Typography>
                          </>
                        ) : (
                          <Typography variant="body2" color="text.secondary">
                            {row.isConstruction
                              ? 'No fields yet. Add fields after saving from the Data Definition page.'
                              : 'Fields can be added after saving from the Data Definition page.'}
                          </Typography>
                        )}
                      </Box>
                    </Stack>
                  </Box>
                );
              })}
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
                <Button
                  variant="outlined"
                  startIcon={<AddCircleOutlineIcon />}
                  onClick={handleAddTable}
                  disabled={loading || !tablesAvailable}
                >
                  Add Table
                </Button>
                <Button
                  variant="outlined"
                  startIcon={<AddCircleOutlineIcon />}
                  onClick={handleOpenAddSourceTable}
                  disabled={loading || sourceTableDialogLoading || (!initialDefinition && !dataObjectId)}
                >
                  Add Source Table
                </Button>
                <Button
                  variant="contained"
                  startIcon={<PostAddIcon />}
                  onClick={handleOpenCreateTable}
                  disabled={loading}
                >
                  Create Table
                </Button>
              </Stack>
            </Stack>

            {formError && <Alert severity="error">{formError}</Alert>}
          </Stack>
          </DialogContent>
          <DialogActions>
            <Button onClick={handleClose} disabled={loading}>
              Cancel
            </Button>
            <LoadingButton type="submit" variant="contained" loading={loading} disabled={loading || !isDirty}>
              Save
            </LoadingButton>
          </DialogActions>
        </Box>
      </Dialog>
      <CreateTableDialog
        open={tableDialogOpen}
        loading={tableDialogLoading}
        mode={tableDialogMode}
        hideSchemaField
        defaultSchemaName={CONSTRUCTION_SCHEMA}
        initialValues={
          editingTable
            ? {
                name: editingTable.name,
                physicalName: editingTable.physicalName,
                schemaName: editingTable.schemaName ?? null,
                description: editingTable.description ?? null,
                tableType: editingTable.tableType ?? null,
                status: editingTable.status ?? 'active'
              }
            : undefined
        }
        onClose={handleTableDialogClose}
        onSubmit={handleTableDialogSubmit}
      />
      {existingTablePrompt && (
        <ConfirmDialog
          open={Boolean(existingTablePrompt)}
          title="Use existing table?"
          description={`A table named "${existingTablePrompt.table.name}" (physical name "${existingTablePrompt.table.physicalName ?? existingTablePrompt.sanitized.physicalName}") already exists in this system. Would you like to add it to the data definition instead of creating a new table?`}
          confirmLabel="Add Existing Table"
          cancelLabel="Keep Editing"
          confirmColor="primary"
          onClose={handleExistingTableCancel}
          onConfirm={handleExistingTableConfirm}
        />
      )}
      <AddExistingSourceTableDialog
        dataObjectId={dataObjectId}
        open={sourceTableDialogOpen}
        tables={availableSourceTables}
        loading={sourceTableDialogLoading}
        error={sourceTableDialogError}
        onClose={() => setSourceTableDialogOpen(false)}
        onSubmit={handleAddSourceTable}
      />

      <ConnectionDataPreviewDialog
        open={previewOpen}
        schemaName={previewSchemaName}
        tableName={previewTableName}
        loading={previewLoading}
        error={previewError}
        preview={previewData}
        onClose={handleClosePreview}
        onRefresh={handleRefreshPreview}
      />
    </>
  );
};

export default DataDefinitionForm;
