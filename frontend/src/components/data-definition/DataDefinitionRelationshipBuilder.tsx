import {
  useCallback,
  useEffect,
  useMemo,
  useState
} from 'react';
import {
  Box,
  Button,
  Chip,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  List,
  ListItem,
  ListItemText,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
  Stack,
  TextField,
  Tooltip,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import LinkIcon from '@mui/icons-material/Link';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';

import ConfirmDialog from '../common/ConfirmDialog';
import { useToast } from '../../hooks/useToast';
import {
  DataDefinitionField,
  DataDefinitionRelationship,
  DataDefinitionRelationshipInput,
  DataDefinitionRelationshipType,
  DataDefinitionRelationshipUpdateInput,
  DataDefinitionTable
} from '../../types/data';

interface DataDefinitionRelationshipBuilderProps {
  tables: DataDefinitionTable[];
  relationships: DataDefinitionRelationship[];
  canEdit: boolean;
  busy?: boolean;
  onCreateRelationship?: (input: DataDefinitionRelationshipInput) => Promise<boolean>;
  onUpdateRelationship?: (
    relationshipId: string,
    input: DataDefinitionRelationshipUpdateInput
  ) => Promise<boolean>;
  onDeleteRelationship?: (relationshipId: string) => Promise<boolean>;
  initialPrimaryFieldId?: string;
  initialForeignFieldId?: string;
}

const relationshipTypeOptions: { value: DataDefinitionRelationshipType; label: string }[] = [
  { value: 'one_to_one', label: 'One to One' },
  { value: 'one_to_many', label: 'One to Many' },
  { value: 'many_to_one', label: 'Many to One' },
  { value: 'many_to_many', label: 'Many to Many' }
];

const relationshipTypeLabel = (value: DataDefinitionRelationshipType) => {
  const match = relationshipTypeOptions.find((option) => option.value === value);
  return match ? match.label : value;
};

const sanitizeNotes = (value: string) => {
  const trimmed = value.trim();
  return trimmed === '' ? null : trimmed;
};

interface RelationshipDialogState {
  mode: 'create' | 'edit';
  primaryFieldId: string;
  foreignFieldId: string;
  relationshipId?: string;
  relationshipType: DataDefinitionRelationshipType;
  notes: string;
}

interface TablePosition {
  tableId: string;
  x: number;
  y: number;
}

interface FieldRef {
  tableId: string;
  fieldId: string;
  x: number;
  y: number;
  width?: number;
}

// Table card component for two-column layout
const TableCard = ({
  table,
  side,
  isSource,
  selectedFieldId,
  onFieldSelect,
  onFieldDragStart,
  onFieldDragEnd,
  onFieldDragOver,
  onFieldDrop,
  onFieldRefChange
}: {
  table: DataDefinitionTable;
  side: 'left' | 'right';
  isSource: boolean;
  selectedFieldId?: string | null;
  onFieldSelect?: (fieldId: string) => void;
  onFieldDragStart?: (fieldId: string, e: React.DragEvent) => void;
  onFieldDragEnd?: (e: React.DragEvent) => void;
  onFieldDragOver?: (e: React.DragEvent) => void;
  onFieldDrop?: (fieldId: string, e: React.DragEvent) => void;
  onFieldRefChange?: (fieldId: string, ref: FieldRef) => void;
}) => {
  const theme = useTheme();
  const tableName = table.alias || table.table.name;
  const physicalName = table.table.schemaName
    ? `${table.table.schemaName}.${table.table.physicalName}`
    : table.table.physicalName;

  // Use only blue header color
  const headerColor = '#3b82f6';
  const headerGradient = 'linear-gradient(90deg, #3b82f6 0%, #2563eb 100%)';

  // Group fields by groupingTab
  const groupedFields = useMemo(() => {
    const groups = new Map<string, DataDefinitionField[]>();
    const ungroupedFields: DataDefinitionField[] = [];

    table.fields.forEach((field) => {
      const groupKey = field.field.groupingTab || '__ungrouped__';
      if (!groups.has(groupKey)) {
        groups.set(groupKey, []);
      }
      groups.get(groupKey)!.push(field);
    });

    // Ensure ungrouped fields are listed first if they exist
    if (groups.has('__ungrouped__')) {
      ungroupedFields.push(...groups.get('__ungrouped__')!);
      groups.delete('__ungrouped__');
    }

    return { groups: Array.from(groups.entries()), ungroupedFields };
  }, [table.fields]);

  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(
    new Set(groupedFields.groups.map(([key]) => key))
  );

  const toggleGroup = useCallback((groupKey: string) => {
    setExpandedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupKey)) {
        next.delete(groupKey);
      } else {
        next.add(groupKey);
      }
      return next;
    });
  }, []);

  const renderFieldBox = (definitionField: DataDefinitionField) => {
    const isSelected = selectedFieldId === definitionField.id;
    const boxRef: React.Ref<HTMLDivElement> = (el) => {
      if (el && onFieldRefChange) {
        setTimeout(() => {
          // Get the canvas container and field element positions
          const fieldRect = el.getBoundingClientRect();
          const canvasEl = el.closest('[data-canvas]');
          
          if (canvasEl) {
            const canvasRect = (canvasEl as HTMLElement).getBoundingClientRect();
            // Calculate position relative to canvas (using right edge as the x coordinate)
            const relativeX = fieldRect.right - canvasRect.left;
            const relativeY = fieldRect.top - canvasRect.top + fieldRect.height / 2;
            const width = fieldRect.width;
            
            onFieldRefChange(definitionField.id, {
              tableId: table.id,
              fieldId: definitionField.id,
              x: relativeX,
              y: relativeY,
              width: width
            });
          }
        }, 0);
      }
    };

    return (
      <Box
        key={definitionField.id}
        ref={boxRef}
        draggable={true}
        onClick={() => onFieldSelect?.(definitionField.id)}
        onDragStart={(e) => onFieldDragStart?.(definitionField.id, e)}
        onDragEnd={(e) => onFieldDragEnd?.(e)}
        onDragOver={(e) => {
          e.preventDefault();
          onFieldDragOver?.(e);
        }}
        onDrop={(e) => {
          e.preventDefault();
          onFieldDrop?.(definitionField.id, e);
        }}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          padding: '8px 10px',
          borderRadius: '6px',
          cursor: 'grab',
          background: isSelected
            ? alpha(headerColor, 0.15)
            : 'rgba(249, 250, 251, 0.6)',
          border: isSelected ? `1.5px solid ${headerColor}` : '1px solid rgba(0, 0, 0, 0.08)',
          transition: 'all 150ms ease',
          '&:active': {
            cursor: 'grabbing'
          },
          '&:hover': {
            background: alpha(headerColor, 0.1),
            borderColor: headerColor
          }
        }}
      >
        <Box
          sx={{
            width: 8,
            height: 8,
            borderRadius: '50%',
            background: headerColor,
            boxShadow: `0 0 6px ${alpha(headerColor, 0.5)}`
          }}
        />
        <Typography
          variant="body2"
          sx={{
            flex: 1,
            fontWeight: isSelected ? 600 : 500,
            color: isSelected ? headerColor : '#374151',
            fontSize: '0.875rem'
          }}
        >
          {definitionField.field.name}
        </Typography>
      </Box>
    );
  };

  return (
    <Paper
      sx={{
        borderRadius: '8px',
        overflow: 'hidden',
        border: `1px solid ${alpha(headerColor, 0.2)}`,
        boxShadow: `0 4px 12px ${alpha(headerColor, 0.08)}`,
        transition: 'all 200ms ease',
        '&:hover': {
          boxShadow: `0 8px 24px ${alpha(headerColor, 0.12)}`
        }
      }}
    >
      <Box
        sx={{
          background: headerGradient,
          padding: '12px',
          display: 'flex',
          alignItems: 'center',
          gap: 1
        }}
      >
        <Typography
          sx={{
            fontWeight: 700,
            color: 'white',
            fontSize: '0.9rem',
            flex: 1
          }}
        >
          {tableName}
        </Typography>
      </Box>

      <Box sx={{ p: 1.5 }}>
        <Typography
          variant="caption"
          sx={{
            color: '#666',
            display: 'block',
            mb: 1,
            fontSize: '0.75rem',
            fontWeight: 500
          }}
        >
          {physicalName}
        </Typography>

        <Stack spacing={0.75}>
          {table.fields.length === 0 ? (
            <Typography variant="caption" color="text.secondary">
              No fields
            </Typography>
          ) : (
            <>
              {/* Render ungrouped fields */}
              {groupedFields.ungroupedFields.length > 0 && (
                <Stack spacing={0.5}>
                  {groupedFields.ungroupedFields.map((field) => renderFieldBox(field))}
                </Stack>
              )}

              {/* Render grouped fields with expandable sections */}
              {groupedFields.groups.map(([groupKey, fields]) => (
                <Accordion
                  key={groupKey}
                  defaultExpanded={expandedGroups.has(groupKey)}
                  onChange={() => toggleGroup(groupKey)}
                  sx={{
                    boxShadow: 'none',
                    border: `1px solid ${alpha(headerColor, 0.15)}`,
                    '&.Mui-expanded': {
                      margin: 0
                    },
                    '&:before': {
                      display: 'none'
                    }
                  }}
                >
                  <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    sx={{
                      background: alpha(headerColor, 0.08),
                      padding: '8px 12px',
                      minHeight: 'auto',
                      '&.Mui-expanded': {
                        minHeight: 'auto'
                      }
                    }}
                  >
                    <Typography
                      variant="caption"
                      sx={{
                        fontWeight: 600,
                        color: headerColor,
                        fontSize: '0.8rem'
                      }}
                    >
                      {groupKey}
                    </Typography>
                  </AccordionSummary>
                  <AccordionDetails sx={{ padding: '8px 12px' }}>
                    <Stack spacing={0.5}>
                      {fields.map((field) => renderFieldBox(field))}
                    </Stack>
                  </AccordionDetails>
                </Accordion>
              ))}
            </>
          )}
        </Stack>
      </Box>
    </Paper>
  );
};

const DataDefinitionRelationshipBuilder = ({
  tables,
  relationships,
  canEdit,
  busy = false,
  onCreateRelationship,
  onUpdateRelationship,
  onDeleteRelationship,
  initialPrimaryFieldId,
  initialForeignFieldId
}: DataDefinitionRelationshipBuilderProps) => {
  const toast = useToast();
  const theme = useTheme();
  const [expanded, setExpanded] = useState(false);
  const [dialogState, setDialogState] = useState<RelationshipDialogState | null>(null);
  const [dialogSubmitting, setDialogSubmitting] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<DataDefinitionRelationship | null>(null);
  const [draggedFieldId, setDraggedFieldId] = useState<string | null>(null);
  const [dragOverFieldId, setDragOverFieldId] = useState<string | null>(null);
  const [tablePositions, setTablePositions] = useState<Map<string, TablePosition>>(() => {
    const positions = new Map<string, TablePosition>();
    tables.forEach((table, idx) => {
      const col = idx % 2;
      const row = Math.floor(idx / 2);
      positions.set(table.id, {
        tableId: table.id,
        x: col * 450 + 20,
        y: row * 320 + 20
      });
    });
    return positions;
  });
  const [draggingTableId, setDraggingTableId] = useState<string | null>(null);
  const [dragOffset, setDragOffset] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [fieldRefs, setFieldRefs] = useState<Map<string, FieldRef>>(new Map());

  const fieldLookup = useMemo(() => {
    const map = new Map<string, { definitionField: DataDefinitionField; table: DataDefinitionTable }>();
    tables.forEach((table) => {
      table.fields.forEach((definitionField) => {
        map.set(definitionField.id, { definitionField, table });
      });
    });
    return map;
  }, [tables]);

  const handleLabel = useCallback(
    (fieldId: string) => {
      const entry = fieldLookup.get(fieldId);
      if (!entry) {
        return 'Unknown field';
      }
      const { definitionField, table } = entry;
      const tableName = table.alias || table.table.name;
      return `${tableName}.${definitionField.field.name}`;
    },
    [fieldLookup]
  );

  // Initialize dialog with provided field IDs
  useEffect(() => {
    if (initialPrimaryFieldId && initialForeignFieldId) {
      setExpanded(true);
      setDialogState({
        mode: 'create',
        primaryFieldId: initialPrimaryFieldId,
        foreignFieldId: initialForeignFieldId,
        relationshipType: 'one_to_one',
        notes: ''
      });
    }
  }, [initialPrimaryFieldId, initialForeignFieldId]);

  const openCreateDialog = useCallback(
    (primaryFieldId: string, foreignFieldId: string) => {
      setDialogState({
        mode: 'create',
        primaryFieldId,
        foreignFieldId,
        relationshipType: 'one_to_one',
        notes: ''
      });
    },
    []
  );

  const openEditDialog = useCallback((relationship: DataDefinitionRelationship) => {
    setDialogState({
      mode: 'edit',
      relationshipId: relationship.id,
      primaryFieldId: relationship.primaryFieldId,
      foreignFieldId: relationship.foreignFieldId,
      relationshipType: relationship.relationshipType,
      notes: relationship.notes ?? ''
    });
  }, []);

  const handleFieldDragStart = useCallback((fieldId: string) => {
    setDraggedFieldId(fieldId);
  }, []);

  const handleFieldDragEnd = useCallback(() => {
    setDraggedFieldId(null);
    setDragOverFieldId(null);
  }, []);

  const handleFieldDragOver = useCallback(() => {
    // Allow drop
  }, []);

  const handleFieldDrop = useCallback((targetFieldId: string) => {
    if (!draggedFieldId) return;
    if (draggedFieldId === targetFieldId) {
      toast.showError('Select different fields');
      return;
    }

    const draggedEntry = fieldLookup.get(draggedFieldId);
    const targetEntry = fieldLookup.get(targetFieldId);

    if (!draggedEntry || !targetEntry) {
      toast.showError('Invalid field selection');
      return;
    }

    // Check if they're from different tables
    if (draggedEntry.table.id === targetEntry.table.id) {
      toast.showError('Select fields from different tables');
      return;
    }

    // Create relationship from dragged field to target field
    openCreateDialog(draggedFieldId, targetFieldId);
    setDraggedFieldId(null);
    setDragOverFieldId(null);
  }, [draggedFieldId, fieldLookup, openCreateDialog, toast]);

  const handleTableMouseDown = useCallback(
    (tableId: string, e: React.MouseEvent) => {
      if (!canEdit || busy) return;
      if ((e.target as HTMLElement).closest('[data-no-drag]')) return; // Prevent dragging from certain elements

      const position = tablePositions.get(tableId);
      if (!position) return;

      setDraggingTableId(tableId);
      setDragOffset({
        x: e.clientX - position.x,
        y: e.clientY - position.y
      });
    },
    [canEdit, busy, tablePositions]
  );

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!draggingTableId) return;

      const position = tablePositions.get(draggingTableId);
      if (!position) return;

      const newX = Math.max(0, e.clientX - dragOffset.x);
      const newY = Math.max(0, e.clientY - dragOffset.y);

      setTablePositions((prev) => {
        const next = new Map(prev);
        next.set(draggingTableId, {
          ...position,
          x: newX,
          y: newY
        });
        return next;
      });
    };

    const handleMouseUp = () => {
      setDraggingTableId(null);
    };

    if (draggingTableId) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      return () => {
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
      };
    }
  }, [draggingTableId, dragOffset, tablePositions]);

  const closeDialog = useCallback(() => {
    setDialogState(null);
  }, []);

  const handleSubmitDialog = useCallback(async () => {
    if (!dialogState) {
      return;
    }

    setDialogSubmitting(true);
    try {
      if (dialogState.mode === 'create') {
        if (!onCreateRelationship) {
          return;
        }
        const success = await onCreateRelationship({
          primaryFieldId: dialogState.primaryFieldId,
          foreignFieldId: dialogState.foreignFieldId,
          relationshipType: dialogState.relationshipType,
          notes: sanitizeNotes(dialogState.notes) ?? undefined
        });
        if (success) {
          setDialogState(null);
        }
      } else if (dialogState.relationshipId) {
        if (!onUpdateRelationship) {
          return;
        }
        const success = await onUpdateRelationship(dialogState.relationshipId, {
          relationshipType: dialogState.relationshipType,
          notes: sanitizeNotes(dialogState.notes)
        });
        if (success) {
          setDialogState(null);
        }
      }
    } finally {
      setDialogSubmitting(false);
    }
  }, [dialogState, onCreateRelationship, onUpdateRelationship]);

  const handleDelete = useCallback(async () => {
    if (!deleteTarget || !onDeleteRelationship || busy) {
      return;
    }
    const success = await onDeleteRelationship(deleteTarget.id);
    if (success) {
      setDeleteTarget(null);
    }
  }, [busy, deleteTarget, onDeleteRelationship]);

  const handleDeleteFromDialog = useCallback(async () => {
    if (
      !dialogState ||
      dialogState.mode !== 'edit' ||
      !dialogState.relationshipId ||
      !onDeleteRelationship ||
      busy
    ) {
      return;
    }

    setDialogSubmitting(true);
    try {
      const success = await onDeleteRelationship(dialogState.relationshipId);
      if (success) {
        setDialogState(null);
      }
    } finally {
      setDialogSubmitting(false);
    }
  }, [busy, dialogState, onDeleteRelationship]);

  const toggleExpanded = useCallback(() => {
    setExpanded((prev) => !prev);
  }, []);

  const renderRelationshipDialog = () => {
    if (!dialogState) {
      return null;
    }

    const primaryLabel = handleLabel(dialogState.primaryFieldId);
    const foreignLabel = handleLabel(dialogState.foreignFieldId);

    const handleTypeChange = (event: SelectChangeEvent<DataDefinitionRelationshipType>) => {
      const value = event.target.value as DataDefinitionRelationshipType;
      setDialogState((prev) => (prev ? { ...prev, relationshipType: value } : prev));
    };

    return (
      <Dialog open onClose={dialogSubmitting ? undefined : closeDialog} fullWidth maxWidth="sm">
        <DialogTitle>
          {dialogState.mode === 'create' ? 'Create Relationship' : 'Edit Relationship'}
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Stack spacing={0.5}>
              <Typography variant="caption" color="text.secondary">
                Primary Field
              </Typography>
              <Typography variant="body2">{primaryLabel}</Typography>
            </Stack>
            <Stack spacing={0.5}>
              <Typography variant="caption" color="text.secondary">
                Foreign Field
              </Typography>
              <Typography variant="body2">{foreignLabel}</Typography>
            </Stack>
            <FormControl size="small">
              <InputLabel id="relationship-type-label">Relationship Type</InputLabel>
              <Select
                labelId="relationship-type-label"
                value={dialogState.relationshipType}
                label="Relationship Type"
                onChange={handleTypeChange}
              >
                {relationshipTypeOptions.map((option) => (
                  <MenuItem key={option.value} value={option.value}>
                    {option.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <TextField
              label="Notes"
              value={dialogState.notes}
              onChange={(event) =>
                setDialogState((prev) => (prev ? { ...prev, notes: event.target.value } : prev))
              }
              multiline
              minRows={2}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          {dialogState.mode === 'edit' && onDeleteRelationship ? (
            <Button
              color="error"
              onClick={handleDeleteFromDialog}
              disabled={dialogSubmitting}
              sx={{ mr: 'auto' }}
            >
              Delete
            </Button>
          ) : null}
          <Button onClick={closeDialog} disabled={dialogSubmitting}>
            Cancel
          </Button>
          <Button
            variant="contained"
            onClick={handleSubmitDialog}
            disabled={dialogSubmitting}
          >
            {dialogState.mode === 'create' ? 'Create' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>
    );
  };

  return (
    <Paper
      variant="outlined"
      sx={{
        p: 2,
        bgcolor: theme.palette.common.white,
        borderColor: alpha(theme.palette.primary.main, 0.2),
        borderLeft: `5px solid ${theme.palette.primary.main}`,
        boxShadow: `0 4px 16px ${alpha(theme.palette.common.black, 0.08)}`,
        borderRadius: '8px',
        background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.98) 0%, rgba(249, 250, 251, 0.98) 100%)'
      }}
    >
      <Stack spacing={2}>
        <Stack direction="row" alignItems="center" spacing={1.5} justifyContent="space-between">
          <Stack direction="row" alignItems="center" spacing={1.5}>
            <Box
              sx={{
                p: 1,
                borderRadius: '6px',
                background: 'linear-gradient(135deg, #2563eb 0%, #3b82f6 100%)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            >
              <LinkIcon sx={{ color: 'white', fontSize: '1.25rem' }} />
            </Box>
            <Stack spacing={0.25}>
              <Typography
                variant="h6"
                sx={{
                  fontWeight: 700,
                  background: 'linear-gradient(135deg, #1e40af 0%, #2563eb 100%)',
                  backgroundClip: 'text',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent'
                }}
              >
                Table Relationships
              </Typography>
            </Stack>
            <Chip
              label={`${relationships.length} relationship${relationships.length === 1 ? '' : 's'}`}
              size="small"
              sx={{
                background: 'linear-gradient(135deg, #dbeafe 0%, #e0e7ff 100%)',
                color: '#1e40af',
                fontWeight: 600
              }}
            />
          </Stack>
          <IconButton
            onClick={toggleExpanded}
            size="small"
            aria-label={expanded ? 'Collapse relationships' : 'Expand relationships'}
            aria-expanded={expanded}
            sx={{
              transition: 'all 200ms ease',
              '&:hover': {
                background: alpha(theme.palette.primary.main, 0.08)
              }
            }}
          >
            {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
          </IconButton>
        </Stack>

        <Collapse in={expanded} unmountOnExit>
          <Stack spacing={2}>
            <Typography
              variant="body2"
              color="text.secondary"
              sx={{
                fontSize: '0.875rem',
                lineHeight: 1.6,
                color: '#666'
              }}
            >
              Drag fields between tables to create relationships. Drag table headers to reposition.
            </Typography>

            {/* Canvas-based free-form layout */}
            <Box
              data-canvas
              sx={{
                position: 'relative',
                minHeight: 600,
                p: 2,
                borderRadius: '8px',
                background: 'linear-gradient(135deg, rgba(249, 250, 251, 0.5) 0%, rgba(240, 249, 255, 0.5) 100%)',
                border: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
                overflow: 'hidden'
              }}
            >
              {/* SVG layer for connecting lines */}
              <svg
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  height: '100%',
                  pointerEvents: 'none',
                  zIndex: 1
                }}
              >
                <defs>
                  <marker
                    id="arrowhead"
                    markerWidth="10"
                    markerHeight="10"
                    refX="9"
                    refY="3"
                    orient="auto"
                  >
                    <polygon points="0 0, 10 3, 0 6" fill="#3b82f6" />
                  </marker>
                </defs>
                {relationships.map((rel) => {
                  const primaryRef = fieldRefs.get(rel.primaryFieldId);
                  const foreignRef = fieldRefs.get(rel.foreignFieldId);

                  if (!primaryRef || !foreignRef) return null;

                  // Connection points on the right edge of primary field and left edge of foreign field
                  const x1 = primaryRef.x;
                  const y1 = primaryRef.y;
                  const x2 = foreignRef.x - (foreignRef.width || 0);
                  const y2 = foreignRef.y;
                  const midX = (x1 + x2) / 2;

                  return (
                    <g key={`line-${rel.id}`}>
                      <path
                        d={`M ${x1} ${y1} Q ${midX} ${y1}, ${midX} ${(y1 + y2) / 2} T ${x2} ${y2}`}
                        stroke="#3b82f6"
                        strokeWidth="2"
                        fill="none"
                        opacity="0.6"
                      />
                    </g>
                  );
                })}
              </svg>

              {/* Tables positioned on canvas */}
              <Box sx={{ position: 'relative', zIndex: 2 }}>
                {tables.map((table) => {
                  const position = tablePositions.get(table.id);
                  if (!position) return null;

                  return (
                    <Box
                      key={table.id}
                      onMouseDown={(e) => handleTableMouseDown(table.id, e)}
                      sx={{
                        position: 'absolute',
                        left: `${position.x}px`,
                        top: `${position.y}px`,
                        width: '320px',
                        cursor: draggingTableId === table.id ? 'grabbing' : 'grab',
                        userSelect: 'none'
                      }}
                    >
                      <TableCard
                        table={table}
                        side="left"
                        isSource={true}
                        selectedFieldId={draggedFieldId}
                        onFieldDragStart={handleFieldDragStart}
                        onFieldDragEnd={handleFieldDragEnd}
                        onFieldDragOver={handleFieldDragOver}
                        onFieldDrop={handleFieldDrop}
                        onFieldRefChange={(fieldId, ref) => {
                          setFieldRefs((prev) => {
                            const next = new Map(prev);
                            next.set(fieldId, ref);
                            return next;
                          });
                        }}
                      />
                    </Box>
                  );
                })}
              </Box>
            </Box>

            <Divider sx={{ background: 'linear-gradient(90deg, transparent, #e0e7ff, transparent)' }} />

            {/* Relationships List */}
            <Stack spacing={1.5}>
              <Typography
                variant="subtitle2"
                sx={{
                  fontWeight: 700,
                  color: '#1e40af',
                  fontSize: '0.95rem'
                }}
              >
                Defined Relationships
              </Typography>
              {relationships.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  No relationships yet. Create one by clicking fields in the diagram above.
                </Typography>
              ) : (
                <List disablePadding>
                  {relationships.map((relationship) => {
                    const primaryLabel = handleLabel(relationship.primaryFieldId);
                    const foreignLabel = handleLabel(relationship.foreignFieldId);
                    const secondary = relationship.notes ? relationship.notes : undefined;

                    return (
                      <ListItem
                        key={relationship.id}
                        disableGutters
                        sx={{
                          p: 1.5,
                          mb: 0.75,
                          borderRadius: '6px',
                          background: 'linear-gradient(135deg, rgba(243, 244, 246, 0.6) 0%, rgba(249, 250, 251, 0.6) 100%)',
                          border: `1px solid ${alpha(theme.palette.primary.main, 0.15)}`,
                          transition: 'all 200ms ease',
                          '&:hover': {
                            background: 'linear-gradient(135deg, rgba(219, 234, 254, 0.4) 0%, rgba(240, 249, 255, 0.4) 100%)',
                            boxShadow: `0 4px 12px ${alpha(theme.palette.primary.main, 0.1)}`
                          }
                        }}
                        secondaryAction={
                          canEdit ? (
                            <Stack direction="row" spacing={0.5}>
                              <IconButton
                                edge="end"
                                aria-label="Edit relationship"
                                onClick={() => openEditDialog(relationship)}
                                disabled={busy}
                                size="small"
                                sx={{
                                  color: theme.palette.primary.main,
                                  '&:hover': {
                                    background: alpha(theme.palette.primary.main, 0.1)
                                  }
                                }}
                              >
                                <EditIcon fontSize="small" />
                              </IconButton>
                              <IconButton
                                edge="end"
                                aria-label="Delete relationship"
                                onClick={() => setDeleteTarget(relationship)}
                                disabled={busy}
                                size="small"
                                sx={{
                                  color: theme.palette.error.main,
                                  '&:hover': {
                                    background: alpha(theme.palette.error.main, 0.1)
                                  }
                                }}
                              >
                                <DeleteIcon fontSize="small" />
                              </IconButton>
                            </Stack>
                          ) : undefined
                        }
                      >
                        <ListItemText
                          primary={`${primaryLabel} → ${foreignLabel}`}
                          primaryTypographyProps={{
                            variant: 'body2',
                            sx: { fontWeight: 600, color: '#1e40af' }
                          }}
                          secondaryTypographyProps={{ component: 'div' }}
                          secondary={
                            <Stack direction="row" spacing={1} alignItems="center" sx={{ mt: 0.75 }}>
                              <Chip
                                label={relationshipTypeLabel(relationship.relationshipType)}
                                size="small"
                                sx={{
                                  background: 'linear-gradient(135deg, #dbeafe 0%, #e0e7ff 100%)',
                                  color: '#1e40af',
                                  fontWeight: 600,
                                  fontSize: '0.75rem'
                                }}
                              />
                              {secondary && (
                                <Tooltip title={secondary}>
                                  <Typography
                                    variant="caption"
                                    color="text.secondary"
                                    sx={{ fontSize: '0.75rem' }}
                                  >
                                    {secondary}
                                  </Typography>
                                </Tooltip>
                              )}
                            </Stack>
                          }
                        />
                      </ListItem>
                    );
                  })}
                </List>
              )}
            </Stack>
          </Stack>
        </Collapse>
      </Stack>

      {renderRelationshipDialog()}

      <ConfirmDialog
        open={Boolean(deleteTarget)}
        title="Delete Relationship"
        description="Are you sure you want to remove this relationship?"
        confirmLabel="Delete"
        confirmColor="error"
        onClose={() => setDeleteTarget(null)}
        onConfirm={handleDelete}
      />
    </Paper>
  );
};

export default DataDefinitionRelationshipBuilder;
