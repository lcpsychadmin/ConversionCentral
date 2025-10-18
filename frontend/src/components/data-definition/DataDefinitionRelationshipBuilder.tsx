import {
  createContext,
  memo,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type MouseEvent
} from 'react';
import {
  Background,
  Connection,
  Controls,
  Edge,
  Handle,
  MiniMap,
  Node,
  NodeProps,
  OnConnectStart,
  OnConnectEnd,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow
} from 'reactflow';
import 'reactflow/dist/style.css';
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
  Typography
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

interface TableNodeData {
  table: DataDefinitionTable;
  canEdit: boolean;
  disabled: boolean;
}

interface RelationshipHoverContextValue {
  hoveredFieldId: string | null;
  connectionHandleId: string | null;
  setHoveredFieldId: (fieldId: string | null) => void;
}

const RelationshipHoverContext = createContext<RelationshipHoverContextValue>(
  {
    hoveredFieldId: null,
    connectionHandleId: null,
    setHoveredFieldId: () => {}
  }
);

const useRelationshipHover = () => useContext(RelationshipHoverContext);

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

const TableNode = memo(({ data }: NodeProps<TableNodeData>) => {
  const { table, canEdit, disabled } = data;
  const { hoveredFieldId, connectionHandleId, setHoveredFieldId } = useRelationshipHover();
  const tableName = table.alias || table.table.name;
  const physicalName = table.table.schemaName
    ? `${table.table.schemaName}.${table.table.physicalName}`
    : table.table.physicalName;

  const activeHighlightId = hoveredFieldId ?? connectionHandleId;
  const [dragOverFieldId, setDragOverFieldId] = useState<string | null>(null);
  const [isDraggingConnection, setIsDraggingConnection] = useState(false);

  return (
    <Paper variant="outlined" sx={{ p: 1.5, minWidth: 240 }}>
      <Stack spacing={0.5}>
        <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
          {tableName}
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {physicalName}
        </Typography>
      </Stack>
      <Divider sx={{ my: 1 }} />
      <Stack 
        spacing={0.75}
        onMouseEnter={() => setIsDraggingConnection(false)}
      >
        {table.fields.length === 0 ? (
          <Typography variant="caption" color="text.secondary">
            No fields in this definition.
          </Typography>
        ) : (
          table.fields.map((definitionField: DataDefinitionField) => {
            const isHighlighted = activeHighlightId === definitionField.id;
            const isDragOver = dragOverFieldId === definitionField.id;
            return (
              <Box
                key={definitionField.fieldId}
                sx={{
                  position: 'relative',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  py: 0.75,
                  pl: 3,
                  pr: 3,
                  borderRadius: 1,
                  cursor: canEdit && !disabled ? 'grab' : 'default',
                  transition: 'all 120ms ease',
                  backgroundColor: isHighlighted || isDragOver ? '#e3f2fd' : 'transparent',
                  boxShadow: isDragOver 
                    ? `inset 0 0 0 2px #1976d2, 0 0 8px rgba(25, 118, 210, 0.3)` 
                    : isHighlighted 
                    ? `inset 0 0 0 1px #1976d2` 
                    : 'none',
                  border: isDragOver ? `2px solid #1976d2` : 'none',
                  '&:active': { cursor: 'grabbing' }
                }}
                onMouseEnter={() => {
                  if (!canEdit || disabled) return;
                  setHoveredFieldId(definitionField.id);
                }}
                onMouseLeave={() => {
                  if (!canEdit || disabled) return;
                  setHoveredFieldId(null);
                  setDragOverFieldId(null);
                }}
              >
                <Handle
                  type="target"
                  position={Position.Left}
                  id={definitionField.id}
                  isConnectable={canEdit && !disabled}
                  onConnect={() => setIsDraggingConnection(true)}
                  style={{
                    background: 'transparent',
                    width: '100%',
                    height: '100%',
                    left: -15,
                    top: 0,
                    borderRadius: 0,
                    border: 'none',
                    transform: 'none',
                    pointerEvents: canEdit && !disabled ? 'auto' : 'none'
                  }}
                />
                <Box
                  sx={{
                    position: 'absolute',
                    left: 2,
                    width: 12,
                    height: 12,
                    borderRadius: '50%',
                    backgroundColor: isHighlighted || isDragOver ? '#1565c0' : '#1976d2',
                    boxShadow: isHighlighted || isDragOver ? '0 0 8px rgba(21, 101, 192, 0.6)' : 'none',
                    transition: 'all 120ms ease',
                    cursor: canEdit && !disabled ? 'crosshair' : 'default'
                  }}
                />
                <Typography 
                  variant="body2" 
                  sx={{ 
                    flexGrow: 1,
                    fontWeight: isHighlighted || isDragOver ? 600 : 400,
                    transition: 'font-weight 120ms ease'
                  }}
                >
                  {definitionField.field.name}
                </Typography>
                <Handle
                  type="source"
                  position={Position.Right}
                  id={definitionField.id}
                  isConnectable={canEdit && !disabled}
                  onConnect={() => setIsDraggingConnection(true)}
                  style={{
                    background: 'transparent',
                    width: '100%',
                    height: '100%',
                    right: -15,
                    top: 0,
                    borderRadius: 0,
                    border: 'none',
                    transform: 'none',
                    pointerEvents: canEdit && !disabled ? 'auto' : 'none'
                  }}
                />
                <Box
                  sx={{
                    position: 'absolute',
                    right: 2,
                    width: 12,
                    height: 12,
                    borderRadius: '50%',
                    backgroundColor: isHighlighted || isDragOver ? '#6a1b9a' : '#9c27b0',
                    boxShadow: isHighlighted || isDragOver ? '0 0 8px rgba(106, 27, 154, 0.6)' : 'none',
                    transition: 'all 120ms ease',
                    cursor: canEdit && !disabled ? 'crosshair' : 'default'
                  }}
                />
              </Box>
            );
          })
        )}
      </Stack>
    </Paper>
  );
});

TableNode.displayName = 'TableNode';

const nodeTypes = { table: TableNode } as const;

type RelationshipDialogMode = 'create' | 'edit';

interface RelationshipDialogState {
  mode: RelationshipDialogMode;
  primaryFieldId: string;
  foreignFieldId: string;
  relationshipId?: string;
  relationshipType: DataDefinitionRelationshipType;
  notes: string;
}

const sanitizeNotes = (value: string) => {
  const trimmed = value.trim();
  return trimmed === '' ? null : trimmed;
};

const RelationshipFlow = memo(
  ({
    nodes,
    edges,
    canEdit,
    disabled,
    onConnect,
    onEdgeClick,
    onConnectStart,
    onConnectEnd
  }: {
    nodes: Node<TableNodeData>[];
    edges: Edge[];
    canEdit: boolean;
    disabled: boolean;
    onConnect: (connection: Connection) => void;
    onEdgeClick: (edge: Edge) => void;
    onConnectStart: OnConnectStart;
    onConnectEnd: OnConnectEnd;
  }) => {
    const { fitView } = useReactFlow();

    useEffect(() => {
      const timer = setTimeout(() => {
        fitView({ padding: 0.2, duration: 300 });
      }, 50);
      return () => clearTimeout(timer);
    }, [fitView, nodes.length]);

    return (
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        nodesDraggable={canEdit && !disabled}
        nodesConnectable={canEdit && !disabled}
        onConnect={onConnect}
        onEdgeClick={(event: MouseEvent, edge: Edge) => {
          event.preventDefault();
          onEdgeClick(edge);
        }}
        onConnectStart={onConnectStart}
        onConnectEnd={onConnectEnd}
        elementsSelectable={canEdit && !disabled}
        panOnScroll
        panOnDrag={[2, 4]}
        minZoom={0.4}
        maxZoom={1.5}
      >
        <MiniMap pannable zoomable style={{ height: 100 }} />
        <Controls showInteractive={false} />
        <Background gap={24} size={1} />
      </ReactFlow>
    );
  }
);

RelationshipFlow.displayName = 'RelationshipFlow';

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
  const [expanded, setExpanded] = useState(false);
  const [dialogState, setDialogState] = useState<RelationshipDialogState | null>(null);
  const [dialogSubmitting, setDialogSubmitting] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<DataDefinitionRelationship | null>(null);
  const [hoveredFieldId, setHoveredFieldId] = useState<string | null>(null);
  const [connectionHandleId, setConnectionHandleId] = useState<string | null>(null);
  const theme = useTheme();

  const hoverContextValue = useMemo(
    () => ({ 
      hoveredFieldId, 
      connectionHandleId, 
      setHoveredFieldId
    }),
    [hoveredFieldId, connectionHandleId]
  );

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

  const nodes: Node<TableNodeData>[] = useMemo(() => {
    if (!tables.length) {
      return [];
    }
    const columns = Math.max(1, Math.min(3, Math.ceil(Math.sqrt(tables.length))));
    const horizontalGap = 320;
    const verticalGap = 260;

    return tables.map((table, index) => {
      const column = index % columns;
      const row = Math.floor(index / columns);
      return {
        id: table.id,
        type: 'table',
        position: { x: column * horizontalGap, y: row * verticalGap },
        data: {
          table,
          canEdit,
          disabled: busy
        }
      } satisfies Node<TableNodeData>;
    });
  }, [tables, canEdit, busy]);

  const edges: Edge[] = useMemo(
    () =>
      relationships.map((relationship) => ({
        id: relationship.id,
        source: relationship.primaryTableId,
        target: relationship.foreignTableId,
        sourceHandle: relationship.primaryFieldId,
        targetHandle: relationship.foreignFieldId,
        label: relationshipTypeLabel(relationship.relationshipType),
        animated: false,
        type: 'smoothstep',
        style: { strokeWidth: 2 },
        labelStyle: {
          fill: '#374151',
          fontWeight: 600
        }
      })),
    [relationships]
  );

  const openCreateDialog = useCallback(
    (connection: Connection) => {
      if (!connection.sourceHandle || !connection.targetHandle) {
        return;
      }
      setDialogState({
        mode: 'create',
        primaryFieldId: connection.sourceHandle,
        foreignFieldId: connection.targetHandle,
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

  const handleConnect = useCallback(
    (connection: Connection) => {
      if (!canEdit || busy) {
        return;
      }
      if (!connection.source || !connection.target || !connection.sourceHandle || !connection.targetHandle) {
        return;
      }
      if (connection.source === connection.target) {
        toast.showError('Choose fields from different tables.');
        return;
      }
      if (connection.sourceHandle === connection.targetHandle) {
        toast.showError('Primary and foreign fields must be different.');
        return;
      }
      const exists = relationships.some(
        (relationship) =>
          relationship.primaryFieldId === connection.sourceHandle &&
          relationship.foreignFieldId === connection.targetHandle
      );
      if (exists) {
        toast.showInfo('This relationship already exists.');
        return;
      }
      openCreateDialog(connection);
    },
    [busy, canEdit, openCreateDialog, relationships, toast]
  );

  const handleEdgeClick = useCallback(
    (edge: Edge) => {
      if (!canEdit || busy) {
        return;
      }
      const relationship = relationships.find((item) => item.id === edge.id);
      if (relationship) {
        openEditDialog(relationship);
      }
    },
    [busy, canEdit, openEditDialog, relationships]
  );

  const handleConnectStart = useCallback(
    (_event: Parameters<OnConnectStart>[0], params: Parameters<OnConnectStart>[1]) => {
      if (!canEdit || busy) {
        return;
      }
      setConnectionHandleId(params.handleId ?? null);
    },
    [busy, canEdit]
  );

  const handleConnectEnd = useCallback<OnConnectEnd>(
    () => {
      setConnectionHandleId(null);
      setHoveredFieldId(null);
    },
    []
  );

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
    <Paper variant="outlined" sx={{ p: 2, bgcolor: theme.palette.common.white, borderColor: alpha(theme.palette.info.main, 0.25), borderLeft: `4px solid ${theme.palette.info.main}`, boxShadow: `0 4px 12px ${alpha(theme.palette.common.black, 0.05)}` }}>
      <Stack spacing={2}>
        <Stack direction="row" alignItems="center" spacing={1} justifyContent="space-between">
          <Stack direction="row" alignItems="center" spacing={1}>
            <LinkIcon color="primary" />
            <Typography variant="h6">Table Relationships</Typography>
            <Chip
              label={`${relationships.length} relationship${relationships.length === 1 ? '' : 's'}`}
              size="small"
            />
          </Stack>
          <IconButton
            onClick={toggleExpanded}
            size="small"
            aria-label={expanded ? 'Collapse relationships' : 'Expand relationships'}
            aria-expanded={expanded}
          >
            {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
          </IconButton>
        </Stack>
        <Collapse in={expanded} unmountOnExit>
          <Stack spacing={2}>
            <Typography variant="body2" color="text.secondary">
              Drag from a primary field to a foreign field to define a relationship. Existing connections can be clicked or managed in the list below.
            </Typography>
            <Box sx={{ height: 360 }}>
              <RelationshipHoverContext.Provider value={hoverContextValue}>
                <ReactFlowProvider>
                  <RelationshipFlow
                    nodes={nodes}
                    edges={edges}
                    canEdit={canEdit}
                    disabled={busy}
                    onConnect={handleConnect}
                    onEdgeClick={handleEdgeClick}
                    onConnectStart={handleConnectStart}
                    onConnectEnd={handleConnectEnd}
                  />
                </ReactFlowProvider>
              </RelationshipHoverContext.Provider>
            </Box>
            <Divider />
            <Stack spacing={1.5}>
              <Typography variant="subtitle2">Defined Relationships</Typography>
              {relationships.length === 0 ? (
                <Typography variant="body2" color="text.secondary">
                  No relationships yet. Create one by connecting fields in the diagram above.
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
                        secondaryAction=
                          {canEdit ? (
                            <Stack direction="row" spacing={1}>
                              <IconButton
                                edge="end"
                                aria-label="Edit relationship"
                                onClick={() => openEditDialog(relationship)}
                                disabled={busy}
                                size="small"
                              >
                                <EditIcon fontSize="small" />
                              </IconButton>
                              <IconButton
                                edge="end"
                                aria-label="Delete relationship"
                                onClick={() => setDeleteTarget(relationship)}
                                disabled={busy}
                                size="small"
                              >
                                <DeleteIcon fontSize="small" />
                              </IconButton>
                            </Stack>
                          ) : undefined}
                      >
                        <ListItemText
                          primary={`${primaryLabel} → ${foreignLabel}`}
                          primaryTypographyProps={{ variant: 'body2' }}
                          secondaryTypographyProps={{ component: 'div' }}
                          secondary={
                            <Stack direction="row" spacing={1} alignItems="center">
                              <Chip
                                label={relationshipTypeLabel(relationship.relationshipType)}
                                size="small"
                                color="primary"
                                variant="outlined"
                              />
                              {secondary && (
                                <Tooltip title={secondary}>
                                  <Typography variant="caption" color="text.secondary">
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
