import { Fragment, useMemo, useState, useCallback, useRef, useEffect, type DragEvent, type MouseEvent, type ReactNode } from 'react';
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  FormControlLabel,
  FormLabel,
  IconButton,
  InputAdornment,
  List,
  ListItem,
  ListItemText,
  MenuItem,
  Paper,
  Radio,
  RadioGroup,
  Stack,
  Table as MuiTable,
  TableBody as MuiTableBody,
  TableCell as MuiTableCell,
  TableContainer,
  TableHead as MuiTableHead,
  TableRow as MuiTableRow,
  TextField,
  Tooltip,
  Typography
} from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import SearchIcon from '@mui/icons-material/Search';
import SaveIcon from '@mui/icons-material/Save';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PublishIcon from '@mui/icons-material/Publish';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { isAxiosError } from 'axios';
import { useQuery } from 'react-query';
import ReactFlow, {
  ReactFlowInstance,
  ReactFlowProvider,
  XYPosition,
  Background,
  Connection,
  Controls,
  MiniMap,
  Node,
  MarkerType,
  type Edge,
  useEdgesState,
  useNodesState
} from 'reactflow';
import 'reactflow/dist/style.css';

import {
  DndContext,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  type DragEndEvent,
  useSensor,
  useSensors
} from '@dnd-kit/core';
import {
  SortableContext,
  arrayMove,
  sortableKeyboardCoordinates,
  horizontalListSortingStrategy,
  useSortable
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

import { fetchTables, fetchFields } from '@services/tableService';
import { fetchReportPreview, type ReportPreviewResponse } from '@services/reportingService';
import type {
  ReportAggregateFn,
  ReportDesignerDefinition,
  ReportJoinType,
  ReportSortDirection
} from '../types/reporting';
import { Field, Table } from '../types/data';
import ReportTableNode, { ReportTableNodeData, REPORTING_FIELD_DRAG_TYPE } from '@components/reporting/ReportTableNode';

type DesignerNode = Node<ReportTableNodeData>;

const nodeTypes = { reportTable: ReportTableNode } as const;

interface DragPayload {
  tableId: string;
}

interface FieldDragPayload {
  tableId: string;
  fieldId: string;
}

const tableDragMimeType = 'application/reporting-table';

type SortDirection = ReportSortDirection;
type JoinType = ReportJoinType;

interface FieldColumnSettings {
  show: boolean;
  sort: SortDirection;
  aggregate: ReportAggregateFn | null;
  criteria: string[];
}

interface SelectedFieldColumn {
  fieldId: string;
  field: Field;
  table?: Table;
  settings: FieldColumnSettings;
}

interface JoinDialogState {
  mode: 'create' | 'edit';
  edgeId?: string | null;
  sourceTableId: string;
  sourceFieldId: string;
  targetTableId: string;
  targetFieldId: string;
  joinType: JoinType;
}

interface JoinEdgeData {
  sourceFieldId?: string;
  targetFieldId?: string;
  joinType?: JoinType;
}

interface OutputColumnRowDescriptor {
  key: string;
  label: string;
  paddingY: number;
  justifyContent?: 'center' | 'flex-start';
  minHeight?: number;
}

interface SortableOutputColumnProps {
  column: SelectedFieldColumn;
  isFirst: boolean;
  isLast: boolean;
  groupingEnabled: boolean;
  aggregateOptions: { label: string; value: ReportAggregateFn }[];
  criteriaRowCount: number;
  rowDescriptors: OutputColumnRowDescriptor[];
  onRemove: (fieldId: string) => void;
  onSortChange: (fieldId: string, sort: SortDirection) => void;
  onAggregateChange: (fieldId: string, aggregate: ReportAggregateFn) => void;
  onShowToggle: (fieldId: string, show: boolean) => void;
  onCriteriaChange: (fieldId: string, index: number, value: string) => void;
}

const SortableOutputColumn = ({
  column,
  isFirst,
  isLast,
  groupingEnabled,
  aggregateOptions,
  rowDescriptors,
  criteriaRowCount,
  onRemove,
  onSortChange,
  onAggregateChange,
  onShowToggle,
  onCriteriaChange
}: SortableOutputColumnProps) => {
  const theme = useTheme();
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: column.fieldId
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition
  };

  type OutputColumnRow = {
    key: string;
    content: ReactNode;
    paddingY: number;
    justifyContent: 'center' | 'flex-start';
    minHeight?: number;
  };

  const rows = rowDescriptors.reduce<OutputColumnRow[]>((acc, descriptor) => {
    const { key } = descriptor;
    let content: ReactNode | null = null;

    if (key === 'field') {
      content = (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <Tooltip title="Drag to reorder">
            <IconButton
              size="small"
              {...attributes}
              {...listeners}
              sx={{
                cursor: 'grab',
                color: alpha(theme.palette.text.secondary, 0.8),
                '&:hover': {
                  color: theme.palette.primary.main
                }
              }}
            >
              <DragIndicatorIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <TextField
            size="small"
            value={column.field.name}
            fullWidth
            InputProps={{ readOnly: true }}
            sx={{ flex: 1, '& .MuiInputBase-input': { fontSize: 13 } }}
          />
          <Tooltip title="Remove column">
            <span>
              <IconButton size="small" onClick={() => onRemove(column.fieldId)}>
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </Box>
      );
    } else if (key === 'table') {
      content = (
        <TextField
          size="small"
          value={column.table?.name ?? '—'}
          fullWidth
          InputProps={{ readOnly: true }}
          sx={{ '& .MuiInputBase-input': { fontSize: 13 } }}
        />
      );
    } else if (key === 'group' && groupingEnabled) {
      const aggregateValue = column.settings.aggregate ?? 'groupBy';
      content = (
        <TextField
          select
          size="small"
          fullWidth
          value={aggregateValue}
          onChange={(event) => onAggregateChange(column.fieldId, event.target.value as ReportAggregateFn)}
          sx={{ '& .MuiInputBase-input': { fontSize: 13 } }}
        >
          {aggregateOptions.map((option) => (
            <MenuItem key={option.value} value={option.value}>
              {option.label}
            </MenuItem>
          ))}
        </TextField>
      );
    } else if (key === 'sort') {
      content = (
        <TextField
          select
          size="small"
          fullWidth
          value={column.settings.sort}
          onChange={(event) => onSortChange(column.fieldId, event.target.value as SortDirection)}
          sx={{ '& .MuiInputBase-input': { fontSize: 13 } }}
        >
          <MenuItem value="none">None</MenuItem>
          <MenuItem value="asc">Ascending</MenuItem>
          <MenuItem value="desc">Descending</MenuItem>
        </TextField>
      );
    } else if (key === 'show') {
      content = (
        <Checkbox
          size="small"
          checked={column.settings.show}
          onChange={(event) => onShowToggle(column.fieldId, event.target.checked)}
        />
      );
    } else if (key.startsWith('criteria-')) {
      const index = Number(key.split('-')[1]);
      if (!Number.isNaN(index) && index < criteriaRowCount) {
        content = (
          <TextField
            size="small"
            fullWidth
            value={column.settings.criteria[index] ?? ''}
            onChange={(event) => onCriteriaChange(column.fieldId, index, event.target.value)}
            placeholder={index === 0 ? 'e.g. >= 100' : ''}
            sx={{ '& .MuiInputBase-input': { fontSize: 13 } }}
          />
        );
      }
    }

    if (!content) {
      return acc;
    }

    acc.push({
      key,
      content,
      paddingY: descriptor.paddingY,
      justifyContent: descriptor.justifyContent ?? 'flex-start',
      minHeight: descriptor.minHeight
    });
    return acc;
  }, []);

  return (
    <Box
      ref={setNodeRef}
      style={style}
      sx={{
        minWidth: 220,
        flex: '0 0 auto',
        display: 'flex',
        flexDirection: 'column',
        borderLeft: isFirst ? 'none' : `1px solid ${theme.palette.divider}`,
        borderRight: isLast ? 'none' : `1px solid ${theme.palette.divider}`,
        backgroundColor: alpha(theme.palette.background.paper, 0.85),
        opacity: isDragging ? 0.9 : 1,
        boxShadow: isDragging ? `0 12px 28px ${alpha(theme.palette.primary.main, 0.18)}` : 'none',
        transition: 'box-shadow 120ms ease, opacity 120ms ease'
      }}
    >
      {rows.map((row, index) => (
        <Box
          key={row.key}
          sx={{
            px: 1.25,
            py: row.paddingY,
            display: 'flex',
            alignItems: 'center',
            justifyContent: row.justifyContent,
            minHeight: row.minHeight,
            borderBottom: index === rows.length - 1 ? 'none' : `1px solid ${theme.palette.divider}`
          }}
        >
          {row.content}
        </Box>
      ))}
    </Box>
  );
};

const designerStorageKey = 'cc_reporting_designer_draft';
const defaultNodeWidth = 260;
const defaultNodeHeight = 320;
const minNodeWidth = 220;
const minNodeHeight = 200;

const ReportingDesignerContent = () => {
  const { data: tables = [], isLoading: tablesLoading, isError: tablesError } = useQuery<Table[]>(['reporting-tables'], fetchTables);
  const { data: fields = [] } = useQuery<Field[]>(['reporting-fields'], fetchFields);

  const [searchTerm, setSearchTerm] = useState('');
  const [nodes, setNodes, onNodesChange] = useNodesState<ReportTableNodeData>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedFieldOrder, setSelectedFieldOrder] = useState<string[]>([]);
  const [fieldSettings, setFieldSettings] = useState<Record<string, FieldColumnSettings>>({});
  const [criteriaRowCount, setCriteriaRowCount] = useState(2);
  const [groupingEnabled, setGroupingEnabled] = useState(false);
  const [outputDropActive, setOutputDropActive] = useState(false);
  const [statusBanner, setStatusBanner] = useState<{ message: string; color: string } | null>(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewData, setPreviewData] = useState<ReportPreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [joinDialogState, setJoinDialogState] = useState<JoinDialogState | null>(null);

  const reactFlowInstanceRef = useRef<ReactFlowInstance<ReportTableNodeData> | null>(null);
  const reactFlowWrapper = useRef<HTMLDivElement | null>(null);
  const hasHydratedDraftRef = useRef(false);
  const draggedFieldRef = useRef<FieldDragPayload | null>(null);
  const theme = useTheme();
  const joinEdgePresentation = useMemo(() => {
    const stroke = theme.palette.primary.main;
    return {
      type: 'smoothstep' as const,
      animated: false,
      style: {
        stroke,
        strokeWidth: 1.8
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: stroke,
        width: 18,
        height: 18
      }
    };
  }, [theme]);

  const fieldsByTable = useMemo(() => {
    const map = new Map<string, Field[]>();
    const seenKeyByTable = new Map<string, Set<string>>();

    fields.forEach((field) => {
      const tableId = field.tableId;
      let list = map.get(tableId);
      if (!list) {
        list = [];
        map.set(tableId, list);
      }

      let seen = seenKeyByTable.get(tableId);
      if (!seen) {
        seen = new Set<string>();
        seenKeyByTable.set(tableId, seen);
      }

      const normalizedName = field.name.trim().toLowerCase();
      const normalizedType = (field.fieldType ?? '').trim().toLowerCase();
      const dedupeKey = `${normalizedName}|${normalizedType}`;

      if (seen.has(dedupeKey)) {
        return;
      }

      seen.add(dedupeKey);
      list.push(field);
    });

    return map;
  }, [fields]);

  const tablesById = useMemo(() => {
    const map = new Map<string, Table>();
    tables.forEach((table) => map.set(table.id, table));
    return map;
  }, [tables]);

  const fieldsById = useMemo(() => {
    const map = new Map<string, Field>();
    fields.forEach((field) => map.set(field.id, field));
    return map;
  }, [fields]);

  const fieldIdSet = useMemo(() => new Set(fields.map((field) => field.id)), [fields]);

  const extractFieldIdFromHandle = useCallback((handleId?: string | null) => {
    if (!handleId) {
      return null;
    }
    const separatorIndex = handleId.indexOf(':');
    if (separatorIndex === -1) {
      return null;
    }
    return handleId.slice(separatorIndex + 1) || null;
  }, []);

  useEffect(() => {
    setSelectedFieldOrder((current) => {
      const filtered = current.filter((fieldId) => fieldIdSet.has(fieldId));
      return filtered.length === current.length ? current : filtered;
    });
  }, [fieldIdSet]);

  useEffect(() => {
    setFieldSettings((current) => {
      let didChange = false;
      const next: Record<string, FieldColumnSettings> = {};

      selectedFieldOrder.forEach((fieldId) => {
        if (!fieldIdSet.has(fieldId)) {
          return;
        }
        const existing = current[fieldId];
        const nextCriteria = Array.from({ length: criteriaRowCount }, (_, index) => existing?.criteria[index] ?? '');
        const nextAggregate = groupingEnabled ? existing?.aggregate ?? 'groupBy' : null;

        if (
          existing &&
          existing.criteria.length === criteriaRowCount &&
          existing.criteria.every((value, index) => value === nextCriteria[index]) &&
          existing.aggregate === nextAggregate
        ) {
          next[fieldId] = existing;
          return;
        }

        didChange = true;
        next[fieldId] = {
          show: existing?.show ?? true,
          sort: existing?.sort ?? 'none',
          aggregate: nextAggregate,
          criteria: nextCriteria
        };
      });

      if (!didChange) {
        const sameKeys = Object.keys(current).length === Object.keys(next).length;
        const sameRefs = sameKeys && Object.keys(next).every((key) => current[key] === next[key]);
        if (sameRefs) {
          return current;
        }
      }

      return next;
    });
  }, [criteriaRowCount, fieldIdSet, groupingEnabled, selectedFieldOrder]);

  const createNodeData = useCallback((table: Table, tableFields: Field[]): ReportTableNodeData => {
    const uniqueFields: Field[] = [];
    const seen = new Set<string>();
    tableFields.forEach((field) => {
      const normalizedName = field.name.trim().toLowerCase();
      const normalizedType = (field.fieldType ?? '').trim().toLowerCase();
      const key = `${normalizedName}|${normalizedType}`;
      if (!seen.has(key)) {
        seen.add(key);
        uniqueFields.push(field);
      }
    });
    const sortedFields = uniqueFields.sort((a, b) => a.name.localeCompare(b.name));
    return {
      tableId: table.id,
      label: table.name,
      subtitle: table.schemaName ? `${table.schemaName}.${table.physicalName}` : table.physicalName,
      meta: table.status ? `Status: ${table.status}` : undefined,
      fields: sortedFields.map((field) => ({
        id: field.id,
        name: field.name,
        type: field.fieldType,
        description: field.description ?? null
      }))
    };
  }, []);

  useEffect(() => {
    if (hasHydratedDraftRef.current) {
      return;
    }
    if (typeof window === 'undefined') {
      hasHydratedDraftRef.current = true;
      return;
    }
    if (nodes.length > 0) {
      hasHydratedDraftRef.current = true;
      return;
    }
    if (tables.length === 0 || fields.length === 0) {
      return;
    }

    const raw = window.localStorage.getItem(designerStorageKey);
    if (!raw) {
      hasHydratedDraftRef.current = true;
      return;
    }

    try {
      const parsed = JSON.parse(raw) as ReportDesignerDefinition | null;
      if (!parsed) {
        hasHydratedDraftRef.current = true;
        return;
      }

      const normalizedCriteriaCount = Math.max(1, parsed.criteriaRowCount ?? 1);
      const groupingFromDraft = Boolean(parsed.groupingEnabled);
      const restoredNodes = (parsed.tables ?? [])
        .filter((placement) => tablesById.has(placement.tableId))
        .map((placement) => {
          const table = tablesById.get(placement.tableId)!;
          const tableFields = fieldsByTable.get(placement.tableId) ?? [];
          const savedWidth = placement.dimensions?.width;
          const savedHeight = placement.dimensions?.height;
          const width = typeof savedWidth === 'number' && savedWidth >= minNodeWidth
            ? savedWidth
            : defaultNodeWidth;
          const height = typeof savedHeight === 'number' && savedHeight >= minNodeHeight
            ? savedHeight
            : defaultNodeHeight;
          return {
            id: placement.tableId,
            type: 'reportTable' as const,
            position: {
              x: placement.position?.x ?? 0,
              y: placement.position?.y ?? 0
            },
            draggable: true,
            selectable: true,
            width,
            height,
            style: {
              width,
              height,
              minWidth: minNodeWidth,
              minHeight: minNodeHeight
            },
            data: createNodeData(table, tableFields)
          } satisfies DesignerNode;
        });

      const restoredNodeIds = new Set(restoredNodes.map((node) => node.id));

      const restoredEdges = (parsed.joins ?? [])
        .filter((join) => restoredNodeIds.has(join.sourceTableId) && restoredNodeIds.has(join.targetTableId))
        .map((join) => {
          const sourceFieldId = join.sourceFieldId ?? undefined;
          const targetFieldId = join.targetFieldId ?? undefined;
          const joinType = (join.joinType as JoinType | undefined) ?? 'inner';
          const baseEdge = {
            id: join.id ?? `${join.sourceTableId}-${join.targetTableId}`,
            source: join.sourceTableId,
            target: join.targetTableId,
            sourceHandle: sourceFieldId ? `source:${sourceFieldId}` : undefined,
            targetHandle: targetFieldId ? `target:${targetFieldId}` : undefined,
            data: {
              sourceFieldId,
              targetFieldId,
              joinType
            }
          };

          return {
            ...baseEdge,
            ...joinEdgePresentation,
            style: { ...joinEdgePresentation.style },
            markerEnd: { ...joinEdgePresentation.markerEnd }
          };
        });

      const orderedColumns = [...(parsed.columns ?? [])].sort((a, b) => a.order - b.order);
      const restoredFieldOrder: string[] = [];
      const restoredFieldSettings: Record<string, FieldColumnSettings> = {};

      orderedColumns.forEach((column) => {
        if (!fieldIdSet.has(column.fieldId)) {
          return;
        }
        const normalizedSort: SortDirection = column.sort === 'asc' || column.sort === 'desc' ? column.sort : 'none';
        const normalizedAggregate: ReportAggregateFn | null = groupingFromDraft
          ? column.aggregate ?? 'groupBy'
          : null;
        restoredFieldOrder.push(column.fieldId);
        restoredFieldSettings[column.fieldId] = {
          show: column.show ?? true,
          sort: normalizedSort,
          aggregate: normalizedAggregate,
          criteria: Array.from({ length: normalizedCriteriaCount }, (_, index) => column.criteria?.[index] ?? '')
        };
      });

      setCriteriaRowCount(normalizedCriteriaCount);
      setGroupingEnabled(groupingFromDraft);
      setNodes(restoredNodes);
      setEdges(restoredEdges);
      setSelectedFieldOrder(restoredFieldOrder);
      setFieldSettings(restoredFieldSettings);
      setStatusBanner({
        message: 'Restored saved reporting draft from your last session.',
        color: 'success.main'
      });
    } catch (error) {
      console.warn('Unable to hydrate reporting designer draft', error);
      setStatusBanner({
        message: 'Stored draft could not be restored. Starting with a fresh canvas.',
        color: 'warning.main'
      });
    } finally {
      hasHydratedDraftRef.current = true;
    }
  }, [
    createNodeData,
    fieldIdSet,
    fields.length,
    fieldsByTable,
    joinEdgePresentation,
    nodes.length,
    tables.length,
    tablesById,
    setNodes,
    setEdges,
    setSelectedFieldOrder,
    setFieldSettings,
    setStatusBanner,
    setCriteriaRowCount,
    setGroupingEnabled
  ]);

  useEffect(() => {
    setEdges((current) =>
      current.map((edge) => ({
        ...edge,
        ...joinEdgePresentation,
        style: { ...joinEdgePresentation.style },
        markerEnd: { ...joinEdgePresentation.markerEnd }
      }))
    );
  }, [joinEdgePresentation, setEdges]);

  const canvasPositionFor = useCallback((index: number): XYPosition => ({
    x: (index % 3) * 260,
    y: Math.floor(index / 3) * 200
  }), []);

  const removeNodeById = useCallback((nodeId: string) => {
    const tableName = tablesById.get(nodeId)?.name ?? 'Table';

    setNodes((current) => current.filter((node) => node.id !== nodeId));
    setEdges((current) => current.filter((edge) => edge.source !== nodeId && edge.target !== nodeId));
    setSelectedFieldOrder((current) => current.filter((fieldId) => fieldsById.get(fieldId)?.tableId !== nodeId));
    setFieldSettings((current) => {
      const relatedFields = fieldsByTable.get(nodeId) ?? [];
      if (relatedFields.length === 0) {
        return current;
      }
      const next = { ...current };
      relatedFields.forEach((field) => {
        delete next[field.id];
      });
      return next;
    });

    setStatusBanner({
      message: `${tableName} removed from the Relationship Canvas.`,
      color: 'warning.main'
    });
  }, [fieldsById, fieldsByTable, setEdges, setFieldSettings, setNodes, setSelectedFieldOrder, setStatusBanner, tablesById]);

  const handleRemoveNode = useCallback((nodeId: string) => {
    const tableName = tablesById.get(nodeId)?.name ?? 'this table';
    const proceed = typeof window === 'undefined'
      ? true
      : window.confirm(`Remove ${tableName} from the Relationship Canvas? Joins and selected columns that reference it will also be cleared.`);
    if (!proceed) {
      return;
    }
    removeNodeById(nodeId);
  }, [removeNodeById, tablesById]);

  const addTableNode = useCallback((table: Table, position?: XYPosition): DesignerNode | null => {
    let createdNode: DesignerNode | null = null;

    setNodes((current) => {
      const existing = current.find((node) => node.id === table.id);
      if (existing) {
        createdNode = existing;
        return current;
      }

      const nextPosition = position ?? canvasPositionFor(current.length);
      const tableFields = fieldsByTable.get(table.id) ?? [];

      const next: DesignerNode = {
        id: table.id,
        type: 'reportTable',
        position: nextPosition,
        draggable: true,
        selectable: true,
        width: defaultNodeWidth,
        height: defaultNodeHeight,
        style: {
          width: defaultNodeWidth,
          height: defaultNodeHeight,
          minWidth: minNodeWidth,
          minHeight: minNodeHeight
        },
        data: {
          ...createNodeData(table, tableFields),
          onRemoveTable: handleRemoveNode
        }
      };

      createdNode = next;
      return [...current, next];
    });

    return createdNode;
  }, [canvasPositionFor, createNodeData, fieldsByTable, handleRemoveNode, setNodes]);

  const focusNode = useCallback((node: DesignerNode | null) => {
    const instance = reactFlowInstanceRef.current;
    if (!node) {
      return;
    }
    if (!instance) {
      return;
    }

    requestAnimationFrame(() => {
      const nodeWidth = node.width ?? (typeof node.style?.width === 'number' ? node.style.width : defaultNodeWidth);
      const nodeHeight = node.height ?? (typeof node.style?.height === 'number' ? node.style.height : defaultNodeHeight);
      const centerX = node.position.x + nodeWidth / 2;
      const centerY = node.position.y + nodeHeight / 2;
      if (typeof instance.setCenter === 'function') {
        instance.setCenter(centerX, centerY, { zoom: 1, duration: 200 });
      } else if (typeof instance.fitView === 'function') {
        instance.fitView({ nodes: [node], padding: 0.4, duration: 200 });
      }
    });
  }, []);

  const handlePaletteAdd = useCallback((table: Table) => {
    const node = addTableNode(table);
    focusNode(node);
  }, [addTableNode, focusNode]);

  useEffect(() => {
    setNodes((current) => {
      if (current.length === 0) {
        return current;
      }

      let didChange = false;

      const nextNodes = current.map((node) => {
        const table = tables.find((item) => item.id === node.id);
        if (!table) {
          return node;
        }

        const tableFields = fieldsByTable.get(node.id) ?? [];
        const updatedData = createNodeData(table, tableFields);

        const existingData = node.data as ReportTableNodeData;
        const sameMeta =
          existingData.label === updatedData.label &&
          existingData.subtitle === updatedData.subtitle &&
          existingData.meta === updatedData.meta;

        const sameFields =
          Array.isArray(existingData.fields) &&
          existingData.fields.length === updatedData.fields.length &&
          existingData.fields.every((field, index) => {
            const other = updatedData.fields[index];
            return (
              field.id === other.id &&
              field.name === other.name &&
              field.type === other.type &&
              field.description === other.description
            );
          });

        if (sameMeta && sameFields) {
          return node;
        }

        didChange = true;
        return {
          ...node,
          data: {
            ...updatedData,
            selectedFieldIds: existingData.selectedFieldIds,
            onFieldAdd: existingData.onFieldAdd,
            onFieldDragEnd: existingData.onFieldDragEnd,
            onFieldDragStart: existingData.onFieldDragStart,
            onFieldJoin: existingData.onFieldJoin,
            onFieldToggle: existingData.onFieldToggle,
            onRemoveTable: existingData.onRemoveTable ?? handleRemoveNode
          },
          draggable: true,
          selectable: true
        };
      });

      return didChange ? nextNodes : current;
    });
  }, [createNodeData, fieldsByTable, handleRemoveNode, setNodes, tables]);

  const filteredTables = useMemo(() => {
    const term = searchTerm.trim().toLowerCase();
    if (!term) {
      return tables;
    }
    return tables.filter((item) =>
      item.name.toLowerCase().includes(term) ||
      item.physicalName.toLowerCase().includes(term) ||
      (item.schemaName ? item.schemaName.toLowerCase().includes(term) : false)
    );
  }, [searchTerm, tables]);

  const nodesById = useMemo(() => new Set(nodes.map((node) => node.id)), [nodes]);

  const aggregateOptions = useMemo<{ label: string; value: ReportAggregateFn }[]>(
    () => [
      { label: 'Group By', value: 'groupBy' },
      { label: 'Sum', value: 'sum' },
      { label: 'Avg', value: 'avg' },
      { label: 'Min', value: 'min' },
      { label: 'Max', value: 'max' },
      { label: 'Count', value: 'count' },
      { label: 'First', value: 'first' },
      { label: 'Last', value: 'last' },
      { label: 'Expression', value: 'expression' },
      { label: 'Where', value: 'where' }
    ],
    []
  );

  const joinTypeOptions = useMemo<{ value: JoinType; label: string; helper: string }[]>(
    () => [
      {
        value: 'inner',
        label: 'Inner Join',
        helper: 'Only include rows where the joined fields match in both tables.'
      },
      {
        value: 'left',
        label: 'Left Join',
        helper: 'Include all rows from the left table and matching rows from the right table.'
      },
      {
        value: 'right',
        label: 'Right Join',
        helper: 'Include all rows from the right table and matching rows from the left table.'
      }
    ],
    []
  );

  const canConfirmJoin = Boolean(joinDialogState?.sourceFieldId && joinDialogState?.targetFieldId);
  const joinDialogSourceName = joinDialogState
    ? tablesById.get(joinDialogState.sourceTableId)?.name ?? joinDialogState.sourceTableId
    : '';
  const joinDialogTargetName = joinDialogState
    ? tablesById.get(joinDialogState.targetTableId)?.name ?? joinDialogState.targetTableId
    : '';
  const joinDialogMode = joinDialogState?.mode ?? 'create';
  const joinDialogIsEdit = joinDialogMode === 'edit';
  const joinDialogTitleText = joinDialogIsEdit ? 'Edit Join' : 'Configure Join';
  const joinDialogConfirmLabel = joinDialogIsEdit ? 'Save Changes' : 'Add Join';
  const joinDialogDescription = joinDialogIsEdit
    ? `Update how ${joinDialogSourceName} links to ${joinDialogTargetName}.`
    : `Define how ${joinDialogSourceName} links to ${joinDialogTargetName}.`;

  const selectedFieldSet = useMemo(() => new Set(selectedFieldOrder), [selectedFieldOrder]);

  const ensureFieldAdded = useCallback((fieldId: string) => {
    if (!fieldIdSet.has(fieldId) || selectedFieldSet.has(fieldId)) {
      return;
    }

    setSelectedFieldOrder((current) => (current.includes(fieldId) ? current : [...current, fieldId]));

    setFieldSettings((current) => {
      const existing = current[fieldId];
      const nextCriteria = Array.from({ length: criteriaRowCount }, (_, index) => existing?.criteria[index] ?? '');

      if (existing) {
        const hasMatchingCriteria = existing.criteria.length === criteriaRowCount && existing.criteria.every((value, index) => value === nextCriteria[index]);
        const aggregateValue = groupingEnabled ? existing.aggregate ?? 'groupBy' : null;
        if (hasMatchingCriteria && existing.aggregate === aggregateValue) {
          return current;
        }
        return {
          ...current,
          [fieldId]: {
            ...existing,
            aggregate: aggregateValue,
            criteria: nextCriteria
          }
        };
      }

      return {
        ...current,
        [fieldId]: {
          show: true,
          sort: 'none',
          aggregate: groupingEnabled ? 'groupBy' : null,
          criteria: nextCriteria
        }
      };
    });
  }, [criteriaRowCount, fieldIdSet, groupingEnabled, selectedFieldSet]);

  const handleFieldDragStart = useCallback((payload: FieldDragPayload) => {
    draggedFieldRef.current = payload;
  }, []);

  const handleFieldDragEnd = useCallback(() => {
    draggedFieldRef.current = null;
  }, []);

  const handleFieldJoin = useCallback((source: FieldDragPayload | null, target: FieldDragPayload) => {
    const resolvedSource = source ?? draggedFieldRef.current;
    if (!resolvedSource) {
      setStatusBanner({
        message: 'Drag a field from another table to define a join.',
        color: 'warning.main'
      });
      return;
    }

    const sourceField = fieldsById.get(resolvedSource.fieldId);
    const targetField = fieldsById.get(target.fieldId);

    if (!sourceField || !targetField) {
      setStatusBanner({
        message: 'Unable to resolve field metadata for the requested join.',
        color: 'error.main'
      });
      draggedFieldRef.current = null;
      return;
    }

    const sourceTableId = sourceField.tableId;
    const targetTableId = targetField.tableId;

    if (sourceTableId === targetTableId) {
      setStatusBanner({
        message: 'Select fields from different tables to create a join.',
        color: 'warning.main'
      });
      draggedFieldRef.current = null;
      return;
    }

    if (!nodesById.has(sourceTableId) || !nodesById.has(targetTableId)) {
      setStatusBanner({
        message: 'Add both tables to the canvas before defining a join.',
        color: 'warning.main'
      });
      draggedFieldRef.current = null;
      return;
    }

    const alreadyExists = edges.some((edge) => {
      const matchesDirect = edge.source === sourceTableId && edge.target === targetTableId;
      const matchesReverse = edge.source === targetTableId && edge.target === sourceTableId;
      if (!matchesDirect && !matchesReverse) {
        return false;
      }

      const edgeData = edge.data as { sourceFieldId?: string; targetFieldId?: string } | undefined;
      if (!edgeData) {
        return false;
      }

      const directMatch =
        matchesDirect &&
        edgeData.sourceFieldId === resolvedSource.fieldId &&
        edgeData.targetFieldId === target.fieldId;

      const reverseMatch =
        matchesReverse &&
        edgeData.sourceFieldId === target.fieldId &&
        edgeData.targetFieldId === resolvedSource.fieldId;

      return directMatch || reverseMatch;
    });

    if (alreadyExists) {
      const sourceTableName = tablesById.get(sourceTableId)?.name ?? sourceTableId;
      const targetTableName = tablesById.get(targetTableId)?.name ?? targetTableId;
      setStatusBanner({
        message: `A join between ${sourceTableName}.${sourceField.name} and ${targetTableName}.${targetField.name} already exists.`,
        color: 'text.secondary'
      });
      draggedFieldRef.current = null;
      return;
    }

    setJoinDialogState({
      mode: 'create',
      sourceTableId,
      sourceFieldId: sourceField.id,
      targetTableId,
      targetFieldId: targetField.id,
      joinType: 'inner'
    });

    draggedFieldRef.current = null;
  }, [edges, fieldsById, nodesById, setStatusBanner, tablesById]);

  const handleJoinDialogClose = useCallback(() => {
    draggedFieldRef.current = null;
    setJoinDialogState(null);
  }, []);

  const handleJoinDialogFieldChange = useCallback((side: 'source' | 'target', fieldId: string) => {
    setJoinDialogState((current) => {
      if (!current) {
        return current;
      }
      return side === 'source'
        ? { ...current, sourceFieldId: fieldId }
        : { ...current, targetFieldId: fieldId };
    });
  }, []);

  const handleJoinDialogTypeChange = useCallback((value: JoinType) => {
    setJoinDialogState((current) => (current ? { ...current, joinType: value } : current));
  }, []);

  const handleJoinDialogConfirm = useCallback(() => {
    if (!joinDialogState) {
      return;
    }

    const { mode, edgeId, sourceTableId, sourceFieldId, targetTableId, targetFieldId, joinType } = joinDialogState;
    const isEditMode = mode === 'edit';
    const sourceField = fieldsById.get(sourceFieldId);
    const targetField = fieldsById.get(targetFieldId);

    if (!sourceField || !targetField) {
      setStatusBanner({
        message: 'Unable to resolve field metadata for the requested join.',
        color: 'error.main'
      });
      draggedFieldRef.current = null;
      setJoinDialogState(null);
      return;
    }

    let duplicate = false;
    let created = false;
    let updated = false;

    setEdges((existing) => {
      const duplicateEdge = existing.some((edge) => {
        if (isEditMode && edge.id === edgeId) {
          return false;
        }

        const matchesDirect = edge.source === sourceTableId && edge.target === targetTableId;
        const matchesReverse = edge.source === targetTableId && edge.target === sourceTableId;
        if (!matchesDirect && !matchesReverse) {
          return false;
        }

        const edgeData = edge.data as JoinEdgeData | undefined;
        if (!edgeData) {
          return false;
        }

        const directMatch =
          matchesDirect &&
          edgeData.sourceFieldId === sourceFieldId &&
          edgeData.targetFieldId === targetFieldId;

        const reverseMatch =
          matchesReverse &&
          edgeData.sourceFieldId === targetFieldId &&
          edgeData.targetFieldId === sourceFieldId;

        return directMatch || reverseMatch;
      });

      if (duplicateEdge) {
        duplicate = true;
        return existing;
      }

      if (isEditMode && edgeId) {
        let didChange = false;
        const nextEdges = existing.map((edge) => {
          if (edge.id !== edgeId) {
            return edge;
          }

          const nextEdge = {
            ...edge,
            source: sourceTableId,
            target: targetTableId,
            sourceHandle: `source:${sourceFieldId}`,
            targetHandle: `target:${targetFieldId}`,
            data: {
              sourceFieldId,
              targetFieldId,
              joinType
            },
            ...joinEdgePresentation,
            style: { ...joinEdgePresentation.style },
            markerEnd: { ...joinEdgePresentation.markerEnd }
          };

          const currentData = edge.data as JoinEdgeData | undefined;
          const sameConnection =
            edge.source === nextEdge.source &&
            edge.target === nextEdge.target &&
            edge.sourceHandle === nextEdge.sourceHandle &&
            edge.targetHandle === nextEdge.targetHandle &&
            (currentData?.sourceFieldId ?? null) === sourceFieldId &&
            (currentData?.targetFieldId ?? null) === targetFieldId &&
            ((currentData?.joinType as JoinType | undefined) ?? 'inner') === joinType;

          if (!sameConnection) {
            didChange = true;
          }

          return nextEdge;
        });

        updated = didChange;
        return nextEdges;
      }

      const existingIds = new Set(existing.map((edge) => edge.id));
      const idBase = `join:${sourceTableId}:${targetTableId}`;
      let nextEdgeId = idBase;
      let counter = 1;
      while (existingIds.has(nextEdgeId)) {
        nextEdgeId = `${idBase}:${counter++}`;
      }

      created = true;
      return [
        ...existing,
        {
          id: nextEdgeId,
          source: sourceTableId,
          target: targetTableId,
          sourceHandle: `source:${sourceFieldId}`,
          targetHandle: `target:${targetFieldId}`,
          data: {
            sourceFieldId,
            targetFieldId,
            joinType
          },
          ...joinEdgePresentation,
          style: { ...joinEdgePresentation.style },
          markerEnd: { ...joinEdgePresentation.markerEnd }
        }
      ];
    });

    const sourceTableName = tablesById.get(sourceTableId)?.name ?? sourceTableId;
    const targetTableName = tablesById.get(targetTableId)?.name ?? targetTableId;

    if (duplicate) {
      setStatusBanner({
        message: `A join between ${sourceTableName}.${sourceField.name} and ${targetTableName}.${targetField.name} already exists.`,
        color: 'text.secondary'
      });
      return;
    }

    const joinLabel = joinTypeOptions.find((option) => option.value === joinType)?.label ?? 'Join';

    if (isEditMode) {
      if (updated) {
        setStatusBanner({
          message: `${joinLabel} updated: ${sourceTableName}.${sourceField.name} ↔ ${targetTableName}.${targetField.name}.`,
          color: 'success.main'
        });
      } else {
        setStatusBanner({
          message: 'No changes detected for the selected join.',
          color: 'text.secondary'
        });
      }
    } else if (created) {
      setStatusBanner({
        message: `${joinLabel} added: ${sourceTableName}.${sourceField.name} ↔ ${targetTableName}.${targetField.name}.`,
        color: 'success.main'
      });
    }

    draggedFieldRef.current = null;
    setJoinDialogState(null);
  }, [fieldsById, joinDialogState, joinEdgePresentation, joinTypeOptions, setEdges, setStatusBanner, tablesById]);

  const handleJoinDialogDelete = useCallback(() => {
    if (!joinDialogState || joinDialogState.mode !== 'edit') {
      return;
    }

    const { edgeId, sourceTableId, sourceFieldId, targetTableId, targetFieldId } = joinDialogState;
    const sourceTableName = tablesById.get(sourceTableId)?.name ?? sourceTableId;
    const targetTableName = tablesById.get(targetTableId)?.name ?? targetTableId;
    const sourceFieldName = fieldsById.get(sourceFieldId)?.name ?? sourceFieldId;
    const targetFieldName = fieldsById.get(targetFieldId)?.name ?? targetFieldId;

    const proceed = typeof window === 'undefined'
      ? true
      : window.confirm(`Remove the join between ${sourceTableName}.${sourceFieldName} and ${targetTableName}.${targetFieldName}?`);

    if (!proceed) {
      return;
    }

    setEdges((current) => current.filter((edge) => {
      if (edgeId && edge.id === edgeId) {
        return false;
      }

      const matchesDirect = edge.source === sourceTableId && edge.target === targetTableId;
      const matchesReverse = edge.source === targetTableId && edge.target === sourceTableId;
      if (!matchesDirect && !matchesReverse) {
        return true;
      }

      const edgeData = edge.data as JoinEdgeData | undefined;
      if (!edgeData) {
        return true;
      }

      const directMatch =
        matchesDirect &&
        edgeData.sourceFieldId === sourceFieldId &&
        edgeData.targetFieldId === targetFieldId;

      const reverseMatch =
        matchesReverse &&
        edgeData.sourceFieldId === targetFieldId &&
        edgeData.targetFieldId === sourceFieldId;

      return !(directMatch || reverseMatch);
    }));

    setStatusBanner({
      message: `Join removed: ${sourceTableName}.${sourceFieldName} ↔ ${targetTableName}.${targetFieldName}.`,
      color: 'warning.main'
    });

    draggedFieldRef.current = null;
    setJoinDialogState(null);
  }, [fieldsById, joinDialogState, setEdges, setJoinDialogState, setStatusBanner, tablesById]);

  const handleConnect = useCallback((connection: Connection) => {
    const { source, target, sourceHandle, targetHandle } = connection;
    if (!source || !target) {
      setStatusBanner({
        message: 'Select both a source and target field to define a join.',
        color: 'warning.main'
      });
      return;
    }

    const sourceFieldId = extractFieldIdFromHandle(sourceHandle);
    const targetFieldId = extractFieldIdFromHandle(targetHandle);

    if (!sourceFieldId || !targetFieldId) {
      setStatusBanner({
        message: 'Use the field-level connection points to define joins.',
        color: 'warning.main'
      });
      return;
    }

    handleFieldJoin({ tableId: source, fieldId: sourceFieldId }, { tableId: target, fieldId: targetFieldId });
  }, [extractFieldIdFromHandle, handleFieldJoin, setStatusBanner]);

  const handleEdgeClick = useCallback((event: MouseEvent, edge: Edge) => {
    event.stopPropagation();

    if (!edge.source || !edge.target) {
      return;
    }

    const edgeData = edge.data as JoinEdgeData | undefined;
    const sourceFieldId = edgeData?.sourceFieldId;
    const targetFieldId = edgeData?.targetFieldId;

    if (!sourceFieldId || !targetFieldId) {
      setStatusBanner({
        message: 'Only joins created in the designer can be edited.',
        color: 'warning.main'
      });
      return;
    }

    setJoinDialogState({
      mode: 'edit',
      edgeId: edge.id,
      sourceTableId: edge.source,
      sourceFieldId,
      targetTableId: edge.target,
      targetFieldId,
      joinType: edgeData?.joinType ?? 'inner'
    });
  }, [setStatusBanner]);

  const selectedFieldColumns = useMemo<SelectedFieldColumn[]>(() => {
    const columns: SelectedFieldColumn[] = [];
    selectedFieldOrder.forEach((fieldId) => {
      const field = fieldsById.get(fieldId);
      if (!field) {
        return;
      }
      const table = tablesById.get(field.tableId);
      const settings = fieldSettings[fieldId] ?? {
        show: true,
        sort: 'none',
        aggregate: groupingEnabled ? 'groupBy' : null,
        criteria: Array(criteriaRowCount).fill('')
      };
      columns.push({ fieldId, field, table, settings });
    });
    return columns;
  }, [criteriaRowCount, fieldSettings, fieldsById, groupingEnabled, selectedFieldOrder, tablesById]);

  const outputColumnSensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 6 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  );

  const outputColumnRowDescriptors = useMemo<OutputColumnRowDescriptor[]>(() => {
    const baseRow = (key: string, label: string, options?: Partial<OutputColumnRowDescriptor>): OutputColumnRowDescriptor => ({
      key,
      label,
      paddingY: 0,
      minHeight: 56,
      ...options
    });

    const rows: OutputColumnRowDescriptor[] = [
      baseRow('field', 'Field'),
      baseRow('table', 'Table')
    ];

    if (groupingEnabled) {
      rows.push(baseRow('group', 'Group By'));
    }

    rows.push(baseRow('sort', 'Sort'));
    rows.push(baseRow('show', 'Show', { justifyContent: 'center' }));

    for (let index = 0; index < criteriaRowCount; index += 1) {
      rows.push(baseRow(`criteria-${index}`, index === 0 ? 'Criteria' : 'Or'));
    }

    return rows;
  }, [criteriaRowCount, groupingEnabled]);

  const handleOutputColumnsDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) {
      return;
    }

    const activeId = String(active.id);
    const overId = String(over.id);

    setSelectedFieldOrder((current) => {
      const oldIndex = current.indexOf(activeId);
      const newIndex = current.indexOf(overId);
      if (oldIndex === -1 || newIndex === -1 || oldIndex === newIndex) {
        return current;
      }
      const next = arrayMove(current, oldIndex, newIndex);
      setStatusBanner({
        message: 'Reordered selected columns.',
        color: 'text.secondary'
      });
      return next;
    });
  }, [setSelectedFieldOrder, setStatusBanner]);

  const reportDefinition = useMemo<ReportDesignerDefinition>(() => {
    const tableSnapshots = nodes.map((node) => {
      const table = tablesById.get(node.id);
      const width = node.width ?? (typeof node.style?.width === 'number' ? node.style.width : undefined);
      const height = node.height ?? (typeof node.style?.height === 'number' ? node.style.height : undefined);
      const dimensions =
        typeof width === 'number' || typeof height === 'number'
          ? {
              ...(typeof width === 'number' ? { width } : {}),
              ...(typeof height === 'number' ? { height } : {})
            }
          : undefined;
      return {
        tableId: node.id,
        label: table?.name ?? node.data.label,
        physicalName: table?.physicalName ?? node.data.subtitle ?? node.data.label,
        schemaName: table?.schemaName ?? null,
        alias: null,
        position: {
          x: node.position.x,
          y: node.position.y
        },
        ...(dimensions ? { dimensions } : {})
      };
    });

    const joinSnapshots = edges.map((edge) => {
      const edgeData = edge.data as { sourceFieldId?: string; targetFieldId?: string; joinType?: JoinType } | undefined;
      return {
        id: edge.id ?? `${edge.source}-${edge.target}`,
        sourceTableId: edge.source,
        targetTableId: edge.target,
        sourceFieldId: edgeData?.sourceFieldId ?? null,
        targetFieldId: edgeData?.targetFieldId ?? null,
        joinType: edgeData?.joinType ?? 'inner'
      };
    });

    const columnSnapshots = selectedFieldColumns.map((column, index) => ({
      order: index,
      fieldId: column.fieldId,
      fieldName: column.field.name,
      fieldDescription: column.field.description ?? null,
      tableId: column.field.tableId,
      tableName: column.table?.name ?? null,
      show: column.settings.show,
      sort: column.settings.sort,
      aggregate: column.settings.aggregate ?? null,
      criteria: column.settings.criteria.map((value) => value.trim())
    }));

    return {
      tables: tableSnapshots,
      joins: joinSnapshots,
      columns: columnSnapshots,
      criteriaRowCount,
      groupingEnabled
    } satisfies ReportDesignerDefinition;
  }, [criteriaRowCount, edges, groupingEnabled, nodes, selectedFieldColumns, tablesById]);

  const handleSortChange = useCallback((fieldId: string, sort: SortDirection) => {
    setFieldSettings((current) => {
      const existing = current[fieldId];
      if (!existing || existing.sort === sort) {
        if (!existing && sort === 'none') {
          return current;
        }
        if (existing && existing.sort === sort) {
          return current;
        }
      }
      const base = existing ?? {
        show: true,
        sort: 'none' as SortDirection,
        aggregate: groupingEnabled ? 'groupBy' : null,
        criteria: Array(criteriaRowCount).fill('')
      };
      return {
        ...current,
        [fieldId]: {
          ...base,
          aggregate: groupingEnabled ? base.aggregate ?? 'groupBy' : null,
          sort
        }
      };
    });
  }, [criteriaRowCount, groupingEnabled]);

  const handleShowToggle = useCallback((fieldId: string, value: boolean) => {
    setFieldSettings((current) => {
      const existing = current[fieldId];
      if (!existing) {
        return {
          ...current,
          [fieldId]: {
            show: value,
            sort: 'none',
            aggregate: groupingEnabled ? 'groupBy' : null,
            criteria: Array(criteriaRowCount).fill('')
          }
        };
      }
      if (existing.show === value) {
        return current;
      }
      return {
        ...current,
        [fieldId]: {
          ...existing,
          aggregate: groupingEnabled ? existing.aggregate ?? 'groupBy' : null,
          show: value
        }
      };
    });
  }, [criteriaRowCount, groupingEnabled]);

  const handleCriteriaChange = useCallback((fieldId: string, index: number, value: string) => {
    setFieldSettings((current) => {
      const existing = current[fieldId];
      if (!existing) {
        const criteria = Array(criteriaRowCount).fill('');
        criteria[index] = value;
        return {
          ...current,
          [fieldId]: {
            show: true,
            sort: 'none',
            aggregate: groupingEnabled ? 'groupBy' : null,
            criteria
          }
        };
      }
      if (existing.criteria[index] === value) {
        return current;
      }
      const criteria = existing.criteria.map((item, idx) => (idx === index ? value : item));
      return {
        ...current,
        [fieldId]: {
          ...existing,
          aggregate: groupingEnabled ? existing.aggregate ?? 'groupBy' : null,
          criteria
        }
      };
    });
  }, [criteriaRowCount, groupingEnabled]);

  const handleAggregateChange = useCallback((fieldId: string, aggregate: ReportAggregateFn) => {
    if (!groupingEnabled) {
      return;
    }

    setFieldSettings((current) => {
      const existing = current[fieldId];
      if (!existing) {
        return {
          ...current,
          [fieldId]: {
            show: true,
            sort: 'none',
            aggregate,
            criteria: Array(criteriaRowCount).fill('')
          }
        };
      }
      if (existing.aggregate === aggregate) {
        return current;
      }
      return {
        ...current,
        [fieldId]: {
          ...existing,
          aggregate
        }
      };
    });
  }, [criteriaRowCount, groupingEnabled]);

  const handleAddCriteriaRow = useCallback(() => {
    setCriteriaRowCount((count) => Math.min(count + 1, 5));
  }, []);

  const handleRemoveCriteriaRow = useCallback(() => {
    setCriteriaRowCount((count) => Math.max(1, count - 1));
  }, []);

  const toggleField = useCallback((fieldId: string) => {
    const field = fieldsById.get(fieldId);
    if (!field) {
      return;
    }

    const removing = selectedFieldSet.has(fieldId);

    setNodes((current) => current.map((node) => {
      if (node.id !== field.tableId) {
        return node;
      }

      const nodeData = node.data as ReportTableNodeData;
      const currentSelected = nodeData.selectedFieldIds ?? [];
      const nextSelected = removing
        ? currentSelected.filter((id) => id !== fieldId)
        : currentSelected.includes(fieldId)
          ? currentSelected
          : [...currentSelected, fieldId];

      if (
        currentSelected.length === nextSelected.length &&
        currentSelected.every((value, index) => value === nextSelected[index])
      ) {
        return node;
      }

      return {
        ...node,
        data: {
          ...nodeData,
          selectedFieldIds: nextSelected,
          onFieldAdd: ensureFieldAdded,
          onFieldDragEnd: handleFieldDragEnd,
          onFieldDragStart: handleFieldDragStart,
          onFieldJoin: handleFieldJoin,
          onFieldToggle: toggleField,
          onRemoveTable: handleRemoveNode
        }
      };
    }));

    if (removing) {
      setSelectedFieldOrder((current) => current.filter((id) => id !== fieldId));
      setFieldSettings((current) => {
        if (!(fieldId in current)) {
          return current;
        }
        const next = { ...current };
        delete next[fieldId];
        return next;
      });
      return;
    }

    ensureFieldAdded(fieldId);
  }, [ensureFieldAdded, fieldsById, handleFieldDragEnd, handleFieldDragStart, handleFieldJoin, handleRemoveNode, selectedFieldSet, setNodes]);

  useEffect(() => {
    setNodes((current) => {
      if (current.length === 0) {
        return current;
      }

      let mutated = false;

      const nextNodes = current.map((node) => {
        const nodeData = node.data as ReportTableNodeData;
        const tableFieldSet = new Set((fieldsByTable.get(node.id) ?? []).map((field) => field.id));
        const selectedIds = selectedFieldOrder.filter((fieldId) => tableFieldSet.has(fieldId));

        const existingSelected = nodeData.selectedFieldIds ?? [];
        const sameSelection = existingSelected.length === selectedIds.length && existingSelected.every((value, index) => value === selectedIds[index]);
        const sameAddHandler = nodeData.onFieldAdd === ensureFieldAdded;
        const sameToggleHandler = nodeData.onFieldToggle === toggleField;
        const sameJoinHandler = nodeData.onFieldJoin === handleFieldJoin;
        const sameDragStartHandler = nodeData.onFieldDragStart === handleFieldDragStart;
        const sameDragEndHandler = nodeData.onFieldDragEnd === handleFieldDragEnd;
        const sameRemoveHandler = nodeData.onRemoveTable === handleRemoveNode;

        if (sameSelection && sameAddHandler && sameToggleHandler && sameJoinHandler && sameDragStartHandler && sameDragEndHandler && sameRemoveHandler) {
          return node;
        }

        mutated = true;

        return {
          ...node,
          data: {
            ...nodeData,
            selectedFieldIds: selectedIds,
            onFieldAdd: ensureFieldAdded,
            onFieldDragEnd: handleFieldDragEnd,
            onFieldDragStart: handleFieldDragStart,
            onFieldJoin: handleFieldJoin,
            onFieldToggle: toggleField,
            onRemoveTable: handleRemoveNode
          }
        };
      });

      return mutated ? nextNodes : current;
    });
  }, [ensureFieldAdded, fieldsByTable, handleFieldDragEnd, handleFieldDragStart, handleFieldJoin, handleRemoveNode, nodes, selectedFieldOrder, setNodes, toggleField]);

  const selectedFieldCount = selectedFieldColumns.length;
  const actionButtonsDisabled = nodes.length === 0 || selectedFieldCount === 0;

  const handlePreviewResults = useCallback(async () => {
    setPreviewOpen(true);
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);

    try {
      const result = await fetchReportPreview(reportDefinition, 100);
      setPreviewData(result);
      setStatusBanner({
        message: `Preview ready. Displaying ${result.rowCount} row${result.rowCount === 1 ? '' : 's'} (limit ${result.limit}).`,
        color: 'success.main'
      });
    } catch (error) {
      const detail = isAxiosError(error)
        ? (error.response?.data?.detail as string | undefined) ?? error.message
        : error instanceof Error
          ? error.message
          : 'Unable to run the preview. Please try again.';
      setPreviewError(detail);
      setStatusBanner({
        message: 'Preview failed. Check the dialog for details.',
        color: 'error.main'
      });
    } finally {
      setPreviewLoading(false);
    }
  }, [reportDefinition, setStatusBanner]);

  const handleSaveDraft = useCallback(() => {
    if (typeof window === 'undefined') {
      setStatusBanner({
        message: 'Unable to persist draft outside a browser environment.',
        color: 'error.main'
      });
      return;
    }

    try {
      window.localStorage.setItem(designerStorageKey, JSON.stringify(reportDefinition));
      setStatusBanner({
        message: 'Draft stored locally. Replace this with a reporting API call once backend endpoints land.',
        color: 'success.main'
      });
    } catch (error) {
      console.error('Failed to persist reporting draft', error);
      setStatusBanner({
        message: 'Unable to persist draft locally. Refer to console for details.',
        color: 'error.main'
      });
    }
  }, [reportDefinition]);

  const handlePreviewClose = useCallback(() => {
    setPreviewOpen(false);
    setPreviewLoading(false);
    setPreviewError(null);
    setPreviewData(null);
  }, []);

  const handleCopyPreviewSql = useCallback(() => {
    if (!previewData?.sql) {
      return;
    }

    if (typeof navigator !== 'undefined' && navigator.clipboard) {
      navigator.clipboard.writeText(previewData.sql).then(() => {
        setStatusBanner({
          message: 'Preview SQL copied to clipboard.',
          color: 'success.main'
        });
      }).catch(() => {
        setStatusBanner({
          message: 'Unable to copy preview SQL to clipboard.',
          color: 'error.main'
        });
      });
    }
  }, [previewData, setStatusBanner]);

  const formatPreviewValue = useCallback((value: unknown): string => {
    if (value === null || value === undefined) {
      return '—';
    }
    if (value instanceof Date) {
      return value.toISOString();
    }
    if (typeof value === 'object') {
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }
    return String(value);
  }, []);

  const handlePublish = useCallback(() => {
    console.groupCollapsed('Reporting designer publish payload');
    console.info(reportDefinition);
    console.groupEnd();
    setStatusBanner({
      message: 'Publish workflow is not wired yet. Payload logged for integration.',
      color: 'text.secondary'
    });
  }, [reportDefinition]);

  const handleGroupingToggle = useCallback(() => {
    setGroupingEnabled((prev) => {
      const next = !prev;

      setFieldSettings((current) => {
        if (!next) {
          let changed = false;
          const reset: Record<string, FieldColumnSettings> = {};
          Object.entries(current).forEach(([fieldId, existing]) => {
            if (!existing) {
              return;
            }
            if (existing.aggregate) {
              changed = true;
            }
            reset[fieldId] = {
              ...existing,
              aggregate: null
            };
          });
          if (!changed) {
            return current;
          }
          return reset;
        }

        let changed = false;
        const seeded: Record<string, FieldColumnSettings> = { ...current };
        selectedFieldOrder.forEach((fieldId) => {
          const existing = current[fieldId];
          if (existing && existing.aggregate) {
            return;
          }
          changed = true;
          seeded[fieldId] = {
            ...(existing ?? {
              show: true,
              sort: 'none',
              criteria: Array(criteriaRowCount).fill('')
            }),
            aggregate: 'groupBy'
          };
        });
        return changed ? seeded : current;
      });

      setStatusBanner({
        message: next
          ? 'Grouping enabled. Configure aggregates for each output column.'
          : 'Grouping disabled. Aggregates cleared from output columns.',
        color: 'text.secondary'
      });

      return next;
    });
  }, [criteriaRowCount, selectedFieldOrder, setFieldSettings, setStatusBanner]);

  const handleResetDraft = useCallback(() => {
    const proceed = typeof window === 'undefined'
      ? true
      : window.confirm('Clear the current reporting draft? This removes your local snapshot and resets the canvas.');

    if (!proceed) {
      return;
    }

    if (typeof window !== 'undefined') {
      window.localStorage.removeItem(designerStorageKey);
    }

    setNodes([]);
    setEdges([]);
    setSelectedFieldOrder([]);
    setFieldSettings({});
    setCriteriaRowCount(2);
    setGroupingEnabled(false);
    setPreviewOpen(false);
    setPreviewData(null);
    setPreviewError(null);
    setPreviewLoading(false);
    hasHydratedDraftRef.current = true;
    setStatusBanner({
      message: 'Draft cleared and canvas reset.',
      color: 'warning.main'
    });
  }, [setEdges, setFieldSettings, setGroupingEnabled, setNodes, setSelectedFieldOrder, setStatusBanner]);

  const handleDragStart = useCallback((event: React.DragEvent<HTMLElement>, table: Table) => {
    const payload: DragPayload = { tableId: table.id };
  event.dataTransfer.setData(tableDragMimeType, JSON.stringify(payload));
    event.dataTransfer.effectAllowed = 'move';
  }, []);

  const handleDragOver = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const handleDrop = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
  const raw = event.dataTransfer.getData(tableDragMimeType);
    if (!raw) {
      return;
    }

    let parsed: DragPayload | null = null;
    try {
      parsed = JSON.parse(raw) as DragPayload;
    } catch (error) {
      return;
    }

    if (!parsed?.tableId) {
      return;
    }

    const table = tables.find((item) => item.id === parsed!.tableId);
    if (!table || !reactFlowWrapper.current) {
      return;
    }

    const instance = reactFlowInstanceRef.current;
    if (!instance) {
      return;
    }

    const flowPoint = typeof instance.screenToFlowPosition === 'function'
      ? instance.screenToFlowPosition({ x: event.clientX, y: event.clientY })
      : instance.project({
        x: event.clientX - reactFlowWrapper.current.getBoundingClientRect().left,
        y: event.clientY - reactFlowWrapper.current.getBoundingClientRect().top
      });

    const node = addTableNode(table, flowPoint);
    focusNode(node);
  }, [addTableNode, focusNode, tables]);

  const handleOutputDragOver = useCallback((event: DragEvent<HTMLElement>) => {
    const types = Array.from(event.dataTransfer?.types ?? []);
    if (!types.includes(REPORTING_FIELD_DRAG_TYPE)) {
      return;
    }
    event.preventDefault();
    event.dataTransfer.dropEffect = 'copy';
    setOutputDropActive(true);
  }, []);

  const handleOutputDragLeave = useCallback(() => {
    setOutputDropActive(false);
  }, []);

  const handleFieldDrop = useCallback((event: DragEvent<HTMLElement>) => {
    event.preventDefault();
    setOutputDropActive(false);
    const raw = event.dataTransfer.getData(REPORTING_FIELD_DRAG_TYPE);
    if (!raw) {
      return;
    }

    let parsed: FieldDragPayload | null = null;
    try {
      parsed = JSON.parse(raw) as FieldDragPayload;
    } catch (error) {
      return;
    }

    if (!parsed?.fieldId) {
      return;
    }

    ensureFieldAdded(parsed.fieldId);
  }, [ensureFieldAdded]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, minHeight: '100vh' }}>
      <Box>
        <Typography variant="h4" sx={{ fontWeight: 600, mb: 1 }}>
          Report Designer
        </Typography>
        <Typography variant="body1" color="text.secondary">
          Compose relational-style report definitions by combining enterprise tables, mapping joins, and shaping output fields prior to publishing.
        </Typography>
      </Box>

      <Paper
        elevation={2}
        sx={{ p: 2.5, flex: 1, display: 'flex', flexDirection: 'column', gap: 2, minHeight: 0 }}
      >
        <Stack
          direction={{ xs: 'column', lg: 'row' }}
          spacing={2}
          alignItems={{ xs: 'stretch', lg: 'flex-start' }}
          sx={{ flex: 1, minHeight: 0 }}
        >
          <Box sx={{ width: { xs: '100%', lg: 300 }, flexShrink: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Table Palette
            </Typography>
            <TextField
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              size="small"
              placeholder="Search tables"
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                )
              }}
            />
            <Paper variant="outlined" sx={{ flex: 1, minHeight: 280, overflowY: 'auto' }}>
              {tablesLoading && (
                <Stack sx={{ py: 6 }} alignItems="center" spacing={1}>
                  <CircularProgress size={24} />
                  <Typography variant="body2" color="text.secondary">
                    Loading tables…
                  </Typography>
                </Stack>
              )}
              {tablesError && (
                <Stack sx={{ py: 6, px: 2 }} spacing={1}>
                  <Typography variant="body2" color="error">
                    Unable to load table catalog. Verify backend availability.
                  </Typography>
                </Stack>
              )}
              {!tablesLoading && !tablesError && (
                <List dense disablePadding>
                  {filteredTables.length === 0 && (
                    <ListItem>
                      <ListItemText
                        primary="No matching tables"
                        primaryTypographyProps={{ variant: 'body2', color: 'text.secondary' }}
                      />
                    </ListItem>
                  )}
                  {filteredTables.map((item) => {
                    const alreadySelected = nodesById.has(item.id);
                    const subtitle = item.schemaName ? `${item.schemaName}.${item.physicalName}` : item.physicalName;
                    return (
                      <ListItem
                        key={item.id}
                        secondaryAction={
                          <Tooltip title={alreadySelected ? 'Already on canvas' : 'Add to canvas'}>
                            <span>
                              <IconButton
                                size="small"
                                color="primary"
                                disabled={alreadySelected}
                                onClick={() => handlePaletteAdd(item)}
                              >
                                <AddCircleOutlineIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                        }
                      >
                        <Stack
                          direction="row"
                          spacing={1}
                          alignItems="center"
                          draggable={!alreadySelected}
                          onDragStart={(event) => handleDragStart(event, item)}
                          sx={{ cursor: alreadySelected ? 'default' : 'grab', flex: 1 }}
                        >
                          <DragIndicatorIcon fontSize="small" color={alreadySelected ? 'disabled' : 'action'} />
                          <ListItemText
                            primary={item.name}
                            secondary={subtitle}
                            primaryTypographyProps={{ fontWeight: 600, fontSize: 14 }}
                            secondaryTypographyProps={{ fontSize: 12 }}
                          />
                        </Stack>
                      </ListItem>
                    );
                  })}
                </List>
              )}
            </Paper>
          </Box>

          <Divider flexItem orientation="vertical" sx={{ display: { xs: 'none', lg: 'block' } }} />

          <Box
            sx={{
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              gap: 2,
              minHeight: { xs: 320, md: 360 },
              minWidth: 0
            }}
          >
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Relationship Canvas
            </Typography>
            <Paper
              variant="outlined"
              sx={{
                minHeight: 280,
                height: { xs: '48vh', md: '56vh', lg: '60vh' },
                borderRadius: 2,
                overflow: 'hidden',
                position: 'relative'
              }}
              ref={reactFlowWrapper}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
            >
              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={handleConnect}
                onEdgeClick={handleEdgeClick}
                nodeTypes={nodeTypes}
                fitView
                minZoom={0.4}
                style={{ width: '100%', height: '100%', background: '#f5f7fb' }}
                onInit={(instance) => { reactFlowInstanceRef.current = instance; }}
              >
                <MiniMap pannable zoomable />
                <Controls position="bottom-right" />
                <Background gap={16} size={0.6} />
              </ReactFlow>
            </Paper>
            <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
              <Button
                variant="contained"
                startIcon={<PlayArrowIcon />}
                disabled={actionButtonsDisabled}
                onClick={handlePreviewResults}
              >
                Preview Results
              </Button>
              <Button
                variant="outlined"
                startIcon={<SaveIcon />}
                disabled={actionButtonsDisabled}
                onClick={handleSaveDraft}
              >
                Save Draft
              </Button>
              <Button
                variant="outlined"
                startIcon={<PublishIcon />}
                disabled={actionButtonsDisabled}
                onClick={handlePublish}
              >
                Publish to Data Object
              </Button>
              <Button
                variant="text"
                color="warning"
                startIcon={<RestartAltIcon />}
                onClick={handleResetDraft}
              >
                Reset Draft
              </Button>
            </Stack>
            {statusBanner && (
              <Typography variant="caption" sx={{ color: statusBanner.color, mt: 0.5 }}>
                {statusBanner.message}
              </Typography>
            )}
          </Box>
        </Stack>

        <Box>
          <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 1 }}>
            Output Fields
          </Typography>
          <Paper variant="outlined" sx={{ p: nodes.length === 0 ? 3 : 0, minHeight: 320 }}>
            {nodes.length === 0 ? (
              <Stack spacing={1} alignItems="center" justifyContent="center" sx={{ height: '100%' }}>
                <Typography variant="body2" color="text.secondary" align="center">
                  Add tables to the canvas to begin defining result columns.
                </Typography>
              </Stack>
            ) : (
              <Box sx={{ p: 2 }}>
                <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 1 }}>
                  <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                    Selected Output Columns
                  </Typography>
                  <Button
                    variant={groupingEnabled ? 'contained' : 'outlined'}
                    color="secondary"
                    size="small"
                    onClick={handleGroupingToggle}
                    disabled={selectedFieldColumns.length === 0}
                  >
                    {groupingEnabled ? 'Disable Group By' : 'Enable Group By'}
                  </Button>
                </Stack>
                <Box
                  sx={{
                    mt: 1,
                    overflowX: 'auto',
                    outline: outputDropActive ? `2px dashed ${theme.palette.secondary.main}` : 'none',
                    outlineOffset: 6,
                    borderRadius: 1,
                    transition: 'outline-color 120ms ease'
                  }}
                  onDragOver={handleOutputDragOver}
                  onDragLeave={handleOutputDragLeave}
                  onDrop={handleFieldDrop}
                >
                  {selectedFieldColumns.length === 0 ? (
                    <Stack spacing={1} alignItems="center" justifyContent="center" sx={{ py: 6 }}>
                      <Typography variant="body2" color="text.secondary" align="center">
                        Use the Relationship Canvas to add columns or drag fields here to seed the result set.
                      </Typography>
                      <Typography variant="caption" color="text.secondary" align="center">
                        Tip: click the selector in any table node to toggle a column.
                      </Typography>
                    </Stack>
                  ) : (
                    <>
                      <Box sx={{ display: 'inline-flex', minWidth: '100%' }}>
                        <Box
                          sx={{
                            width: 140,
                            flexShrink: 0,
                            border: `1px solid ${theme.palette.divider}`,
                            borderRight: `1px solid ${theme.palette.divider}`,
                            borderRadius: '8px 0 0 8px',
                            overflow: 'hidden',
                            backgroundColor: alpha(theme.palette.primary.light, 0.12)
                          }}
                        >
                          {outputColumnRowDescriptors.map((row, index) => (
                            <Box
                              key={row.key}
                              sx={{
                                px: 1.25,
                                py: row.paddingY,
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: row.justifyContent ?? 'flex-start',
                                fontWeight: 600,
                                minHeight: row.minHeight,
                                borderBottom:
                                  index === outputColumnRowDescriptors.length - 1
                                    ? 'none'
                                    : `1px solid ${theme.palette.divider}`
                              }}
                            >
                              {row.label}
                            </Box>
                          ))}
                        </Box>
                        <DndContext
                          sensors={outputColumnSensors}
                          collisionDetection={closestCenter}
                          onDragEnd={handleOutputColumnsDragEnd}
                        >
                          <SortableContext
                            items={selectedFieldColumns.map((column) => column.fieldId)}
                            strategy={horizontalListSortingStrategy}
                          >
                            <Box
                              sx={{
                                display: 'flex',
                                flex: 1,
                                border: `1px solid ${theme.palette.divider}`,
                                borderLeft: 'none',
                                borderRadius: '0 8px 8px 0',
                                overflow: 'hidden',
                                backgroundColor: alpha(theme.palette.background.paper, 0.85)
                              }}
                            >
                              {selectedFieldColumns.map((column, index) => (
                                <SortableOutputColumn
                                  key={column.fieldId}
                                  column={column}
                                  isFirst={index === 0}
                                  isLast={index === selectedFieldColumns.length - 1}
                                  groupingEnabled={groupingEnabled}
                                  aggregateOptions={aggregateOptions}
                                  rowDescriptors={outputColumnRowDescriptors}
                                  criteriaRowCount={criteriaRowCount}
                                  onRemove={toggleField}
                                  onSortChange={handleSortChange}
                                  onAggregateChange={handleAggregateChange}
                                  onShowToggle={handleShowToggle}
                                  onCriteriaChange={handleCriteriaChange}
                                />
                              ))}
                            </Box>
                          </SortableContext>
                        </DndContext>
                      </Box>
                      <Stack direction="row" spacing={1} sx={{ mt: 1 }}>
                        <Button variant="text" size="small" onClick={handleAddCriteriaRow}>
                          Add criteria row
                        </Button>
                        {criteriaRowCount > 1 && (
                          <Button variant="text" size="small" onClick={handleRemoveCriteriaRow}>
                            Remove last criteria row
                          </Button>
                        )}
                      </Stack>
                    </>
                  )}
                </Box>
              </Box>
            )}
          </Paper>
        </Box>
      </Paper>
      <Dialog open={Boolean(joinDialogState)} onClose={handleJoinDialogClose} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ pb: 1.5 }}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            {joinDialogTitleText}
          </Typography>
        </DialogTitle>
        {joinDialogState && (
          <>
            <DialogContent dividers>
              <Stack spacing={3}>
                <Typography variant="body2" color="text.secondary">
                  {joinDialogDescription}
                </Typography>

                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                  <Box sx={{ flex: 1 }}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                      Source
                    </Typography>
                    <TextField
                      label="Source Table"
                      size="small"
                      value={joinDialogSourceName}
                      InputProps={{ readOnly: true }}
                      fullWidth
                    />
                    <TextField
                      select
                      label="Source Field"
                      size="small"
                      sx={{ mt: 1.5 }}
                      value={joinDialogState.sourceFieldId}
                      onChange={(event) => handleJoinDialogFieldChange('source', event.target.value)}
                      fullWidth
                    >
                      {(fieldsByTable.get(joinDialogState.sourceTableId) ?? []).map((field) => (
                        <MenuItem key={field.id} value={field.id}>
                          {field.name}
                        </MenuItem>
                      ))}
                    </TextField>
                  </Box>
                  <Box sx={{ flex: 1 }}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                      Target
                    </Typography>
                    <TextField
                      label="Target Table"
                      size="small"
                      value={joinDialogTargetName}
                      InputProps={{ readOnly: true }}
                      fullWidth
                    />
                    <TextField
                      select
                      label="Target Field"
                      size="small"
                      sx={{ mt: 1.5 }}
                      value={joinDialogState.targetFieldId}
                      onChange={(event) => handleJoinDialogFieldChange('target', event.target.value)}
                      fullWidth
                    >
                      {(fieldsByTable.get(joinDialogState.targetTableId) ?? []).map((field) => (
                        <MenuItem key={field.id} value={field.id}>
                          {field.name}
                        </MenuItem>
                      ))}
                    </TextField>
                  </Box>
                </Stack>

                <FormControl component="fieldset" sx={{ width: '100%' }}>
                  <FormLabel component="legend" sx={{ fontWeight: 600, mb: 0.5 }}>
                    Join Type
                  </FormLabel>
                  <RadioGroup
                    value={joinDialogState.joinType}
                    onChange={(event) => handleJoinDialogTypeChange(event.target.value as JoinType)}
                  >
                    {joinTypeOptions.map((option) => (
                      <FormControlLabel
                        key={option.value}
                        value={option.value}
                        control={<Radio size="small" />}
                        label={(
                          <Box>
                            <Typography variant="body2" sx={{ fontWeight: 600 }}>
                              {option.label}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">
                              {option.helper}
                            </Typography>
                          </Box>
                        )}
                        sx={{ alignItems: 'flex-start', mr: 0, mt: 0.25 }}
                      />
                    ))}
                  </RadioGroup>
                </FormControl>
              </Stack>
            </DialogContent>
            <DialogActions sx={{ px: 2.5, py: 1.5 }}>
              {joinDialogIsEdit && (
                <Button color="error" onClick={handleJoinDialogDelete}>
                  Remove Join
                </Button>
              )}
              <Button onClick={handleJoinDialogClose}>Cancel</Button>
              <Button onClick={handleJoinDialogConfirm} variant="contained" disabled={!canConfirmJoin}>
                {joinDialogConfirmLabel}
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>

      <Dialog open={previewOpen} onClose={handlePreviewClose} maxWidth="lg" fullWidth>
        <DialogTitle sx={{ pb: 1.5 }}>
          <Stack direction="row" alignItems="center" justifyContent="space-between">
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Preview Results
            </Typography>
            <Tooltip title="Copy SQL">
              <span>
                <IconButton size="small" onClick={handleCopyPreviewSql} disabled={!previewData?.sql || previewLoading}>
                  <ContentCopyIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          </Stack>
        </DialogTitle>
        <DialogContent dividers>
          {previewLoading ? (
            <Box sx={{ py: 6, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <CircularProgress size={32} />
            </Box>
          ) : previewError ? (
            <Alert severity="error">{previewError}</Alert>
          ) : previewData ? (
            <Stack spacing={2}>
              <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
                <Chip label={`Rows: ${previewData.rowCount}`} size="small" />
                <Chip label={`Limit: ${previewData.limit}`} size="small" />
                <Chip label={`Duration: ${previewData.durationMs.toFixed(1)} ms`} size="small" />
              </Stack>
              <TableContainer component={Paper} variant="outlined" sx={{ maxHeight: 360 }}>
                <MuiTable size="small" stickyHeader>
                  <MuiTableHead>
                    <MuiTableRow>
                      {previewData.columns.map((column) => (
                        <MuiTableCell key={column} sx={{ fontWeight: 600 }}>
                          {column}
                        </MuiTableCell>
                      ))}
                    </MuiTableRow>
                  </MuiTableHead>
                  <MuiTableBody>
                    {previewData.rows.length === 0 ? (
                      <MuiTableRow>
                        <MuiTableCell colSpan={Math.max(previewData.columns.length, 1)} align="center">
                          <Typography variant="body2" color="text.secondary">
                            No rows returned for the current selection.
                          </Typography>
                        </MuiTableCell>
                      </MuiTableRow>
                    ) : (
                      previewData.rows.map((row, rowIndex) => (
                        <MuiTableRow key={rowIndex} hover>
                          {previewData.columns.map((column) => {
                            const cellValue = row[column];
                            return (
                              <MuiTableCell key={`${rowIndex}-${column}`} sx={{ whiteSpace: 'nowrap', maxWidth: 240 }}>
                                {formatPreviewValue(cellValue)}
                              </MuiTableCell>
                            );
                          })}
                        </MuiTableRow>
                      ))
                    )}
                  </MuiTableBody>
                </MuiTable>
              </TableContainer>
              <Box>
                <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 0.5 }}>
                  Generated SQL
                </Typography>
                <Box
                  component="pre"
                  sx={{
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                    fontFamily: 'Roboto Mono, monospace',
                    fontSize: 13,
                    m: 0,
                    p: 1.5,
                    backgroundColor: alpha(theme.palette.primary.light, 0.08),
                    borderRadius: 1
                  }}
                >
                  {previewData.sql}
                </Box>
              </Box>
            </Stack>
          ) : (
            <Typography variant="body2" color="text.secondary">
              Configure tables, fields, and joins, then select &ldquo;Preview Results&rdquo; to fetch a sample of the dataset.
            </Typography>
          )}
        </DialogContent>
        <DialogActions sx={{ px: 2.5, py: 1.5 }}>
          <Button onClick={handlePreviewClose} variant="outlined">
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

const ReportingDesignerPage = () => (
  <ReactFlowProvider>
    <ReportingDesignerContent />
  </ReactFlowProvider>
);

export default ReportingDesignerPage;
