import {
  Fragment,
  useMemo,
  useState,
  useCallback,
  useRef,
  useEffect,
  type DragEvent,
  type MouseEvent,
  type ReactNode,
  type SyntheticEvent
} from 'react';
import useDesignerStore from '../stores/designerStore';
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
  ListItemButton,
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
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PublishIcon from '@mui/icons-material/Publish';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import SaveIcon from '@mui/icons-material/Save';
import SearchIcon from '@mui/icons-material/Search';
import { SimpleTreeView } from '@mui/x-tree-view/SimpleTreeView';
import { TreeItem } from '@mui/x-tree-view/TreeItem';
import { isAxiosError } from 'axios';
import { useLocation, useNavigate } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from 'react-query';
import ReactFlow, {
  Background,
  Connection,
  Controls,
  MarkerType,
  MiniMap,
  Node,
  ReactFlowInstance,
  ReactFlowProvider,
  XYPosition,
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
  horizontalListSortingStrategy,
  sortableKeyboardCoordinates,
  useSortable
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

import PageHeader from '@components/common/PageHeader';
import ConfirmDialog from '@components/common/ConfirmDialog';
import ReportTableNode, { REPORTING_FIELD_DRAG_TYPE, ReportTableNodeData } from '@components/reporting/ReportTableNode';
import { fetchAllDataDefinitions } from '@services/dataDefinitionService';
import { fetchDataObjects } from '@services/dataObjectService';
import { fetchProcessAreas } from '@services/processAreaService';
import { fetchFields, fetchReportingTables } from '@services/tableService';
import {
  createReport,
  deleteReport,
  fetchReport,
  fetchReportPreview,
  listReports,
  publishReport,
  reportQueryKeys,
  updateReport,
  type ReportPreviewResponse,
  type ReportPublishPayload,
  type ReportSavePayload,
  type ReportUpdatePayload
} from '@services/reportingService';
import type { DataDefinition, DataObject, Field, ProcessArea, Table } from '../types/data';
import type {
  ReportAggregateFn,
  ReportDetail,
  ReportDesignerDefinition,
  ReportJoinType,
  ReportSortDirection,
  ReportSummary
} from '../types/reporting';

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

type RecentDropSentinelWindow = Window & {
  __CC_RECENT_DROP?: boolean;
};

interface OutputColumnRowDescriptor {
  key: string;
  label: string;
  paddingY: number;
  justifyContent?: 'center' | 'flex-start';
  minHeight?: number;
}

type PaletteNodeType = 'productTeam' | 'dataObject' | 'system' | 'table' | 'unassigned';

interface PaletteTreeNode {
  id: string;
  label: string;
  secondary?: string | null;
  type: PaletteNodeType;
  tableCount?: number;
  table?: Table;
  children?: PaletteTreeNode[];
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

const arraysShallowEqual = (first: readonly string[], second: readonly string[]): boolean => {
  if (first.length !== second.length) {
    return false;
  }

  for (let index = 0; index < first.length; index += 1) {
    if (first[index] !== second[index]) {
      return false;
    }
  }

  return true;
};

const ReportingDesignerContent = () => {
  const {
    data: tables = [],
    isLoading: tablesLoading,
    isError: tablesLoadError,
    error: tablesError
  } = useQuery<Table[]>(['reporting-tables'], fetchReportingTables);
  const {
    data: dataDefinitions = [],
    isLoading: definitionsLoading,
    isError: definitionsLoadError,
    error: definitionsError
  } = useQuery<DataDefinition[]>(['reporting-data-definitions'], fetchAllDataDefinitions);
  const {
    data: dataObjects = [],
    isLoading: dataObjectsLoading,
    isError: dataObjectsLoadError,
    error: dataObjectsError
  } = useQuery<DataObject[]>(['reporting-data-objects'], fetchDataObjects);
  const {
    data: processAreas = [],
    isLoading: processAreasLoading,
    isError: processAreasLoadError,
    error: processAreasError
  } = useQuery<ProcessArea[]>(['reporting-process-areas'], fetchProcessAreas);
  const queryClient = useQueryClient();
  const {
    data: draftReports = [],
    isLoading: draftReportsLoading,
    isFetching: draftReportsFetching,
    refetch: refetchDraftReports
  } = useQuery(reportQueryKeys.list('draft'), () => listReports('draft'), {
    staleTime: 30_000
  });

  const createReportMutation = useMutation<ReportDetail, unknown, ReportSavePayload>(createReport);
  const updateReportMutation = useMutation<
    ReportDetail,
    unknown,
    { reportId: string; payload: ReportUpdatePayload }
  >(({ reportId, payload }) => updateReport(reportId, payload));
  const publishReportMutation = useMutation<
    ReportDetail,
    unknown,
    { reportId: string; payload: ReportPublishPayload }
  >(({ reportId, payload }) => publishReport(reportId, payload));
  const deleteReportMutation = useMutation<void, unknown, string>((reportId) => deleteReport(reportId));

  const location = useLocation();
  const navigate = useNavigate();

  const paletteLoading = tablesLoading || definitionsLoading || dataObjectsLoading || processAreasLoading;
  const paletteError = tablesLoadError || definitionsLoadError || dataObjectsLoadError || processAreasLoadError;
  const paletteErrorMessage = useMemo(() => {
    const rawError =
      (tablesError as unknown) ??
      (definitionsError as unknown) ??
      (dataObjectsError as unknown) ??
      (processAreasError as unknown);
    if (isAxiosError(rawError)) {
      return rawError.response?.data?.detail ?? rawError.message ?? 'Unable to load table palette data.';
    }
    if (rawError instanceof Error) {
      return rawError.message;
    }
    return 'Unable to load table palette data.';
  }, [dataObjectsError, definitionsError, processAreasError, tablesError]);
  const { data: rawFields = [] } = useQuery<Field[]>(['reporting-fields'], fetchFields);

  const databricksTableIdSet = useMemo(() => new Set(tables.map((table) => table.id)), [tables]);
  const allowedTableIds = useMemo(() => {
    const set = new Set<string>();
    dataDefinitions.forEach((definition) => {
      definition.tables.forEach((definitionTable) => {
        set.add(definitionTable.tableId);
      });
    });
    return set;
  }, [dataDefinitions]);

  const eligibleTableIds = useMemo(() => {
    if (databricksTableIdSet.size === 0 && allowedTableIds.size === 0) {
      return databricksTableIdSet;
    }

    const combined = new Set<string>();
    databricksTableIdSet.forEach((tableId) => combined.add(tableId));
    allowedTableIds.forEach((tableId) => combined.add(tableId));
    return combined;
  }, [allowedTableIds, databricksTableIdSet]);

  const fields = useMemo(
    () => rawFields.filter((field) => eligibleTableIds.has(field.tableId)),
    [eligibleTableIds, rawFields]
  );

  const [searchTerm, setSearchTerm] = useState('');
  const [nodes, setNodes, onNodesChange] = useNodesState<ReportTableNodeData>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedFieldOrder, setSelectedFieldOrder] = useState<string[]>([]);
  const [fieldSettings, setFieldSettings] = useState<Record<string, FieldColumnSettings>>({});
  const [criteriaRowCount, setCriteriaRowCount] = useState(2);
  const [groupingEnabled, setGroupingEnabled] = useState(false);
  const [activeReport, setActiveReport] = useState<ReportSummary | null>(null);
  const [reportName, setReportName] = useState('');
  const [reportDescription, setReportDescription] = useState('');
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [draftListOpen, setDraftListOpen] = useState(false);
  const [draftNameInput, setDraftNameInput] = useState('');
  const [draftDescriptionInput, setDraftDescriptionInput] = useState('');
  const [pendingPersistAction, setPendingPersistAction] = useState<'draft' | 'publish'>('draft');
  const [persisting, setPersisting] = useState(false);
  const [dialogError, setDialogError] = useState<string | null>(null);
  const [loadingReportId, setLoadingReportId] = useState<string | null>(null);
  const [deletingReportId, setDeletingReportId] = useState<string | null>(null);
  const [outputDropActive, setOutputDropActive] = useState(false);
  const [statusBanner, setStatusBanner] = useState<{ message: string; color: string } | null>(null);
  const [removeDialogState, setRemoveDialogState] = useState<{ nodeId: string; name: string } | null>(null);
  const [selectedProductTeamId, setSelectedProductTeamId] = useState<string | null>(null);
  const [selectedDataObjectId, setSelectedDataObjectId] = useState<string | null>(null);
  const [draftProductTeamIdInput, setDraftProductTeamIdInput] = useState<string | null>(null);
  const [draftDataObjectIdInput, setDraftDataObjectIdInput] = useState<string | null>(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewData, setPreviewData] = useState<ReportPreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [joinDialogState, setJoinDialogState] = useState<JoinDialogState | null>(null);
  const [pendingCatalogReportId, setPendingCatalogReportId] = useState<string | null>(null);

  const reactFlowInstanceRef = useRef<ReactFlowInstance<ReportTableNodeData> | null>(null);
  const reactFlowWrapper = useRef<HTMLDivElement | null>(null);
  const hasHydratedDraftRef = useRef(false);
  const draggedFieldRef = useRef<FieldDragPayload | null>(null);
  const previousExpandedNodesRef = useRef<string[] | null>(null);
  const draftNameInputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (saveDialogOpen) {
      draftNameInputRef.current?.focus();
    }
  }, [saveDialogOpen]);

  const theme = useTheme();
  const isDarkMode = theme.palette.mode === 'dark';
  const canvasBackground = useMemo(
    () =>
      alpha(
        isDarkMode ? theme.palette.background.default : theme.palette.background.paper,
        isDarkMode ? 0.9 : 0.96
      ),
    [isDarkMode, theme]
  );
  const canvasGridColor = useMemo(
    () => alpha(theme.palette.divider, isDarkMode ? 0.6 : 0.3),
    [isDarkMode, theme]
  );
  const miniMapBackground = useMemo(
    () =>
      alpha(
        isDarkMode ? theme.palette.background.paper : theme.palette.common.white,
        isDarkMode ? 0.92 : 0.9
      ),
    [isDarkMode, theme]
  );
  const miniMapNodeColor = useMemo(
    () => alpha(theme.palette.primary.main, isDarkMode ? 0.8 : 0.6),
    [isDarkMode, theme]
  );
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
    dataDefinitions.forEach((definition) => {
      definition.tables.forEach((definitionTable) => {
        map.set(definitionTable.tableId, definitionTable.table);
      });
    });
    return map;
  }, [dataDefinitions, tables]);

  const dataObjectsById = useMemo(() => {
    const map = new Map<string, DataObject>();
    dataObjects.forEach((dataObject) => {
      map.set(dataObject.id, dataObject);
    });
    return map;
  }, [dataObjects]);

  const processAreasById = useMemo(() => {
    const map = new Map<string, ProcessArea>();
    processAreas.forEach((processArea) => {
      map.set(processArea.id, processArea);
    });
    return map;
  }, [processAreas]);

  const sortedProcessAreas = useMemo(() => {
    const items = [...processAreas];
    items.sort((a, b) => a.name.localeCompare(b.name));
    return items;
  }, [processAreas]);

  const sortedDataObjects = useMemo(() => {
    const items = [...dataObjects];
    items.sort((a, b) => a.name.localeCompare(b.name));
    return items;
  }, [dataObjects]);

  const filteredDataObjects = useMemo(() => {
    if (!draftProductTeamIdInput) {
      return sortedDataObjects;
    }
    return sortedDataObjects.filter(
      (dataObject) => (dataObject.processAreaId ?? null) === draftProductTeamIdInput
    );
  }, [draftProductTeamIdInput, sortedDataObjects]);

  const paletteHierarchy = useMemo<PaletteTreeNode[]>(() => {
    const productTeamNodes = new Map<string, PaletteTreeNode>();
    const dataObjectNodes = new Map<string, PaletteTreeNode>();
    const systemNodes = new Map<string, PaletteTreeNode>();
    const assignedTableIds = new Set<string>();

    const ensureProductTeamNode = (processAreaId: string | null): PaletteTreeNode => {
      const key = processAreaId ?? 'unassigned';
      let existing = productTeamNodes.get(key);
      if (!existing) {
        const processArea = processAreaId ? processAreasById.get(processAreaId) : null;
        existing = {
          id: `pt-${key}`,
          label: processArea?.name ?? 'Unassigned Product Team',
          secondary: processArea?.description ?? null,
          type: 'productTeam',
          children: [],
          tableCount: 0
        };
        productTeamNodes.set(key, existing);
      }
      return existing;
    };

    const ensureDataObjectNode = (productTeamNode: PaletteTreeNode, dataObjectId: string): PaletteTreeNode => {
      let node = dataObjectNodes.get(dataObjectId);
      if (!node) {
        const dataObject = dataObjectsById.get(dataObjectId);
        node = {
          id: `do-${dataObjectId}`,
          label: dataObject?.name ?? 'Unassigned Data Object',
          secondary: dataObject?.description ?? null,
          type: 'dataObject',
          children: [],
          tableCount: 0
        };
        dataObjectNodes.set(dataObjectId, node);
        productTeamNode.children!.push(node);
      }
      return node;
    };

    const ensureSystemNode = (
      dataObjectNode: PaletteTreeNode,
      dataObjectId: string,
      definition: DataDefinition
    ): PaletteTreeNode => {
      const systemKey = `${dataObjectId}::${definition.systemId ?? 'unassigned'}`;
      let node = systemNodes.get(systemKey);
      if (!node) {
        const referencedSystem =
          definition.system ??
          dataObjectsById.get(dataObjectId)?.systems?.find((system) => system.id === definition.systemId) ??
          null;
        const systemLabel = referencedSystem?.name ?? referencedSystem?.physicalName ?? 'Unassigned System';
        const systemSecondary = referencedSystem?.physicalName && referencedSystem?.physicalName !== systemLabel
          ? referencedSystem.physicalName
          : definition.description ?? null;
        node = {
          id: `sys-${systemKey}`,
          label: systemLabel,
          secondary: systemSecondary,
          type: 'system',
          children: [],
          tableCount: 0
        };
        systemNodes.set(systemKey, node);
        dataObjectNode.children!.push(node);
      }
      return node;
    };

    dataDefinitions.forEach((definition) => {
      if (!definition.tables.length) {
        return;
      }

      const dataObject = dataObjectsById.get(definition.dataObjectId);
      const productTeamNode = ensureProductTeamNode(dataObject?.processAreaId ?? null);
      const dataObjectNode = ensureDataObjectNode(productTeamNode, definition.dataObjectId);
      const systemNode = ensureSystemNode(dataObjectNode, definition.dataObjectId, definition);

      definition.tables.forEach((definitionTable) => {
        const table = tablesById.get(definitionTable.tableId) ?? definitionTable.table;
        if (!table) {
          return;
        }

        assignedTableIds.add(table.id);

        const duplicate = systemNode.children?.some(
          (child: PaletteTreeNode) =>
            child.type === 'table' && child.table?.id === table.id && child.id === `tbl-${definitionTable.id}`
        );
        if (duplicate) {
          return;
        }

        const tableNode: PaletteTreeNode = {
          id: `tbl-${definitionTable.id}`,
          label: definitionTable.alias ?? table.name,
          secondary: table.schemaName ? `${table.schemaName}.${table.physicalName}` : table.physicalName,
          type: 'table',
          table
        };

        systemNode.children!.push(tableNode);
        systemNode.tableCount = (systemNode.tableCount ?? 0) + 1;
        dataObjectNode.tableCount = (dataObjectNode.tableCount ?? 0) + 1;
        productTeamNode.tableCount = (productTeamNode.tableCount ?? 0) + 1;
      });
    });

    const sortNodes = (nodes?: PaletteTreeNode[]) => {
      if (!nodes) {
        return;
      }
      nodes.sort((a, b) => a.label.localeCompare(b.label));
      nodes.forEach((node) => {
        if (node.children && node.children.length) {
          sortNodes(node.children);
        }
      });
    };

    const hierarchy = Array.from(productTeamNodes.values());
    sortNodes(hierarchy);

    const unassignedTables = tables.filter((table) => !assignedTableIds.has(table.id));
    if (unassignedTables.length > 0) {
      const sortedUnassigned = [...unassignedTables].sort((a, b) => a.name.localeCompare(b.name));
      const unassignedNode: PaletteTreeNode = {
        id: 'unassigned-root',
        label: 'Unassigned Tables',
        secondary: 'Not yet linked to a data object',
        type: 'unassigned',
        children: sortedUnassigned.map((table) => ({
          id: `tbl-unassigned-${table.id}`,
          label: table.name,
          secondary: table.schemaName ? `${table.schemaName}.${table.physicalName}` : table.physicalName,
          type: 'table',
          table
        })),
        tableCount: sortedUnassigned.length
      };
      hierarchy.push(unassignedNode);
    }

    return hierarchy;
  }, [dataDefinitions, dataObjectsById, processAreasById, tables, tablesById]);

  const filteredPaletteHierarchy = useMemo<PaletteTreeNode[]>(() => {
    const term = searchTerm.trim().toLowerCase();
    if (!term) {
      return paletteHierarchy;
    }

    const filterNode = (node: PaletteTreeNode): PaletteTreeNode | null => {
      const labelMatches = node.label.toLowerCase().includes(term);
      const secondaryMatches = node.secondary ? node.secondary.toLowerCase().includes(term) : false;
      if (node.type === 'table') {
        return labelMatches || secondaryMatches ? node : null;
      }

      const filteredChildren = (node.children ?? [])
        .map(filterNode)
        .filter((child): child is PaletteTreeNode => Boolean(child));

      if (filteredChildren.length > 0 || labelMatches || secondaryMatches) {
        const tableCount = filteredChildren.reduce<number>((total, child) => {
          if (child.type === 'table') {
            return total + 1;
          }
          return total + (child.tableCount ?? 0);
        }, 0);
        return {
          ...node,
          children: filteredChildren,
          tableCount
        };
      }

      return null;
    };

    return paletteHierarchy
      .map(filterNode)
      .filter((node): node is PaletteTreeNode => Boolean(node));
  }, [paletteHierarchy, searchTerm]);

  const autoExpandedNodeIds = useMemo(() => {
    const ids: string[] = [];
    const collect = (node: PaletteTreeNode) => {
      if (node.type !== 'table') {
        ids.push(node.id);
        node.children?.forEach(collect);
      }
    };
    paletteHierarchy.forEach(collect);
    return ids;
  }, [paletteHierarchy]);

  const [expandedNodes, setExpandedNodes] = useState<string[]>([]);
  const validExpandedNodeIds = useMemo(() => new Set(autoExpandedNodeIds), [autoExpandedNodeIds]);

  useEffect(() => {
    setExpandedNodes((current) => {
      const filtered = current.filter((id) => validExpandedNodeIds.has(id));
      return arraysShallowEqual(current, filtered) ? current : filtered;
    });
  }, [validExpandedNodeIds]);

  useEffect(() => {
    const hasSearchTerm = searchTerm.trim().length > 0;

    if (hasSearchTerm) {
      if (previousExpandedNodesRef.current === null) {
        previousExpandedNodesRef.current = expandedNodes;
      }

      if (!arraysShallowEqual(expandedNodes, autoExpandedNodeIds)) {
        setExpandedNodes(autoExpandedNodeIds);
      }
      return;
    }

    if (previousExpandedNodesRef.current !== null) {
      const previousExpanded = previousExpandedNodesRef.current;
      previousExpandedNodesRef.current = null;

      if (!arraysShallowEqual(expandedNodes, previousExpanded)) {
        setExpandedNodes(previousExpanded);
      }
    }
  }, [autoExpandedNodeIds, expandedNodes, searchTerm]);

  const fieldsById = useMemo(() => {
    const map = new Map<string, Field>();
    fields.forEach((field) => map.set(field.id, field));
    return map;
  }, [fields]);

  const fieldIdSet = useMemo(() => new Set(fields.map((field) => field.id)), [fields]);

  const resetDesignerState = useCallback(
    (options?: { message?: string; color?: string }) => {
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
      setActiveReport(null);
      setReportName('');
      setReportDescription('');
      setDraftNameInput('');
      setDraftDescriptionInput('');
      setSaveDialogOpen(false);
      setPendingPersistAction('draft');
      setDialogError(null);
      setPersisting(false);
      setSelectedProductTeamId(null);
      setSelectedDataObjectId(null);
      setDraftProductTeamIdInput(null);
      setDraftDataObjectIdInput(null);
      if (options?.message) {
        setStatusBanner({ message: options.message, color: options.color ?? 'text.secondary' });
      }
      if (typeof window !== 'undefined') {
        window.localStorage.removeItem(designerStorageKey);
      }
      // mark as clean
      try {
        useDesignerStore.getState().setHasUnsaved(false);
      } catch {
        // ignore
      }
      hasHydratedDraftRef.current = true;
    },
    [
      setActiveReport,
      setCriteriaRowCount,
      setDraftDescriptionInput,
      setDraftNameInput,
      setDraftDataObjectIdInput,
      setDraftProductTeamIdInput,
      setEdges,
      setFieldSettings,
      setGroupingEnabled,
      setNodes,
      setPendingPersistAction,
      setPersisting,
      setPreviewData,
      setPreviewError,
      setPreviewLoading,
      setPreviewOpen,
      setReportDescription,
      setReportName,
      setSaveDialogOpen,
      setSelectedDataObjectId,
      setSelectedFieldOrder,
      setSelectedProductTeamId,
      setStatusBanner
    ]
  );


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

  const hydrateDesignerFromDefinition = useCallback(
    (definition: ReportDesignerDefinition, message?: string) => {
      const normalizedCriteriaCount = Math.max(1, definition.criteriaRowCount ?? 1);
      const groupingFromDraft = Boolean(definition.groupingEnabled);

      const restoredNodes = (definition.tables ?? [])
        .filter((placement) => tablesById.has(placement.tableId))
        .map((placement) => {
          const table = tablesById.get(placement.tableId)!;
          const tableFields = fieldsByTable.get(placement.tableId) ?? [];
          const savedWidth = placement.dimensions?.width;
          const savedHeight = placement.dimensions?.height;
          const width = typeof savedWidth === 'number' && savedWidth >= minNodeWidth ? savedWidth : defaultNodeWidth;
          const height = typeof savedHeight === 'number' && savedHeight >= minNodeHeight ? savedHeight : defaultNodeHeight;

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

      const restoredEdges = (definition.joins ?? [])
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

      const orderedColumns = [...(definition.columns ?? [])].sort((a, b) => a.order - b.order);
      const restoredFieldOrder: string[] = [];
      const restoredFieldSettings: Record<string, FieldColumnSettings> = {};

      orderedColumns.forEach((column) => {
        if (!fieldIdSet.has(column.fieldId)) {
          return;
        }
        const normalizedSort: SortDirection = column.sort === 'asc' || column.sort === 'desc' ? column.sort : 'none';
        const normalizedAggregate: ReportAggregateFn | null = groupingFromDraft ? column.aggregate ?? 'groupBy' : null;
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

      if (message) {
        setStatusBanner({ message, color: 'success.main' });
      }
    },
    [
      createNodeData,
      fieldIdSet,
      fieldsByTable,
      joinEdgePresentation,
      setCriteriaRowCount,
      setEdges,
      setFieldSettings,
      setGroupingEnabled,
      setNodes,
      setSelectedFieldOrder,
      setStatusBanner,
      tablesById
    ]
  );

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
    setRemoveDialogState({ nodeId, name: tableName });
  }, [setRemoveDialogState, tablesById]);

  const handleConfirmRemoveNode = useCallback(() => {
    if (!removeDialogState) {
      return;
    }
    removeNodeById(removeDialogState.nodeId);
    setRemoveDialogState(null);
  }, [removeDialogState, removeNodeById, setRemoveDialogState]);

  const handleCancelRemoveNode = useCallback(() => {
    setRemoveDialogState(null);
  }, [setRemoveDialogState]);

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

  // Add a short activation delay so quick clicks/taps do not activate
  // the pointer sensor. Without this, the PointerSensor may call
  // preventDefault on pointer events in some environments which can
  // block normal link navigation that follows shortly after interacting
  // with the designer. The small delay preserves drag UX while avoiding
  // accidental activation for quick clicks.
  const outputColumnSensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 6, delay: 150 } }),
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

  // When the in-memory report definition changes after initial hydration,
  // mark the designer as having unsaved changes so navigation guards can warn.
  useEffect(() => {
    try {
      if (hasHydratedDraftRef.current) {
        useDesignerStore.getState().setHasUnsaved(true);
      } else {
        // first build after hydration — treat as clean
        hasHydratedDraftRef.current = true;
      }
    } catch {
      // ignore
    }
  }, [reportDefinition]);

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

  const handleOpenPersistDialog = useCallback(
    (action: 'draft' | 'publish') => {
      if (persisting) {
        return;
      }
      const initialName = reportName || activeReport?.name || '';
      const initialDescription = reportDescription || activeReport?.description || '';
      const initialProductTeamId = selectedProductTeamId ?? activeReport?.productTeamId ?? null;
      const initialDataObjectId = selectedDataObjectId ?? activeReport?.dataObjectId ?? null;
      setPendingPersistAction(action);
      setDraftNameInput(initialName);
      setDraftDescriptionInput(initialDescription);
      setDraftProductTeamIdInput(initialProductTeamId);
      setDraftDataObjectIdInput(initialDataObjectId);
      setDialogError(null);
      setSaveDialogOpen(true);
    },
    [
      activeReport,
      persisting,
      reportDescription,
      reportName,
      selectedDataObjectId,
      selectedProductTeamId
    ]
  );

  const handlePersistDialogClose = useCallback(() => {
    if (persisting) {
      return;
    }
    setSaveDialogOpen(false);
    setDialogError(null);
  }, [persisting]);

  const handlePersistConfirm = useCallback(async () => {
    const trimmedName = draftNameInput.trim();
    const trimmedDescription = draftDescriptionInput.trim();

    if (!trimmedName) {
      setDialogError('Report name is required.');
      return;
    }

    const normalizedDescription = trimmedDescription.length > 0 ? trimmedDescription : null;
    const selectedDataObject = draftDataObjectIdInput ? dataObjectsById.get(draftDataObjectIdInput) ?? null : null;
    const resolvedProductTeamId = selectedDataObject
      ? selectedDataObject.processAreaId ?? null
      : draftProductTeamIdInput ?? null;
    const resolvedDataObjectId = draftDataObjectIdInput ?? null;

    if (resolvedDataObjectId && !selectedDataObject) {
      setDialogError('Selected data object is no longer available.');
      return;
    }

    if (selectedDataObject && resolvedProductTeamId && selectedDataObject.processAreaId && selectedDataObject.processAreaId !== resolvedProductTeamId) {
      setDialogError('Selected data object belongs to a different product team.');
      return;
    }

    if (pendingPersistAction === 'publish') {
      if (!resolvedDataObjectId) {
        setDialogError('Select a data object before publishing.');
        return;
      }
      if (!resolvedProductTeamId) {
        setDialogError('The selected data object is not assigned to a product team yet.');
        return;
      }
    }

    const payloadDefinition = reportDefinition;
    const basePayload: ReportSavePayload = {
      name: trimmedName,
      description: normalizedDescription,
      definition: payloadDefinition,
      productTeamId: resolvedProductTeamId,
      dataObjectId: resolvedDataObjectId
    };

    setPersisting(true);
    setDialogError(null);

    try {
      let result: ReportDetail;

      if (pendingPersistAction === 'draft') {
        if (activeReport) {
          const updatePayload: ReportUpdatePayload = {
            name: trimmedName,
            description: normalizedDescription,
            definition: payloadDefinition,
            productTeamId: resolvedProductTeamId,
            dataObjectId: resolvedDataObjectId
          };
          result = await updateReportMutation.mutateAsync({ reportId: activeReport.id, payload: updatePayload });
        } else {
          result = await createReportMutation.mutateAsync(basePayload);
        }

        hydrateDesignerFromDefinition(result.definition, `Draft "${result.name}" saved.`);
      } else {
        if (activeReport) {
          const publishPayload: ReportPublishPayload = {
            name: trimmedName,
            description: normalizedDescription,
            definition: payloadDefinition,
            productTeamId: resolvedProductTeamId!,
            dataObjectId: resolvedDataObjectId!
          };
          result = await publishReportMutation.mutateAsync({ reportId: activeReport.id, payload: publishPayload });
        } else {
          const created = await createReportMutation.mutateAsync(basePayload);
          const publishPayload: ReportPublishPayload = {
            name: trimmedName,
            description: normalizedDescription,
            definition: payloadDefinition,
            productTeamId: resolvedProductTeamId!,
            dataObjectId: resolvedDataObjectId!
          };
          result = await publishReportMutation.mutateAsync({ reportId: created.id, payload: publishPayload });
        }

        hydrateDesignerFromDefinition(result.definition, `Report "${result.name}" published.`);
      }

      setActiveReport(result);
      setReportName(result.name);
      setReportDescription(result.description ?? '');
      setDraftNameInput(result.name);
      setDraftDescriptionInput(result.description ?? '');
      setSelectedProductTeamId(result.productTeamId ?? null);
      setSelectedDataObjectId(result.dataObjectId ?? null);
      setDraftProductTeamIdInput(result.productTeamId ?? null);
      setDraftDataObjectIdInput(result.dataObjectId ?? null);
      setSaveDialogOpen(false);
      setDialogError(null);
      setPendingPersistAction('draft');

      if (typeof window !== 'undefined') {
        window.localStorage.removeItem(designerStorageKey);
      }
      hasHydratedDraftRef.current = true;

      await Promise.all([
        queryClient.invalidateQueries(reportQueryKeys.list()),
        queryClient.invalidateQueries(reportQueryKeys.list('draft')),
        queryClient.invalidateQueries(reportQueryKeys.detail(result.id))
      ]);

      refetchDraftReports();
    } catch (error) {
      const message = isAxiosError(error)
        ? error.response?.data?.detail ?? error.message
        : error instanceof Error
          ? error.message
          : 'Unable to persist the report. Please try again.';
      setDialogError(message);
      setStatusBanner({ message, color: 'error.main' });
    } finally {
      setPersisting(false);
    }
  }, [
    activeReport,
    createReportMutation,
    dataObjectsById,
    draftDataObjectIdInput,
    draftDescriptionInput,
    draftNameInput,
    draftProductTeamIdInput,
    hydrateDesignerFromDefinition,
    pendingPersistAction,
    publishReportMutation,
    queryClient,
    refetchDraftReports,
    reportDefinition,
    setActiveReport,
    setDraftDataObjectIdInput,
    setDraftProductTeamIdInput,
    setReportDescription,
    setReportName,
    setSelectedDataObjectId,
    setSelectedProductTeamId,
    setStatusBanner,
    updateReportMutation
  ]);

  const formatTimestamp = (value?: string | null) => {
    if (!value) {
      return '—';
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
  };

  const handleOpenDraftList = useCallback(() => {
    setDraftListOpen(true);
    refetchDraftReports();
  }, [refetchDraftReports]);

  const handleDraftListClose = useCallback(() => {
    if (persisting) {
      return;
    }
    setDraftListOpen(false);
  }, [persisting]);

  const loadReportDetail = useCallback(
    async (reportId: string, messageFactory?: (detail: ReportDetail) => string) => {
      setLoadingReportId(reportId);
      setDialogError(null);
      try {
        const detail = await fetchReport(reportId);
        const bannerMessage = messageFactory ? messageFactory(detail) : `Report "${detail.name}" loaded.`;
        hydrateDesignerFromDefinition(detail.definition, bannerMessage);
        setActiveReport(detail);
        setReportName(detail.name);
        setReportDescription(detail.description ?? '');
        setDraftNameInput(detail.name);
        setDraftDescriptionInput(detail.description ?? '');
        setSelectedProductTeamId(detail.productTeamId ?? null);
        setSelectedDataObjectId(detail.dataObjectId ?? null);
        setDraftProductTeamIdInput(detail.productTeamId ?? null);
        setDraftDataObjectIdInput(detail.dataObjectId ?? null);
        setDraftListOpen(false);
        setSaveDialogOpen(false);
        setPendingPersistAction('draft');
        setPreviewOpen(false);
        setPreviewData(null);
        setPreviewError(null);
        setPreviewLoading(false);
        if (typeof window !== 'undefined') {
          window.localStorage.removeItem(designerStorageKey);
        }
        hasHydratedDraftRef.current = true;
      } catch (error) {
        const message = isAxiosError(error)
          ? error.response?.data?.detail ?? error.message
          : error instanceof Error
            ? error.message
            : 'Unable to load the selected report. Please try again.';
        setStatusBanner({ message, color: 'error.main' });
      } finally {
        setLoadingReportId(null);
      }
    },
    [
      setDialogError,
      setDraftListOpen,
      hydrateDesignerFromDefinition,
      setActiveReport,
      setDraftDescriptionInput,
      setDraftNameInput,
      setDraftDataObjectIdInput,
      setDraftProductTeamIdInput,
      setPendingPersistAction,
      setPreviewData,
      setPreviewError,
      setPreviewLoading,
      setPreviewOpen,
      setReportDescription,
      setReportName,
      setSelectedDataObjectId,
      setSelectedProductTeamId,
      setSaveDialogOpen,
      setLoadingReportId,
      setStatusBanner
    ]
  );

  // Only process any router-provided state once on initial mount. Previously
  // this effect ran whenever `location` changed and could call
  // `navigate(location.pathname, { replace: true })` repeatedly which
  // interfered with subsequent client-side navigation in some cases. Guard
  // with a ref so we only consume the initial `location.state` once.
  const initialLocationStateProcessedRef = useRef(false);
  useEffect(() => {
    if (initialLocationStateProcessedRef.current) {
      return;
    }
    initialLocationStateProcessedRef.current = true;
    const state = location.state as { reportId?: string } | null;
    if (state?.reportId) {
      setPendingCatalogReportId(state.reportId);
      navigate(location.pathname, { replace: true });
    }
  }, [location, navigate]);

  useEffect(() => {
    if (!pendingCatalogReportId) {
      return;
    }
    if (tablesLoading || definitionsLoading || dataObjectsLoading || processAreasLoading) {
      return;
    }

    void loadReportDetail(pendingCatalogReportId, (detail) => `Report "${detail.name}" loaded for editing.`).finally(() => {
      setPendingCatalogReportId(null);
    });
  }, [
    dataObjectsLoading,
    definitionsLoading,
    loadReportDetail,
    pendingCatalogReportId,
    processAreasLoading,
    tablesLoading
  ]);

  const handleLoadDraftReport = useCallback(
    async (report: ReportSummary) => {
      await loadReportDetail(report.id, (detail) => `Draft "${detail.name}" loaded.`);
    },
    [loadReportDetail]
  );

  const handleDeleteDraftReport = useCallback(
    async (report: ReportSummary) => {
      if (typeof window !== 'undefined') {
        const confirmDelete = window.confirm(`Delete draft "${report.name}"? This action cannot be undone.`);
        if (!confirmDelete) {
          return;
        }
      }

      setDeletingReportId(report.id);

      try {
        await deleteReportMutation.mutateAsync(report.id);

        await Promise.all([
          queryClient.invalidateQueries(reportQueryKeys.list()),
          queryClient.invalidateQueries(reportQueryKeys.list('draft')),
          queryClient.invalidateQueries(reportQueryKeys.detail(report.id))
        ]);

        refetchDraftReports();

        if (activeReport?.id === report.id) {
          resetDesignerState({ message: `Draft "${report.name}" deleted.`, color: 'warning.main' });
        } else {
          setStatusBanner({ message: `Draft "${report.name}" deleted.`, color: 'warning.main' });
        }
      } catch (error) {
        const message = isAxiosError(error)
          ? error.response?.data?.detail ?? error.message
          : error instanceof Error
            ? error.message
            : 'Unable to delete the draft. Please try again.';
        setStatusBanner({ message, color: 'error.main' });
      } finally {
        setDeletingReportId(null);
      }
    },
    [
      activeReport,
      deleteReportMutation,
      queryClient,
      refetchDraftReports,
      resetDesignerState,
      setStatusBanner
    ]
  );

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
    handleOpenPersistDialog('draft');
  }, [handleOpenPersistDialog]);

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
    handleOpenPersistDialog('publish');
  }, [handleOpenPersistDialog]);

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
    resetDesignerState({ message: 'Draft cleared and canvas reset.', color: 'warning.main' });
  }, [resetDesignerState]);

  const handleDragStart = useCallback((event: React.DragEvent<HTMLElement>, table: Table) => {
    const payload: DragPayload = { tableId: table.id };
    event.dataTransfer.setData(tableDragMimeType, JSON.stringify(payload));
    event.dataTransfer.effectAllowed = 'move';
  }, []);

  const handleDragOver = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    // Only prevent default when the drag payload matches our table drag MIME type.
    // Preventing default for unrelated drag events can cause navigation clicks to be suppressed.
    const types = Array.from(event.dataTransfer?.types ?? []);
    if (!types.includes(tableDragMimeType)) {
      return;
    }
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const handleDrop = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    // Check for our specific table drag payload first. Avoid preventing default for unrelated drops.
    const raw = event.dataTransfer.getData(tableDragMimeType);
    if (!raw) {
      return;
    }
    event.preventDefault();

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
    // Set a short-lived sentinel so the global click-inspector can ignore
    // a spurious defaultPrevented state that sometimes persists after a
    // React Flow drop. Cleared after 300ms.
    try {
      const sentinelWindow = window as RecentDropSentinelWindow;
      sentinelWindow.__CC_RECENT_DROP = true;
      setTimeout(() => {
        sentinelWindow.__CC_RECENT_DROP = false;
      }, 300);
    } catch {
      // noop
    }
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
    // Only treat drops that include our reporting field drag type as a field drop.
    const raw = event.dataTransfer.getData(REPORTING_FIELD_DRAG_TYPE);
    setOutputDropActive(false);
    if (!raw) {
      return;
    }
    event.preventDefault();

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

  const renderTreeItem = useCallback(
    (node: PaletteTreeNode): ReactNode => {
      if (node.type === 'table' && node.table) {
        const alreadySelected = nodesById.has(node.table.id);
        return (
          <TreeItem
            key={node.id}
            itemId={node.id}
            label={
              <Stack
                direction="row"
                spacing={1}
                alignItems="center"
                draggable={!alreadySelected}
                onDragStart={(event) => {
                  if (alreadySelected) {
                    event.preventDefault();
                    return;
                  }
                  event.stopPropagation();
                  handleDragStart(event, node.table!);
                }}
                onDoubleClick={(event) => {
                  event.stopPropagation();
                  if (!alreadySelected) {
                    handlePaletteAdd(node.table!);
                  }
                }}
                sx={{ cursor: alreadySelected ? 'default' : 'grab', py: 0.5, pr: 0.5 }}
              >
                <DragIndicatorIcon fontSize="small" color={alreadySelected ? 'disabled' : 'action'} />
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography
                    variant="body2"
                    sx={{ fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis' }}
                  >
                    {node.label}
                  </Typography>
                  {node.secondary && (
                    <Typography
                      variant="caption"
                      color="text.secondary"
                      sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis' }}
                    >
                      {node.secondary}
                    </Typography>
                  )}
                </Box>
                <Tooltip title={alreadySelected ? 'Already on canvas' : 'Add to canvas'}>
                  <span>
                    <IconButton
                      size="small"
                      color="primary"
                      disabled={alreadySelected}
                      onClick={(event) => {
                        event.stopPropagation();
                        if (!alreadySelected) {
                          handlePaletteAdd(node.table!);
                        }
                      }}
                    >
                      <AddCircleOutlineIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>
              </Stack>
            }
          />
        );
      }

      const childCount = node.children?.reduce<number>((total, child) => {
        if (child.type === 'table') {
          return total + 1;
        }
        return total + (child.tableCount ?? 0);
      }, 0) ?? node.tableCount ?? 0;

      return (
        <TreeItem
          key={node.id}
          itemId={node.id}
          label={
            <Stack direction="row" alignItems="center" spacing={1} sx={{ py: 0.5, pr: 0.5 }}>
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Typography
                  variant="body2"
                  sx={{ fontWeight: node.type === 'productTeam' || node.type === 'unassigned' ? 600 : 500, overflow: 'hidden', textOverflow: 'ellipsis' }}
                >
                  {node.label}
                </Typography>
                {node.secondary && (
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    sx={{ display: 'block', overflow: 'hidden', textOverflow: 'ellipsis' }}
                  >
                    {node.secondary}
                  </Typography>
                )}
              </Box>
              {childCount > 0 && (
                <Chip size="small" label={childCount} sx={{ height: 18 }} />
              )}
            </Stack>
          }
        >
          {node.children?.map((child: PaletteTreeNode) => renderTreeItem(child))}
        </TreeItem>
      );
    },
    [handleDragStart, handlePaletteAdd, nodesById]
  );

  const selectedProductTeam = selectedProductTeamId
    ? processAreasById.get(selectedProductTeamId) ?? null
    : null;
  const selectedProductTeamLabel = selectedProductTeam?.name
    ?? (selectedProductTeamId ? activeReport?.productTeamName ?? 'Unknown product team' : 'Unassigned');
  const selectedDataObject = selectedDataObjectId
    ? dataObjectsById.get(selectedDataObjectId) ?? null
    : null;
  const selectedDataObjectLabel = selectedDataObject?.name
    ?? (selectedDataObjectId ? activeReport?.dataObjectName ?? 'Unknown data object' : 'Unassigned');
  const selectedDataObjectTeamName = selectedDataObject?.processAreaId
    ? processAreasById.get(selectedDataObject.processAreaId)?.name ?? null
    : null;
  const productTeamHelperText =
    pendingPersistAction === 'publish' ? 'Product team required to publish.' : 'Optional for drafts.';
  const dataObjectHelperText =
    pendingPersistAction === 'publish' ? 'Data object required to publish.' : 'Optional for drafts.';
  const dataObjectPlaceholderLabel = dataObjects.length === 0
    ? 'No data objects available'
    : draftProductTeamIdInput && filteredDataObjects.length === 0
      ? 'No data objects for selected product team'
      : 'Select data object';

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, minHeight: '100vh' }}>
      <PageHeader
        title="Report Designer"
        subtitle="Compose relational-style report definitions by combining enterprise tables, mapping joins, and shaping output fields prior to publishing."
      />
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
              {paletteLoading && (
                <Stack sx={{ py: 6 }} alignItems="center" spacing={1}>
                  <CircularProgress size={24} />
                  <Typography variant="body2" color="text.secondary">
                    Loading table palette…
                  </Typography>
                </Stack>
              )}
              {paletteError && (
                <Stack sx={{ py: 6, px: 2 }} spacing={1}>
                  <Typography variant="body2" color="error">
                    {paletteErrorMessage}
                  </Typography>
                </Stack>
              )}
              {!paletteLoading && !paletteError && (
                filteredPaletteHierarchy.length === 0 ? (
                  <Stack sx={{ py: 6, px: 2 }} spacing={1} alignItems="center">
                    <Typography variant="body2" color="text.secondary" align="center">
                      {allowedTableIds.size === 0
                        ? 'No tables are linked to data definitions yet. Add tables to a data definition to make them available here.'
                        : 'No matching tables. Refine your search or clear the filter.'}
                    </Typography>
                  </Stack>
                ) : (
                  <SimpleTreeView
                    expandedItems={expandedNodes}
                    onExpandedItemsChange={(_: SyntheticEvent | null, itemIds: string[]) => setExpandedNodes(itemIds)}
                    disableSelection
                    slots={{ collapseIcon: ExpandMoreIcon, expandIcon: ChevronRightIcon }}
                    sx={{ py: 1, pr: 1 }}
                  >
                    {filteredPaletteHierarchy.map((node) => renderTreeItem(node))}
                  </SimpleTreeView>
                )
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
                style={{ width: '100%', height: '100%', background: canvasBackground }}
                onInit={(instance) => { reactFlowInstanceRef.current = instance; }}
              >
                <MiniMap
                  pannable
                  zoomable
                  nodeColor={() => miniMapNodeColor}
                  style={{ background: miniMapBackground, borderRadius: 8 }}
                />
                <Controls position="bottom-right" />
                <Background color={canvasGridColor} gap={24} size={0.6} />
              </ReactFlow>
            </Paper>
            <Stack
              direction={{ xs: 'column', sm: 'row' }}
              spacing={1}
              alignItems={{ xs: 'flex-start', sm: 'center' }}
              sx={{ flexWrap: 'wrap' }}
            >
              <Chip
                variant="outlined"
                color={selectedProductTeamId ? 'primary' : 'default'}
                label={`Product Team: ${selectedProductTeamLabel}`}
              />
              <Chip
                variant="outlined"
                color={selectedDataObjectId ? 'primary' : 'default'}
                label={
                  selectedDataObjectTeamName && selectedDataObjectLabel !== 'Unassigned'
                    ? `Data Object: ${selectedDataObjectLabel} (${selectedDataObjectTeamName})`
                    : `Data Object: ${selectedDataObjectLabel}`
                }
              />
            </Stack>
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
                disabled={actionButtonsDisabled || persisting}
                onClick={handleSaveDraft}
              >
                Save Draft
              </Button>
              <Button
                variant="outlined"
                startIcon={<FolderOpenIcon />}
                onClick={handleOpenDraftList}
                disabled={persisting}
              >
                Load Draft
              </Button>
              <Button
                variant="outlined"
                startIcon={<PublishIcon />}
                disabled={actionButtonsDisabled || persisting}
                onClick={handlePublish}
              >
                Publish to Report Catalog
              </Button>
              <Button
                variant="text"
                color="warning"
                startIcon={<RestartAltIcon />}
                disabled={persisting}
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

      <Dialog open={saveDialogOpen} onClose={handlePersistDialogClose} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ pb: 1.5 }}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            {pendingPersistAction === 'draft' ? 'Save Draft' : 'Publish Report'}
          </Typography>
        </DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <Typography variant="body2" color="text.secondary">
              {pendingPersistAction === 'draft'
                ? 'Name this draft and provide an optional description before saving to the reporting catalog.'
                : 'Confirm the report details before publishing to the reporting catalog.'}
            </Typography>
            <TextField
              label="Report Name"
              value={draftNameInput}
              onChange={(event) => setDraftNameInput(event.target.value)}
              inputRef={draftNameInputRef}
              required
              disabled={persisting}
            />
            <TextField
              label="Description"
              value={draftDescriptionInput}
              onChange={(event) => setDraftDescriptionInput(event.target.value)}
              multiline
              minRows={2}
              disabled={persisting}
            />
            <TextField
              select
              label="Product Team"
              value={draftProductTeamIdInput ?? ''}
              onChange={(event) => {
                const value = event.target.value as string;
                const nextValue = value ? value : null;
                setDraftProductTeamIdInput(nextValue);
                if (draftDataObjectIdInput) {
                  const currentDataObject = dataObjectsById.get(draftDataObjectIdInput);
                  if (!currentDataObject) {
                    setDraftDataObjectIdInput(null);
                    return;
                  }
                  const currentProcessAreaId = currentDataObject.processAreaId ?? null;
                  const mismatchedSelectedTeam = currentProcessAreaId && nextValue && currentProcessAreaId !== nextValue;
                  const clearedTeamWhileAssigned = currentProcessAreaId && !nextValue;
                  const assignedTeamWhileUnassigned = !currentProcessAreaId && nextValue;
                  if (mismatchedSelectedTeam || clearedTeamWhileAssigned || assignedTeamWhileUnassigned) {
                    setDraftDataObjectIdInput(null);
                  }
                }
              }}
              helperText={productTeamHelperText}
              disabled={persisting}
            >
              <MenuItem value="">Unassigned</MenuItem>
              {sortedProcessAreas.map((processArea) => (
                <MenuItem key={processArea.id} value={processArea.id}>
                  {processArea.name}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Data Object"
              value={draftDataObjectIdInput ?? ''}
              onChange={(event) => {
                const value = event.target.value as string;
                const nextValue = value ? value : null;
                setDraftDataObjectIdInput(nextValue);
                if (nextValue) {
                  const nextDataObject = dataObjectsById.get(nextValue);
                  setDraftProductTeamIdInput(nextDataObject?.processAreaId ?? null);
                }
              }}
              helperText={dataObjectHelperText}
              disabled={persisting || dataObjects.length === 0}
            >
              <MenuItem value="">
                {dataObjectPlaceholderLabel}
              </MenuItem>
              {filteredDataObjects.map((dataObject) => {
                const associatedTeamName = dataObject.processAreaId
                  ? processAreasById.get(dataObject.processAreaId)?.name ?? 'Unknown product team'
                  : 'Unassigned';
                return (
                  <MenuItem key={dataObject.id} value={dataObject.id}>
                    {associatedTeamName === 'Unassigned'
                      ? dataObject.name
                      : `${dataObject.name} — ${associatedTeamName}`}
                  </MenuItem>
                );
              })}
            </TextField>
            {dialogError && <Alert severity="error">{dialogError}</Alert>}
          </Stack>
        </DialogContent>
        <DialogActions sx={{ px: 2.5, py: 1.5 }}>
          <Button onClick={handlePersistDialogClose} disabled={persisting}>
            Cancel
          </Button>
          <Button
            onClick={handlePersistConfirm}
            variant="contained"
            startIcon={
              persisting ? (
                <CircularProgress size={18} color="inherit" />
              ) : pendingPersistAction === 'draft' ? (
                <SaveIcon />
              ) : (
                <PublishIcon />
              )
            }
            disabled={persisting || !draftNameInput.trim()}
          >
            {pendingPersistAction === 'draft' ? 'Save Draft' : 'Publish Report'}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={draftListOpen} onClose={handleDraftListClose} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ pb: 1.5 }}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Load Saved Draft
          </Typography>
        </DialogTitle>
        <DialogContent dividers>
          {draftReportsLoading || draftReportsFetching ? (
            <Stack spacing={1.5} alignItems="center" sx={{ py: 4 }}>
              <CircularProgress size={24} />
              <Typography variant="body2" color="text.secondary">
                Fetching drafts…
              </Typography>
            </Stack>
          ) : draftReports.length === 0 ? (
            <Typography variant="body2" color="text.secondary">
              No saved drafts are available yet. Save a draft to see it listed here.
            </Typography>
          ) : (
            <List dense disablePadding>
              {draftReports.map((report) => (
                <ListItem
                  key={report.id}
                  disablePadding
                  secondaryAction={
                    <Tooltip title="Delete draft">
                      <span>
                        <IconButton
                          edge="end"
                          size="small"
                          color="error"
                          onClick={() => handleDeleteDraftReport(report)}
                          disabled={deletingReportId === report.id || persisting}
                        >
                          {deletingReportId === report.id ? (
                            <CircularProgress size={18} color="inherit" />
                          ) : (
                            <DeleteOutlineIcon fontSize="small" />
                          )}
                        </IconButton>
                      </span>
                    </Tooltip>
                  }
                >
                  <ListItemButton
                    onClick={() => handleLoadDraftReport(report)}
                    disabled={loadingReportId === report.id || persisting}
                  >
                    <ListItemText
                      primary={report.name}
                      primaryTypographyProps={{ fontWeight: 600 }}
                      secondary={`Updated ${formatTimestamp(report.updatedAt)}`}
                      secondaryTypographyProps={{ variant: 'caption' }}
                    />
                    {(loadingReportId === report.id || activeReport?.id === report.id) && (
                      <Stack direction="row" alignItems="center" spacing={1} sx={{ ml: 1 }}>
                        {loadingReportId === report.id ? (
                          <CircularProgress size={18} />
                        ) : (
                          <Chip label="Active" color="primary" size="small" />
                        )}
                      </Stack>
                    )}
                  </ListItemButton>
                </ListItem>
              ))}
            </List>
          )}
        </DialogContent>
        <DialogActions sx={{ px: 2.5, py: 1.5 }}>
          <Button onClick={handleDraftListClose}>Close</Button>
        </DialogActions>
      </Dialog>

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
      <ConfirmDialog
        open={Boolean(removeDialogState)}
        title="Remove Table"
        description={`Remove ${removeDialogState?.name ?? 'this table'} from the Relationship Canvas? Joins and selected columns that reference it will also be cleared.`}
        confirmLabel="Remove"
        onClose={handleCancelRemoveNode}
        onConfirm={handleConfirmRemoveNode}
      />
    </Box>
  );
};

const ReportingDesignerPage = () => (
  <ReactFlowProvider>
    <ReportingDesignerContent />
  </ReactFlowProvider>
);

export default ReportingDesignerPage;
