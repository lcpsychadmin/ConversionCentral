export type ReportSortDirection = 'none' | 'asc' | 'desc';
export type ReportAggregateFn =
  | 'groupBy'
  | 'sum'
  | 'avg'
  | 'min'
  | 'max'
  | 'count'
  | 'first'
  | 'last'
  | 'expression'
  | 'where';

export type ReportJoinType = 'inner' | 'left' | 'right';

export interface ReportDesignerTablePlacement {
  tableId: string;
  label: string;
  physicalName: string;
  schemaName?: string | null;
  alias?: string | null;
  position: {
    x: number;
    y: number;
  };
  dimensions?: {
    width?: number;
    height?: number;
  };
}

export interface ReportDesignerJoin {
  id: string;
  sourceTableId: string;
  targetTableId: string;
  sourceFieldId?: string | null;
  targetFieldId?: string | null;
  joinType?: ReportJoinType;
}

export interface ReportDesignerColumn {
  order: number;
  fieldId: string;
  fieldName: string;
  fieldDescription?: string | null;
  tableId: string;
  tableName?: string | null;
  show: boolean;
  sort: ReportSortDirection;
  aggregate?: ReportAggregateFn | null;
  criteria: string[];
}

export interface ReportDesignerDefinition {
  tables: ReportDesignerTablePlacement[];
  joins: ReportDesignerJoin[];
  columns: ReportDesignerColumn[];
  criteriaRowCount: number;
  groupingEnabled: boolean;
}

export type ReportStatus = 'draft' | 'published';

export interface ReportSummary {
  id: string;
  name: string;
  description?: string | null;
  status: ReportStatus;
  createdAt: string;
  updatedAt: string;
  publishedAt?: string | null;
  productTeamId?: string | null;
  productTeamName?: string | null;
  dataObjectId?: string | null;
  dataObjectName?: string | null;
  workspaceId?: string | null;
  workspaceName?: string | null;
}

export interface ReportDetail extends ReportSummary {
  definition: ReportDesignerDefinition;
}

export interface ReportDatasetPayload {
  limit?: number;
}

export interface ReportDatasetResponse {
  reportId: string;
  name: string;
  limit: number;
  rowCount: number;
  generatedAt: string;
  columns: string[];
  rows: Record<string, unknown>[];
}
