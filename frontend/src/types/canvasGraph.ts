// Shared interfaces describing the dbt Canvas graph payload exchanged between the
// React Flow builder and backend translators.

type CanvasExtendedString = string & { readonly __canvasBrand?: never };

export type CanvasGraphResourceType =
  | 'source'
  | 'model'
  | 'seed'
  | 'snapshot'
  | 'exposure'
  | 'metric'
  | CanvasExtendedString;

export type CanvasGraphEdgeType =
  | 'ref'
  | 'source'
  | 'exposure'
  | 'metric'
  | CanvasExtendedString;

export type CanvasGraphLayer =
  | 'source'
  | 'staging'
  | 'intermediate'
  | 'mart'
  | 'analytics'
  | 'exposure'
  | CanvasExtendedString;

export type CanvasGraphMaterialization =
  | 'view'
  | 'table'
  | 'incremental'
  | 'ephemeral'
  | 'seed'
  | 'snapshot'
  | CanvasExtendedString;

export type CanvasGraphAccess = 'public' | 'protected' | 'private' | CanvasExtendedString;

export type CanvasGraphJoinType =
  | 'inner'
  | 'left'
  | 'right'
  | 'full'
  | 'cross'
  | 'anti'
  | 'semi'
  | CanvasExtendedString;

export interface CanvasGraphPosition {
  x: number;
  y: number;
}

export interface CanvasGraphSize {
  width: number;
  height: number;
}

export interface CanvasGraphHookConfig {
  pre?: string[];
  post?: string[];
}

export interface CanvasGraphIncrementalConfig {
  strategy?: string;
  uniqueKey?: string | string[];
  partitionBy?: string | string[];
  clusterBy?: string[];
}

export interface CanvasGraphColumnTest {
  name: string;
  arguments?: Record<string, unknown>;
}

export type CanvasGraphColumnTests = Record<string, CanvasGraphColumnTest[]>;

export interface CanvasGraphModelTest {
  name: string;
  arguments?: Record<string, unknown>;
}

export interface CanvasGraphNodeExposure {
  name: string;
  type: 'dashboard' | 'notebook' | 'ml' | 'analysis' | CanvasExtendedString;
  url?: string;
  maturity?: 'low' | 'medium' | 'high' | CanvasExtendedString;
  description?: string | null;
  owner?: string | null;
}

export interface CanvasGraphNodeMetric {
  name: string;
  label?: string;
  description?: string | null;
  calculation?: string;
  meta?: Record<string, unknown>;
}

export interface CanvasGraphColumn {
  id: string;
  name: string;
  dataType?: string;
  description?: string | null;
  tests?: CanvasGraphColumnTest[];
  meta?: Record<string, unknown>;
}

export interface CanvasGraphNodeConfig {
  materialization?: CanvasGraphMaterialization;
  access?: CanvasGraphAccess;
  tags?: string[];
  owner?: string | null;
  path?: string | null;
  sql?: string | null;
  description?: string | null;
  meta?: Record<string, unknown>;
  hooks?: CanvasGraphHookConfig;
  incremental?: CanvasGraphIncrementalConfig;
  columnTests?: CanvasGraphColumnTests;
  tests?: CanvasGraphModelTest[];
  exposures?: CanvasGraphNodeExposure[];
  metrics?: CanvasGraphNodeMetric[];
}

export interface CanvasGraphNodeOrigin {
  dataDefinitionTableId?: string;
  systemId?: string;
  schemaName?: string;
  tableName?: string;
  isConstructed?: boolean;
}

export interface CanvasGraphNode {
  id: string;
  name: string;
  label: string;
  resourceType: CanvasGraphResourceType;
  position: CanvasGraphPosition;
  size?: CanvasGraphSize;
  layer?: CanvasGraphLayer;
  description?: string | null;
  origin?: CanvasGraphNodeOrigin;
  config?: CanvasGraphNodeConfig;
  columns?: CanvasGraphColumn[];
  meta?: Record<string, unknown>;
}

export interface CanvasGraphJoinColumn {
  column: string;
  fieldId?: string;
}

export interface CanvasGraphJoinFilter {
  expression: string;
  description?: string | null;
}

export interface CanvasGraphJoin {
  type?: CanvasGraphJoinType;
  condition?: string;
  sourceColumns?: CanvasGraphJoinColumn[];
  targetColumns?: CanvasGraphJoinColumn[];
  filters?: CanvasGraphJoinFilter[];
}

export interface CanvasGraphEdge {
  id: string;
  source: string;
  target: string;
  type: CanvasGraphEdgeType;
  description?: string | null;
  relationshipId?: string;
  join?: CanvasGraphJoin;
  meta?: Record<string, unknown>;
}

export interface CanvasGraphGroup {
  id: string;
  label: string;
  layer?: CanvasGraphLayer;
  color?: string;
  description?: string | null;
}

export interface CanvasGraphMetadata {
  projectId?: string;
  projectName?: string;
  definitionId?: string;
  dbtVersion?: string | null;
  generatedBy?: string | null;
  generatedAt?: string;
  description?: string | null;
}

export interface CanvasGraph {
  metadata?: CanvasGraphMetadata;
  nodes: CanvasGraphNode[];
  edges: CanvasGraphEdge[];
  groups?: CanvasGraphGroup[];
  warnings?: string[];
}
