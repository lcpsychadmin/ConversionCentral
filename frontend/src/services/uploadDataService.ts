import client from './api/client';
import {
  DataWarehouseTarget,
  UploadDataCreateResponse,
  UploadDataPreview,
  UploadTableMode
} from '../types/data';

interface UploadDataPreviewResponsePayload {
  columns: Array<{
    original_name: string;
    field_name: string;
    inferred_type: string;
  }>;
  sample_rows: (string | null)[][];
  total_rows: number;
}

interface PreviewOptions {
  hasHeader?: boolean;
  delimiter?: string | null;
}

export const previewUploadData = async (
  file: File,
  options?: PreviewOptions
): Promise<UploadDataPreview> => {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('has_header', options?.hasHeader === false ? 'false' : 'true');
  if (options?.delimiter) {
    formData.append('delimiter', options.delimiter);
  }

  const response = await client.post<UploadDataPreviewResponsePayload>('/upload-data/preview', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  });

  return {
    columns: response.data.columns.map((column) => ({
      originalName: column.original_name,
      fieldName: column.field_name,
      inferredType: column.inferred_type
    })),
    sampleRows: response.data.sample_rows,
    totalRows: response.data.total_rows
  };
};

interface UploadDataCreateResponsePayload {
  table_name: string;
  schema_name?: string | null;
  catalog?: string | null;
  rows_inserted: number;
  target_warehouse: DataWarehouseTarget;
  table_id?: string | null;
  constructed_table_id?: string | null;
  data_definition_id?: string | null;
  data_definition_table_id?: string | null;
}

interface CreateTableOptions {
  tableName: string;
  file: File;
  targetWarehouse: DataWarehouseTarget;
  schemaName?: string;
  catalog?: string;
  mode?: UploadTableMode;
  hasHeader?: boolean;
  delimiter?: string | null;
  productTeamId: string;
  dataObjectId: string;
  systemId: string;
}

export const createTableFromUpload = async (
  options: CreateTableOptions
): Promise<UploadDataCreateResponse> => {
  const formData = new FormData();
  formData.append('file', options.file);
  formData.append('table_name', options.tableName);
  formData.append('target', options.targetWarehouse);
  formData.append('mode', options.mode ?? 'create');
  formData.append('has_header', options.hasHeader === false ? 'false' : 'true');
  formData.append('product_team_id', options.productTeamId);
  formData.append('data_object_id', options.dataObjectId);
  formData.append('system_id', options.systemId);

  if (options.schemaName) {
    formData.append('schema_name', options.schemaName);
  }
  if (options.catalog) {
    formData.append('catalog', options.catalog);
  }
  if (options.delimiter) {
    formData.append('delimiter', options.delimiter);
  }

  const response = await client.post<UploadDataCreateResponsePayload>('/upload-data/create-table', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  });

  return {
    tableName: response.data.table_name,
    schemaName: response.data.schema_name ?? null,
    catalog: response.data.catalog ?? null,
    rowsInserted: response.data.rows_inserted,
    targetWarehouse: response.data.target_warehouse,
    tableId: response.data.table_id ?? null,
    constructedTableId: response.data.constructed_table_id ?? null,
    dataDefinitionId: response.data.data_definition_id ?? null,
    dataDefinitionTableId: response.data.data_definition_table_id ?? null
  };
};

export const deleteUploadedTable = async (tableId: string): Promise<void> => {
  await client.delete(`/upload-data/tables/${tableId}`);
};
