import type { AvailableSourceTable } from '../../services/dataDefinitionService';

const normalize = (value?: string | null) => (value ?? '').trim().toLowerCase();

export const toSourceTableKey = (
  catalogName?: string | null,
  schemaName?: string | null,
  tableName?: string | null
): string | null => {
  if (!tableName) {
    return null;
  }

  const normalizedCatalog = normalize(catalogName);
  const normalizedSchema = normalize(schemaName);
  const normalizedTable = tableName.trim().toLowerCase();

  return `${normalizedCatalog}::${normalizedSchema}::${normalizedTable}`;
};

export const toSchemaTableKey = (
  schemaName?: string | null,
  tableName?: string | null
): string | null => {
  if (!tableName) {
    return null;
  }
  const normalizedSchema = normalize(schemaName);
  const normalizedTable = tableName.trim().toLowerCase();
  return `${normalizedSchema}::${normalizedTable}`;
};

export const sourceKeyToSchemaKey = (sourceKey?: string | null): string | null => {
  if (!sourceKey) {
    return null;
  }
  const parts = sourceKey.split('::');
  if (parts.length < 3) {
    return null;
  }
  const [, schema, table] = parts;
  if (!table) {
    return null;
  }
  return toSchemaTableKey(schema, table);
};

export const buildSourceTableKeyList = (tableList: AvailableSourceTable[]): string[] => {
  const keys = new Set<string>();
  tableList.forEach((table) => {
    const key = toSourceTableKey(table.catalogName, table.schemaName, table.tableName);
    if (key) {
      keys.add(key);
    }
  });
  return Array.from(keys);
};
