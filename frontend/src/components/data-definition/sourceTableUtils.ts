import type { AvailableSourceTable } from '../../services/dataDefinitionService';

export const toSourceTableKey = (
  catalogName?: string | null,
  schemaName?: string | null,
  tableName?: string | null
): string | null => {
  if (!tableName) {
    return null;
  }

  const normalizedCatalog = (catalogName ?? '').trim().toLowerCase();
  const normalizedSchema = (schemaName ?? '').trim().toLowerCase();
  const normalizedTable = tableName.trim().toLowerCase();

  return `${normalizedCatalog}::${normalizedSchema}::${normalizedTable}`;
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
