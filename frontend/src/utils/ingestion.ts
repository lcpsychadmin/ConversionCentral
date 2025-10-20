import { System, SystemConnection } from '../types/data';

const sanitizePart = (value?: string | null): string => {
  if (!value) {
    return 'segment';
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return 'segment';
  }
  return trimmed
    .replace(/[^A-Za-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase() || 'segment';
};

export const buildIngestionTargetName = (
  system: System | undefined,
  connection: SystemConnection,
  schemaName: string | null | undefined,
  tableName: string
): string => {
  const systemPart = system?.name || system?.physicalName || connection.systemId;
  const schemaPart = schemaName ?? 'dbo';
  return [sanitizePart(systemPart), sanitizePart(schemaPart), sanitizePart(tableName)].join('_');
};
