import { RelationalDatabaseType, SystemConnectionFormValues } from '../types/data';

export interface JdbcConnectionDetails {
  databaseType: RelationalDatabaseType;
  host: string;
  port: string;
  database: string;
  username: string;
  password: string;
  options: Record<string, string>;
}

const ALLOW_EMPTY_QUERY_KEYS = new Set(['schema']);

const buildQueryString = (options?: Record<string, string>): string => {
  if (!options) return '';
  const entries = Object.entries(options).filter(([key, value]) => {
    if (!key || value === undefined) {
      return false;
    }
    if (value === '') {
      return ALLOW_EMPTY_QUERY_KEYS.has(key);
    }
    return true;
  });
  if (!entries.length) return '';
  const query = entries
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join('&');
  return query ? `?${query}` : '';
};

export const buildJdbcConnectionString = (values: SystemConnectionFormValues): string => {
  const { databaseType, host, port, database, username, password, options } = values;

  if (!databaseType) {
    throw new Error('Database type is required.');
  }

  const sanitizedHost = host.trim();
  if (!sanitizedHost) {
    throw new Error('Host is required.');
  }

  const sanitizedDatabase = database.trim();
  if (!sanitizedDatabase) {
    throw new Error('Database name is required.');
  }

  const credentials = username
    ? password
      ? `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`
      : `${encodeURIComponent(username)}@`
    : '';
  const portSegment = port ? `:${port}` : '';
  const query = buildQueryString(options);

  return `jdbc:${databaseType}://${credentials}${sanitizedHost}${portSegment}/${sanitizedDatabase}${query}`;
};

export const parseJdbcConnectionString = (
  connectionString: string | null | undefined
): JdbcConnectionDetails | null => {
  if (!connectionString || !connectionString.startsWith('jdbc:')) {
    return null;
  }

  try {
    const raw = connectionString.replace(/^jdbc:/, '');
    const url = new URL(raw);

    const databaseType = url.protocol.replace(':', '') as RelationalDatabaseType;
    const host = url.hostname ?? '';
    const port = url.port ?? '';
    const database = url.pathname.replace(/^\//, '');
    const username = url.username ?? '';
    const password = url.password ?? '';
    const options: Record<string, string> = {};
    url.searchParams.forEach((value, key) => {
      options[key] = value;
    });

    return {
      databaseType,
      host,
      port,
      database,
      username,
      password,
      options
    };
  } catch (error) {
    return null;
  }
};

export const formatConnectionSummary = (connectionString: string | null | undefined): string => {
  const parsed = parseJdbcConnectionString(connectionString);
  if (!parsed) {
    return connectionString ?? '—';
  }

  const { databaseType, username, host, port, database } = parsed;
  const credentials = username ? `${username}@` : '';
  const portSegment = port ? `:${port}` : '';
  return `${databaseType}://${credentials}${host}${portSegment}/${database}`;
};
