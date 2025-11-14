export interface PaginatedResponse<T> {
  value?: T[];
  count?: number;
}

// Normalizes API responses that may either return a raw array or a { value, count } envelope.
export const ensureArrayResponse = <T>(payload: T[] | PaginatedResponse<T> | undefined): T[] => {
  if (!payload) {
    return [];
  }

  if (Array.isArray(payload)) {
    return payload;
  }

  if (payload.value && Array.isArray(payload.value)) {
    return payload.value;
  }

  return [];
};
