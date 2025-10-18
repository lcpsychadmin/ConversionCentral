import client from './api/client';
import { FieldLoad } from '../types/data';

export const fetchFieldLoads = async (): Promise<FieldLoad[]> => {
  const response = await client.get<FieldLoad[]>('/field-loads');
  return response.data;
};
