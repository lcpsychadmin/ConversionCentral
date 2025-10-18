import client from './api/client';
import { ProjectSummary } from '../types/data';

export const fetchDashboardSummary = async (): Promise<ProjectSummary> => {
  const response = await client.get('/dashboard/summary');
  return response.data;
};
