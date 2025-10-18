import client from './api/client';
export const fetchDashboardSummary = async () => {
    const response = await client.get('/dashboard/summary');
    return response.data;
};
