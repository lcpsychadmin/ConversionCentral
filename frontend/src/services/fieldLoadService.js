import client from './api/client';
export const fetchFieldLoads = async () => {
    const response = await client.get('/field-loads');
    return response.data;
};
