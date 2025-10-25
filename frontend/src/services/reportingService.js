import client from './api/client';
export const fetchReportPreview = async (definition, limit = 100) => {
    const response = await client.post('/reporting/preview', {
        definition,
        limit
    });
    return response.data;
};
