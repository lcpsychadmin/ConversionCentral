import client from './api/client';
/**
 * Fetch all constructed tables for an execution context
 */
export async function fetchConstructedTables(executionContextId) {
    const response = await client.get('/constructed-tables', {
        params: { executionContextId }
    });
    return response.data;
}
/**
 * Fetch all fields for a constructed table
 */
export async function fetchConstructedFields(constructedTableId) {
    const response = await client.get('/constructed-fields', {
        params: { constructedTableId }
    });
    return response.data;
}
/**
 * Fetch all data rows for a constructed table
 */
export async function fetchConstructedData(constructedTableId) {
    const response = await client.get(`/constructed-data/${constructedTableId}/by-table`);
    return response.data;
}
/**
 * Save constructed data with batch validation
 */
export async function batchSaveConstructedData(constructedTableId, request) {
    const response = await client.post(`/constructed-data/${constructedTableId}/batch-save`, request);
    return response.data;
}
/**
 * Create a single constructed data row
 */
export async function createConstructedData(data) {
    const response = await client.post('/constructed-data', data);
    return response.data;
}
/**
 * Update a single constructed data row
 */
export async function updateConstructedData(id, data) {
    const response = await client.put(`/constructed-data/${id}`, data);
    return response.data;
}
/**
 * Delete a constructed data row
 */
export async function deleteConstructedData(id) {
    await client.delete(`/constructed-data/${id}`);
}
/**
 * Fetch all validation rules for a constructed table
 */
export async function fetchValidationRules(constructedTableId) {
    const response = await client.get('/constructed-data-validation-rules', {
        params: { constructed_table_id: constructedTableId }
    });
    return response.data;
}
/**
 * Create a validation rule
 */
export async function createValidationRule(data) {
    const response = await client.post('/constructed-data-validation-rules', data);
    return response.data;
}
/**
 * Update a validation rule
 */
export async function updateValidationRule(id, data) {
    const response = await client.put(`/constructed-data-validation-rules/${id}`, data);
    return response.data;
}
/**
 * Delete a validation rule
 */
export async function deleteValidationRule(id) {
    await client.delete(`/constructed-data-validation-rules/${id}`);
}
/**
 * Fetch all process areas
 */
export async function fetchProcessAreas() {
    const response = await client.get('/process-areas');
    return response.data;
}
/**
 * Fetch all data objects for a process area
 */
export async function fetchDataObjects(processAreaId) {
    const response = await client.get('/data-objects', {
        params: { processAreaId }
    });
    return response.data;
}
/**
 * Fetch all systems
 */
export async function fetchSystems() {
    const response = await client.get('/systems');
    return response.data;
}
/**
 * Fetch ALL data definitions with construction tables (no filter)
 * Used to display all available tables upfront
 */
export async function fetchAllConstructionDefinitions() {
    try {
        const response = await client.get('/data-definitions');
        // Filter for tables marked as construction
        return response.data
            .map(def => ({
            ...def,
            tables: def.tables?.filter(table => table.isConstruction && !!table.constructedTableId) || []
        }))
            .filter(def => def.tables.length > 0);
    }
    catch (error) {
        console.error('Error fetching all construction definitions:', error);
        return [];
    }
}
/**
 * Fetch data definitions marked as construction tables
 * Filtered by dataObjectId and systemId
 */
export async function fetchConstructionDefinitions(dataObjectId, systemId) {
    const response = await client.get('/data-definitions', {
        params: { data_object_id: dataObjectId, system_id: systemId }
    });
    // Filter for tables marked as construction
    return response.data.map(def => ({
        ...def,
        tables: def.tables?.filter(table => table.isConstruction && !!table.constructedTableId) || []
    })).filter(def => def.tables.length > 0);
}
