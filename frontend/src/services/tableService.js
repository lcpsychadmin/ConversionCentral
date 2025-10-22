import client from './api/client';
export const mapTable = (payload) => ({
    id: payload.id,
    systemId: payload.systemId,
    name: payload.name,
    physicalName: payload.physicalName,
    schemaName: payload.schemaName ?? null,
    description: payload.description ?? null,
    tableType: payload.tableType ?? null,
    status: payload.status,
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
export const mapField = (payload) => ({
    id: payload.id,
    tableId: payload.tableId,
    name: payload.name,
    description: payload.description ?? null,
    applicationUsage: payload.applicationUsage ?? null,
    businessDefinition: payload.businessDefinition ?? null,
    enterpriseAttribute: payload.enterpriseAttribute ?? null,
    fieldType: payload.fieldType,
    fieldLength: payload.fieldLength ?? null,
    decimalPlaces: payload.decimalPlaces ?? null,
    systemRequired: payload.systemRequired,
    businessProcessRequired: payload.businessProcessRequired,
    suppressedField: payload.suppressedField,
    active: payload.active,
    legalRegulatoryImplications: payload.legalRegulatoryImplications ?? null,
    securityClassification: payload.securityClassification ?? null,
    dataValidation: payload.dataValidation ?? null,
    referenceTable: payload.referenceTable ?? null,
    groupingTab: payload.groupingTab ?? null,
    createdAt: payload.createdAt,
    updatedAt: payload.updatedAt
});
export const fetchTables = async () => {
    const response = await client.get('/tables');
    return response.data.map(mapTable);
};
export const fetchFields = async () => {
    const response = await client.get('/fields');
    return response.data.map(mapField);
};
export const createTable = async (input) => {
    const response = await client.post('/tables', {
        system_id: input.systemId,
        name: input.name,
        physical_name: input.physicalName,
        schema_name: input.schemaName ?? null,
        description: input.description ?? null,
        table_type: input.tableType ?? null,
        status: input.status ?? 'active'
    });
    return mapTable(response.data);
};
export const updateTable = async (id, input) => {
    const payload = {};
    if (input.systemId !== undefined)
        payload.system_id = input.systemId;
    if (input.name !== undefined)
        payload.name = input.name;
    if (input.physicalName !== undefined)
        payload.physical_name = input.physicalName;
    if (input.schemaName !== undefined)
        payload.schema_name = input.schemaName;
    if (input.description !== undefined)
        payload.description = input.description;
    if (input.tableType !== undefined)
        payload.table_type = input.tableType;
    if (input.status !== undefined)
        payload.status = input.status;
    const response = await client.put(`/tables/${id}`, payload);
    return mapTable(response.data);
};
export const createField = async (input) => {
    const response = await client.post('/fields', {
        table_id: input.tableId,
        name: input.name,
        description: input.description ?? null,
        application_usage: input.applicationUsage ?? null,
        business_definition: input.businessDefinition ?? null,
        enterprise_attribute: input.enterpriseAttribute ?? null,
        field_type: input.fieldType,
        field_length: input.fieldLength ?? null,
        decimal_places: input.decimalPlaces ?? null,
        system_required: input.systemRequired ?? false,
        business_process_required: input.businessProcessRequired ?? false,
        suppressed_field: input.suppressedField ?? false,
        active: input.active ?? true,
        legal_regulatory_implications: input.legalRegulatoryImplications ?? null,
        security_classification: input.securityClassification ?? null,
        data_validation: input.dataValidation ?? null,
        reference_table: input.referenceTable ?? null,
        grouping_tab: input.groupingTab ?? null
    });
    return mapField(response.data);
};
export const updateField = async (id, input) => {
    const payload = {};
    if (input.tableId !== undefined)
        payload.table_id = input.tableId;
    if (input.name !== undefined)
        payload.name = input.name;
    if (input.description !== undefined)
        payload.description = input.description;
    if (input.applicationUsage !== undefined)
        payload.application_usage = input.applicationUsage;
    if (input.businessDefinition !== undefined)
        payload.business_definition = input.businessDefinition;
    if (input.enterpriseAttribute !== undefined)
        payload.enterprise_attribute = input.enterpriseAttribute;
    if (input.fieldType !== undefined)
        payload.field_type = input.fieldType;
    if (input.fieldLength !== undefined)
        payload.field_length = input.fieldLength;
    if (input.decimalPlaces !== undefined)
        payload.decimal_places = input.decimalPlaces;
    if (input.systemRequired !== undefined)
        payload.system_required = input.systemRequired;
    if (input.businessProcessRequired !== undefined) {
        payload.business_process_required = input.businessProcessRequired;
    }
    if (input.suppressedField !== undefined)
        payload.suppressed_field = input.suppressedField;
    if (input.active !== undefined)
        payload.active = input.active;
    if (input.legalRegulatoryImplications !== undefined) {
        payload.legal_regulatory_implications = input.legalRegulatoryImplications;
    }
    if (input.securityClassification !== undefined) {
        payload.security_classification = input.securityClassification;
    }
    if (input.dataValidation !== undefined)
        payload.data_validation = input.dataValidation;
    if (input.referenceTable !== undefined)
        payload.reference_table = input.referenceTable;
    if (input.groupingTab !== undefined)
        payload.grouping_tab = input.groupingTab;
    const response = await client.put(`/fields/${id}`, payload);
    return mapField(response.data);
};
export const fetchTablePreview = async (tableId, limit = 100) => {
    const response = await client.get(`/tables/${tableId}/preview`, {
        params: {
            limit
        }
    });
    return response.data;
};
