import client from './api/client';
import { DatabricksDataType } from '../types/data';

interface DatabricksDataTypeResponse {
  name: string;
  category: string;
  supports_decimal_places: boolean;
}

const mapDataType = (payload: DatabricksDataTypeResponse): DatabricksDataType => ({
  name: payload.name,
  category: payload.category,
  supportsDecimalPlaces: Boolean(payload.supports_decimal_places)
});

export const fetchDatabricksDataTypes = async (): Promise<DatabricksDataType[]> => {
  const response = await client.get<DatabricksDataTypeResponse[]>('/databricks/data-types');
  return response.data.map(mapDataType);
};
