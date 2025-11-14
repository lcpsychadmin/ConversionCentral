import { useQuery } from 'react-query';

import { fetchDatabricksDataTypes } from '../services/databricksMetadataService';
import { DatabricksDataType } from '../types/data';

const DATA_TYPES_KEY = ['databricks-data-types'];

export const useDatabricksDataTypes = () => {
  return useQuery<DatabricksDataType[]>(DATA_TYPES_KEY, fetchDatabricksDataTypes, {
    staleTime: 60 * 60 * 1000
  });
};
