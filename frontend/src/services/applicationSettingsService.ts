import client from './api/client';
import { CompanySettings, CompanySettingsUpdateInput } from '../types/data';

export const ADMIN_EMAIL_QUERY_KEY = ['application-settings', 'admin-email'] as const;
export const COMPANY_SETTINGS_QUERY_KEY = ['application-settings', 'company'] as const;

interface AdminEmailResponse {
  email: string | null;
}

interface CompanySettingsResponse {
  site_title: string | null;
  logo_data_url: string | null;
}

export const fetchAdminEmailSetting = async (): Promise<AdminEmailResponse> => {
  const response = await client.get<AdminEmailResponse>('/application-settings/admin-email');
  return response.data;
};

export const updateAdminEmailSetting = async (email: string): Promise<AdminEmailResponse> => {
  const response = await client.put<AdminEmailResponse>('/application-settings/admin-email', {
    email
  });
  return response.data;
};

const transformCompanySettings = (payload: CompanySettingsResponse): CompanySettings => ({
  siteTitle: payload.site_title,
  logoDataUrl: payload.logo_data_url
});

export const fetchCompanySettings = async (): Promise<CompanySettings> => {
  const response = await client.get<CompanySettingsResponse>('/application-settings/company');
  return transformCompanySettings(response.data);
};

export const updateCompanySettings = async (
  input: CompanySettingsUpdateInput
): Promise<CompanySettings> => {
  const response = await client.put<CompanySettingsResponse>('/application-settings/company', {
    site_title: input.siteTitle,
    logo_data_url: input.logoDataUrl
  });
  return transformCompanySettings(response.data);
};
