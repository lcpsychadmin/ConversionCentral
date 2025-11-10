import client from './api/client';
import { CompanySettings, CompanySettingsUpdateInput } from '../types/data';
import { DEFAULT_ACCENT_COLOR, DEFAULT_THEME_MODE } from '../theme/theme';

export const ADMIN_EMAIL_QUERY_KEY = ['application-settings', 'admin-email'] as const;
export const COMPANY_SETTINGS_QUERY_KEY = ['application-settings', 'company'] as const;

interface AdminEmailResponse {
  email: string | null;
}

interface CompanySettingsResponse {
  site_title?: string | null;
  logo_data_url?: string | null;
  siteTitle?: string | null;
  logoDataUrl?: string | null;
  theme_mode?: string | null;
  accent_color?: string | null;
  themeMode?: string | null;
  accentColor?: string | null;
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

const transformCompanySettings = (payload: CompanySettingsResponse): CompanySettings => {
  const themeModeRaw = payload.themeMode ?? payload.theme_mode ?? DEFAULT_THEME_MODE;
  const accentColorRaw = payload.accentColor ?? payload.accent_color ?? DEFAULT_ACCENT_COLOR;

  return {
    siteTitle: payload.siteTitle ?? payload.site_title ?? null,
    logoDataUrl: payload.logoDataUrl ?? payload.logo_data_url ?? null,
    themeMode: (themeModeRaw === 'dark' ? 'dark' : 'light'),
    accentColor: accentColorRaw.toLowerCase()
  };
};

export const fetchCompanySettings = async (): Promise<CompanySettings> => {
  const response = await client.get<CompanySettingsResponse>('/application-settings/company');
  return transformCompanySettings(response.data);
};

export const updateCompanySettings = async (
  input: CompanySettingsUpdateInput
): Promise<CompanySettings> => {
  const response = await client.put<CompanySettingsResponse>('/application-settings/company', {
    site_title: input.siteTitle,
    logo_data_url: input.logoDataUrl,
    theme_mode: input.themeMode,
    accent_color: input.accentColor
  });
  return transformCompanySettings(response.data);
};
