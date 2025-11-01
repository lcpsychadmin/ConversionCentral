import client from './api/client';

export const ADMIN_EMAIL_QUERY_KEY = ['application-settings', 'admin-email'] as const;

interface AdminEmailResponse {
  email: string | null;
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
